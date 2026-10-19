package main

// OpenTelemetry tracing, off by default. With -otel-endpoint set the engine
// exports OTLP/HTTP spans (one per request) to any OTLP collector (Jaeger,
// Grafana Tempo, an OpenTelemetry Collector); with it unset the middleware
// compiles away to a pass-through and the hot path is untouched.

import (
	"context"
	"crypto/tls"
	"net/http"
	"strings"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	"go.opentelemetry.io/otel/propagation"
	sdkresource "go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.26.0"
	"go.opentelemetry.io/otel/trace"
)

// setupTracing initializes the global tracer provider for OTLP/HTTP export.
// It returns a shutdown function (flush outstanding spans) and an error.
// An empty endpoint means tracing stays off: the return is (nil-shutdown, nil).
func setupTracing(endpoint string, ratio float64) (func(context.Context) error, error) {
	if endpoint == "" {
		return nil, nil
	}
	if ratio <= 0 || ratio > 1 {
		ratio = 1
	}
	// accept bare host:port and https:// variants the way collectors are
	// commonly configured
	if !strings.Contains(endpoint, "://") {
		endpoint = "http://" + endpoint
	}
	exporter, err := otlptracehttp.New(context.Background(),
		otlptracehttp.WithEndpointURL(endpoint),
		otlptracehttp.WithTLSClientConfig(&tls.Config{InsecureSkipVerify: strings.HasPrefix(endpoint, "http://")}),
		otlptracehttp.WithTimeout(5*time.Second))
	if err != nil {
		return nil, err
	}
	res, err := sdkresource.Merge(sdkresource.Default(),
		sdkresource.NewWithAttributes(semconv.SchemaURL,
			semconv.ServiceName("kavach"),
			semconv.ServiceVersion("2.0.0")))
	if err != nil {
		return nil, err
	}
	tp := sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(exporter),
		sdktrace.WithResource(res),
		sdktrace.WithSampler(sdktrace.ParentBased(sdktrace.TraceIDRatioBased(ratio))))
	otel.SetTracerProvider(tp)
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(
		propagation.TraceContext{}, propagation.Baggage{}))
	return tp.Shutdown, nil
}

// statusWriter captures the response status so the span can record it.
type statusWriter struct {
	http.ResponseWriter
	status int
}

func (w *statusWriter) WriteHeader(code int) {
	w.status = code
	w.ResponseWriter.WriteHeader(code)
}

func (w *statusWriter) Write(b []byte) (int, error) {
	if w.status == 0 {
		w.status = http.StatusOK
	}
	return w.ResponseWriter.Write(b)
}

// tracingMiddleware wraps the mux with one request span each. Disabled
// (tracer nil) it returns the original handler unchanged: zero overhead.
func (e *Engine) tracingMiddleware(next http.Handler) http.Handler {
	if e.tracer == nil {
		return next
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ctx, span := e.tracer.Start(r.Context(), "kavach "+r.Method+" "+r.URL.Path,
			trace.WithSpanKind(trace.SpanKindServer),
			trace.WithAttributes(
				semconv.HTTPRequestMethodKey.String(r.Method),
				semconv.URLPath(r.URL.Path),
				semconv.NetworkProtocolVersion(r.Proto)))
		sw := &statusWriter{ResponseWriter: w}
		next.ServeHTTP(sw, r.WithContext(ctx))
		status := sw.status
		if status == 0 {
			status = http.StatusOK
		}
		span.SetAttributes(semconv.HTTPResponseStatusCode(status))
		if status >= 500 {
			span.RecordError(httpError{status: status})
		}
		span.End()
	})
}

type httpError struct{ status int }

func (e httpError) Error() string { return "http " + itoa(e.status) }

func itoa(i int) string {
	if i == 0 {
		return "0"
	}
	var b [4]byte
	p := len(b)
	neg := i < 0
	if neg {
		i = -i
	}
	for i > 0 {
		p--
		b[p] = byte('0' + i%10)
		i /= 10
	}
	if neg {
		p--
		b[p] = '-'
	}
	return string(b[p:])
}
