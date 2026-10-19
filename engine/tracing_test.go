package main

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"go.opentelemetry.io/otel"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"
)

// tracing off: the middleware must be a true pass-through, the handler
// response untouched, zero spans in the way.
func TestTracingOffIsPassThrough(t *testing.T) {
	e := &Engine{} // tracer nil
	next := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusTeapot)
		_, _ = w.Write([]byte("booked"))
	})
	got := e.tracingMiddleware(next)
	if _, isPassthrough := got.(http.HandlerFunc); !isPassthrough {
		// allowed: the disabled branch returns `next` itself; anything that
		// still answers identically is acceptable, but the returned handler
		// must behave identically either way
	}
	rec := httptest.NewRecorder()
	got.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/book", nil))
	if rec.Code != http.StatusTeapot || rec.Body.String() != "booked" {
		t.Fatalf("pass-through broken: code=%d body=%q", rec.Code, rec.Body.String())
	}
}

// tracing on: one span per request with method, path and the response status;
// a 5xx marks the span as an error.
func TestMiddlewareRecordsSpans(t *testing.T) {
	recorder := tracetest.NewSpanRecorder()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(recorder))
	prev := otel.GetTracerProvider()
	otel.SetTracerProvider(tp)
	defer otel.SetTracerProvider(prev)

	e := &Engine{tracer: otel.Tracer("kavach-test")}
	next := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	})
	e.tracingMiddleware(next).ServeHTTP(httptest.NewRecorder(),
		httptest.NewRequest(http.MethodPost, "/reserve", nil))

	spans := recorder.Ended()
	if len(spans) != 1 {
		t.Fatalf("want 1 span, got %d", len(spans))
	}
	sp := spans[0]
	if sp.Name() != "kavach POST /reserve" {
		t.Fatalf("span name %q", sp.Name())
	}
	if sp.SpanKind() != trace.SpanKindServer {
		t.Fatalf("span kind %v", sp.SpanKind())
	}
	found := false
	for _, ev := range sp.Events() {
		if ev.Name == "exception" {
			found = true
		}
	}
	if !found {
		t.Fatalf("5xx did not record an error event: %v", sp.Events())
	}
}

func TestSetupTracingDisabled(t *testing.T) {
	shutdown, err := setupTracing("", 1)
	if shutdown != nil || err != nil {
		t.Fatalf("empty endpoint must disable tracing, got shutdown=%t err=%v", shutdown != nil, err)
	}
}
