package main

// In-process micro-benchmarks: how long does ONE request take when there is
// no network at all? This measures the engine's own decision path
// (auth -> rate limit -> idempotency -> worker -> CAS -> group-commit),
// which is the smallest number Kavach can honestly claim.
//
//   go test -bench . -benchtime 20000x -benchmem

import (
	"bytes"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func newBenchEngine(b *testing.B, seats int) *Engine {
	dir := b.TempDir()
	e := &Engine{
		inv:       NewInventory(seats, 0),
		userLim:   NewLimiter(1e9, 1e9),
		ipLim:     NewLimiter(1e9, 1e9),
		auth:      NewAuth("bench:btenant", "bsecret"),
		idem:      NewIdemStore(),
		res:       NewReservationStore(),
		queue:     make(chan *job, 100000),
		lat:       NewLatencyRing(100000),
		holdTTL:   300 * time.Second,
		wl:        NewWaitlist(),
		waitCap:   100000,
		webhookCh: make(chan map[string]interface{}, 1000),
		httpc:     &http.Client{Timeout: 5 * time.Second},
		started:   time.Now(),
	}
	gc1, err := NewGroupCommit(dir+"/b.jsonl", 200000)
	if err != nil {
		b.Fatal(err)
	}
	gc2, err := NewGroupCommit(dir+"/h.jsonl", 200000)
	if err != nil {
		b.Fatal(err)
	}
	e.bookGC, e.holdGC = gc1, gc2
	for i := 0; i < 4; i++ {
		go e.worker()
	}
	return e
}

func signedReq(r *http.Request, body []byte, secret []byte, rid string) *http.Request {
	ts := fmt.Sprintf("%d", time.Now().Unix())
	mac := hmac.New(sha256.New, secret)
	mac.Write([]byte(ts + "\n" + rid + "\n" + r.Method + "\n" + r.URL.RequestURI() + "\n" + "bench" + "\n"))
	mac.Write(body)
	r.Header.Set("Authorization", "bench")
	r.Header.Set("X-Kavach-Timestamp", ts)
	r.Header.Set("X-Kavach-Request-Id", rid)
	r.Header.Set("X-Kavach-Signature", hex.EncodeToString(mac.Sum(nil)))
	return r
}

func BenchmarkLimiterAllow(b *testing.B) {
	lim := NewLimiter(1e9, 1e9)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if !lim.Allow("bench-user") {
			b.Fatal("limited")
		}
	}
}

func BenchmarkBookInProcess(b *testing.B) {
	e := newBenchEngine(b, 1000000)
	mux := http.NewServeMux()
	mux.HandleFunc("/book", e.handleBook)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		body := []byte(fmt.Sprintf(`{"user_id":"u%d","idempotency_key":"k%d","seats":1}`, i%10000, i))
		r := httptest.NewRequest("POST", "/book", bytes.NewReader(body))
		signedReq(r, body, e.auth.secret, fmt.Sprintf("r%d", i))
		w := httptest.NewRecorder()
		mux.ServeHTTP(w, r)
		if w.Code != http.StatusOK {
			b.Fatalf("code %d: %s", w.Code, w.Body.String())
		}
	}
}

func BenchmarkSeatsReadInProcess(b *testing.B) {
	e := newBenchEngine(b, 100000)
	mux := http.NewServeMux()
	mux.HandleFunc("/seats", e.handleSeats)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		w := httptest.NewRecorder()
		mux.ServeHTTP(w, httptest.NewRequest("GET", "/seats", nil))
		if w.Code != http.StatusOK {
			b.Fatalf("code %d", w.Code)
		}
	}
}
