package main

import (
	"bytes"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestShardOfDeterministic(t *testing.T) {
	for _, ev := range []string{"default", "concert-a", "icc-final-2026", "x"} {
		first := ShardOf(ev, 4)
		for i := 0; i < 50; i++ {
			if got := ShardOf(ev, 4); got != first {
				t.Fatalf("ShardOf(%q) moved: %d then %d", ev, first, got)
			}
		}
	}
	if got := ShardOf("anything", 1); got != 0 {
		t.Fatalf("n=1 must always be shard 0, got %d", got)
	}
}

func TestShardOfSpreads(t *testing.T) {
	counts := make([]int, 4)
	for i := 0; i < 4000; i++ {
		counts[ShardOf("event-"+strings.Repeat("k", i%7)+"-"+string(rune('a'+i%26))+string(rune(i)), 4)]++
	}
	for i, c := range counts {
		if c < 500 || c > 1500 { // ~1000 expected; wild skew would starve a shard
			t.Fatalf("shard %d got %d of 4000 keys", i, c)
		}
	}
}

func TestRoutePicksShardAndRestoresBody(t *testing.T) {
	var gotBody string
	var gotEngine *Engine
	router := NewShardRouter([]*Engine{{}, {}, {}})
	h := func(e *Engine, w http.ResponseWriter, r *http.Request) {
		gotEngine = e
		b := make([]byte, 4096)
		n, _ := r.Body.Read(b)
		gotBody = string(b[:n])
	}
	req := httptest.NewRequest(http.MethodPost, "/book", strings.NewReader(`{"user_id":"u1","idempotency_key":"k1","seats":2,"event_id":"concert-a"}`))
	w := httptest.NewRecorder()
	router.route(h, true)(w, req)
	if gotEngine != router.shards[ShardOf("concert-a", 3)] {
		t.Fatalf("wrong shard picked")
	}
	if gotBody != `{"user_id":"u1","idempotency_key":"k1","seats":2,"event_id":"concert-a"}` {
		t.Fatalf("body not restored whole: %q", gotBody)
	}
}

func TestRouteBadEventID(t *testing.T) {
	router := NewShardRouter([]*Engine{{}})
	w := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/book", strings.NewReader(`{"event_id":"bad id with spaces"}`))
	router.route(func(*Engine, http.ResponseWriter, *http.Request) {}, true)(w, req)
	if w.Code != http.StatusBadRequest {
		t.Fatalf("bad event id must be 400, got %d", w.Code)
	}
}

func TestScanReplaysFirstHit(t *testing.T) {
	notFound := func(*Engine, http.ResponseWriter, *http.Request) {
		writeJSON(httptest.NewRecorder(), http.StatusNotFound, Result{Status: "not_found"})
	}
	// shard 0 misses, shard 1 answers
	e0 := &Engine{}
	hits := map[*Engine]string{}
	router := NewShardRouter([]*Engine{e0, {}})
	h := func(e *Engine, w http.ResponseWriter, r *http.Request) {
		if e == e0 {
			writeJSON(w, http.StatusNotFound, Result{Status: "not_found"})
			return
		}
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`{"status":"held"}`))
		hits[e] = "yes"
	}
	_ = notFound
	w := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/reservation?id=RSV00000001", nil)
	router.scan(h, false)(w, req)
	if w.Code != http.StatusOK || !strings.Contains(w.Body.String(), "held") {
		t.Fatalf("scan must replay the hitting shard, got %d %s", w.Code, w.Body.String())
	}
	if _, ok := hits[e0]; ok {
		t.Fatalf("missed shard answered")
	}
}

func TestProbeWriterReplay(t *testing.T) {
	p := newProbeWriter()
	p.Header().Set("Retry-After", "2")
	p.WriteHeader(http.StatusConflict)
	p.Write([]byte(`{"status":"expired"}`))
	w := httptest.NewRecorder()
	p.replay(w)
	if w.Code != http.StatusConflict || !bytes.Contains(w.Body.Bytes(), []byte("expired")) || w.Header().Get("Retry-After") != "2" {
		t.Fatalf("replay lost state: %d %s", w.Code, w.Body.String())
	}
}

// An oversized body must reach the handler still oversized: routing reads a
// 16 KiB+1 window to find event_id and restores what it read, so the
// handler's MaxBytesReader rejects it with the same 400 it always returned.
// Restoring nothing made the handler see an empty body and fail the
// signature with 401 instead of the size, which is the wrong denial.
func TestOversizedBodySurvivesRouting(t *testing.T) {
	big := bytes.Repeat([]byte("x"), 20000)
	r := httptest.NewRequest(http.MethodPost, "/reserve", bytes.NewReader(big))
	requestEventID(r, true) // routing consumed the body only to find event_id
	restored, _ := io.ReadAll(r.Body)
	if !bytes.Equal(restored, big[:16385]) {
		t.Fatalf("oversized body not restored as its own prefix: got %d bytes, want 16385", len(restored))
	}
	mr := http.MaxBytesReader(httptest.NewRecorder(), io.NopCloser(bytes.NewReader(restored)), 16384)
	if _, err := io.ReadAll(mr); err == nil {
		t.Fatal("MaxBytesReader accepted a restored oversized body")
	}
	// a normal body must still round-trip byte for byte
	small := []byte(`{"event_id":"concert-a","seats":[1]}`)
	r2 := httptest.NewRequest(http.MethodPost, "/book", bytes.NewReader(small))
	requestEventID(r2, true)
	restored2, _ := io.ReadAll(r2.Body)
	if !bytes.Equal(restored2, small) {
		t.Fatalf("normal body corrupted by routing: %q", restored2)
	}
}
