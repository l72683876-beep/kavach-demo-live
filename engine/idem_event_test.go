package main

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

// newIdemEngine: a small working engine for fingerprint regression tests,
// shaped like newBenchEngine but on testing.T.
func newIdemEngine(t *testing.T, seats int) *Engine {
	t.Helper()
	dir := t.TempDir()
	e := &Engine{
		inv:       NewInventory(seats, 0),
		userLim:   NewLimiter(1e9, 1e9),
		ipLim:     NewLimiter(1e9, 1e9),
		auth:      NewAuth("bench:btenant", "bsecret"),
		idem:      NewIdemStore(),
		res:       NewReservationStore(),
		queue:     make(chan *job, 1000),
		lat:       NewLatencyRing(1000),
		holdTTL:   300 * time.Second,
		wl:        NewWaitlist(),
		waitCap:   1000,
		webhookCh: make(chan map[string]interface{}, 100),
		httpc:     &http.Client{Timeout: 5 * time.Second},
		started:   time.Now(),
	}
	gc1, err := NewGroupCommit(dir+"/b.jsonl", 1000)
	if err != nil {
		t.Fatal(err)
	}
	gc2, err := NewGroupCommit(dir+"/h.jsonl", 1000)
	if err != nil {
		t.Fatal(err)
	}
	e.bookGC, e.holdGC = gc1, gc2
	for i := 0; i < 2; i++ {
		go e.worker()
	}
	return e
}

func postBook(t *testing.T, e *Engine, body, query string) *httptest.ResponseRecorder {
	t.Helper()
	mux := http.NewServeMux()
	mux.HandleFunc("/book", e.handleBook)
	r := httptest.NewRequest("POST", "/book"+query, strings.NewReader(body))
	signedReq(r, []byte(body), e.auth.secret, fmt.Sprintf("r%d", time.Now().UnixNano()))
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, r)
	return w
}

// TestIdemKeyBoundToEvent: the idempotency fingerprint must include the event.
// The same key + user + seats on a DIFFERENT event is a conflict, never a
// silent replay of the first event's booking (red-team run 11 finding).
func TestIdemKeyBoundToEvent(t *testing.T) {
	e := newIdemEngine(t, 100)
	mk := func(ev string) string {
		return fmt.Sprintf(`{"user_id":"u1","idempotency_key":"k1","seats":2,"event_id":%q}`, ev)
	}

	w1 := postBook(t, e, mk("evA"), "")
	if w1.Code != http.StatusOK {
		t.Fatalf("first book: %d %s", w1.Code, w1.Body.String())
	}

	// same key on a different event: conflict, and the second event must
	// not consume or reuse the first booking
	w2 := postBook(t, e, mk("evB"), "")
	if w2.Code != http.StatusConflict {
		t.Fatalf("same key on a different event must 409 conflict, got %d %s", w2.Code, w2.Body.String())
	}

	// a genuine retry of the first request still replays its stored result
	w3 := postBook(t, e, mk("evA"), "")
	if w3.Code != http.StatusOK {
		t.Fatalf("retry replay broken: %d %s", w3.Code, w3.Body.String())
	}
	if w3.Body.String() != w1.Body.String() {
		t.Fatalf("retry body changed:\n got  %s\n want %s", w3.Body.String(), w1.Body.String())
	}

	// the event named by the query parameter must fingerprint the same as
	// the same event named in the body
	w4 := postBook(t, e, `{"user_id":"u1","idempotency_key":"k1","seats":2}`, "?event=evA")
	if w4.Code != http.StatusOK {
		t.Fatalf("query-param retry must replay the same result, got %d %s", w4.Code, w4.Body.String())
	}
	if w4.Body.String() != w1.Body.String() {
		t.Fatalf("query/body naming disagree:\n query %s\n body  %s", w4.Body.String(), w1.Body.String())
	}
}

// TestWaitlistKeyBoundToEvent: the waitlist idem key is bound to the event too.
func TestWaitlistKeyBoundToEvent(t *testing.T) {
	e := newIdemEngine(t, 4) // tiny pool: sell it out so joins actually queue
	for _, k := range []string{"ka", "kb"} {
		body := fmt.Sprintf(`{"user_id":"u9","idempotency_key":%q,"seats":2,"event_id":"warmup"}`, k)
		w := postBook(t, e, body, "")
		if w.Code != http.StatusOK {
			t.Fatalf("warmup book %s: %d %s", k, w.Code, w.Body.String())
		}
	}
	mk := func(ev string) string {
		return fmt.Sprintf(`{"user_id":"u1","idempotency_key":"w1","seats":2,"event_id":%q}`, ev)
	}
	mux := http.NewServeMux()
	mux.HandleFunc("/waitlist", e.handleWaitlistJoin)
	post := func(body string) *httptest.ResponseRecorder {
		r := httptest.NewRequest("POST", "/waitlist", strings.NewReader(body))
		signedReq(r, []byte(body), e.auth.secret, fmt.Sprintf("w%d", time.Now().UnixNano()))
		w := httptest.NewRecorder()
		mux.ServeHTTP(w, r)
		return w
	}
	w1 := post(mk("evA"))
	if w1.Code != http.StatusOK {
		t.Fatalf("first join: %d %s", w1.Code, w1.Body.String())
	}
	w2 := post(mk("evB"))
	if w2.Code != http.StatusConflict {
		t.Fatalf("same key on a different event must 409 conflict, got %d %s", w2.Code, w2.Body.String())
	}
	w3 := post(mk("evA"))
	if w3.Code != http.StatusOK || w3.Body.String() != w1.Body.String() {
		t.Fatalf("retry replay broken: %d %s vs %s", w3.Code, w3.Body.String(), w1.Body.String())
	}
}
