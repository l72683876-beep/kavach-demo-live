package main

// Replication (cluster phase 1): a follower must converge to exactly the
// leader's state machine by tailing its WAL, keep converging across a
// leader restart, refuse writes while it is a follower, and the leader's
// stream endpoint must reject unauthenticated callers.

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func newReplEngine(t *testing.T, dir string, seats int) *Engine {
	t.Helper()
	e := &Engine{
		inv:     NewInventory(seats, 0),
		idem:    NewIdemStore(),
		res:     NewReservationStore(),
		wl:      NewWaitlist(),
		snapDir: dir,
		holdTTL: 60 * time.Second,
	}
	e.bookGC, _ = NewGroupCommit(dir+"/bookings.jsonl", 10000)
	e.holdGC, _ = NewGroupCommit(dir+"/holds.jsonl", 10000)
	recoverState(dir, e.bookGC.path, e.holdGC.path,
		e.inv, e.res, e.wl, e.idem, e.holdTTL, &e.bid, &e.rid, &waitlistSeq)
	return e
}

func driveTraffic(t *testing.T, e *Engine, bookings, holds int) int {
	t.Helper()
	lines := 0
	for i := 0; i < bookings; i++ {
		id := fmt.Sprintf("BK%08d", atomic.AddUint64(&e.bid, 1))
		key := fmt.Sprintf("rk-%d", i)
		e.freeze.RLock()
		e.inv.markSold([]int{i % 100})
		if _, known, conflict := e.idem.claim("t", key, ""); !known && !conflict {
			e.idem.finish("t", key, Result{Status: "confirmed", Seats: []int{i % 100}, BookingID: id})
		}
		e.appendLog(id, key, "t", "u", []int{i % 100})
		e.freeze.RUnlock()
		lines++
	}
	for i := 0; i < holds; i++ {
		rid := fmt.Sprintf("RS%08d", atomic.AddUint64(&e.rid, 1))
		r := &Reservation{ID: rid, Tenant: "t", User: "u", Seats: []int{100 + i},
			ExpiresAt: time.Now().Add(60 * time.Second), State: resHeld}
		e.freeze.RLock()
		e.res.put(r, "")
		e.inv.markHeld(r.Seats)
		e.appendHold("reserved", r, "")
		e.freeze.RUnlock()
		lines++
		if i%2 == 0 { // half confirm, a third of the rest release
			e.freeze.RLock()
			r.transition(resHeld, resConfirmed)
			e.appendHold("confirmed", r, "BKFAKE")
			e.freeze.RUnlock()
			lines++
		} else if i%3 == 0 {
			e.freeze.RLock()
			atomic.StoreInt32(&r.State, resReleased)
			e.inv.freeHeld(r.Seats)
			e.appendHold("released", r, "")
			e.freeze.RUnlock()
			lines++
		}
	}
	return lines
}

func resStates(e *Engine) map[string]int32 {
	e.res.mu.Lock()
	defer e.res.mu.Unlock()
	out := map[string]int32{}
	for id, r := range e.res.m {
		out[id] = atomic.LoadInt32(&r.State)
	}
	return out
}

func waitForCatchup(t *testing.T, follower *Engine, wantLines int64) {
	t.Helper()
	deadline := time.Now().Add(8 * time.Second)
	for time.Now().Before(deadline) {
		if atomic.LoadInt64(&follower.replicatedLines) >= wantLines &&
			follower.replBookPos+follower.replHoldPos > 0 {
			time.Sleep(400 * time.Millisecond) // let the next applier tick settle
			return
		}
		time.Sleep(150 * time.Millisecond)
	}
	t.Fatalf("follower did not catch up: replicatedLines=%d want>=%d", atomic.LoadInt64(&follower.replicatedLines), wantLines)
}

func assertSameState(t *testing.T, leader, follower *Engine) {
	t.Helper()
	lSeats, fSeats := seatStates(leader), seatStates(follower)
	for i := range lSeats {
		if lSeats[i] != fSeats[i] {
			t.Fatalf("seat %d diverged: leader=%d follower=%d", i, lSeats[i], fSeats[i])
		}
	}
	lRes, fRes := resStates(leader), resStates(follower)
	if len(lRes) != len(fRes) {
		t.Fatalf("reservation count diverged: leader=%d follower=%d", len(lRes), len(fRes))
	}
	for id, st := range lRes {
		if fRes[id] != st {
			t.Fatalf("reservation %s diverged: leader=%d follower=%d", id, st, fRes[id])
		}
	}
}

func TestReplicationFollowerConverges(t *testing.T) {
	dirL, dirF := t.TempDir(), t.TempDir()
	leader := newReplEngine(t, dirL, 200)
	follower := newReplEngine(t, dirF, 200)
	leader.replKey, leader.replSecret = "replkey", []byte("replsecret")
	follower.replKey, follower.replSecret = "replkey", []byte("replsecret")

	var up atomic.Bool
	up.Store(true)
	mux := http.NewServeMux()
	mux.HandleFunc("/internal/replicate", func(w http.ResponseWriter, r *http.Request) {
		if !up.Load() {
			http.Error(w, `{"status":"unavailable"}`, http.StatusServiceUnavailable)
			return
		}
		leader.handleReplicate(w, r)
	})
	srv := httptest.NewServer(mux)
	t.Cleanup(func() {
		srv.CloseClientConnections() // the stream handlers run until the client hangs up
		srv.Close()
	})

	follower.replFrom = srv.URL
	follower.runFollower()

	// burst one: traffic BEFORE and WHILE the follower tails
	n1 := driveTraffic(t, leader, 40, 12)
	waitForCatchup(t, follower, int64(n1))
	assertSameState(t, leader, follower)

	// burst two: the leader's stream is torn down and refused, traffic keeps
	// flowing, then the leader comes back: the follower must resume from its
	// own WAL tail, never re-apply a line it already has, never skip one.
	up.Store(false)
	srv.CloseClientConnections()
	n2 := driveTraffic(t, leader, 20, 6)
	up.Store(true)
	waitForCatchup(t, follower, int64(n1+n2))
	assertSameState(t, leader, follower)
}

func TestFollowerRejectsWrites(t *testing.T) {
	e := newReplEngine(t, t.TempDir(), 100)
	e.replFrom = "http://127.0.0.1:1"
	mux := http.NewServeMux()
	mux.HandleFunc("/book", e.handleBook)
	srv := httptest.NewServer(mux)
	defer srv.Close()
	resp, err := http.Post(srv.URL+"/book", "application/json", strings.NewReader(`{"seats":[1],"idempotency_key":"k"}`))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("follower write: got %d want 503", resp.StatusCode)
	}
}

func TestReplicateRequiresAuth(t *testing.T) {
	leader := newReplEngine(t, t.TempDir(), 100)
	leader.replKey, leader.replSecret = "replkey", []byte("replsecret")
	srv := httptest.NewServer(http.HandlerFunc(leader.handleReplicate))
	defer srv.Close()

	resp, err := http.Get(srv.URL + "/internal/replicate?log=bookings&after=0")
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("unsigned replicate: got %d want 401", resp.StatusCode)
	}

	req, _ := http.NewRequest("GET", srv.URL+"/internal/replicate?log=bookings&after=0", nil)
	for k, v := range signReplicationRequest("replkey", []byte("WRONG"), "GET", "/internal/replicate?log=bookings&after=0") {
		req.Header.Set(k, v)
	}
	resp, err = http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("wrong-secret replicate: got %d want 401", resp.StatusCode)
	}

	// the right secret streams frames
	driveTraffic(t, leader, 5, 0)
	req, _ = http.NewRequest("GET", srv.URL+"/internal/replicate?log=bookings&after=0", nil)
	for k, v := range signReplicationRequest("replkey", []byte("replsecret"), "GET", "/internal/replicate?log=bookings&after=0") {
		req.Header.Set(k, v)
	}
	resp, err = http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("signed replicate: got %d want 200", resp.StatusCode)
	}
	buf := make([]byte, 4096)
	n, _ := resp.Body.Read(buf)
	if !strings.Contains(string(buf[:n]), `"line"`) {
		t.Fatalf("signed replicate streamed no frames: %q", string(buf[:n]))
	}
}
