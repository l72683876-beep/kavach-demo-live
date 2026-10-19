package main

// Snapshot correctness: a snapshot must round-trip live state exactly, and
// recovery must converge to the same state whether the process died before
// or after log rotation. Every case here kills an engine mid-flight and
// rebuilds it the way main() does: recoverState, then compare seat-by-seat.

import (
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type snapshotHarness struct {
	dir      string
	bookBase string
	holdBase string
	e        *Engine
}

func newHarness(t *testing.T, seats int) *snapshotHarness {
	t.Helper()
	dir := t.TempDir()
	h := &snapshotHarness{
		dir:      dir,
		bookBase: filepath.Join(dir, "bookings.jsonl"),
		holdBase: filepath.Join(dir, "holds.jsonl"),
	}
	h.restart(t, seats)
	return h
}

// restart tears the engine down and rebuilds it the way main() does.
func (h *snapshotHarness) restart(t *testing.T, seats int) {
	t.Helper()
	e := &Engine{
		inv:  NewInventory(seats, 0),
		idem: NewIdemStore(),
		res:  NewReservationStore(),
		wl:   NewWaitlist(),
		snapDir: h.dir,
	}
	e.bookGC, _ = NewGroupCommit(h.bookBase, 10000)
	e.holdGC, _ = NewGroupCommit(h.holdBase, 10000)
	snapSeq, b, r, w, lines := recoverState(h.dir, h.bookBase, h.holdBase,
		e.inv, e.res, e.wl, e.idem, 60*time.Second, &e.bid, &e.rid, &waitlistSeq)
	t.Logf("restart: snapshot=%d bookings=%d reservations=%d waitlist=%d tailLines=%d", snapSeq, b, r, w, lines)
	if snapSeq > 0 {
		if _, p, ok := loadSnapshot(h.dir); ok {
			t.Logf("snapshot payload: %d idem entries: %v", len(p.Idem), p.Idem)
		}
	}
	h.e = e
}

// close is a deliberate no-op: the group committers' goroutines live until
// the test process ends, which is fine, and touching them at teardown raced
// TempDir cleanup.
func (h *snapshotHarness) close() {}

// seatStates returns the engine's seat array as a comparable slice.
func seatStates(e *Engine) []int32 {
	out := make([]int32, len(e.inv.seats))
	copy(out, e.inv.seats)
	return out
}

func assertSameSeats(t *testing.T, want, got []int32) {
	t.Helper()
	if len(want) != len(got) {
		t.Fatalf("seat count changed across recovery: %d vs %d", len(want), len(got))
	}
	for i := range want {
		if want[i] != got[i] {
			t.Fatalf("seat %d: before=%d after=%d", i, want[i], got[i])
		}
	}
}

func TestSnapshotRoundTrip(t *testing.T) {
	h := newHarness(t, 100)
	defer h.close()
	e := h.e

	// live traffic: two bookings, one hold, one waitlist entry
	e.appendLog("BK000001", "k1", "tenant1", "userA", []int{0, 1})
	e.inv.markSold([]int{0, 1})
	if _, known, _ := e.idem.claim("tenant1", "k1", ""); known {
		t.Fatal("k1 unexpectedly claimed")
	}
	e.idem.finish("tenant1", "k1", Result{Status: "confirmed", Seats: []int{0, 1}, BookingID: "BK000001"})

	r := &Reservation{ID: "RS000001", Tenant: "tenant1", User: "userB",
		Seats: []int{5, 6, 7}, CreatedAt: time.Now(), ExpiresAt: time.Now().Add(60 * time.Second)}
	atomic.StoreInt32(&r.State, resHeld)
	if !e.res.put(r, "k-hold") {
		t.Fatal("reservation idem alias already claimed")
	}
	e.inv.markHeld(r.Seats)
	e.appendHold("reserved", r, "")

	e.wl.join("tenant1", "k-wait", "userC", 2, 100, "default")

	wantRes, _ := e.res.get("RS000001")

	if err := e.runSnapshot(); err != nil {
		t.Fatalf("runSnapshot: %v", err)
	}
	if _, err := os.Stat(filepath.Join(h.dir, "snapshot-1.bin")); err != nil {
		t.Fatalf("snapshot file missing: %v", err)
	}
	// logs rotated into segments
	if _, err := os.Stat(h.bookBase + ".snap-1"); err != nil {
		t.Fatalf("book segment missing: %v", err)
	}

	// post-snapshot traffic: a booking and a confirm land AFTER the snapshot,
	// so recovery must pick them up from the tail
	e.appendLog("BK000002", "k2", "tenant1", "userD", []int{10})
	e.inv.markSold([]int{10})
	e.idem.finish("tenant1", "k2", Result{Status: "confirmed", Seats: []int{10}, BookingID: "BK000002"})
	if wantRes.transition(resHeld, resConfirmed) {
		wantRes.BookingID = "BK000001"
		e.appendHold("confirmed", wantRes, "BK000001")
	}
	finalSeats := seatStates(e)
	h.close()

	// crash + recover
	h.restart(t, 100)
	got := seatStates(h.e)
	assertSameSeats(t, finalSeats, got)

	res, ok := h.e.res.get("RS000001")
	if !ok {
		t.Fatal("reservation lost across snapshot recovery")
	}
	if st := atomic.LoadInt32(&res.State); st != resConfirmed {
		t.Fatalf("reservation state: want confirmed, got %d", st)
	}
	if res.BookingID != "BK000001" {
		t.Fatalf("reservation booking id: got %q", res.BookingID)
	}
	if entry, ok := h.e.idem.lookup("tenant1", "k2"); !ok || entry.BookingID != "BK000002" {
		t.Fatalf("post-snapshot idem entry lost: %+v ok=%v", entry, ok)
	}
	if entry, ok := h.e.idem.lookup("tenant1", "k1"); !ok || entry.BookingID != "BK000001" {
		t.Fatalf("pre-snapshot idem entry lost: %+v ok=%v", entry, ok)
	}
	if _, ok := h.e.res.byIdem("tenant1", "k-hold"); !ok {
		t.Fatal("reservation idempotency alias lost in snapshot")
	}
	entries := h.e.wl.listFor("tenant1", "userC")
	if len(entries) != 1 || entries[0].Seats != 2 {
		t.Fatalf("waitlist entry lost: %+v", entries)
	}
	if h.e.bid < 2 {
		t.Fatalf("booking counter regressed: %d", h.e.bid)
	}
}

func TestCrashBetweenSnapshotAndRotate(t *testing.T) {
	h := newHarness(t, 100)
	defer h.close()
	e := h.e

	e.appendLog("BK000001", "k1", "t", "u", []int{0})
	e.inv.markSold([]int{0})

	// take a snapshot the hard way, WITHOUT rotating: simulate the process
	// dying between writeSnapshot and rotate
	e.snapMu.Lock()
	e.freeze.Lock()
	e.bookGC.waitQuiesced()
	e.holdGC.waitQuiesced()
	seq := e.snapSeq + 1
	p := e.capture()
	e.snapSeq = seq
	e.freeze.Unlock()
	e.snapMu.Unlock()
	if err := writeSnapshot(h.dir, int64(seq), p); err != nil {
		t.Fatal(err)
	}

	// more traffic after the snapshot, still in the un-rotated live log
	e.appendLog("BK000002", "k2", "t", "u", []int{1})
	e.inv.markSold([]int{1})
	final := seatStates(e)
	h.close()

	h.restart(t, 100)
	// both seats must be sold: one from the snapshot, one from the tail
	assertSameSeats(t, final, seatStates(h.e))
}

func TestCorruptSnapshotFallsBackToFullReplay(t *testing.T) {
	h := newHarness(t, 100)
	defer h.close()
	e := h.e

	e.appendLog("BK000001", "k1", "t", "u", []int{0})
	e.inv.markSold([]int{0})
	if err := e.runSnapshot(); err != nil {
		t.Fatal(err)
	}
	e.appendLog("BK000002", "k2", "t", "u", []int{1})
	e.inv.markSold([]int{1})
	final := seatStates(e)
	h.close()

	// corrupt the newest snapshot's payload
	path := filepath.Join(h.dir, "snapshot-1.bin")
	raw, _ := os.ReadFile(path)
	for i := 20; i < 40; i++ {
		raw[i] ^= 0xFF
	}
	os.WriteFile(path, raw, 0644)

	h.restart(t, 100)
	// checksum failure must degrade to full log replay, never to a partial state
	assertSameSeats(t, final, seatStates(h.e))
}

func TestPruneKeepsTwoGenerations(t *testing.T) {
	h := newHarness(t, 50)
	defer h.close()
	for i := 0; i < 3; i++ {
		if err := h.e.runSnapshot(); err != nil {
			t.Fatal(err)
		}
	}
	for _, name := range []string{"snapshot-1.bin", "bookings.jsonl.snap-1", "holds.jsonl.snap-1"} {
		if _, err := os.Stat(filepath.Join(h.dir, name)); !os.IsNotExist(err) {
			t.Fatalf("%s should be pruned after a third snapshot", name)
		}
	}
	for _, name := range []string{"snapshot-2.bin", "snapshot-3.bin"} {
		if _, err := os.Stat(filepath.Join(h.dir, name)); err != nil {
			t.Fatalf("%s should be retained: %v", name, err)
		}
	}
	// recovery from the newest snapshot must still see all state
	h.close()
	h.restart(t, 50)
	total, sold, held := h.e.inv.stats()
	if sold+held != 0 || total != 50 {
		t.Fatalf("unexpected inventory after triple snapshot: %d/%d/%d", total, sold, held)
	}
}

func TestConcurrentTrafficUnderSnapshot(t *testing.T) {
	h := newHarness(t, 500)
	defer h.close()
	e := h.e

	var wg sync.WaitGroup
	stop := make(chan struct{})
	workers := 8
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for i := 0; ; i++ {
				select {
				case <-stop:
					return
				default:
				}
				seats := []int{(id*100 + i) % 400}
				id2 := fmt.Sprintf("BK%06d", id*100000+i)
				key := fmt.Sprintf("k-%d-%d", id, i)
				e.freeze.RLock()
				// engine semantics: state mutates BEFORE the log line is
				// committed, so a snapshot's offset always covers events
				// whose effects are already in memory
				e.inv.markSold(seats)
				if _, known, conflict := e.idem.claim("t", key, ""); !known && !conflict {
					e.idem.finish("t", key, Result{Status: "confirmed", Seats: seats, BookingID: id2})
				}
				e.appendLog(id2, key, "t", fmt.Sprintf("u%d", id), seats)
				e.freeze.RUnlock()
			}
		}(w)
	}
	// snapshots while traffic runs: freeze must never deadlock or lose a line
	for i := 0; i < 3; i++ {
		time.Sleep(30 * time.Millisecond)
		if err := e.runSnapshot(); err != nil {
			t.Fatal(err)
		}
	}
	close(stop)
	wg.Wait()
	final := seatStates(e)
	h.close()

	h.restart(t, 500)
	assertSameSeats(t, final, seatStates(h.e))
}
