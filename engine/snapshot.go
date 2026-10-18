package main

// ---------- WAL snapshots + compaction ----------
//
// The append-only logs are the source of truth and recovery replays them from
// line zero. That is correct but recovery time is linear in log age, and the
// logs only ever grow. Snapshots fix both: periodically the engine freezes
// writes for the microseconds it takes to copy live state, dumps it to a
// checksummed snapshot file, then rotates the logs so new writes start a
// fresh segment. Recovery becomes "load the newest snapshot + replay the
// short tail after its recorded offset".
//
// The durability guarantee is unchanged: a snapshot only ever captures state
// whose commits were already fsynced, and the snapshot is fully on disk
// before the logs rotate. Every crash lands in one of two recoverable
// states: snapshot + tail replay, or full log replay.

import (
	"bufio"
	"crypto/sha256"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"os"
	"log"
	"path/filepath"
	"sort"
	"strings"
	"sync/atomic"
	"time"
)

const snapshotMagic = "KVS1"
const snapshotVersion = uint32(1)

// snapshotPayload is everything replayLogs reconstructs, captured at one
// instant. The offsets record the fsynced byte position of each log at
// capture time: events up to those offsets are INSIDE the snapshot, events
// after them are replayed from the log tail.
type snapshotPayload struct {
	Seq        int64  `json:"seq"`
	BookSource string `json:"book_source"`
	BookOffset int64  `json:"book_offset"`
	HoldSource string `json:"hold_source"`
	HoldOffset int64  `json:"hold_offset"`
	CreatedAt  int64  `json:"created_at"`

	Seats      []int32 `json:"seats"`
	SeatOffset int     `json:"seat_offset"`

	Reservations []snapReservation `json:"reservations"`
	ResIdem      map[string]string `json:"res_idem,omitempty"`
	Waitlist     []snapWaitEntry   `json:"waitlist"`
	WaitlistSeq  uint64            `json:"waitlist_seq"`
	Idem         map[string]snapIdem `json:"idem"`
	Bid          uint64            `json:"bid"`
	Rid          uint64            `json:"rid"`
}

type snapReservation struct {
	ID          string `json:"id"`
	Tenant      string `json:"tenant"`
	User        string `json:"user"`
	Seats       []int  `json:"seats"`
	State       int32  `json:"state"`
	Fingerprint string `json:"fingerprint,omitempty"`
	CreatedAt   int64  `json:"created_at"`
	ExpiresAt   int64  `json:"expires_at"`
	BookingID   string `json:"booking_id,omitempty"`
}

type snapWaitEntry struct {
	ID       string `json:"id"`
	Tenant   string `json:"tenant"`
	User     string `json:"user"`
	Seats    int    `json:"seats"`
	QueuedAt int64  `json:"queued_at"`
}

type snapIdem struct {
	Status      string `json:"status"`
	Seats       []int  `json:"seats,omitempty"`
	BookingID   string `json:"booking_id,omitempty"`
	Fingerprint string `json:"fingerprint,omitempty"`
	LastSeen    int64  `json:"last_seen"`
}

// ---------- capture ----------

// capture copies live state under the engine freeze lock. Callers must hold
// e.freeze exclusively and both group committers must be quiesced, so the
// copy is an exact instant: every included event was already fsynced, and no
// new event can be applied while the copy runs.
func (e *Engine) capture() *snapshotPayload {
	p := &snapshotPayload{
		BookSource: e.bookGC.path,
		HoldSource: e.holdGC.path,
		BookOffset: e.bookGC.flushedBytes(),
		HoldOffset: e.holdGC.flushedBytes(),
		CreatedAt:  time.Now().UnixNano(),
		Seq:        int64(atomic.LoadUint64(&e.snapSeq)),
	}

	p.Seats = make([]int32, len(e.inv.seats))
	copy(p.Seats, e.inv.seats)
	p.SeatOffset = e.inv.offset

	e.res.mu.Lock()
	p.Reservations = make([]snapReservation, 0, len(e.res.m))
	for _, r := range e.res.m {
		seats := make([]int, len(r.Seats))
		copy(seats, r.Seats)
		p.Reservations = append(p.Reservations, snapReservation{
			ID: r.ID, Tenant: r.Tenant, User: r.User, Seats: seats,
			State: atomic.LoadInt32(&r.State), Fingerprint: r.Fingerprint,
			CreatedAt: r.CreatedAt.UnixNano(), ExpiresAt: r.ExpiresAt.UnixNano(),
			BookingID: r.BookingID,
		})
	}
	// the idempotency alias index: reservation id per tenant|key
	p.ResIdem = make(map[string]string, len(e.res.idemIndex))
	for k, id := range e.res.idemIndex {
		p.ResIdem[k] = id
	}
	e.res.mu.Unlock()

	e.wl.mu.Lock()
	p.Waitlist = make([]snapWaitEntry, 0, len(e.wl.order))
	for _, w := range e.wl.order {
		p.Waitlist = append(p.Waitlist, snapWaitEntry{
			ID: w.ID, Tenant: w.Tenant, User: w.User, Seats: w.Seats,
			QueuedAt: w.QueuedAt.UnixNano(),
		})
	}
	e.wl.mu.Unlock()
	p.WaitlistSeq = atomic.LoadUint64(&waitlistSeq)

	// idempotency store: only settled entries from the last 10 minutes
	// (one hold window plus slack). In-flight entries are deliberately
	// dropped: a crashed "processing" entry is exactly what a client retry
	// resolves, which is the same contract a restart has always had.
	cutoff := time.Now().Add(-10 * time.Minute)
	e.idem.mu.Lock()
	idem := make(map[string]snapIdem, len(e.idem.m))
	for k, entry := range e.idem.m {
		if entry.Status == "processing" || entry.LastSeen.Before(cutoff) {
			continue
		}
		seats := make([]int, len(entry.Seats))
		copy(seats, entry.Seats)
		idem[k] = snapIdem{
			Status: entry.Status, Seats: seats, BookingID: entry.BookingID,
			Fingerprint: entry.Fingerprint, LastSeen: entry.LastSeen.UnixNano(),
		}
	}
	e.idem.mu.Unlock()
	p.Idem = idem

	p.Bid = atomic.LoadUint64(&e.bid)
	p.Rid = atomic.LoadUint64(&e.rid)
	return p
}

// ---------- container format ----------

// File layout: magic "KVS1" | version uint32 LE | payload length uint64 LE |
// payload (JSON) | SHA-256 of payload (32 bytes). A snapshot that does not
// match its checksum is never loaded; recovery falls back to full log replay.
func writeSnapshot(dir string, seq int64, p *snapshotPayload) error {
	payload, err := json.Marshal(p)
	if err != nil {
		return err
	}
	buf := make([]byte, 0, len(payload)+48)
	buf = append(buf, snapshotMagic...)
	var num [8]byte
	binary.LittleEndian.PutUint32(num[:4], snapshotVersion)
	buf = append(buf, num[:4]...)
	binary.LittleEndian.PutUint64(num[:], uint64(len(payload)))
	buf = append(buf, num[:]...)
	buf = append(buf, payload...)
	sum := sha256.Sum256(payload)
	buf = append(buf, sum[:]...)

	tmp := filepath.Join(dir, fmt.Sprintf("snapshot-%d.bin.tmp", seq))
	final := filepath.Join(dir, fmt.Sprintf("snapshot-%d.bin", seq))
	f, err := os.Create(tmp)
	if err != nil {
		return err
	}
	w := bufio.NewWriter(f)
	if _, err := w.Write(buf); err != nil {
		f.Close()
		os.Remove(tmp)
		return err
	}
	if err := w.Flush(); err != nil {
		f.Close()
		os.Remove(tmp)
		return err
	}
	if err := f.Sync(); err != nil {
		f.Close()
		os.Remove(tmp)
		return err
	}
	if err := f.Close(); err != nil {
		os.Remove(tmp)
		return err
	}
	if err := os.Rename(tmp, final); err != nil {
		os.Remove(tmp)
		return err
	}
	// fsync the directory so the rename itself is durable
	if d, err := os.Open(dir); err == nil {
		d.Sync()
		d.Close()
	}
	return nil
}

// loadSnapshot returns the newest valid snapshot in dir. An invalid newest
// file is NOT a reason to trust an older one silently in production, but a
// torn write can only affect the newest file (writes are tmp+rename), so
// falling back to full replay is the honest recovery.
func loadSnapshot(dir string) (int64, *snapshotPayload, bool) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return 0, nil, false
	}
	best := int64(-1)
	var bestPath string
	for _, en := range entries {
		name := en.Name()
		if !strings.HasPrefix(name, "snapshot-") || !strings.HasSuffix(name, ".bin") {
			continue
		}
		var seq int64
		if _, err := fmt.Sscanf(strings.TrimSuffix(strings.TrimPrefix(name, "snapshot-"), ".bin"), "%d", &seq); err != nil {
			continue
		}
		if seq > best {
			best, bestPath = seq, filepath.Join(dir, name)
		}
	}
	if best < 0 {
		return 0, nil, false
	}
	raw, err := os.ReadFile(bestPath)
	if err != nil {
		return 0, nil, false
	}
	if len(raw) < 48 || string(raw[:4]) != snapshotMagic {
		return 0, nil, false
	}
	if binary.LittleEndian.Uint32(raw[4:8]) != snapshotVersion {
		return 0, nil, false
	}
	n := binary.LittleEndian.Uint64(raw[8:16])
	if uint64(len(raw)) < 16+n+32 {
		return 0, nil, false
	}
	payload := raw[16 : 16+n]
	want := raw[16+n : 16+n+32]
	got := sha256.Sum256(payload)
	if got != [32]byte(want) {
		return 0, nil, false
	}
	var p snapshotPayload
	if json.Unmarshal(payload, &p) != nil {
		return 0, nil, false
	}
	return best, &p, true
}

// ---------- apply ----------

func applySnapshot(p *snapshotPayload, inv *Inventory, res *ReservationStore,
	wl *Waitlist, idem *IdemStore, bid, rid, wlSeq *uint64) {

	inv.seats = make([]int32, len(p.Seats))
	copy(inv.seats, p.Seats)
	inv.offset = p.SeatOffset
	var sold, held int64
	for _, s := range inv.seats {
		switch s {
		case seatSold:
			sold++
		case seatHeld:
			held++
		}
	}
	atomic.StoreInt64(&inv.sold, sold)
	atomic.StoreInt64(&inv.held, held)

	res.mu.Lock()
	res.m = make(map[string]*Reservation, len(p.Reservations))
	res.idemIndex = make(map[string]string, len(p.ResIdem))
	for _, sr := range p.Reservations {
		r := &Reservation{
			ID: sr.ID, Tenant: sr.Tenant, User: sr.User, Seats: sr.Seats,
			Fingerprint: sr.Fingerprint,
			CreatedAt:   time.Unix(0, sr.CreatedAt), ExpiresAt: time.Unix(0, sr.ExpiresAt),
			BookingID: sr.BookingID,
		}
		atomic.StoreInt32(&r.State, sr.State)
		res.m[r.ID] = r
	}
	for k, id := range p.ResIdem {
		res.idemIndex[k] = id
	}
	res.mu.Unlock()

	wl.mu.Lock()
	wl.order = make([]*WaitEntry, 0, len(p.Waitlist))
	wl.idem = map[string]string{}
	for _, sw := range p.Waitlist {
		wl.order = append(wl.order, &WaitEntry{
			ID: sw.ID, Tenant: sw.Tenant, User: sw.User, Seats: sw.Seats,
			QueuedAt: time.Unix(0, sw.QueuedAt),
		})
	}
	wl.mu.Unlock()

	idem.mu.Lock()
	idem.m = make(map[string]*idemEntry, len(p.Idem))
	for k, si := range p.Idem {
		idem.m[k] = &idemEntry{
			Result:      Result{Status: si.Status, Seats: si.Seats, BookingID: si.BookingID},
			Fingerprint: si.Fingerprint,
			LastSeen:    time.Unix(0, si.LastSeen),
		}
	}
	idem.mu.Unlock()

	if p.Bid > *bid {
		*bid = p.Bid
	}
	if p.Rid > *rid {
		*rid = p.Rid
	}
	if p.WaitlistSeq > *wlSeq {
		*wlSeq = p.WaitlistSeq
	}
}

// highestSnapshotSeq scans dir for the newest snapshot file's sequence, so a
// restarted engine continues the numbering instead of overwriting an older
// snapshot file that segments on disk may still refer to.
func highestSnapshotSeq(dir string) int64 {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return 0
	}
	best := int64(0)
	for _, en := range entries {
		name := en.Name()
		if !strings.HasPrefix(name, "snapshot-") || !strings.HasSuffix(name, ".bin") {
			continue
		}
		var seq int64
		if _, err := fmt.Sscanf(strings.TrimSuffix(strings.TrimPrefix(name, "snapshot-"), ".bin"), "%d", &seq); err != nil {
			continue
		}
		if seq > best {
			best = seq
		}
	}
	return best
}

// ---------- recovery glue ----------

// segmentPaths returns the rotated segments of base with seq >= minSeq
// (oldest first), so recovery replays every segment the snapshot does not
// already cover.
func segmentPaths(base string, minSeq int64) []string {
	dir := filepath.Dir(base)
	prefix := filepath.Base(base) + ".snap-"
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil
	}
	var out []string
	for _, en := range entries {
		name := en.Name()
		if !strings.HasPrefix(name, prefix) {
			continue
		}
		var seq int64
		if _, err := fmt.Sscanf(strings.TrimPrefix(name, prefix), "%d", &seq); err != nil {
			continue
		}
		if seq >= minSeq {
			out = append(out, filepath.Join(dir, name))
		}
	}
	sort.Strings(out)
	return out
}

// recoverState loads the newest snapshot (if any) and replays the tail of
// every log segment after the snapshot's recorded offsets. It returns the
// snapshot sequence used (0 = none) and how many log lines were replayed.
func recoverState(dir, bookBase, holdBase string, inv *Inventory, res *ReservationStore,
	wl *Waitlist, idem *IdemStore, holdTTL time.Duration,
	bid, rid, wlSeq *uint64) (int64, int, int, int, int) {

	seq, snap, ok := loadSnapshot(dir)
	starts := map[string]int64{}
	var paths []string
	if ok {
		applySnapshot(snap, inv, res, wl, idem, bid, rid, wlSeq)
		// The snapshot's offsets refer to the file that was live at capture
		// time. After rotation that content lives in base.snap-<seq>;
		// before rotation (a crash between snapshot and rotate) it is still
		// the live base file. Check which one exists.
		for _, pair := range []struct {
			source string
			base   string
			offset int64
		}{
			{snap.BookSource, bookBase, snap.BookOffset},
			{snap.HoldSource, holdBase, snap.HoldOffset},
		} {
			seg := pair.base + fmt.Sprintf(".snap-%d", seq)
			if fileExists(seg) {
				starts[seg] = pair.offset
			} else {
				starts[pair.base] = pair.offset
			}
		}
		// segments at or above the snapshot's sequence hold only
		// post-snapshot events; anything below is already inside the snapshot
		paths = append(paths, segmentPaths(bookBase, seq)...)
		paths = append(paths, segmentPaths(holdBase, seq)...)
	} else {
		// no usable snapshot: every segment ever written replays from zero
		paths = append(paths, segmentPaths(bookBase, 0)...)
		paths = append(paths, segmentPaths(holdBase, 0)...)
	}
	paths = append(paths, bookBase, holdBase)
	b, r, w, lines := replayLogsPaths(paths, inv, res, wl, idem, holdTTL, bid, rid, wlSeq, starts)
	return seq, b, r, w, lines
}

func fileExists(p string) bool {
	_, err := os.Stat(p)
	return err == nil
}

// pruneSnapshots keeps two generations: the current snapshot and the one
// before it. Older snapshot files and the log segments below them are
// deleted only because a NEWER complete snapshot exists, so deleting them
// can never lose the recovery path.
func pruneSnapshots(dir string, seq int64, bases ...string) {
	keep := seq - 1
	entries, err := os.ReadDir(dir)
	if err != nil {
		return
	}
	for _, en := range entries {
		name := en.Name()
		var s int64
		if strings.HasPrefix(name, "snapshot-") && strings.HasSuffix(name, ".bin") {
			if _, err := fmt.Sscanf(strings.TrimSuffix(strings.TrimPrefix(name, "snapshot-"), ".bin"), "%d", &s); err != nil {
				continue
			}
		} else {
			matched := false
			for _, base := range bases {
				prefix := filepath.Base(base) + ".snap-"
				if strings.HasPrefix(name, prefix) {
					if _, err := fmt.Sscanf(strings.TrimPrefix(name, prefix), "%d", &s); err != nil {
						continue
					}
					matched = true
					break
				}
			}
			if !matched {
				continue
			}
		}
		if s < keep {
			os.Remove(filepath.Join(dir, name))
		}
	}
}

// ---------- the snapshot cycle ----------

// runSnapshot takes one snapshot and rotates both logs. The sequence:
// freeze writers, wait for every submitted log line to be fsynced, copy
// state, unfreeze (requests resume on the old segments), write the snapshot
// to disk, rotate, prune. A failure at any step leaves the logs intact and
// the engine running: the next trigger simply tries again.
func (e *Engine) runSnapshot() error {
	e.snapMu.Lock()
	defer e.snapMu.Unlock()

	e.freeze.Lock()
	e.bookGC.waitQuiesced()
	e.holdGC.waitQuiesced()
	atomic.StoreInt64(&e.eventsSince, 0)
	atomic.StoreInt64(&e.lastSnapAt, time.Now().UnixNano())
	seq := atomic.AddUint64(&e.snapSeq, 1)
	p := e.capture()
	e.freeze.Unlock()

	if err := writeSnapshot(e.snapDir, int64(seq), p); err != nil {
		atomic.AddUint64(&e.snapSeq, ^uint64(0)) // roll the sequence back: this snapshot never happened
		return fmt.Errorf("write snapshot: %w", err)
	}
	suffix := fmt.Sprintf(".snap-%d", seq)
	if err := e.bookGC.rotateTo(suffix); err != nil {
		return fmt.Errorf("rotate bookings: %w", err)
	}
	if err := e.holdGC.rotateTo(suffix); err != nil {
		return fmt.Errorf("rotate holds: %w", err)
	}
	pruneSnapshots(e.snapDir, int64(seq), e.bookGC.path, e.holdGC.path)
	atomic.AddInt64(&e.snapTotal, 1)
	log.Printf("kavach snapshot %d taken: %d seats, %d reservations, %d waitlist entries, %d idem entries; logs rotated",
		seq, len(p.Seats), len(p.Reservations), len(p.Waitlist), len(p.Idem))
	return nil
}
