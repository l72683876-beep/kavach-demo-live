// Kavach: a booking engine built to survive a stampede.
// Core v2.1: adds Postgres write-behind (optional, via -pg-dsn and `go build -tags postgres`):
// the local append-only logs stay the source of truth and a background tailer ships
// every committed line to Postgres with a per-node cursor, so a crash between
// insert and cursor update can never duplicate a row. On top of v2.0: multi-node mode (seat-range sharding: every node owns a
// disjoint slice of the inventory, requests forward around the ring when a
// node's slice runs out, so a cluster scales horizontally with the same
// never-double-sell guarantee and NO Redis dependency) on top of v1.9: restart recovery (the event log replays on boot, so held
// seats, reservations, waitlist positions and id counters survive a crash)
// and built-in chaos injection (random worker stalls, CONTAINED worker
// panics that a single recover() absorbs) on top of v1.8: waitlist mode
// (sold out? join a FIFO queue; freed seats go to the front of the line
// automatically, with optional webhook notifications) on top of v1.7: group commit (concurrent writes share one
// fsync; the persist-before-ack guarantee is unchanged) on top of core v1.6 (Milestone 2):
// the hold/confirm reservation lifecycle
// (reserve with a TTL, confirm, release, an expiry worker that returns
// abandoned holds to inventory atomically) on top of core v1.5:
// bearer API keys, HMAC request signing, replay protection, layered rate
// limits, an in-flight capacity gate, strict input validation, and
// persist-before-ack durable logging, on top of the v1 core: bounded
// admission queue, worker pool, atomic CAS inventory, idempotency keys,
// append-only booking log.
package main

import (
	"context"
	"bufio"
	"bytes"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"math/rand"
	"net"
	"net/http"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/trace"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// ---------- seat inventory (the accuracy core) ----------

const (
	seatFree = int32(0)
	seatHeld = int32(1)
	seatSold = int32(2)
)

type Inventory struct {
	seats  []int32
	offset int // first GLOBAL seat index this slice owns (cluster mode)
	cursor uint64
	sold   int64
	held   int64
}

func NewInventory(n, offset int) *Inventory {
	return &Inventory{seats: make([]int32, n), offset: offset}
}

// tryTake atomically locks n seats, or returns nil when not enough are free.
// A compare-and-swap on each seat means two workers can never hold the same seat.
func (inv *Inventory) tryTake(n int) []int {
	l := len(inv.seats)
	if int(atomic.LoadInt64(&inv.sold))+int(atomic.LoadInt64(&inv.held))+n > l {
		return nil // fast path: everything gone or held
	}
	picked := make([]int, 0, n)
	start := int(atomic.AddUint64(&inv.cursor, uint64(n)) % uint64(l))
	for i := 0; i < l && len(picked) < n; i++ {
		idx := (start + i) % l
		if atomic.CompareAndSwapInt32(&inv.seats[idx], seatFree, seatHeld) {
			picked = append(picked, idx)
		}
	}
	if len(picked) < n { // not enough together: give back what we held
		for _, idx := range picked {
			atomic.CompareAndSwapInt32(&inv.seats[idx], seatHeld, seatFree)
		}
		return nil
	}
	atomic.AddInt64(&inv.held, int64(n))
	out := make([]int, len(picked))
	for i, idx := range picked {
		out[i] = idx + inv.offset // callers speak in GLOBAL seat indices
	}
	return out
}

// commit moves held seats to sold (the confirm step). Seats arrive as
// GLOBAL indices; the offset maps them onto this node's slice.
func (inv *Inventory) commit(seats []int) {
	for _, s := range seats {
		atomic.StoreInt32(&inv.seats[s-inv.offset], seatSold)
	}
	atomic.AddInt64(&inv.sold, int64(len(seats)))
	atomic.AddInt64(&inv.held, -int64(len(seats)))
}

// release returns held seats to the free pool (cancel or expiry).
func (inv *Inventory) release(seats []int) {
	for _, s := range seats {
		atomic.CompareAndSwapInt32(&inv.seats[s-inv.offset], seatHeld, seatFree)
	}
	atomic.AddInt64(&inv.held, -int64(len(seats)))
}

// markSold/markHeld/freeHeld apply global seat indices outside the normal
// take/commit path; restart recovery replays through them.
func (inv *Inventory) markSold(seats []int) {
	fromHeld := int64(0)
	applied := int64(0)
	for _, s := range seats {
		if s-inv.offset < 0 || s-inv.offset >= len(inv.seats) {
			continue // replaying a log from a bigger inventory: nothing to mark
		}
		applied++ // only seats that actually landed on this slice count, so a
		// follower replaying a leader's other-slice bookings does not inflate
		// its sold counter (the cluster view aggregates per-node counters)
		i := s - inv.offset
		if atomic.SwapInt32(&inv.seats[i], seatSold) == seatHeld {
			fromHeld++ // this seat was replayed as a hold first: stop counting it held
		}
	}
	atomic.AddInt64(&inv.sold, applied)
	atomic.AddInt64(&inv.held, -fromHeld)
}

func (inv *Inventory) markHeld(seats []int) {
	n := int64(0)
	for _, s := range seats {
		if s-inv.offset < 0 || s-inv.offset >= len(inv.seats) {
			continue // replaying a log from a bigger inventory: nothing to mark
		}
		if atomic.CompareAndSwapInt32(&inv.seats[s-inv.offset], seatFree, seatHeld) {
			n++
		}
	}
	atomic.AddInt64(&inv.held, n)
}

func (inv *Inventory) freeHeld(seats []int) {
	n := int64(0)
	for _, s := range seats {
		if s-inv.offset < 0 || s-inv.offset >= len(inv.seats) {
			continue // replaying a log from a bigger inventory: nothing to mark
		}
		if atomic.CompareAndSwapInt32(&inv.seats[s-inv.offset], seatHeld, seatFree) {
			n++
		}
	}
	atomic.AddInt64(&inv.held, -n)
}

func (inv *Inventory) stats() (total, sold, held int) {
	return len(inv.seats), int(atomic.LoadInt64(&inv.sold)), int(atomic.LoadInt64(&inv.held))
}


// peerSliceSeats asks a peer how many seats its own slice holds, retrying
// briefly because a cluster's nodes may still be booting. Cluster-mode peers
// answer /seats with a per-node map keyed by their addr; standalone engines
// answer with a flat total.
func peerSliceSeats(peer string) (int, error) {
	client := &http.Client{Timeout: 3 * time.Second}
	var lastErr error
	for attempt := 0; attempt < 20; attempt++ {
		resp, err := client.Get("http://" + peer + "/seats")
		if err != nil {
			lastErr = err
			time.Sleep(500 * time.Millisecond)
			continue
		}
		var body map[string]any
		err = json.NewDecoder(resp.Body).Decode(&body)
		resp.Body.Close()
		if err != nil {
			lastErr = err
			time.Sleep(500 * time.Millisecond)
			continue
		}
		if nodes, ok := body["nodes"].(map[string]any); ok {
			if m, ok := nodes[peer].(map[string]any); ok {
				if t, ok := m["total"].(float64); ok {
					return int(t), nil
				}
			}
			return 0, fmt.Errorf("/seats has no node entry for %s", peer)
		}
		if t, ok := body["total"].(float64); ok {
			return int(t), nil
		}
		return 0, fmt.Errorf("/seats missing total")
	}
	return 0, lastErr
}

// ---------- rate limiter (the anti-bot gate) ----------

type bucket struct {
	tokens float64
	last   time.Time
}

type Limiter struct {
	mu     sync.Mutex
	rate   float64 // tokens per second
	burst  float64
	bucket map[string]*bucket
}

func NewLimiter(rate, burst float64) *Limiter {
	return &Limiter{rate: rate, burst: burst, bucket: make(map[string]*bucket)}
}

func (l *Limiter) Allow(key string) bool {
	if l.rate <= 0 { // 0 disables this layer
		return true
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	now := time.Now()
	b, ok := l.bucket[key]
	if !ok {
		if len(l.bucket) >= 1000000 { // bound memory; recycle an old bucket
			for k := range l.bucket {
				delete(l.bucket, k)
				break
			}
		}
		b = &bucket{tokens: l.burst, last: now}
		l.bucket[key] = b
	}
	b.tokens += now.Sub(b.last).Seconds() * l.rate
	if b.tokens > l.burst {
		b.tokens = l.burst
	}
	b.last = now
	if b.tokens >= 1 {
		b.tokens--
		return true
	}
	return false
}

// ---------- authentication: bearer keys, HMAC signing, replay protection ----------

func newAuthWithTenantSecrets(keySpec, secret, tenantSecretSpec string) *Auth {
	a := NewAuth(keySpec, secret)
	if tenantSecretSpec != "" {
		a.setTenantSecrets(tenantSecretSpec)
	}
	return a
}

type Auth struct {
	mu      sync.Mutex
	enabled bool
	keys    map[string]string // bearer key -> tenant
	secret  []byte            // when non-empty, HMAC signing is required
	secrets map[string][]byte // optional per-tenant HMAC secrets (tenant -> secret)
	window  int64             // timestamp tolerance in seconds
	nonces  map[string]int64  // request id -> expiry unix
}

func NewAuth(keySpec, secret string) *Auth {
	a := &Auth{keys: map[string]string{}, nonces: map[string]int64{}, window: 300}
	if keySpec != "" {
		a.enabled = true
		for _, kv := range strings.Split(keySpec, ",") {
			parts := strings.SplitN(kv, ":", 2)
			if len(parts) == 2 {
				a.keys[parts[0]] = parts[1]
			} else if parts[0] != "" {
				a.keys[parts[0]] = parts[0] // key doubles as tenant name
			}
		}
	}
	if secret != "" {
		a.secret = []byte(secret)
	}
	return a
}

// setTenantSecrets installs per-tenant HMAC secrets from a "tenant:secret,..."
// spec. When set, each request is verified against ITS tenant's secret and a
// tenant without an entry is rejected; a signature computed under one tenant's
// secret can no longer be replayed under another tenant's key. The shared
// -hmac-secret remains the fallback only when no per-tenant secrets are set.
func (a *Auth) setTenantSecrets(spec string) {
	a.secrets = map[string][]byte{}
	for _, kv := range strings.Split(spec, ",") {
		parts := strings.SplitN(kv, ":", 2)
		if len(parts) == 2 && parts[0] != "" {
			a.secrets[parts[0]] = []byte(parts[1])
		}
	}
	if len(a.secrets) == 0 {
		a.secrets = nil
	}
}

// verify authenticates the request. body is the raw request body (may be nil
// for GET). Returns the tenant, or a non-zero HTTP status on rejection.
func (a *Auth) verify(r *http.Request, body []byte) (string, int, string) {
	if !a.enabled {
		return "local", 0, ""
	}
	key := r.Header.Get("Authorization")
	key = strings.TrimPrefix(key, "Bearer ")
	tenant, ok := a.keys[key]
	if !ok {
		return "", http.StatusUnauthorized, "unauthorized"
	}
	var secret []byte
	if len(a.secrets) > 0 {
		secret = a.secrets[tenant]
		if secret == nil {
			return "", http.StatusUnauthorized, "unauthorized"
		}
	} else if a.secret != nil {
		secret = a.secret
	}
	if secret == nil { // bearer-only mode: keys are the whole story
		return tenant, 0, ""
	}
	ts := r.Header.Get("X-Kavach-Timestamp")
	rid := r.Header.Get("X-Kavach-Request-Id")
	sig := r.Header.Get("X-Kavach-Signature")
	if ts == "" || rid == "" || sig == "" {
		return "", http.StatusUnauthorized, "unauthorized"
	}
	tsVal, err := strconv.ParseInt(ts, 10, 64)
	now := time.Now().Unix()
	if err != nil || tsVal < now-a.window || tsVal > now+a.window {
		return "", http.StatusUnauthorized, "unauthorized"
	}
	a.mu.Lock()
	if exp, seen := a.nonces[rid]; seen && exp > now {
		a.mu.Unlock()
		return "", http.StatusUnauthorized, "replay"
	}
	a.nonces[rid] = now + a.window
	a.mu.Unlock()
	mac := hmac.New(sha256.New, secret)
	// The MAC covers the timestamp, request id, the METHOD, the REQUEST URI,
	// the tenant's api key, and the body: a captured signature is bound to
	// exactly one request AND one key, so it cannot be replayed against a
	// different path or reused under another tenant's key (closes the shared-
	// secret cross-tenant replay gap; SDKs updated in lockstep 2026-10-18).
	mac.Write([]byte(ts + "\n" + rid + "\n"))
	mac.Write([]byte(r.Method + "\n" + r.URL.RequestURI() + "\n" + key + "\n"))
	mac.Write(body)
	want, err := hex.DecodeString(sig)
	if err != nil || !hmac.Equal(mac.Sum(nil), want) {
		return "", http.StatusUnauthorized, "unauthorized"
	}
	return tenant, 0, ""
}

func (a *Auth) pruneNonces() {
	now := time.Now().Unix()
	a.mu.Lock()
	defer a.mu.Unlock()
	for k, exp := range a.nonces {
		if exp < now {
			delete(a.nonces, k)
		}
	}
}

// ---------- idempotency (a retry never double-books) ----------

type Result struct {
	Status    string `json:"status"`
	Seats     []int  `json:"seats,omitempty"`
	BookingID string `json:"booking_id,omitempty"`
}

type idemEntry struct {
	Result
	Fingerprint string
	Tenant      string
	LastSeen    time.Time
}

type IdemStore struct {
	mu sync.Mutex
	m  map[string]*idemEntry // key = tenant + "|" + idempotency key
}

func NewIdemStore() *IdemStore { return &IdemStore{m: make(map[string]*idemEntry)} }

func (s *IdemStore) lookup(tenant, key string) (*idemEntry, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	e, ok := s.m[tenant+"|"+key]
	return e, ok
}

// claim registers a new operation. Returns (existing entry, true) when the
// key is already known, (nil, false) when this call won the race. A known key
// with a different fingerprint is a conflict (fingerprint != "").
func (s *IdemStore) claim(tenant, key, fp string) (*idemEntry, bool, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	k := tenant + "|" + key
	if e, ok := s.m[k]; ok {
		e.LastSeen = time.Now()
		if fp != "" && e.Fingerprint != "" && e.Fingerprint != fp {
			return e, true, true // conflict: same key, different request
		}
		return e, true, false
	}
	s.m[k] = &idemEntry{Fingerprint: fp, Tenant: tenant, LastSeen: time.Now(),
		Result: Result{Status: "processing"}}
	return nil, false, false
}

func (s *IdemStore) finish(tenant, key string, r Result) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if e, ok := s.m[tenant+"|"+key]; ok {
		e.Result = r
		e.LastSeen = time.Now()
	}
}

// trim evicts only entries idle beyond minAge, never an in-flight one.
func (s *IdemStore) trim(max int, minAge time.Duration) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.m) <= max {
		return
	}
	cutoff := time.Now().Add(-minAge)
	for k, e := range s.m {
		if len(s.m) <= max {
			break
		}
		if e.Status != "processing" && e.LastSeen.Before(cutoff) {
			delete(s.m, k)
		}
	}
}

// ---------- reservations: the hold/confirm lifecycle ----------

const (
	resHeld      = int32(1)
	resConfirmed = int32(2)
	resExpired   = int32(3)
	resReleased  = int32(4)
)

func resStateName(s int32) string {
	switch s {
	case resHeld:
		return "held"
	case resConfirmed:
		return "confirmed"
	case resExpired:
		return "expired"
	case resReleased:
		return "released"
	}
	return "unknown"
}

type Reservation struct {
	ID          string
	Tenant      string
	User        string
	Seats       []int
	State       int32 // atomic; CAS gives single-winner transitions
	Fingerprint string
	CreatedAt   time.Time
	ExpiresAt   time.Time
	BookingID   string
}

// transition returns true when this caller won the state change.
func (r *Reservation) transition(from, to int32) bool {
	return atomic.CompareAndSwapInt32(&r.State, from, to)
}

type ReservationStore struct {
	mu        sync.Mutex
	m         map[string]*Reservation
	idemIndex map[string]string // tenant|key -> reservation id
}

func NewReservationStore() *ReservationStore {
	return &ReservationStore{m: map[string]*Reservation{}, idemIndex: map[string]string{}}
}

func (s *ReservationStore) get(id string) (*Reservation, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	r, ok := s.m[id]
	return r, ok
}

func (s *ReservationStore) byIdem(tenant, key string) (*Reservation, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	id, ok := s.idemIndex[tenant+"|"+key]
	if !ok {
		return nil, false
	}
	r, ok := s.m[id]
	return r, ok
}

// put registers a reservation and its idempotency alias. Returns false when
// the idempotency key was already claimed (the caller then re-reads byIdem).
func (s *ReservationStore) put(r *Reservation, idemKey string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if idemKey != "" {
		ik := r.Tenant + "|" + idemKey
		if _, exists := s.idemIndex[ik]; exists {
			return false
		}
		s.idemIndex[ik] = r.ID
	}
	s.m[r.ID] = r
	return true
}

// expire candidates collects held reservations past their deadline; the
// caller then transitions each one (outside the lock, single-winner CAS).
func (s *ReservationStore) expired(now time.Time) []*Reservation {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []*Reservation
	for _, r := range s.m {
		if atomic.LoadInt32(&r.State) == resHeld && now.After(r.ExpiresAt) {
			out = append(out, r)
		}
	}
	return out
}

// ---------- waitlist (sold out? join the queue) ----------

// WaitEntry is one place in line. Entries are strictly FIFO: when seats free
// up, the front of the line is served first; a front entry that needs more
// seats than are free blocks the ones behind it (fairness over utilization).
type WaitEntry struct {
	ID       string
	Tenant   string
	User     string
	Seats    int
	Event    string // the event the join named; a different event on the same key is a conflict
	pos      int // filled in by listFor
	QueuedAt time.Time
}

var waitlistSeq uint64

type Waitlist struct {
	mu    sync.Mutex
	order []*WaitEntry
	idem  map[string]string // tenant|key -> waitlist id
}

func NewWaitlist() *Waitlist { return &Waitlist{idem: map[string]string{}} }

// byIdem returns the live entry for an idempotency key, if it is still queued.
func (w *Waitlist) byIdem(tenant, key string) (*WaitEntry, bool) {
	w.mu.Lock()
	defer w.mu.Unlock()
	id, ok := w.idem[tenant+"|"+key]
	if !ok {
		return nil, false
	}
	for _, e := range w.order {
		if e.ID == id {
			return e, true
		}
	}
	return nil, false // fulfilled or cancelled: the key is free again
}

// join appends to the line. Returns (nil) when the waitlist is full.
func (w *Waitlist) join(tenant, key, user string, seats int, cap int, ev string) *WaitEntry {
	w.mu.Lock()
	defer w.mu.Unlock()
	if len(w.order) >= cap {
		return nil
	}
	if ev == "" {
		ev = defaultEventID
	}
	e := &WaitEntry{ID: fmt.Sprintf("WL%08d", atomic.AddUint64(&waitlistSeq, 1)), Tenant: tenant,
		User: user, Seats: seats, Event: ev, QueuedAt: time.Now()}
	w.order = append(w.order, e)
	if key != "" {
		w.idem[tenant+"|"+key] = e.ID
	}
	return e
}

// cancel removes an entry from the line. Tenant-scoped.
func (w *Waitlist) cancel(id, tenant string) (*WaitEntry, bool) {
	w.mu.Lock()
	defer w.mu.Unlock()
	for i, e := range w.order {
		if e.ID == id && e.Tenant == tenant {
			w.order = append(w.order[:i], w.order[i+1:]...)
			return e, true
		}
	}
	return nil, false
}

// remove drops an entry by id regardless of tenant (replay uses it).
func (w *Waitlist) remove(id string) bool {
	w.mu.Lock()
	defer w.mu.Unlock()
	for i, e := range w.order {
		if e.ID == id {
			w.order = append(w.order[:i], w.order[i+1:]...)
			return true
		}
	}
	return false
}

// listFor returns the caller's live entries with their 1-based positions.
func (w *Waitlist) listFor(tenant, user string) []*WaitEntry {
	w.mu.Lock()
	defer w.mu.Unlock()
	var out []*WaitEntry
	for pos, e := range w.order {
		if e.Tenant == tenant && e.User == user {
			e.pos = pos + 1
			out = append(out, e)
		}
	}
	return out
}

// popReady serves the front of the line while the inventory can satisfy it.
// The seats are taken atomically; the caller owns them from that moment.
func (w *Waitlist) popReady(inv *Inventory) (*WaitEntry, []int, bool) {
	w.mu.Lock()
	defer w.mu.Unlock()
	for len(w.order) > 0 {
		front := w.order[0]
		seats := inv.tryTake(front.Seats)
		if seats == nil {
			return nil, nil, false // front blocked: FIFO discipline
		}
		w.order = w.order[1:]
		return front, seats, true
	}
	return nil, nil, false
}

// ---------- latency sampling ----------

type LatencyRing struct {
	buf []int64 // microseconds
	n   uint64
}

func NewLatencyRing(size int) *LatencyRing { return &LatencyRing{buf: make([]int64, size)} }

func (r *LatencyRing) add(us int64) {
	i := atomic.AddUint64(&r.n, 1) - 1
	r.buf[i%uint64(len(r.buf))] = us
}

func (r *LatencyRing) snapshot() (count int, p50, p95, p99 float64) {
	total := atomic.LoadUint64(&r.n)
	if total == 0 {
		return 0, 0, 0, 0
	}
	valid := int(total)
	if valid > len(r.buf) {
		valid = len(r.buf)
	}
	sample := make([]int64, valid)
	copy(sample, r.buf[:valid])
	sort.Slice(sample, func(i, j int) bool { return sample[i] < sample[j] })
	pick := func(p float64) float64 {
		return float64(sample[int(p*float64(valid-1))]) / 1000.0 // ms
	}
	return valid, pick(0.50), pick(0.95), pick(0.99)
}

// ---------- group commit: one fsync per batch of writes ----------

// GroupCommit turns per-write fsyncs into per-batch fsyncs. Concurrent
// writers hand their line to the batcher and block until a single
// flush+fsync covering the whole batch completes. The durability guarantee
// is unchanged: nothing is acknowledged before it is on disk.
type commitReq struct {
	line   string
	done   chan struct{}
	rotate string // non-empty: rotate the live log to this segment suffix
	errCh  chan error
}

type GroupCommit struct {
	ch   chan commitReq
	file *os.File
	w    *bufio.Writer
	path string

	// progress counters: submitted lines vs fsynced lines/bytes. The
	// snapshotter uses them to find the exact durable byte offset its
	// snapshot covers, and to wait until every submitted line is on disk.
	submitted    int64
	flushedLines int64
	flushPos     int64
}

func NewGroupCommit(path string, queue int) (*GroupCommit, error) {
	// append mode: the log survives restarts (recovery replays it), so a
	// boot must never truncate it
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		return nil, err
	}
	g := &GroupCommit{ch: make(chan commitReq, queue), file: f, w: bufio.NewWriterSize(f, 1<<20), path: path}
	go g.run()
	return g, nil
}

// run owns the file: it takes the first pending write, drains everything
// already queued behind it, writes the whole batch, syncs ONCE, and releases
// every writer in the batch.
func (g *GroupCommit) run() {
	for first := range g.ch {
		if first.rotate != "" { // rotation serializes behind pending writes
			if first.errCh != nil {
				first.errCh <- g.doRotate(first.rotate)
			}
			continue
		}
		var rot []commitReq
		batch := []commitReq{first}
	drain:
		for len(batch) < 4096 {
			select {
			case r := <-g.ch:
				if r.rotate != "" { // a rotation is its own fsync boundary
					rot = append(rot, r)
					continue
				}
				batch = append(batch, r)
			default:
				break drain
			}
		}
		for _, r := range batch {
			g.w.WriteString(r.line)
			g.w.WriteByte('\n')
		}
		g.w.Flush()
		g.file.Sync()
		var n int64
		for _, r := range batch {
			n += int64(len(r.line) + 1)
		}
		atomic.AddInt64(&g.flushedLines, int64(len(batch)))
		atomic.AddInt64(&g.flushPos, n)
		for _, r := range batch {
			close(r.done)
		}
		// rotations queued behind this batch run now: every line submitted
		// before them is fsynced, everything after lands in the new segment
		for _, r := range rot {
			if r.errCh != nil {
				r.errCh <- g.doRotate(r.rotate)
			}
		}
	}
}

// commit blocks until the batch containing line has been flushed and fsynced.
func (g *GroupCommit) commit(line string) {
	atomic.AddInt64(&g.submitted, 1)
	r := commitReq{line: line, done: make(chan struct{})}
	g.ch <- r
	<-r.done
}

// flushedBytes is the durable byte position of the log: every byte before it
// is fsynced. A snapshot taken under the engine freeze lock records this
// value per log; everything at or before it is inside the snapshot.
func (g *GroupCommit) flushedBytes() int64 { return atomic.LoadInt64(&g.flushPos) }

// waitQuiesced blocks until every submitted line has been fsynced. Called
// under the engine freeze lock, so no new lines can appear while waiting.
func (g *GroupCommit) waitQuiesced() {
	for atomic.LoadInt64(&g.submitted) != atomic.LoadInt64(&g.flushedLines) {
		time.Sleep(2 * time.Millisecond)
	}
}

// rotateTo moves the live log to <path><suffix> and opens a fresh one. It is
// executed on the committer goroutine, so it serializes behind every pending
// write: an event either lands in the old segment (beyond the snapshot
// offset, replayed from the tail) or in the new one. Never both, never lost.
func (g *GroupCommit) rotateTo(suffix string) error {
	done := make(chan error, 1)
	g.ch <- commitReq{rotate: suffix, errCh: done}
	return <-done
}

func (g *GroupCommit) reopen() error {
	f, err := os.OpenFile(g.path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		return err
	}
	g.file = f
	g.w = bufio.NewWriterSize(f, 1<<20)
	return nil
}

func (g *GroupCommit) doRotate(suffix string) error {
	if err := g.w.Flush(); err != nil {
		return err
	}
	if err := g.file.Sync(); err != nil {
		return err
	}
	if err := g.file.Close(); err != nil {
		return err
	}
	seg := g.path + suffix
	if err := os.Rename(g.path, seg); err != nil {
		// the rename failed before any data moved: keep serving on the old file
		if rb := g.reopen(); rb != nil {
			return fmt.Errorf("rotate rename failed (%v) AND reopen failed (%v)", err, rb)
		}
		return err
	}
	if err := g.reopen(); err != nil {
		// roll the segment back so the live path exists again
		if rb := os.Rename(seg, g.path); rb == nil {
			if rb2 := g.reopen(); rb2 == nil {
				return fmt.Errorf("rotate reopened old log (%v); new log unavailable (%v)", rb2, err)
			}
		}
		return err
	}
	if d, err := os.Open(filepath.Dir(g.path)); err == nil {
		d.Sync()
		d.Close()
	}
	// the live file restarted, so its durable byte position restarts with
	// it: the snapshot offsets recorded from here on are offsets INTO the
	// live file, which is exactly how recovery applies them (a cumulative
	// counter here would seek past the end of the rotated segment and
	// silently drop every post-capture line it held).
	atomic.StoreInt64(&g.flushPos, 0)
	return nil
}

// ---------- restart recovery: the event log is the source of truth ----------

// replayLogs rebuilds live state from the append-only logs: sold and held
// seats, every reservation with its final state, live waitlist entries, and
// the id counters. Events apply in timestamp order, so the state after a
// crash is exactly the state before it. Returns (bookings, reservations,
// waitlist entries) restored. Logs written before v1.9 carry no expiry;
// those holds get a fresh TTL on recovery.
func replayLogs(bookPath, holdPath string, inv *Inventory, res *ReservationStore,
	wl *Waitlist, idem *IdemStore, holdTTL time.Duration,
	bid, rid, waitlistSeq *uint64) (int, int, int) {
	b, r, wq, _ := replayLogsPaths([]string{bookPath, holdPath}, inv, res, wl, idem,
		holdTTL, bid, rid, waitlistSeq, nil)
	return b, r, wq
}

// replayLogsPaths replays one or more log files (the live logs plus any
// rotated segments) in one merged, timestamp-ordered pass. starts maps a
// file path to the byte offset recovery must seek to: everything before it
// is already inside a snapshot, so replaying it would double-apply state.
func replayLogsPaths(paths []string, inv *Inventory, res *ReservationStore,
	wl *Waitlist, idem *IdemStore, holdTTL time.Duration,
	bid, rid, waitlistSeq *uint64, starts map[string]int64) (int, int, int, int) {

	var events []replayEvent
	lines := 0
	readLog := func(path string) {
		f, err := os.Open(path)
		if err != nil {
			return // no log yet: a first boot
		}
		defer f.Close()
		if off := starts[path]; off > 0 {
			if _, err := f.Seek(off, io.SeekStart); err != nil {
				log.Printf("kavach recovery: seek to %d in %s failed (%v); replaying from zero", off, path, err)
			}
		}
		sc := bufio.NewScanner(f)
		sc.Buffer(make([]byte, 1<<20), 1<<20)
		for sc.Scan() {
			lines++
			if ev, ok := parseReplayEvent(sc.Bytes()); ok {
				events = append(events, ev)
			}
		}
	}
	for _, p := range paths {
		readLog(p)
	}
	sort.Slice(events, func(i, j int) bool { return events[i].ts < events[j].ts })

	bookings, reservations, queued := applyReplayEvents(events, inv, res, wl, idem,
		holdTTL, bid, rid, waitlistSeq)
	return bookings, reservations, queued, lines
}

// ---------- the engine ----------

type job struct {
	key    string
	tenant string
	user   string
	fp     string
	seats  int
	resp   chan Result
}

type Engine struct {
	inv      *Inventory
	userLim  *Limiter
	ipLim    *Limiter
	auth     *Auth
	idem     *IdemStore
	res      *ReservationStore
	queue    chan *job
	bookGC *GroupCommit
	holdGC *GroupCommit
	bid      uint64
	rid      uint64
	inFlight int64
	holdTTL    time.Duration
	wl         *Waitlist
	waitCap    int
	webhookURL string
	webhookCh  chan map[string]interface{}
	chaosStallMs int
	tracer        trace.Tracer // OpenTelemetry spans per request (nil = tracing off)
	chaosProb    float64
	chaosPanic   float64

	// replication (cluster phase 1): a follower tails the leader's WAL and
	// applies it through the shared replay path; a leader serves it.
	replKey      string
	replSecret   []byte
	replFrom     string
	replStreams  int64
	replicatedLines int64
	replConnected   int32
	replPromoted    int32 // 1 = this follower was promoted and now serves writes
	replBookPos  int64 // local WAL bytes already applied (bookings)
	replHoldPos  int64 // local WAL bytes already applied (holds)
	replNonces   map[string]int64
	replNonceMu  sync.Mutex

	// snapshot machinery: freeze gates every state mutation, so the
	// snapshotter can copy exact instant state without racing writers.
	freeze          sync.RWMutex
	snapEvery       int
	snapInterval    time.Duration
	snapDir         string
	snapMu          sync.Mutex
	snapSeq         uint64
	snapTotal       int64
	lastSnapAt      int64
	eventsSince     int64
	recoveryLines   int64
	peers        []string
	selfIdx      int
	httpc        *http.Client
	pgShipped    int64
	pgFailed     int64

	received     int64
	ipLimited    int64
	userLimited  int64
	overloaded   int64
	authFailures int64
	replays      int64
	conflicts    int64
	confirmed    int64
	soldOut      int64
	errors       int64
	holdsCreated int64
	holdsConfirm int64
	holdsRelease int64
	holdsExpired int64
	waitQueued   int64
	waitFulfilled int64
	waitCancelled int64
	webhookSent   int64
	webhookFailed int64
	chaosStalls   int64
	chaosPanics   int64
	forwarded     int64
	lat          *LatencyRing
	started      time.Time
}

// appendLog writes, flushes, and fsyncs the booking BEFORE the caller may
// acknowledge it: persist-before-ack. A confirmed response means the record
// is on disk.
func (e *Engine) appendLog(id, key, tenant, user string, seats []int) {
	line, _ := json.Marshal(map[string]interface{}{
		"booking_id": id, "key": key, "tenant": tenant, "user": user, "seats": seats, "ts": time.Now().UnixNano(),
	})
	atomic.AddInt64(&e.eventsSince, 1)
	e.bookGC.commit(string(line))
}

// appendHold records a reservation lifecycle event (reserved/confirmed/
// released/expired) with the same persist-before-ack discipline.
func (e *Engine) appendHold(event string, r *Reservation, bookingID string) {
	line, _ := json.Marshal(map[string]interface{}{
		"event": event, "reservation_id": r.ID, "tenant": r.Tenant, "user": r.User,
		"seats": r.Seats, "booking_id": bookingID, "expires_at": r.ExpiresAt.UnixNano(), "ts": time.Now().UnixNano(),
	})
	atomic.AddInt64(&e.eventsSince, 1)
	e.holdGC.commit(string(line))
}

func (e *Engine) process(j *job) {
	if prev, ok := e.idem.lookup(j.tenant, j.key); ok {
		// same key: same payload returns the stored result, a different
		// payload is a conflict, either way never a second booking
		if prev.Fingerprint != "" && j.fp != "" && prev.Fingerprint != j.fp {
			atomic.AddInt64(&e.conflicts, 1)
			j.resp <- Result{Status: "idempotency_conflict"}
			return
		}
		j.resp <- prev.Result
		return
	}
	existing, claimed, conflict := e.idem.claim(j.tenant, j.key, j.fp)
	if conflict {
		atomic.AddInt64(&e.conflicts, 1)
		j.resp <- Result{Status: "idempotency_conflict"}
		return
	}
	if claimed {
		j.resp <- existing.Result // stored result or "processing"
		return
	}
	seats := e.inv.tryTake(j.seats)
	if seats == nil {
		e.idem.finish(j.tenant, j.key, Result{Status: "sold_out"})
		atomic.AddInt64(&e.soldOut, 1)
		j.resp <- Result{Status: "sold_out"}
		return
	}
	id := fmt.Sprintf("BK%08d", atomic.AddUint64(&e.bid, 1))
	e.appendLog(id, j.key, j.tenant, j.user, seats) // durable BEFORE the ack leaves the process
	e.inv.commit(seats)
	atomic.AddInt64(&e.confirmed, 1)
	res := Result{Status: "confirmed", Seats: seats, BookingID: id}
	e.idem.finish(j.tenant, j.key, res)
	j.resp <- res
}

// worker pulls from the admission queue. Chaos injection lives here: a
// random stall (a slow downstream dependency) or a synthetic panic, always
// contained by one recover() so a panicking worker can never take the
// engine down. The job answers "error"; the process lives on.
func (e *Engine) worker() {
	for j := range e.queue {
		func() {
			defer func() {
				if r := recover(); r != nil {
					atomic.AddInt64(&e.chaosPanics, 1)
					j.resp <- Result{Status: "error"}
				}
			}()
			if e.chaosProb > 0 && rand.Float64() < e.chaosProb {
				atomic.AddInt64(&e.chaosStalls, 1)
				time.Sleep(time.Duration(rand.Intn(e.chaosStallMs+1)) * time.Millisecond)
			}
			if e.chaosPanic > 0 && rand.Float64() < e.chaosPanic {
				panic("kavach chaos injection")
			}
			e.freeze.RLock()
			e.process(j)
			e.freeze.RUnlock()
		}()
	}
}

// expireSweep returns abandoned holds to the free pool. Each reservation
// changes state by CAS, so a confirm racing an expiry has exactly one winner.
func (e *Engine) expireSweep() {
	e.freeze.RLock()
	defer e.freeze.RUnlock()
	now := time.Now()
	for _, r := range e.res.expired(now) {
		if r.transition(resHeld, resExpired) {
			e.inv.release(r.Seats)
			atomic.AddInt64(&e.holdsExpired, 1)
			e.appendHold("expired", r, "")
		}
	}
	e.serveWaitlist()
}

// notifyWebhook posts a fire-and-forget JSON event to the integrator's URL
// if one is configured. It never blocks the request path: full or failing
// deliveries are counted, never propagated.
func (e *Engine) notifyWebhook(ev map[string]interface{}) {
	if e.webhookURL == "" {
		return
	}
	select {
	case e.webhookCh <- ev:
	default:
		atomic.AddInt64(&e.webhookFailed, 1)
	}
}

// serveWaitlist serves the front of the line while seats are free. Called
// after every event that returns seats to the pool (expiry or release).
func (e *Engine) serveWaitlist() {
	for {
		entry, seats, ok := e.wl.popReady(e.inv)
		if !ok {
			return
		}
		res := &Reservation{
			ID:        fmt.Sprintf("RSV%08d", atomic.AddUint64(&e.rid, 1)),
			Tenant:    entry.Tenant,
			User:      entry.User,
			Seats:     seats,
			CreatedAt: time.Now(),
			ExpiresAt: time.Now().Add(e.holdTTL),
			State:     resHeld,
		}
		e.res.put(res, "") // no idempotency alias: the engine holds on their behalf
		atomic.AddInt64(&e.holdsCreated, 1)
		atomic.AddInt64(&e.waitFulfilled, 1)
		e.appendHold("reserved", res, "")
		e.appendWaitEvent("waitlist_fulfilled", entry)
		e.notifyWebhook(map[string]interface{}{
			"event":           "waitlist_fulfilled",
			"waitlist_id":     entry.ID,
			"reservation_id":  res.ID,
			"tenant":          entry.Tenant,
			"user_id":         entry.User,
			"seats":           seats,
			"hold_expires_at": res.ExpiresAt.Unix(),
		})
	}
}

func (e *Engine) handleWaitlistJoin(w http.ResponseWriter, r *http.Request) {
	if e.followerReject(w) {
		return
	}
	var req struct {
		User    string `json:"user_id"`
		IdemKey string `json:"idempotency_key"`
		Seats   int    `json:"seats"`
		EventID string `json:"event_id"`
	}
	tenant, _, ok := e.precheck(w, r, &req)
	if !ok {
		return
	}
	defer atomic.AddInt64(&e.inFlight, -1)
	if req.User == "" || req.IdemKey == "" || req.Seats < 1 || req.Seats > 4 {
		atomic.AddInt64(&e.errors, 1)
		writeJSON(w, http.StatusBadRequest, Result{Status: "bad_request"})
		return
	}
	if !e.userLim.Allow(tenant + ":" + req.User) {
		atomic.AddInt64(&e.userLimited, 1)
		w.Header().Set("Retry-After", "2")
		writeJSON(w, http.StatusTooManyRequests, Result{Status: "rate_limited"})
		return
	}
	ev := eventOf(r, req.EventID)
	if ev == "" {
		ev = defaultEventID
	}
	if prev, found := e.wl.byIdem(tenant, req.IdemKey); found { // idempotent join
		if prev.Seats != req.Seats || prev.Event != ev { // same key, different payload: conflict
			atomic.AddInt64(&e.conflicts, 1)
			writeJSON(w, http.StatusConflict, map[string]interface{}{"status": "idempotency_conflict"})
			return
		}
		e.writeWaitEntry(w, http.StatusOK, prev)
		return
	}
	// the waitlist exists for sold-out moments: with seats free, reserving
	// directly is both faster and fairer
	total, sold, held := e.inv.stats()
	if total-sold-held >= req.Seats {
		writeJSON(w, http.StatusConflict, map[string]interface{}{"status": "seats_available"})
		return
	}
	entry := e.wl.join(tenant, req.IdemKey, req.User, req.Seats, e.waitCap, ev)
	if entry == nil {
		writeJSON(w, http.StatusConflict, map[string]interface{}{"status": "waitlist_full"})
		return
	}
	atomic.AddInt64(&e.waitQueued, 1)
	e.appendWaitEvent("waitlisted", entry) // durable BEFORE the ack
	e.writeWaitEntry(w, http.StatusOK, entry)
}

// handleWaitlistList: the caller's live entries with positions.
func (e *Engine) handleWaitlistList(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	tenant, st, msg := e.auth.verify(r, nil)
	if st != 0 {
		writeJSON(w, st, Result{Status: msg})
		return
	}
	user := r.URL.Query().Get("user_id")
	if user == "" {
		writeJSON(w, http.StatusBadRequest, Result{Status: "bad_request"})
		return
	}
	entries := e.wl.listFor(tenant, user)
	type row struct {
		WaitlistID string `json:"waitlist_id"`
		Seats      int    `json:"seats"`
		Position   int    `json:"position"`
	}
	rows := make([]row, 0, len(entries))
	for _, en := range entries {
		rows = append(rows, row{WaitlistID: en.ID, Seats: en.Seats, Position: en.pos})
	}
	writeJSON(w, http.StatusOK, map[string]interface{}{"status": "ok", "entries": rows})
}

// handleWaitlistCancel: leave the line before your turn comes.
func (e *Engine) handleWaitlistCancel(w http.ResponseWriter, r *http.Request) {
	if e.followerReject(w) {
		return
	}
	var req struct {
		WaitlistID string `json:"waitlist_id"`
		EventID    string `json:"event_id"`
	}
	tenant, _, ok := e.precheck(w, r, &req)
	if !ok {
		return
	}
	defer atomic.AddInt64(&e.inFlight, -1)
	entry, found := e.wl.cancel(req.WaitlistID, tenant)
	if !found {
		writeJSON(w, http.StatusNotFound, Result{Status: "not_found"})
		return
	}
	atomic.AddInt64(&e.waitCancelled, 1)
	e.appendWaitEvent("waitlist_cancelled", entry)
	writeJSON(w, http.StatusOK, map[string]interface{}{"status": "cancelled", "waitlist_id": entry.ID})
}

func (e *Engine) appendWaitEvent(event string, en *WaitEntry) {
	line, _ := json.Marshal(map[string]interface{}{
		"event": event, "waitlist_id": en.ID, "tenant": en.Tenant, "user_id": en.User,
		"seats": en.Seats, "ts": time.Now().UnixNano(),
	})
	e.holdGC.commit(string(line))
}

func (e *Engine) writeWaitEntry(w http.ResponseWriter, code int, en *WaitEntry) {
	pos := 0
	for _, x := range e.wl.listFor(en.Tenant, en.User) {
		if x.ID == en.ID {
			pos = x.pos
		}
	}
	writeJSON(w, code, map[string]interface{}{
		"status": "queued", "waitlist_id": en.ID, "seats": en.Seats, "position": pos,
	})
}

func writeJSON(w http.ResponseWriter, code int, v interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	json.NewEncoder(w).Encode(v)
}

func (e *Engine) clientIP(r *http.Request) string {
	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		return r.RemoteAddr
	}
	return host
}

// ---------- multi-node: seat-range sharding with ring forwarding ----------

// forwardFor sends the caller's original request (raw body plus auth
// headers) around the ring of peers. The OWNING node answers: its own
// idempotency store and its own seat slice make a forwarded retry resolve
// to the same outcome as the original attempt, so a lost response can
// never become a second booking.
func (e *Engine) forwardFor(r *http.Request, path, method string, rawBody []byte, query string) (int, []byte, bool) {
	if len(e.peers) <= 1 {
		return 0, nil, false
	}
	for k := 1; k < len(e.peers); k++ {
		peer := e.peers[(e.selfIdx+k)%len(e.peers)]
		u := "http://" + peer + path
		if query != "" {
			u += "?" + query
		}
		var rd io.Reader
		if rawBody != nil {
			rd = bytes.NewReader(rawBody)
		}
		req, err := http.NewRequest(method, u, rd)
		if err != nil {
			continue
		}
		if rawBody != nil {
			req.Header.Set("Content-Type", "application/json")
		}
		for _, h := range []string{"Authorization", "X-Kavach-Timestamp", "X-Kavach-Request-Id", "X-Kavach-Signature"} {
			if v := r.Header.Get(h); v != "" {
				req.Header.Set(h, v)
			}
		}
		req.Header.Set("X-Kavach-Forwarded-By", fmt.Sprintf("node%d", e.selfIdx))
		resp, err := e.httpc.Do(req)
		if err != nil {
			continue // peer down: try the next one in the ring
		}
		b, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		atomic.AddInt64(&e.forwarded, 1)
		return resp.StatusCode, b, true
	}
	return 0, nil, false
}

// precheck runs the shared front half of every write endpoint: method,
// bounded body read, authentication, strict schema validation, the layered
// rate limits, and the global in-flight gate. It returns the tenant, the
// parsed request, and an ok flag (a response was already written when false).
func (e *Engine) precheck(w http.ResponseWriter, r *http.Request, dst interface{}) (string, []byte, bool) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return "", nil, false
	}
	if r.Header.Get("X-Kavach-Shard-Probe") == "" { // a scan probe must not count its host request once per shard
		atomic.AddInt64(&e.received, 1)
	}
	r.Body = http.MaxBytesReader(w, r.Body, 16384) // bounded input
	body, err := io.ReadAll(r.Body)
	if err != nil {
		atomic.AddInt64(&e.errors, 1)
		writeJSON(w, http.StatusBadRequest, Result{Status: "bad_request"})
		return "", nil, false
	}
	tenant, st, msg := e.auth.verify(r, body)
	if st != 0 {
		if msg == "replay" {
			atomic.AddInt64(&e.replays, 1)
		} else {
			atomic.AddInt64(&e.authFailures, 1)
		}
		w.Header().Set("Cache-Control", "no-store")
		writeJSON(w, st, Result{Status: msg})
		return "", nil, false
	}
	dec := json.NewDecoder(strings.NewReader(string(body)))
	dec.DisallowUnknownFields() // strict schema: reject, never sanitize silently
	if err := dec.Decode(dst); err != nil {
		atomic.AddInt64(&e.errors, 1)
		writeJSON(w, http.StatusBadRequest, Result{Status: "bad_request"})
		return "", nil, false
	}
	// cluster peers forward through the limiter on purpose: the caller was
	// already limited at the edge node; everything else is still checked
	if r.Header.Get("X-Kavach-Forwarded-By") == "" && r.Header.Get("X-Kavach-Shard-Probe") == "" && !e.ipLim.Allow(e.clientIP(r)) {
		atomic.AddInt64(&e.ipLimited, 1)
		w.Header().Set("Retry-After", "2")
		writeJSON(w, http.StatusTooManyRequests, Result{Status: "rate_limited"})
		return "", nil, false
	}
	if n := atomic.AddInt64(&e.inFlight, 1); n > 500000 { // global capacity gate
		atomic.AddInt64(&e.inFlight, -1)
		atomic.AddInt64(&e.overloaded, 1)
		w.Header().Set("Retry-After", "5")
		writeJSON(w, http.StatusServiceUnavailable, Result{Status: "overloaded"})
		return "", nil, false
	}
	return tenant, body, true
}

func (e *Engine) handleBook(w http.ResponseWriter, r *http.Request) {
	if e.followerReject(w) {
		return
	}
	var req struct {
		User    string `json:"user_id"`
		IdemKey string `json:"idempotency_key"`
		Seats   int    `json:"seats"`
		EventID string `json:"event_id"`
	}
	tenant, body, ok := e.precheck(w, r, &req)
	if !ok {
		return
	}
	defer atomic.AddInt64(&e.inFlight, -1)
	if req.User == "" || req.IdemKey == "" || req.Seats < 1 || req.Seats > 4 {
		atomic.AddInt64(&e.errors, 1)
		writeJSON(w, http.StatusBadRequest, Result{Status: "bad_request"})
		return
	}
	if !e.userLim.Allow(tenant + ":" + req.User) { // layer 2: per user
		atomic.AddInt64(&e.userLimited, 1)
		w.Header().Set("Retry-After", "2")
		writeJSON(w, http.StatusTooManyRequests, Result{Status: "rate_limited"})
		return
	}

	j := &job{key: req.IdemKey, tenant: tenant, user: req.User,
		fp:    fpFor(req.IdemKey, req.User, req.Seats, eventOf(r, req.EventID)),
		seats: req.Seats, resp: make(chan Result, 1)}
	t0 := time.Now()
	select {
	case e.queue <- j:
	default: // queue full: say so instantly instead of dying later
		w.Header().Set("Retry-After", "2")
		writeJSON(w, http.StatusTooManyRequests, Result{Status: "queue_full"})
		return
	}
	select {
	case res := <-j.resp:
		e.lat.add(time.Since(t0).Microseconds())
		if res.Status == "sold_out" && len(e.peers) > 1 { // this slice is done: ask the ring
			if code, body, ok := e.forwardFor(r, "/book", http.MethodPost, body, ""); ok {
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(code)
				w.Write(body)
				return
			}
		}
		if res.Status == "error" { // a contained worker panic
			writeJSON(w, http.StatusInternalServerError, res)
			return
		}
		if res.Status == "idempotency_conflict" { // same code /reserve returns
			writeJSON(w, http.StatusConflict, res)
			return
		}
		writeJSON(w, http.StatusOK, res)
	case <-time.After(20 * time.Second):
		e.lat.add(time.Since(t0).Microseconds())
		w.Header().Set("Retry-After", "5")
		writeJSON(w, http.StatusServiceUnavailable, Result{Status: "timeout"})
	}
}

// handleReserve: PUT A HOLD on seats with a TTL, without buying yet.
// The seats leave the free pool immediately; they return automatically when
// the hold expires, or on an explicit release.
func (e *Engine) handleReserve(w http.ResponseWriter, r *http.Request) {
	if e.followerReject(w) {
		return
	}
	var req struct {
		User    string `json:"user_id"`
		IdemKey string `json:"idempotency_key"`
		Seats   int    `json:"seats"`
		EventID string `json:"event_id"`
	}
	tenant, body, ok := e.precheck(w, r, &req)
	if !ok {
		return
	}
	defer atomic.AddInt64(&e.inFlight, -1)
	if req.User == "" || req.IdemKey == "" || req.Seats < 1 || req.Seats > 4 {
		atomic.AddInt64(&e.errors, 1)
		writeJSON(w, http.StatusBadRequest, Result{Status: "bad_request"})
		return
	}
	if !e.userLim.Allow(tenant + ":" + req.User) { // layer 2: per user
		atomic.AddInt64(&e.userLimited, 1)
		w.Header().Set("Retry-After", "2")
		writeJSON(w, http.StatusTooManyRequests, Result{Status: "rate_limited"})
		return
	}

	// idempotent reserve: the same key returns the same reservation; the same
	// key with a different payload is a conflict
	fp := fpFor(req.IdemKey, req.User, req.Seats, eventOf(r, req.EventID))
	if prev, found := e.res.byIdem(tenant, req.IdemKey); found {
		if prev.Fingerprint != fp {
			atomic.AddInt64(&e.conflicts, 1)
			writeJSON(w, http.StatusConflict, map[string]interface{}{"status": "idempotency_conflict"})
			return
		}
		e.writeReservation(w, http.StatusOK, prev)
		return
	}

	seats := e.inv.tryTake(req.Seats)
	if seats == nil {
		atomic.AddInt64(&e.soldOut, 1)
		if len(e.peers) > 1 { // this slice is done: ask the ring
			if code, respBody, ok := e.forwardFor(r, "/reserve", http.MethodPost, body, ""); ok {
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(code)
				w.Write(respBody)
				return
			}
		}
		writeJSON(w, http.StatusOK, Result{Status: "sold_out"})
		return
	}
	res := &Reservation{
		ID:          fmt.Sprintf("RSV%08d", atomic.AddUint64(&e.rid, 1)),
		Tenant:      tenant,
		User:        req.User,
		Seats:       seats,
		Fingerprint: fp,
		CreatedAt:   time.Now(),
		ExpiresAt:   time.Now().Add(e.holdTTL),
		State:       resHeld,
	}
	if !e.res.put(res, req.IdemKey) { // lost a concurrent race on the same key
		e.inv.release(seats)
		if prev, found := e.res.byIdem(tenant, req.IdemKey); found {
			e.writeReservation(w, http.StatusOK, prev)
			return
		}
		writeJSON(w, http.StatusInternalServerError, Result{Status: "error"})
		return
	}
	atomic.AddInt64(&e.holdsCreated, 1)
	e.appendHold("reserved", res, "") // durable BEFORE the ack
	e.writeReservation(w, http.StatusOK, res)
}

// handleConfirm: turn a live hold into a confirmed booking. The transition
// HELD -> CONFIRMED is a single-winner CAS: a confirm racing the expiry
// worker has exactly one winner, so a seat can never be both expired and sold.
func (e *Engine) handleConfirm(w http.ResponseWriter, r *http.Request) {
	if e.followerReject(w) {
		return
	}
	var req struct {
		ReservationID string `json:"reservation_id"`
		User          string `json:"user_id"`
		EventID       string `json:"event_id"`
	}
	tenant, body, ok := e.precheck(w, r, &req)
	if !ok {
		return
	}
	defer atomic.AddInt64(&e.inFlight, -1)
	res, found := e.res.get(req.ReservationID)
	if !found || res.Tenant != tenant { // 404 both ways: never leak existence
		if len(e.peers) > 1 { // another node may own this hold
			if code, respBody, ok := e.forwardFor(r, "/confirm", http.MethodPost, body, ""); ok {
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(code)
				w.Write(respBody)
				return
			}
		}
		writeJSON(w, http.StatusNotFound, Result{Status: "not_found"})
		return
	}
	if req.User != "" && req.User != res.User {
		writeJSON(w, http.StatusForbidden, Result{Status: "forbidden"})
		return
	}
	switch atomic.LoadInt32(&res.State) {
	case resConfirmed: // idempotent confirm: same booking back
		e.writeReservation(w, http.StatusOK, res)
		return
	case resExpired:
		writeJSON(w, http.StatusConflict, map[string]interface{}{"status": "expired"})
		return
	case resReleased:
		writeJSON(w, http.StatusConflict, map[string]interface{}{"status": "released"})
		return
	}
	if !res.transition(resHeld, resConfirmed) { // expiry or release won the race
		e.writeReservationState(w, res)
		return
	}
	bookingID := fmt.Sprintf("BK%08d", atomic.AddUint64(&e.bid, 1))
	res.BookingID = bookingID
	e.appendLog(bookingID, "", res.Tenant, res.User, res.Seats) // durable BEFORE the ack
	e.inv.commit(res.Seats)
	atomic.AddInt64(&e.confirmed, 1)
	atomic.AddInt64(&e.holdsConfirm, 1)
	e.appendHold("confirmed", res, bookingID)
	e.writeReservation(w, http.StatusOK, res)
}

// handleRelease: give held seats back before the TTL runs out.
func (e *Engine) handleRelease(w http.ResponseWriter, r *http.Request) {
	if e.followerReject(w) {
		return
	}
	var req struct {
		ReservationID string `json:"reservation_id"`
		EventID       string `json:"event_id"`
	}
	tenant, body, ok := e.precheck(w, r, &req)
	if !ok {
		return
	}
	defer atomic.AddInt64(&e.inFlight, -1)
	res, found := e.res.get(req.ReservationID)
	if !found || res.Tenant != tenant {
		if len(e.peers) > 1 { // another node may own this hold
			if code, respBody, ok := e.forwardFor(r, "/release", http.MethodPost, body, ""); ok {
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(code)
				w.Write(respBody)
				return
			}
		}
		writeJSON(w, http.StatusNotFound, Result{Status: "not_found"})
		return
	}
	switch atomic.LoadInt32(&res.State) {
	case resReleased:
		e.writeReservation(w, http.StatusOK, res)
		return
	case resConfirmed:
		writeJSON(w, http.StatusConflict, map[string]interface{}{"status": "confirmed"})
		return
	case resExpired:
		writeJSON(w, http.StatusConflict, map[string]interface{}{"status": "expired"})
		return
	}
	if res.transition(resHeld, resReleased) {
		e.inv.release(res.Seats)
		atomic.AddInt64(&e.holdsRelease, 1)
		e.appendHold("released", res, "")
		e.writeReservation(w, http.StatusOK, res)
		go e.serveWaitlist()
		return
	}
	e.writeReservationState(w, res)
}

// handleReservation: the status of one hold, tenant-scoped. A client that
// lost a response can resolve the outcome here instead of guessing.
func (e *Engine) handleReservation(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	tenant, st, msg := e.auth.verify(r, nil)
	if st != 0 {
		writeJSON(w, st, Result{Status: msg})
		return
	}
	id := r.URL.Query().Get("id")
	if id == "" {
		writeJSON(w, http.StatusBadRequest, Result{Status: "bad_request"})
		return
	}
	res, found := e.res.get(id)
	if !found || res.Tenant != tenant {
		if len(e.peers) > 1 { // another node may own this hold
			if code, respBody, ok := e.forwardFor(r, "/reservation", http.MethodGet, nil, "id="+id); ok {
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(code)
				w.Write(respBody)
				return
			}
		}
		writeJSON(w, http.StatusNotFound, Result{Status: "not_found"})
		return
	}
	e.writeReservation(w, http.StatusOK, res)
}

func (e *Engine) writeReservation(w http.ResponseWriter, code int, res *Reservation) {
	writeJSON(w, code, map[string]interface{}{
		"status":          resStateName(atomic.LoadInt32(&res.State)),
		"reservation_id":  res.ID,
		"seats":           res.Seats,
		"hold_expires_at": res.ExpiresAt.Unix(),
		"hold_seconds":    int(time.Until(res.ExpiresAt).Seconds()),
		"booking_id":      res.BookingID,
	})
}

func (e *Engine) writeReservationState(w http.ResponseWriter, res *Reservation) {
	writeJSON(w, http.StatusConflict, map[string]interface{}{
		"status":         resStateName(atomic.LoadInt32(&res.State)),
		"reservation_id": res.ID,
		"booking_id":     res.BookingID,
	})
}

// handleStatus lets a client resolve an ambiguous timeout on the direct
// /book path: query the stored outcome of an idempotency key instead of
// blindly re-submitting.
func (e *Engine) handleStatus(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	tenant, st, msg := e.auth.verify(r, nil)
	if st != 0 {
		writeJSON(w, st, Result{Status: msg})
		return
	}
	key := r.URL.Query().Get("idempotency_key")
	if key == "" {
		writeJSON(w, http.StatusBadRequest, Result{Status: "bad_request"})
		return
	}
	if entry, ok := e.idem.lookup(tenant, key); ok {
		writeJSON(w, http.StatusOK, entry.Result)
		return
	}
	if len(e.peers) > 1 { // another node may own this booking
		if code, respBody, ok := e.forwardFor(r, "/status", http.MethodGet, nil, "idempotency_key="+key); ok {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(code)
			w.Write(respBody)
			return
		}
	}
	writeJSON(w, http.StatusNotFound, Result{Status: "not_found"})
}


// handleSeatsState serves the live per-seat state map as one compact string:
// index i is global seat i (minus the node's offset in cluster mode),
// characters '0' free, '1' held, '2' sold. Read-only and additive; the
// demo UI paints its chairs from it in real time.
func (e *Engine) handleSeatsState(w http.ResponseWriter, r *http.Request) {
	inv := e.inv
	buf := make([]byte, len(inv.seats))
	for i := range inv.seats {
		buf[i] = byte('0' + atomic.LoadInt32(&inv.seats[i]))
	}
	writeJSON(w, http.StatusOK, map[string]interface{}{
		"offset": inv.offset, "states": string(buf),
	})
}

func (e *Engine) handleSeats(w http.ResponseWriter, r *http.Request) {
	total, sold, held := e.inv.stats()
	local := map[string]int{"total": total, "sold": sold, "held": held, "free": total - sold - held}
	if len(e.peers) <= 1 || r.URL.Query().Get("local") == "1" {
		writeJSON(w, http.StatusOK, local)
		return
	}
	// cluster view: every node's local slice, summed. Same shape as before,
	// plus a per-node breakdown so you can see the sharding work.
	agg := map[string]int{"total": 0, "sold": 0, "held": 0, "free": 0}
	nodes := map[string]map[string]int{}
	add := func(name string, m map[string]int) {
		nodes[name] = m
		for _, k := range []string{"total", "sold", "held", "free"} {
			agg[k] += m[k]
		}
	}
	add(e.peers[e.selfIdx], local)
	for k := 1; k < len(e.peers); k++ {
		peer := e.peers[(e.selfIdx+k)%len(e.peers)]
		var m map[string]int
		var lastErr error
		for attempt := 0; attempt < 2; attempt++ { // a stale idle conn must not drop a live node from the view
			resp, err := e.httpc.Get("http://" + peer + "/seats?local=1")
			if err == nil {
				json.NewDecoder(resp.Body).Decode(&m)
				resp.Body.Close()
				break
			}
			lastErr = err
			time.Sleep(100 * time.Millisecond)
		}
		if m == nil {
			log.Printf("kavach cluster: peer %s query failed: %v", peer, lastErr)
			continue // a down node just drops out of the sum
		}
		add(peer, m)
	}
	writeJSON(w, http.StatusOK, map[string]interface{}{
		"cluster": true, "total": agg["total"], "sold": agg["sold"],
		"held": agg["held"], "free": agg["free"], "nodes": nodes,
	})
}

func (e *Engine) handleMetrics(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, e.metricsMap())
}

// metricsMap builds the /metrics document. The shard router sums these
// across shards, so every counter stays here in its raw per-shard form.
func (e *Engine) metricsMap() map[string]interface{} {
	count, p50, p95, p99 := e.lat.snapshot()
	return map[string]interface{}{
		"received":          atomic.LoadInt64(&e.received),
		"rate_limited":      atomic.LoadInt64(&e.userLimited) + atomic.LoadInt64(&e.ipLimited),
		"ip_rate_limited":   atomic.LoadInt64(&e.ipLimited),
		"user_rate_limited": atomic.LoadInt64(&e.userLimited),
		"auth_failures":     atomic.LoadInt64(&e.authFailures),
		"replays_rejected":  atomic.LoadInt64(&e.replays),
		"idem_conflicts":    atomic.LoadInt64(&e.conflicts),
		"overloaded":        atomic.LoadInt64(&e.overloaded),
		"in_flight":         atomic.LoadInt64(&e.inFlight),
		"confirmed":         atomic.LoadInt64(&e.confirmed),
		"sold_out":          atomic.LoadInt64(&e.soldOut),
		"errors":            atomic.LoadInt64(&e.errors),
		"holds_created":     atomic.LoadInt64(&e.holdsCreated),
		"holds_confirmed":   atomic.LoadInt64(&e.holdsConfirm),
		"holds_released":    atomic.LoadInt64(&e.holdsRelease),
		"holds_expired":     atomic.LoadInt64(&e.holdsExpired),
		"waitlist_queued":     atomic.LoadInt64(&e.waitQueued),
		"waitlist_fulfilled":  atomic.LoadInt64(&e.waitFulfilled),
		"waitlist_cancelled":  atomic.LoadInt64(&e.waitCancelled),
		"webhook_sent":        atomic.LoadInt64(&e.webhookSent),
		"webhook_failed":      atomic.LoadInt64(&e.webhookFailed),
		"forwarded":           atomic.LoadInt64(&e.forwarded),
		"replication_streams":    atomic.LoadInt64(&e.replStreams),
		"replicated_lines":       atomic.LoadInt64(&e.replicatedLines),
		"replication_connected":  int(atomic.LoadInt32(&e.replConnected)),
		"role": func() string {
			if e.replFrom == "" || atomic.LoadInt32(&e.replPromoted) == 1 {
				return "leader"
			}
			return "follower"
		}(),
		"pg_shipped":          atomic.LoadInt64(&e.pgShipped),
		"pg_failed":           atomic.LoadInt64(&e.pgFailed),
		"chaos_stalls":        atomic.LoadInt64(&e.chaosStalls),
		"chaos_panics":        atomic.LoadInt64(&e.chaosPanics),
		"snapshots_total":     atomic.LoadInt64(&e.snapTotal),
		"last_snapshot_age_seconds": func() float64 {
			ls := atomic.LoadInt64(&e.lastSnapAt)
			if ls == 0 {
				return -1
			}
			return time.Since(time.Unix(0, ls)).Seconds()
		}(),
		"recovery_replayed_lines": atomic.LoadInt64(&e.recoveryLines),
		"latency_ms":        map[string]interface{}{"p50": p50, "p95": p95, "p99": p99, "samples": count},
		"uptime_seconds":    time.Since(e.started).Seconds(),
	}
}

func main() {
	seats := flag.Int("seats", 5000, "seats in inventory")
	addr := flag.String("addr", ":8080", "listen address")
	workers := flag.Int("workers", 16, "worker count")
	shards := flag.Int("shards", 1, "multi-shard: number of independent event shards in this process, routed by event_id hash (1 = single pool)")
	logPath := flag.String("log", "bookings.jsonl", "confirmed-booking event log path")
	holdsLog := flag.String("holds-log", "holds.jsonl", "reservation lifecycle event log path")
	holdTTL := flag.Int("hold-ttl", 300, "reservation hold time-to-live in seconds")
	waitCap := flag.Int("waitlist-cap", 100000, "maximum waitlist entries")
	webhookURL := flag.String("webhook-url", "", "POST waitlist_fulfilled events to this URL")
	peersSpec := flag.String("peers", "", "cluster: comma-separated addr:port of EVERY node including this one (seat-range sharding; empty = single node)")
	pgDSN := flag.String("pg-dsn", "", "Postgres write-behind: DSN to ship committed events to, e.g. postgres://user@host:5432/db?sslmode=disable (needs: go build -tags postgres)")
	nodeID := flag.String("node-id", "n1", "identity of this node in the Postgres write-behind sink")
	replKeySpec := flag.String("replicate-key", "", "replication channel api key (leader serves, follower signs)")
	replSecretSpec := flag.String("replicate-secret", "", "replication channel HMAC secret; empty disables /internal/replicate")
	replFromSpec := flag.String("replicate-from", "", "follower: leader base URL to tail WAL segments from (e.g. http://127.0.0.1:8080)")
	chaosStallMs := flag.Int("chaos-stall-ms", 0, "chaos: inject random worker stalls up to this many ms (0 = off)")
	chaosProb := flag.Float64("chaos-prob", 0, "chaos: probability a request hits a stall")
	chaosPanic := flag.Float64("chaos-panic", 0, "chaos: probability a worker panics (contained; the engine survives)")
	otelEndpoint := flag.String("otel-endpoint", "", "OpenTelemetry: OTLP/HTTP endpoint to export request spans to, e.g. http://localhost:4318 (empty = tracing off)")
	otelRatio := flag.Float64("otel-ratio", 1, "OpenTelemetry: trace sample ratio 0..1 (1 = trace every request)")
	apiKeys := flag.String("api-keys", "", "API keys as key:tenant,key:tenant (empty = local mode, auth off)")
	secret := flag.String("hmac-secret", "", "HMAC secret; when set, requests must be signed (X-Kavach-Timestamp, X-Kavach-Request-Id, X-Kavach-Signature over timestamp\\nrequest-id\\nmethod\\nuri\\napi-key\\nbody; the api key is part of the MAC in both modes)")
	tenantSecrets := flag.String("hmac-secrets", "", "per-tenant HMAC secrets as tenant:secret,...; verifies each request against ITS tenant's secret (overrides -hmac-secret; tenants without an entry are rejected)")
	userRate := flag.Float64("user-rate", 2, "per-user requests/second (0 disables)")
	userBurst := flag.Float64("user-burst", 5, "per-user burst")
	snapshotEvery := flag.Int("snapshot-every", 0, "WAL snapshots: take a snapshot every N committed log events, then rotate the logs (0 = off)")
	snapshotInterval := flag.Int("snapshot-interval", 0, "WAL snapshots: also snapshot after this many seconds with new events (0 = off; -snapshot-every alone also works)")
	ipRate := flag.Float64("ip-rate", 20, "per-IP requests/second (0 disables)")
	ipBurst := flag.Float64("ip-burst", 40, "per-IP burst")
	flag.Parse()

	var peers []string
	selfIdx := 0
	if *peersSpec != "" {
		portOf := func(x string) string {
			if _, port, err := net.SplitHostPort(x); err == nil {
				return port
			}
			return strings.TrimPrefix(x, ":")
		}
		for _, p := range strings.Split(*peersSpec, ",") {
			if p = strings.TrimSpace(p); p != "" {
				peers = append(peers, p)
			}
		}
		found := false
		for i, p := range peers {
			if portOf(p) == portOf(*addr) {
				selfIdx, found = i, true
				break
			}
		}
		if !found {
			log.Fatalf("this node's addr %s is not in -peers", *addr)
		}
	}
	offset := 0
	if len(peers) > 1 && selfIdx > 0 {
		// This node owns seats [offset, offset+seats): the offset is the sum
		// of the slice sizes of every peer listed BEFORE us. -peers carries
		// no seat counts, so ask each preceding peer for its own slice size;
		// the old selfIdx*seats shortcut was only correct when every node has
		// the same seat count, and a wrong offset overlaps two nodes' slices
		// and can double-book global seats. Fail hard rather than guess.
		for i := 0; i < selfIdx; i++ {
			n, err := peerSliceSeats(peers[i])
			if err != nil {
				log.Fatalf("kavach: cannot compute slice offset: peer %s: %v", peers[i], err)
			}
			offset += n
		}
	}
	if *shards > 1 {
		if *shards > 64 {
			log.Fatal("kavach: -shards is capped at 64 (each shard reserves a disjoint 10M id range)")
		}
		if *peersSpec != "" || *replFromSpec != "" || *replKeySpec != "" || *replSecretSpec != "" {
			log.Fatal("kavach: multi-shard (-shards > 1) is single-node in this phase; it does not yet combine with -peers or -replicate-* (per-shard replication is the next milestone)")
		}
	}

	nShards := *shards
	// shared across shards: one auth table, one idempotency store, one rate
	// limit budget per tenant and per IP. Splitting these would let a client
	// buy four rate limits by writing four event ids.
	userLim := NewLimiter(*userRate, *userBurst)
	ipLim := NewLimiter(*ipRate, *ipBurst)
	auth := newAuthWithTenantSecrets(*apiKeys, *secret, *tenantSecrets)
	idem := NewIdemStore()
	perShardWorkers := *workers / nShards
	if perShardWorkers < 1 {
		perShardWorkers = 1
	}
	shardsList := make([]*Engine, nShards)
	var recBook, recRes, recWait, recLines, maxSnapSeq int64
	for i := 0; i < nShards; i++ {
		bookPath, holdPath := *logPath, *holdsLog
		snapDir := filepath.Dir(*logPath)
		if nShards > 1 { // per-shard state directories keep WALs and snapshots isolated
			sub := filepath.Join(filepath.Dir(*logPath), fmt.Sprintf("shard-%02d", i))
			if err := os.MkdirAll(sub, 0o755); err != nil {
				log.Fatalf("shard state dir: %v", err)
			}
			bookPath = filepath.Join(sub, filepath.Base(*logPath))
			holdPath = filepath.Join(sub, filepath.Base(*holdsLog))
			snapDir = sub
		}
		shardOffset := 0
		if nShards == 1 && len(peers) > 1 {
			shardOffset = offset // the real per-peer slice offset computed above
		}
		e := &Engine{
			inv:      NewInventory(*seats, shardOffset),
			userLim:  userLim,
			ipLim:    ipLim,
			auth:     auth,
			idem:     idem,
			res:      NewReservationStore(),
			queue:    make(chan *job, 100000),
			lat:      NewLatencyRing(1000000),
			holdTTL:  time.Duration(*holdTTL) * time.Second,
			wl:       NewWaitlist(),
			waitCap:    *waitCap,
			webhookURL: *webhookURL,
			webhookCh:  make(chan map[string]interface{}, 10000),
			replNonces: make(map[string]int64),
			chaosStallMs: *chaosStallMs,
			chaosProb:    *chaosProb,
			chaosPanic:   *chaosPanic,
			peers:        peers,
			selfIdx:      selfIdx,
			httpc:        &http.Client{Timeout: 5 * time.Second},
			started:  time.Now(),
			snapEvery:    *snapshotEvery,
			snapInterval: time.Duration(*snapshotInterval) * time.Second,
			snapDir:      snapDir,
			lastSnapAt:   time.Now().UnixNano(),
		}
		// restart recovery: newest snapshot first (if any), then the log tail
		// after its recorded offsets; without a snapshot, the full logs replay.
		snapSeq, bRec, rRec, wRec, rLines := recoverState(snapDir, bookPath, holdPath,
			e.inv, e.res, e.wl, e.idem, e.holdTTL, &e.bid, &e.rid, &waitlistSeq)
		atomic.StoreInt64(&e.recoveryLines, int64(rLines))
		// snapshot numbering continues from disk: a fresh run must never
		// overwrite a snapshot file that an older recovery path may still need
		if uint64(snapSeq) > e.snapSeq {
			e.snapSeq = uint64(snapSeq)
		}
		if hs := highestSnapshotSeq(snapDir); uint64(hs) > e.snapSeq {
			e.snapSeq = uint64(hs)
		}
		if int64(snapSeq) > maxSnapSeq {
			maxSnapSeq = int64(snapSeq)
		}
		recBook += int64(bRec)
		recRes += int64(rRec)
		recWait += int64(wRec)
		recLines += int64(rLines)
		// disjoint BK/RSV id ranges: per shard in multi-shard mode, per node
		// in cluster mode; the waitlist sequence stays a process-wide counter
		if nShards > 1 {
			floor := uint64(i) * 10000000
			if e.bid < floor {
				e.bid = floor
			}
			if e.rid < floor {
				e.rid = floor
			}
		} else if len(peers) > 1 {
			floor := uint64(selfIdx) * 10000000
			if e.bid < floor {
				e.bid = floor
			}
			if e.rid < floor {
				e.rid = floor
			}
			if waitlistSeq < floor {
				waitlistSeq = floor
			}
		}
		if *pgDSN != "" { // write-behind: the local logs stay the source of truth, Postgres mirrors them
			sinkID := *nodeID
			if nShards > 1 {
				sinkID = fmt.Sprintf("%s-s%d", *nodeID, i)
			}
			sink, err := NewPgSink(*pgDSN, sinkID, []string{bookPath, holdPath}, &e.pgShipped, &e.pgFailed)
			if err != nil {
				log.Fatalf("postgres sink: %v", err)
			}
			_ = sink
			log.Printf("kavach write-behind: shipping %s and %s to Postgres (node %s)", bookPath, holdPath, sinkID)
		}
		bookGC, err := NewGroupCommit(bookPath, 200000)
		if err != nil {
			log.Fatal(err)
		}
		holdGC, err := NewGroupCommit(holdPath, 200000)
		if err != nil {
			log.Fatal(err)
		}
		e.bookGC = bookGC
		e.holdGC = holdGC
		for w := 0; w < perShardWorkers; w++ {
			go e.worker()
		}
		if *snapshotEvery > 0 || *snapshotInterval > 0 {
			go func(shard *Engine) { // snapshot trigger: volume or interval, never both idle
				t := time.NewTicker(1 * time.Second)
				for range t.C {
					ev := atomic.LoadInt64(&shard.eventsSince)
					if ev <= 0 {
						continue
					}
					due := *snapshotEvery > 0 && ev >= int64(*snapshotEvery)
					if !due && *snapshotInterval > 0 {
						due = time.Since(time.Unix(0, atomic.LoadInt64(&shard.lastSnapAt))) >= shard.snapInterval
					}
					if due {
						if err := shard.runSnapshot(); err != nil {
							log.Printf("kavach snapshot failed (state keeps running on the logs): %v", err)
						}
					}
				}
			}(e)
		}
		go func(shard *Engine) { // webhook poster: deliveries never block the request path
			client := &http.Client{Timeout: 5 * time.Second}
			for ev := range shard.webhookCh {
				b, _ := json.Marshal(ev)
				resp, err := client.Post(shard.webhookURL, "application/json", bytes.NewReader(b))
				if err != nil {
					atomic.AddInt64(&shard.webhookFailed, 1)
					continue
				}
				resp.Body.Close()
				if resp.StatusCode < 300 {
					atomic.AddInt64(&shard.webhookSent, 1)
				} else {
					atomic.AddInt64(&shard.webhookFailed, 1)
				}
			}
		}(e)
		shardsList[i] = e
	}
	e := shardsList[0]

	// idempotency store hygiene (the store is shared): evict only idle entries
	go func() {
		t := time.NewTicker(20 * time.Second)
		for range t.C {
			idem.trim(2000000, 10*time.Minute)
		}
	}()
	go func() { // replay-nonce hygiene
		t := time.NewTicker(60 * time.Second)
		for range t.C {
			auth.pruneNonces()
		}
	}()
	go func() { // hold expiry: abandoned holds return to the free pool, every shard
		t := time.NewTicker(1 * time.Second)
		for range t.C {
			for _, shard := range shardsList {
				shard.expireSweep()
			}
		}
	}()
	if maxSnapSeq > 0 {
		log.Printf("kavach recovery: snapshot %d loaded; %d bookings, %d reservations, %d waitlist entries restored; %d tail lines replayed", maxSnapSeq, recBook, recRes, recWait, recLines)
	} else if recBook+recRes+recWait > 0 {
		log.Printf("kavach recovery: %d bookings, %d reservations, %d waitlist entries restored", recBook, recRes, recWait)
	}

	if *replFromSpec != "" {
		if *snapshotEvery > 0 || *snapshotInterval > 0 {
			log.Fatal("kavach: a replication follower cannot enable snapshots in phase 1 (its WAL tail is applied live; snapshots on a follower come later)")
		}
		e.replFrom = *replFromSpec
		go e.runFollower()
	}
	if *replKeySpec != "" && *replSecretSpec != "" {
		e.replKey = *replKeySpec
		e.replSecret = []byte(*replSecretSpec)
		http.HandleFunc("/internal/replicate", e.handleReplicate)
		http.HandleFunc("/internal/promote", e.handlePromote)
	}
	sr := NewShardRouter(shardsList)
	http.HandleFunc("/book", sr.route((*Engine).handleBook, true))
	http.HandleFunc("/reserve", sr.route((*Engine).handleReserve, true))
	http.HandleFunc("/confirm", sr.scan((*Engine).handleConfirm, true))
	http.HandleFunc("/release", sr.scan((*Engine).handleRelease, true))
	http.HandleFunc("/reservation", sr.scan((*Engine).handleReservation, false))
	http.HandleFunc("/waitlist", sr.handleWaitlist)
	http.HandleFunc("/waitlist/cancel", sr.scan((*Engine).handleWaitlistCancel, true))
	http.HandleFunc("/status", sr.shards[0].handleStatus) // the idem store is shared
	http.HandleFunc("/seats", sr.route((*Engine).handleSeats, false))
	http.HandleFunc("/seats/state", sr.route((*Engine).handleSeatsState, false))
	http.HandleFunc("/metrics", sr.handleMetrics)
	mode := "local (auth off)"
	if e.auth.enabled {
		mode = "authenticated"
	}
	log.Printf("kavach engine on %s: %d shard(s) x %d seats, %d workers/shard, %d CPUs, hold-ttl=%ds, mode=%s, peers=%d", *addr, nShards, *seats, perShardWorkers, runtime.NumCPU(), *holdTTL, mode, len(peers))
	shutdownTracing, terr := setupTracing(*otelEndpoint, *otelRatio)
	if terr != nil {
		log.Printf("kavach: otel tracing disabled (%v)", terr)
	}
	if shutdownTracing != nil {
		e.tracer = otel.Tracer("kavach")
		defer shutdownTracing(context.Background())
		log.Printf("kavach: otel tracing on, endpoint %s, sample ratio %.2f", *otelEndpoint, *otelRatio)
	}
	log.Fatal(http.ListenAndServe(*addr, e.tracingMiddleware(http.DefaultServeMux)))
}
