package main

// Cluster mode, phase 1: the WAL is the replication stream. A leader serves
// its bookings/holds logs over /internal/replicate; a follower tails them,
// appends every line to its OWN log files (so it is durable on its own and
// replays through the exact same recovery path), and applies new tail lines
// through the same code replayLogsPaths uses. No second state machine, no
// drift. Writes on a follower are refused with 503; failover (phase 2) is
// what turns a follower into a leader.

import (
	"bufio"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync/atomic"
	"time"
)

// ---------- shared replay primitives (used by recovery AND the follower) ----------

type replayEvent struct {
	kind      string
	ts        int64
	bookingID string
	key       string
	userID    string
	resID     string
	tenant    string
	user      string
	seats     []int
	wlID      string
	wlSeats   int
	expiresAt int64
}

// parseReplayEvent decodes one WAL line into a replayEvent. Unknown or
// unparsable lines are skipped by the caller, exactly as recovery does.
func parseReplayEvent(line []byte) (replayEvent, bool) {
	var m map[string]interface{}
	if json.Unmarshal(line, &m) != nil {
		return replayEvent{}, false
	}
	gs := func(k string) string { s, _ := m[k].(string); return s }
	gf := func(k string) float64 { f, _ := m[k].(float64); return f }
	gi := func(k string) []int {
		arr, _ := m[k].([]interface{})
		out := make([]int, 0, len(arr))
		for _, v := range arr {
			f, _ := v.(float64)
			out = append(out, int(f))
		}
		return out
	}
	return replayEvent{
		kind: gs("event"), ts: int64(gf("ts")), bookingID: gs("booking_id"),
		resID: gs("reservation_id"), tenant: gs("tenant"), user: gs("user"),
		userID: gs("user_id"),
		seats:  gi("seats"), wlID: gs("waitlist_id"), wlSeats: int(gf("seats")),
		key:    gs("key"), expiresAt: int64(gf("expires_at")),
	}, true
}

// applyReplayEvents applies parsed WAL events to engine state. This is the
// ONE apply path: recovery (replayLogsPaths) and the live follower both call
// it, so a follower is byte-for-byte the state machine its leader is.
func applyReplayEvents(events []replayEvent, inv *Inventory, res *ReservationStore,
	wl *Waitlist, idem *IdemStore, holdTTL time.Duration,
	bid, rid, waitlistSeq *uint64) (bookings, reservations, queued int) {

	idNum := func(id string) uint64 {
		if len(id) > 2 {
			if n, err := strconv.Atoi(id[2:]); err == nil {
				return uint64(n)
			}
		}
		return 0
	}
	stateFromName := map[string]int32{"released": resReleased, "expired": resExpired}

	for _, ev := range events {
		switch {
		case ev.kind == "" && ev.bookingID != "": // a confirmed /book booking
			inv.markSold(ev.seats)
			if idNum(ev.bookingID) > *bid {
				*bid = idNum(ev.bookingID)
			}
			if ev.key != "" { // the retry-safety guarantee survives restarts too
				fp := fmt.Sprintf("%x", sha256.Sum256([]byte(fmt.Sprintf("%s|%s|%d", ev.key, ev.user, len(ev.seats)))))
				idem.mu.Lock()
				idem.m[ev.tenant+"|"+ev.key] = &idemEntry{
					Fingerprint: fp, Tenant: ev.tenant, LastSeen: time.Now(),
					Result: Result{Status: "confirmed", Seats: ev.seats, BookingID: ev.bookingID}}
				idem.mu.Unlock()
			}
			bookings++
		case ev.kind == "reserved":
			expires := time.Now().Add(holdTTL)
			if ev.expiresAt > 0 {
				expires = time.Unix(0, ev.expiresAt)
			}
			res.put(&Reservation{ID: ev.resID, Tenant: ev.tenant, User: ev.user,
				Seats: ev.seats, ExpiresAt: expires, State: resHeld}, "")
			inv.markHeld(ev.seats)
			if idNum(ev.resID) > *rid {
				*rid = idNum(ev.resID)
			}
			reservations++
		case ev.kind == "confirmed":
			// the seats were already counted sold by the booking line;
			// here only the reservation's own state and id matter
			if r, ok := res.get(ev.resID); ok {
				r.transition(resHeld, resConfirmed)
				r.BookingID = ev.bookingID
			}
		case ev.kind == "released" || ev.kind == "expired":
			if r, ok := res.get(ev.resID); ok {
				atomic.StoreInt32(&r.State, stateFromName[ev.kind])
			}
			inv.freeHeld(ev.seats)
		case ev.kind == "waitlisted":
			owner := ev.user // waitlist events carry user_id, hold events carry user
			if ev.userID != "" {
				owner = ev.userID
			}
			e := &WaitEntry{ID: ev.wlID, Tenant: ev.tenant, User: owner,
				Seats: ev.wlSeats, QueuedAt: time.Unix(0, ev.ts)}
			wl.mu.Lock()
			wl.order = append(wl.order, e)
			wl.mu.Unlock()
			if idNum(ev.wlID) > *waitlistSeq {
				*waitlistSeq = idNum(ev.wlID)
			}
			queued++
		case ev.kind == "waitlist_cancelled" || ev.kind == "waitlist_fulfilled":
			wl.remove(ev.wlID)
		}
	}
	return bookings, reservations, queued
}

// ---------- replication auth (same MAC composition as the client API, own key) ----------

func (e *Engine) verifyReplication(r *http.Request) bool {
	if e.replSecret == nil || e.replKey == "" {
		return false
	}
	key := strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer ")
	if key != e.replKey {
		return false
	}
	ts := r.Header.Get("X-Kavach-Timestamp")
	rid := r.Header.Get("X-Kavach-Request-Id")
	sig := r.Header.Get("X-Kavach-Signature")
	if ts == "" || rid == "" || sig == "" {
		return false
	}
	tsVal, err := strconv.ParseInt(ts, 10, 64)
	now := time.Now().Unix()
	if err != nil || tsVal < now-300 || tsVal > now+300 {
		return false
	}
	e.replNonceMu.Lock()
	if e.replNonces == nil { // engines built outside main() may not have run the initializer
		e.replNonces = make(map[string]int64)
	}
	if exp, seen := e.replNonces[rid]; seen && exp > now {
		e.replNonceMu.Unlock()
		return false
	}
	e.replNonces[rid] = now + 300
	e.replNonceMu.Unlock()
	mac := hmac.New(sha256.New, e.replSecret)
	mac.Write([]byte(ts + "\n" + rid + "\n" + r.Method + "\n" + r.URL.RequestURI() + "\n" + key + "\n"))
	want, err := hex.DecodeString(sig)
	if err != nil || !hmac.Equal(mac.Sum(nil), want) {
		return false
	}
	return true
}

// signReplicationRequest builds the HMAC headers the follower sends upstream.
func signReplicationRequest(key string, secret []byte, method, uri string) map[string]string {
	ts := strconv.FormatInt(time.Now().Unix(), 10)
	rid := fmt.Sprintf("repl%d%d", time.Now().UnixNano(), time.Now().UnixNano()%1000003)
	mac := hmac.New(sha256.New, secret)
	mac.Write([]byte(ts + "\n" + rid + "\n" + method + "\n" + uri + "\n" + key + "\n"))
	return map[string]string{
		"Authorization":       "Bearer " + key,
		"X-Kavach-Timestamp":  ts,
		"X-Kavach-Request-Id": rid,
		"X-Kavach-Signature":  hex.EncodeToString(mac.Sum(nil)),
	}
}

// ---------- leader side: stream the WAL ----------

type replFrame struct {
	Off  int64  `json:"off"` // cumulative byte offset AFTER this line, across the log's segments+live file
	Line string `json:"line"`
}

// logStreamFiles returns the log's files in stream order: rotated segments
// (oldest first) then the live log.
func logStreamFiles(base string) []string {
	files := segmentPaths(base, 0)
	if _, err := os.Stat(base); err == nil {
		files = append(files, base)
	}
	return files
}

// streamLogFrom writes every complete line of the logical log at or after
// `after` (a cumulative byte offset) as replFrames. It tails the live file
// until the client goes away. Returns when the stream ends.
func streamLogFrom(w http.ResponseWriter, r *http.Request, base string, after int64) {
	flusher, _ := w.(http.Flusher)
	enc := json.NewEncoder(w)
	emit := func(f replFrame) bool {
		if err := enc.Encode(f); err != nil {
			return false
		}
		flusher.Flush()
		return true
	}
	for {
		if r.Context().Err() != nil {
			return
		}
		cum := int64(0)
		caughtUp := true
		files := logStreamFiles(base)
		for idx, path := range files {
			st, err := os.Stat(path)
			if err != nil {
				continue
			}
			size := st.Size()
			isLive := idx == len(files)-1 && path == base
			if after >= cum+size {
				cum += size
				continue
			}
			f, err := os.Open(path)
			if err != nil {
				cum += size
				continue
			}
			startInFile := after - cum
			if startInFile > 0 {
				if _, err := f.Seek(startInFile, io.SeekStart); err != nil {
					f.Close()
					cum += size
					continue
				}
			}
			pos := startInFile
			rd := bufio.NewReaderSize(f, 1<<20)
			for {
				line, rerr := rd.ReadString('\n')
				if len(line) > 0 && line[len(line)-1] == '\n' {
					// a complete line: emit and advance
					off := cum + pos + int64(len(line))
					if off > after {
						if !emit(replFrame{Off: off, Line: strings.TrimRight(line, "\n")}) {
							f.Close()
							return
						}
						after = off
					}
					pos += int64(len(line))
					continue
				}
				// partial line (writer mid-batch) or clean EOF
				if rerr == io.EOF || rerr == nil {
					if isLive {
						caughtUp = false // stay on the live file and wait for more
					}
					break
				}
				log.Printf("kavach replicate: read %s: %v", path, rerr)
				break
			}
			after = cum + pos
			f.Close()
			if !caughtUp {
				break // keep tailing this (live) file on the next outer pass
			}
			cum += size
		}
		if caughtUp {
			select {
			case <-time.After(50 * time.Millisecond):
			case <-r.Context().Done():
				return
			}
		}
	}
}

func (e *Engine) handleReplicate(w http.ResponseWriter, r *http.Request) {
	if e.replSecret == nil {
		http.Error(w, `{"status":"not_found"}`, http.StatusNotFound)
		return
	}
	if !e.verifyReplication(r) {
		http.Error(w, `{"status":"unauthorized"}`, http.StatusUnauthorized)
		return
	}
	logName := r.URL.Query().Get("log")
	var base string
	switch logName {
	case "bookings":
		base = e.bookGC.path
	case "holds":
		base = e.holdGC.path
	default:
		http.Error(w, `{"status":"bad_request"}`, http.StatusBadRequest)
		return
	}
	after, _ := strconv.ParseInt(r.URL.Query().Get("after"), 10, 64)
	w.Header().Set("Content-Type", "application/x-ndjson")
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(http.StatusOK)
	atomic.AddInt64(&e.replStreams, 1)
	defer atomic.AddInt64(&e.replStreams, -1)
	streamLogFrom(w, r, base, after)
}

// ---------- follower side ----------

// runFollower tails the leader's two logs, appends every line to this
// node's OWN log files (durable WAL of its own), and a single applier
// goroutine replays new tail bytes through the shared apply path.
func (e *Engine) runFollower() {
	// apply positions start at the end of our own WAL: recovery has already
	// replayed every byte on disk, so the applier must resume after them,
	// never re-read from zero (a restart would double-apply the whole log)
	e.replBookPos = fileSize(e.bookGC.path)
	e.replHoldPos = fileSize(e.holdGC.path)
	for _, logName := range []string{"bookings", "holds"} {
		go e.followOne(logName)
	}
	go e.followerApplier()
}

func (e *Engine) followOne(logName string) {
	var base string
	if logName == "bookings" {
		base = e.bookGC.path
	} else {
		base = e.holdGC.path
	}
	for {
		if atomic.LoadInt32(&e.replConnected) == 0 {
			atomic.StoreInt32(&e.replConnected, 1)
		}
		localSize := fileSize(base)
		uri := fmt.Sprintf("/internal/replicate?log=%s&after=%d", logName, localSize)
		req, err := http.NewRequest("GET", strings.TrimSuffix(e.replFrom, "/")+uri, nil)
		if err != nil {
			time.Sleep(time.Second)
			continue
		}
		for k, v := range signReplicationRequest(e.replKey, e.replSecret, "GET", uri) {
			req.Header.Set(k, v)
		}
		resp, err := http.DefaultClient.Do(req)
		if err != nil || resp.StatusCode != 200 {
			if resp != nil {
				resp.Body.Close()
			}
			atomic.StoreInt32(&e.replConnected, 0)
			log.Printf("kavach follower: leader %s unreachable (%v); retrying", e.replFrom, err)
			time.Sleep(time.Second)
			continue
		}
		rd := bufio.NewReaderSize(resp.Body, 1<<20)
		f, openErr := os.OpenFile(base, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
		if openErr != nil {
			resp.Body.Close()
			log.Printf("kavach follower: open %s: %v", base, openErr)
			time.Sleep(time.Second)
			continue
		}
		for {
			line, rerr := rd.ReadString('\n')
			if len(line) > 0 {
				var fr replFrame
				if jerr := json.Unmarshal([]byte(line), &fr); jerr == nil && fr.Off > localSize {
					f.Write([]byte(fr.Line))
					f.Write([]byte("\n"))
					localSize = fr.Off
				}
			}
			if rerr != nil {
				break
			}
		}
		f.Close()
		resp.Body.Close()
		// stream ended (leader restart, network blip): reconnect from the
		// local file's size, which is exactly where we left the leader.
	}
}

// followerApplier replays everything in the local WAL that recovery has not
// yet applied, in timestamp order, through the shared apply path. The local
// file is the single source of truth: a line is applied exactly once, and a
// restart replays the whole file the normal way.
func (e *Engine) followerApplier() {
	tick := time.NewTicker(200 * time.Millisecond)
	defer tick.Stop()
	for range tick.C {
		e.applyLocalTail(e.bookGC.path, &e.replBookPos)
		e.applyLocalTail(e.holdGC.path, &e.replHoldPos)
	}
}

func (e *Engine) applyLocalTail(path string, pos *int64) {
	st, err := os.Stat(path)
	if err != nil || st.Size() <= *pos {
		return
	}
	f, err := os.Open(path)
	if err != nil {
		return
	}
	defer f.Close()
	if _, err := f.Seek(*pos, io.SeekStart); err != nil {
		return
	}
	var events []replayEvent
	newPos := *pos
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 1<<20), 1<<20)
	for sc.Scan() {
		if ev, ok := parseReplayEvent(sc.Bytes()); ok {
			events = append(events, ev)
		}
		newPos += int64(len(sc.Bytes())) + 1
	}
	if len(events) == 0 {
		*pos = newPos // skip unparsable bytes, never stall the applier
		return
	}
	sort.SliceStable(events, func(i, j int) bool { return events[i].ts < events[j].ts })
	n := len(events)
	applyReplayEvents(events, e.inv, e.res, e.wl, e.idem, e.holdTTL, &e.bid, &e.rid, &waitlistSeq)
	*pos = newPos
	atomic.AddInt64(&e.replicatedLines, int64(n))
}

// followerReject answers writes on a follower. Phase 1: a follower serves
// reads; writes come back with failover in phase 2.
func (e *Engine) followerReject(w http.ResponseWriter) bool {
	if e.replFrom == "" {
		return false
	}
	w.Header().Set("Retry-After", "5")
	w.WriteHeader(http.StatusServiceUnavailable)
	w.Write([]byte(`{"status":"follower","error":"this node is a replication follower; writes are served by the leader"}`))
	return true
}

func fileSize(path string) int64 {
	st, err := os.Stat(path)
	if err != nil {
		return 0
	}
	return st.Size()
}

