package main

// Multi-shard (phase 2b): one process can host N independent inventories,
// each its own single-writer engine with its own WAL, snapshot directory and
// worker pool. A request names its event with "event_id" (JSON body field)
// or "event" (query parameter); a stable FNV-1a hash of that key picks the
// shard, so every operation on one inventory still runs on exactly one
// writer and none of the per-inventory guarantees change.
//
// Shard 0 with -shards=1 is byte-for-byte today's engine: the same log
// paths, the same response shapes, the same idempotency behavior.

import (
	"bytes"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io"
	"net/http"
	"regexp"
	"strings"
)

const defaultEventID = "default"

// eventIDRe bounds the event key: printable URL-safe characters, 128 chars.
// A bad key is a bad_request, never a silent default.
var eventIDRe = regexp.MustCompile(`^[A-Za-z0-9._:-]{1,128}$`)

// ShardOf maps an event key to a shard index. A pure function, stable
// across restarts: the same event always lands on the same shard.
func ShardOf(event string, n int) int {
	if n <= 1 {
		return 0
	}
	h := fnv.New32a()
	_, _ = h.Write([]byte(event))
	return int(h.Sum32() % uint32(n))
}

// ShardRouter owns the shard engines and routes every client request to the
// one that owns its event.
type ShardRouter struct {
	shards []*Engine
}

func NewShardRouter(shards []*Engine) *ShardRouter { return &ShardRouter{shards: shards} }

func (sr *ShardRouter) pick(ev string) *Engine {
	return sr.shards[ShardOf(ev, len(sr.shards))]
}

// requestEventID extracts the event key: the "event" query parameter wins,
// then the "event_id" body field. The body is read and restored whole when
// fromBody is set, because the HMAC signature covers the exact body bytes
// and the handler underneath must see them unchanged. The second return is
// false when the key is present but malformed.
func requestEventID(r *http.Request, fromBody bool) (string, bool) {
	ev := r.URL.Query().Get("event")
	if ev == "" && fromBody && r.Body != nil {
		body := readBodyBounded(r)
		if len(body) > 0 {
			var peek struct {
				EventID string `json:"event_id"`
			}
			_ = json.Unmarshal(body, &peek)
			if peek.EventID != "" {
				ev = peek.EventID
			}
		}
		r.Body = io.NopCloser(strings.NewReader(string(body)))
	}
	return ev, ev == "" || eventIDRe.MatchString(ev)
}

// eventOf returns the event key the request named: the "event" query
// parameter wins, then the "event_id" body field. It mirrors requestEventID,
// so idempotency fingerprints and shard routing always agree: the same
// logical event fingerprints the same, whichever way the client named it.
func eventOf(r *http.Request, bodyEvent string) string {
	if ev := r.URL.Query().Get("event"); ev != "" {
		return ev
	}
	return bodyEvent
}

// fpFor builds an idempotency fingerprint: key, user, seats AND the event the
// request named. Binding the event closes cross-event replay: the same key,
// user and seats on a DIFFERENT event is a conflict, never a silent replay of
// the first event's booking.
func fpFor(key, user string, seats int, ev string) string {
	if ev == "" {
		ev = defaultEventID
	}
	return fmt.Sprintf("%x", sha256.Sum256([]byte(fmt.Sprintf("%s|%s|%d|%s", key, user, seats, ev))))
}

// readBodyBounded reads the request body once, bounded to a 16 KiB+1 routing
// window, and leaves it restorable for the handler underneath. An oversized
// body is restored as its own truncated prefix so the handler's
// MaxBytesReader still rejects it with the same 400 it always returned;
// restoring nothing would make the handler see an empty body and reject the
// signature with 401 instead of the size, which is the wrong denial.
func readBodyBounded(r *http.Request) []byte {
	body, err := io.ReadAll(io.LimitReader(r.Body, 16385))
	r.Body.Close()
	if err != nil && len(body) == 0 {
		return nil // a body that never produced bytes: the handler's read fails the same way
	}
	return body
}

// stripProbeHeaders removes headers a client must never be able to set to
// influence internal routing or rate-limit accounting.
func stripProbeHeaders(r *http.Request) {
	r.Header.Del("X-Kavach-Shard-Probe")
}

// route wraps a shard handler with event routing.
func (sr *ShardRouter) route(h func(*Engine, http.ResponseWriter, *http.Request), fromBody bool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		stripProbeHeaders(r)
		ev, ok := requestEventID(r, fromBody)
		if !ok {
			writeJSON(w, http.StatusBadRequest, Result{Status: "bad_request"})
			return
		}
		if ev == "" {
			ev = defaultEventID
		}
		h(sr.pick(ev), w, r)
	}
}

// probeWriter captures one handler's response so a scan across shards can
// replay the winning answer to the client. Nothing a probe writes reaches
// the client unless its shard hits.
type probeWriter struct {
	hdr    http.Header
	status int
	body   bytes.Buffer
}

func newProbeWriter() *probeWriter { return &probeWriter{hdr: make(http.Header)} }

func (p *probeWriter) Header() http.Header { return p.hdr }

func (p *probeWriter) Write(b []byte) (int, error) {
	if p.status == 0 {
		p.status = http.StatusOK
	}
	return p.body.Write(b)
}

func (p *probeWriter) WriteHeader(status int) {
	if p.status == 0 {
		p.status = status
	}
}

func (p *probeWriter) replay(w http.ResponseWriter) {
	for k, vs := range p.hdr {
		for _, v := range vs {
			w.Header().Add(k, v)
		}
	}
	if p.status != 0 {
		w.WriteHeader(p.status)
	}
	_, _ = w.Write(p.body.Bytes())
}

// scan serves an endpoint whose identifier (a reservation or waitlist id)
// does not name its shard: try the shard the request names, else try every
// shard and replay the first answer that is not not_found. Only the shard
// actually holding the id performs any work; the rest answer 404 without
// touching state.
func (sr *ShardRouter) scan(h func(*Engine, http.ResponseWriter, *http.Request), fromBody bool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		stripProbeHeaders(r)
		if len(sr.shards) == 1 {
			h(sr.shards[0], w, r)
			return
		}
		if ev := r.URL.Query().Get("event"); ev != "" {
			if !eventIDRe.MatchString(ev) {
				writeJSON(w, http.StatusBadRequest, Result{Status: "bad_request"})
				return
			}
			h(sr.pick(ev), w, r)
			return
		}
		if fromBody {
			body := readBodyBounded(r)
			var peek struct {
				EventID string `json:"event_id"`
			}
			_ = json.Unmarshal(body, &peek)
			r.Body = io.NopCloser(strings.NewReader(string(body)))
			if peek.EventID != "" {
				if !eventIDRe.MatchString(peek.EventID) {
					writeJSON(w, http.StatusBadRequest, Result{Status: "bad_request"})
					return
				}
				h(sr.pick(peek.EventID), w, r)
				return
			}
		}
		for _, e := range sr.shards {
			p := newProbeWriter()
			r.Header.Set("X-Kavach-Shard-Probe", "1")
			h(e, p, r)
			if p.status != http.StatusNotFound {
				p.replay(w)
				return
			}
		}
		writeJSON(w, http.StatusNotFound, Result{Status: "not_found"})
	}
}

// metricSumKeys are the /metrics counters that sum across shards.
var metricSumKeys = []string{
	"received", "rate_limited", "ip_rate_limited", "user_rate_limited",
	"auth_failures", "replays_rejected", "idem_conflicts", "overloaded",
	"in_flight", "confirmed", "sold_out", "errors",
	"holds_created", "holds_confirmed", "holds_released", "holds_expired",
	"waitlist_queued", "waitlist_fulfilled", "waitlist_cancelled",
	"webhook_sent", "webhook_failed", "forwarded",
	"replication_streams", "replicated_lines",
	"pg_shipped", "pg_failed", "chaos_stalls", "chaos_panics",
	"snapshots_total", "recovery_replayed_lines",
}

// handleMetrics: one shard returns today's exact shape; several return the
// counters summed across shards plus a per-shard breakdown (latency
// percentiles are per shard, because percentiles do not sum).
func (sr *ShardRouter) handleMetrics(w http.ResponseWriter, r *http.Request) {
	if len(sr.shards) == 1 {
		sr.shards[0].handleMetrics(w, r)
		return
	}
	views := make([]map[string]interface{}, 0, len(sr.shards))
	for _, e := range sr.shards {
		views = append(views, e.metricsMap())
	}
	sum := make(map[string]interface{}, len(metricSumKeys)+3)
	for _, k := range metricSumKeys {
		var t int64
		for _, v := range views {
			switch n := v[k].(type) {
			case int64:
				t += n
			case int:
				t += int64(n)
			}
		}
		sum[k] = t
	}
	sum["role"] = "leader"
	sum["shards"] = len(sr.shards)
	sum["shard_metrics"] = views
	writeJSON(w, http.StatusOK, sum)
}

// handleWaitlist: POST joins (routed by event), GET lists the caller's
// entries across every shard.
func (sr *ShardRouter) handleWaitlist(w http.ResponseWriter, r *http.Request) {
	stripProbeHeaders(r)
	if r.Method != http.MethodGet {
		sr.route((*Engine).handleWaitlistJoin, true)(w, r)
		return
	}
	if len(sr.shards) == 1 {
		sr.shards[0].handleWaitlistList(w, r)
		return
	}
	_, st, msg := sr.shards[0].auth.verify(r, nil) // auth is shared; each shard re-verifies in its handler
	if st != 0 {
		writeJSON(w, st, Result{Status: msg})
		return
	}
	user := r.URL.Query().Get("user_id")
	if user == "" {
		writeJSON(w, http.StatusBadRequest, Result{Status: "bad_request"})
		return
	}
	type row struct {
		WaitlistID string `json:"waitlist_id"`
		Seats      int    `json:"seats"`
		Position   int    `json:"position"`
	}
	rows := make([]row, 0, 8)
	for _, e := range sr.shards {
		p := newProbeWriter()
		e.handleWaitlistList(p, r)
		if p.status != http.StatusOK {
			continue
		}
		var out struct {
			Entries []row `json:"entries"`
		}
		_ = json.Unmarshal(p.body.Bytes(), &out)
		rows = append(rows, out.Entries...)
	}
	writeJSON(w, http.StatusOK, map[string]interface{}{"status": "ok", "entries": rows})
}
