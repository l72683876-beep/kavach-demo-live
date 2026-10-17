//go:build postgres

package main

// Postgres write-behind. The append-only local logs stay the source of truth:
// the engine acknowledges a booking after the LOCAL log is durable, exactly as
// before. This sink tails those files and ships every line to Postgres in the
// background, with a per-node cursor in the database itself. A crash between
// insert and cursor update re-inserts the same lines, and the primary key
// turns that into a no-op: at-least-once shipping that lands exactly-once.
//
// Postgres never sits in the request path. If it is slow or down, the engine
// keeps serving and the tailer retries until the connection comes back.

import (
	"context"
	"io"
	"os"
	"path/filepath"
	"sync/atomic"
	"time"

	"github.com/jackc/pgx/v5"
)

type PgSink struct {
	nodeID  string
	shipped *int64
	failed  *int64
	ctx     context.Context
	cancel  context.CancelFunc
}

// NewPgSink connects, creates the schema, resumes every file from its stored
// cursor, and starts a tailer per file. files are the log paths to ship.
func NewPgSink(dsn, nodeID string, files []string, shipped, failed *int64) (*PgSink, error) {
	ctx, cancel := context.WithCancel(context.Background())
	s := &PgSink{nodeID: nodeID, shipped: shipped, failed: failed, ctx: ctx, cancel: cancel}
	conn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		cancel()
		return nil, err
	}
	if err := s.ensureSchema(ctx, conn); err != nil {
		conn.Close(context.Background())
		cancel()
		return nil, err
	}
	for _, f := range files {
		off, err := s.loadCursor(ctx, conn, f)
		if err != nil {
			conn.Close(context.Background())
			cancel()
			return nil, err
		}
		go s.tailLoop(dsn, f, off)
	}
	go func() {
		<-ctx.Done()
		conn.Close(context.Background())
	}()
	return s, nil
}

func (s *PgSink) ensureSchema(ctx context.Context, conn *pgx.Conn) error {
	if _, err := conn.Exec(ctx, `CREATE TABLE IF NOT EXISTS kavach_events (
		node_id   text        NOT NULL,
		source    text        NOT NULL,
		line_off  bigint      NOT NULL,
		logged_at timestamptz NOT NULL DEFAULT now(),
		data      jsonb       NOT NULL,
		PRIMARY KEY (node_id, source, line_off))`); err != nil {
		return err
	}
	if _, err := conn.Exec(ctx, `CREATE TABLE IF NOT EXISTS kavach_ship_state (
		node_id     text   NOT NULL,
		source      text   NOT NULL,
		byte_offset bigint NOT NULL,
		PRIMARY KEY (node_id, source))`); err != nil {
		return err
	}
	if _, err := conn.Exec(ctx, `CREATE INDEX IF NOT EXISTS kavach_events_data ON kavach_events USING gin (data)`); err != nil {
		return err
	}
	return nil
}

func (s *PgSink) loadCursor(ctx context.Context, conn *pgx.Conn, path string) (int64, error) {
	var off int64
	err := conn.QueryRow(ctx, `SELECT byte_offset FROM kavach_ship_state WHERE node_id=$1 AND source=$2`,
		s.nodeID, filepath.Base(path)).Scan(&off)
	if err == pgx.ErrNoRows {
		return 0, nil // first boot against this database: ship the full history
	}
	return off, err
}

// tailLoop owns one connection per file and reconnects through failures.
func (s *PgSink) tailLoop(dsn, path string, off int64) {
	var conn *pgx.Conn
	for {
		if s.ctx.Err() != nil {
			return
		}
		if conn == nil {
			c, err := pgx.Connect(s.ctx, dsn)
			if err != nil {
				atomic.AddInt64(s.failed, 1)
				time.Sleep(2 * time.Second)
				continue
			}
			conn = c
		}
		newOff, dead := s.pump(conn, path, off)
		if dead {
			conn.Close(context.Background())
			conn = nil
			continue
		}
		off = newOff
		time.Sleep(200 * time.Millisecond)
	}
}

// pump ships every COMPLETE line appended since off, then advances the cursor.
func (s *PgSink) pump(conn *pgx.Conn, path string, off int64) (int64, bool) {
	f, err := os.Open(path)
	if err != nil {
		return off, false // file not created yet: nothing to ship
	}
	defer f.Close()
	st, err := f.Stat()
	if err != nil || st.Size() <= off {
		return off, false
	}
	buf := make([]byte, st.Size()-off)
	if _, err := io.ReadFull(f, buf); err != nil {
		return off, false
	}
	var lines []string
	var offs []int64
	consumed := off
	start := 0
	for i := 0; i < len(buf); i++ {
		if buf[i] == '\n' {
			lines = append(lines, string(buf[start:i]))
			offs = append(offs, off+int64(start))
			consumed = off + int64(i+1)
			start = i + 1
		}
	}
	if len(lines) == 0 {
		return off, false // a half-written tail line waits for its newline
	}
	ctx, cancel := context.WithTimeout(s.ctx, 15*time.Second)
	defer cancel()
	batch := &pgx.Batch{}
	for i := range lines {
		batch.Queue(`INSERT INTO kavach_events (node_id, source, line_off, data)
			VALUES ($1,$2,$3,$4) ON CONFLICT DO NOTHING`,
			s.nodeID, filepath.Base(path), offs[i], lines[i])
	}
	batch.Queue(`INSERT INTO kavach_ship_state (node_id, source, byte_offset)
		VALUES ($1,$2,$3)
		ON CONFLICT (node_id, source) DO UPDATE SET byte_offset = EXCLUDED.byte_offset`,
		s.nodeID, filepath.Base(path), consumed)
	if err := conn.SendBatch(ctx, batch).Close(); err != nil {
		atomic.AddInt64(s.failed, 1)
		return off, true // reconnect and retry the same bytes
	}
	atomic.AddInt64(s.shipped, int64(len(lines)))
	return consumed, false
}
