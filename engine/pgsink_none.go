//go:build !postgres

package main

import "errors"

// PgSink is the no-op stand-in compiled when the engine is built without
// the postgres build tag. The default build stays dependency-free.
type PgSink struct{}

// NewPgSink always refuses: rebuild with `go build -tags postgres` to enable
// the Postgres write-behind sink.
func NewPgSink(dsn, nodeID string, files []string, shipped, failed *int64) (*PgSink, error) {
	return nil, errors.New("this binary was built without Postgres support; rebuild with: go build -tags postgres")
}
