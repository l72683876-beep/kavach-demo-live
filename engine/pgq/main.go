// pgq is a tiny SQL helper for inspecting the Postgres write-behind sink:
//
//	go run ./pgq 'SELECT count(*) FROM kavach_events'
//
// It exists so you never need a separate psql install to check Kavach's data.
package main

import (
	"context"
	"fmt"
	"os"

	"github.com/jackc/pgx/v5"
)

func main() {
	if len(os.Args) < 2 {
		fmt.Fprintln(os.Stderr, "usage: go run ./pgq 'SQL'")
		os.Exit(2)
	}
	dsn := os.Getenv("KAVACH_PG_DSN")
	if dsn == "" {
		dsn = "postgres://kavach@127.0.0.1:5432/postgres?sslmode=disable"
	}
	ctx := context.Background()
	conn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		fmt.Fprintln(os.Stderr, "connect:", err)
		os.Exit(1)
	}
	defer conn.Close(ctx)
	rows, err := conn.Query(ctx, os.Args[1])
	if err != nil {
		fmt.Fprintln(os.Stderr, "query:", err)
		os.Exit(1)
	}
	defer rows.Close()
	fields := rows.FieldDescriptions()
	for rows.Next() {
		vals, err := rows.Values()
		if err != nil {
			fmt.Fprintln(os.Stderr, "scan:", err)
			os.Exit(1)
		}
		for i, v := range vals {
			if i > 0 {
				fmt.Print(" | ")
			}
			fmt.Printf("%v", v)
		}
		_ = fields
		fmt.Println()
	}
}
