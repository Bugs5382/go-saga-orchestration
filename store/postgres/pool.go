// Package postgres is the production Store implementation. It uses pgx
// for connection pooling and golang-migrate to apply schema migrations.
// Both cmd/api and cmd/engine call postgres.Migrate(dsn) immediately
// after postgres.Open succeeds to apply any pending migrations at boot.
package postgres

/*
MIT License

Copyright (c) 2026 Bugs5382

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE USE OR PERFORMANCE OF THIS SOFTWARE.
*/

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/Bugs5382/go-saga-orchestration/store"
)

// Verify postgres.Store satisfies the store.Store interface.
var _ store.Store = (*Store)(nil)

// Store wraps a pgxpool.Pool and implements store.Store.
type Store struct {
	pool *pgxpool.Pool
}

// Open dials Postgres using the supplied DSN. Caller MUST defer Close().
func Open(ctx context.Context, dsn string) (*Store, error) {
	if dsn == "" {
		return nil, fmt.Errorf("postgres: DSN is empty")
	}
	cfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		return nil, fmt.Errorf("postgres: parse DSN: %w", err)
	}
	cfg.MaxConns = 20
	cfg.MinConns = 2
	cfg.MaxConnLifetime = 30 * time.Minute
	cfg.MaxConnIdleTime = 5 * time.Minute

	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return nil, fmt.Errorf("postgres: connect: %w", err)
	}
	pingCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	if err := pool.Ping(pingCtx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("postgres: ping: %w", err)
	}
	return &Store{pool: pool}, nil
}

// Close releases the pool. Safe to call once.
func (s *Store) Close() {
	if s.pool != nil {
		s.pool.Close()
	}
}

// Pool exposes the underlying pgxpool for migrations + tests.
func (s *Store) Pool() *pgxpool.Pool { return s.pool }
