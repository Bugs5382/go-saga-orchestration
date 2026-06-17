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
	"embed"
	"errors"
	"fmt"

	"github.com/golang-migrate/migrate/v4"
	_ "github.com/golang-migrate/migrate/v4/database/pgx/v5"
	"github.com/golang-migrate/migrate/v4/source/iofs"
)

//go:embed migrations/*.sql
var migrationsFS embed.FS

// Migrate applies every up-migration in store/postgres/migrations
// that has not yet been recorded in the schema_migrations table. Safe to
// call on every boot — no-ops if the schema is already at Head.
//
// The migrations are embedded in the binary at build time so deploys do
// not need a sidecar migration job or an out-of-band run. cmd/api and
// cmd/engine each call this immediately after postgres.Open succeeds.
//
// DSN must use the pgx5 form, e.g.
// "postgres://user:pass@host:5432/db?sslmode=disable".
func Migrate(dsn string) error {
	if dsn == "" {
		return fmt.Errorf("postgres.Migrate: DSN is empty")
	}
	src, err := iofs.New(migrationsFS, "migrations")
	if err != nil {
		return fmt.Errorf("postgres.Migrate: open source: %w", err)
	}
	m, err := migrate.NewWithSourceInstance("iofs", src, "pgx5://"+stripScheme(dsn))
	if err != nil {
		return fmt.Errorf("postgres.Migrate: new migrate: %w", err)
	}
	if err := m.Up(); err != nil && !errors.Is(err, migrate.ErrNoChange) {
		return fmt.Errorf("postgres.Migrate: up: %w", err)
	}
	// m.Close returns two errors (source + db); ignore — both are nil if Up succeeded.
	_, _ = m.Close()
	return nil
}

// stripScheme converts "postgres://..." or "postgresql://..." to "..."
// so the caller can prefix "pgx5://" for golang-migrate's pgx5 driver.
// Returns dsn unchanged if no scheme is present.
func stripScheme(dsn string) string {
	for _, p := range []string{"postgres://", "postgresql://"} {
		if len(dsn) > len(p) && dsn[:len(p)] == p {
			return dsn[len(p):]
		}
	}
	return dsn
}
