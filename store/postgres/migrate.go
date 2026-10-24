package postgres

/*
MIT License

Copyright (c) 2026 Shane

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
	"embed"
	"errors"
	"fmt"
	"net/url"
	"strings"
	"time"

	"github.com/golang-migrate/migrate/v4"
	"github.com/golang-migrate/migrate/v4/database"
	_ "github.com/golang-migrate/migrate/v4/database/pgx/v5"
	"github.com/golang-migrate/migrate/v4/source/iofs"
	"github.com/jackc/pgx/v5"
)

// migrationsTable is the table Migrate records its state in. It is schema
// qualified so the connection's search_path never decides where it lives.
const migrationsTable = `"public"."schema_migrations"`

// adoptTimeout bounds the one-off move of a legacy migrations table.
const adoptTimeout = 30 * time.Second

//go:embed migrations/*.sql
var migrationsFS embed.FS

// Migrate applies every up-migration in store/postgres/migrations
// that has not yet been recorded in public.schema_migrations. Safe
// to call on every boot — no-ops if the schema is already at Head.
//
// The table is always public.schema_migrations, whatever the connection's
// search_path is. Earlier releases used an unqualified name, so the table
// followed search_path: with the default "$user", public it moved into a
// schema named after the role as soon as one existed, and every migration ran
// again. When public.schema_migrations is missing but an unqualified
// schema_migrations resolves on the search_path, Migrate first moves that
// table into public, so a database migrated that way keeps its state. A DSN
// that sets x-migrations-table itself keeps that table and skips the move.
//
// The migrations are embedded in the binary at build time so deploys do
// not need a sidecar migration job or an out-of-band run. cmd/api and
// cmd/engine each call this immediately after postgres.Open succeeds.
//
// DSN must use the pgx5 form, e.g.
// "postgres://user:pass@host:5432/db?sslmode=disable".
//
// Migrations require a direct (session) PostgreSQL connection and MUST NOT run
// through a transaction-pooled PgBouncer. golang-migrate relies on session
// advisory locks, SELECT CURRENT_SCHEMA(), and prepared statements, all of
// which transaction pooling breaks. Callers that route normal traffic through
// PgBouncer should point the migration DSN at the primary (or a session-mode
// port) instead.
func Migrate(dsn string) error {
	if dsn == "" {
		return fmt.Errorf("postgres.Migrate: DSN is empty")
	}
	src, err := iofs.New(migrationsFS, "migrations")
	if err != nil {
		return fmt.Errorf("postgres.Migrate: open source: %w", err)
	}
	migrateURL, pinned, err := migrationsURL(dsn)
	if err != nil {
		return fmt.Errorf("postgres.Migrate: %w", err)
	}
	if pinned {
		ctx, cancel := context.WithTimeout(context.Background(), adoptTimeout)
		defer cancel()
		if err := adoptMigrationsTable(ctx, dsn); err != nil {
			return fmt.Errorf("postgres.Migrate: %w", err)
		}
	}
	m, err := migrate.NewWithSourceInstance("iofs", src, migrateURL)
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

// migrationsURL builds the golang-migrate pgx5 URL for dsn with the
// migrations table pinned to migrationsTable. pinned is false when the DSN
// already names its own x-migrations-table, which is then left alone.
func migrationsURL(dsn string) (string, bool, error) {
	u, err := url.Parse("pgx5://" + stripScheme(dsn))
	if err != nil {
		return "", false, fmt.Errorf("parse DSN: %w", err)
	}
	q := u.Query()
	if q.Get("x-migrations-table") != "" {
		return u.String(), false, nil
	}
	q.Set("x-migrations-table", migrationsTable)
	q.Set("x-migrations-table-quoted", "1")
	u.RawQuery = q.Encode()
	return u.String(), true, nil
}

// adoptLegacyMigrationsTable moves a schema_migrations table that an older
// release created on the search_path into public, unless public already has
// one. It runs under golang-migrate's advisory lock for the pinned table, so
// it never races a concurrent Migrate.
const adoptLegacyMigrationsTable = `
DO $$
DECLARE
	legacy regclass;
BEGIN
	IF to_regclass('public.schema_migrations') IS NULL THEN
		legacy := to_regclass('schema_migrations');
		IF legacy IS NOT NULL THEN
			EXECUTE format('ALTER TABLE %s SET SCHEMA public', legacy);
		END IF;
	END IF;
END
$$`

// adoptMigrationsTable runs adoptLegacyMigrationsTable on its own connection.
func adoptMigrationsTable(ctx context.Context, dsn string) error {
	cfg, err := pgx.ParseConfig(connDSN(dsn))
	if err != nil {
		return fmt.Errorf("parse DSN: %w", err)
	}
	conn, err := pgx.ConnectConfig(ctx, cfg)
	if err != nil {
		return fmt.Errorf("connect: %w", err)
	}
	defer func() { _ = conn.Close(context.Background()) }()

	var dbName string
	if err := conn.QueryRow(ctx, "SELECT current_database()").Scan(&dbName); err != nil {
		return fmt.Errorf("current database: %w", err)
	}
	lockID, err := database.GenerateAdvisoryLockId(dbName, "public", "schema_migrations")
	if err != nil {
		return fmt.Errorf("advisory lock id: %w", err)
	}
	return pgx.BeginFunc(ctx, conn, func(tx pgx.Tx) error {
		if _, err := tx.Exec(ctx, "SELECT pg_advisory_xact_lock($1)", lockID); err != nil {
			return fmt.Errorf("lock migrations table: %w", err)
		}
		if _, err := tx.Exec(ctx, adoptLegacyMigrationsTable); err != nil {
			return fmt.Errorf("adopt legacy schema_migrations: %w", err)
		}
		return nil
	})
}

// connDSN turns dsn into a postgres:// URL pgx can dial, dropping the x-
// options that only golang-migrate understands.
func connDSN(dsn string) string {
	u, err := url.Parse("postgres://" + stripScheme(dsn))
	if err != nil {
		return dsn
	}
	q := u.Query()
	for k := range q {
		if strings.HasPrefix(k, "x-") {
			q.Del(k)
		}
	}
	u.RawQuery = q.Encode()
	return u.String()
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
