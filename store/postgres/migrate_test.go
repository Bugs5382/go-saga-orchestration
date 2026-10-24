package postgres_test

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
	"net/url"
	"os"
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/Bugs5382/go-saga-orchestration/store/postgres"
)

// migrateTestDB creates an empty database on the server TEST_POSTGRES_DSN
// points at, drops it when the test ends, and returns a connection to it and
// its DSN.
func migrateTestDB(t *testing.T) (*pgx.Conn, string) {
	t.Helper()
	dsn := os.Getenv("TEST_POSTGRES_DSN")
	if dsn == "" {
		t.Skip("set TEST_POSTGRES_DSN to run postgres migration tests")
	}
	ctx := context.Background()
	admin, err := pgx.Connect(ctx, dsn)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	t.Cleanup(func() { _ = admin.Close(context.Background()) })

	name := "saga_migrate_" + strings.ReplaceAll(uuid.NewString(), "-", "")
	if _, err := admin.Exec(ctx, "CREATE DATABASE "+name); err != nil {
		t.Fatalf("create database: %v", err)
	}
	t.Cleanup(func() {
		_, _ = admin.Exec(context.Background(), "DROP DATABASE IF EXISTS "+name+" WITH (FORCE)")
	})

	u, err := url.Parse(dsn)
	if err != nil {
		t.Fatalf("parse TEST_POSTGRES_DSN (a postgres:// URL is required): %v", err)
	}
	u.Path = "/" + name
	conn, err := pgx.Connect(ctx, u.String())
	if err != nil {
		t.Fatalf("connect to test database: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close(context.Background()) })
	return conn, u.String()
}

// createRoleSchema creates a schema named after the connected role, so the
// default search_path ("$user", public) resolves unqualified names there first.
func createRoleSchema(t *testing.T, conn *pgx.Conn) string {
	t.Helper()
	var role string
	if err := conn.QueryRow(context.Background(), "SELECT current_user").Scan(&role); err != nil {
		t.Fatalf("current_user: %v", err)
	}
	if _, err := conn.Exec(context.Background(), "CREATE SCHEMA "+pgx.Identifier{role}.Sanitize()); err != nil {
		t.Fatalf("create schema %s: %v", role, err)
	}
	return role
}

// migrationTables lists the schemas that hold a schema_migrations table.
func migrationTables(t *testing.T, conn *pgx.Conn) []string {
	t.Helper()
	rows, err := conn.Query(context.Background(),
		"SELECT table_schema FROM information_schema.tables WHERE table_name = 'schema_migrations' ORDER BY table_schema")
	if err != nil {
		t.Fatalf("list schema_migrations tables: %v", err)
	}
	schemas, err := pgx.CollectRows(rows, pgx.RowTo[string])
	if err != nil {
		t.Fatalf("collect schema_migrations tables: %v", err)
	}
	return schemas
}

// publicVersion reads the version recorded in public.schema_migrations.
func publicVersion(t *testing.T, conn *pgx.Conn) (int64, bool) {
	t.Helper()
	var (
		version int64
		dirty   bool
	)
	if err := conn.QueryRow(context.Background(),
		"SELECT version, dirty FROM public.schema_migrations").Scan(&version, &dirty); err != nil {
		t.Fatalf("read public.schema_migrations: %v", err)
	}
	return version, dirty
}

// TestMigrate_IgnoresRoleSchemaOnSearchPath runs Migrate, then creates a
// schema named after the role so search_path resolves "$user" first. The
// second Migrate must be a no-op that still reads public.schema_migrations.
func TestMigrate_IgnoresRoleSchemaOnSearchPath(t *testing.T) {
	conn, dsn := migrateTestDB(t)

	if err := postgres.Migrate(dsn); err != nil {
		t.Fatalf("first Migrate: %v", err)
	}
	want, dirty := publicVersion(t, conn)
	if dirty || want == 0 {
		t.Fatalf("after first Migrate: version %d dirty %v, want a clean head version", want, dirty)
	}

	createRoleSchema(t, conn)

	if err := postgres.Migrate(dsn); err != nil {
		t.Fatalf("second Migrate: %v", err)
	}
	if got := migrationTables(t, conn); len(got) != 1 || got[0] != "public" {
		t.Errorf("schema_migrations tables in %v, want only public", got)
	}
	if got, dirty := publicVersion(t, conn); got != want || dirty {
		t.Errorf("after second Migrate: version %d dirty %v, want %d clean", got, dirty, want)
	}
}

// TestMigrate_RoleSchemaBeforeFirstRun covers a fresh database where the
// role's schema already exists: the migration state still lands in public.
func TestMigrate_RoleSchemaBeforeFirstRun(t *testing.T) {
	conn, dsn := migrateTestDB(t)
	createRoleSchema(t, conn)

	if err := postgres.Migrate(dsn); err != nil {
		t.Fatalf("Migrate: %v", err)
	}
	if got := migrationTables(t, conn); len(got) != 1 || got[0] != "public" {
		t.Errorf("schema_migrations tables in %v, want only public", got)
	}
	if err := postgres.Migrate(dsn); err != nil {
		t.Fatalf("second Migrate: %v", err)
	}
}

// TestMigrate_AdoptsTableFromSearchPath covers a database migrated before the
// table was pinned, while a schema named after the role already existed: its
// state sits in that schema. Migrate moves it to public and applies nothing.
func TestMigrate_AdoptsTableFromSearchPath(t *testing.T) {
	conn, dsn := migrateTestDB(t)
	ctx := context.Background()

	if err := postgres.Migrate(dsn); err != nil {
		t.Fatalf("first Migrate: %v", err)
	}
	want, _ := publicVersion(t, conn)
	role := createRoleSchema(t, conn)
	if _, err := conn.Exec(ctx, "ALTER TABLE public.schema_migrations SET SCHEMA "+pgx.Identifier{role}.Sanitize()); err != nil {
		t.Fatalf("move schema_migrations to %s: %v", role, err)
	}

	if err := postgres.Migrate(dsn); err != nil {
		t.Fatalf("Migrate on the old layout: %v", err)
	}
	if got := migrationTables(t, conn); len(got) != 1 || got[0] != "public" {
		t.Errorf("schema_migrations tables in %v, want only public", got)
	}
	if got, dirty := publicVersion(t, conn); got != want || dirty {
		t.Errorf("version %d dirty %v, want %d clean", got, dirty, want)
	}
}
