package postgres

import (
	"net/url"
	"strings"
	"testing"
)

const testDSN = "postgres://acme@localhost:5432/acme"

func TestWithSearchPathDefaultsToPublicViaOptions(t *testing.T) {
	t.Parallel()
	got, err := withSearchPath(testDSN)
	if err != nil {
		t.Fatalf("withSearchPath() error = %v", err)
	}
	u, err := url.Parse(got)
	if err != nil {
		t.Fatalf("parse result: %v", err)
	}
	// The schema must ride in the "options" startup field (PgBouncer forwards
	// it), never as a bare "search_path" startup parameter (PgBouncer rejects
	// it).
	if opts := u.Query().Get("options"); !strings.Contains(opts, "-c search_path=public") {
		t.Errorf("withSearchPath() options = %q, want it to contain -c search_path=public", opts)
	}
	if u.Query().Get("search_path") != "" {
		t.Errorf("withSearchPath() left a bare search_path query param: %q", got)
	}
}

func TestWithSearchPathFoldsSearchPathParamIntoOptions(t *testing.T) {
	t.Parallel()
	got, err := withSearchPath(testDSN + "?search_path=app")
	if err != nil {
		t.Fatalf("withSearchPath() error = %v", err)
	}
	u, err := url.Parse(got)
	if err != nil {
		t.Fatalf("parse result: %v", err)
	}
	if opts := u.Query().Get("options"); !strings.Contains(opts, "-c search_path=app") {
		t.Errorf("withSearchPath() options = %q, want it to honour search_path=app", opts)
	}
	if u.Query().Get("search_path") != "" {
		t.Errorf("withSearchPath() should move search_path into options, got %q", got)
	}
}

func TestWithSearchPathLeavesExistingOptionsSearchPathUntouched(t *testing.T) {
	t.Parallel()
	dsn := testDSN + "?options=" + url.QueryEscape("-c search_path=custom")
	got, err := withSearchPath(dsn)
	if err != nil {
		t.Fatalf("withSearchPath() error = %v", err)
	}
	u, err := url.Parse(got)
	if err != nil {
		t.Fatalf("parse result: %v", err)
	}
	if opts := u.Query().Get("options"); !strings.Contains(opts, "search_path=custom") || strings.Contains(opts, "public") {
		t.Errorf("withSearchPath() options = %q, want the caller's search_path=custom preserved", opts)
	}
}

func TestWithSearchPathPreservesExistingOptionsAndQuery(t *testing.T) {
	t.Parallel()
	dsn := testDSN + "?sslmode=disable&options=" + url.QueryEscape("-c statement_timeout=5000")
	got, err := withSearchPath(dsn)
	if err != nil {
		t.Fatalf("withSearchPath() error = %v", err)
	}
	u, err := url.Parse(got)
	if err != nil {
		t.Fatalf("parse result: %v", err)
	}
	if u.Query().Get("sslmode") != "disable" {
		t.Errorf("withSearchPath() dropped sslmode: %q", got)
	}
	opts := u.Query().Get("options")
	if !strings.Contains(opts, "statement_timeout=5000") || !strings.Contains(opts, "search_path=public") {
		t.Errorf("withSearchPath() options = %q, want both the existing option and search_path=public", opts)
	}
}

func TestWithSearchPathRejectsUnparseableDSN(t *testing.T) {
	t.Parallel()
	if _, err := withSearchPath("postgres://%zz"); err == nil {
		t.Fatal("withSearchPath() error = nil, want an error for an unparseable dsn")
	}
}
