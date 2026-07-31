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
