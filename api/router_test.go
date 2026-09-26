package api

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
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
)

// Client-supplied forwarding headers must not rewrite the request's remote
// address. chi's RealIP did exactly that (GHSA-3fxj-6jh8-hvhx and related).
func TestNewRouter_IgnoresSpoofedForwardingHeaders(t *testing.T) {
	r := NewRouter(nil, nil, nil, nil, nil, nil, nil, nil, nil, nil)
	r.Get("/test/remote-addr", func(w http.ResponseWriter, req *http.Request) {
		_, _ = io.WriteString(w, req.RemoteAddr)
	})

	req := httptest.NewRequest(http.MethodGet, "/test/remote-addr", nil)
	req.RemoteAddr = "192.0.2.10:4242"
	req.Header.Set("X-Forwarded-For", "203.0.113.99")
	req.Header.Set("X-Real-IP", "203.0.113.99")
	req.Header.Set("True-Client-IP", "203.0.113.99")
	rec := httptest.NewRecorder()
	r.ServeHTTP(rec, req)

	if got := rec.Body.String(); got != "192.0.2.10:4242" {
		t.Fatalf("RemoteAddr = %q, want the connection address 192.0.2.10:4242", got)
	}
}
