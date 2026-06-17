package verbs

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
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/Bugs5382/go-saga-orchestration/domain"
	"github.com/Bugs5382/go-saga-orchestration/secrets"
)

func TestHTTPRequest_GET_JSONBody(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{"ok": true, "count": 3})
	}))
	defer srv.Close()

	v := HTTPRequestVerb{Secrets: secrets.NewMemory(nil)}
	out, err := v.Execute(context.Background(), domain.SagaRun{}, domain.Step{
		Inputs: map[string]any{"url": srv.URL},
	})
	if err != nil {
		t.Fatalf("unexpected: %v", err)
	}
	if out["http_result_status"].(int64) != 200 {
		t.Errorf("status = %v, want 200", out["http_result_status"])
	}
	body, ok := out["http_result"].(map[string]any)
	if !ok {
		t.Fatalf("body not map: %T", out["http_result"])
	}
	if body["ok"] != true {
		t.Errorf("body.ok = %v, want true", body["ok"])
	}
}

func TestHTTPRequest_MissingURL_Errors(t *testing.T) {
	v := HTTPRequestVerb{Secrets: secrets.NewMemory(nil)}
	_, err := v.Execute(context.Background(), domain.SagaRun{}, domain.Step{Inputs: map[string]any{}})
	if err == nil {
		t.Errorf("expected error for missing url")
	}
}

func TestHTTPRequest_SecretAuth(t *testing.T) {
	gotAuth := ""
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotAuth = r.Header.Get("Authorization")
		w.WriteHeader(http.StatusNoContent)
	}))
	defer srv.Close()

	sec := secrets.NewMemory(map[string]string{"my-token": "Bearer abc123"})
	v := HTTPRequestVerb{Secrets: sec}
	_, err := v.Execute(context.Background(), domain.SagaRun{}, domain.Step{
		Inputs: map[string]any{"url": srv.URL, "secret_ref": "my-token"},
	})
	if err != nil {
		t.Fatalf("unexpected: %v", err)
	}
	if gotAuth != "Bearer abc123" {
		t.Errorf("Authorization header = %q, want %q", gotAuth, "Bearer abc123")
	}
}
