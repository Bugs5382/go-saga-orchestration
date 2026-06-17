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
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/Bugs5382/go-saga-orchestration/domain"
	"github.com/Bugs5382/go-saga-orchestration/secrets"
)

func TestWebhookEmit_Sync_PostsAndReportsStatus(t *testing.T) {
	gotBody := ""
	gotSig := ""
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		gotBody = string(b)
		gotSig = r.Header.Get("X-Webhook-Sig")
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	v := WebhookEmitVerb{Secrets: secrets.NewMemory(map[string]string{"k": "supersecret"})}
	out, err := v.Execute(context.Background(), domain.SagaRun{}, domain.Step{
		Inputs: map[string]any{"url": srv.URL, "body": map[string]any{"event": "x"}, "secret_ref": "k"},
	})
	if err != nil {
		t.Fatalf("unexpected: %v", err)
	}
	if out["webhook_result_status"].(int64) != 200 {
		t.Errorf("status = %v, want 200", out["webhook_result_status"])
	}
	if !strings.Contains(gotBody, `"event":"x"`) {
		t.Errorf("body did not contain event=x: %q", gotBody)
	}
	if !strings.HasPrefix(gotSig, "sha256=") {
		t.Errorf("X-Webhook-Sig wrong: %q", gotSig)
	}
}

func TestWebhookEmit_Async_ReturnsImmediately(t *testing.T) {
	done := make(chan struct{}, 1)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusAccepted)
		done <- struct{}{}
	}))
	defer srv.Close()

	v := WebhookEmitVerb{Secrets: secrets.NewMemory(nil)}
	out, err := v.Execute(context.Background(), domain.SagaRun{}, domain.Step{
		Inputs: map[string]any{"url": srv.URL, "body": "x", "async": true},
	})
	if err != nil {
		t.Fatalf("unexpected: %v", err)
	}
	if out["webhook_result_async"] != true {
		t.Errorf("expected _async=true marker, got %+v", out)
	}
	<-done // ensure the goroutine fired the request
}

func TestWebhookEmit_MissingURL_Errors(t *testing.T) {
	v := WebhookEmitVerb{}
	_, err := v.Execute(context.Background(), domain.SagaRun{}, domain.Step{
		Inputs: map[string]any{"body": "x"},
	})
	if err == nil {
		t.Errorf("expected error for missing url")
	}
}
