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
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/Bugs5382/go-saga-orchestration/domain"
	"github.com/Bugs5382/go-saga-orchestration/secrets"
)

// WebhookEmitVerb POSTs a payload to an external URL. Inputs:
//   - "url"        (required, string)
//   - "body"       (required, any; JSON-marshalled into the request body)
//   - "secret_ref" (optional, string; resolves to an HMAC-SHA256 signing key.
//     When present, X-Webhook-Sig header = "sha256=<hex>" of the body using the key.)
//   - "timeout_s"  (optional, number; default 15s)
//   - "headers"    (optional, map[string]any → string headers)
//   - "async"      (optional, bool; default false. When true: fire request
//     in a goroutine, return immediately without awaiting response. Failures
//     are logged but not surfaced. Use for fire-and-forget notifications.)
//   - "out_var"    (optional, string; default "webhook_result"). Only populated
//     on synchronous (non-async) success: {out_var}_status = int64 code.
type WebhookEmitVerb struct {
	Secrets secrets.Resolver
	Client  *http.Client
}

// Execute POSTs the JSON-marshalled body to the URL (optionally HMAC-signed).
// In async mode it fires the request in a goroutine and returns immediately;
// otherwise it awaits the response and returns the status code under out_var.
func (v WebhookEmitVerb) Execute(ctx context.Context, run domain.SagaRun, step domain.Step) (map[string]any, error) {
	url, _ := step.Inputs["url"].(string)
	if url == "" {
		return nil, fmt.Errorf("webhook_emit: url required")
	}
	bodyAny, ok := step.Inputs["body"]
	if !ok {
		return nil, fmt.Errorf("webhook_emit: body required")
	}
	bs, err := json.Marshal(bodyAny)
	if err != nil {
		return nil, fmt.Errorf("webhook_emit: marshal body: %w", err)
	}
	timeoutS := 15.0
	if t, ok := step.Inputs["timeout_s"].(float64); ok {
		timeoutS = t
	}
	async, _ := step.Inputs["async"].(bool)
	outVar, _ := step.Inputs["out_var"].(string)
	if outVar == "" {
		outVar = "webhook_result"
	}

	req, err := http.NewRequestWithContext(ctx, "POST", url, bytes.NewReader(bs))
	if err != nil {
		return nil, fmt.Errorf("webhook_emit: build: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	if hdrs, ok := step.Inputs["headers"].(map[string]any); ok {
		for k, val := range hdrs {
			req.Header.Set(k, fmt.Sprint(val))
		}
	}
	if ref, ok := step.Inputs["secret_ref"].(string); ok && ref != "" {
		if v.Secrets == nil {
			return nil, fmt.Errorf("webhook_emit: secret_ref %q but no Secrets resolver", ref)
		}
		key, err := v.Secrets.Get(ref)
		if err != nil {
			return nil, fmt.Errorf("webhook_emit: secret: %w", err)
		}
		mac := hmac.New(sha256.New, []byte(key))
		mac.Write(bs)
		req.Header.Set("X-Webhook-Sig", "sha256="+hex.EncodeToString(mac.Sum(nil)))
	}

	client := v.Client
	if client == nil {
		client = &http.Client{Timeout: time.Duration(timeoutS * float64(time.Second))}
	}

	if async {
		go func() {
			resp, err := client.Do(req)
			if err == nil {
				_, _ = io.Copy(io.Discard, resp.Body)
				_ = resp.Body.Close()
			}
		}()
		return map[string]any{outVar + "_async": true}, nil
	}

	resp, err := client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("webhook_emit: do: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()
	_, _ = io.Copy(io.Discard, resp.Body) // drain
	return map[string]any{
		outVar + "_status": int64(resp.StatusCode),
	}, nil
}
