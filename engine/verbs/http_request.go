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
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/Bugs5382/go-saga-orchestration/domain"
	"github.com/Bugs5382/go-saga-orchestration/secrets"
)

// HTTPRequestVerb issues a synchronous outbound HTTP request and merges
// the response into Variables. Inputs:
//   - "method"     (optional, string; default "GET")
//   - "url"        (required, string)
//   - "headers"    (optional, map[string]any → stringified into request headers)
//   - "body"       (optional, any; JSON-marshalled into request body)
//   - "timeout_s"  (optional, number; default 30s)
//   - "secret_ref" (optional, string; resolves to a value set as Authorization header)
//   - "out_var"    (optional, string; default "http_result"). Result keys:
//     {out_var} = parsed JSON body if application/json, else raw string.
//     {out_var}_status  = int64 status code.
//     {out_var}_headers = map[string]string of response headers.
type HTTPRequestVerb struct {
	Secrets secrets.Resolver
	Client  *http.Client // optional; nil → constructed per-call with timeout_s
}

// Execute issues the configured HTTP request (resolving secret_ref into an
// Authorization header when set) and returns the parsed body, status code,
// and response headers keyed off out_var.
func (v HTTPRequestVerb) Execute(ctx context.Context, run domain.SagaRun, step domain.Step) (map[string]any, error) {
	method, _ := step.Inputs["method"].(string)
	if method == "" {
		method = "GET"
	}
	url, _ := step.Inputs["url"].(string)
	if url == "" {
		return nil, fmt.Errorf("http_request: url required")
	}
	timeoutS := 30.0
	if t, ok := step.Inputs["timeout_s"].(float64); ok {
		timeoutS = t
	}
	outVar, _ := step.Inputs["out_var"].(string)
	if outVar == "" {
		outVar = "http_result"
	}

	var body io.Reader
	if b, ok := step.Inputs["body"]; ok && b != nil {
		bs, err := json.Marshal(b)
		if err != nil {
			return nil, fmt.Errorf("http_request: marshal body: %w", err)
		}
		body = bytes.NewReader(bs)
	}

	req, err := http.NewRequestWithContext(ctx, method, url, body)
	if err != nil {
		return nil, fmt.Errorf("http_request: build: %w", err)
	}
	if hdrs, ok := step.Inputs["headers"].(map[string]any); ok {
		for k, val := range hdrs {
			req.Header.Set(k, fmt.Sprint(val))
		}
	}
	if ref, ok := step.Inputs["secret_ref"].(string); ok && ref != "" {
		if v.Secrets == nil {
			return nil, fmt.Errorf("http_request: secret_ref %q given but no Secrets resolver wired", ref)
		}
		val, err := v.Secrets.Get(ref)
		if err != nil {
			return nil, fmt.Errorf("http_request: secret: %w", err)
		}
		req.Header.Set("Authorization", val)
	}

	client := v.Client
	if client == nil {
		client = &http.Client{Timeout: time.Duration(timeoutS * float64(time.Second))}
	}
	resp, err := client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("http_request: do: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()
	rb, _ := io.ReadAll(resp.Body)

	var parsed any = string(rb)
	if strings.HasPrefix(resp.Header.Get("Content-Type"), "application/json") {
		var j any
		if json.Unmarshal(rb, &j) == nil {
			parsed = j
		}
	}

	respHeaders := map[string]string{}
	for k, vs := range resp.Header {
		if len(vs) > 0 {
			respHeaders[k] = vs[0]
		}
	}

	return map[string]any{
		outVar:              parsed,
		outVar + "_status":  int64(resp.StatusCode),
		outVar + "_headers": respHeaders,
	}, nil
}
