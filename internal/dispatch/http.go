// Package dispatch provides transport-specific delivery of action dispatch
// payloads for the ActionRegistration dispatch descriptor (issue #59). gRPC
// dispatch stays in the mq package; this package holds the http transport.
package dispatch

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
	"fmt"
	"io"
	"net/http"
	"time"
)

// HTTPDispatcher POSTs an action dispatch payload to a worker's callback URL.
// It implements verbs.ActionHTTPDispatcher: workers consuming over the http
// transport report their result asynchronously via the result-callback REST
// endpoint, so a 2xx here means "accepted", not "completed".
type HTTPDispatcher struct {
	// Client is the HTTP client used to POST the payload. nil defaults to a
	// client with a 10s timeout.
	Client *http.Client
}

// NewHTTPDispatcher returns an HTTPDispatcher with a default 10s-timeout client.
func NewHTTPDispatcher() *HTTPDispatcher {
	return &HTTPDispatcher{Client: &http.Client{Timeout: 10 * time.Second}}
}

// DispatchHTTP POSTs payload as application/json to address. A non-2xx status
// is an error. The worker acknowledges receipt; the saga result arrives later
// over the result-callback REST endpoint.
func (d *HTTPDispatcher) DispatchHTTP(ctx context.Context, address string, payload []byte) error {
	client := d.Client
	if client == nil {
		client = &http.Client{Timeout: 10 * time.Second}
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, address, bytes.NewReader(payload))
	if err != nil {
		return fmt.Errorf("dispatch http: build request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("dispatch http: post %s: %w", address, err)
	}
	defer func() { _ = resp.Body.Close() }()
	_, _ = io.Copy(io.Discard, resp.Body)
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("dispatch http: %s returned status %d", address, resp.StatusCode)
	}
	return nil
}
