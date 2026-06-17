package licensing

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
	"fmt"
	"net/http"
	"net/url"
	"slices"
	"time"

	"github.com/google/uuid"
)

// HTTPFeatureResolver resolves feature flags by GET-ing a remote endpoint that
// returns the set of enabled features for a tenant. It is a generic Resolver
// implementation; wrap it with licensing.Cached in production.
type HTTPFeatureResolver struct {
	BaseURL      string       // e.g. "http://feature-service:8080"
	FeaturesPath string       // path returning {"features":[...]}; default "/features"
	HTTP         *http.Client // optional; nil uses a default 5s-timeout client
}

type featuresResp struct {
	Features []string `json:"features"`
}

// IsFeatureEnabled is non-cached. Per-request overrides are ignored at this
// layer (the Cached wrapper applies them).
func (c *HTTPFeatureResolver) IsFeatureEnabled(ctx context.Context, tenant *uuid.UUID, feature string, _ map[string]bool) (bool, error) {
	path := c.FeaturesPath
	if path == "" {
		path = "/features"
	}
	q := url.Values{}
	if tenant != nil {
		q.Set("tenant_id", tenant.String())
	}
	u := c.BaseURL + path
	if len(q) > 0 {
		u += "?" + q.Encode()
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
	if err != nil {
		return false, fmt.Errorf("feature resolver: build request: %w", err)
	}
	client := c.HTTP
	if client == nil {
		client = &http.Client{Timeout: 5 * time.Second}
	}
	resp, err := client.Do(req)
	if err != nil {
		return false, fmt.Errorf("feature resolver: do: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		return false, fmt.Errorf("feature resolver: unexpected status %d", resp.StatusCode)
	}
	var body featuresResp
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		return false, fmt.Errorf("feature resolver: decode: %w", err)
	}
	return slices.Contains(body.Features, feature), nil
}
