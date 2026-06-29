package licensing

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
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestHTTPFeatureResolver_FeaturePresent(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(featuresResp{Features: []string{"wf.timers", "wf.parallel"}})
	}))
	defer srv.Close()

	c := &HTTPFeatureResolver{BaseURL: srv.URL}
	v, err := c.IsFeatureEnabled(context.Background(), nil, "wf.timers", nil)
	if err != nil || !v {
		t.Errorf("got (%v, %v), want (true, nil)", v, err)
	}
}

func TestHTTPFeatureResolver_FeatureAbsent(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(featuresResp{Features: []string{"wf.timers"}})
	}))
	defer srv.Close()

	c := &HTTPFeatureResolver{BaseURL: srv.URL}
	v, err := c.IsFeatureEnabled(context.Background(), nil, "wf.parallel", nil)
	if err != nil || v {
		t.Errorf("got (%v, %v), want (false, nil)", v, err)
	}
}

func TestHTTPFeatureResolver_500Errors(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer srv.Close()

	c := &HTTPFeatureResolver{BaseURL: srv.URL}
	_, err := c.IsFeatureEnabled(context.Background(), nil, "wf.timers", nil)
	if err == nil {
		t.Errorf("expected error on 500")
	}
}
