package verbs

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

import "github.com/Bugs5382/go-saga-orchestration/domain"

// GroupToFeature maps each license-group name to its feature flag.
// "common" maps to the empty string — that's the sentinel for
// "no gate, always allowed".
var GroupToFeature = map[string]string{
	"common":               "",
	"observability":        "wf.observability",
	"external_io_advanced": "wf.external_io",
	"waits":                "wf.timers",
	"events_and_signals":   "wf.event_driven",
	"human_interaction":    "wf.user_tasks",
	"parallel_control":     "wf.parallel",
	"loops_and_recovery":   "wf.loops_recovery",
	"compositions":         "wf.compositions",
}

// LicenseGroupForStep returns the effective license-group for the
// given step. Most verbs have a static group (set in registry.Default
// via RegistryEntry.LicenseGroup). One exception: http_request has a
// dynamic group depending on its inputs — GET with no secret_ref is
// `common`; everything else is `external_io_advanced`. This function
// applies that dynamic override.
func LicenseGroupForStep(step domain.Step, regGroup string) string {
	if step.Type == domain.StepTypeHTTPRequest {
		method, _ := step.Inputs["method"].(string)
		secret, _ := step.Inputs["secret_ref"].(string)
		if (method == "" || method == "GET") && secret == "" {
			return "common"
		}
		return "external_io_advanced"
	}
	return regGroup
}
