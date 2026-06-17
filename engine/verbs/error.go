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
	"fmt"

	"github.com/Bugs5382/go-saga-orchestration/domain"
)

// ErrorVerb halts the saga with a non-retryable error. Inputs:
//   - "code"    (required, string)
//   - "message" (optional, string)
type ErrorVerb struct{}

// Execute always returns a non-retryable error built from the required code
// and optional message.
func (ErrorVerb) Execute(_ context.Context, _ domain.SagaRun, step domain.Step) (map[string]any, error) {
	code, _ := step.Inputs["code"].(string)
	if code == "" {
		return nil, fmt.Errorf("error: code required")
	}
	msg, _ := step.Inputs["message"].(string)
	if msg == "" {
		return nil, fmt.Errorf("%s", code)
	}
	return nil, fmt.Errorf("%s: %s", code, msg)
}
