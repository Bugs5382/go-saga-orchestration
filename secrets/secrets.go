// Package secrets resolves a secret_ref string (e.g. "vault://path/to/key")
// into a value at runtime. It ships an in-memory map-backed stub;
// real Vault/SealedSecrets integration is a platform-services concern.
package secrets

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

import "fmt"

// Resolver looks up a secret by reference.
type Resolver interface {
	Get(ref string) (string, error)
}

// Memory is a test-time / dev-time resolver. Production wires a real impl.
type Memory struct {
	M map[string]string
}

// NewMemory returns a Memory resolver backed by the given ref-to-value map.
func NewMemory(m map[string]string) *Memory { return &Memory{M: m} }

// Get returns the value for ref, or an error if the ref is not in the map.
func (r *Memory) Get(ref string) (string, error) {
	v, ok := r.M[ref]
	if !ok {
		return "", fmt.Errorf("secrets: ref not found: %s", ref)
	}
	return v, nil
}
