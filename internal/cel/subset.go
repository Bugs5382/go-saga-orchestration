package cel

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
	celpkg "github.com/google/cel-go/cel"
	"github.com/google/cel-go/ext"
)

// subsetOptions returns the EnvOption set that enables only the v1
// CEL surface. Today we enable:
//   - the stdlib (arithmetic, string ops, equality, &&, ||, in)
//   - the strings extension (substring, replace, split, etc.)
//
// We deliberately do NOT enable:
//   - native function bindings (no Go host functions exposed to admins)
//   - file / time / network functions beyond CEL's pure helpers
//
// As the spec extends to v1.5+ (script rule type, custom helpers) this
// is the one place to widen the surface.
func subsetOptions() []celpkg.EnvOption {
	return []celpkg.EnvOption{
		celpkg.StdLib(),
		ext.Strings(),
	}
}
