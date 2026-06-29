package examples_test

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
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/Bugs5382/go-saga-orchestration/clock"
	"github.com/Bugs5382/go-saga-orchestration/domain"
	"github.com/Bugs5382/go-saga-orchestration/engine"
	"github.com/Bugs5382/go-saga-orchestration/engine/verbs"
	"github.com/Bugs5382/go-saga-orchestration/secrets"
	"github.com/Bugs5382/go-saga-orchestration/store/memory"
)

// TestExampleWorkflowsAreValid loads every examples/workflows/*.json, confirms
// it parses into a WorkflowDefinition, passes engine.ValidateDefinition, names
// only known verbs, and that every Next/Branch edge and Start/Entrypoint
// references an existing step. Keeps the examples honest as the engine evolves.
func TestExampleWorkflowsAreValid(t *testing.T) {
	reg := verbs.Default(memory.New(), clock.SystemClock{}, secrets.NewMemory(nil), nil, nil, nil)
	files, err := filepath.Glob("*.json")
	if err != nil {
		t.Fatalf("glob: %v", err)
	}
	if len(files) == 0 {
		t.Fatal("no example workflows found")
	}
	for _, f := range files {
		f := f
		t.Run(filepath.Base(f), func(t *testing.T) {
			raw, err := os.ReadFile(f)
			if err != nil {
				t.Fatalf("read: %v", err)
			}
			var def domain.WorkflowDefinition
			if err := json.Unmarshal(raw, &def); err != nil {
				t.Fatalf("unmarshal: %v", err)
			}
			if err := engine.ValidateDefinition(def); err != nil {
				t.Fatalf("ValidateDefinition: %v", err)
			}
			ids := map[string]bool{}
			for _, s := range def.Steps {
				ids[s.ID] = true
			}
			if def.Start == "" || !ids[def.Start] {
				t.Fatalf("start %q missing", def.Start)
			}
			for name, sid := range def.Entrypoints {
				if !ids[sid] {
					t.Fatalf("entrypoint %q -> missing step %q", name, sid)
				}
			}
			for _, s := range def.Steps {
				if s.Type != domain.StepTypeEnd {
					if _, ok := reg[s.Type]; !ok {
						t.Fatalf("step %q uses unknown verb %q", s.ID, s.Type)
					}
				}
				if s.Next != "" && !ids[s.Next] {
					t.Fatalf("step %q next %q missing", s.ID, s.Next)
				}
				for bk, br := range s.Branches {
					if br.Next != "" && !ids[br.Next] {
						t.Fatalf("step %q branch %q next %q missing", s.ID, bk, br.Next)
					}
				}
			}
		})
	}
}
