# AGENTS.md - go-saga-orchestration

Guide for AI agents working in this repository. Pair with `CLAUDE.md` (the working agreement and
hook-enforced rules). Keep this file current when the build, layout, or public API changes.

## What this is

A standalone, solution-agnostic saga orchestrator and synchronous CEL rule evaluator. It ships as a
Go library you embed in-process, and as two reference service binaries (`cmd/api`, `cmd/engine`)
backed by Postgres + RabbitMQ. The engine executes workflow definitions made of 31 step types
("verbs"); CEL expressions drive conditions, transforms, filters, and routing.

The one thing to understand before changing the engine: a workflow is a `domain.WorkflowDefinition`
of `domain.Step`s, and the coordinator (`engine`) advances a `domain.SagaRun` one step at a time,
dispatching each step to a verb handler resolved from the registry. New behavior is almost always a
new verb under `engine/verbs`, not a change to the coordinator.

## Using go-saga-orchestration

The public surface is the `saga` facade. Embed it with `saga.InMemory()` (in-memory store,
in-process advance; for tests and simple automations) or `saga.New(saga.Options{Store: ..., ...})`
for production wiring. Register custom step types with `RegisterVerb` before publishing workflows.
Do not import `internal/*` from outside the module; those are infrastructure.

## Documentation

The docs are single-sourced as Markdown under `website/docs/` (a Docusaurus site published to
<https://bugs5382.github.io/go-saga-orchestration/>). There is no top-level `docs/` folder — read
and edit the Markdown in `website/docs/` directly. The Go API reference under
`website/docs/reference/` and the changelog copy are generated (gitignored) — never hand-edit them;
regenerate with `npm run gen` in `website/`.

For agents consuming the published site over HTTP rather than the repo, the build emits a plain-text
bundle of every page at `/llms-full.txt` (with an index at `/llms.txt`).

## Layout

- `saga/` - public facade (`InMemory`, `New`, `*saga.Saga`).
- `domain/` - core types (`WorkflowDefinition`, `SagaRun`, `Step`, `RuleDefinition`).
- `engine/`, `engine/verbs/` - coordinator + the 31 verb implementations + `verbs.HandlerFunc`.
- `store/`, `store/memory`, `store/postgres` - `Store` interface, in-memory impl, Postgres impl + migrations.
- `api/` - REST handlers, router, and `api/openapi.yaml`.
- `internal/{cel,rules,mq,grpc,config,logging}` - infrastructure (not for direct import).
- `cmd/api`, `cmd/engine` - the two service binaries.
- `clients/go/worker` - Go worker SDK (nested module).
- `proto/` - gRPC worker liveness service + generated code.
- `test/` - unit tests + fixtures; `test/e2e` - end-to-end tests.

## Build, test, lint

Commands are defined in `Taskfile.yaml` (run with `go-task`):

- Build: `task build` (`go build ./...`)
- Test: `task test` (`go test ./...`). The `test/e2e` suite runs against the in-memory store, so it
  needs no Postgres/RabbitMQ; the service binaries and `cmd/engine` do.
- Lint: `task lint` (gofmt check, `golangci-lint run`, `yamllint .`)
- License headers: `task license` (CI dry-run check) / `task license:fix` (inject MIT headers).

## Conventions and gotchas

- See `CLAUDE.md` for the branch/commit/PR rules; they are enforced by the git hooks in
  `.claude/hooks` (run `bash .claude/hooks/install.sh` once per clone).
- Every Go source file carries an MIT license header (enforced by the golic CI job); run
  `task license:fix` after adding files.
- All configuration is environment variables (`internal/config/config.go`); there is no config file.
- Verb feature groups are license-gated at publish and runtime via the `licensing` resolver; use
  `licensing.StubAllowAll{}` in tests.
