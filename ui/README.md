# ui/

Reserved for the **reusable UI framework** for `go-saga-orchestration`.

This directory will hold a front-end (with its own tooling and build) that other projects
can integrate. It lives outside the Go module — the Go toolchain ignores it because it
contains no `.go` files.

The UI consumes the engine over its HTTP API (`/api/v1/...`); see `../docs/api.md` and
`../api/openapi.yaml` once the documentation effort lands.

**Status:** planned / empty scaffold.
