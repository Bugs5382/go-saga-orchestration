---
sidebar_position: 4
---

# 🔑 Feature Gating & Licensing

go-saga-orchestration ships under the permissive **MIT license** — you can embed
it in anything, commercial or free, with no royalty or feature restriction. That
software license is separate from the engine's **feature-gating layer** described
on this page.

The feature-gating layer is a single authorization hook the engine calls before
running gated work. It is deliberately generic: you can use it to enforce **paid
licensing tiers**, to implement **RBAC** (role-based access control), or both —
the engine doesn't care what the answer means, only whether a `(tenant, feature)`
pair is allowed.

---

## The resolver hook

Every gate goes through one interface:

```go
// licensing.Resolver
type Resolver interface {
	IsFeatureEnabled(
		ctx context.Context,
		tenantID *uuid.UUID,        // the principal: a tenant, org, or user
		feature string,             // the capability being requested
		overrides map[string]bool,  // per-request allow/deny, wins over the resolver
	) (bool, error)
}
```

Return `true` to allow, `false` to deny. The `tenantID` is your principal and
`feature` is your permission — how you map them is entirely up to your
implementation. That is what makes the same hook serve licensing and RBAC.

---

## What is gated, and when

Each verb belongs to a **license group**. A group maps to a **feature flag**; the
engine asks the resolver whether that flag is enabled. The `common` group is never
gated.

| License group | Feature flag | Example verbs |
|---|---|---|
| `common` | _(never gated)_ | `set_var`, `transform`, `noop`, `log` |
| `waits` | `wf.timers` | `wait_duration`, `wait_until` |
| `events_and_signals` | `wf.event_driven` | `emit_event`, `wait_for_event`, `emit_signal` |
| `parallel_control` | `wf.parallel` | `parallel`, `foreach` |
| `loops_and_recovery` | `wf.loops_recovery` | `while`, `try_catch`, `cancel` |
| `human_interaction` | `wf.user_tasks` | `manual_approval`, `collect_input` |
| `compositions` | `wf.compositions` | `sub_saga`, `spawn_saga` |
| `external_io_advanced` | `wf.external_io` | authenticated `http_request`, `webhook_emit` |
| `observability` | `wf.observability` | `metric_emit` |
| _(cron triggers)_ | `wf.cron_triggers` | scheduled trigger starts |

The check fires in three places:

- **At publish** — `ValidateDefinition` rejects a workflow whose steps reference a
  feature the tenant lacks, so unlicensed workflows never go live.
- **At runtime** — each step re-checks its feature before executing (entitlements
  can change between publish and run).
- **At trigger time** — cron-scheduled starts check `wf.cron_triggers` both when
  the REST trigger is created and when the dispatcher fires it.

A denied check fails the publish or the step with a `license_gate` error naming
the step, group, and required feature.

---

## Use it for licensing (paid tiers)

Map a tenant's purchased plan to the feature flags it unlocks. A `free` tenant
publishing a workflow that uses `parallel` is rejected; a `premium` tenant is
allowed.

```go
type PlanResolver struct {
	planOf  func(uuid.UUID) string         // tenant -> "free" | "premium"
	unlocks map[string]map[string]bool      // plan  -> feature -> enabled
}

func (r PlanResolver) IsFeatureEnabled(_ context.Context, tenant *uuid.UUID, feature string, overrides map[string]bool) (bool, error) {
	if v, ok := overrides[feature]; ok {
		return v, nil // per-request override wins (e.g. a trial grant)
	}
	if tenant == nil {
		return false, nil
	}
	return r.unlocks[r.planOf(*tenant)][feature], nil
}
```

## Use it for RBAC

The exact same hook is a per-principal permission check. Treat `feature` as a
permission name and `tenantID` as the role-bearing principal, and deny verbs a
role isn't allowed to run — independent of whether anyone paid.

```go
// Allow only roles that hold the permission for the requested feature.
func (r RoleResolver) IsFeatureEnabled(_ context.Context, principal *uuid.UUID, feature string, _ map[string]bool) (bool, error) {
	if principal == nil {
		return false, nil
	}
	role := r.roleOf(*principal)         // e.g. "operator", "viewer"
	return r.permits(role, feature), nil // RBAC policy lookup
}
```

Because licensing and RBAC share one interface, you can also compose them — wrap a
plan check and a role check and require both to pass.

---

## Built-in resolvers

| Resolver | Use |
|---|---|
| `licensing.StubAllowAll{}` | Allows everything. The default for `saga.InMemory()` and tests. |
| `licensing.NewCached(inner, ttl)` | Wraps any resolver with a per-`(tenant, feature)` cache and applies per-request `overrides`. Wrap your real resolver with this in production. |
| `licensing.HTTPFeatureResolver{BaseURL: ...}` | Resolves flags by `GET`-ing a remote service that returns `{"features":[...]}` for a tenant. Wrap it with `Cached`. |

---

## Wiring it in

Pass your resolver to `saga.New`; omit it (or use `saga.InMemory()`) to allow
everything during development.

```go
sc, err := saga.New(saga.Options{
	Store:     store,
	Licensing: licensing.NewCached(PlanResolver{ /* ... */ }, 5*time.Minute),
})
```

See [Embedding](embedding#-production-wiring) for the rest of the production
options and [Testing](testing) for asserting gated behavior with `StubAllowAll`.
