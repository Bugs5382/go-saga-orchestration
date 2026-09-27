# Changelog

## v0.7.0 - 2026-09-27

### What Changed 👀

#### 🚀 Features

- refactor(logging): log through Bugs5382/go-log @Bugs5382 (#113)
- feat(api): add workflow definitions HTTP API (list/get/save) and fix ListRuns NULL scan @Bugs5382 (#99)

#### 🐛 Bug Fixes

- fix(store): deep-copy memory store reads and fix parallel join wakeups @Bugs5382 (#117)
- fix(deps): clear the govulncheck and gosec findings @Bugs5382 (#108)

#### ⚠️ Changes

- refactor(logging): log through Bugs5382/go-log @Bugs5382 (#113)

#### 📄 Documentation

- docs: correct the step type count in the verb reference and AGENTS.md @Bugs5382 (#114)
- docs: use the "Next 🚧" version-picker label @Bugs5382 (#100)
- docs(readme): state one count of saga step types @Bugs5382 (#111)
- docs(readme): apply the lite emoji treatment @Bugs5382 (#104)

### Extra

**Full Changelog**: https://github.com/Bugs5382/go-saga-orchestration/compare/v0.6.2...v0.7.0

## v0.6.1 - 2026-07-06

### What Changed 👀

#### 📄 Documentation

- docs(website): snapshot version-0.6.0 and roll next to v0.7.0 @Bugs5382 (#93)

### Extra

**Full Changelog**: https://github.com/Bugs5382/go-saga-orchestration/compare/v0.6.0...v0.6.1

## v0.6.0 - 2026-07-06

### What Changed 👀

#### 🚀 Features

- feat(engine): add join verb to reconvene independently-spawned streams @Bugs5382 (#87)

#### 🐛 Bug Fixes

- fix(website): label the next docs version v0.6.0 @Bugs5382 (#91)
- fix(website): escape MDX-hazardous chars in the generated changelog page @Bugs5382 (#89)

#### 📄 Documentation

- fix(website): label the next docs version v0.6.0 @Bugs5382 (#91)

### Extra

**Full Changelog**: https://github.com/Bugs5382/go-saga-orchestration/compare/v0.5.0...v0.6.0

## v0.5.0 - 2026-07-06

### What Changed 👀

#### 🚀 Features

- feat(api): add POST /sagas/{id}/cancel REST endpoint @Bugs5382 (#85)
- feat(store): add AcquireAdvisoryLock helper for single-leader election @Bugs5382 (#84)

### Extra

**Full Changelog**: https://github.com/Bugs5382/go-saga-orchestration/compare/v0.4.0...v0.5.0

## v0.4.0 - 2026-06-29

### What Changed 👀

#### 🚀 Features

- feat(engine): run-level Cancel(runID) API + persist failed-run error @Bugs5382 (#81)

### Extra

**Full Changelog**: https://github.com/Bugs5382/go-saga-orchestration/compare/v0.3.0...v0.4.0

## v0.3.0 - 2026-06-29

### What Changed 👀

#### 🚀 Features

- feat(deploy): gate cron dispatcher for a dedicated isolated engine pod @Bugs5382 (#76)
- feat(engine): execute step-level retry and compensation @Bugs5382 (#77)
- feat(engine): set TriggerID and record trigger-fire audit rows @Bugs5382 (#72)
- feat(registry): optional dispatch descriptor (http/rmq) on ActionRegistration @Bugs5382 (#73)
- feat(engine): interval-based cron cadences @Bugs5382 (#70)
- feat(engine): cron-scheduled workflow triggers @Bugs5382 (#68)
- feat(deploy): publish container images and a Helm chart @Bugs5382 (#65)

#### 📄 Documentation

- docs: publish a versioned Docusaurus site to GitHub Pages @Bugs5382 (#74)

### Extra

**Full Changelog**: https://github.com/Bugs5382/go-saga-orchestration/compare/v0.2.2...v0.3.0

## v0.2.2 - 2026-06-25

### What Changed 👀

#### 🐛 Bug Fixes

- fix(postgres): correct UpsertWorkflowDefinition id and NULL current\_step scans @Bugs5382 (#62)

### Extra

**Full Changelog**: https://github.com/Bugs5382/go-saga-orchestration/compare/v0.2.1...v0.2.2

## v0.2.1 - 2026-06-20

### What Changed 👀

#### 🐛 Bug Fixes

- fix(modules): collapse clients/go/worker into the root module @Bugs5382 (#55)

### Extra

**Full Changelog**: https://github.com/Bugs5382/go-saga-orchestration/compare/v0.2.0...v0.2.1

## v0.2.0 - 2026-06-20

### What Changed 👀

#### 🚀 Features

- feat(store): add Redis/Valkey store backend @Bugs5382 (#50)

#### 🐛 Bug Fixes

- fix(modules): make clients/go/worker externally consumable + retract v0.1.1 @Bugs5382 (#53)

#### ⚡ Performance

- perf(engine): benchmark and tune the coordinator hot path @Bugs5382 (#47)

#### 🧩 Dependency Updates

- chore(deps): bump the github-actions group with 3 updates @[dependabot[bot]](https://github.com/apps/dependabot) (#49)

### Extra

**Full Changelog**: https://github.com/Bugs5382/go-saga-orchestration/compare/v0.1.0...v0.2.0

## v0.1.0 - 2026-06-17

### What Changed 👀

#### 🚀 Features

- feat(domain): workflow and saga-run domain model @Bugs5382 (#28)
- feat(store): store interface with in-memory and Postgres backends @Bugs5382 (#29)
- feat(resolvers): clock, secrets, and licensing stubs @Bugs5382 (#30)
- feat(cel): CEL evaluation and decision-table rules @Bugs5382 (#31)
- feat(mq): RabbitMQ dispatch topology @Bugs5382 (#32)
- feat(verbs): implement the 31 saga step types @Bugs5382 (#33)
- feat(engine): coordinator and step advancement @Bugs5382 (#34)
- feat(grpc): worker liveness streaming protocol @Bugs5382 (#35)
- feat(api): REST API and OpenAPI specification @Bugs5382 (#36)
- feat(saga): high-level embedding facade @Bugs5382 (#37)
- feat(clients): Go worker SDK @Bugs5382 (#38)
- feat(cmd): api and engine service binaries @Bugs5382 (#39)

#### 📄 Documentation

- docs: architecture, embedding, and verb references @Bugs5382 (#41)

#### 🧩 Dependency Updates

- chore(deps): bump the github-actions group with 8 updates [@dependabot](https://github.com/apps/dependabot) (#27)

### Extra

**Full Changelog**: https://github.com/Bugs5382/go-saga-orchestration/commits/v0.1.0
