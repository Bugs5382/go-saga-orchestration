---
slug: /
sidebar_position: 0
title: Introduction
---

# go-saga-orchestration

A standalone, solution-agnostic **saga orchestrator + synchronous CEL rule evaluator** you can
embed as a Go library or run as a two-binary service.

- **31 saga step types** — transforms, HTTP/webhooks, timers, signals, events, parallel fan-out,
  foreach, loops, try/catch, human tasks, sub-sagas, and more.
- **Embed or deploy** — run in-process with zero infrastructure, or deploy two Docker-friendly
  binaries backed by Postgres + RabbitMQ.
- **CEL expressions** for conditions, transforms, filters, and routing.
- **Scheduled & event-driven starts**, durable timers, and a license-gated feature model.

## Where to go next

- **[Getting started](getting-started)** — build a working saga from empty.
- **[Architecture](architecture)** — engine internals, coordinator, MQ topology, stores.
- **[Deployment](deployment)** — container images and the Helm chart.
- **[Verbs reference](verbs)** — every step type.
