# AnchorBell Industrial Platform Evolution

Status: architecture audit and implementation baseline, 2026-09-05.

## 1. Decision

AnchorBell is an industrial quantitative execution platform. Simulation,
replay, validation, and analytics are subordinate operational environments;
they are not the product identity and never own exchange authority.

The immutable core remains Rust-first, event-driven, maker-only, fail-closed,
hot-path in memory, and asynchronously persisted. Production stays disabled
by default and no architecture change may weaken that boundary.

## 2. Current inventory

The current Rust workspace has 95 source files and approximately 23,897 Rust
lines. The deployable engine contains these planes:

| Plane | Systems |
| --- | --- |
| Control | topology registry, runtime, recovery, dashboard |
| Market data | Binance transport, subscriptions, connection supervision, metadata/capability, reference/FX, anchor |
| Decision | strategy, session/calendar, inventory/capital, portfolio, risk, funding |
| Execution | signed gateway, order lifecycle, user data, reconciliation, recovery/checkpoint |
| Simulation | simulation runtime, replay, backtest, fill/queue/latency models |
| Analytics | validation, evidence, reports; non-authoritative |
| Observability | recorder, telemetry, audit, health/readiness/liveness |
## 3. Findings from the implementation audit

1. The system catalog exists, but before this change it was a mostly static
   description. Health was manually reportable, missing health was not part of
   readiness, and dependency health did not form a transitive admission gate.
2. The runtime owned a registry but did not expose a production admission
   method. A live entrypoint could therefore bypass the intended topology
   contract unless an outer supervisor remembered to enforce it.
3. The engine now exposes canonical simulation and analytics modules. The largest simulator file still combines runtime, allocation, fills, state, metrics, and reporting responsibilities, so the next scale gate is responsibility extraction behind typed ports.
4. The repository contains both the Rust engine and an archival Python
   prototype under research/legacy-python/src/; the catalog correctly excludes the prototype from
   live authority, but the boundary is not enforced by a CI vocabulary gate.
5. More than 11 GB of untracked target-* build/run trees accumulated on the
   Windows host. They are build artifacts, not evidence, and were not covered
   by the ignore policy.
6. The existing baseline is healthy but incomplete: 272 Rust tests passed and
   the architecture gate passed before this implementation slice. That proves
   regression coverage, not live exchange or profitability evidence.

## 4. Implemented in this slice

- Added first-class analytics.validation and operations.supervisor nodes.
- Added canonical analytics and simulation modules for downstream code; obsolete implementation names were removed.
- Added registry bootstrap discovery, transitive readiness reports, health
  expiry detection, and fail-closed missing-health behavior.
- Added runtime health reporting, refresh, readiness inspection, and an
  explicit require_live_execution admission method.
- Added target-*/ artifact isolation to .gitignore; existing artifacts
  were not deleted or overwritten.
## 5. Target architecture

All systems register a descriptor with identity, plane, role, authority,
mutability, dependencies, health interval, capabilities, and recovery intent.
The registry is the topology source; it is not a god object. State remains
owned by each system and crosses boundaries through typed events and ports.

The runtime lifecycle is:

discover -> validate topology -> bootstrap health -> start dependencies ->
observe health -> admit capability -> run -> diagnose -> drain -> reconcile ->
restart or halt.

A capability is tradable only when its own health and the complete dependency
closure are fresh, internally valid, and non-degraded. Missing observations
are not interpreted as healthy. Recovery is automatic for restartable
adapters and always followed by reconciliation before risk can resume.

The event path is:

market input -> normalize -> capability gate -> bounded dispatcher ->
decision snapshot -> strategy -> safety risk -> order intent -> gateway ->
lifecycle -> reconciliation -> asynchronous evidence.

No decision module opens sockets, reads credentials, writes durable media, or
directly mutates exchange state. No recorder or analytics consumer can block
or alter the decision path.

## 6. Expansion model

The system must scale by descriptors and adapters, not by duplicating safety
logic. Future expansion assumptions are:

- multiple venues and product families;
- thousands of symbols partitioned into supervised market shards;
- multiple policy lineages sharing one reference service;
- account, desk, tenant, and portfolio risk budgets;
- active/standby supervisors with process fencing;
- schema-versioned event storage and replay;
- automatic capacity planning from queue, latency, and error telemetry;
- champion/challenger policy evaluation in shadow mode;
- rollbackable policy promotion with immutable lineage and evidence digests.
## 7. Operational vocabulary

| Operational concept | Canonical operational term |
| --- | --- |
| simulation runtime | simulation runtime |
| multi-policy execution | batch simulation |
| run version | run ID and policy lineage ID |
| non-authoritative output | validation/evidence record |
| validation output | analytics/validation |
| simulation result | simulation result |

All callers, production documentation, dashboards, metrics, and APIs use the canonical terms. Removed names are not accepted as aliases.

## 8. Delivery order

1. Bind every supervisor to registry health publication and recovery events.
2. Make the reference/anchor service the sole refresh authority; expose anchor
   provenance, validity, lineage, and refresh outcomes as typed events.
3. Split the large simulation engine into execution model, allocation,
   accounting ledger, metrics projection, and report/export systems.
4. Move evidence and analytics behind non-authoritative ports; reject their
   imports from execution and decision modules in CI.
5. Introduce machine-readable capability manifests and contract tests for each
   venue/product adapter.
6. Add automatic artifact retention and run manifests; never mix binaries,
   transient target trees, and durable evidence.
7. Add shadow policy promotion, rollback, and resource-budget gates.
8. Only after these gates pass, consider multi-venue and multi-tenant scale.

## 9. Acceptance definition

The architecture is complete when a fresh process can discover its topology,
detect a missing/stale/contradictory dependency without an operator checklist,
stop only the affected capability, recover and reconcile restartable systems,
and prove every order intent's source snapshot, gate decisions, lifecycle,
accounting, and evidence lineage. A favorable simulation result alone never
authorizes production.


## 10. Runtime control-plane slice now implemented

The live composition root now owns a control plane backed by the authoritative
system registry. It reports market, reference, anchor, risk, lifecycle, and
execution health from runtime observations instead of an operator checklist.

A 250ms control-plane heartbeat expires silent dependencies automatically. A
blocked execution closure cancels working orders, stops new risk, and emits a
machine-readable gate event. Recovery requires fresh inputs, a supervisor
reconnect transition, per-symbol exchange reconciliation, and only then a
resume transition.

System descriptors now expose executable contracts: required dependencies,
provided capabilities, and recovery policy. Simulation batch manifests also
carry a versioned operational identity so future replay, batch, and live-like
runs can share one evidence lineage without treating validation labels as system
authority.
