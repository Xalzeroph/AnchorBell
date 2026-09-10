# Simulation stability and realtime processing — 2026-09-10

## Observed failure

On the ECS simulation, the metrics snapshot grew to 8,482,144 bytes with
34,393 rejection details. The market-event to processing timestamp gap reached
129,729 ms. Every portfolio drawdown evaluation called `summary()`, cloning the
entire unbounded rejection history for each market event. The same full summary
was also used for performance samples and periodic JSON snapshots.

The batch loop used a biased select with market input ahead of depth resync,
reference updates, FX updates, and supervisor completion. A continuously ready
market queue could starve those sources. Snapshot timers also retained Tokio's
missed-tick burst behavior, causing overdue snapshots to compete with ingress.

## Changes

- Portfolio drawdown and performance samples use an accounting-only summary.
  Financial aggregation and valuation-completeness rules are shared with the
  full summary; diagnostic maps and record strings are not cloned on this path.
- In-memory rejection details retain the latest 1,024 entries in chronological
  order. `rejected_entries` and per-reason `gate_rejections` remain cumulative.
  `gate_rejection_records_truncated` indicates whether older details were evicted.
  The dashboard labels this as a recent diagnostic window. Existing ledger and
  market recording paths remain in place.
- Batch selection fairly polls ready sources. Shutdown is checked before each
  selection and remains selectable while waiting for input.
- Both simulation runners skip obsolete snapshot timer ticks after a stall.
- Diagnostic/summary ownership is extracted into `runtime_diagnostics.rs` so
  the runtime source remains within the existing 250,000-byte resource budget.

Strategy thresholds, fee assumptions, position limits, drawdown limits, feed
queue capacity, and the policy for invalidating runs on overflow are unchanged.

## Verification and measurement

Regression tests reproduce unbounded retention, diagnostic cloning on the
accounting path, and missed-tick bursts before the fixes. They cover bounded
internal storage, cumulative counts, chronological eviction, incomplete
valuation, identical financial fields, and virtual-time snapshot scheduling.

The manual `accounting_history_benchmark` creates 8,192 identical rejections and
measures 100 accounting snapshots on the same ECS, using a debug build:

| Measurement | Before | After |
| --- | ---: | ---: |
| 100 accounting snapshots | 222,437 microseconds | 28 microseconds |
| Serialized full summary | 1,195,425 bytes | 150,047 bytes |

This microbenchmark measures diagnostic overhead on the accounting path.
End-to-end feed latency also includes network, exchange timestamps, disk I/O,
and strategy work; it must be checked independently after deployment.

Reproduce with:

```sh
cargo test --lib --locked accounting_history_benchmark -- --ignored --nocapture
cargo test --workspace --locked
cargo clippy --workspace --all-targets --all-features --locked -- -D warnings
cargo fmt --all -- --check
python3 scripts/architecture_gate.py
python3 scripts/resource_gate.py
node --check engine/web/app.js
```
