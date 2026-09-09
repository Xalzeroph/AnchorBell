# AnchorBell architecture governance

This document is the review contract for changes that affect strategy, execution,
simulation, configuration, or evidence. It is deliberately stricter than a
style guide: a change is incomplete when its behavior cannot be reproduced and
audited from its declared inputs.

## 1. Ownership boundaries

The runtime is composed from these ownership domains:

| Domain | Owns | Must not own |
| --- | --- | --- |
| market | Binance event parsing, normalization, freshness, and recording | strategy choices or credentials |
| strategy | anchor, session, quote, inventory, and experiment decisions | sockets, credentials, or exchange mutations |
| risk | exposure limits, stale-data gates, deadlines, and permission decisions | order transport or hidden defaults |
| execution | order intent validation, lifecycle, reconciliation, and gateway calls | research labels or future information |
| replay / backtest | deterministic event ordering and explicit fill assumptions | production authorization |
| runtime | dependency composition, persistence, and observability | silently changing policy |

A module may depend on a lower-level contract only through its typed interface.
The strategy must be runnable against simulation, replay, and exchange adapters
without changing its decision semantics.

## 2. Configuration provenance

Business behavior must come from one of three declared sources:

1. live Binance metadata and exchange filters;
2. explicit user input captured by the control surface; or
3. versioned JSON/TOML configuration committed with the experiment or deployment.

Examples include fee schedules, symbol universes, session windows, funding
deadlines, quote policy, execution policy, and experiment variants. Rust source
may define types, validation, and safe absence behavior, but must not hide a
business value behind a production Default, an unexplained literal, or a
test-only fixture that can leak into runtime.

Every resolved configuration must carry its source, version or hash, and
effective timestamp. Unknown, stale, contradictory, or missing exchange data
fails closed.

## 3. Execution policy

Normal entry and reduction are maker-first and post-only. An emergency taker is
not a second strategy and never creates exposure. It must be:

- reduce-only and symbol/side bounded;
- registered in the resolved configuration;
- selected only after a causal feasibility/cost/safety decision;
- represented in the order intent and audit trail with its reason;
- disabled when its policy, exchange rules, or evidence are incomplete.

The policy must be adaptive to current deadline, queue/fill evidence, mark/index
quality, position state, and configured fees. Do not encode a fixed timeout or
fee assumption in the execution path.

## 4. Experiment and evidence contract

Each run must persist:

- immutable input/configuration identifiers and hashes;
- Binance rule and fee sources used at decision time;
- event and receipt-time windows, latency, and data-quality status;
- fill model, queue assumptions, funding treatment, and fee attribution;
- policy variant, emergency-taker activations, residual exposure, and reason codes;
- net PnL, drawdown, markout, turnover, costs, and invalid/insufficient-data states.

A result is not evidence that one module is best unless alternatives use the
same data, calendar, fee model, execution assumptions, seeds, and acceptance
gates. Promotion requires out-of-sample or replay evidence plus a reviewable
manifest; a higher return alone is not sufficient.

## 5. Review gates

Every behavioral change needs focused tests and documentation. Before merge:

- formatting, locked tests, clippy, and architecture/resource gates pass;
- no unresolved merge markers or secret material are present;
- configuration changes include schema/version and migration coverage;
- simulation, Testnet, and Production capability boundaries remain explicit;
- the exact commit, configuration hash, data window, and verification commands
  are recorded in the review.

This contract does not authorize Production order submission. Production remains
disabled until its independent operational gates are satisfied.
