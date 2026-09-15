# AnchorBell

AnchorBell is a hard-cut, maker-first decision core for the rule:

> bind a verified external-market close as an anchor, trade only while that
> market is closed, and reduce before the earliest hard deadline.

The normative specification is docs/ANCHORBELL_SYSTEM_SPEC.md.

The unified mathematical structure is documented in docs/ANCHORBELL_MATHEMATICAL_STRUCTURE.md.

## Current architecture

The code has one root model and one dependency direction:

- model.rs: exact integer prices and quantities, Anchor, ClosedWindow,
  AnchorEpisode, BinanceContract, MarketSnapshot, AccountSnapshot, EvidenceFrame,
  CandidateOrder, ValidatedOrder and joint outcomes.
- policy.rs: semantic StrategyPlan, candidate evaluation and wait comparison.
- evolution.rs: immutable A0-A6 invariants and champion/challenger promotion.
- execution.rs: monotone lifecycle and an explicit execution-port boundary.
- ledger.rs: append-only hash-linked evidence ledger.
- runtime.rs: composition root that evaluates frames and records evidence.
- main.rs: build identity and plan identity only.

The core has no credentials, network client, REST/WebSocket dependency or live-order
transport. ExecutionPort is deliberately only a boundary. This commit must not
be interpreted as production-ready trading software; a Binance adapter can be
added only behind the validated-order boundary and only after replay, sealed OOS,
shadow and canary evidence.

## Invariants

Every additional-risk action must prove:

1. one valid AnchorId and AnchorEpisode;
2. an external closed window and a valid entry deadline;
3. fresh, causal and complete EvidenceFrame;
4. current Binance filters, status, position mode, min/max notional bounds
   when supplied, and contract digest;
5. reconciled, fresh account state with a source identity and an executable exit path;
6. robust joint outcome value greater than waiting;
7. maker-only ordinary entry.

Unknown state is fail-closed. Residual exposure is reported, never converted into
a synthetic fill. Strategy evolution may update evidence estimators and plan
artifacts, but cannot change the anchor, closed-market, Binance, causal,
maker-entry or reconciliation invariants.

## Verification

Run from the repository root:

    cargo fmt --all
    cargo test --workspace --all-targets
    cargo run --quiet

The last command prints the build and plan identity; it does not connect to an
exchange and does not submit an order.

## Calibration and Binance legality

Calibration is not a bootstrap constant. CalibrationState is an ordered bounded
log of observed attempts, fills, side and signed conditional markouts. It has a
schema and model version, JSON persistence, replay validation and a SHA-256
evidence digest. Admission requires 30 effective markouts plus at least 15
complete observations for each side, and uses a chronological train/holdout
split. Before admission, a ColdStart snapshot has zero historical influence and
the entry policy cannot use historical execution evidence.

The account model independently validates reconciliation, observation
freshness and source identity. The Binance contract model explicitly validates
position mode and positionSide, including separate Hedge long/short legs,
post-only GTX time-in-force, reduce-only and closePosition support, conditional
order support, priceProtect/triggerProtect semantics, current filters, contract
freshness and remaining rate-limit budget. A CandidateOrder that fails any of
these checks cannot become a ValidatedOrder. Entry also requires current
open-order capacity and an explicitly enabled self-trade-prevention mode.
Price and quantity alignment follows exchange modulo rules: price modulo
tickSize is zero and quantity modulo stepSize is zero; min/max values are
independent bounds.
and passive reductions carry the visible queue-ahead quantity. Hard-deadline flattening is
a separate IOC reduce-only route with its own validator and ExecutionPort method;
it is never silently treated as an ordinary maker order. The funding boundary is
a typed FundingSchedule with next settlement, interval, observation freshness
and source digest, not a weekday or fixed-eight-hour strategy constant.

The calibrated executable value is:

    V_path = weighted(derived_cycle_net_value)
    U = U_anchor + U_execution + U_timing + U_model
    L = floor(V_path) - U
    V_exec(s) = floor(L * fill_probability_s / 10000) + robust_markout_lower_s

with all terms represented as integer pico-basis-points. This is conditional on
causal evidence and is only compared against wait after the closed-window,
contract and account gates pass.

## Retention safety

Simulation retention compacts only terminal or stale-orphan top-level market,
evidence and FX JSONL streams after provenance hashing and a successful zstd
integrity test. The strategy decision ledger under CORE_V1/records.jsonl is
not a retention target. A running process with the same output root blocks
compaction; a stale run with no writer may be finalized after the minimum age.
Every finalized archive carries the original byte count, line count and SHA-256
alongside the compressed SHA-256, and retention never treats free-space
pressure as permission to remove protected strategy records.
