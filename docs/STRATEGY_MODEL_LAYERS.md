# AnchorBell Strategy Model Layers

This document is the coverage map for the AnchorBell strategy. It prevents
local improvements from silently bypassing data truth, execution economics,
risk, or evidence requirements.

## 1. Authority and dependency rule

The strategy is a seven-layer causal DAG. A later layer may consume an earlier
layer's typed state, but analytics and evidence can never create order
authority or mutate an earlier state.

```text
L0 Truth and contracts
        ↓
L1 State estimation and uncertainty
        ↓
L2 Alpha and admission
        ↓
L3 Execution economics
        ↓
L4 Position, portfolio, and deadline risk
        ↓
L5 Accounting and lifecycle completion
        ↓
L6 Evidence, promotion, and observability
```

CORE_V1 uses the validated L0-L5 path plus the permanent F1/F5/F7 safety
overlays. F2/F3/F4/F6 remain challengers. M8 is an L4 funding overlay and is
blocked until symbol-level funding metadata is complete. M9 is an isolated
challenger and is blocked until independent OOS calibration reaches its sample
floor. L6 is non-authoritative: it can reject promotion but cannot place or
resize an order.

## 2. Complete module inventory

| Layer | Module | Responsibility | Current implementation | Mathematical upgrade target |
|---|---|---|---|---|
| L0 | Instrument truth | Symbol, asset class, venue, Binance contract identity | `market/instrument_registry.rs`, `strategy/universe.rs` | Versioned capability joins; fail closed on unknown mappings |
| L0 | Exchange metadata | Filters, position mode, fees, endpoints, weights | `market/metadata.rs`, `execution/binance_runtime_config` | Snapshot validity intervals and no stale fallback |
| L0 | Market-data integrity | Sequence, freshness, book/mark/index consistency | `market/freshness.rs`, `orderbook.rs`, `runtime.rs` | Causal event-time/receipt-time separation and integrity state |
| L0 | Anchor and calendar truth | External close, FX, session and flatten deadlines | `reference_authority.rs`, `calendar.rs`, `historical.rs` | Immutable closure episodes and region-specific clocks |
| L1 | Fair-value fusion | Anchor/index/mark/mid reference estimate | `strategy/reference_model.rs` | Robust weighted fusion with dispersion-aware confidence |
| L1 | Regime state | Calm, normal, stressed, dislocated and transition states | reference model + runtime | Hysteresis, persistence and state-transition penalties |
| L1 | Residual dynamics | Residual magnitude, drift, reversion and half-life | `calibration.rs`, runtime | Causal robust state-space / mean-reversion evidence |
| L1 | Execution feedback | Markout, fill hazard, queue and latency observations | `calibration.rs`, runtime | Wilson bounds, tail quantiles, serial-effective samples |
| L2 | Edge signal | Anchor-relative executable buy/sell edge | `signal_policy.rs`, `anchor_maker.rs` | Net-edge after uncertainty, adverse selection and deadline cost |
| L2 | Directional microstructure | Queue imbalance and trend conflict by side | `signal_policy.rs`, runtime | Side-specific robust conflict model and peer-region factor |
| L2 | Evidence admission | F7 evidence gate and explainable rejection | runtime, observability | Graded data/risk/evidence states; never relax on raw order count |
| L3 | Quote construction | Maker-first price and post-only intent | `quote_engine.rs`, `maker_exit.rs` | Quote distance as a constrained optimization variable |
| L3 | Queue/fill model | Queue ahead, trade-through and fill probability | `backtest_realism.rs`, runtime; queue probability now continuously sizes Core V1 | Censored survival / competing-risk treatment of partial fills |
| L3 | Exchange feasibility | Price, quantity, notional and percent filters | `market/metadata.rs`, `simulation/runtime.rs` pre-admission + final validation | Exact projection onto the discrete Binance-valid feasible set; fail closed when it exceeds risk capacity |
| L3 | Cost model | Maker/taker fees, slippage and adverse selection | `execution`, runtime | Net-of-cost edge and fee-regime versioning |
| L4 | Inventory control | Position cap, skew and reduction priority | `strategy/inventory.rs`, runtime | Convex risk budget and monotone reducing path |
| L4 | Cross-symbol/region risk | Common-mode concentration and region factor | runtime | Robust covariance shrinkage and factor-neutral allocation |
| L4 | Tail and drawdown | F5 tail, symbol/portfolio drawdown and hard stops | runtime, `portfolio_guard.rs` | Expected shortfall, path-dependent drawdown and ruin bounds |
| L4 | Funding/deadline flatten | M8 funding overlay and earliest deadline | `m8.rs`, runtime, `flatten.rs` | Event-time funding uncertainty and deadline-constrained control |
| L5 | PnL/accounting | Realized, unrealized, funding, fees and strategy alpha | runtime, `execution/pnl.rs` | Strict attribution: market beta versus execution residual |
| L5 | Lifecycle/reconciliation | Order state, position truth and recovery | `execution/reconciliation.rs`, `recovery.rs` | Unknown-state monotonic risk reduction |
| L5 | Flat completion | End-of-run flatness and residual exposure alarm | runtime, `flatten.rs` | Completion as a hard terminal condition, not a report field; unknown maker fill confidence fails closed and residual flatten attempts are explicit |
| L6 | Method lineage | Core, challenger, ablation and overlay identities | `method_catalog.rs`, `experiment_plan.rs` | Paired event-tape comparisons with immutable lineage |
| L6 | Calibration/OOS | Rolling calibration and independent folds | `calibration.rs`, `oos_validation.rs` | Hierarchical shrinkage, block dependence and finite-sample bounds |
| L6 | Stress/promotion | Candidate selection and promotion barriers | `promotion_policy.rs` | Worst-fold and tail-survival constraints before ranking |
| L6 | Audit/dashboard | Rejection reasons, delay, book, funding and per-symbol metrics | `observability.rs`, `engine/web` | Schema-driven views; every metric carries source/time/version |

Total coverage: **7 layers, 26 modules**. No strategy decision is considered
complete until it has a path through all applicable L0-L5 modules and an L6
evidence record.

## 3. Mathematical model stack

The implementation order is causal and conservative:

1. **Typed state and robust observation:** exact scaled integers, event-time
   ordering, median/MAD, Huber-style bounded influence, and explicit missing
   state.
2. **Dependent-data inference:** HAC/Newey-West effective samples, Wilson
   one-sided bounds, block/bootstrap-style fold separation, and tail/ES
   metrics. IID assumptions are not permitted for event-driven data.
3. **Regime and residual process:** finite-state regime transitions with
   hysteresis; signed residual drift, reversion evidence, and adverse
   selection as separate causal processes.
4. **Hierarchical cross-section:** symbol estimates shrink toward their
   region factor only when peer observations are present; a single symbol
   cannot set the portfolio's belief.
5. **Constrained control:** quote size and inventory are solutions of monotone
   bounded risk budgets. Reducing orders are never penalized by risk-increasing
   overlays.
6. **Promotion logic:** economic alpha is separated from market direction;
   only independent OOS/stress evidence can change a method's disposition.

Complex mathematics is admitted only when its parameters are observable in the
causal event tape. A more complicated formula without identifiable data is a
model risk, not an improvement.

## 4. Upgrade order

The next implementation passes follow the dependency graph:

1. Finish L0/L1 state contracts and schema-driven telemetry.
2. Upgrade L2 edge/evidence admission using the state estimates.
3. Upgrade L3 queue, cost and exact exchange-feasible projection.
4. Upgrade L4 factor-neutral inventory, tail and deadline control.
5. Upgrade L5 attribution, reconciliation and terminal flatness.
6. Upgrade L6 paired OOS/stress/promotion and Dashboard analysis.

Each pass must add a pure test for monotonicity, causality, fail-closed
behavior, and reducing-path preservation before the next layer is changed.
