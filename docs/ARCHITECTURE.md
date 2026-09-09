# AnchorBell Architecture

## 1. Scope and decision authority

AnchorBell is a Rust-first, event-driven industrial quantitative platform for
short-horizon, maker-first trading of Binance equity-related perpetual futures
during periods when the related equity market is closed.

The system seeks temporary, risk-adjusted relative-value opportunities between
an external equity reference and the Binance contract. It is not a generic
grid bot, a directional predictor, or a guarantee of convergence.

External projects and industry conventions are references
only. Final decisions always follow AnchorBell's confirmed requirements:

- Rust owns the live core and all latency-sensitive paths.
- Strategy, market data, risk, execution, replay, and persistence are decoupled.
- New risk may only be created by an explicit maker-first policy.
- Invalid, stale, unknown, or unreconciled state fails closed.
- Production is disabled by default and requires explicit authorization.
- Simulation, replay, backtest, and live use the same typed domain contracts.
- The system does not infer exchange behavior from assumptions when live
  exchange metadata is available.

This document is the architectural authority for the design described here.

## 2. Non-negotiable invariants
1. No taker order can be produced by the normal strategy path.
2. Every opening order passes anchor, session, pricing-mode, signal, inventory,
   portfolio, exchange-filter, and maker-first gates.
3. No new risk is allowed when market data or the external anchor is stale.
4. Unknown order, position, account, or connection state cannot increase risk.
5. Equity-market opening and funding settlement are independent risk clocks.
6. The earlier effective flatten deadline always wins.
7. Flattening remains maker-first unless a separately approved emergency policy
   explicitly changes that invariant.
8. The system must expose residual exposure when maker-first flattening cannot
   guarantee completion before a deadline.
9. Hot paths never synchronously write SQLite, JSONL, Parquet, or other media.
10. Credentials never enter source code, logs, commits, reports, or replay data.
11. Binance network I/O exists only in exchange adapters and supervisors.
12. Strategy code cannot directly open sockets or call exchange endpoints.
13. Testnet and production are separate typed environments.
14. Every report records data, model, configuration, code, and time assumptions.
15. External functionality is rejected when it weakens these invariants.
16. A Hong Kong issuer with active ADR/ADS price discovery cannot enter the FrozenClose universe; unknown market quality fails closed.

## 3. Conceptual architecture

The architecture is organized as independent planes connected by typed events.

```
Instrument and Calendar
          |
          v
Equity Anchor -----> Futures Market Data          |                  |
          +---------> Strategy and Risk
                              |
                              v
                       Execution Gateway
                              |
                              v
                  Lifecycle and Reconciliation
                              |
                              v
                 Evidence, Replay, and Reporting
```

The live path is:

```
receive -> normalize -> route -> update snapshot -> evaluate gates
-> create order intent -> validate -> submit -> receive lifecycle event
-> update position -> reconcile -> audit
```

Recorders, metrics, reports, and checkpoints consume copied typed events on
asynchronous bounded paths. They cannot block or alter trading decisions.

## 3.1 Credential and session boundary

The dashboard has two separate credential lifetimes:

- `PersistentCredentialStore` is an execution-side adapter. On Windows it uses
  the current user's Credential Manager, with independent entries for `testnet`
  and `production`.
- `DashboardSession` is process memory only. Saving a credential loads it into
  the current session; clearing the session does not delete the stored entry.
- The UI never reads a secret back. It exposes only `has_credentials`,
  `saved_credentials`, and store availability.
- Blank credentials on an explicit session apply request mean “load the selected
  environment's stored credential”; a missing or invalid entry fails closed.
- The credential adapter is not on the market or decision hot path. It is only
  used during startup, explicit save/delete, status inspection, or session apply.
- Non-Windows builds do not fall back to plaintext persistence. They report the
  secure store as unavailable.

This boundary preserves operator convenience without turning ordinary project
files, browser storage, logs, or Git history into a secret store.

## 4. Domain boundaries

### 4.1 Instrument domain
The instrument catalog is the authoritative mapping between:

- external equity identifier;
- Binance contract symbol;
- contract type and settlement asset;
- price and quantity filters;
- minimum notional and percent-price rules;
- leverage and margin metadata;
- trading schedule;
- pricing-mode metadata;
- funding schedule metadata;
- catalog version and observed timestamp;
- issuer-level ADR/ADS status and evidence timestamp;
- ADR price-discovery classification and observation window.

Every strategy decision carries an instrument and catalog version. A changed
mapping or filter invalidates the affected decision rather than silently
reusing stale metadata.

The public Binance capability gate treats `exchangeInfo` as authoritative for
order admission. A runtime-eligible symbol must expose valid
`PRICE_FILTER`, `LOT_SIZE`, `MIN_NOTIONAL`, and `PERCENT_PRICE` fields;
precision fields alone are insufficient. The accompanying book, mark/index,
and funding snapshot carries an observation timestamp and expires after five
seconds unless refreshed. Batch refreshes use bounded concurrency and preserve
input order, so a larger universe does not turn metadata refresh into an
unbounded fan-out. Each market shard also has a bounded read-silence timeout;
when no frame arrives within that budget, the shard is recycled through the
connection supervisor instead of waiting forever. A successful WebSocket
connection without a successful capability snapshot is not a tradable state.

`MarketCapabilityGate` is the typed composition boundary for this rule:
it declares the exact execution universe, accepts only snapshots for declared
symbols, validates every snapshot before admission, and exposes readiness plus
missing symbols. An invalid refresh removes the symbol's previous ready state;
there is no stale fallback. Strategy and execution may be composed only after
this gate is ready for the complete universe.

The reviewed catalog currently contains fifteen Binance TradFi instruments:
two A-share instruments and thirteen Hong Kong-region instruments. The
FrozenClose execution universe hard-excludes six Hong Kong issuer mappings
whose ADR/ADS markets currently provide active price discovery; five additional
issuers have ADR/ADS evidence but currently inactive, stale, or ineffective OTC
markets and remain eligible only under the recorded market-quality policy. The
A-share and Hong Kong groups are separate typed regions. They
use independent exchange-local session calendars because their morning, lunch,
afternoon, holiday, and flatten deadlines are not interchangeable. The Testnet
catalog is an environment capability, not the source of truth for the production
universe; unavailable Testnet instruments remain non-tradable until Binance
exposes them there.

### 4.2 Price domain

Prices, quantities, rates, and notionals use typed scaled integers or exact
decimal values. Floating-point values are not used for order validation,
position accounting, thresholds, or risk limits.

Required price roles:

- `E`: external equity reference;
- `I`: Binance Price Index;
- `M`: Binance Mark Price;- `Q`: executable bid, ask, or candidate quote.

The system keeps these prices separate. Mark Price is relevant to unrealized
PnL and liquidation, but is not automatically the external fair value.

### 4.3 Session and pricing-mode domain

Each contract has an explicit state such as:

- Regular;
- PreMarket;
- AfterHours;
- Overnight;
- Fixed;
- OrderbookEwma;
- Transition;
- Weekend;
- Holiday;
- Unknown.

Transition and Unknown are risk states, not ordinary trading sessions. They
disable new entries until the mode and data quality are valid.

The calendar provider supplies exchange and underlying-market sessions. Local
weekday logic is never the authority for funding or trading availability.

## 5. Binance TradFi pricing model
Binance traditional-asset perpetuals can use different Price Index and Mark
Price mechanisms across normal hours, pre-market, after-hours, overnight,
weekends, holidays, and low-liquidity conditions. The system must therefore
model pricing mode as data, not as a constant.

The external reference is an anchor, not an eternal fair value. During a
non-regular mode, the fair-value estimator may reduce external-anchor weight
and increase the weight of Binance index, mark, order-book, FX, dividend, and
mode-specific information.

The estimator must expose:

- fair value;
- source components;
- component age;
- uncertainty;
- mode;
- transition status;
- confidence;
- rejection reason.

No signal is valid when the estimator cannot explain the fair value it
produces.

## 6. Funding schedule and settlement

Funding is a per-contract event stream, not a global timer.
For each contract, maintain:

- `next_funding_time`;
- `funding_interval_hours`, when supplied;
- latest displayed rate;
- estimated next rate;
- last settled rate;
- rate type: Regular, Special, or Unknown;
- mark price associated with settlement;
- source and observation time;
- schedule version.

The adapter obtains schedule and rate information from exchange metadata and
market streams. The normal UTC 8-hour pattern is only a default assumption;
it must never override symbol-level exchange metadata.

A funding history record can include Regular or Special funding. Equity-related
perpetuals may also require dividend-related special adjustments. The funding
model must preserve that distinction and must not treat the latest displayed
rate as a guaranteed next settlement rate.

Expected funding cost is:

```
expected_funding_cost
= expected_rate * position_notional
+ funding_uncertainty_penalty
```
Actual settled cost is recorded from exchange truth whenever available:

```
funding_payment
= actual_settled_rate * position_notional_at_settlement
```

### Weekend rule

A weekend or holiday means that the underlying equity market may be closed.
It does not prove that Binance has no funding event.

Therefore:

- Weekend and Holiday affect the equity session and pricing mode.
- Funding existence is determined by `next_funding_time`.
- Funding cadence is determined by symbol-level metadata.
- Missing or contradictory funding metadata fails closed for new risk.
- The engine records observed funding events to validate assumptions.
- Backtests use historical funding records, not weekday-derived guesses.

## 7. Dual flatten policy

The system has two independent flatten triggers.

### 7.1 Equity opening flatten
For each mapped equity market:

```
equity_open_deadline = regular_open_time - 30 minutes
```

At this point:

- stop all new entries;
- cancel all risk-increasing orders;
- lower permitted inventory;
- start passive reduce-only flattening;
- shorten order TTL;
- increase exit scheduling priority.

The thirty-minute point starts the exit process; it is not a promise that
every position will fill instantly.

### 7.2 Funding settlement flatten

For each contract with a valid next funding event:

```
funding_deadline = next_funding_time - 5 minutes
```

At this point:
- stop all new entries;
- cancel all orders that can increase risk;
- submit or refresh only reduce-only maker intents;
- record funding avoidance state;
- escalate if the position remains open.

A contract with no funding event currently reported does not receive a
weekday-based synthetic funding deadline.

### 7.3 Effective deadline

The earliest deadline wins:

```
effective_flatten_deadline
= min(equity_open_deadline, funding_deadline)
```

The scheduler must also account for estimated time to flatten:

```
flatten_start
= min(
    effective_flatten_deadline,
    deadline - estimated_flatten_duration - safety_buffer
  )
```
Estimated duration is derived from observed partial-fill speed, queue
position, liquidity, order size, latency, and current market regime.

### 7.4 Flatten state machine

```
Trading
  -> StopNewRisk
  -> CancelRiskIncreasing
  -> PassiveReduceOnly
  -> Flat
```

If the deadline is reached with exposure remaining:

```
ResidualExposure
  -> FreezeNewRisk
  -> HighPriorityAlert
  -> ContinueReduceOnly
  -> ReconcileAfterEvent
```

Maker-only execution cannot guarantee immediate exit in an illiquid or
one-sided market. The system must report this limitation rather than encode a
false stop guarantee.

## 8. Relative-value signal
The basic decompositions are:

```
external_gap = (E - I) / I
market_basis  = (Q - I) / I
total_gap     = (Q - E) / E
```

A signal is valid only when:

- external anchor is fresh and valid;
- instrument mapping is current;
- Price Index and Mark Price are available;
- pricing mode is known and permitted;
- order book is fresh and internally consistent;
- volatility and spread are within limits;
- the gap persists through a confirmation window;
- expected net edge exceeds the dynamic threshold.

The dynamic threshold is:

```
threshold = max(
    fees
  + expected_funding
  + spread
  + latency  + anchor_uncertainty
  + adverse_selection
  + liquidity
  + safety_margin,
    statistical_threshold
)
```

Statistical thresholds are contract-specific and mode-specific. They can use
rolling median, MAD, volatility, percentiles, half-life, and recent realized
convergence. A single global percentage is not sufficient.

The expected-value gate is:

```
expected_trade_value =
    expected_reversion * fill_probability * confidence
  - fees
  - funding
  - spread
  - latency
  - adverse_selection
  - inventory_cost
  - capital_cost
```

Only positive expected value with sufficient safety margin can create an order
intent. A high fill probability alone is not a valid reason to quote.
## 9. Quote optimization and inventory

The strategy produces candidate maker quotes rather than a binary buy/sell
command.

For every candidate price and quantity:

```
quote_score =
    expected_net_edge
  * fill_probability
  * signal_confidence
  - inventory_penalty
  - adverse_selection_penalty
  - latency_penalty
  - capital_penalty
```

Inventory-adjusted reservation value:

```
reservation_value
= fair_value
- inventory_penalty
- time_penalty
- risk_penalty
```
When long inventory is excessive:

- reduce or remove bids;
- reduce bid size;
- make passive offers more effective;
- prioritize sell-to-reduce intents.

When short inventory is excessive, apply the opposite behavior.

No infinite averaging, uncontrolled grid expansion, or martingale recovery is
allowed. Entry layers are bounded by symbol, group, account, and portfolio
budgets.

## 10. Microstructure models

### 10.1 Queue and fill model

The fill model tracks:

- quantity ahead at the price level;
- new orders joining the queue;
- cancellations ahead;
- aggressive volume consuming the level;
- partial fills;
- time priority;
- network and exchange acknowledgement latency;
- order replacement and queue reset.
A touch of the best bid or ask does not imply a fill.

### 10.2 Adverse-selection model

For every fill, record markout at multiple horizons:

- 100 milliseconds;
- 1 second;
- 5 seconds;
- 30 seconds.

The model estimates whether the fill was followed by unfavorable price
movement. If a quote level has high fill probability but poor post-fill
returns, the strategy widens, reduces quantity, raises its threshold, or
pauses that side.

### 10.3 Order refresh

Small fair-value changes do not automatically cause cancellation and
replacement. Replacing an order can lose queue priority and consume exchange
rate budget. Refresh decisions use materiality, remaining edge, queue value,
risk state, and deadline pressure.

During flattening, risk reduction takes priority over queue preservation.

## 11. Execution architecture
The strategy emits an immutable `OrderIntent`. The execution layer performs:

1. maker-first capability validation;
2. reduce-only and position-side validation;
3. price and quantity filter validation;
4. notional and rate-limit validation;
5. idempotency and client-order-id assignment;
6. signing;
7. transport;
8. response correlation and status validation;
9. lifecycle event emission.

The order lifecycle is explicit:

```
Planned
 -> Submitted
 -> AckPending
 -> Working
 -> PartiallyFilled
 -> Filled
 -> CancelPending
 -> Canceled
```

Error and uncertainty states are also first-class:

```Rejected
Unknown
ReconciliationRequired
RecoveryBlocked
```

Unknown cannot be interpreted as canceled, filled, or harmless.

The Binance order transport validates order.place twice: before network I/O,
the payload must contain a complete LIMIT + GTX maker request; after a
successful exchange response, the response must have HTTP-like status 200
and an identity-complete result (orderId, symbol, client order ID, and status)
whose symbol and client order ID exactly match the request. Anything else is an
explicit transport error and cannot create a lifecycle fill.
An unbound gateway reports Unavailable rather than manufacturing acceptance.

## 12. Reconciliation and recovery

The account supervisor maintains exchange truth for:

- open orders;
- order status;
- positions;
- balances;
- margin;
- funding charges;
- connection state.

After disconnect, timeout, restart, or ambiguous response:

1. stop new risk;
2. preserve durable intent and correlation identifiers;
3. reconnect;
4. query exchange state;
5. reconcile orders and positions;
6. resolve unknowns;
7. cancel unsafe orders and explicitly apply remote terminal status;
8. repeat snapshot/reconciliation after cancellation or any fill delta;
9. resume only after all required gates pass.

User data streams require keepalive and reconnection supervision. A recovered
connection is not equivalent to a recovered account state.

## 13. Scale and concurrency

The system supports a large contract universe without one operating-system
thread per contract.

### 13.1 Data planes

- Equity Anchor Plane: lower-frequency close/open/reference updates.
- Futures Market Plane: high-frequency book, trade, mark, funding, and status.
- Strategy and Risk Plane: event-driven per-contract decisions.
- Execution Plane: shared order and user-data connection pools.
- Evidence Plane: asynchronous recording, metrics, and audit.

### 13.2 Sharding

- Map `contract_id` to a shard in O(1).
- Run one event loop per shard or bounded runtime worker.
- Keep hot snapshots in memory.
- Use bounded queues and explicit backpressure.
- Batch compatible computations.
- Avoid global locks in the market-data hot path.
- Keep slow persistence and reports off the decision path.

The market adapter exposes a deterministic subscription planner. It sorts and
normalizes symbols, rejects duplicates and empty streams, and partitions the
universe into bounded subscription shards before any socket is opened. Each
shard retains the same proxy, timeout, reconnect, and frame limits so the
runtime can supervise them independently. This is a transport-scale boundary;
it does not grant a shard permission to bypass per-contract risk gates. The plan also builds a symbol-to-shard index for average O(1) routing, so dispatch does not scan the full universe on every event.
### 13.3 Priority

When resources are constrained:

```
forced flatten
> reduce-only
> cancel risk-increasing
> refresh existing quote
> new entry
```

Budgets exist at:

- contract;
- equity group;
- shard;
- account;
- global connection;
- global order-rate level.

## 14. Backtest and replay

Backtests must be event-driven and execution-aware. Required inputs and
models include:

- tick or event data;- order-book updates;
- external anchor;
- Price Index;
- Mark Price;
- funding events and actual historical rates;
- equity schedule and holidays;
- pricing-mode transitions;
- queue position;
- partial fills;
- latency;
- cancel latency;
- maker fees;
- margin and funding;
- stale data and disconnects;
- exchange filters;
- delistings and symbol changes.

A replay uses the same strategy and risk contracts as live. Only the data
source, clock, execution adapter, and fill simulator are replaced.

Every report contains:

- dataset digest;
- configuration digest;
- strategy and model version;
- source commit SHA;
- time range;
- assumptions;- fill and latency model;
- fee and funding model;
- result metrics;
- rejected-event counts.

Required metrics include net PnL, drawdown, inventory peak, inventory duration,
fill ratio, partial-fill ratio, cancel ratio, markout, adverse-selection loss,
funding cost, stale duration, unknown states, residual exposure, and
performance by market mode.

Validation uses walk-forward splits, parameter sensitivity, conservative
fills, latency perturbations, funding perturbations, and stress scenarios.
Sharpe ratio alone is not an acceptance criterion.

## 15. Validation and machine learning boundary

Rules and risk gates own the final authority. Statistical and machine-learning
models may estimate:

- volatility;
- mean-reversion half-life;
- fill probability;
- queue hazard;
- adverse selection;
- regime confidence;
- quote size and distance.

They may not bypass:
- maker-first validation;
- anchor validity;
- session flatten;
- funding flatten;
- stale-data guard;
- account limits;
- reconciliation;
- production authorization.

The preferred progression is rules, statistics, survival/fill models, then
supervised microstructure models. End-to-end reinforcement learning is not a
production dependency until its data, simulator, interpretability, and
failure behavior are independently proven.

## 16. Module ownership

| Module | Responsibility |
| --- | --- |
| `domain` | typed prices, quantities, instruments, orders, positions, states |
| `market` | streams, decoding, snapshots, timestamps, reconnect input |
| `reference` | equity anchor, calendar, FX, dividends, corporate actions |
| `strategy` | fair value, basis, regime, thresholds, inventory, quotes |
| `microstructure` | queue, fill, latency, adverse selection |
| `risk` | typed hard-risk decisions and incremental funding/tail overlays; never creates orders |
| `execution` | signing, transport, lifecycle, filters, reconciliation |
| `replay` | ordered events, deterministic clock, checkpoints |
| `backtest` | matching, fill, fee, funding, margin, latency models |
| `runtime` | composition, shards, bounded queues, supervision |
| `observability` | metrics, tracing, audit, health, reports |
| `validation` | offline analysis only; never live hot-path code |

The dependency direction points inward toward typed domain contracts. Adapters
depend on domain ports; domain and strategy do not depend on network clients,
databases, or Python.

## 17. Safety gates

Normal operation is enabled only when all gates pass:

- environment is Testnet by default or explicitly authorized Production;
- Production read-only access and order submission use separate policy gates;
- Testnet and Production load different credential environment variables;
- instrument and filters are current;
- anchor is valid and fresh;
- pricing mode is allowed;
- market data is fresh;
- funding schedule is known or explicitly irrelevant;
- position and order state are reconciled;
- account and portfolio budgets are available;
- maker-first capability is confirmed;
- no flatten deadline is active;
- no recovery or kill-switch state is active.

Any gate failure prevents new risk. Reduce-only recovery remains available
when the execution adapter can safely provide it.

## 18. Verification and definition of done
A change is complete only with:

- typed implementation;
- focused unit and property tests;
- adapter contract tests;
- deterministic replay tests;
- fake-exchange lifecycle tests;
- stale, funding, weekend, holiday, and flatten tests;
- concurrency and bounded-queue tests;
- reconnect and crash-recovery tests;
- documentation;
- exact commit SHA;
- reproducible verification command.

Minimum flatten test matrix:

| Scenario | Expected result |
| --- | --- |
| Normal session, valid anchor | New maker risk may be evaluated |
| Equity open within 30 minutes | Stop new risk and reduce only |
| Funding within 5 minutes | Stop new risk and reduce only |
| Both deadlines active | Earlier deadline controls |
| Weekend without reported funding | No synthetic funding event; weekend guard applies |
| Weekend with reported funding | Funding flatten applies |
| Unknown funding schedule | No new risk; alert and reconcile |
| Stale anchor | No new risk |
| Stale market data | No new risk |
| Residual position at deadline | ResidualExposure and high-priority alert |
| Disconnect during flatten | Recovery first; no new risk |
| Special funding record | Preserve and account separately |
| Maker order not filled | Never claim flat without exchange evidence |

## 19. External reference policy

NautilusTrader informs event-driven live/backtest parity and Rust order-book
boundaries. Hummingbot informs inventory skew and connector separation.
LEAN informs pluggable fill, fee, slippage, and margin models. ABIDES informs
high-fidelity event-driven market simulation, price-time priority, and latency.

These references do not authorize importing:

- Python into the live hot path;
- a simple touch-equals-fill assumption;
- unrestricted grids or martingale;
- unrestricted taker execution;
- generic abstractions that weaken AnchorBell's gates;
- weekday-based funding assumptions.

## 20. Operational release ladder

1. Validation-only.
2. Deterministic replay and execution-aware backtest.
3. Simulation gateway.
4. Binance Testnet or Demo evidence.
5. Production-disabled release.
6. Separate production safety review, only if explicitly requested.

Passing a backtest or Testnet scenario does not authorize production trading.
Production remains disabled until an explicit, separately recorded
authorization and safety review exist.


## 21. Validation addendum: what must be promoted into architecture

Recent limit-order validation and mature execution systems converge on one
principle: the value of a maker order is conditional on its state, queue,
latency, fill outcome, and inventory context. Fill probability is not a
sufficient objective.

AnchorBell therefore promotes the following from validation ideas into typed
runtime contracts:

- conditional order value;
- probabilistic queue state;
- feed, decision, and exchange latency;
- post-fill markout;
- flatten feasibility;
- lower-confidence-bound risk decisions;
- model and data drift detection.

The strategy remains deterministic at the authority boundary. Models estimate
quantities; typed policy gates decide whether an order is allowed.

## 22. Conditional order value

For each candidate order, maintain an immutable evaluation:

```
order_value =    P(fill | state, price, size, latency)
  * E(net_outcome | fill, state)
  - inventory_cost
  - deadline_cost
  - capital_cost
```

The state includes:

- side and price level;
- queue-ahead estimate;
- queue-behind estimate;
- opposing queue size;
- recent trade flow;
- order-book imbalance;
- volatility;
- spread;
- anchor confidence;
- pricing mode;
- funding distance;
- equity-open distance;
- current inventory;
- expected time in position.

This replaces the unsafe rule:

```
high fill probability => good order```

The system only quotes when conditional order value remains positive after
costs and the lower confidence bound exceeds the configured safety margin.

The order evaluator is a pure port. It can be implemented first with
deterministic statistics, then calibrated with survival or hazard models. A
model may not directly submit an order.

## 23. Queue observability and uncertainty

Binance market data generally exposes aggregated price-level information rather
than the complete identity and lifetime of every resting order. Exact queue
position is therefore not observable in the same way as an L3 market-by-order
feed.

AnchorBell must never claim exact queue position when only L1/L2 evidence is
available. It maintains:

```
queue_ahead_estimate
queue_ahead_lower_bound
queue_ahead_upper_bound
queue_confidence
```

The fill simulator runs at least three cases:
- optimistic queue consumption;
- central estimate;
- conservative queue consumption.

Production policy uses the conservative or lower-confidence result. Backtest
reports all three so that a strategy cannot hide sensitivity to queue
assumptions.

When a data source later provides order-level information, it may implement a
stronger queue adapter without changing strategy or risk contracts. Nautilus
Trader explicitly distinguishes L1, L2, and L3 order-book representations;
AnchorBell follows the same capability distinction while remaining Binance
specific.

## 24. Latency as a first-class risk variable

Latency is not one fixed benchmark number. Maintain separate measurements for:

- exchange event time to local receipt;
- local receipt to normalized event;
- event to strategy decision;
- decision to order serialization;
- serialization to socket write;
- socket write to exchange acknowledgement;
- acknowledgement to local lifecycle event;
- cancel request to cancel confirmation.

Each quote carries:
- event-time age;
- local monotonic age;
- estimated exchange round-trip;
- expiry budget;
- data completeness;
- clock-skew status.

A quote is invalid if its expected edge is smaller than the value that can be
lost during its latency budget. A latency spike reduces quote size, widens
quotes, or disables new risk.

Latency perturbation is mandatory in backtests. A result that only works at
zero latency is not evidence.

## 25. Flatten feasibility, not only flatten deadlines

A deadline alone is insufficient for maker-first execution. Every live position
must expose:

```
estimated_time_to_flat
estimated_fillable_quantity
flatten_confidence
deadline_slack
```

The scheduler computes:
```
deadline_slack
= effective_flatten_deadline
- now
- estimated_time_to_flat
- safety_buffer
```

Policy:

- positive slack: continue staged passive reduction;
- low slack: cancel risk-increasing orders and increase reduce-only priority;
- negative slack: enter ResidualExposure before the deadline;
- unknown feasibility: fail closed and alert.

The estimator uses recent realized reduction speed, current depth, partial-fill
rate, queue uncertainty, volatility, and connection health. It must not use a
constant assumption for every contract.

This rule is especially important for low-liquidity equity perpetuals and for
the funding deadline, where maker-first execution cannot guarantee completion.

## 26. Lower-confidence-bound decision policy

Point estimates are too fragile for production. For every material model output,
store:
- point estimate;
- lower bound;
- upper bound;
- sample count;
- calibration window;
- model version;
- data age;
- drift status.

Examples:

```
fill_probability_lcb
reversion_return_lcb
adverse_selection_ucb
time_to_flat_ucb
anchor_uncertainty_ucb
```

A new entry is permitted only if the conservative combination remains valid:

```
net_edge_lcb
= reversion_return_lcb
- cost_ucb
- adverse_selection_ucb
- inventory_cost_ucb
- latency_cost_ucb```

The risk engine uses upper bounds for losses and lower bounds for gains. This
prevents a small sample of favorable fills from authorizing oversized risk.

## 27. Calibration and model governance

Validation models need a separate calibration plane:

```
raw events -> feature snapshots -> offline calibration
-> versioned model artifact -> shadow evaluation
-> approval evidence -> runtime policy
```

Every model artifact records:

- source dataset digest;
- feature schema;
- training or calibration period;
- target definition;
- code commit;
- hyperparameters;
- validation splits;
- calibration metrics;
- known failure regimes;
- expiration or review time.
Runtime model changes are immutable and auditable. Online learning cannot mutate
production behavior without producing a new version and passing the same
evidence gate.

Use champion/challenger evaluation in shadow mode. The challenger may produce
advisory decisions, but only the approved champion can influence the live
policy, and only through typed bounded fields.

## 28. Mature-solution mapping

The following practices are adopted selectively:

| Reference | Adopt | Boundary |
| --- | --- | --- |
| hftbacktest | full tick/book replay, queue, feed and order latency | does not define AnchorBell strategy or safety |
| NautilusTrader | event-driven parity, typed data/execution boundaries, L1/L2/L3 capability model | no generic multi-venue expansion in the core |
| Hummingbot | inventory skew, connector isolation, controlled refresh | simple PMM is insufficient for external anchors |
| LEAN | replaceable fill, fee, slippage, margin, and funding models | default bar-based fills are not accepted |
| ABIDES | high-fidelity exchange and agent simulation for validation | not a production execution dependency |

The architectural test for every imported practice is:

1. Does it solve a defined AnchorBell problem?
2. Does it preserve maker-first behavior?
3. Does it preserve fail-closed risk?
4. Does it preserve live/replay contract parity?
5. Can its result be reproduced and independently audited?
If any answer is no, the practice remains a validation reference only.

## 29. New acceptance metrics

In addition to PnL and drawdown, every strategy version must report:

- conditional order value by price level;
- fill probability versus post-fill markout;
- markout at 100ms, 1s, 5s, and 30s;
- queue-estimate sensitivity;
- optimistic, central, and conservative fill results;
- latency sensitivity;
- time-to-flat error;
- deadline-slack distribution;
- funding-avoidance success;
- residual exposure rate;
- anchor confidence by pricing mode;
- model calibration error;
- drift and out-of-distribution counts.

A strategy is not accepted when its profitability disappears under conservative
queue, realistic latency, or lower-confidence-bound assumptions.

## 30. Required implementation consequences

The architecture now requires these ports and value objects:

- `ConditionalOrderValue`;
- `QueueEstimate`;
- `LatencyBudget`;
- `MarkoutObservation`;
- `FlattenFeasibility`;
- `FundingSchedule`;
- `FundingScheduleStatus`;
- `ModelEvidence`;
- `ConfidenceInterval`;
- `DataQualityStatus`;
- `ResidualExposure`.

The quote scheduler must consume these objects instead of directly reading raw
market fields. The risk engine must be able to reject an otherwise profitable
quote because its queue uncertainty, latency, or flatten feasibility is unsafe.

FundingSchedule must also carry an explicit status: `Scheduled`, `NoEvent`, or
`Unknown`. A missing next-funding timestamp is `Unknown` by default and therefore
blocks new risk. Only an independently verified exchange state may construct
`NoEvent`; this prevents a weekend or holiday assumption from silently authorizing
entries.

The backtest engine must use the same objects with simulated observations.
This keeps the implementation honest: any quantity required for live
authorization must also exist in replay evidence.

## 31. Final validation principle

The system should not ask:

```
Will price eventually return?
```

It should ask:
```
Given this exact market state, queue uncertainty, latency, inventory,
funding clock, equity session, and exit feasibility, is this particular maker
order still worth submitting under conservative assumptions?
```

That is the boundary between a simple deviation bot and a production-grade,
evidence-driven relative-value market-making system.
