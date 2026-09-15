# AnchorBell Unified System Specification

Status: hard-cut refactor baseline; version system-v1; date 2026-09-14.

This is the only normative specification after the refactor. Earlier strategy
documents are historical research. Old APIs, configurations and compatibility
layers are not preserved.

## 1. Decision theorem

AnchorBell trades Binance USD-M perpetuals against the last completed close of
an A-share or Hong-Kong equity market. New risk exists only while that market
is closed. Ordinary entry is maker-only.

tradable = valid_anchor AND external_closed AND fresh_complete_information
           AND verified_binance_contract AND reconciled_account
           AND positive_robust_exitable_value

Every action belongs to one AnchorEpisode. No episode means no entry. No
ValidatedOrder means no execution. A local fill observation never changes
position until exchange authority confirms it.

Let F_t be all ordered and validated information available at time t. A
complete outcome path contains queue, partial fills, markout, anchor endpoint,
funding, exit fills and deadlines. Choose the action with the largest robust
conditional value, and accept only when it beats the robust value of wait:

    a* = argmax lower_value(a | F_t)
    lower_value(a*) > lower_value(wait | F_t)

## 2. Reality to canonical object

completed external close plus ClosureEvidence -> Anchor
one closed period plus bound closure evidence -> AnchorEpisode
Binance rules at time t -> BinanceContract
book, index, mark, freshness -> MarketSnapshot
account, reconciliation time and remote orders -> AccountSnapshot
proposal -> CandidateOrder
rule-checked proposal -> ValidatedOrder
full fill/markout/exit path -> OutcomeDistribution
all input facts -> EvidenceFrame
evolvable semantic policy -> StrategyPlan

## 3. Axioms

Axiom A0, time and causality: every event has exchange time, sequence when
available, and unique id; decisions use only F_t; duplicate channels are
deduplicated; an order lifecycle also rejects a repeated event id after later
progress; local clocks are timeout guards; drift, gaps or conflicts halt new
risk.

Axiom A1, anchor: the anchor is a completed final close of a specified
instrument, venue, timezone and date. Calendar and source completion are both
required. Holiday, half-day, halt and missing-data cases are not silently
filled. FX, corporate action, contract conversion and carry are separate
recorded transforms. Every action in an episode references one AnchorId.

Axiom A2, closed window: closed is calendar state plus venue state plus data
freshness, not absence of quotes. Entry is allowed only after close and before
pre-open, auction and flatten windows. After entry_deadline only reduce-only
actions are legal. The earlier of external open and funding settlement is the
exit boundary. Unknown calendar facts block entry.

Axiom A3, Binance contract: exchangeInfo, status, contract type, filters,
precision, position mode and account permission are inputs. Price satisfies
PRICE_FILTER with price modulo tickSize equal to zero and independent min/max
bounds; quantity satisfies LOT_SIZE with quantity modulo stepSize equal to zero
and independent min/max bounds; notional satisfies MIN_NOTIONAL and, when supplied by NOTIONAL,
also stays below the exchange maximum. Maker
proves non-crossing before send. positionSide, reduceOnly, closePosition and
hedge mode are validated together. Current open-order capacity and an enabled
self-trade-prevention mode are required for additional maker risk. mark, index,
workingType, priceProtect, triggerProtect, rate limits and connection state
are recorded. A stale or inconsistent contract digest blocks new orders.

Axiom A4, information: bid, ask, depth, trade, index, mark, funding, account
state and server time are distinct facts. Book snapshot and deltas prove
continuity. Core prices and quantities use integer minimum units. Freshness and
source identity belong to each stream; a previously reconciled account is not
valid forever.
Missing is not zero and unknown risk is not no risk.

Axiom A5, execution: exchange events and REST reconciliation are authoritative
for position and remote orders. Local lifecycle records intent, never a
fabricated fill. Unknown order, external fill, quantity regression or symbol
mismatch enters halt/reconcile. Ordinary entry is maker-only; taker is an
independent audited reduce-only emergency program.

Axiom A6, risk and exit: position, notional, margin, liquidation buffer,
episode loss and portfolio loss are hard boundaries. Each entry needs an
executable exit path. Residual position at hard flatten is reported, never
claimed flat.

## 4. Structure and semantics

AnchorEpisode contains Anchor, ClosedWindow, BinanceContract, FundingSchedule
and EpisodeLedger. Its states are Unborn, Eligible, Building, Harvesting,
Reducing, Flat and Closed. Unknown can only transition to Reconcile or Halt.

EvidenceFrame contains episode, the explicit ClosureEvidence event, anchor,
calendar, funding, contract snapshot, continuous book, index/mark/server time,
account, remote orders, fees, lifecycle, calibration and model version. Closure
evidence binds the expected close time to an observed event, source digest and
known calendar state; a bare boolean cannot establish closure. Facts are stored
once; inferences cite fact ids. Live, simulation and replay share validation.

StrategyPlan means BindAnchor, RequireClosedWindow, MeasureResidual,
EstimateJointOutcome, AdmitIfRobustValueBeatsWait, QuotePassive,
ReduceAt(earliest equity-open or funding deadline), and ReconcileBeforeResume.
The interpreter emits only Wait, PlaceMaker(ValidatedOrder), or
ReduceOnly(ValidatedOrder).

For a candidate, Y is markout plus anchor convergence minus fees, funding,
adverse selection, latency, exit cost and risk penalty. Value is the infimum of
conditional expected Y over a confidence set. It is not P(fill) times
unconditional alpha.

## 5. Evolution and code boundary

Allowed candidates satisfy Pi_anchor intersect Pi_closed intersect Pi_binance
intersect Pi_risk intersect Pi_causal. Promotion order is legality, all axioms,
survival under stress, robust value over champion and wait, stability, then
simplicity.

collect -> validate -> freeze evidence -> replay/backtest -> sealed OOS
-> shadow challenger -> bounded canary -> champion

Evolution can change evidence estimators and StrategyPlan artifacts, never A0-A6,
permissions, the closed-market principle, reconciliation or the sealed test set.

Code direction is model -> policy -> execution -> runtime, with ledger events
at every boundary. Model has no network, files or Tokio dependency. Decision has
no REST/WebSocket dependency. Adapters cannot change policy meaning.

Release proof: every entry traces to AnchorId, ClosedWindow and contract digest;
every field has freshness, source and event id; live and replay agree; partial
fills, cancel races, unknown orders and position mismatches are tested; Unknown
never creates risk. The model cannot guarantee profit, but makes every profit

## 6. Calibration contract

CalibrationState is the only source for learned execution quantities. It stores
ordered observations of attempted quantity, confirmed fill quantity, side and
signed conditional markout. The state is bounded, versioned and serializable.
Loading a state requires identity validation and replay equality; malformed,
reordered or future-edited state is rejected.

Admission requires at least 30 effective markouts, at least 15 complete Buy
observations and at least 15 complete Sell observations. The train/validation
split is chronological: the first two thirds are used for training statistics
and the final third is a holdout. Markout values are sorted only inside each
statistical calculation; they are never sorted before the temporal split. A
future regime deterioration therefore revokes admission instead of contaminating
the training set. Before admission, an empty state is represented as ColdStart and the policy
emits calibration_unavailable; a nonempty state that has not passed admission
is represented as Validating and emits insufficient_history. Both states block
new risk while preserving reduction and hard-deadline exit paths. No bootstrap
constant or hand-written fallback can create new risk. When configured, every
observation is atomically persisted and replay-validated.

After admission, Buy and Sell retain separate attempt, fill-probability and
robust-markout estimates. For side s, path lower value L, side-specific fill
probability p_s and side-specific robust markout m_s:

    V_exec(s) = floor(L * p_s / 10000) + m_s.

Direction is therefore represented by the anchor residual, the causal outcome
path and the side-specific execution evidence; it is not assumed that long and
short execution are symmetric.

## 7. Binance legality contract

BinanceContract and CandidateOrder jointly represent the exchange rule surface.
The validator checks status and freshness, PRICE_FILTER modulo tickSize plus min/max bounds,
LOT_SIZE modulo stepSize plus min/max bounds, notional, post-only GTX, position
mode/positionSide, reduce-only, closePosition, conditional-order support,
priceProtect/triggerProtect, open-order capacity, self-trade prevention and
rate-limit budget. A rule mismatch produces no validated order. The model records workingType
explicitly even when an ordinary passive limit has no trigger; this prevents
conditional-order semantics from being silently invented by adapters.

FundingSchedule is a first-class episode object. It carries the next funding
settlement, exchange-reported interval, observation time, freshness bound and
source digest. Entry completeness requires a fresh schedule; the strategy does
not infer a funding boundary from a weekday or a fixed eight-hour constant.
For Hedge Mode, the API-level reduceOnly flag is not sent; the position side
and closing direction are validated as the reduction proof. The account snapshot
therefore carries a signed One-way position and separate non-negative long and
short Hedge legs. If both Hedge legs are open, a single-order policy step refuses
to claim that the net position is flat and emits residual exposure for a
two-order reconciliation path. One-way and Hedge Mode therefore have different
explicit order semantics.

ValidatedOrder also records the visible queue ahead at validation time and an
OrderRoute: PassiveMaker, PassiveReduceOnly or EmergencyReduceOnly. The latter
is IOC and is only emitted at the hard deadline. It has a distinct
execution-port method and cannot be confused with ordinary maker flow.

## 8. Outcome semantics and action competition

ExecutionCycle is the typed causal path for an entry action. It records side,
static anchor price, entry and exit prices, requested quantity, entry and exit
filled quantities, queue-ahead quantities, traded-through quantities observed
after activation, entry/exit latency, entry/exit
fees, exit cost, funding cost, deadline risk and event times. Gross convergence
value is derived from those observations rather
than accepted as a free-standing prediction:

    G = floor(side * (exit_price - entry_price)
              * filled_exit * PICO_BPS
              / (anchor_price * requested_quantity)).

The cycle is terminal only when exit quantity equals entry quantity and the
exit event occurs before the hard deadline. Its net value is derived as:

    N = G - entry_fee - exit_fee - exit_cost
        - funding_cost - deadline_risk_cost.

OutcomeScenario contains either this complete cycle or an explicit Wait value.
An incomplete cycle is not converted to a zero-profit scenario.

QueueFillEstimate uses only causal throughput observed after the order's
latency.
An exchange-authoritative fill event may not exceed the causal fill quantity;
a local intent event can never create a fill. With queue-ahead quantity q_a,
own quantity q_o and traded-through quantity q_t:

    q_fill = min(q_o, max(0, q_t - q_a))
    p_fill = floor(10000 * q_fill / q_o).

ExecutionCycle applies this same bound independently to entry and exit; a
positive claimed fill without positive causal throughput is invalid. A path
with no entry fill is not a Cycle and cannot be used as a profitable outcome.

This is a structural fill estimate, not a tuned probability constant. The
latency is part of the observation boundary, so pre-placement or pre-latency
trades cannot be counted as fill evidence.

OutcomeDistribution requires positive scenario weights summing to 10000 basis
points and every scenario to be terminal. Each scenario net value is:

    N_i = derived_cycle_net_value_i
          or explicit_wait_value_i.

The uncertainty is a typed budget, not a black-box threshold:

    U = U_anchor + U_execution + U_timing + U_model
    L = floor(sum(weight_i * N_i) / 10000) - U.

Each component must be non-negative and represent a distinct information
failure or execution risk. The lower value is therefore auditable: a decision
can identify whether its edge is consumed by anchor uncertainty, fill/queue
uncertainty, timing uncertainty, or model uncertainty.

The old minimum-over-scenarios rule is not used because it discarded the
probability information and made any tiny tail scenario dominate all decisions.
Missing costs, nonterminal paths and invalid weights remain a hard rejection;
they are never converted to zero. Runtime serialization and ledger append
failures are returned as errors; they are not replaced with empty payloads or
unrecorded decisions. The evidence ledger hashes payload, event metadata and
the previous event digest, and verifies the head as well as every link.

Wait is a first-class action with its own terminal outcome distribution. An
entry is admitted only when its calibrated executable lower value is strictly
greater than Wait and greater than the opposite direction. There is no
independent minimum-profit hyperparameter in the decision gate.
