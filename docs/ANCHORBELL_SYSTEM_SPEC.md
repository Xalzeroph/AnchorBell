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

completed external close -> Anchor
one closed period -> AnchorEpisode
Binance rules at time t -> BinanceContract
book, index, mark, freshness -> MarketSnapshot
account and remote orders -> AccountSnapshot
proposal -> CandidateOrder
rule-checked proposal -> ValidatedOrder
full fill/markout/exit path -> OutcomeDistribution
all input facts -> EvidenceFrame
evolvable semantic policy -> StrategyPlan

## 3. Axioms

Axiom A0, time and causality: every event has exchange time, sequence when
available, and unique id; decisions use only F_t; duplicate channels are
deduplicated; local clocks are timeout guards; drift, gaps or conflicts halt
new risk.

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
PRICE_FILTER; quantity satisfies LOT_SIZE; notional satisfies MIN_NOTIONAL or
NOTIONAL when supplied. Maker proves non-crossing before send. positionSide,
reduceOnly, closePosition and hedge mode are validated together. mark, index,
workingType, priceProtect, triggerProtect, rate limits and connection state
are recorded. A stale or inconsistent contract digest blocks new orders.

Axiom A4, information: bid, ask, depth, trade, index, mark, funding and server
time are distinct facts. Book snapshot and deltas prove continuity. Core prices
and quantities use integer minimum units. Freshness belongs to each stream.
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

EvidenceFrame contains episode, anchor, calendar, funding, contract snapshot,
continuous book, index/mark/server time, account, remote orders, fees,
lifecycle, calibration and model version. Facts are stored once; inferences
cite fact ids. Live, simulation and replay share validation.

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
the training set. Before admission, only a ColdStart snapshot with zero
historical influence is available. No bootstrap constant or hand-written fallback
can create new risk.

After admission, Buy and Sell retain separate attempt, fill-probability and
robust-markout estimates. For side s, path lower value L, side-specific fill
probability p_s and side-specific robust markout m_s:

    V_exec(s) = floor(L * p_s / 10000) + m_s.

Direction is therefore represented by the anchor residual, the causal outcome
path and the side-specific execution evidence; it is not assumed that long and
short execution are symmetric.

## 7. Binance legality contract

BinanceContract and CandidateOrder jointly represent the exchange rule surface.
The validator checks status and freshness, PRICE_FILTER, LOT_SIZE, notional,
post-only GTX, position mode/positionSide, reduce-only, closePosition,
conditional-order support, priceProtect/triggerProtect and rate limit budget.
A rule mismatch produces no validated order. The model records workingType
explicitly even when an ordinary passive limit has no trigger; this prevents
conditional-order semantics from being silently invented by adapters.

## 8. Outcome semantics and action competition

ExecutionCycle is the typed causal path for an entry action. It records side,
static anchor price, entry and exit prices, requested quantity, entry and exit
filled quantities, entry/exit fees, exit cost, funding cost, deadline risk and
event times. Gross convergence value is derived from those observations rather
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
they are never converted to zero.

Wait is a first-class action with its own terminal outcome distribution. An
entry is admitted only when its calibrated executable lower value is strictly
greater than Wait and greater than the opposite direction. There is no
independent minimum-profit hyperparameter in the decision gate.
