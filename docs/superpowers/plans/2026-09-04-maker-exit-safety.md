# Maker Exit Safety Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Correct passive exit quoting, decouple scheduled exits from entry alpha, and prohibit trading writes in read-only runs.

**Architecture:** A pure, integer-tick exit decision is consumed by paper and live adapters. The live adapter owns order identity, confirmed positions, asynchronous cancellation and the write-permission boundary; neither submitting nor canceling changes inventory. Existing Maker-only transport and production activation restrictions remain intact.

**Tech Stack:** Rust 2021, Tokio, existing serde/reqwest adapters; no new production dependency.

**Spec:** `docs/superpowers/specs/2026-09-04-maker-exit-safety-design.md` (user approved).

## Global Constraints

- 保留 Maker-only、生产双开关、行情过期保护和仓位上限。
- 不连接交易账户验证，也不部署到服务器。自动化验证全部使用离线输入和本地测试。
- `--send-orders=false` 表示禁止交易写操作，包括下单、单笔撤单、批量撤单、退出阶段撤换以及退出清理阶段撤单。
- 多仓减仓：卖出，价格为卖一。空仓减仓：买入，价格为买一。零仓位：不创建订单。
- 要求 `0 < bid < ask`。零价差、倒挂、无效价格、未来时间戳和过期行情不产生退出订单。
- 不把提交、接受或撤单成功当作成交；仓位只能由已确认的账户/成交事实更新。
- 只处理当前策略已跟踪且归属明确的订单；无法确定所有权时停止并报告。
- 遵循 Rust 2021，保持现有依赖，不引入新生产依赖。
- 本批不替换整个 OMS，不修改锚点来源、入场算法或交易品种。
- Work in the existing dedicated checkout `E:\binance\AnchorBell`, branch `codex/safety-core`. Do not push, merge, use account credentials, or run live binaries. Use `apply_patch` for edits.

## Execution and validation

Use `$env:Path = "$env:USERPROFILE\.cargo\bin;" + $env:Path` in each PowerShell session. Baseline is `1bfd5f2`; the prior install turn ran 237 passing library tests. Recheck a clean baseline before implementation. Each task uses RED/GREEN, focused iteration, one full suite before its commit, and a task-scoped independent review. The controller handles final whole-branch review.

### Task 1: Shared passive exit contract and deadline adapter

**Files:** Modify `engine/src/strategy/flatten.rs`, `engine/src/strategy/mod.rs`, `engine/src/strategy/calendar.rs`; create `engine/src/strategy/maker_exit.rs` and module-local tests to keep quote validation separate from schedule representation.

**Interfaces:** Consume `DualFlattenPlan`, `OrderIntent`, `Side`, existing `calendar_for` and dated holiday/half-day rules. Produce the following public types through `strategy/mod.rs` (types derive Debug, Clone, Copy, PartialEq, Eq):

```rust
pub struct ExitBook {
    pub bid: i64,
    pub ask: i64,
    pub observed_at_ms: u64,
}
pub struct ExitConstraints {
    pub min_price: i64,
    pub max_price: i64,
    pub price_tick: i64,
    pub min_quantity: i64,
    pub max_quantity: i64,
    pub quantity_step: i64,
    pub min_notional: i64,
    pub quantity_scale: u32,
    pub observed_at_ms: u64,
    pub max_age_ms: u64,
}
pub enum ExitWorkingOrder {
    None,
    Pending,
    Confirmed { side: Side, price: i64, remaining: i64, reduce_only: bool },
}
pub struct MakerExitInput {
    pub symbol: u32,
    pub position: i64,
    pub position_confirmed: bool,
    pub now_ms: u64,
    pub max_book_age_ms: u64,
    pub plan: DualFlattenPlan,
    pub book: Option<ExitBook>,
    pub constraints: Option<ExitConstraints>,
    pub working: ExitWorkingOrder,
}
pub enum MakerExitDecision {
    Trading,
    Flat,
    WaitForReconciliation,
    KeepWorking,
    CancelWorking,
    Submit(OrderIntent),
    ResidualExposure,
    Blocked(ExitBlockReason),
}
pub fn decide_maker_exit(input: MakerExitInput) -> MakerExitDecision;
```

`ExitBlockReason` distinguishes InvalidInput, InvalidBook, StaleBook, MissingConstraints, InvalidConstraints, StaleConstraints, and Dust. `min_notional` is expressed in quote-currency ticks at the price scale; multiply it by `10^quantity_scale` when comparing against `price * quantity` in checked i128. Price bounds supplied by the adapter already include applicable percent-price limits.

- [ ] **Step 1: Add failing behavior tests.** Test long/short quoting, zero position, invalid symbol, `i64::MIN`, locked/crossed books, future/stale data, missing/expired filters, quantity caps/round-down/dust, and unknown order/position states. Use a unit fixture with now=1000, funding deadline=10000, funding lead=9000, bid=9880, ask=9890, scale=0, quantity=10, min quantity=1, max quantity=50, step=1, min notional=1, valid timestamps=1000. Literal key expectation:

```rust
assert_eq!(decide_maker_exit(long_input), MakerExitDecision::Submit(
    OrderIntent::maker_sell(7, 9890, 10)
));
assert_eq!(decide_maker_exit(short_input), MakerExitDecision::Submit(
    OrderIntent::maker_buy(7, 9880, 10)
));
```

For scaled quantities use position=150, quantity_scale=2, max_quantity=125, step=10: quantity must be 120; notional uses 1.20 units, not 120. At an upper bound of 125 the function must never submit 130. Pending order must return WaitForReconciliation. Same-price safe reduce-only order must return KeepWorking, including a partially filled order whose positive remaining quantity is no greater than both the remaining position and per-order cap. Do not reapply new-order minimum/step constraints to an already accepted partial remainder. Wrong-side/non-reducing/stale-price working order must request cancellation, not immediately submit a replacement.

- [ ] **Step 2: Verify RED.** Run `cargo test --lib strategy::maker_exit --locked`. Record relevant failure before implementing. If interface scaffolding is needed, use nonfunctional results solely to compile the behavior tests and demonstrate assertion failures before logic implementation.

- [ ] **Step 3: Implement pure decisions.** Classify schedule with `plan.phase_at(now_ms, true)` rather than allowing a zero position to bypass time windows. Unknown position/pending order blocks all submissions. Before the window return Trading only when funding status is not Unknown; unknown schedule blocks new risk. During reduction validate inputs/constraints, derive side/price, use `checked_abs`, cap quantity and round down. Permit returning CancelWorking for an existing known order when trading must stop and a fresh safe replacement cannot be formed. At/after the hard deadline return ResidualExposure for nonzero position; never generate a synthetic fill. Flat with a working order returns CancelWorking or WaitForReconciliation rather than declaring completion.

```rust
let quantity = input.position.checked_abs().ok_or(ExitBlockReason::InvalidInput)?;
let capped = quantity.min(constraints.max_quantity);
let rounded = capped - capped % constraints.quantity_step;
let notional = i128::from(price).checked_mul(i128::from(rounded));
let floor = i128::from(constraints.min_notional)
    .checked_mul(10_i128.checked_pow(constraints.quantity_scale).ok_or(ExitBlockReason::InvalidConstraints)?);
```

Use the fragment inside an internal Result-returning validation helper; public decisions remain explicit enum values. Validate all divisors, bounds and timestamp fields before arithmetic.

- [ ] **Step 4: Supply a deterministic calendar boundary.** Add `EquitySessionCalendar::exit_deadline_at(timestamp_ms: u64) -> Option<u64>`. Return the current active risk deadline during auction/open sessions, otherwise the next risk deadline, using the existing region auction start and afternoon reopen definitions. Include supported-year, holiday, weekend and half-day rules; never invent trading sessions for unsupported dates. Morning auction is already the existing conservative boundary, so retain it rather than moving the exit later to continuous trading. Compute ISO weekday correctly from exchange-local day. Test afternoon lead, a Friday-night next-session lookup, holiday, half-day omission of afternoon, and unsupported year. No wall-clock reads.

- [ ] **Step 5: Verify GREEN and commit.** Run `cargo fmt --all`, focused maker_exit/calendar tests, `cargo test --workspace --locked`. Commit only task files with `fix: add deterministic passive exit decisions` and record exact tests in the report.

### Task 2: Integrate shared exit decisions into paper and replay

**Files:** Modify `engine/src/paper.rs`; add `engine/src/paper/exit_tests.rs` if needed to avoid expanding the already large in-file tests; update `docs/PAPER_BACKTEST_TESTNET_RUNBOOK.md` for explicit simulation constraints.

**Interfaces:** Consume Task 1 `MakerExitInput`, `ExitBook`, `ExitConstraints`, `ExitWorkingOrder`, `MakerExitDecision`, `decide_maker_exit`, and `EquitySessionCalendar::exit_deadline_at`. Produce `PaperEngine::evaluate_exits_at(timestamp_ms: u64) -> Result<Vec<PaperRecord>, PaperError>` (adapt engine type name to existing `PaperEngine` spelling if different) plus `set_exit_constraints(symbol: &str, constraints: ExitConstraints) -> Result<(), PaperError>`. Preserve existing public paper run configuration struct literals by adding engine-level state/setter rather than new mandatory config fields.

- [ ] **Step 1: Reproduce the paper rejection.** Using existing paper test event builders, establish a long position, then a funding reduction window with bid=9880/ask=9890. Assert a reduce-only SELL at 9890 is placed and position remains unchanged before a compatible trade. Mirror for short BUY at 9880. Add explicit tick-only exit evaluation, stale-book/fresh-mark, `i64::MIN`, valid-order retention, and hard-deadline residual tests. Existing trade fixture helpers must exercise the real paper engine, not source-string assertions.

```rust
let before = engine.summary().fill_count;
let records = engine.evaluate_exits_at(reduction_start).unwrap();
assert!(records.iter().any(|record| record.kind == "order_placed"
    && record.side.as_deref() == Some("SELL") && record.price_ticks == Some(9890)));
assert_eq!(engine.summary().fill_count, before);
```

Read actual existing summary method signatures and use their owning API without changing production access solely for a test.

- [ ] **Step 2: Verify RED.** Run focused paper exit tests; record the original nonzero-spread rejection and no-tick-exit failures before changing paper logic.

- [ ] **Step 3: Wire the common decision.** Track book receipt timestamp separately from mark. Store per-symbol constraints with explicit simulation origin. Baseline simulation constraints must be finite and documented: price step=1 tick, quantity step=1 tick, minimum quantity=1 tick, maximum quantity=configured max_position, minimum notional=1 quote tick, quantity scale from run config, finite positive price maximum. Explicit simulated constraints have timestamps advanced by the replay clock; downloaded exchange constraints retain their actual timestamp and expire normally. Emit the simulation assumption in paper records/summary metadata, never imply these are fetched exchange filters.

At each event and in `evaluate_exits_at`, derive/preserve the dual deadline and map current working state into the shared decision. A reduction window cancels entry orders before a reducing order. Existing paper cancel latency must remain modeled: do not instantaneously replace an order when the configured cancel delay is nonzero; allow compatible fills while cancellation is in flight, then use updated position. Correctly capped partial reducing orders keep their queue if their remaining quantity is still safe; do not churn merely because a fill reduced the outstanding amount.

Map Submit to reduce-only placement, CancelWorking to delayed cancellation, KeepWorking/WaitForReconciliation to no new writes, and ResidualExposure to the existing `record`/`RecordFields` construction carrying the remaining position (never a fill). Keep entry alpha intact but never evaluate it when an exit decision blocks new risk. Preserve cutoff state so a new mark with a later funding time cannot silently re-enable entry after the previous deadline. Reset a completed exit session only via an explicit new valid anchor/session with zero position and no working order. Keep legacy epoch-based offline fixtures deterministic using explicitly simulated schedules, not enabling unsupported dates for live data.

- [ ] **Step 4: Verify integration.** Tests must prove compatible aggressor/price/entry latency and cancellation latency remain required, positions are capped under partial fills, and EOF still cancels without faking fills. Run paper tests then the full workspace suite.

- [ ] **Step 5: Commit.** `git add` only paper/tests/runbook files; commit `fix: share passive exit decisions with paper replay`; report RED/GREEN and any API adaptations.

### Task 3: Enforce live write permission and independently schedule exits

**Files:** Modify `engine/src/bin/anchorbell_live.rs`, `engine/src/execution/supervisor.rs`, `engine/src/execution/mod.rs`; create `engine/src/execution/trading_permission.rs`; use a focused `engine/src/bin/anchorbell_live/exit.rs` (or sibling helper module) for orchestration and offline adapter tests. Use existing `market/metadata.rs` APIs; do not add external dependencies.

**Interfaces:** Consume Task 1 exit API and existing `BinanceRestClient::{place_maker_order,cancel_order,query_order,position_risk,current_open_orders}`, `PublicMarketMetadataClient`, `ExecutionSupervisor`. Produce `TradingPermission::new(send_orders: bool, deployment_allows_orders: bool)` and `TradingPermission::execute<T, E, F, Fut>(&self, operation: F) -> Result<Option<T>, E>` where `F: FnOnce() -> Fut`, `Fut: Future<Output=Result<T,E>>`. Denied permission must not even construct/call the network-operation closure.

- [ ] **Step 1: Write failing permission tests.** Use a counter in the *operation closure* to prove real permission behavior, not a mock permission object. Test both disabled dimensions and enabled errors/success.

```rust
let calls = std::cell::Cell::new(0);
let result: Result<Option<()>, ()> = TradingPermission::new(false, true).execute(|| {
    calls.set(calls.get() + 1);
    async { Ok(()) }
}).await;
assert_eq!(result, Ok(None));
assert_eq!(calls.get(), 0);
```

- [ ] **Step 2: Demonstrate existing runner failures offline.** Extract a testable decision/execution boundary around real runner logic, with a test-local exchange adapter or injected closures only at transport boundaries. Test no-entry-alpha exit, timer-only trigger, repeated Flattening processing, read-only startup/exit/shutdown zero writes, unknown cancel no replacement, foreign order ID cannot remove own order, matching partial fill updates remaining quantity, and confirmed account state required after cancellation.

- [ ] **Step 3: Implement permission and identity barrier.** Implement execute as `if self.allowed { operation().await.map(Some) } else { Ok(None) }`, with private `allowed=send_orders && deployment_allows_orders`. Every place/cancel path in this runner passes through this boundary, including startup cleanup and shutdown. Remove broad symbol cancel usage; only cancel known current-run IDs. At startup, any pre-existing unclaimed order blocks startup; a shared `anchorbell-` prefix alone is not proof of current-run ownership. Do not alter generic exchange client behavior for other binaries.

Keep working orders in the map until matching terminal evidence confirms them. Match both symbol and client_order_id before applying an order event. Track original, cumulative filled and remaining quantities, reduce-only flag, and pending/unknown state; reject regression/overfill. After cancel completion, query current position and confirm the canceled order terminal before replacing; on transport uncertainty keep the identity, freeze new risk and report. A failed query is not a flat position. Never compute confirmed position from order acknowledgement.

- [ ] **Step 4: Add independent exit loop.** Add a 250 ms interval to `tokio::select!`; process exits after events and timer ticks before the entry-alpha path. The same async helper must be invoked by both paths, so tests cover actual orchestration. Add a supervisor `evaluate_exit` method that does not require an entry intent and accepts Healthy/Flattening but refuses unknown remote state. Derive per-symbol equity/funding `DualFlattenPlan`, latch the earliest active hard deadline, and retain residual state rather than replacing an expired funding deadline with a future one.

```rust
let mut exit_tick = tokio::time::interval(Duration::from_millis(250));
exit_tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
```

Track local receipt timestamps alongside exchange event timestamps; both future exchange timestamps and stale receipt/exchange snapshots block new exit quotes. Fresh mark cannot mask stale book. Funding unknown blocks new risk, but an already latched deadline remains valid. A missing or unsupported equity calendar must not manufacture a session.

Get authoritative exchange filters via `PublicMarketMetadataClient` in a bounded background refresh, use 60 s refresh/120 s metadata TTL for static exchangeInfo filters, and keep dynamic mark/book validity at 5000 ms. Parse exact integer scales; intersect PRICE_FILTER with current PERCENT_PRICE bounds (scaled integer multiplication), and compute min notional in price ticks. Missing, failed, future, contradictory or expired metadata blocks exit submissions; no unlimited fallback. No blocking network refresh inside the pure function or 250 ms loop. Observed filters remain valid until their TTL, but a failed refresh must never update their observation timestamp.

- [ ] **Step 5: Residual and shutdown behavior.** At a hard deadline preserve monitoring, tracked working orders, and residual risk; log transitions/changes rather than the same warning every tick. Keep processing risk-stopped inputs without reopening risk. At configured duration/Ctrl-C, attempt permitted cancellation of owned orders, report each cancellation outcome and authoritative-or-explicitly-unknown positions, then exit. Error/disconnect paths share this cleanup; a cleanup error must not suppress the original error or claim flat. No taker fallback.

- [ ] **Step 6: Verify and commit.** Add offline tests for stale/future filters and decimal conversion including tiny quantity scales, order partial-fill/cancel races, half-day/afternoon deadlines, timer-only exits and all denied-write paths. Run focused tests, full suite and clippy, commit `fix: schedule live exits behind an explicit write boundary`. Record report evidence and any unresolved account-state limitations, not optimistic claims.

### Task 4: Document verified guarantees and run final checks

**Files:** Modify `README.md`, `README.zh-CN.md`, `docs/TESTNET_RUNBOOK.md`, `docs/DUAL_ENVIRONMENT_RUNBOOK.md`, `docs/PAPER_BACKTEST_TESTNET_RUNBOOK.md`, and approved spec status. Add a concise test/limitations note under `docs/` only if existing runbooks cannot own it.

**Interfaces:** Consume implemented behavior and prior reports. Produce consistent operator docs: Maker-only exit is a target, not guaranteed execution; read-only prohibits every trading write; no assumption that Testnet proves profitability; exact simulation assumptions and residual handling.

- [ ] **Step 1: Audit wording and behavior.** Compare docs against shared core and runner changes, particularly deadlines, filter TTL, ownership and failure states. Human prose does not need fake source-text tests.

- [ ] **Step 2: Edit docs.** Replace unconditional guarantee wording with: `Targets passive reduction before the risk deadline; unfilled residual exposure is reported and never treated as a synthetic fill.` Chinese: `目标是在风险截止前被动减仓；未成交的残余仓位会明确报告，不会被视为已经平仓。` Document user stop/configured duration, existing-order startup halt, no new production authority and no automatic taker fallback. Mark the spec implemented only after all acceptance requirements have evidence.

- [ ] **Step 3: Run exact final verification.** `cargo fmt --all -- --check`, `cargo build --workspace --locked`, `cargo test --workspace --locked`, `cargo clippy --workspace --all-targets --all-features --locked -- -D warnings`, `git diff --check`. Capture counts and failure status. Do not alter unrelated code solely to make a new compiler lint pass without reporting.

- [ ] **Step 4: Commit and hand off.** Commit docs with `docs: clarify maker exit limits and read-only safety`; independent final whole-branch review follows. Keep branch local until user requests push/PR; do not merge or run trading.
