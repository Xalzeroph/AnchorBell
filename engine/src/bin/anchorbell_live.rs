use std::{
    collections::BTreeMap,
    env, process,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use static_anchor_engine::{
    execution::{
        BinanceCredentials, BinanceEnvironment, BinanceMakerOrderRequest, BinanceOpenOrder,
        BinanceOrderResponse, BinanceRestClient, BinanceUserDataStream, DeploymentConfig,
        ExecutionSupervisor, GateDecision, Side, SupervisorConfig, SupervisorState,
        TradingPermission, UserDataEvent, LIVE_SYMBOLS,
    },
    market::{
        binance::{BinanceMarketEvent, BookTicker, MarkPrice},
        BinanceC2cFxClient, BinanceC2cFxPoller, BinanceExecutionFilters, BinanceMarketConfig,
        BinanceMarketFeed, BinanceMarketStream, FxPollerConfig, FxUpdate,
        PublicMarketMetadataClient,
    },
    paper::load_binance_index_anchor_set,
    strategy::{
        adaptive_intent_from_market, calendar_for, decide_maker_exit, profile_for, AnchorCurrency,
        DualFlattenPlan, EquityRegion, ExitBook, ExitConstraints, ExitWorkingOrder, FlattenPhase,
        FundingRateKind, FundingSchedule, MakerExitDecision,
    },
};

const MAX_FRAME_BYTES: usize = 1_048_576;
const RECV_WINDOW_MS: u64 = 5_000;
const MARKET_MAX_AGE_MS: u64 = 5_000;
const METADATA_REFRESH_MS: u64 = 60_000;
const METADATA_MAX_AGE_MS: u64 = 120_000;
const EQUITY_EXIT_LEAD_MS: u64 = 30 * 60 * 1_000;

#[derive(Debug)]
struct Args {
    environment: BinanceEnvironment,
    duration_secs: u64,
    proxy: Option<String>,
    price_scale: u32,
    quantity_scale: u32,
    max_position: i64,
    quantity: i64,
    entry_threshold_bps: i64,
    max_mark_index_gap_bps: i64,
    max_anchor_age_ms: u64,
    funding_lead_ms: u64,
    max_subscriptions_per_shard: usize,
    send_orders: bool,
}

#[derive(Debug)]
enum Event {
    Market(BinanceMarketEvent),
    Fx(FxUpdate),
    User(UserDataEvent),
    Metadata(Result<MetadataSnapshot, String>),
    Halt(String),
}

#[derive(Debug, Default)]
struct SymbolState {
    book: Option<BookTicker>,
    book_received_at_ms: u64,
    mark: Option<MarkPrice>,
    mark_received_at_ms: u64,
    position_ticks: i64,
    position_confirmed: bool,
    position_observed_at_ms: u64,
    exit_plan: Option<DualFlattenPlan>,
    last_exit_report: Option<String>,
    last_mark_price_ticks: Option<i64>,
    ewma_abs_return_bps: i64,
}

#[derive(Debug, Clone)]
struct WorkingOrder {
    client_order_id: String,
    side: Side,
    price_ticks: i64,
    quantity_ticks: i64,
    executed_quantity_ticks: i64,
    fill_observed_at_ms: u64,
    reduce_only: bool,
    cancel_pending: bool,
}

impl WorkingOrder {
    fn remaining_quantity(&self) -> Option<i64> {
        self.quantity_ticks
            .checked_sub(self.executed_quantity_ticks)
    }

    fn exit_state(&self) -> ExitWorkingOrder {
        if self.cancel_pending {
            ExitWorkingOrder::Pending
        } else {
            self.remaining_quantity()
                .map_or(ExitWorkingOrder::Pending, |remaining| {
                    ExitWorkingOrder::Confirmed {
                        side: self.side,
                        price: self.price_ticks,
                        remaining,
                        reduce_only: self.reduce_only,
                    }
                })
        }
    }
}

#[derive(Debug)]
struct MetadataSnapshot {
    observed_at_ms: u64,
    filters: BTreeMap<String, BinanceExecutionFilters>,
}

#[tokio::main]
async fn main() {
    let args = match parse_args() {
        Ok(value) => value,
        Err(error) => fail(&error),
    };
    match run(args).await {
        Ok(code) => process::exit(code),
        Err(error) => {
            eprintln!("live runner failed: {error}");
            process::exit(1);
        }
    }
}

async fn run(args: Args) -> Result<i32, String> {
    let deployment = DeploymentConfig::from_process_environment()
        .map_err(|error| format!("deployment config rejected: {error:?}"))?;
    if deployment.environment != args.environment {
        return Err(format!(
            "--environment {} does not match ANCHORBELL_BINANCE_ENV={}",
            args.environment, deployment.environment
        ));
    }
    let credentials = load_credentials(args.environment)?;
    let policy = deployment.policy(true);
    let permission = TradingPermission::new(args.send_orders, policy.allow_live_orders);
    if args.send_orders && !policy.allow_live_orders {
        return Err("order submission is disabled by deployment policy".into());
    }
    let client = Arc::new(
        BinanceRestClient::new(args.environment, policy, args.proxy.as_deref())
            .map_err(|error| error.to_string())?,
    );
    let server_time = client.server_time_ms().await.map_err(|e| e.to_string())?;
    let symbols = LIVE_SYMBOLS
        .iter()
        .map(|s| (*s).to_owned())
        .collect::<Vec<_>>();
    let anchors = load_binance_index_anchor_set(
        args.environment,
        &symbols,
        args.price_scale,
        args.proxy.as_deref(),
    )
    .await
    .map_err(|error| format!("cannot load Binance index anchors: {error}"))?
    .anchors;
    if anchors.len() != LIVE_SYMBOLS.len() {
        return Err("anchor set is not the exact nine-symbol universe".into());
    }

    let mut supervisor = ExecutionSupervisor::new(SupervisorConfig {
        max_market_age_ms: 5_000,
        max_fx_age_ms: 5_000,
        funding_lead_ms: args.funding_lead_ms,
        max_position: args.max_position,
        quantity_scale: args.quantity_scale,
    })
    .map_err(|reason| format!("supervisor config rejected: {reason:?}"))?;
    let mut state = BTreeMap::<String, SymbolState>::new();
    for symbol in &symbols {
        reconcile_symbol(&client, &credentials, symbol, args.quantity_scale).await?;
        state.insert(
            symbol.clone(),
            SymbolState {
                position_confirmed: true,
                position_observed_at_ms: server_time,
                ..Default::default()
            },
        );
    }

    let (tx, mut rx) = tokio::sync::mpsc::channel::<Event>(16_384);
    let event_overflow = Arc::new(AtomicBool::new(false));
    spawn_market(&args, tx.clone(), event_overflow.clone())?;
    spawn_fx(&args, tx.clone())?;
    spawn_metadata(&args, tx.clone())?;
    let listen_key = spawn_user_data(
        &args,
        client.clone(),
        credentials.clone(),
        tx.clone(),
        event_overflow.clone(),
    )
    .await?;
    spawn_keepalive(client.clone(), credentials.clone(), listen_key, tx.clone());

    println!(
        "{}",
        serde_json::json!({
            "event":"live_canary_started",
            "environment":args.environment.as_str(),
            "server_time_ms":server_time,
            "symbols":symbols,
            "order_submission":args.send_orders,
            "paper_orders":false,
        })
    );

    let mut fx_at = BTreeMap::<String, u64>::new();
    let mut metadata = BTreeMap::<String, BinanceExecutionFilters>::new();
    let mut metadata_observed_at_ms = 0_u64;
    let deadline = tokio::time::sleep(Duration::from_secs(args.duration_secs));
    tokio::pin!(deadline);
    let mut exit_tick = tokio::time::interval(Duration::from_millis(250));
    exit_tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    let mut supervisor_ready = false;
    let mut working = BTreeMap::<String, WorkingOrder>::new();
    let mut order_sequence = 0_u64;

    let outcome = 'session: loop {
        tokio::select! {
            _ = &mut deadline => break Ok(()),
            signal = tokio::signal::ctrl_c() => {
                break match signal {
                    Ok(()) => Ok(()),
                    Err(error) => Err(format!("Ctrl-C handler failed: {error}")),
                };
            }
            _ = exit_tick.tick() => {}
            event = rx.recv() => {
                let Some(event) = event else {
                    supervisor.on_disconnect();
                    break 'session Err("all event producers stopped; risk stopped".into());
                };
                match event {
                    Event::Market(value) => apply_market(&mut state, value, now_ms()),
                    Event::Fx(value) => {
                        fx_at.insert(value.currency, value.observed_at_ms);
                    }
                    Event::User(value) => {
                        if let Err(error) = apply_user(
                            &mut state,
                            &mut working,
                            &value,
                            args.quantity_scale,
                        ) {
                            break 'session Err(error);
                        }
                        if let Err(reason) = supervisor.on_user_data(value) {
                            break 'session Err(format!("user-data risk halt: {reason:?}"));
                        }
                        if supervisor.state() == SupervisorState::Flattening
                            && state.values().all(|item| {
                                item.position_confirmed && item.position_ticks == 0
                            })
                            && working.is_empty()
                        {
                            if let Err(reason) = supervisor.confirm_flattened() {
                                break 'session Err(format!("flatten confirmation rejected: {reason:?}"));
                            }
                            println!("{}", serde_json::json!({"event":"flatten_confirmed"}));
                        }
                    }
                    Event::Metadata(result) => match result {
                        Ok(snapshot) => {
                            metadata = snapshot.filters;
                            metadata_observed_at_ms = snapshot.observed_at_ms;
                            println!("{}", serde_json::json!({
                                "event":"execution_metadata_refreshed",
                                "observed_at_ms":metadata_observed_at_ms,
                                "symbols":metadata.len(),
                            }));
                        }
                        Err(reason) => eprintln!("{}", serde_json::json!({
                            "event":"execution_metadata_refresh_failed",
                            "reason":reason,
                        })),
                    },
                    Event::Halt(reason) => {
                        supervisor.on_disconnect();
                        break 'session Err(reason);
                    }
                }
            }
        }

        if event_overflow.load(Ordering::Acquire) {
            supervisor.on_disconnect();
            break 'session Err("event queue overflow; remote state may be incomplete".into());
        }
        let now = now_ms();
        for symbol in &symbols {
            let local = state.get(symbol).expect("initialized symbol");
            let Some(profile) = profile_for(symbol) else {
                break 'session Err(format!("no profile for {symbol}"));
            };
            let market_at = local
                .book
                .as_ref()
                .map(|value| value.event_time_ms.min(local.book_received_at_ms))
                .unwrap_or(0);
            let currency = profile.anchor_currency.as_str().to_owned();
            let anchor_ready = anchors
                .get(symbol)
                .is_some_and(|anchor| anchor.valid_at(now, args.max_anchor_age_ms));
            let next_funding = local
                .mark
                .as_ref()
                .map(|value| value.next_funding_time_ms)
                .unwrap_or(0);
            if let Err(reason) = supervisor.observe_symbol(
                symbol,
                market_at,
                fx_at.get(&currency).copied().unwrap_or(0),
                anchor_ready,
                equity_is_closed_at(profile.region, now),
                next_funding > now,
                next_funding,
                local.position_ticks,
            ) {
                break 'session Err(format!("observation rejected: {reason:?}"));
            }
        }
        if !supervisor_ready {
            if let Err(reason) = supervisor.reconciliation_clean() {
                break 'session Err(format!("initial reconciliation rejected: {reason:?}"));
            }
            supervisor_ready = true;
            println!("{}", serde_json::json!({"event":"supervisor_healthy"}));
        }

        if let Err(error) = process_live_exits(
            &mut supervisor,
            &mut state,
            &mut working,
            &metadata,
            metadata_observed_at_ms,
            &client,
            &credentials,
            permission,
            &args,
            now,
            &mut order_sequence,
        )
        .await
        {
            break 'session Err(error);
        }

        if supervisor.state() == SupervisorState::Healthy {
            for symbol in &symbols {
                let Some(intent) = make_intent(
                    symbol,
                    state.get(symbol).expect("initialized symbol"),
                    anchors
                        .get(symbol)
                        .expect("validated anchor")
                        .close_price_ticks,
                    &args,
                ) else {
                    continue;
                };
                match supervisor.evaluate(symbol, intent, now) {
                    GateDecision::Allow if !working.contains_key(symbol) => {
                        order_sequence = order_sequence.saturating_add(1);
                        let sequence = order_sequence;
                        let placed = permission
                            .execute(|| {
                                place_order(
                                    &client,
                                    &credentials,
                                    symbol,
                                    intent,
                                    args.price_scale,
                                    args.quantity_scale,
                                    now,
                                    sequence,
                                    false,
                                )
                            })
                            .await;
                        match placed {
                            Ok(Some(order)) => {
                                if order.executed_quantity_ticks > 0 || order.cancel_pending {
                                    if let Some(local) = state.get_mut(symbol) {
                                        local.position_confirmed = false;
                                    }
                                }
                                println!(
                                    "{}",
                                    serde_json::json!({
                                        "event":"order_accepted",
                                        "symbol":symbol,
                                        "client_order_id":order.client_order_id,
                                        "side":side_name(order.side),
                                        "price_ticks":order.price_ticks,
                                        "quantity_ticks":order.quantity_ticks,
                                    })
                                );
                                working.insert(symbol.clone(), order);
                            }
                            Ok(None) => {}
                            Err(error) => break 'session Err(error),
                        }
                    }
                    GateDecision::Halt(
                        static_anchor_engine::execution::GateReason::NotHealthy
                        | static_anchor_engine::execution::GateReason::MarketStale
                        | static_anchor_engine::execution::GateReason::FxStale
                        | static_anchor_engine::execution::GateReason::AnchorUnavailable,
                    ) => {
                        if working.contains_key(symbol) {
                            if let Err(error) = cancel_and_reconcile(
                                &client,
                                &credentials,
                                permission,
                                symbol,
                                &mut working,
                                &mut state,
                                args.quantity_scale,
                            )
                            .await
                            {
                                break 'session Err(error);
                            }
                        }
                    }
                    GateDecision::Halt(reason) => {
                        break 'session Err(format!("gate halt for {symbol}: {reason:?}"));
                    }
                    GateDecision::Allow | GateDecision::Flatten(_) | GateDecision::NoAction(_) => {}
                }
            }
        }
    };

    cleanup_owned_orders(
        &client,
        &credentials,
        permission,
        &mut working,
        &mut state,
        args.quantity_scale,
    )
    .await;
    println!(
        "{}",
        serde_json::json!({
            "event":"live_canary_stopped",
            "state":format!("{:?}", supervisor.state()),
            "symbols":symbols,
            "order_submission":args.send_orders,
            "flat_start_required":true,
            "residual_positions":state.iter().map(|(symbol, value)| serde_json::json!({
                "symbol":symbol,
                "position_ticks":value.position_confirmed.then_some(value.position_ticks),
                "position_known":value.position_confirmed,
            })).collect::<Vec<_>>(),
            "owned_orders_remaining":working.len(),
        })
    );
    outcome.map(|_| 0)
}

#[allow(clippy::too_many_arguments)]
async fn process_live_exits(
    supervisor: &mut ExecutionSupervisor,
    state: &mut BTreeMap<String, SymbolState>,
    working: &mut BTreeMap<String, WorkingOrder>,
    metadata: &BTreeMap<String, BinanceExecutionFilters>,
    metadata_observed_at_ms: u64,
    client: &BinanceRestClient,
    credentials: &BinanceCredentials,
    permission: TradingPermission,
    args: &Args,
    now: u64,
    order_sequence: &mut u64,
) -> Result<(), String> {
    for symbol in LIVE_SYMBOLS {
        if !matches!(supervisor.evaluate_exit(symbol), GateDecision::Allow) {
            continue;
        }
        if !market_snapshots_fresh(
            state.get(symbol).expect("initialized symbol"),
            now,
            MARKET_MAX_AGE_MS,
        ) {
            if working.contains_key(symbol) {
                cancel_and_reconcile(
                    client,
                    credentials,
                    permission,
                    symbol,
                    working,
                    state,
                    args.quantity_scale,
                )
                .await?;
            }
            report_exit_change(state, symbol, "blocked: stale or future book/mark");
            continue;
        }

        let Some(candidate) = build_exit_plan(
            state.get(symbol).expect("initialized symbol"),
            profile_for(symbol)
                .ok_or_else(|| format!("no profile for {symbol}"))?
                .region,
            now,
            args.funding_lead_ms,
        ) else {
            report_exit_change(state, symbol, "blocked: no supported exit schedule");
            continue;
        };
        {
            let local = state.get_mut(symbol).expect("initialized symbol");
            latch_earliest_plan(&mut local.exit_plan, candidate);
        }
        let plan = state[symbol].exit_plan.expect("plan latched");
        if plan.phase_at(now, true) != FlattenPhase::Trading
            && supervisor.state() == SupervisorState::Healthy
        {
            supervisor
                .begin_flatten()
                .map_err(|reason| format!("flatten transition rejected: {reason:?}"))?;
        }

        let constraints = metadata.get(symbol).and_then(|filters| {
            exit_constraints_from_filters(
                filters,
                state[symbol].mark.as_ref()?.mark_price.0,
                args.price_scale,
                args.quantity_scale,
                metadata_observed_at_ms,
            )
        });
        let local = state.get(symbol).expect("initialized symbol");
        let decision = decide_maker_exit(static_anchor_engine::strategy::MakerExitInput {
            symbol: stable_symbol_id(symbol),
            position: local.position_ticks,
            position_confirmed: local.position_confirmed,
            now_ms: now,
            max_book_age_ms: MARKET_MAX_AGE_MS,
            plan,
            book: local.book.as_ref().map(|book| ExitBook {
                bid: book.bid_price.0,
                ask: book.ask_price.0,
                observed_at_ms: book.event_time_ms.min(local.book_received_at_ms),
            }),
            constraints,
            working: working
                .get(symbol)
                .map_or(ExitWorkingOrder::None, WorkingOrder::exit_state),
        });

        match decision {
            MakerExitDecision::Submit(intent) => {
                *order_sequence = order_sequence.saturating_add(1);
                let sequence = *order_sequence;
                let placed = permission
                    .execute(|| {
                        place_order(
                            client,
                            credentials,
                            symbol,
                            intent,
                            args.price_scale,
                            args.quantity_scale,
                            now,
                            sequence,
                            true,
                        )
                    })
                    .await?;
                if let Some(order) = placed {
                    if order.executed_quantity_ticks > 0 || order.cancel_pending {
                        if let Some(local) = state.get_mut(symbol) {
                            local.position_confirmed = false;
                        }
                    }
                    working.insert(symbol.to_owned(), order);
                    report_exit_change(state, symbol, "reduce-only maker order accepted");
                } else {
                    report_exit_change(state, symbol, "reduce-only write denied");
                }
            }
            MakerExitDecision::CancelWorking => {
                cancel_and_reconcile(
                    client,
                    credentials,
                    permission,
                    symbol,
                    working,
                    state,
                    args.quantity_scale,
                )
                .await?;
                report_exit_change(
                    state,
                    symbol,
                    if permission.allowed() {
                        "owned order cancel/reconciliation requested"
                    } else {
                        "owned order cancellation denied by read-only mode"
                    },
                );
            }
            MakerExitDecision::ResidualExposure => {
                if working.get(symbol).is_some_and(|order| !order.reduce_only) {
                    cancel_and_reconcile(
                        client,
                        credentials,
                        permission,
                        symbol,
                        working,
                        state,
                        args.quantity_scale,
                    )
                    .await?;
                }
                let position = state[symbol].position_ticks;
                report_exit_change(
                    state,
                    symbol,
                    &format!("residual exposure at hard deadline: {position}"),
                );
            }
            MakerExitDecision::WaitForReconciliation => {
                if working
                    .get(symbol)
                    .is_some_and(|order| order.cancel_pending)
                {
                    reconcile_owned_order(
                        client,
                        credentials,
                        symbol,
                        working,
                        state,
                        args.quantity_scale,
                    )
                    .await?;
                }
                if plan.phase_at(now, true) == FlattenPhase::ResidualExposure {
                    report_exit_change(
                        state,
                        symbol,
                        "residual exposure unknown while reconciliation is pending",
                    );
                } else {
                    report_exit_change(state, symbol, "waiting for order/position reconciliation");
                }
            }
            MakerExitDecision::Blocked(reason) => {
                if working.get(symbol).is_some_and(|order| !order.reduce_only) {
                    cancel_and_reconcile(
                        client,
                        credentials,
                        permission,
                        symbol,
                        working,
                        state,
                        args.quantity_scale,
                    )
                    .await?;
                }
                report_exit_change(state, symbol, &format!("blocked: {reason:?}"));
            }
            MakerExitDecision::Trading => {
                report_exit_change(state, symbol, "trading window");
            }
            MakerExitDecision::Flat => {
                report_exit_change(state, symbol, "flat in exit window");
            }
            MakerExitDecision::KeepWorking => {
                report_exit_change(state, symbol, "keeping passive reduce-only order");
            }
        }
    }
    Ok(())
}

fn deadline_key(plan: DualFlattenPlan) -> u64 {
    plan.hard_deadline_ms().unwrap_or(u64::MAX)
}

fn latch_earliest_plan(current: &mut Option<DualFlattenPlan>, candidate: DualFlattenPlan) {
    if current.is_none_or(|value| deadline_key(candidate) < deadline_key(value)) {
        *current = Some(candidate);
    }
}

fn build_exit_plan(
    state: &SymbolState,
    region: EquityRegion,
    now: u64,
    funding_lead_ms: u64,
) -> Option<DualFlattenPlan> {
    let equity_deadline = calendar_for(region).exit_deadline_at(now);
    let funding = match state.mark.as_ref() {
        Some(mark) if mark.next_funding_time_ms > mark.event_time_ms => FundingSchedule::new(
            Some(mark.next_funding_time_ms),
            None,
            mark.latest_funding_rate_e8.map(|rate| rate / 100),
            FundingRateKind::Regular,
            mark.event_time_ms,
        )?,
        Some(mark) => FundingSchedule::new(
            None,
            None,
            mark.latest_funding_rate_e8.map(|rate| rate / 100),
            FundingRateKind::Unknown,
            mark.event_time_ms,
        )?,
        None => return None,
    };
    DualFlattenPlan::new(
        now,
        equity_deadline,
        funding,
        EQUITY_EXIT_LEAD_MS,
        funding_lead_ms,
    )
}

fn market_snapshots_fresh(state: &SymbolState, now: u64, max_age_ms: u64) -> bool {
    let fresh = |event_at: u64, received_at: u64| {
        event_at > 0
            && received_at > 0
            && event_at <= now
            && received_at <= now
            && now - event_at <= max_age_ms
            && now - received_at <= max_age_ms
    };
    state
        .book
        .as_ref()
        .is_some_and(|book| fresh(book.event_time_ms, state.book_received_at_ms))
        && state
            .mark
            .as_ref()
            .is_some_and(|mark| fresh(mark.event_time_ms, state.mark_received_at_ms))
}

fn report_exit_change(state: &mut BTreeMap<String, SymbolState>, symbol: &str, detail: &str) {
    let local = state.get_mut(symbol).expect("initialized symbol");
    if local.last_exit_report.as_deref() != Some(detail) {
        eprintln!(
            "{}",
            serde_json::json!({"event":"maker_exit_state","symbol":symbol,"detail":detail})
        );
        local.last_exit_report = Some(detail.to_owned());
    }
}

async fn reconcile_position(
    client: &BinanceRestClient,
    credentials: &BinanceCredentials,
    symbol: &str,
    state: &mut BTreeMap<String, SymbolState>,
    quantity_scale: u32,
    required_after_ms: u64,
) -> Result<(), String> {
    let timestamp = client.server_time_ms().await.map_err(|e| e.to_string())?;
    let risks = client
        .position_risk(credentials, Some(symbol), timestamp, RECV_WINDOW_MS)
        .await
        .map_err(|e| e.to_string())?;
    let mut rows = risks.iter().filter(|row| row.symbol == symbol);
    let row = rows
        .next()
        .ok_or_else(|| format!("positionRisk missing {symbol}"))?;
    if rows.next().is_some() {
        return Err(format!("multiple position legs for {symbol}"));
    }
    ensure_position_snapshot_fresh(symbol, row.update_time_ms, required_after_ms)?;
    let position = parse_ticks(&row.position_amount, quantity_scale)
        .ok_or_else(|| format!("invalid position precision for {symbol}"))?;
    let local = state.get_mut(symbol).expect("initialized symbol");
    local.position_ticks = position;
    local.position_observed_at_ms = row.update_time_ms;
    local.position_confirmed = true;
    Ok(())
}

fn ensure_position_snapshot_fresh(
    symbol: &str,
    observed_at_ms: u64,
    required_after_ms: u64,
) -> Result<(), String> {
    if required_after_ms > 0 && observed_at_ms < required_after_ms {
        return Err(format!(
            "positionRisk for {symbol} predates latest confirmed fill"
        ));
    }
    Ok(())
}

async fn cancel_and_reconcile(
    client: &BinanceRestClient,
    credentials: &BinanceCredentials,
    permission: TradingPermission,
    symbol: &str,
    working: &mut BTreeMap<String, WorkingOrder>,
    state: &mut BTreeMap<String, SymbolState>,
    quantity_scale: u32,
) -> Result<(), String> {
    let Some(mut order) = working.remove(symbol) else {
        return Ok(());
    };
    if order.cancel_pending {
        working.insert(symbol.to_owned(), order);
        return reconcile_owned_order(client, credentials, symbol, working, state, quantity_scale)
            .await;
    }
    let order_id = order.client_order_id.clone();
    let canceled = permission
        .execute(|| cancel_order(client, credentials, symbol, &order_id))
        .await;
    let response = match canceled {
        Ok(Some(response)) => response,
        Ok(None) => {
            working.insert(symbol.to_owned(), order);
            return Ok(());
        }
        Err(error) => {
            order.cancel_pending = true;
            working.insert(symbol.to_owned(), order);
            if let Some(local) = state.get_mut(symbol) {
                local.position_confirmed = false;
            }
            return Err(format!(
                "cancel outcome unknown for {symbol}/{order_id}: {error}"
            ));
        }
    };
    if let Err(error) = update_order_from_response(&mut order, &response, quantity_scale) {
        order.cancel_pending = true;
        working.insert(symbol.to_owned(), order);
        return Err(error);
    }
    order.cancel_pending = true;

    working.insert(symbol.to_owned(), order);
    reconcile_owned_order(client, credentials, symbol, working, state, quantity_scale).await
}

async fn reconcile_owned_order(
    client: &BinanceRestClient,
    credentials: &BinanceCredentials,
    symbol: &str,
    working: &mut BTreeMap<String, WorkingOrder>,
    state: &mut BTreeMap<String, SymbolState>,
    quantity_scale: u32,
) -> Result<(), String> {
    let Some(mut order) = working.remove(symbol) else {
        return Ok(());
    };
    let timestamp = match client.server_time_ms().await {
        Ok(value) => value,
        Err(error) => {
            working.insert(symbol.to_owned(), order);
            return Err(error.to_string());
        }
    };
    let response = client
        .query_order(
            credentials,
            symbol,
            &order.client_order_id,
            timestamp,
            RECV_WINDOW_MS,
        )
        .await
        .map_err(|error| {
            format!(
                "order reconciliation failed for {}/{}: {error}",
                symbol, order.client_order_id
            )
        });
    let response = match response {
        Ok(response) => response,
        Err(error) => {
            working.insert(symbol.to_owned(), order);
            return Err(error);
        }
    };
    if let Err(error) = update_order_from_response(&mut order, &response, quantity_scale) {
        working.insert(symbol.to_owned(), order);
        return Err(error);
    }
    if !is_terminal_order_status(&response.status) {
        working.insert(symbol.to_owned(), order);
        return Ok(());
    }
    let required_after_ms = order.fill_observed_at_ms;
    let already_confirmed = state.get(symbol).is_some_and(|local| {
        local.position_confirmed && local.position_observed_at_ms >= required_after_ms
    });
    if already_confirmed {
        return Ok(());
    }
    if let Some(local) = state.get_mut(symbol) {
        local.position_confirmed = false;
    }
    if let Err(error) = reconcile_position(
        client,
        credentials,
        symbol,
        state,
        quantity_scale,
        required_after_ms,
    )
    .await
    {
        working.insert(symbol.to_owned(), order);
        return Err(error);
    }
    Ok(())
}

fn update_order_from_response(
    order: &mut WorkingOrder,
    response: &BinanceOrderResponse,
    quantity_scale: u32,
) -> Result<(), String> {
    if response.client_order_id != order.client_order_id {
        return Err("order response identity mismatch".into());
    }
    let executed = parse_ticks(&response.executed_quantity, quantity_scale)
        .ok_or_else(|| "invalid executed quantity in order response".to_owned())?;
    if executed < order.executed_quantity_ticks || executed > order.quantity_ticks {
        return Err("cumulative order fill regressed or exceeded original quantity".into());
    }
    if executed > order.executed_quantity_ticks {
        if response.update_time_ms == 0 {
            return Err("filled order response is missing updateTime".into());
        }
        order.fill_observed_at_ms = order.fill_observed_at_ms.max(response.update_time_ms);
    }
    order.executed_quantity_ticks = executed;
    Ok(())
}

fn is_terminal_order_status(status: &str) -> bool {
    matches!(
        status,
        "FILLED" | "CANCELED" | "EXPIRED" | "EXPIRED_IN_MATCH" | "REJECTED"
    )
}

async fn cleanup_owned_orders(
    client: &BinanceRestClient,
    credentials: &BinanceCredentials,
    permission: TradingPermission,
    working: &mut BTreeMap<String, WorkingOrder>,
    state: &mut BTreeMap<String, SymbolState>,
    quantity_scale: u32,
) {
    for symbol in working.keys().cloned().collect::<Vec<_>>() {
        let result = cancel_and_reconcile(
            client,
            credentials,
            permission,
            &symbol,
            working,
            state,
            quantity_scale,
        )
        .await;
        let cleanup_result = if !permission.allowed() {
            "denied_read_only"
        } else if result.is_ok() && !working.contains_key(&symbol) {
            "confirmed"
        } else {
            "unknown"
        };
        eprintln!(
            "{}",
            serde_json::json!({
                "event":"owned_order_cleanup",
                "symbol":symbol,
                "result":cleanup_result,
                "detail":result.err(),
            })
        );
    }
    for symbol in LIVE_SYMBOLS {
        if let Err(error) =
            reconcile_position(client, credentials, symbol, state, quantity_scale, 0).await
        {
            if let Some(local) = state.get_mut(symbol) {
                local.position_confirmed = false;
            }
            eprintln!(
                "{}",
                serde_json::json!({"event":"position_cleanup_unknown","symbol":symbol,"detail":error})
            );
        }
    }
}

async fn reconcile_symbol(
    client: &BinanceRestClient,
    credentials: &BinanceCredentials,
    symbol: &str,
    quantity_scale: u32,
) -> Result<(), String> {
    let timestamp = client.server_time_ms().await.map_err(|e| e.to_string())?;
    let open = client
        .current_open_orders(credentials, Some(symbol), timestamp, RECV_WINDOW_MS)
        .await
        .map_err(|e| e.to_string())?;
    ensure_startup_orders_clear(symbol, &open)?;
    let timestamp = client.server_time_ms().await.map_err(|e| e.to_string())?;
    let risks = client
        .position_risk(credentials, Some(symbol), timestamp, RECV_WINDOW_MS)
        .await
        .map_err(|e| e.to_string())?;
    let mut rows = risks.iter().filter(|row| row.symbol == symbol);
    let row = rows
        .next()
        .ok_or_else(|| format!("positionRisk missing {symbol}"))?;
    let position = parse_ticks(&row.position_amount, quantity_scale)
        .ok_or_else(|| format!("invalid position precision for {symbol}"))?;
    if rows.next().is_some() {
        return Err(format!("multiple position legs for {symbol}"));
    }
    if position != 0 {
        return Err(format!("non-flat start on {symbol}: {position}"));
    }
    Ok(())
}

fn ensure_startup_orders_clear(symbol: &str, open: &[BinanceOpenOrder]) -> Result<(), String> {
    if open.is_empty() {
        Ok(())
    } else {
        Err(format!(
            "startup found unclaimed open orders for {symbol}; ownership cannot be proven"
        ))
    }
}

fn apply_market(
    state: &mut BTreeMap<String, SymbolState>,
    event: BinanceMarketEvent,
    received_at_ms: u64,
) {
    match event {
        BinanceMarketEvent::BookTicker(value) => {
            if let Some(local) = state.get_mut(&value.symbol) {
                local.book = Some(value);
                local.book_received_at_ms = received_at_ms;
            }
        }
        BinanceMarketEvent::MarkPrice(value) => {
            if let Some(local) = state.get_mut(&value.symbol) {
                if let Some(previous) = local.last_mark_price_ticks {
                    if previous > 0 && value.mark_price.0 > 0 {
                        let change_bps =
                            ((i128::from(value.mark_price.0) - i128::from(previous)).abs() * 10_000
                                / i128::from(previous))
                            .clamp(0, i128::from(i64::MAX)) as i64;
                        local.ewma_abs_return_bps = ewma(local.ewma_abs_return_bps, change_bps);
                    }
                }
                local.last_mark_price_ticks = Some(value.mark_price.0);
                local.mark = Some(value);
                local.mark_received_at_ms = received_at_ms;
            }
        }
        BinanceMarketEvent::AggTrade(_) => {}
    }
}

fn ewma(previous: i64, sample: i64) -> i64 {
    if previous <= 0 {
        sample.max(0)
    } else {
        ((i128::from(previous) * 7 + i128::from(sample.max(0)) * 3) / 10)
            .clamp(0, i128::from(i64::MAX)) as i64
    }
}

fn apply_user(
    state: &mut BTreeMap<String, SymbolState>,
    working: &mut BTreeMap<String, WorkingOrder>,
    event: &UserDataEvent,
    quantity_scale: u32,
) -> Result<(), String> {
    match event {
        UserDataEvent::OrderUpdate(update) => {
            let Some(order) = working.get_mut(&update.symbol) else {
                return Ok(());
            };
            if order.client_order_id != update.client_order_id {
                return Ok(());
            }
            let cumulative = parse_ticks(&update.executed_quantity, quantity_scale)
                .ok_or_else(|| format!("invalid order fill precision for {}", update.symbol))?;
            if cumulative < order.executed_quantity_ticks || cumulative > order.quantity_ticks {
                return Err(format!("invalid cumulative fill for {}", update.symbol));
            }
            if cumulative != order.executed_quantity_ticks {
                if update.transaction_time_ms == 0 {
                    return Err(format!("fill timestamp missing for {}", update.symbol));
                }
                order.executed_quantity_ticks = cumulative;
                order.fill_observed_at_ms =
                    order.fill_observed_at_ms.max(update.transaction_time_ms);
                if let Some(local) = state.get_mut(&update.symbol) {
                    local.position_confirmed = false;
                }
            }
            if is_terminal_order_status(&update.status) {
                order.cancel_pending = true;
            }
        }
        UserDataEvent::AccountUpdate(update) => {
            for position in &update.positions {
                let value = parse_ticks(&position.position_amount, quantity_scale)
                    .ok_or_else(|| format!("invalid account precision for {}", position.symbol))?;
                if let Some(local) = state.get_mut(&position.symbol) {
                    if update.transaction_time_ms < local.position_observed_at_ms {
                        return Err(format!("stale account position for {}", position.symbol));
                    }
                    local.position_ticks = value;
                    local.position_confirmed = true;
                    local.position_observed_at_ms = update.transaction_time_ms;
                }
            }
        }
        UserDataEvent::ListenKeyExpired => {}
    }
    Ok(())
}

fn make_intent(
    symbol: &str,
    state: &SymbolState,
    anchor_ticks: i64,
    args: &Args,
) -> Option<static_anchor_engine::execution::OrderIntent> {
    let book = state.book.as_ref()?;
    let mark = state.mark.as_ref()?;
    let now = now_ms();
    if !market_snapshots_fresh(state, now, MARKET_MAX_AGE_MS) {
        return None;
    }
    let market_at = mark.event_time_ms.min(book.event_time_ms);
    adaptive_intent_from_market(
        stable_symbol_id(symbol),
        book.bid_price,
        book.bid_quantity.0,
        book.ask_price,
        book.ask_quantity.0,
        static_anchor_engine::strategy::PriceTicks(anchor_ticks),
        mark.index_price,
        mark.mark_price,
        state.position_ticks,
        args.max_position,
        args.quantity,
        state.ewma_abs_return_bps,
        args.entry_threshold_bps,
        4,
        args.max_mark_index_gap_bps,
        now.saturating_sub(market_at),
        5_000,
    )
}

fn spawn_market(
    args: &Args,
    tx: tokio::sync::mpsc::Sender<Event>,
    event_overflow: Arc<AtomicBool>,
) -> Result<(), String> {
    let endpoints = args.environment.endpoints();
    let shards = BinanceMarketConfig::for_symbols(
        endpoints.market_ws_base,
        &LIVE_SYMBOLS,
        BinanceMarketFeed::ReferenceAndTrades,
        args.price_scale,
        args.quantity_scale,
        MAX_FRAME_BYTES,
        5_000,
        15_000,
        args.proxy.clone(),
        static_anchor_engine::market::ReconnectPolicy {
            max_attempts: Some(3),
            ..Default::default()
        },
        args.max_subscriptions_per_shard,
    )
    .map_err(|e| format!("{e:?}"))?;
    for shard in shards {
        let producer = tx.clone();
        let overflow = event_overflow.clone();
        tokio::spawn(async move {
            let mut stream = BinanceMarketStream::new(shard);
            let result = stream
                .run_until_error(|event| {
                    try_send_event(&producer, &overflow, Event::Market(event));
                })
                .await;
            let _ = producer
                .send(Event::Halt(format!("market stream ended: {result:?}")))
                .await;
        });
    }
    Ok(())
}

fn spawn_fx(args: &Args, tx: tokio::sync::mpsc::Sender<Event>) -> Result<(), String> {
    let client = BinanceC2cFxClient::new(args.proxy.as_deref()).map_err(|e| e.to_string())?;
    let currencies = vec![AnchorCurrency::Cny, AnchorCurrency::Hkd];
    let poller = BinanceC2cFxPoller::new(client, &currencies, FxPollerConfig::high_frequency())
        .map_err(|e| e.to_string())?;
    let (fx_tx, mut fx_rx) = tokio::sync::mpsc::channel::<FxUpdate>(128);
    let producer = tx.clone();
    tokio::spawn(async move {
        if let Err(error) = poller.run(fx_tx).await {
            let _ = producer
                .send(Event::Halt(format!("FX stream ended: {error}")))
                .await;
        }
    });
    tokio::spawn(async move {
        while let Some(update) = fx_rx.recv().await {
            if tx.send(Event::Fx(update)).await.is_err() {
                break;
            }
        }
    });
    Ok(())
}

fn spawn_metadata(args: &Args, tx: tokio::sync::mpsc::Sender<Event>) -> Result<(), String> {
    let client = PublicMarketMetadataClient::new(
        args.environment.endpoints().rest_base,
        args.proxy.as_deref(),
    )
    .map_err(|error| error.to_string())?;
    tokio::spawn(async move {
        loop {
            let result = async {
                let rows = client
                    .exchange_info()
                    .await
                    .map_err(|error| error.to_string())?;
                let mut filters = BTreeMap::new();
                for symbol in LIVE_SYMBOLS {
                    let row = rows
                        .iter()
                        .find(|row| row.symbol == symbol)
                        .ok_or_else(|| format!("exchangeInfo missing {symbol}"))?;
                    if !row.is_trading_tradifi_perpetual() {
                        return Err(format!("{symbol} is not a trading TradFi perpetual"));
                    }
                    filters.insert(
                        symbol.to_owned(),
                        row.execution_filters().map_err(|error| error.to_string())?,
                    );
                }
                Ok(MetadataSnapshot {
                    observed_at_ms: now_ms(),
                    filters,
                })
            }
            .await;
            if tx.send(Event::Metadata(result)).await.is_err() {
                return;
            }
            tokio::time::sleep(Duration::from_millis(METADATA_REFRESH_MS)).await;
        }
    });
    Ok(())
}

async fn spawn_user_data(
    args: &Args,
    client: Arc<BinanceRestClient>,
    credentials: BinanceCredentials,
    tx: tokio::sync::mpsc::Sender<Event>,
    event_overflow: Arc<AtomicBool>,
) -> Result<String, String> {
    let listen_key = client
        .start_user_data_stream(&credentials)
        .await
        .map_err(|e| e.to_string())?;
    let stream =
        BinanceUserDataStream::new(args.environment, listen_key.clone(), args.proxy.clone())
            .map_err(|e| e.to_string())?;
    tokio::spawn(async move {
        let result = stream
            .run(|event| {
                try_send_event(&tx, &event_overflow, Event::User(event));
            })
            .await;
        let _ = tx
            .send(Event::Halt(format!("user data stream ended: {result:?}")))
            .await;
    });
    Ok(listen_key)
}

fn try_send_event(tx: &tokio::sync::mpsc::Sender<Event>, overflow: &AtomicBool, event: Event) {
    if tx.try_send(event).is_err() {
        overflow.store(true, Ordering::Release);
    }
}

fn spawn_keepalive(
    client: Arc<BinanceRestClient>,
    credentials: BinanceCredentials,
    listen_key: String,
    tx: tokio::sync::mpsc::Sender<Event>,
) {
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(Duration::from_secs(30 * 60));
        loop {
            interval.tick().await;
            if let Err(error) = client
                .keepalive_user_data_stream(&credentials, &listen_key)
                .await
            {
                let _ = tx
                    .send(Event::Halt(format!("listenKey keepalive failed: {error}")))
                    .await;
                return;
            }
        }
    });
}

// This edge adapter keeps authentication, order identity, and exchange scales explicit.
#[allow(clippy::too_many_arguments)]
async fn place_order(
    client: &BinanceRestClient,
    credentials: &BinanceCredentials,
    symbol: &str,
    intent: static_anchor_engine::execution::OrderIntent,
    price_scale: u32,
    quantity_scale: u32,
    now: u64,
    sequence: u64,
    reduce_only: bool,
) -> Result<WorkingOrder, String> {
    let client_order_id = format!("anchorbell-{}-{}", now, sequence);
    let response = client
        .place_maker_order(
            credentials,
            BinanceMakerOrderRequest {
                symbol: symbol.to_owned(),
                side: intent.side,
                price: format_ticks(intent.price, price_scale),
                quantity: format_ticks(intent.quantity, quantity_scale),
                client_order_id: client_order_id.clone(),
                reduce_only,
            },
            client.server_time_ms().await.map_err(|e| e.to_string())?,
            RECV_WINDOW_MS,
        )
        .await
        .map_err(|e| e.to_string())?;
    let executed_quantity_ticks = parse_ticks(&response.executed_quantity, quantity_scale)
        .ok_or_else(|| "invalid executed quantity in placement response".to_owned())?;
    if executed_quantity_ticks < 0 || executed_quantity_ticks > intent.quantity {
        return Err("placement response fill exceeds original quantity".into());
    }
    Ok(WorkingOrder {
        client_order_id,
        side: intent.side,
        price_ticks: intent.price,
        quantity_ticks: intent.quantity,
        executed_quantity_ticks,
        fill_observed_at_ms: if executed_quantity_ticks > 0 {
            if response.update_time_ms == 0 {
                return Err("filled placement response is missing updateTime".into());
            }
            response.update_time_ms
        } else {
            0
        },
        reduce_only,
        cancel_pending: is_terminal_order_status(&response.status),
    })
}

async fn cancel_order(
    client: &BinanceRestClient,
    credentials: &BinanceCredentials,
    symbol: &str,
    client_order_id: &str,
) -> Result<BinanceOrderResponse, String> {
    client
        .cancel_order(
            credentials,
            symbol,
            client_order_id,
            client.server_time_ms().await.map_err(|e| e.to_string())?,
            RECV_WINDOW_MS,
        )
        .await
        .map_err(|e| e.to_string())
}

fn equity_is_closed_at(region: EquityRegion, timestamp_ms: u64) -> bool {
    let local_seconds = timestamp_ms / 1_000 + 8 * 3_600;
    let local_day = local_seconds / 86_400;
    let weekday = ((local_day + 3) % 7 + 1) as u8;
    let minute = (local_seconds % 86_400 / 60) as u16;
    let calendar = calendar_for(region);
    let date_key = static_anchor_engine::strategy::EquitySessionCalendar::date_key_from_timestamp(
        timestamp_ms,
    );
    calendar.entry_allowed_on_date(date_key, weekday, minute, 30, true, true)
}

fn load_credentials(environment: BinanceEnvironment) -> Result<BinanceCredentials, String> {
    BinanceCredentials::from_environment_for(environment)
        .map_err(|error| format!("credentials unavailable: {error:?}"))
}

fn stable_symbol_id(symbol: &str) -> u32 {
    let mut hash = 2_166_136_261_u32;
    for byte in symbol.bytes() {
        hash = (hash ^ u32::from(byte)).wrapping_mul(16_777_619);
    }
    if hash == 0 {
        1
    } else {
        hash
    }
}

fn exit_constraints_from_filters(
    filters: &BinanceExecutionFilters,
    mark_price_ticks: i64,
    price_scale: u32,
    quantity_scale: u32,
    observed_at_ms: u64,
) -> Option<ExitConstraints> {
    if mark_price_ticks <= 0 {
        return None;
    }
    let static_min = parse_unsigned_ticks(&filters.min_price, price_scale)?;
    let static_max = parse_unsigned_ticks(&filters.max_price, price_scale)?;
    let (down, down_scale) = parse_positive_decimal(&filters.multiplier_down)?;
    let (up, up_scale) = parse_positive_decimal(&filters.multiplier_up)?;
    let percent_min = multiply_decimal_bound(mark_price_ticks, down, down_scale, true)?;
    let percent_max = multiply_decimal_bound(mark_price_ticks, up, up_scale, false)?;
    let min_price = static_min.max(percent_min);
    let max_price = static_max.min(percent_max);
    Some(ExitConstraints {
        min_price,
        max_price,
        price_tick: parse_unsigned_ticks(&filters.price_tick, price_scale)?,
        min_quantity: parse_unsigned_ticks(&filters.min_quantity, quantity_scale)?,
        max_quantity: parse_unsigned_ticks(&filters.max_quantity, quantity_scale)?,
        quantity_step: parse_unsigned_ticks(&filters.quantity_step, quantity_scale)?,
        min_notional: parse_unsigned_ticks(&filters.min_notional, price_scale)?,
        quantity_scale,
        observed_at_ms,
        max_age_ms: METADATA_MAX_AGE_MS,
    })
}

fn parse_unsigned_ticks(value: &str, scale: u32) -> Option<i64> {
    let parsed = parse_ticks(value, scale)?;
    (parsed > 0).then_some(parsed)
}

fn parse_positive_decimal(value: &str) -> Option<(i128, u32)> {
    if value.starts_with('-') {
        return None;
    }
    let (whole, fraction) = value.split_once('.').unwrap_or((value, ""));
    if whole.is_empty()
        || !whole.bytes().all(|byte| byte.is_ascii_digit())
        || !fraction.bytes().all(|byte| byte.is_ascii_digit())
        || fraction.len() > 18
    {
        return None;
    }
    let scale = u32::try_from(fraction.len()).ok()?;
    let divisor = 10_i128.checked_pow(scale)?;
    let mantissa = whole
        .parse::<i128>()
        .ok()?
        .checked_mul(divisor)?
        .checked_add(if fraction.is_empty() {
            0
        } else {
            fraction.parse::<i128>().ok()?
        })?;
    (mantissa > 0).then_some((mantissa, scale))
}

fn multiply_decimal_bound(value: i64, mantissa: i128, scale: u32, round_up: bool) -> Option<i64> {
    let divisor = 10_i128.checked_pow(scale)?;
    let numerator = i128::from(value).checked_mul(mantissa)?;
    let adjusted = if round_up {
        numerator.checked_add(divisor.checked_sub(1)?)?
    } else {
        numerator
    };
    i64::try_from(adjusted / divisor).ok()
}

fn parse_ticks(value: &str, scale: u32) -> Option<i64> {
    let (negative, unsigned) = value
        .strip_prefix('-')
        .map_or((false, value), |v| (true, v));
    let (whole, fraction) = unsigned.split_once('.').unwrap_or((unsigned, ""));
    if whole.is_empty()
        || !whole.bytes().all(|b| b.is_ascii_digit())
        || !fraction.bytes().all(|b| b.is_ascii_digit())
        || (fraction.len() > scale as usize
            && fraction.as_bytes()[scale as usize..]
                .iter()
                .any(|byte| *byte != b'0'))
    {
        return None;
    }
    let multiplier = 10_i128.checked_pow(scale)?;
    let whole = whole.parse::<i128>().ok()?.checked_mul(multiplier)?;
    let significant_fraction = &fraction[..fraction.len().min(scale as usize)];
    let fraction = if significant_fraction.is_empty() {
        0
    } else {
        significant_fraction.parse::<i128>().ok()?.checked_mul(
            10_i128.checked_pow(scale.saturating_sub(significant_fraction.len() as u32))?,
        )?
    };
    let signed = if negative {
        whole.checked_add(fraction)?.checked_neg()?
    } else {
        whole.checked_add(fraction)?
    };
    i64::try_from(signed).ok()
}

fn format_ticks(value: i64, scale: u32) -> String {
    let negative = value < 0;
    let value = value.unsigned_abs();
    let multiplier = 10_u64.pow(scale.min(18));
    let whole = value / multiplier;
    let fraction = value % multiplier;
    if scale == 0 {
        return format!("{}{}", if negative { "-" } else { "" }, whole);
    }
    format!(
        "{}{}.{:0width$}",
        if negative { "-" } else { "" },
        whole,
        fraction,
        width = scale as usize
    )
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .min(u128::from(u64::MAX)) as u64
}

fn side_name(side: Side) -> &'static str {
    match side {
        Side::Buy => "BUY",
        Side::Sell => "SELL",
    }
}

fn parse_args() -> Result<Args, String> {
    let mut args = Args {
        environment: BinanceEnvironment::Testnet,
        duration_secs: 60,
        proxy: None,
        price_scale: 8,
        quantity_scale: 8,
        max_position: 1,
        quantity: 1,
        entry_threshold_bps: 100,
        max_mark_index_gap_bps: 50,
        max_anchor_age_ms: 0,
        funding_lead_ms: 300_000,
        max_subscriptions_per_shard: 64,
        send_orders: false,
    };
    let mut values = env::args().skip(1);
    while let Some(flag) = values.next() {
        match flag.as_str() {
            "--help" | "-h" => {
                println!("anchorbell_live [--environment testnet|production] [--duration-secs N] [--proxy URL] [--send-orders]");
                process::exit(0);
            }
            "--environment" => {
                args.environment = values
                    .next()
                    .ok_or("missing --environment")?
                    .parse()
                    .map_err(|_| "invalid --environment".to_owned())?
            }
            "--duration-secs" => args.duration_secs = parse_next(&mut values, &flag)?,
            "--proxy" => args.proxy = Some(values.next().ok_or("missing --proxy")?),
            "--price-scale" => args.price_scale = parse_next(&mut values, &flag)?,
            "--quantity-scale" => args.quantity_scale = parse_next(&mut values, &flag)?,
            "--max-position" => args.max_position = parse_next(&mut values, &flag)?,
            "--quantity" => args.quantity = parse_next(&mut values, &flag)?,
            "--entry-threshold-bps" => args.entry_threshold_bps = parse_next(&mut values, &flag)?,
            "--max-mark-index-gap-bps" => {
                args.max_mark_index_gap_bps = parse_next(&mut values, &flag)?
            }
            "--max-anchor-age-ms" => args.max_anchor_age_ms = parse_next(&mut values, &flag)?,
            "--funding-lead-ms" => args.funding_lead_ms = parse_next(&mut values, &flag)?,
            "--max-subscriptions-per-shard" => {
                args.max_subscriptions_per_shard = parse_next(&mut values, &flag)?
            }
            "--send-orders" => args.send_orders = true,
            other => return Err(format!("unknown flag {other}")),
        }
    }
    if args.duration_secs == 0
        || args.price_scale > 18
        || args.quantity_scale > 18
        || args.max_position <= 0
        || args.quantity <= 0
        || args.entry_threshold_bps < 0
        || args.max_mark_index_gap_bps < 0
        || args.max_subscriptions_per_shard == 0
    {
        return Err("invalid numeric configuration".into());
    }
    Ok(args)
}

fn parse_next<T: std::str::FromStr>(
    values: &mut impl Iterator<Item = String>,
    flag: &str,
) -> Result<T, String> {
    values
        .next()
        .ok_or_else(|| format!("missing {flag}"))?
        .parse()
        .map_err(|_| format!("invalid value for {flag}"))
}

fn fail(message: &str) -> ! {
    eprintln!("{message}");
    process::exit(2);
}

#[cfg(test)]
mod tests {
    use super::*;
    use static_anchor_engine::{
        execution::{AccountUpdate, OrderUpdate, PositionUpdate},
        strategy::MakerExitInput,
    };

    fn book(event_time_ms: u64) -> BookTicker {
        BookTicker {
            symbol: "CXMTUSDT".into(),
            event_time_ms,
            transaction_time_ms: event_time_ms,
            update_id: 1,
            bid_price: static_anchor_engine::strategy::PriceTicks(9_880),
            bid_quantity: static_anchor_engine::strategy::Quantity(10),
            ask_price: static_anchor_engine::strategy::PriceTicks(9_890),
            ask_quantity: static_anchor_engine::strategy::Quantity(10),
        }
    }

    fn mark(event_time_ms: u64, funding_ms: u64) -> MarkPrice {
        MarkPrice {
            symbol: "CXMTUSDT".into(),
            event_time_ms,
            mark_price: static_anchor_engine::strategy::PriceTicks(9_885),
            index_price: static_anchor_engine::strategy::PriceTicks(9_887),
            next_funding_time_ms: funding_ms,
            latest_funding_rate_e8: Some(100),
        }
    }

    fn filters() -> BinanceExecutionFilters {
        BinanceExecutionFilters {
            min_price: "1.00".into(),
            max_price: "200.00".into(),
            price_tick: "0.10".into(),
            min_quantity: "0.001".into(),
            max_quantity: "50.000".into(),
            quantity_step: "0.001".into(),
            min_notional: "5.00".into(),
            multiplier_up: "1.10".into(),
            multiplier_down: "0.90".into(),
        }
    }

    fn working() -> WorkingOrder {
        WorkingOrder {
            client_order_id: "anchorbell-owned-1".into(),
            side: Side::Sell,
            price_ticks: 9_890,
            quantity_ticks: 10,
            executed_quantity_ticks: 0,
            fill_observed_at_ms: 0,
            reduce_only: true,
            cancel_pending: false,
        }
    }

    fn order_update(client_order_id: &str, status: &str, executed: &str) -> UserDataEvent {
        UserDataEvent::OrderUpdate(Box::new(OrderUpdate {
            event_time_ms: 1_100,
            transaction_time_ms: 1_100,
            symbol: "CXMTUSDT".into(),
            client_order_id: client_order_id.into(),
            order_id: 7,
            side: "SELL".into(),
            order_type: "LIMIT".into(),
            time_in_force: "GTX".into(),
            status: status.into(),
            execution_type: status.into(),
            executed_quantity: executed.into(),
            last_filled_quantity: executed.into(),
            average_price: "9890".into(),
            reduce_only: true,
        }))
    }

    fn order_response(status: &str, executed: &str, update_time_ms: u64) -> BinanceOrderResponse {
        BinanceOrderResponse {
            order_id: 7,
            symbol: "CXMTUSDT".into(),
            status: status.into(),
            client_order_id: "anchorbell-owned-1".into(),
            side: "SELL".into(),
            time_in_force: "GTX".into(),
            order_type: "LIMIT".into(),
            price: "98.90".into(),
            original_quantity: "10".into(),
            executed_quantity: executed.into(),
            average_price: "98.90".into(),
            update_time_ms,
            reduce_only: true,
        }
    }

    #[test]
    fn static_filters_and_percent_price_are_scaled_exactly() {
        let value = exit_constraints_from_filters(&filters(), 10_000, 2, 3, 1_000).unwrap();
        assert_eq!(value.min_price, 9_000);
        assert_eq!(value.max_price, 11_000);
        assert_eq!(value.price_tick, 10);
        assert_eq!(value.min_quantity, 1);
        assert_eq!(value.max_quantity, 50_000);
        assert_eq!(value.min_notional, 500);
        assert_eq!(parse_ticks("2.5000", 1), Some(25));
        assert_eq!(parse_ticks("2.5001", 1), None);
    }

    #[test]
    fn fresh_mark_never_masks_a_stale_book() {
        let state = SymbolState {
            book: Some(book(1)),
            book_received_at_ms: 1,
            mark: Some(mark(7_000, 10_000)),
            mark_received_at_ms: 7_000,
            ..Default::default()
        };
        assert!(!market_snapshots_fresh(&state, 7_000, MARKET_MAX_AGE_MS));
    }

    #[test]
    fn funding_window_produces_passive_exit_without_entry_alpha() {
        let state = SymbolState {
            book: Some(book(1_000)),
            book_received_at_ms: 1_000,
            mark: Some(mark(1_000, 10_000)),
            mark_received_at_ms: 1_000,
            position_ticks: 10,
            position_confirmed: true,
            ..Default::default()
        };
        let plan = build_exit_plan(&state, EquityRegion::AShare, 1_000, 9_000).unwrap();
        let decision = decide_maker_exit(MakerExitInput {
            symbol: 7,
            position: 10,
            position_confirmed: true,
            now_ms: 1_000,
            max_book_age_ms: MARKET_MAX_AGE_MS,
            plan,
            book: Some(ExitBook {
                bid: 9_880,
                ask: 9_890,
                observed_at_ms: 1_000,
            }),
            constraints: Some(ExitConstraints {
                min_price: 1,
                max_price: 20_000,
                price_tick: 10,
                min_quantity: 1,
                max_quantity: 50,
                quantity_step: 1,
                min_notional: 1,
                quantity_scale: 0,
                observed_at_ms: 1_000,
                max_age_ms: METADATA_MAX_AGE_MS,
            }),
            working: ExitWorkingOrder::None,
        });
        assert_eq!(
            decision,
            MakerExitDecision::Submit(static_anchor_engine::execution::OrderIntent::maker_sell(
                7, 9_890, 10
            ))
        );
    }

    #[test]
    fn an_existing_deadline_cannot_roll_forward() {
        let no_funding =
            |now| FundingSchedule::no_event(None, None, FundingRateKind::Unknown, now).unwrap();
        let first = DualFlattenPlan::new(1_000, Some(10_000), no_funding(1_000), 100, 100).unwrap();
        let later = DualFlattenPlan::new(2_000, Some(20_000), no_funding(2_000), 100, 100).unwrap();
        let mut latched = Some(first);
        latch_earliest_plan(&mut latched, later);
        assert_eq!(latched.unwrap().hard_deadline_ms(), Some(10_000));
    }

    #[test]
    fn foreign_terminal_event_cannot_remove_an_owned_order() {
        let mut states = BTreeMap::from([(
            "CXMTUSDT".to_owned(),
            SymbolState {
                position_ticks: 10,
                position_confirmed: true,
                ..Default::default()
            },
        )]);
        let mut orders = BTreeMap::from([("CXMTUSDT".to_owned(), working())]);
        apply_user(
            &mut states,
            &mut orders,
            &order_update("anchorbell-foreign", "CANCELED", "0"),
            0,
        )
        .unwrap();
        assert_eq!(orders["CXMTUSDT"].client_order_id, "anchorbell-owned-1");
        assert!(!orders["CXMTUSDT"].cancel_pending);

        apply_user(
            &mut states,
            &mut orders,
            &order_update("anchorbell-owned-1", "CANCELED", "3.0000"),
            0,
        )
        .unwrap();
        assert_eq!(orders["CXMTUSDT"].remaining_quantity(), Some(7));
        assert_eq!(orders["CXMTUSDT"].fill_observed_at_ms, 1_100);
        assert!(orders["CXMTUSDT"].cancel_pending);
        assert!(!states["CXMTUSDT"].position_confirmed);
    }

    #[test]
    fn fill_reconciliation_rejects_regression_overfill_and_missing_time() {
        let mut order = working();
        update_order_from_response(
            &mut order,
            &order_response("PARTIALLY_FILLED", "3", 1_100),
            0,
        )
        .unwrap();
        assert_eq!(order.executed_quantity_ticks, 3);
        assert_eq!(order.fill_observed_at_ms, 1_100);

        assert!(update_order_from_response(
            &mut order,
            &order_response("PARTIALLY_FILLED", "2", 1_200),
            0
        )
        .is_err());
        assert!(
            update_order_from_response(&mut order, &order_response("FILLED", "11", 1_200), 0)
                .is_err()
        );
        assert_eq!(order.executed_quantity_ticks, 3);

        let mut fresh = working();
        assert!(
            update_order_from_response(&mut fresh, &order_response("FILLED", "1", 0), 0).is_err()
        );
        assert_eq!(fresh.executed_quantity_ticks, 0);
    }

    #[test]
    fn position_snapshot_must_not_predate_latest_fill() {
        assert!(ensure_position_snapshot_fresh("CXMTUSDT", 1_100, 1_100).is_ok());
        assert!(ensure_position_snapshot_fresh("CXMTUSDT", 1_099, 1_100).is_err());
        assert!(ensure_position_snapshot_fresh("CXMTUSDT", 0, 0).is_ok());
    }

    #[test]
    fn confirmed_account_update_is_required_after_a_fill() {
        let mut states = BTreeMap::from([(
            "CXMTUSDT".to_owned(),
            SymbolState {
                position_ticks: 10,
                position_confirmed: false,
                ..Default::default()
            },
        )]);
        let mut orders = BTreeMap::new();
        apply_user(
            &mut states,
            &mut orders,
            &UserDataEvent::AccountUpdate(AccountUpdate {
                event_time_ms: 1_200,
                transaction_time_ms: 1_200,
                positions: vec![PositionUpdate {
                    symbol: "CXMTUSDT".into(),
                    position_amount: "7.0000".into(),
                    entry_price: "1".into(),
                    unrealized_profit: "0".into(),
                    position_side: "BOTH".into(),
                }],
            }),
            0,
        )
        .unwrap();
        assert!(states["CXMTUSDT"].position_confirmed);
        assert_eq!(states["CXMTUSDT"].position_ticks, 7);
    }

    #[test]
    fn startup_never_claims_orders_by_prefix() {
        let open = BinanceOpenOrder {
            symbol: "CXMTUSDT".into(),
            client_order_id: "anchorbell-old-process".into(),
            status: "NEW".into(),
            executed_quantity: "0".into(),
            order_id: 1,
        };
        assert!(ensure_startup_orders_clear("CXMTUSDT", &[]).is_ok());
        assert!(ensure_startup_orders_clear("CXMTUSDT", &[open]).is_err());
    }

    #[test]
    fn every_exchange_terminal_status_enters_reconciliation() {
        for status in [
            "FILLED",
            "CANCELED",
            "EXPIRED",
            "EXPIRED_IN_MATCH",
            "REJECTED",
        ] {
            assert!(is_terminal_order_status(status));
        }
        assert!(!is_terminal_order_status("NEW"));
        assert!(!is_terminal_order_status("PARTIALLY_FILLED"));
    }

    #[tokio::test]
    async fn event_queue_overflow_is_fail_closed() {
        let (tx, _rx) = tokio::sync::mpsc::channel(1);
        let overflow = AtomicBool::new(false);
        try_send_event(&tx, &overflow, Event::Halt("first".into()));
        assert!(!overflow.load(Ordering::Acquire));
        try_send_event(&tx, &overflow, Event::Halt("dropped".into()));
        assert!(overflow.load(Ordering::Acquire));
    }
}
