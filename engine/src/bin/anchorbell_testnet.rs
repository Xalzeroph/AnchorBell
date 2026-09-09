use std::{
    env,
    path::PathBuf,
    process,
    str::FromStr,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use anchorbell_engine::{
    execution::{
        BinanceCredentials, BinanceEnvironment, BinanceMakerOrderRequest, BinanceOrderResponse,
        BinanceRestClient, DeploymentConfig, PersistentCredentialStore, SessionCheckpoint, Side,
    },
    market::{
        binance::{BinanceMarketEvent, BookTicker, MarkPrice},
        BinanceMarketConfig, BinanceMarketStream, BinanceSubscription, ReconnectPolicy,
    },
    strategy::{adaptive_intent_from_market, StrategyProfile},
};

#[derive(Debug)]
struct Args {
    symbol: String,
    anchor_ticks: i64,
    price_scale: u32,
    quantity_scale: u32,
    requested_quantity: i64,
    max_position: i64,
    entry_threshold_bps: i64,
    max_mark_index_gap_bps: i64,
    duration_secs: u64,
    poll_ms: u64,
    min_replace_ms: u64,
    recv_window_ms: u64,
    environment: BinanceEnvironment,
    proxy: Option<String>,
    checkpoint_path: Option<PathBuf>,
    send_orders: bool,
}

#[derive(Debug, Clone)]
struct WorkingOrder {
    client_order_id: String,
    side: Side,
    price_ticks: i64,
    quantity_ticks: i64,
}

#[derive(Debug, Default)]
struct State {
    book: Option<BookTicker>,
    mark: Option<MarkPrice>,
    last_mark_price_ticks: Option<i64>,
    ewma_abs_return_bps: i64,
    working: Option<WorkingOrder>,
    position_ticks: i64,
    last_action_ms: u64,
    next_order_id: u64,
    last_proposal: Option<(Side, i64, i64)>,
    halted: bool,
}

#[tokio::main]
async fn main() {
    let args = match parse_args() {
        Ok(args) => args,
        Err(message) => fail(&message),
    };
    if let Err(message) = validate_args(&args) {
        fail(&message);
    }
    match run(args).await {
        Ok(exit_code) => process::exit(exit_code),
        Err(message) => {
            eprintln!("testnet runner failed: {message}");
            process::exit(1);
        }
    }
}

async fn run(args: Args) -> Result<i32, String> {
    let deployment = DeploymentConfig::from_process_environment()
        .map_err(|error| format!("deployment config rejected: {error:?}"))?;
    if deployment.environment != args.environment {
        return Err(format!(
            "--environment {} does not match {}={}",
            args.environment,
            anchorbell_engine::execution::ENVIRONMENT_VAR,
            deployment.environment
        ));
    }
    let credentials = load_credentials(args.environment)?;
    let policy = deployment.policy(true);
    if args.send_orders && !policy.allow_live_orders {
        return Err(
            "order submission is disabled; set ANCHORBELL_ENABLE_ORDER_SUBMISSION=1 \
             (and production confirmation when applicable)"
                .into(),
        );
    }
    let client = BinanceRestClient::new(args.environment, policy, args.proxy.as_deref())
        .map_err(|error| error.to_string())?;
    let server_time = client
        .server_time_ms()
        .await
        .map_err(|error| error.to_string())?;
    println!(
        "{}",
        serde_json::json!({
            "event": "runner_started",
            "environment": args.environment.as_str(),
            "symbol": args.symbol,
            "server_time_ms": server_time,
            "order_submission": args.send_orders,
        })
    );

    let session_id = format!("{}-{}", args.symbol, server_time);
    if let Some(path) = args.checkpoint_path.as_deref().filter(|path| path.exists()) {
        let checkpoint = SessionCheckpoint::read(path)
            .map_err(|error| format!("checkpoint rejected: {error}"))?;
        if checkpoint.symbol != args.symbol || checkpoint.environment != args.environment.as_str() {
            return Err("checkpoint symbol/environment does not match this run".into());
        }
        println!(
            "{}",
            serde_json::json!({
                "event": "checkpoint_loaded",
                "path": path,
                "risk_stopped": checkpoint.risk_stopped,
                "position_ticks": checkpoint.position_ticks,
                "working_orders": checkpoint.working_order_ids.len(),
            })
        );
    }

    let mut state = State::default();
    reconcile_open_orders(&args, &client, &credentials, &mut state).await?;
    refresh_position(&args, &client, &credentials, &mut state).await?;
    if state.position_ticks != 0 {
        return Err(format!(
            "runner requires a flat start; remote position ticks={}",
            state.position_ticks
        ));
    }

    let subscription =
        BinanceSubscription::new(&args.symbol).map_err(|error| format!("{error:?}"))?;
    let endpoints = args.environment.endpoints();
    let public_config = BinanceMarketConfig {
        market_ws_base: endpoints.public_market_ws_base.into(),
        subscriptions: vec![subscription.clone().book_ticker_only()],
        price_scale: args.price_scale,
        quantity_scale: args.quantity_scale,
        max_frame_bytes: 1_048_576,
        connect_timeout_ms: 5_000,
        read_timeout_ms: 15_000,
        http_proxy: args.proxy.clone(),
        reconnect: ReconnectPolicy {
            max_attempts: Some(3),
            ..ReconnectPolicy::default()
        },
    };
    let market_config = BinanceMarketConfig {
        market_ws_base: endpoints.market_ws_base.into(),
        subscriptions: vec![subscription.market_reference_and_trades()],
        price_scale: args.price_scale,
        quantity_scale: args.quantity_scale,
        max_frame_bytes: 1_048_576,
        connect_timeout_ms: 5_000,
        read_timeout_ms: 15_000,
        http_proxy: args.proxy.clone(),
        reconnect: ReconnectPolicy {
            max_attempts: Some(3),
            ..ReconnectPolicy::default()
        },
    };
    let (event_tx, mut event_rx) = tokio::sync::mpsc::channel(4096);
    let public_event_tx = event_tx.clone();
    let public_task = tokio::spawn(async move {
        let mut stream = BinanceMarketStream::new(public_config);
        stream
            .run_until_error(|event| {
                if public_event_tx.try_send(event).is_err() {
                    eprintln!("public market event queue is full; event dropped");
                }
            })
            .await
            .map_err(|error| error.to_string())
    });
    let market_task = tokio::spawn(async move {
        let mut stream = BinanceMarketStream::new(market_config);
        stream
            .run_until_error(|event| {
                if event_tx.try_send(event).is_err() {
                    eprintln!("market event queue is full; event dropped");
                }
            })
            .await
            .map_err(|error| error.to_string())
    });

    let mut poll = tokio::time::interval(Duration::from_millis(args.poll_ms));
    let deadline = tokio::time::sleep(Duration::from_secs(args.duration_secs));
    tokio::pin!(deadline);
    let interrupt = tokio::signal::ctrl_c();
    tokio::pin!(interrupt);
    println!("runner is live; press Ctrl-C to stop");
    loop {
        tokio::select! {
            _ = &mut deadline => {
                println!("duration reached");
                break;
            }
            _ = &mut interrupt => {
                println!("interrupt received");
                break;
            }
            event = event_rx.recv() => {
                let Some(event) = event else {
                    state.halted = true;
                    eprintln!("market stream closed");
                    break;
                };
                apply_market_event(&mut state, event);
                if let Err(error) =
                    rebalance(&args, &client, &credentials, &mut state).await
                {
                    state.halted = true;
                    eprintln!("risk halt: {error}");
                    break;
                }
            }
            _ = poll.tick() => {
                if let Err(error) =
                    poll_account(&args, &client, &credentials, &mut state).await
                {
                    state.halted = true;
                    eprintln!("account reconciliation halt: {error}");
                    break;
                }
                if let Err(error) =
                    rebalance(&args, &client, &credentials, &mut state).await
                {
                    state.halted = true;
                    eprintln!("risk halt: {error}");
                    break;
                }
            }
        }
    }
    market_task.abort();
    public_task.abort();

    if args.send_orders && !state.halted {
        if let Err(error) = cancel_working(&args, &client, &credentials, &mut state).await {
            state.halted = true;
            eprintln!("shutdown cancellation failed: {error}");
        }
    }
    if !state.halted {
        refresh_position(&args, &client, &credentials, &mut state).await?;
        reconcile_open_orders(&args, &client, &credentials, &mut state).await?;
    }
    if let Some(path) = args.checkpoint_path.as_deref() {
        let mut checkpoint =
            SessionCheckpoint::new(session_id, args.environment.as_str(), &args.symbol);
        checkpoint.position_ticks = state.position_ticks;
        checkpoint.gross_position_ticks = state.position_ticks.checked_abs().unwrap_or(i64::MAX);
        checkpoint
            .portfolio_positions
            .insert(args.symbol.to_ascii_uppercase(), state.position_ticks);
        checkpoint.working_order_ids = state
            .working
            .as_ref()
            .map(|order| vec![order.client_order_id.clone()])
            .unwrap_or_default();
        checkpoint.risk_stopped = state.halted || state.position_ticks != 0;
        checkpoint
            .write_atomic(path)
            .map_err(|error| format!("checkpoint write failed: {error}"))?;
    }

    println!(
        "{}",
        serde_json::json!({
            "event": "runner_stopped",
            "symbol": args.symbol,
            "halted": state.halted,
            "position_ticks": state.position_ticks,
            "working_order": state.working.as_ref().map(|order| &order.client_order_id),
        })
    );
    if state.halted {
        return Ok(3);
    }
    if state.position_ticks != 0 || state.working.is_some() {
        return Ok(3);
    }
    Ok(0)
}

fn apply_market_event(state: &mut State, event: BinanceMarketEvent) {
    match event {
        BinanceMarketEvent::BookTicker(book) => state.book = Some(book),
        BinanceMarketEvent::MarkPrice(mark) => {
            if let Some(previous) = state.last_mark_price_ticks {
                if previous > 0 && mark.mark_price.0 > 0 {
                    let change_bps = ((i128::from(mark.mark_price.0) - i128::from(previous)).abs()
                        * 10_000
                        / i128::from(previous))
                    .clamp(0, i128::from(i64::MAX)) as i64;
                    state.ewma_abs_return_bps = ewma(state.ewma_abs_return_bps, change_bps);
                }
            }
            state.last_mark_price_ticks = Some(mark.mark_price.0);
            state.mark = Some(mark);
        }
        BinanceMarketEvent::AggTrade(_) => {}
        BinanceMarketEvent::DepthUpdate(_) => {}
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

fn load_credentials(environment: BinanceEnvironment) -> Result<BinanceCredentials, String> {
    match BinanceCredentials::from_environment_for(environment) {
        Ok(credentials) => Ok(credentials),
        Err(environment_error) => PersistentCredentialStore
            .load(environment)
            .map_err(|store_error| {
                format!(
                    "credentials unavailable from environment ({environment_error:?}) or secure store ({store_error:?})"
                )
            })?
            .ok_or_else(|| {
                format!(
                    "credentials unavailable from environment ({environment_error:?}); no secure credential saved for {}",
                    environment.as_str()
                )
            }),
    }
}

async fn poll_account(
    args: &Args,
    client: &BinanceRestClient,
    credentials: &BinanceCredentials,
    state: &mut State,
) -> Result<(), String> {
    if let Some(working) = state.working.clone() {
        let timestamp = server_time(client).await?;
        let response = client
            .query_order(
                credentials,
                &args.symbol,
                &working.client_order_id,
                timestamp,
                args.recv_window_ms,
            )
            .await
            .map_err(|error| error.to_string())?;
        println!(
            "{}",
            serde_json::json!({
                "event": "order_status",
                "client_order_id": response.client_order_id,
                "status": response.status,
                "executed_quantity": response.executed_quantity,
            })
        );
        if is_terminal(&response.status) {
            state.working = None;
        } else {
            update_working(state, &response, &working);
        }
    }
    refresh_position(args, client, credentials, state).await?;
    reconcile_open_orders(args, client, credentials, state).await
}

async fn reconcile_open_orders(
    args: &Args,
    client: &BinanceRestClient,
    credentials: &BinanceCredentials,
    state: &mut State,
) -> Result<(), String> {
    let timestamp = server_time(client).await?;
    let remote = client
        .current_open_orders(
            credentials,
            Some(&args.symbol),
            timestamp,
            args.recv_window_ms,
        )
        .await
        .map_err(|error| error.to_string())?;
    for order in remote {
        let is_active = state
            .working
            .as_ref()
            .is_some_and(|working| working.client_order_id == order.client_order_id);
        if is_active {
            continue;
        }
        if !order.client_order_id.starts_with("anchorbell-") {
            return Err(format!(
                "untracked remote open order {}; refusing to coexist",
                order.client_order_id
            ));
        }
        if !args.send_orders {
            return Err(format!(
                "read-only mode found AnchorBell open order {}; enable orders to reconcile it",
                order.client_order_id
            ));
        }
        let timestamp = server_time(client).await?;
        client
            .cancel_order(
                credentials,
                &args.symbol,
                &order.client_order_id,
                timestamp,
                args.recv_window_ms,
            )
            .await
            .map_err(|error| error.to_string())?;
        println!(
            "{}",
            serde_json::json!({
                "event": "orphan_order_canceled",
                "client_order_id": order.client_order_id,
            })
        );
    }
    Ok(())
}

async fn refresh_position(
    args: &Args,
    client: &BinanceRestClient,
    credentials: &BinanceCredentials,
    state: &mut State,
) -> Result<(), String> {
    let timestamp = server_time(client).await?;
    let risks = client
        .position_risk(
            credentials,
            Some(&args.symbol),
            timestamp,
            args.recv_window_ms,
        )
        .await
        .map_err(|error| error.to_string())?;
    let mut matching = risks.iter().filter(|risk| risk.symbol == args.symbol);
    let first = matching
        .next()
        .ok_or_else(|| "positionRisk returned no requested symbol".to_owned())?;
    let total = parse_decimal_ticks(&first.position_amount, args.quantity_scale)?;
    if matching.next().is_some() {
        return Err("multiple position legs returned; refusing to infer net exposure".into());
    }
    state.position_ticks = total;
    if state.position_ticks.checked_abs().unwrap_or(i64::MAX) > args.max_position {
        return Err(format!(
            "remote position exceeds max_position: {} > {}",
            state.position_ticks.checked_abs().unwrap_or(i64::MAX),
            args.max_position
        ));
    }
    Ok(())
}

async fn rebalance(
    args: &Args,
    client: &BinanceRestClient,
    credentials: &BinanceCredentials,
    state: &mut State,
) -> Result<(), String> {
    let Some(book) = state.book.as_ref() else {
        return Ok(());
    };
    let Some(mark) = state.mark.as_ref() else {
        return Ok(());
    };
    let (bid, ask, bid_quantity, ask_quantity) = (
        book.bid_price.0,
        book.ask_price.0,
        book.bid_quantity.0,
        book.ask_quantity.0,
    );
    let (mark_price, index_price) = (mark.mark_price.0, mark.index_price.0);
    let gap = (i128::from(mark_price) - i128::from(index_price)).abs() * 10_000;
    let market_valid = bid > 0
        && ask >= bid
        && bid_quantity > 0
        && ask_quantity > 0
        && index_price > 0
        && gap <= i128::from(args.max_mark_index_gap_bps) * i128::from(index_price);
    if !market_valid {
        if state.working.is_some() && args.send_orders {
            cancel_working(args, client, credentials, state).await?;
        }
        return Ok(());
    }
    let market_at = mark.event_time_ms.max(book.event_time_ms);
    let Some(mut intent) = adaptive_intent_from_market(
        stable_symbol_id(&args.symbol),
        anchorbell_engine::strategy::PriceTicks(bid),
        bid_quantity,
        anchorbell_engine::strategy::PriceTicks(ask),
        ask_quantity,
        anchorbell_engine::strategy::PriceTicks(args.anchor_ticks),
        mark.index_price,
        mark.mark_price,
        state.position_ticks,
        args.max_position,
        args.requested_quantity,
        state.ewma_abs_return_bps,
        args.entry_threshold_bps,
        4,
        args.max_mark_index_gap_bps,
        now_ms().saturating_sub(market_at),
        5_000,
    ) else {
        state.last_proposal = None;
        if state.working.is_some() && args.send_orders {
            cancel_working(args, client, credentials, state).await?;
        }
        return Ok(());
    };
    intent.quantity = intent.quantity.min(max_order_quantity(
        state.position_ticks,
        intent.side,
        args.max_position,
    ));
    if intent.quantity <= 0 {
        state.last_proposal = None;
        if state.working.is_some() && args.send_orders {
            cancel_working(args, client, credentials, state).await?;
        }
        return Ok(());
    }
    let proposal = (intent.side, intent.price, intent.quantity);
    if !args.send_orders {
        if state.last_proposal != Some(proposal) {
            println!(
                "{}",
                serde_json::json!({
                    "event": "maker_proposal",
                    "symbol": args.symbol,
                    "side": side_name(intent.side),
                    "price_ticks": intent.price,
                    "quantity_ticks": intent.quantity,
                    "post_only": intent.post_only,
                })
            );
            state.last_proposal = Some(proposal);
        }
        return Ok(());
    }
    if state.halted {
        return Ok(());
    }
    if state.working.as_ref().is_some_and(|working| {
        working.side == intent.side
            && working.price_ticks == intent.price
            && working.quantity_ticks == intent.quantity
    }) {
        return Ok(());
    }
    let now = now_ms();
    if now.saturating_sub(state.last_action_ms) < args.min_replace_ms {
        return Ok(());
    }
    if state.working.is_some() {
        cancel_working(args, client, credentials, state).await?;
    }
    let client_order_id = format!("anchorbell-{}-{}", now, state.next_order_id);
    state.next_order_id = state.next_order_id.saturating_add(1);
    let timestamp = server_time(client).await?;
    let response = client
        .place_maker_order(
            credentials,
            BinanceMakerOrderRequest {
                symbol: args.symbol.clone(),
                side: intent.side,
                price: format_ticks(intent.price, args.price_scale),
                quantity: format_ticks(intent.quantity, args.quantity_scale),
                client_order_id: client_order_id.clone(),
                reduce_only: false,
            },
            timestamp,
            args.recv_window_ms,
        )
        .await
        .map_err(|error| error.to_string())?;
    println!(
        "{}",
        serde_json::json!({
            "event": "order_accepted",
            "order_id": response.order_id,
            "client_order_id": response.client_order_id,
            "status": response.status,
            "side": side_name(intent.side),
            "price_ticks": intent.price,
            "quantity_ticks": intent.quantity,
        })
    );
    if is_terminal(&response.status) {
        state.working = None;
    } else {
        state.working = Some(WorkingOrder {
            client_order_id,
            side: intent.side,
            price_ticks: intent.price,
            quantity_ticks: intent.quantity,
        });
    }
    state.last_action_ms = now_ms();
    Ok(())
}

async fn cancel_working(
    args: &Args,
    client: &BinanceRestClient,
    credentials: &BinanceCredentials,
    state: &mut State,
) -> Result<(), String> {
    let Some(working) = state.working.clone() else {
        return Ok(());
    };
    let timestamp = server_time(client).await?;
    let response = client
        .cancel_order(
            credentials,
            &args.symbol,
            &working.client_order_id,
            timestamp,
            args.recv_window_ms,
        )
        .await
        .map_err(|error| error.to_string())?;
    println!(
        "{}",
        serde_json::json!({
            "event": "order_canceled",
            "client_order_id": response.client_order_id,
            "status": response.status,
            "executed_quantity": response.executed_quantity,
        })
    );
    state.working = None;
    state.last_action_ms = now_ms();
    Ok(())
}

fn update_working(state: &mut State, response: &BinanceOrderResponse, previous: &WorkingOrder) {
    state.working = Some(WorkingOrder {
        client_order_id: response.client_order_id.clone(),
        side: previous.side,
        price_ticks: previous.price_ticks,
        quantity_ticks: previous.quantity_ticks,
    });
}

fn is_terminal(status: &str) -> bool {
    matches!(
        status,
        "FILLED" | "CANCELED" | "EXPIRED" | "REJECTED" | "EXPIRED_IN_MATCH"
    )
}

fn max_order_quantity(position: i64, side: Side, max_position: i64) -> i64 {
    let position = i128::from(position);
    let max_position = i128::from(max_position);
    let allowed = match side {
        Side::Buy => max_position - position,
        Side::Sell => max_position + position,
    };
    clamp_i128(allowed.max(0))
}

fn parse_decimal_ticks(value: &str, scale: u32) -> Result<i64, String> {
    let (negative, unsigned) = match value.strip_prefix('-') {
        Some(unsigned) => (true, unsigned),
        None => (false, value),
    };
    let (whole, fraction) = unsigned.split_once('.').unwrap_or((unsigned, ""));
    if whole.is_empty()
        || !whole.bytes().all(|byte| byte.is_ascii_digit())
        || !fraction.bytes().all(|byte| byte.is_ascii_digit())
    {
        return Err(format!("invalid decimal position {value}"));
    }
    if fraction.len() > scale as usize
        && fraction[scale as usize..].bytes().any(|byte| byte != b'0')
    {
        return Err(format!(
            "position precision exceeds quantity scale: {value}"
        ));
    }
    let multiplier = 10_i128
        .checked_pow(scale)
        .ok_or_else(|| "quantity scale is too large".to_owned())?;
    let whole_value = whole
        .parse::<i128>()
        .map_err(|_| format!("position is too large: {value}"))?;
    let mut result = whole_value
        .checked_mul(multiplier)
        .ok_or_else(|| format!("position is too large: {value}"))?;
    let mut fraction_text = fraction.to_owned();
    fraction_text.truncate(scale as usize);
    while fraction_text.len() < scale as usize {
        fraction_text.push('0');
    }
    if !fraction_text.is_empty() {
        result = result
            .checked_add(
                fraction_text
                    .parse::<i128>()
                    .map_err(|_| format!("invalid decimal position {value}"))?,
            )
            .ok_or_else(|| format!("position is too large: {value}"))?;
    }
    let signed = if negative { -result } else { result };
    i64::try_from(signed).map_err(|_| format!("position is too large: {value}"))
}

fn clamp_i128(value: i128) -> i64 {
    value.clamp(i128::from(i64::MIN), i128::from(i64::MAX)) as i64
}

fn format_ticks(value: i64, scale: u32) -> String {
    let divisor = 10_i64.checked_pow(scale).unwrap_or(i64::MAX);
    let absolute = value.unsigned_abs();
    let whole = absolute / divisor as u64;
    let fraction = absolute % divisor as u64;
    if scale == 0 {
        return if value < 0 {
            format!("-{whole}")
        } else {
            whole.to_string()
        };
    }
    let result = format!("{whole}.{fraction:0width$}", width = scale as usize);
    if value < 0 {
        format!("-{result}")
    } else {
        result
    }
}

fn stable_symbol_id(symbol: &str) -> u32 {
    let mut hash = 2_166_136_261_u32;
    for byte in symbol.as_bytes() {
        hash ^= u32::from(*byte);
        hash = hash.wrapping_mul(16_777_619);
    }
    if hash == 0 {
        1
    } else {
        hash
    }
}

fn side_name(side: Side) -> &'static str {
    match side {
        Side::Buy => "BUY",
        Side::Sell => "SELL",
    }
}

async fn server_time(client: &BinanceRestClient) -> Result<u64, String> {
    client
        .server_time_ms()
        .await
        .map_err(|error| error.to_string())
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_millis() as u64)
        .unwrap_or(0)
}

fn validate_args(args: &Args) -> Result<(), String> {
    if args.symbol.is_empty() || !args.symbol.bytes().all(|byte| byte.is_ascii_alphanumeric()) {
        return Err("symbol must be ASCII alphanumeric".into());
    }
    if args.anchor_ticks <= 0
        || args.price_scale > 18
        || args.quantity_scale > 18
        || args.requested_quantity <= 0
        || args.max_position <= 0
        || args.entry_threshold_bps < 0
        || args.max_mark_index_gap_bps < 0
        || args.duration_secs == 0
        || args.poll_ms == 0
    {
        return Err("invalid positive/scale/duration configuration".into());
    }
    Ok(())
}

fn parse_args() -> Result<Args, String> {
    let profile = StrategyProfile::load("config/anchorbell-simulation.json")
        .map_err(|error| format!("cannot load strategy profile: {error}"))?;
    let mut symbol = None;
    let mut anchor_ticks = None;
    let mut price_scale = profile.price_scale;
    let mut quantity_scale = profile.quantity_scale;
    let mut requested_quantity = profile.requested_quantity;
    let mut max_position = profile.max_position;
    let mut entry_threshold_bps = profile.entry_threshold_bps;
    let mut max_mark_index_gap_bps = profile.max_mark_index_gap_bps;
    let mut duration_secs = profile.duration_secs;
    let mut poll_ms = profile.metrics_refresh_ms;
    let mut min_replace_ms = profile.quote_reprice_min_interval_ms;
    let mut recv_window_ms = profile.max_stale_ms;
    let mut environment = BinanceEnvironment::Testnet;
    let mut proxy = env::var("ANCHORBELL_HTTP_PROXY").ok();
    let mut checkpoint_path = None;
    let mut send_orders = false;
    let mut args = env::args().skip(1);
    while let Some(flag) = args.next() {
        match flag.as_str() {
            "--help" | "-h" => {
                print_usage();
                process::exit(0);
            }
            "--symbol" => symbol = Some(next(&mut args, &flag)?.to_ascii_uppercase()),
            "--anchor-ticks" => anchor_ticks = Some(parse(&mut args, &flag)?),
            "--price-scale" => price_scale = parse(&mut args, &flag)?,
            "--quantity-scale" => quantity_scale = parse(&mut args, &flag)?,
            "--quantity" => requested_quantity = parse(&mut args, &flag)?,
            "--max-position" => max_position = parse(&mut args, &flag)?,
            "--entry-threshold-bps" => entry_threshold_bps = parse(&mut args, &flag)?,
            "--max-mark-index-gap-bps" => max_mark_index_gap_bps = parse(&mut args, &flag)?,
            "--duration-secs" => duration_secs = parse(&mut args, &flag)?,
            "--poll-ms" => poll_ms = parse(&mut args, &flag)?,
            "--min-replace-ms" => min_replace_ms = parse(&mut args, &flag)?,
            "--recv-window-ms" => recv_window_ms = parse(&mut args, &flag)?,
            "--environment" => environment = parse(&mut args, &flag)?,
            "--proxy" => proxy = Some(next(&mut args, &flag)?),
            "--checkpoint" => checkpoint_path = Some(PathBuf::from(next(&mut args, &flag)?)),
            "--send-orders" => send_orders = true,
            unknown => return Err(format!("unknown option {unknown}; use --help")),
        }
    }
    Ok(Args {
        symbol: symbol.ok_or("missing --symbol")?,
        anchor_ticks: anchor_ticks.ok_or("missing --anchor-ticks")?,
        price_scale,
        quantity_scale,
        requested_quantity,
        max_position,
        entry_threshold_bps,
        max_mark_index_gap_bps,
        duration_secs,
        poll_ms,
        min_replace_ms,
        recv_window_ms,
        environment,
        proxy,
        checkpoint_path,
        send_orders,
    })
}

fn next(args: &mut impl Iterator<Item = String>, flag: &str) -> Result<String, String> {
    args.next().ok_or_else(|| format!("{flag} needs a value"))
}

fn parse<T>(args: &mut impl Iterator<Item = String>, flag: &str) -> Result<T, String>
where
    T: FromStr,
    T::Err: std::fmt::Debug,
{
    next(args, flag)?
        .parse()
        .map_err(|error| format!("invalid {flag}: {error:?}"))
}

fn print_usage() {
    eprintln!(
        "usage: anchorbell_testnet --anchor-ticks N [options]\n\
         options: --symbol BTCUSDT --environment testnet|production\n\
         --price-scale N --quantity-scale N --quantity N --max-position N\n\
         --entry-threshold-bps N --max-mark-index-gap-bps N\n\
         --duration-secs N --poll-ms N --min-replace-ms N --recv-window-ms N\n\
         --proxy URL --checkpoint PATH --send-orders"
    );
}

fn fail(message: &str) -> ! {
    eprintln!("{message}");
    print_usage();
    process::exit(2);
}
