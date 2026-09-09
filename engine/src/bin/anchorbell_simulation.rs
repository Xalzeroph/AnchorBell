use std::{collections::BTreeMap, env, fs, path::PathBuf, process, str::FromStr, time::Duration};

use anchorbell_engine::{
    execution::BinanceEnvironment,
    platform::RuntimeProfile,
    runtime::{timestamp_ms, RuntimeHealthReporter},
    simulation::{
        allocate_positions, load_anchor_file, load_index_anchor_set, run_simulation,
        BinanceIndexAnchorSet, PositionMode, SimulationConfig, SimulationPolicyVariant,
    },
    strategy::StrategyProfile,
};

#[derive(Debug)]
struct Args {
    anchors: Option<PathBuf>,
    index_anchors: bool,
    symbols: Option<Vec<String>>,
    environment: BinanceEnvironment,
    strategy_variant: SimulationPolicyVariant,
    threshold_scale_ppm: i64,
    records: Option<PathBuf>,
    market_records: Option<PathBuf>,
    anchor_report: Option<PathBuf>,
    fx_records: Option<PathBuf>,
    metrics: Option<PathBuf>,
    metrics_refresh_ms: u64,
    fx_refresh_ms: u64,
    fx_max_age_ms: u64,
    proxy: Option<String>,
    duration_secs: u64,
    index_anchor_refresh_ms: u64,
    price_scale: u32,
    quantity_scale: u32,
    max_subscriptions_per_shard: usize,
    connect_timeout_ms: u64,
    read_timeout_ms: u64,
    entry_threshold_bps: i64,
    max_position: i64,
    requested_quantity: i64,
    capital_usdt: Option<String>,
    capital_cny: Option<String>,
    position_modes: Option<String>,
    max_mark_index_gap_bps: i64,
    max_anchor_age_ms: u64,
    fee_ppm: i64,
    quote_reprice_min_interval_ms: u64,
}

async fn load_index_anchors_with_retry(
    environment: BinanceEnvironment,
    symbols: &[String],
    price_scale: u32,
    proxy: Option<&str>,
) -> Result<BinanceIndexAnchorSet, String> {
    let mut last_error = String::from("unknown index anchor error");
    for attempt in 0..3 {
        match load_index_anchor_set(environment, symbols, price_scale, proxy).await {
            Ok(anchor_set) => return Ok(anchor_set),
            Err(error) => {
                last_error = error.to_string();
                if attempt < 2 {
                    let delay_ms = 500_u64 << attempt;
                    eprintln!(
                        "index anchor request failed (attempt {}/3), retrying in {delay_ms}ms: {last_error}",
                        attempt + 1
                    );
                    tokio::time::sleep(Duration::from_millis(delay_ms)).await;
                }
            }
        }
    }
    Err(last_error)
}

#[tokio::main]
async fn main() {
    let args = match parse_args() {
        Ok(args) => args,
        Err(message) => fail(message),
    };
    let strategy_profile = StrategyProfile::load("config/anchorbell-simulation.json")
        .unwrap_or_else(|error| fail(format!("cannot load strategy profile: {error}")));
    let mut health = RuntimeHealthReporter::new("target/simulation-runtime-audit.jsonl");
    health
        .start(RuntimeProfile::Simulation, timestamp_ms())
        .await
        .unwrap_or_else(|error| fail(format!("simulation health bootstrap failed: {error}")));
    let requested_symbols = args.symbols.clone();
    let mut index_anchor_conversions = None;
    let all_anchors = if args.index_anchors {
        let symbols = requested_symbols
            .as_deref()
            .filter(|symbols| !symbols.is_empty())
            .unwrap_or_else(|| {
                fail("--index-anchors requires an explicit --symbols list");
            });
        let anchor_set = load_index_anchors_with_retry(
            args.environment,
            symbols,
            args.price_scale,
            args.proxy.as_deref(),
        )
        .await
        .unwrap_or_else(|error| fail(format!("cannot load Binance index anchors: {error}")));
        index_anchor_conversions = Some(anchor_set.conversions);
        anchor_set.anchors
    } else {
        let path = args.anchors.as_deref().unwrap_or_else(|| {
            fail("missing --anchors; use --index-anchors for the live Binance index source");
        });
        load_anchor_file(path).unwrap_or_else(|error| {
            fail(format!("cannot load anchors: {error}"));
        })
    };
    let fx_records = args.fx_records.clone().or_else(|| {
        args.index_anchors
            .then(|| PathBuf::from("target\\simulation-index-fx.jsonl"))
    });
    let symbols = requested_symbols.unwrap_or_else(|| all_anchors.keys().cloned().collect());
    let mut anchors = std::collections::BTreeMap::new();
    for symbol in &symbols {
        let Some(anchor) = all_anchors.get(symbol) else {
            fail(format!("no anchor row for subscribed symbol {symbol}"));
        };
        anchors.insert(symbol.clone(), *anchor);
    }
    let capital_input = match (args.capital_usdt.as_deref(), args.capital_cny.as_deref()) {
        (Some(_), Some(_)) => fail("--capital-usdt and --capital-cny are mutually exclusive"),
        (Some(capital), None) => Some((
            "USDT",
            capital,
            parse_scaled_decimal(capital, args.price_scale)
                .unwrap_or_else(|error| fail(format!("invalid --capital-usdt: {error}"))),
        )),
        (None, Some(capital)) => {
            let capital_cny_ticks = parse_scaled_decimal(capital, args.price_scale)
                .unwrap_or_else(|error| fail(format!("invalid --capital-cny: {error}")));
            let cny_per_usdt_ppm = index_anchor_conversions
                .as_ref()
                .and_then(|conversions| {
                    conversions
                        .values()
                        .find(|conversion| conversion.local_currency == "CNY")
                })
                .map(|conversion| conversion.local_per_usdt_ppm)
                .unwrap_or_else(|| fail("--capital-cny requires a live CNY/USDT FX conversion"));
            let capital_usdt_ticks =
                local_capital_to_usdt_ticks(capital_cny_ticks, cny_per_usdt_ppm).unwrap_or_else(
                    |error| fail(format!("cannot convert --capital-cny to USDT: {error}")),
                );
            Some(("CNY", capital, capital_usdt_ticks))
        }
        (None, None) => None,
    };
    let position_allocations = match (capital_input.as_ref(), args.position_modes.as_deref()) {
        (Some((_, _, capital_ticks)), modes) => {
            let modes = parse_position_modes(modes.unwrap_or_default(), &symbols, args.price_scale)
                .unwrap_or_else(|error| fail(format!("invalid --position-modes: {error}")));
            Some(
                allocate_positions(&anchors, *capital_ticks, &modes, args.quantity_scale)
                    .unwrap_or_else(|error| fail(format!("cannot allocate capital: {error}"))),
            )
        }
        (None, Some(_)) => fail("--position-modes requires --capital-usdt or --capital-cny"),
        (None, None) => None,
    };
    let allocation_report = position_allocations.clone();
    let anchor_report = anchors.clone();
    let snapshot_report = serde_json::json!({
        "environment": args.environment.as_str(),
        "strategy_variant": args.strategy_variant.label(),
        "threshold_scale_ppm": args.threshold_scale_ppm,
        "anchor_source": if args.index_anchors {
            "binance_premium_index"
        } else {
            "csv"
        },
        "anchors": anchor_report,
        "index_anchor_conversions": index_anchor_conversions.clone(),
        "fx": {
            "records": fx_records.clone(),
            "refresh_interval_ms": args.fx_refresh_ms,
            "max_stale_ms": args.fx_max_age_ms,
        },
        "symbols": symbols,
        "price_scale": args.price_scale,
        "capital_input_currency": capital_input.as_ref().map(|(currency, _, _)| *currency),
        "capital_input": capital_input.as_ref().map(|(_, value, _)| *value),
        "capital_usdt_ticks": capital_input.as_ref().map(|(_, _, capital_ticks)| *capital_ticks),
        "position_allocations": allocation_report,
    });
    if let Some(path) = args.anchor_report.as_deref() {
        if let Some(parent) = path
            .parent()
            .filter(|parent| !parent.as_os_str().is_empty())
        {
            fs::create_dir_all(parent).unwrap_or_else(|error| {
                fail(format!("cannot create anchor report directory: {error}"))
            });
        }
        let bytes = serde_json::to_vec_pretty(&snapshot_report)
            .expect("anchor snapshot report is serializable");
        fs::write(path, bytes)
            .unwrap_or_else(|error| fail(format!("cannot write anchor report: {error}")));
    }
    let result = match run_simulation(
        SimulationConfig {
            environment: args.environment,
            strategy_variant: args.strategy_variant,
            threshold_scale_ppm: args.threshold_scale_ppm,
            symbols: symbols.clone(),
            price_scale: args.price_scale,
            quantity_scale: args.quantity_scale,
            position_allocations: position_allocations.clone(),
            max_subscriptions_per_shard: args.max_subscriptions_per_shard,
            connect_timeout_ms: args.connect_timeout_ms,
            read_timeout_ms: args.read_timeout_ms,
            duration_secs: args.duration_secs,
            index_anchor_refresh_ms: if args.index_anchors {
                args.index_anchor_refresh_ms
            } else {
                0
            },
            http_proxy: args.proxy,
            market_output_path: args.market_records,
            fx_output_path: fx_records.clone(),
            metrics_output_path: args.metrics.clone(),
            metrics_refresh_ms: args.metrics_refresh_ms,
            fx_refresh_ms: args.fx_refresh_ms,
            fx_max_age_ms: args.fx_max_age_ms,
            quote_reprice_min_interval_ms: args.quote_reprice_min_interval_ms,
            emergency_execution: strategy_profile.emergency_execution,
            fee_schedule_source: strategy_profile.fee_schedule.source.clone(),
        },
        anchors,
        args.entry_threshold_bps,
        args.max_position,
        args.requested_quantity,
        args.max_mark_index_gap_bps,
        args.max_anchor_age_ms,
        args.fee_ppm,
        args.records,
    )
    .await
    {
        Ok(result) => result,
        Err(error) => {
            let reason = error.to_string();
            let _ = health
                .halted("simulation.runtime", timestamp_ms(), &reason)
                .await;
            fail(format!("simulation run failed: {error}"));
        }
    };
    health
        .ready("simulation.runtime", timestamp_ms())
        .await
        .unwrap_or_else(|error| fail(format!("simulation health completion failed: {error}")));
    let report = serde_json::json!({
        "environment": args.environment.as_str(),
        "strategy_variant": args.strategy_variant.label(),
        "threshold_scale_ppm": args.threshold_scale_ppm,
        "anchor_source": if args.index_anchors {
            "binance_premium_index"
        } else {
            "csv"
        },
        "anchors": anchor_report,
        "index_anchor_conversions": index_anchor_conversions,
        "symbols": symbols,
        "duration_secs": args.duration_secs,
        "summary": result.summary,
        "records_written": result.records_written,
        "records_dropped": result.records_dropped,
        "market_records_written": result.market_records_written,
        "market_records_dropped": result.market_records_dropped,
        "fx": {
            "records": fx_records,
            "refresh_interval_ms": args.fx_refresh_ms,
            "max_stale_ms": args.fx_max_age_ms,
            "records_written": result.fx_records_written,
            "records_dropped": result.fx_records_dropped,
            "last_update_at_ms": result.fx_last_update_at_ms,
            "fresh_at_end": result.fx_fresh_at_end,
        },
        "stopped_by_duration": result.stopped_by_duration,
    });
    println!(
        "{}",
        serde_json::to_string_pretty(&report).expect("report is serializable")
    );
}

fn parse_args() -> Result<Args, String> {
    let profile = StrategyProfile::load("config/anchorbell-simulation.json")
        .map_err(|error| format!("cannot load strategy profile: {error}"))?;
    let mut anchors = None;
    let mut index_anchors = false;
    let mut symbols = None;
    let mut environment = profile.environment;
    let mut strategy_variant = parse_strategy_variant(&profile.default_strategy_variant)?;
    let mut threshold_scale_ppm = profile.threshold_scale_ppm;
    let mut records = None;
    let mut market_records = None;
    let mut anchor_report = None;
    let mut fx_records = None;
    let mut metrics = Some(PathBuf::from("target\\simulation-metrics.json"));
    let mut metrics_refresh_ms = profile.metrics_refresh_ms;

    let mut fx_refresh_ms = profile.fx_refresh_ms;
    let mut fx_max_age_ms = profile.fx_max_age_ms;
    let mut proxy = None;
    // Zero means continuous simulation mode; stop only on operator action or a
    // supervised feed failure.
    let mut duration_secs = profile.duration_secs;
    let mut index_anchor_refresh_ms = profile.index_anchor_refresh_ms;
    let mut price_scale = profile.price_scale;
    let mut quantity_scale = profile.quantity_scale;
    let mut max_subscriptions_per_shard = profile.max_subscriptions_per_shard;
    let mut connect_timeout_ms = profile.connect_timeout_ms;
    let mut read_timeout_ms = profile.read_timeout_ms;
    // This is only the adaptive model's hard floor, not a fixed entry
    // threshold. The runtime adds cost, volatility, uncertainty, liquidity,
    // and inventory components.
    let mut entry_threshold_bps = profile.entry_threshold_bps;
    let mut max_position = profile.max_position;
    let mut requested_quantity = profile.requested_quantity;
    let mut capital_usdt = None;
    let mut capital_cny = None;
    let mut position_modes = None;
    let mut max_mark_index_gap_bps = profile.max_mark_index_gap_bps;
    let mut max_anchor_age_ms = profile.max_anchor_age_ms;
    // Binance USDⓈ-M base maker fee: 0.02% = 200 ppm. Override explicitly when needed.
    let mut fee_ppm = profile.fee_schedule.maker_fee_ppm;
    let mut quote_reprice_min_interval_ms = profile.quote_reprice_min_interval_ms;
    let mut args = env::args().skip(1);
    while let Some(flag) = args.next() {
        match flag.as_str() {
            "--help" | "-h" => {
                print_usage();
                process::exit(0);
            }
            "--anchors" => anchors = Some(PathBuf::from(next(&mut args, &flag)?)),
            "--index-anchors" => index_anchors = true,
            "--symbols" => {
                let value = next(&mut args, &flag)?;
                let values = value
                    .split(',')
                    .map(|value| value.trim().to_ascii_uppercase())
                    .filter(|value| !value.is_empty())
                    .collect::<Vec<_>>();
                if values.is_empty() {
                    return Err("--symbols cannot be empty".into());
                }
                symbols = Some(values);
            }
            "--environment" => environment = parse(&mut args, &flag)?,
            "--strategy-variant" => {
                strategy_variant = parse_strategy_variant(&next(&mut args, &flag)?)?
            }
            "--threshold-scale-ppm" => threshold_scale_ppm = parse(&mut args, &flag)?,
            "--records" => records = Some(PathBuf::from(next(&mut args, &flag)?)),
            "--market-records" => market_records = Some(PathBuf::from(next(&mut args, &flag)?)),
            "--anchor-report" => anchor_report = Some(PathBuf::from(next(&mut args, &flag)?)),
            "--fx-records" => fx_records = Some(PathBuf::from(next(&mut args, &flag)?)),
            "--metrics" => metrics = Some(PathBuf::from(next(&mut args, &flag)?)),
            "--metrics-refresh-ms" => metrics_refresh_ms = parse(&mut args, &flag)?,
            "--fx-refresh-ms" => fx_refresh_ms = parse(&mut args, &flag)?,
            "--fx-max-age-ms" => fx_max_age_ms = parse(&mut args, &flag)?,
            "--proxy" => proxy = Some(next(&mut args, &flag)?),
            "--duration-secs" => duration_secs = parse(&mut args, &flag)?,
            "--index-anchor-refresh-ms" => index_anchor_refresh_ms = parse(&mut args, &flag)?,
            "--price-scale" => price_scale = parse(&mut args, &flag)?,
            "--quantity-scale" => quantity_scale = parse(&mut args, &flag)?,
            "--max-subscriptions-per-shard" => {
                max_subscriptions_per_shard = parse(&mut args, &flag)?
            }
            "--connect-timeout-ms" => connect_timeout_ms = parse(&mut args, &flag)?,
            "--read-timeout-ms" => read_timeout_ms = parse(&mut args, &flag)?,
            "--entry-threshold-bps" => entry_threshold_bps = parse(&mut args, &flag)?,
            "--max-position" => max_position = parse(&mut args, &flag)?,
            "--quantity" => requested_quantity = parse(&mut args, &flag)?,
            "--capital-usdt" => capital_usdt = Some(next(&mut args, &flag)?),
            "--capital-cny" => capital_cny = Some(next(&mut args, &flag)?),
            "--position-modes" => position_modes = Some(next(&mut args, &flag)?),
            "--max-mark-index-gap-bps" => max_mark_index_gap_bps = parse(&mut args, &flag)?,
            "--max-anchor-age-ms" => max_anchor_age_ms = parse(&mut args, &flag)?,
            "--maker-fee-ppm" | "--fee-ppm" => fee_ppm = parse(&mut args, &flag)?,
            "--quote-reprice-min-interval-ms" => {
                quote_reprice_min_interval_ms = parse(&mut args, &flag)?
            }
            unknown => return Err(format!("unknown option {unknown}; use --help")),
        }
    }
    Ok(Args {
        anchors,
        index_anchors,
        symbols,
        environment,
        strategy_variant,
        threshold_scale_ppm,
        records,
        market_records,
        anchor_report,
        fx_records,
        metrics,
        metrics_refresh_ms,
        fx_refresh_ms,
        fx_max_age_ms,
        proxy,
        duration_secs,
        index_anchor_refresh_ms,
        price_scale,
        quantity_scale,
        max_subscriptions_per_shard,
        connect_timeout_ms,
        read_timeout_ms,
        entry_threshold_bps,
        max_position,
        requested_quantity,
        capital_usdt,
        capital_cny,
        position_modes,
        max_mark_index_gap_bps,
        max_anchor_age_ms,
        fee_ppm,
        quote_reprice_min_interval_ms,
    })
}

fn parse_strategy_variant(value: &str) -> Result<SimulationPolicyVariant, String> {
    anchorbell_engine::strategy::resolve_strategy_method(value, &[])
        .map_err(|reason| format!("unsupported --strategy-variant {}: {reason}", value.trim()))
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

fn parse_scaled_decimal(value: &str, scale: u32) -> Result<i64, String> {
    if scale > 18 {
        return Err("decimal scale must be at most 18".to_owned());
    }
    let value = value.trim();
    let (negative, value) = match value.strip_prefix('-') {
        Some(value) => (true, value),
        None => (false, value.strip_prefix('+').unwrap_or(value)),
    };
    let mut parts = value.split('.');
    let whole_text = parts.next().unwrap_or_default();
    let fraction_text = parts.next().unwrap_or_default();
    if parts.next().is_some()
        || (whole_text.is_empty() && fraction_text.is_empty())
        || fraction_text.len() > scale as usize
        || !whole_text.bytes().all(|byte| byte.is_ascii_digit())
        || !fraction_text.bytes().all(|byte| byte.is_ascii_digit())
    {
        return Err("expected a non-negative decimal number".to_owned());
    }
    let whole = if whole_text.is_empty() {
        0
    } else {
        whole_text
            .parse::<i128>()
            .map_err(|_| "decimal whole part overflows".to_owned())?
    };
    let fraction = if fraction_text.is_empty() {
        0
    } else {
        fraction_text
            .parse::<i128>()
            .map_err(|_| "decimal fraction overflows".to_owned())?
    };
    let unit = 10_i128.pow(scale);
    let scaled = whole
        .checked_mul(unit)
        .and_then(|value| {
            value.checked_add(fraction * 10_i128.pow(scale - fraction_text.len() as u32))
        })
        .ok_or_else(|| "decimal value overflows".to_owned())?;
    let scaled = if negative { -scaled } else { scaled };
    i64::try_from(scaled).map_err(|_| "decimal value overflows".to_owned())
}

fn local_capital_to_usdt_ticks(
    local_capital_ticks: i64,
    local_per_usdt_ppm: i64,
) -> Result<i64, String> {
    if local_capital_ticks <= 0 || local_per_usdt_ppm <= 0 {
        return Err("capital and FX rate must be positive".to_owned());
    }
    let converted = i128::from(local_capital_ticks)
        .checked_mul(1_000_000)
        .and_then(|value| value.checked_div(i128::from(local_per_usdt_ppm)))
        .ok_or_else(|| "capital conversion overflows".to_owned())?;
    i64::try_from(converted).map_err(|_| "converted USDT capital overflows".to_owned())
}

fn parse_position_modes(
    spec: &str,
    symbols: &[String],
    price_scale: u32,
) -> Result<BTreeMap<String, PositionMode>, String> {
    let mut modes = BTreeMap::new();
    for item in spec
        .split(',')
        .map(str::trim)
        .filter(|item| !item.is_empty())
    {
        let (symbol, mode) = item
            .split_once('=')
            .ok_or_else(|| "use SYMBOL=equal|weight:N|fixed:USDT".to_owned())?;
        let symbol = symbol.trim().to_ascii_uppercase();
        if !symbols.iter().any(|candidate| candidate == &symbol) {
            return Err(format!("unknown symbol {symbol}"));
        }
        if modes.contains_key(&symbol) {
            return Err(format!("duplicate symbol {symbol}"));
        }
        let mode = mode.trim().to_ascii_lowercase();
        let parsed = if mode == "equal" {
            PositionMode::Equal
        } else if let Some(weight) = mode.strip_prefix("weight:") {
            PositionMode::Weight(
                weight
                    .parse::<u64>()
                    .map_err(|_| "weight must be a positive integer".to_owned())?,
            )
        } else if let Some(budget) = mode.strip_prefix("fixed:") {
            PositionMode::FixedUsdt(parse_scaled_decimal(budget, price_scale)?)
        } else {
            return Err(format!("unsupported mode {mode}"));
        };
        modes.insert(symbol, parsed);
    }
    Ok(modes)
}

fn print_usage() {
    eprintln!(
        "usage: anchorbell_simulation (--anchors ANCHORS.csv | --index-anchors --symbols SYMBOLS) [options]\n\
         options: --symbols BTCUSDT,ETHUSDT --environment testnet|production\n\
         --records PATH --market-records PATH --anchor-report PATH --fx-records PATH --metrics PATH --metrics-refresh-ms N --fx-refresh-ms N --fx-max-age-ms N --proxy URL --duration-secs N --index-anchor-refresh-ms N\n\
         --price-scale N --quantity-scale N --max-position N --quantity N\n\
         --capital-usdt N | --capital-cny N --position-modes SYMBOL=equal,SYMBOL=weight:2,SYMBOL=fixed:1000\n\
         --entry-threshold-bps N --max-mark-index-gap-bps N\n\
         --max-anchor-age-ms N --maker-fee-ppm N"
    );
}

fn fail(message: impl std::fmt::Display) -> ! {
    eprintln!("{message}");
    print_usage();
    process::exit(2);
}
