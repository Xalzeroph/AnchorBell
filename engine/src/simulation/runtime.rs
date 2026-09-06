//! Shared simulation-trading and replay execution engine.
//!
//! The simulation path consumes the same Binance bookTicker, markPrice, and
//! aggregate-trade events as the live adapter.  A passive order is filled only
//! when a public aggregate trade is at the order price and its aggressor side
//! is compatible with the order.  No bar-only shortcut is used here.

use std::{
    collections::{BTreeMap, VecDeque},
    fs::File,
    io::{BufRead, BufReader, BufWriter, Write},
    path::{Path, PathBuf},
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use serde::Serialize;
use thiserror::Error;
use tokio::sync::mpsc;

use crate::{
    backtest::{MakerQuote, TopOfBook},
    execution::BinanceEnvironment,
    execution::{OrderIntent, Side},
    market::{
        binance::{AggTrade, BinanceMarketEvent, BookTicker, MarkPrice},
        recorder::{add_event_lineage, market_event_to_json},
        BinanceC2cFxClient, BinanceC2cFxPoller, BinanceMarketConfig, BinanceMarketFeed,
        BinanceMarketStream, FxPollerConfig, FxUpdate, PublicMarketMetadataClient, ReconnectPolicy,
    },
    observability::{DecisionAudit, DecisionGateAudit, DECISION_AUDIT_SCHEMA_VERSION},
    orderbook::LocalOrderBook,
    risk::evaluate_funding_overlay,
    runtime::{
        io::{spawn_line_writer, write_json_atomic, AsyncLineWriter},
        CausalLedger, DataQuality, EventEnvelope, EventSource,
    },
    strategy::{
        calendar::{calendar_for, EquitySessionCalendar},
        capital::{dynamic_weights, CapitalRiskInput},
        decide_m9, decide_maker_exit, profile_for, side_adverse_selection_bps,
        side_adverse_selection_pico_bps,
        universe::instrument_for,
        AdaptiveThreshold, AnchorCurrency, AnchorMakerStrategy, CalibrationSnapshot,
        CalibrationState, DataQualityStatus, DualFlattenPlan, ExitBook, ExitConstraints,
        ExitWorkingOrder, FairValueEstimate, FundingRateKind, FundingSchedule, M9Action, M9Input,
        MakerExitDecision, MakerExitInput, SignalInput, VenueSessionState,
    },
};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub struct AnchorSnapshot {
    pub close_price_ticks: i64,
    pub observed_at_ms: u64,
    pub valid_until_ms: u64,
}

impl AnchorSnapshot {
    pub fn valid_at(self, now_ms: u64, _max_age_ms: u64) -> bool {
        // Anchor lifetime is source- and calendar-defined. A generic short TTL
        // incorrectly invalidates a final equity close over weekends and
        // holidays; valid_until_ms is the authoritative expiry when supplied.
        self.close_price_ticks > 0
            && (self.observed_at_ms == 0 || now_ms >= self.observed_at_ms)
            && (self.valid_until_ms == 0 || now_ms < self.valid_until_ms)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize)]
pub enum SimulationPolicyVariant {
    M0Fixed,
    M1AdaptiveRisk,
    M2Microstructure,
    M3FillAware,
    M4Statistical,
    /// Robust challenger: tail-risk surcharge plus stress-based size/risk gates.
    M5Robust,
    /// M5 signal/risk rules with a separate dynamic capital allocator.
    M6DynamicCapital,
    /// Evidence-gated challenger: M6 plus regime/robust-edge hard gates.
    M7EvidenceGated,
    /// M7 plus funding-aware carry/avoid/tolerate/exit control.
    M8FundingAware,
    /// M8 funding control plus deadline-constrained causal residual DRO-MPC.
    M9DeadlineCausalDroMpc,
}

impl SimulationPolicyVariant {
    pub fn label(self) -> &'static str {
        match self {
            Self::M0Fixed => "m0_fixed",
            Self::M1AdaptiveRisk => "m1_adaptive_risk",
            Self::M2Microstructure => "m2_microstructure",
            Self::M3FillAware => "m3_fill_aware",
            Self::M4Statistical => "m4_statistical",
            Self::M5Robust => "m5_robust",
            Self::M6DynamicCapital => "m6_dynamic_capital",
            Self::M7EvidenceGated => "m7_evidence_gated",
            Self::M8FundingAware => "m8_funding_aware",
            Self::M9DeadlineCausalDroMpc => "m9_deadline_causal_dro_mpc",
        }
    }

    fn uses_microstructure(self) -> bool {
        self >= Self::M2Microstructure
    }

    fn uses_fill_gate(self) -> bool {
        self >= Self::M3FillAware
    }

    fn uses_statistical_term(self) -> bool {
        self >= Self::M4Statistical
    }

    fn uses_tail_guard(self) -> bool {
        self >= Self::M5Robust
    }

    fn uses_dynamic_capital(self) -> bool {
        self >= Self::M6DynamicCapital
    }
}

#[derive(Debug, Clone)]
pub enum PositionMode {
    Equal,
    Weight(u64),
    FixedUsdt(i64),
    /// Runtime allocator mode; direct allocation still requires a risk snapshot.
    Dynamic,
}

impl PositionMode {
    fn label(&self) -> String {
        match self {
            Self::Equal => "equal".to_owned(),
            Self::Weight(weight) => format!("weight:{weight}"),
            Self::FixedUsdt(_) => "fixed_usdt".to_owned(),
            Self::Dynamic => "dynamic".to_owned(),
        }
    }

    fn weight(&self) -> Option<u64> {
        match self {
            Self::Equal => Some(1),
            Self::Weight(weight) => Some(*weight),
            Self::FixedUsdt(_) | Self::Dynamic => None,
        }
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct PositionAllocation {
    pub mode: String,
    pub budget_usdt_ticks: i64,
    pub max_position: i64,
    pub requested_quantity: i64,
}

pub fn allocate_positions(
    anchors: &BTreeMap<String, AnchorSnapshot>,
    total_capital_usdt_ticks: i64,
    modes: &BTreeMap<String, PositionMode>,
    quantity_scale: u32,
) -> Result<BTreeMap<String, PositionAllocation>, SimulationError> {
    if anchors.is_empty() || total_capital_usdt_ticks <= 0 || quantity_scale > 18 {
        return Err(SimulationError::InvalidConfig(
            "capital allocation requires anchors, positive capital, and a valid quantity scale",
        ));
    }
    if modes.keys().any(|symbol| !anchors.contains_key(symbol)) {
        return Err(SimulationError::InvalidConfig(
            "position mode references an unknown symbol",
        ));
    }

    let mut fixed_total = 0_i128;
    let mut variable_weight = 0_u128;
    let mut resolved_modes = BTreeMap::new();
    for symbol in anchors.keys() {
        let mode = modes.get(symbol).cloned().unwrap_or(PositionMode::Equal);
        match &mode {
            PositionMode::FixedUsdt(budget) if *budget > 0 => {
                fixed_total = fixed_total.saturating_add(i128::from(*budget));
            }
            PositionMode::FixedUsdt(_) => {
                return Err(SimulationError::InvalidConfig(
                    "fixed position capital must be positive",
                ));
            }
            PositionMode::Dynamic => {
                return Err(SimulationError::InvalidConfig(
                    "dynamic position mode requires runtime risk observations",
                ));
            }
            PositionMode::Equal | PositionMode::Weight(_) => {
                let weight = mode.weight().unwrap_or(0);
                if weight == 0 {
                    return Err(SimulationError::InvalidConfig(
                        "position mode weight must be positive",
                    ));
                }
                variable_weight = variable_weight.saturating_add(u128::from(weight));
            }
        }
        resolved_modes.insert(symbol.clone(), mode);
    }
    if fixed_total > i128::from(total_capital_usdt_ticks) {
        return Err(SimulationError::InvalidConfig(
            "fixed position capital exceeds total capital",
        ));
    }

    let remaining = i128::from(total_capital_usdt_ticks) - fixed_total;
    let mut variable_left = variable_weight;
    let mut variable_budget_left = remaining;
    let mut allocations = BTreeMap::new();
    for (symbol, mode) in resolved_modes {
        let mode_label = mode.label();
        let budget = match mode {
            PositionMode::FixedUsdt(budget) => i128::from(budget),
            PositionMode::Dynamic => unreachable!("dynamic mode was rejected above"),
            mode => {
                let weight = u128::from(mode.weight().unwrap_or(0));
                let budget = if variable_left == weight {
                    variable_budget_left
                } else {
                    remaining.saturating_mul(i128::try_from(weight).unwrap_or(i128::MAX))
                        / i128::try_from(variable_weight).unwrap_or(i128::MAX)
                };
                variable_left = variable_left.saturating_sub(weight);
                variable_budget_left = variable_budget_left.saturating_sub(budget);
                budget
            }
        };
        let anchor_price = anchors
            .get(&symbol)
            .map(|anchor| anchor.close_price_ticks)
            .filter(|price| *price > 0)
            .ok_or(SimulationError::InvalidConfig(
                "capital allocation requires positive anchor prices",
            ))?;
        let requested_quantity =
            budget.saturating_mul(10_i128.pow(quantity_scale)) / i128::from(anchor_price);
        if requested_quantity <= 0 || requested_quantity > i128::from(i64::MAX) {
            return Err(SimulationError::InvalidConfig(
                "capital allocation is below the minimum quantity or overflows",
            ));
        }
        allocations.insert(
            symbol,
            PositionAllocation {
                mode: mode_label,
                budget_usdt_ticks: i64::try_from(budget).unwrap_or(i64::MAX),
                max_position: i64::try_from(requested_quantity).unwrap_or(i64::MAX),
                requested_quantity: i64::try_from(requested_quantity).unwrap_or(i64::MAX),
            },
        );
    }
    Ok(allocations)
}

#[derive(Debug, Error)]
pub enum SimulationError {
    #[error("invalid simulation configuration: {0}")]
    InvalidConfig(&'static str),
    #[error("invalid anchor row {row}: {reason}")]
    InvalidAnchorRow { row: usize, reason: &'static str },
    #[error("duplicate anchor symbol: {0}")]
    DuplicateAnchor(String),
    #[error("no anchors were loaded")]
    NoAnchors,
    #[error("I/O error: {0}")]
    Io(String),
    #[error("market stream error: {0}")]
    Market(String),
    #[error("event causality validation failed: {0}")]
    Causality(String),
    #[error("JSON error: {0}")]
    Json(String),
    #[error("replay parse failed at line {line}: {error:?}")]
    ReplayParse {
        line: usize,
        error: crate::market::binance::ParseError,
    },
    #[error("replay timestamp moved backwards from {previous_ms} to {current_ms}")]
    ReplayOutOfOrder { previous_ms: u64, current_ms: u64 },
    #[error("replay event symbol is not configured: {0}")]
    ReplaySymbolNotConfigured(String),
}

impl From<std::io::Error> for SimulationError {
    fn from(error: std::io::Error) -> Self {
        Self::Io(error.to_string())
    }
}

impl From<serde_json::Error> for SimulationError {
    fn from(error: serde_json::Error) -> Self {
        Self::Json(error.to_string())
    }
}

pub fn load_anchor_file(path: &Path) -> Result<BTreeMap<String, AnchorSnapshot>, SimulationError> {
    let reader = BufReader::new(File::open(path)?);
    let mut anchors = BTreeMap::new();
    for (index, line) in reader.lines().enumerate() {
        let row = index + 1;
        let line = line?;
        let line = line.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        let fields = line.split(',').map(str::trim).collect::<Vec<_>>();
        if fields
            .first()
            .is_some_and(|field| field.eq_ignore_ascii_case("symbol"))
        {
            continue;
        }
        if fields.len() != 4 {
            return Err(SimulationError::InvalidAnchorRow {
                row,
                reason: "expected symbol,close_price_ticks,observed_at_ms,valid_until_ms",
            });
        }
        let symbol = normalize_symbol(fields[0]).ok_or(SimulationError::InvalidAnchorRow {
            row,
            reason: "symbol must be non-empty ASCII alphanumeric text",
        })?;
        let close_price_ticks =
            fields[1]
                .parse::<i64>()
                .map_err(|_| SimulationError::InvalidAnchorRow {
                    row,
                    reason: "close_price_ticks must be an integer",
                })?;
        let observed_at_ms =
            fields[2]
                .parse::<u64>()
                .map_err(|_| SimulationError::InvalidAnchorRow {
                    row,
                    reason: "observed_at_ms must be an unsigned integer",
                })?;
        let valid_until_ms =
            fields[3]
                .parse::<u64>()
                .map_err(|_| SimulationError::InvalidAnchorRow {
                    row,
                    reason: "valid_until_ms must be an unsigned integer",
                })?;
        if close_price_ticks <= 0
            || (valid_until_ms != 0 && observed_at_ms != 0 && valid_until_ms <= observed_at_ms)
        {
            return Err(SimulationError::InvalidAnchorRow {
                row,
                reason: "anchor price must be positive and validity must be ordered",
            });
        }
        if anchors
            .insert(
                symbol.clone(),
                AnchorSnapshot {
                    close_price_ticks,
                    observed_at_ms,
                    valid_until_ms,
                },
            )
            .is_some()
        {
            return Err(SimulationError::DuplicateAnchor(symbol));
        }
    }
    if anchors.is_empty() {
        return Err(SimulationError::NoAnchors);
    }
    Ok(anchors)
}

/// Fetches the official Binance TradFi index price for every selected symbol
/// and materializes a run-local static anchor. No credentials or order API are
/// involved. The caller controls the run lifetime; the anchor itself has no
/// file-backed expiry and cannot silently survive a process restart.
///
/// A Binance index anchor stays in USDT for strategy and execution math; the
/// local equivalent is recorded separately after multiplying by the live
/// local-currency-per-USDT FX midpoint.
#[derive(Debug, Clone, Serialize)]
pub struct IndexAnchorConversion {
    pub index_price_usdt_ticks: i64,
    pub local_currency: String,
    pub index_price_local_ticks: i64,
    pub local_per_usdt_ppm: i64,
    pub fx_buy_local_per_usdt_ppm: i64,
    pub fx_sell_local_per_usdt_ppm: i64,
    pub fx_observed_at_ms: u64,
    pub fx_source: String,
    pub index_observed_at_ms: u64,
}

#[derive(Debug, Clone)]
pub struct BinanceIndexAnchorSet {
    pub anchors: BTreeMap<String, AnchorSnapshot>,
    pub conversions: BTreeMap<String, IndexAnchorConversion>,
}

pub(crate) async fn load_index_anchor_set_internal(
    environment: BinanceEnvironment,
    symbols: &[String],
    price_scale: u32,
    http_proxy: Option<&str>,
) -> Result<BinanceIndexAnchorSet, SimulationError> {
    if symbols.is_empty() {
        return Err(SimulationError::InvalidConfig(
            "index anchors require at least one symbol",
        ));
    }
    let mut seen = BTreeMap::new();
    let mut selected_metadata = Vec::with_capacity(symbols.len());
    let client = PublicMarketMetadataClient::new(environment.endpoints().rest_base, http_proxy)
        .map_err(|error| SimulationError::Market(format!("index anchor client: {error}")))?;
    let exchange_info = client
        .exchange_info()
        .await
        .map_err(|error| SimulationError::Market(format!("index anchor exchangeInfo: {error}")))?;

    for symbol in symbols {
        let normalized = normalize_symbol(symbol).ok_or(SimulationError::InvalidConfig(
            "index anchor symbol must be non-empty ASCII alphanumeric text",
        ))?;
        if instrument_for(&normalized).is_none() {
            return Err(SimulationError::InvalidConfig(
                "index anchor symbols must be selected TradFi instruments",
            ));
        }
        if seen.insert(normalized.clone(), ()).is_some() {
            return Err(SimulationError::DuplicateAnchor(normalized));
        }
        let metadata = exchange_info
            .iter()
            .find(|metadata| metadata.symbol == normalized)
            .cloned()
            .ok_or_else(|| {
                SimulationError::Market(format!(
                    "Binance exchangeInfo has no selected symbol {normalized}"
                ))
            })?;
        if !metadata.is_trading_tradifi_perpetual() {
            return Err(SimulationError::Market(format!(
                "selected symbol {normalized} is not a trading TradFi perpetual"
            )));
        }
        selected_metadata.push(metadata);
    }

    // Two in-flight snapshots preserve a 250 ms process-wide REST cadence
    // while overlapping network latency; the gate, not task count, controls
    // request pressure.
    let snapshot_symbols = selected_metadata
        .iter()
        .map(|metadata| metadata.symbol.clone())
        .collect::<Vec<_>>();
    let snapshots = client.premium_index_snapshots(&snapshot_symbols, 2).await;
    let observed_now_ms = now_ms();
    let mut fx_quotes = BTreeMap::new();
    let fx_client = BinanceC2cFxClient::new(http_proxy)
        .map_err(|error| SimulationError::Market(format!("index anchor FX client: {error}")))?;
    let needs_cny = selected_metadata.iter().any(|metadata| {
        profile_for(&metadata.symbol)
            .is_some_and(|profile| profile.anchor_currency == AnchorCurrency::Cny)
    });
    let needs_hkd = selected_metadata.iter().any(|metadata| {
        profile_for(&metadata.symbol)
            .is_some_and(|profile| profile.anchor_currency == AnchorCurrency::Hkd)
    });
    if needs_cny {
        let quote = fx_client
            .midpoint(AnchorCurrency::Cny)
            .await
            .map_err(|error| {
                SimulationError::Market(format!("index anchor CNY/USDT FX: {error}"))
            })?;
        fx_quotes.insert(AnchorCurrency::Cny.as_str().to_owned(), quote);
    }
    if needs_hkd {
        let quote = fx_client
            .midpoint(AnchorCurrency::Hkd)
            .await
            .map_err(|error| {
                SimulationError::Market(format!("index anchor HKD/USDT FX: {error}"))
            })?;
        fx_quotes.insert(AnchorCurrency::Hkd.as_str().to_owned(), quote);
    }

    let mut anchors = BTreeMap::new();
    let mut conversions = BTreeMap::new();
    for timed_snapshot in snapshots {
        let timed_snapshot = timed_snapshot.map_err(|error| {
            SimulationError::Market(format!("index anchor premium snapshot: {error}"))
        })?;
        timed_snapshot
            .snapshot
            .validate_for_anchor_price(timed_snapshot.observed_at_ms, observed_now_ms)
            .map_err(|error| {
                SimulationError::Market(format!("index anchor validation: {error}"))
            })?;
        let snapshot = timed_snapshot.snapshot;
        let symbol = snapshot.symbol.clone();
        let profile = profile_for(&symbol).ok_or_else(|| {
            SimulationError::Market(format!("no anchor currency profile for {symbol}"))
        })?;
        let fx_quote = fx_quotes
            .get(profile.anchor_currency.as_str())
            .ok_or_else(|| {
                SimulationError::Market(format!(
                    "missing {}/USDT FX quote for {symbol}",
                    profile.anchor_currency.as_str()
                ))
            })?;
        let index_price =
            crate::market::binance::parse_price_ticks(&snapshot.index_price, price_scale).map_err(
                |error| {
                    SimulationError::Market(format!(
                        "index anchor price for {symbol} is invalid: {error:?}"
                    ))
                },
            )?;
        if index_price.0 <= 0 {
            return Err(SimulationError::Market(format!(
                "index anchor price for {symbol} is not positive"
            )));
        }
        let local_price = fx_quote
            .convert_usdt_ticks_to_local(index_price.0)
            .ok_or_else(|| {
                SimulationError::Market(format!(
                    "local FX conversion overflow for {symbol} at {}",
                    profile.anchor_currency.as_str()
                ))
            })?;
        anchors.insert(
            symbol.clone(),
            AnchorSnapshot {
                close_price_ticks: index_price.0,
                observed_at_ms: timed_snapshot.observed_at_ms,
                // A static anchor remains valid until the authority publishes a
                // replacement or the calendar/session layer invalidates it.
                valid_until_ms: 0,
            },
        );
        conversions.insert(
            symbol,
            IndexAnchorConversion {
                index_price_usdt_ticks: index_price.0,
                local_currency: profile.anchor_currency.as_str().to_owned(),
                index_price_local_ticks: local_price,
                local_per_usdt_ppm: fx_quote.midpoint_local_per_usdt_ppm,
                fx_buy_local_per_usdt_ppm: fx_quote.buy_local_per_usdt_ppm,
                fx_sell_local_per_usdt_ppm: fx_quote.sell_local_per_usdt_ppm,
                fx_observed_at_ms: fx_quote.observed_at_ms,
                fx_source: fx_quote.source.to_owned(),
                index_observed_at_ms: timed_snapshot.observed_at_ms,
            },
        );
    }
    if anchors.len() != seen.len() || conversions.len() != seen.len() {
        return Err(SimulationError::Market(
            "Binance returned an incomplete index-anchor set".to_owned(),
        ));
    }
    Ok(BinanceIndexAnchorSet {
        anchors,
        conversions,
    })
}

pub async fn load_binance_index_anchors(
    environment: BinanceEnvironment,
    symbols: &[String],
    price_scale: u32,
    http_proxy: Option<&str>,
) -> Result<BTreeMap<String, AnchorSnapshot>, SimulationError> {
    Ok(
        load_index_anchor_set_internal(environment, symbols, price_scale, http_proxy)
            .await?
            .anchors,
    )
}

fn normalize_symbol(value: &str) -> Option<String> {
    let symbol = value.trim().to_ascii_uppercase();
    (!symbol.is_empty() && symbol.bytes().all(|byte| byte.is_ascii_alphanumeric()))
        .then_some(symbol)
}

#[derive(Debug, Clone, Copy)]
struct BookState {
    bid_price_ticks: i64,
    bid_quantity: i64,
    ask_price_ticks: i64,
    ask_quantity: i64,
}

#[derive(Debug, Clone, Copy)]
struct PendingMarkout {
    side: Side,
    fill_price_ticks: i64,
    due_at_ms: u64,
}

#[derive(Debug, Clone, Copy)]
struct WorkingOrder {
    client_id: u64,
    /// Stable decision that authorized this order; propagated to fills/cancels.
    decision_id: Option<u64>,
    side: Side,
    price_ticks: i64,
    remaining_quantity: i64,
    reduce_only: bool,
    /// Quantity resting ahead of this order when it reached the exchange.
    queue_ahead_quantity: i64,
    /// Absolute distance from the contemporaneous mid, in basis points.
    quote_distance_bps: i64,
    placed_at_ms: u64,
    /// Wall-clock/simulation time at which the exchange can first accept the order.
    exchange_arrival_at_ms: u64,
    cancel_requested_at_ms: Option<u64>,
}

#[derive(Debug, Clone)]
struct SimulationSymbolState {
    symbol_id: u32,
    calibration: CalibrationState,
    anchor: AnchorSnapshot,
    book: Option<BookState>,
    local_book: LocalOrderBook,
    last_book_update_id: Option<u64>,
    last_book_event_at_ms: u64,
    mark_price_ticks: Option<i64>,
    index_price_ticks: Option<i64>,
    next_funding_time_ms: u64,
    /// Exchange event time used for ordering, funding, and signal-age semantics.
    last_mark_time_ms: u64,
    /// Local receipt time used exclusively for transport/data-freshness gating.
    last_mark_received_at_ms: u64,
    last_trade_id: Option<u64>,
    last_mark_price_ticks: Option<i64>,
    ewma_abs_return_bps: i64,
    ewma_spread_bps: i64,
    ewma_abs_return_micro_bps: i64,
    ewma_spread_micro_bps: i64,
    ewma_abs_return_pico_bps: i64,
    ewma_spread_pico_bps: i64,
    near_miss_count: u64,
    adaptive_relief_bps: i64,
    adaptive_relief_micro_bps: i64,
    adaptive_relief_pico_bps: i64,
    working: Option<WorkingOrder>,
    position: i64,
    average_entry_ticks: i64,
    realized_pnl_ticks: i64,
    market_pnl_ticks: i64,
    strategy_pnl_ticks: i64,
    funding_pnl_ticks: i64,
    fees_ticks: i64,
    latest_funding_rate_e8: Option<i64>,
    last_settled_funding_time_ms: u64,
    fills: u64,
    winning_fills: u64,
    losing_fills: u64,
    pending_markouts: VecDeque<PendingMarkout>,
    ewma_adverse_markout_micro_bps: i64,
    ewma_adverse_markout_pico_bps: i64,
    evaluated_markouts: u64,
    adverse_markouts: u64,
}

#[derive(Debug, Clone, Serialize)]
pub struct SimulationRecord {
    pub timestamp_ms: u64,
    pub exchange_event_time_ms: u64,
    pub received_at_ms: u64,
    pub decision_id: Option<u64>,
    /// Self-describing ledger tag for multi-strategy multi-policy executions.
    pub strategy_variant: String,
    pub kind: String,
    pub symbol: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub client_id: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub side: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub price_ticks: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub quantity: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub order_age_ms: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub queue_ahead_quantity: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub quote_distance_bps: Option<i64>,
    pub position: i64,
    pub realized_pnl_ticks: i64,
    pub market_pnl_ticks: i64,
    pub strategy_pnl_ticks: i64,
    pub funding_pnl_ticks: i64,
    pub fees_ticks: i64,
    pub net_pnl_ticks: i64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub detail: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub decision_audit: Option<DecisionAudit>,
}

struct RecordFields<'a> {
    kind: &'a str,
    decision_id: Option<u64>,
    client_id: Option<u64>,
    side: Option<Side>,
    price_ticks: Option<i64>,
    quantity: Option<i64>,
    order_age_ms: Option<u64>,
    queue_ahead_quantity: Option<i64>,
    quote_distance_bps: Option<i64>,
    detail: Option<&'a str>,
}

#[derive(Debug, Clone, Serialize)]
pub struct SimulationSummary {
    pub event_count: u64,
    pub order_count: u64,
    pub fill_count: u64,
    pub filled_quantity: i64,
    pub rejected_entries: u64,
    /// Rejections partitioned by the owning layer (strategy/risk/execution).
    pub gate_rejections: BTreeMap<String, u64>,
    pub realized_pnl_ticks: i64,
    pub unrealized_pnl_ticks: i64,
    pub market_pnl_ticks: i64,
    pub strategy_pnl_ticks: i64,
    pub funding_pnl_ticks: i64,
    pub gross_pnl_ticks: i64,
    pub fees_ticks: i64,
    pub net_pnl_ticks: i64,
    pub maker_fee_ppm: i64,
    pub unrealized_valuation_complete: bool,
    pub current_absolute_position: i64,
    pub peak_absolute_position: i64,
    pub working_orders: u64,
    pub flat_at_end: bool,
}

#[derive(Debug, Clone, Serialize)]
pub struct FinalSettlement {
    pub records: Vec<SimulationRecord>,
    pub summary: SimulationSummary,
    pub flatten_requested: bool,
    pub settlement_status: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub enum ThresholdStatus {
    Ready,
    WarmingUp,
    InsufficientData,
    InvalidInput,
    ModelFailure,
}

impl ThresholdStatus {
    fn label(self) -> &'static str {
        match self {
            Self::Ready => "ready",
            Self::WarmingUp => "warming_up",
            Self::InsufficientData => "insufficient_data",
            Self::InvalidInput => "invalid_input",
            Self::ModelFailure => "model_failure",
        }
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct ThresholdMetrics {
    pub floor_bps: i64,
    pub residual_volatility_bps: i64,
    pub cost_bps: i64,
    pub uncertainty_bps: i64,
    pub deadline_risk_bps: i64,
    pub safety_margin_bps: i64,
    pub spread_bps: i64,
    pub adverse_selection_bps: i64,
    pub liquidity_bps: i64,
    pub inventory_bps: i64,
    pub statistical_bps: i64,
    pub tail_risk_bps: i64,
    pub floor_pico_bps: i64,
    pub residual_volatility_pico_bps: i64,
    pub cost_pico_bps: i64,
    pub uncertainty_pico_bps: i64,
    pub deadline_risk_pico_bps: i64,
    pub safety_margin_pico_bps: i64,
    pub spread_pico_bps: i64,
    pub adverse_selection_pico_bps: i64,
    pub liquidity_pico_bps: i64,
    pub inventory_pico_bps: i64,
    pub statistical_pico_bps: i64,
    pub tail_risk_pico_bps: i64,
    pub required_bps: Option<i64>,
    /// Exact internal hurdle, in 1e-12 bps.
    pub required_pico_bps: Option<i64>,
    /// Compatibility diagnostic, rounded from pico-bps.
    pub required_micro_bps: Option<i64>,
}

#[derive(Debug, Clone, Copy)]
struct ThresholdDiagnostic {
    status: ThresholdStatus,
    threshold: Option<AdaptiveThreshold>,
    prior_used: bool,
    missing_component: Option<&'static str>,
}

const FUNDING_FLATTEN_LEAD_MS: u64 = 5 * 60 * 1_000;
const MICRO_BPS_SCALE: i64 = 1_000_000;
const PICO_BPS_SCALE: i64 = 1_000_000_000_000;
const EWMA_PREVIOUS_WEIGHT_PPM: i64 = 700_000;
const EWMA_SAMPLE_WEIGHT_PPM: i64 = 300_000;
const ADAPTIVE_RELIEF_MAX_PICO_BPS: i64 = 20 * PICO_BPS_SCALE;
const ADAPTIVE_RELIEF_STEP_PICO_BPS: i64 = PICO_BPS_SCALE / 4;
const ADAPTIVE_NEAR_MISS_WINDOW_PICO_BPS: i64 = 20 * PICO_BPS_SCALE;
const MARKOUT_HORIZON_MS: u64 = 30 * 1_000;
const THRESHOLD_PRIOR_VOLATILITY_PICO_BPS: i64 = 10 * PICO_BPS_SCALE;
const THRESHOLD_PRIOR_SPREAD_PICO_BPS: i64 = 2 * PICO_BPS_SCALE;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SimulationRiskState {
    Trading,
    ReduceOnlyEquitySession,
    /// Conservative M1-M7 funding-deadline gate.
    ReduceOnlyFundingDeadline,
    /// M8 only: economic funding cost justifies reducing a held position.
    ReduceOnlyFundingRisk,
    /// M8 only: no new risk, but do not manufacture a flatten order.
    NoEntryFunding,
    ReduceOnlyTailRisk,
    HaltFundingMetadata,
    HaltMarketData,
    HaltAnchor,
}

impl SimulationRiskState {
    fn label(self) -> &'static str {
        match self {
            Self::Trading => "trading",
            Self::ReduceOnlyEquitySession => "reduce_only_equity_session",
            Self::ReduceOnlyFundingDeadline => "reduce_only_funding_deadline",
            Self::ReduceOnlyFundingRisk => "reduce_only_funding_risk",
            Self::NoEntryFunding => "no_entry_funding",
            Self::ReduceOnlyTailRisk => "reduce_only_tail_risk",
            Self::HaltFundingMetadata => "halt_funding_metadata",
            Self::HaltMarketData => "halt_market_data",
            Self::HaltAnchor => "halt_anchor",
        }
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct SymbolMetrics {
    pub symbol: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub position_mode: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub allocated_capital_usdt_ticks: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub allocated_capital_usdt: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub target_quantity: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub target_quantity_units: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub position_notional_usdt_ticks: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub position_notional_usdt: Option<String>,
    pub position: i64,
    pub fills: u64,
    pub winning_fills: u64,
    pub losing_fills: u64,
    pub realized_pnl_ticks: i64,
    pub unrealized_pnl_ticks: i64,
    pub market_pnl_ticks: i64,
    pub strategy_pnl_ticks: i64,
    pub funding_pnl_ticks: i64,
    pub fees_ticks: i64,
    pub net_pnl_ticks: i64,
    pub risk_metrics: Option<RiskMetrics>,
    pub anchor_age_ms: Option<u64>,
    pub anchor_final_close: bool,
    pub calendar_state: String,
    pub next_funding_time_ms: u64,
    pub latest_funding_rate_e8: Option<i64>,
    pub funding_flatten_deadline_ms: Option<u64>,
    pub funding_action: String,
    pub funding_carry_bps: i64,
    pub funding_net_edge_bps: i64,
    pub risk_state: String,
    /// Human-readable reason the current symbol is not entering.
    pub entry_block_reason: String,
    pub data_quality: DataQualityStatus,
    pub mark_age_ms: Option<u64>,
    pub bid_price_ticks: Option<i64>,
    pub ask_price_ticks: Option<i64>,
    pub anchor_price_ticks: i64,
    pub mark_price_ticks: Option<i64>,
    pub index_price_ticks: Option<i64>,
    pub ewma_abs_return_bps: i64,
    pub ewma_spread_bps: i64,
    pub ewma_abs_return_micro_bps: i64,
    pub ewma_spread_micro_bps: i64,
    pub ewma_abs_return_pico_bps: i64,
    pub ewma_spread_pico_bps: i64,
    pub ewma_adverse_markout_bps: i64,
    pub ewma_adverse_markout_micro_bps: i64,
    pub ewma_adverse_markout_pico_bps: i64,
    pub evaluated_markouts: u64,
    pub adverse_markouts: u64,
    pub adaptive_relief_bps: i64,
    pub adaptive_relief_micro_bps: i64,
    /// Exact adaptive relief, in 1e-12 bps.
    pub adaptive_relief_pico_bps: i64,
    pub buy_edge_bps: Option<i64>,
    pub sell_edge_bps: Option<i64>,
    /// Compatibility edge diagnostics in micro-bps.
    pub buy_edge_micro_bps: Option<i64>,
    pub sell_edge_micro_bps: Option<i64>,
    /// Exact price edge diagnostics in pico-bps.
    pub buy_edge_pico_bps: Option<i64>,
    pub sell_edge_pico_bps: Option<i64>,
    pub liquidity_ratio_bps: Option<i64>,
    pub liquidity_penalty_bps: Option<i64>,
    pub liquidity_fill_probability_bps: Option<u16>,
    pub fair_value_ticks: Option<i64>,
    pub fair_value_confidence_bps: Option<i64>,
    pub market_regime: Option<String>,
    pub threshold_status: String,
    pub threshold_prior_used: bool,
    pub threshold_missing_component: Option<String>,
    pub m9_calibration: CalibrationSnapshot,
    pub threshold: Option<ThresholdMetrics>,
}

#[derive(Debug, Clone, Serialize)]
pub struct SymbolPerformancePoint {
    pub symbol: String,
    pub position: i64,
    pub market_pnl_ticks: i64,
    pub strategy_pnl_ticks: i64,
    pub funding_pnl_ticks: i64,
    pub fees_ticks: i64,
    pub net_pnl_ticks: i64,
}

#[derive(Debug, Clone, Serialize)]
pub struct PerformancePoint {
    pub observed_at_ms: u64,
    pub market_pnl_ticks: i64,
    pub strategy_pnl_ticks: i64,
    pub funding_pnl_ticks: i64,
    pub fees_ticks: i64,
    pub gross_pnl_ticks: i64,
    pub net_pnl_ticks: i64,
    pub current_absolute_position: i64,
    pub symbols: Vec<SymbolPerformancePoint>,
}

#[derive(Debug, Clone, Serialize)]
pub struct ModelAssumptions {
    pub fill_model: String,
    pub queue_ahead: i64,
    pub trade_through: i64,
    pub market_to_decision_ms: u64,
    pub decision_to_exchange_ms: u64,
    pub cancel_to_exchange_ms: u64,
}

#[derive(Debug, Clone, Serialize)]
pub struct RiskMetrics {
    pub status: String,
    pub sample_count: usize,
    pub observed_seconds: f64,
    pub total_return_pct: f64,
    pub max_drawdown_pct: f64,
    pub win_rate_pct: f64,
    pub average_return_bps: f64,
    pub profit_factor: Option<f64>,
    /// Annualized Sharpe; null until enough independent history exists.
    pub sharpe_ratio: Option<f64>,
    /// Annualized Sortino; null until enough independent history exists.
    pub sortino_ratio: Option<f64>,
}

#[derive(Debug, Clone, Serialize)]
pub struct MetricsSnapshot {
    pub observed_at_ms: u64,
    pub strategy_variant: String,
    pub last_market_event_at_ms: u64,
    pub last_received_at_ms: u64,
    pub summary: SimulationSummary,
    pub symbols: Vec<SymbolMetrics>,
    pub history: Vec<PerformancePoint>,
    pub risk_metrics: Option<RiskMetrics>,
    pub calendar_snapshot: String,
    pub maker_fee_source: String,
    pub funding_model: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub capital_usdt_ticks: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub capital_usdt: Option<String>,
    pub model_assumptions: ModelAssumptions,
}

#[derive(Debug, Clone)]
pub struct SimulationEngine {
    strategy: AnchorMakerStrategy,
    strategy_variant: SimulationPolicyVariant,
    max_position: i64,
    requested_quantity: i64,
    max_mark_index_gap_bps: i64,
    max_anchor_age_ms: u64,
    fee_ppm: i64,
    price_scale: u32,
    quantity_scale: u32,
    realism: crate::backtest::realism::RealisticFillModel,
    quote_reprice_min_interval_ms: u64,
    live_risk_gates: bool,
    threshold_scale_ppm: i64,
    position_allocations: BTreeMap<String, PositionAllocation>,
    capital_usdt_ticks: Option<i64>,
    states: BTreeMap<String, SimulationSymbolState>,
    next_client_id: u64,
    next_decision_id: u64,
    event_count: u64,
    order_count: u64,
    fill_count: u64,
    filled_quantity: i64,
    rejected_entries: u64,
    gate_rejections: BTreeMap<String, u64>,
    peak_absolute_position: i64,
    last_event_at_ms: u64,
    last_received_at_ms: u64,
    dynamic_capital_refresh_ms: u64,
    last_dynamic_capital_update_ms: u64,
    causal_ledger: CausalLedger,
}

impl SimulationEngine {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        anchors: BTreeMap<String, AnchorSnapshot>,
        entry_threshold_bps: i64,
        max_position: i64,
        requested_quantity: i64,
        max_mark_index_gap_bps: i64,
        max_anchor_age_ms: u64,
        fee_ppm: i64,
        quantity_scale: u32,
    ) -> Result<Self, SimulationError> {
        if anchors.is_empty()
            || entry_threshold_bps < 0
            || max_position <= 0
            || requested_quantity <= 0
            || max_mark_index_gap_bps < 0
            || fee_ppm < 0
            || quantity_scale > 18
        {
            return Err(SimulationError::InvalidConfig(
                "anchors, position, quantity, thresholds, and fee must be valid",
            ));
        }
        let states: BTreeMap<String, SimulationSymbolState> = anchors
            .into_iter()
            .map(|(symbol, anchor)| {
                (
                    symbol.clone(),
                    SimulationSymbolState {
                        symbol_id: stable_symbol_id(&symbol),
                        calibration: CalibrationState::new(symbol.clone()),
                        anchor,
                        book: None,
                        local_book: LocalOrderBook::default(),
                        last_book_update_id: None,
                        last_book_event_at_ms: 0,
                        mark_price_ticks: None,
                        index_price_ticks: None,
                        next_funding_time_ms: 0,
                        last_mark_time_ms: 0,
                        last_mark_received_at_ms: 0,
                        last_trade_id: None,
                        last_mark_price_ticks: None,
                        ewma_abs_return_bps: 0,
                        ewma_spread_bps: 0,
                        ewma_abs_return_micro_bps: 0,
                        ewma_spread_micro_bps: 0,
                        ewma_abs_return_pico_bps: 0,
                        ewma_spread_pico_bps: 0,
                        ewma_adverse_markout_micro_bps: 0,
                        ewma_adverse_markout_pico_bps: 0,
                        evaluated_markouts: 0,
                        adverse_markouts: 0,
                        pending_markouts: VecDeque::new(),
                        near_miss_count: 0,
                        adaptive_relief_bps: 0,
                        adaptive_relief_micro_bps: 0,
                        adaptive_relief_pico_bps: 0,
                        working: None,
                        position: 0,
                        average_entry_ticks: 0,
                        realized_pnl_ticks: 0,
                        market_pnl_ticks: 0,
                        strategy_pnl_ticks: 0,
                        funding_pnl_ticks: 0,
                        fees_ticks: 0,
                        latest_funding_rate_e8: None,
                        last_settled_funding_time_ms: 0,
                        fills: 0,
                        winning_fills: 0,
                        losing_fills: 0,
                    },
                )
            })
            .collect();
        let position_allocations = states
            .keys()
            .map(|symbol| {
                (
                    symbol.clone(),
                    PositionAllocation {
                        mode: "allocation_default".to_owned(),
                        budget_usdt_ticks: 0,
                        max_position,
                        requested_quantity,
                    },
                )
            })
            .collect();
        Ok(Self {
            strategy: AnchorMakerStrategy::new(entry_threshold_bps, 0),
            strategy_variant: SimulationPolicyVariant::M4Statistical,
            max_position,
            requested_quantity,
            max_mark_index_gap_bps,
            max_anchor_age_ms,
            fee_ppm,
            price_scale: 8,
            quantity_scale,
            realism: crate::backtest::realism::RealisticFillModel::default(),
            quote_reprice_min_interval_ms: 0,
            live_risk_gates: false,
            threshold_scale_ppm: 1_000_000,
            position_allocations,
            capital_usdt_ticks: None,
            states,
            next_client_id: 1,
            next_decision_id: 1,
            event_count: 0,
            order_count: 0,
            fill_count: 0,
            filled_quantity: 0,
            rejected_entries: 0,
            gate_rejections: BTreeMap::new(),
            peak_absolute_position: 0,
            last_event_at_ms: 0,
            last_received_at_ms: 0,
            dynamic_capital_refresh_ms: 60_000,
            last_dynamic_capital_update_ms: 0,
            causal_ledger: CausalLedger::default(),
        })
    }

    pub fn with_realism(mut self, realism: crate::backtest::realism::RealisticFillModel) -> Self {
        self.realism = realism;
        self
    }

    pub fn with_live_risk_gates(mut self) -> Self {
        self.live_risk_gates = true;
        self
    }

    /// Hold a same-side quote briefly before replacing it. This models the
    /// operational cost of cancel/replace churn and leaves urgent reduce-only
    /// actions unrestricted.
    pub fn with_quote_reprice_min_interval_ms(mut self, interval_ms: u64) -> Self {
        self.quote_reprice_min_interval_ms = interval_ms.min(10_000);
        self
    }

    pub fn with_threshold_scale_ppm(mut self, scale_ppm: i64) -> Self {
        self.threshold_scale_ppm = scale_ppm.clamp(0, 1_000_000);
        self
    }

    pub fn with_strategy_variant(mut self, variant: SimulationPolicyVariant) -> Self {
        self.strategy_variant = variant;
        self
    }

    /// Recompute M6 target weights no more often than this interval. A zero
    /// interval is clamped to one second to prevent event-driven churn.
    pub fn with_dynamic_capital_refresh_ms(mut self, refresh_ms: u64) -> Self {
        self.dynamic_capital_refresh_ms = refresh_ms.max(1_000);
        self
    }

    pub fn with_price_scale(mut self, price_scale: u32) -> Self {
        self.price_scale = price_scale;
        self
    }

    pub fn with_position_allocations(
        mut self,
        allocations: BTreeMap<String, PositionAllocation>,
    ) -> Result<Self, SimulationError> {
        if allocations.len() != self.states.len()
            || allocations
                .keys()
                .any(|symbol| !self.states.contains_key(symbol))
            || allocations.values().any(|allocation| {
                allocation.budget_usdt_ticks <= 0
                    || allocation.max_position <= 0
                    || allocation.requested_quantity <= 0
            })
        {
            return Err(SimulationError::InvalidConfig(
                "position allocations must cover every symbol with positive values",
            ));
        }
        self.capital_usdt_ticks = Some(
            allocations
                .values()
                .map(|allocation| allocation.budget_usdt_ticks)
                .sum(),
        );
        self.position_allocations = allocations;
        Ok(self)
    }

    pub fn on_event(&mut self, event: BinanceMarketEvent) -> Vec<SimulationRecord> {
        self.on_event_ref(&event)
    }

    /// Borrowed event path using exchange event time as the local clock. Replay
    /// should call `on_event_at_ref` when a recorded receipt time is available.
    pub fn on_event_ref(&mut self, event: &BinanceMarketEvent) -> Vec<SimulationRecord> {
        self.on_event_at_ref(event, event_time_ms(event))
    }

    /// Canonical event entrypoint. All live, replay, and simulation callers
    /// should pass through the envelope so identity, freshness, sequence, and
    /// causality are validated before the state machine mutates.
    pub fn on_enveloped_event(
        &mut self,
        envelope: &EventEnvelope<BinanceMarketEvent>,
    ) -> Result<Vec<SimulationRecord>, SimulationError> {
        self.causal_ledger
            .commit(envelope)
            .map_err(|error| SimulationError::Causality(error.to_string()))?;
        Ok(self.on_event_at_ref(&envelope.payload, envelope.received_at_ms))
    }

    /// Processes an event with its observed local time kept separate from the
    /// exchange timestamp. This is the boundary where real network latency is
    /// introduced into simulation/replay without changing strategy signal inputs.
    pub fn on_event_at_ref(
        &mut self,
        event: &BinanceMarketEvent,
        received_at_ms: u64,
    ) -> Vec<SimulationRecord> {
        self.event_count = self.event_count.saturating_add(1);
        self.last_event_at_ms = event_time_ms(event);
        self.last_received_at_ms = received_at_ms;
        let mut records = self.settle_pending_cancels(received_at_ms);
        records.extend(self.refresh_dynamic_allocations(self.last_event_at_ms));
        records.extend(match event {
            BinanceMarketEvent::BookTicker(ticker) => self.on_book_ticker(ticker),
            BinanceMarketEvent::MarkPrice(mark) => self.on_mark_price(mark, received_at_ms),
            BinanceMarketEvent::AggTrade(trade) => self.on_agg_trade(trade),
            BinanceMarketEvent::DepthUpdate(depth) => self.on_depth_update(depth),
        });
        records
    }

    fn settle_pending_cancels(&mut self, timestamp_ms: u64) -> Vec<SimulationRecord> {
        let latency = self.realism.latency.cancel_to_exchange_ms;
        if latency == 0 {
            return Vec::new();
        }
        let symbols = self.states.keys().cloned().collect::<Vec<_>>();
        let mut records = Vec::new();
        for symbol in symbols {
            let due = self.states[&symbol].working.is_some_and(|order| {
                order.cancel_requested_at_ms.is_some_and(|requested_at| {
                    timestamp_ms >= requested_at.saturating_add(latency)
                })
            });
            if !due {
                continue;
            }
            let order = self
                .states
                .get_mut(&symbol)
                .and_then(|state| state.working.take())
                .expect("pending cancel order exists");
            if let Some(state) = self.states.get_mut(&symbol) {
                state
                    .calibration
                    .observe_order_terminal(timestamp_ms, order.placed_at_ms);
            }
            let state = self.states.get(&symbol).expect("symbol state exists");
            records.push(self.record(
                &symbol,
                state,
                timestamp_ms,
                RecordFields {
                    kind: "order_canceled",
                    decision_id: order.decision_id,
                    client_id: Some(order.client_id),
                    side: Some(order.side),
                    price_ticks: Some(order.price_ticks),
                    quantity: Some(order.remaining_quantity),
                    order_age_ms: Some(timestamp_ms.saturating_sub(order.placed_at_ms)),
                    queue_ahead_quantity: Some(order.queue_ahead_quantity),
                    quote_distance_bps: Some(order.quote_distance_bps),
                    detail: Some("cancel acknowledged after exchange latency"),
                },
            ));
        }
        records
    }

    fn refresh_dynamic_allocations(&mut self, timestamp_ms: u64) -> Vec<SimulationRecord> {
        if !self.strategy_variant.uses_dynamic_capital()
            || self.capital_usdt_ticks.is_none()
            || (self.last_dynamic_capital_update_ms > 0
                && timestamp_ms.saturating_sub(self.last_dynamic_capital_update_ms)
                    < self.dynamic_capital_refresh_ms)
        {
            return Vec::new();
        }
        let risk_inputs = self
            .states
            .iter()
            .map(|(symbol, state)| {
                let gap_bps = match (state.mark_price_ticks, state.index_price_ticks) {
                    (Some(mark), Some(index)) => bps_between(mark, index),
                    _ => 1_000,
                };
                let tail_bps = if self.strategy_variant.uses_tail_guard() {
                    m5_tail_stress_bps(state)
                } else {
                    0
                };
                let risk_bps = 1_i64
                    .saturating_add(state.ewma_abs_return_bps.saturating_mul(3))
                    .saturating_add(state.ewma_spread_bps)
                    .saturating_add(gap_bps / 2)
                    .saturating_add(tail_bps / 2)
                    .max(1);
                let eligible = data_quality_for(state, timestamp_ms, self.max_mark_index_gap_bps)
                    == DataQualityStatus::Fresh
                    && state.anchor.valid_at(timestamp_ms, self.max_anchor_age_ms)
                    && (!self.live_risk_gates
                        || funding_entry_allowed_variant(
                            state,
                            timestamp_ms,
                            self.strategy_variant,
                            self.fee_ppm,
                        ));
                (symbol.clone(), CapitalRiskInput { risk_bps, eligible })
            })
            .collect::<BTreeMap<_, _>>();
        let weights = match dynamic_weights(&risk_inputs, 500, 3_000) {
            Ok(weights) => weights,
            Err(_) => return Vec::new(),
        };
        let total_capital = self.capital_usdt_ticks.unwrap_or(0);
        if total_capital <= 0 {
            return Vec::new();
        }
        let symbols = self.states.keys().cloned().collect::<Vec<_>>();
        let mut budget_left = i128::from(total_capital);
        let mut weight_left = 10_000_i64;
        let mut allocations = BTreeMap::new();
        for (index, symbol) in symbols.iter().enumerate() {
            let weight = weights[symbol].weight_bps;
            let budget = if index + 1 == symbols.len() || weight_left == weight {
                budget_left
            } else {
                i128::from(total_capital) * i128::from(weight) / 10_000
            };
            budget_left = budget_left.saturating_sub(budget);
            weight_left = weight_left.saturating_sub(weight);
            let anchor_price = self.states[symbol].anchor.close_price_ticks;
            let quantity = budget.saturating_mul(10_i128.pow(self.quantity_scale))
                / i128::from(anchor_price.max(1));
            if budget <= 0 || quantity <= 0 || quantity > i128::from(i64::MAX) {
                return Vec::new();
            }
            allocations.insert(
                symbol.clone(),
                PositionAllocation {
                    mode: format!("dynamic:w{}:r{}", weight, weights[symbol].risk_bps),
                    budget_usdt_ticks: i64::try_from(budget).unwrap_or(i64::MAX),
                    max_position: i64::try_from(quantity).unwrap_or(i64::MAX),
                    requested_quantity: i64::try_from(quantity).unwrap_or(i64::MAX),
                },
            );
        }
        let changed_symbols = symbols
            .iter()
            .filter(|symbol| {
                let old = self.position_allocations.get(*symbol);
                let candidate = allocations.get(*symbol);
                old.map(|allocation| {
                    candidate.is_some_and(|candidate| {
                        allocation.budget_usdt_ticks != candidate.budget_usdt_ticks
                            || allocation.max_position != candidate.max_position
                    })
                })
                .unwrap_or(true)
            })
            .cloned()
            .collect::<Vec<_>>();
        if changed_symbols.is_empty() {
            self.last_dynamic_capital_update_ms = timestamp_ms;
            return Vec::new();
        }
        self.position_allocations = allocations;
        self.last_dynamic_capital_update_ms = timestamp_ms;
        let mut records = Vec::new();
        for symbol in changed_symbols {
            let state = self.states.get(&symbol).expect("symbol state exists");
            records.push(
                self.record(
                    &symbol,
                    state,
                    timestamp_ms,
                    RecordFields {
                        kind: "capital_rebalance",
                        decision_id: None,
                        client_id: None,
                        side: None,
                        price_ticks: state.mark_price_ticks,
                        quantity: self
                            .position_allocations
                            .get(&symbol)
                            .map(|a| a.requested_quantity),
                        order_age_ms: None,
                        queue_ahead_quantity: None,
                        quote_distance_bps: None,
                        detail: Some("M6 dynamic risk-budget target updated"),
                    },
                ),
            );
            records.extend(self.rebalance_symbol(&symbol, timestamp_ms));
        }
        records
    }

    pub fn refresh_anchors(
        &mut self,
        anchors: BTreeMap<String, AnchorSnapshot>,
        timestamp_ms: u64,
    ) {
        for (symbol, anchor) in anchors {
            let Some(state) = self.states.get_mut(&symbol) else {
                continue;
            };
            let current_anchor_after_close =
                anchor_refresh_allowed(&symbol, state.anchor.observed_at_ms);
            let current_day = local_day(state.anchor.observed_at_ms);
            let candidate_day = local_day(anchor.observed_at_ms);
            if anchor.close_price_ticks > 0
                && anchor.observed_at_ms > state.anchor.observed_at_ms
                && (candidate_day > current_day
                    || (candidate_day == current_day && !current_anchor_after_close))
                && anchor_refresh_allowed(&symbol, timestamp_ms)
            {
                state.anchor = anchor;
            }
        }
    }

    pub fn cancel_all(&mut self, timestamp_ms: u64, detail: &str) -> Vec<SimulationRecord> {
        // End-of-replay cleanup is an explicit simulator boundary: emit the
        // local cancel acknowledgement so the final ledger is not left with
        // phantom working orders.
        let cancel_latency = self.realism.latency.cancel_to_exchange_ms;
        self.realism.latency.cancel_to_exchange_ms = 0;
        let symbols = self.states.keys().cloned().collect::<Vec<_>>();
        let records = symbols
            .into_iter()
            .flat_map(|symbol| self.cancel_symbol(&symbol, timestamp_ms, detail))
            .collect();
        self.realism.latency.cancel_to_exchange_ms = cancel_latency;
        records
    }

    /// Cancels all working quotes and submits reduce-only maker orders for
    /// residual positions. It never fabricates a fill: callers must continue
    /// feeding market events until the returned orders actually fill.
    pub fn flatten_all(&mut self, timestamp_ms: u64, detail: &str) -> Vec<SimulationRecord> {
        let mut records = self.cancel_all(timestamp_ms, detail);
        let symbols = self.states.keys().cloned().collect::<Vec<_>>();
        for symbol in symbols {
            let position = self.states[&symbol].position;
            if position == 0 {
                continue;
            }
            let desired = self.states[&symbol].book.map(|book| OrderIntent {
                symbol: self.states[&symbol].symbol_id,
                side: if position > 0 { Side::Sell } else { Side::Buy },
                price: if position > 0 {
                    book.ask_price_ticks
                } else {
                    book.bid_price_ticks
                },
                quantity: position.checked_abs().unwrap_or(i64::MAX),
                post_only: true,
            });
            if let Some(intent) = desired {
                records.extend(self.place_symbol(&symbol, intent, timestamp_ms, true, None));
            } else {
                let state = self.states.get(&symbol).expect("symbol state exists");
                records.push(self.record(
                    &symbol,
                    state,
                    timestamp_ms,
                    RecordFields {
                        kind: "flatten_unavailable",
                        decision_id: None,
                        client_id: None,
                        side: None,
                        price_ticks: None,
                        quantity: Some(position.checked_abs().unwrap_or(i64::MAX)),
                        order_age_ms: None,
                        queue_ahead_quantity: None,
                        quote_distance_bps: None,
                        detail: Some("reduce-only maker flatten requires a valid book"),
                    },
                ));
            }
        }
        records
    }

    /// Normal shutdown boundary: cancel, request maker-only flattening, and
    /// return a settlement that explicitly distinguishes flat, pending, and
    /// unflattened residual states.
    pub fn shutdown(&mut self, timestamp_ms: u64, detail: &str) -> FinalSettlement {
        let flatten_requested = self.states.values().any(|state| state.position != 0);
        let records = self.flatten_all(timestamp_ms, detail);
        let summary = self.summary();
        let settlement_status = if summary.flat_at_end {
            "flat"
        } else if summary.working_orders > 0 {
            "flatten_orders_working"
        } else if summary.current_absolute_position > 0 {
            "residual_position_unflattened"
        } else {
            "not_flat"
        }
        .to_owned();
        FinalSettlement {
            records,
            summary,
            flatten_requested,
            settlement_status,
        }
    }

    fn reject_entry(&mut self, owner: &'static str) {
        self.rejected_entries = self.rejected_entries.saturating_add(1);
        *self.gate_rejections.entry(owner.to_owned()).or_default() += 1;
    }

    pub fn summary(&self) -> SimulationSummary {
        let mut current_absolute_position = 0_i64;
        let mut realized_pnl_ticks = 0_i64;
        let mut unrealized_pnl_ticks = 0_i64;
        let mut market_pnl_ticks = 0_i64;
        let mut strategy_pnl_ticks = 0_i64;
        let mut funding_pnl_ticks = 0_i64;
        let mut fees_ticks = 0_i64;
        let mut working_orders = 0_u64;
        let mut unrealized_valuation_complete = true;
        for state in self.states.values() {
            current_absolute_position = current_absolute_position
                .saturating_add(state.position.checked_abs().unwrap_or(i64::MAX));
            realized_pnl_ticks = realized_pnl_ticks.saturating_add(state.realized_pnl_ticks);
            market_pnl_ticks = market_pnl_ticks.saturating_add(state.market_pnl_ticks);
            strategy_pnl_ticks = strategy_pnl_ticks.saturating_add(state.strategy_pnl_ticks);
            funding_pnl_ticks = funding_pnl_ticks.saturating_add(state.funding_pnl_ticks);
            fees_ticks = fees_ticks.saturating_add(state.fees_ticks);
            working_orders += u64::from(state.working.is_some());
            if state.position != 0 {
                match unrealized_pnl(state, self.quantity_scale) {
                    Some(pnl) => unrealized_pnl_ticks = unrealized_pnl_ticks.saturating_add(pnl),
                    None => unrealized_valuation_complete = false,
                }
            }
        }
        let flat_at_end = current_absolute_position == 0 && working_orders == 0;
        SimulationSummary {
            event_count: self.event_count,
            order_count: self.order_count,
            fill_count: self.fill_count,
            filled_quantity: self.filled_quantity,
            rejected_entries: self.rejected_entries,
            gate_rejections: self.gate_rejections.clone(),
            realized_pnl_ticks,
            unrealized_pnl_ticks,
            market_pnl_ticks,
            strategy_pnl_ticks,
            funding_pnl_ticks,
            gross_pnl_ticks: market_pnl_ticks
                .saturating_add(strategy_pnl_ticks)
                .saturating_add(funding_pnl_ticks),
            fees_ticks,
            net_pnl_ticks: market_pnl_ticks
                .saturating_add(strategy_pnl_ticks)
                .saturating_add(funding_pnl_ticks)
                .saturating_sub(fees_ticks),
            maker_fee_ppm: self.fee_ppm,
            unrealized_valuation_complete,
            current_absolute_position,
            peak_absolute_position: self.peak_absolute_position,
            working_orders,
            flat_at_end,
        }
    }

    pub fn checkpoint_view(&self) -> (u64, i64, Vec<String>) {
        let position_ticks = self
            .states
            .values()
            .map(|state| state.position)
            .fold(0_i64, i64::saturating_add);
        let working_order_ids = self
            .states
            .values()
            .filter_map(|state| {
                state
                    .working
                    .map(|order| format!("{}:{}", self.strategy_variant.label(), order.client_id))
            })
            .collect();
        (self.last_event_at_ms, position_ticks, working_order_ids)
    }

    pub fn restore_calibration_states(&mut self, seeds: &BTreeMap<String, CalibrationState>) {
        for (symbol, seed) in seeds {
            if seed.instrument != symbol.as_str() {
                continue;
            }
            if let Some(state) = self.states.get_mut(symbol) {
                state.calibration = seed.clone();
            }
        }
    }

    pub fn calibration_snapshots(
        &self,
        fee_pico_bps: i64,
    ) -> BTreeMap<String, CalibrationSnapshot> {
        self.states
            .iter()
            .map(|(symbol, state)| (symbol.clone(), state.calibration.snapshot(fee_pico_bps)))
            .collect()
    }

    pub fn metrics_snapshot(
        &self,
        observed_at_ms: u64,
        last_received_at_ms: u64,
    ) -> MetricsSnapshot {
        let symbols = self
            .states
            .iter()
            .map(|(symbol, state)| {
                let (requested_quantity, max_position) = self
                    .position_allocations
                    .get(symbol)
                    .map(|allocation| (allocation.requested_quantity, allocation.max_position))
                    .unwrap_or((self.requested_quantity, self.max_position));
                let quote_quantity = state
                    .book
                    .map(|book| {
                        liquidity_adjusted_quantity(
                            requested_quantity,
                            book.bid_quantity,
                            book.ask_quantity,
                        )
                    })
                    .unwrap_or(requested_quantity);
                let (bid_price_ticks, ask_price_ticks) = state
                    .book
                    .map(|book| (Some(book.bid_price_ticks), Some(book.ask_price_ticks)))
                    .unwrap_or((None, None));
                let fair_value = fair_value_for_state(state);
                let threshold_diagnostic = dynamic_threshold_diagnostic_for(
                    state,
                    self.strategy_variant,
                    self.strategy.entry_threshold_bps,
                    self.fee_ppm,
                    quote_quantity,
                    max_position,
                    self.last_event_at_ms,
                );
                let threshold = threshold_diagnostic
                    .threshold
                    .map(|threshold| scale_threshold_non_fee(threshold, self.threshold_scale_ppm));
                let calendar_state = calendar_state_for(symbol, self.last_event_at_ms);
                let data_quality =
                    data_quality_for(state, self.last_event_at_ms, self.max_mark_index_gap_bps);
                let equity_entry_allowed = !self.live_risk_gates
                    || simulation_session_allows_entry(symbol, self.last_event_at_ms);
                let funding_known = !self.live_risk_gates
                    || (state.next_funding_time_ms > self.last_event_at_ms
                        && state.latest_funding_rate_e8.is_some());
                let anchor_allowed = state
                    .anchor
                    .valid_at(self.last_event_at_ms, self.max_anchor_age_ms)
                    && (!self.live_risk_gates
                        || state.anchor.observed_at_ms == 0
                        || simulation_anchor_usable(
                            symbol,
                            state.anchor.observed_at_ms,
                            self.last_event_at_ms,
                        ));
                let funding_decision =
                    m8_funding_decision(state, self.last_event_at_ms, max_position, self.fee_ppm);
                let funding_overlay = evaluate_funding_overlay(
                    funding_decision.action,
                    if state.latest_funding_rate_e8.is_some() {
                        crate::m8::FundingRateStatus::Observed
                    } else {
                        crate::m8::FundingRateStatus::Missing
                    },
                    funding_decision.funding_carry_bps,
                    state.position,
                );
                let funding_allowed = !self.live_risk_gates
                    || if self.strategy_variant == SimulationPolicyVariant::M8FundingAware {
                        funding_overlay.allow_base_strategy
                    } else {
                        funding_entry_allowed_variant(
                            state,
                            self.last_event_at_ms,
                            self.strategy_variant,
                            self.fee_ppm,
                        )
                    };
                let risk_state = if !equity_entry_allowed {
                    SimulationRiskState::ReduceOnlyEquitySession
                } else if !matches!(data_quality, DataQualityStatus::Fresh) {
                    SimulationRiskState::HaltMarketData
                } else if !anchor_allowed {
                    SimulationRiskState::HaltAnchor
                } else if self.strategy_variant.uses_tail_guard() && m5_tail_reduce_only(state) {
                    SimulationRiskState::ReduceOnlyTailRisk
                } else if !funding_known {
                    SimulationRiskState::HaltFundingMetadata
                } else if self.strategy_variant == SimulationPolicyVariant::M8FundingAware {
                    match funding_overlay.state {
                        crate::risk::FundingRiskState::ReduceOnly => {
                            SimulationRiskState::ReduceOnlyFundingRisk
                        }
                        crate::risk::FundingRiskState::Adverse => {
                            SimulationRiskState::NoEntryFunding
                        }
                        crate::risk::FundingRiskState::Halt => {
                            SimulationRiskState::HaltFundingMetadata
                        }
                        crate::risk::FundingRiskState::Neutral
                        | crate::risk::FundingRiskState::Favorable => SimulationRiskState::Trading,
                    }
                } else if !funding_allowed {
                    SimulationRiskState::ReduceOnlyFundingDeadline
                } else {
                    SimulationRiskState::Trading
                };
                let anchor_age_ms = (state.anchor.observed_at_ms > 0).then(|| {
                    self.last_event_at_ms
                        .saturating_sub(state.anchor.observed_at_ms)
                });
                // Signal age is measured entirely on the exchange clock.
                // Receipt timestamps remain transport telemetry only.
                let mark_age_ms = (state.last_mark_time_ms > 0).then(|| {
                    self.last_event_at_ms
                        .saturating_sub(state.last_mark_time_ms)
                });
                let reference_ticks = fair_value
                    .map(|estimate| estimate.price.0)
                    .unwrap_or(state.anchor.close_price_ticks);
                let buy_edge_pico_bps =
                    bid_price_ticks.and_then(|price| edge_pico_bps(reference_ticks, price));
                let sell_edge_pico_bps =
                    ask_price_ticks.and_then(|price| edge_pico_bps(price, reference_ticks));
                let buy_edge_bps = buy_edge_pico_bps.map(pico_bps_to_bps);
                let sell_edge_bps = sell_edge_pico_bps.map(pico_bps_to_bps);
                let buy_edge_micro_bps = buy_edge_pico_bps.map(pico_bps_to_micro);
                let sell_edge_micro_bps = sell_edge_pico_bps.map(pico_bps_to_micro);
                let entry_block_reason = entry_block_reason_for(
                    state,
                    risk_state,
                    threshold,
                    threshold_diagnostic.status,
                    state.adaptive_relief_pico_bps,
                    buy_edge_pico_bps,
                    sell_edge_pico_bps,
                );

                SymbolMetrics {
                    symbol: symbol.clone(),
                    position_mode: self
                        .position_allocations
                        .get(symbol)
                        .map(|allocation| allocation.mode.clone()),
                    allocated_capital_usdt_ticks: self
                        .position_allocations
                        .get(symbol)
                        .filter(|allocation| allocation.budget_usdt_ticks > 0)
                        .map(|allocation| allocation.budget_usdt_ticks),
                    allocated_capital_usdt: self
                        .position_allocations
                        .get(symbol)
                        .filter(|allocation| allocation.budget_usdt_ticks > 0)
                        .map(|allocation| {
                            crate::execution::binance_wire::format_ticks(
                                allocation.budget_usdt_ticks,
                                self.price_scale,
                            )
                        }),
                    target_quantity: self
                        .position_allocations
                        .get(symbol)
                        .map(|allocation| allocation.requested_quantity),
                    target_quantity_units: self.position_allocations.get(symbol).map(
                        |allocation| {
                            crate::execution::binance_wire::format_ticks(
                                allocation.requested_quantity,
                                self.quantity_scale,
                            )
                        },
                    ),
                    position_notional_usdt_ticks: state.mark_price_ticks.map(|price| {
                        clamp_i128(
                            i128::from(price.abs()) * i128::from(state.position.abs())
                                / quantity_scale_multiplier(self.quantity_scale),
                        )
                    }),
                    position_notional_usdt: state.mark_price_ticks.map(|price| {
                        let notional_ticks = clamp_i128(
                            i128::from(price.abs()) * i128::from(state.position.abs())
                                / quantity_scale_multiplier(self.quantity_scale),
                        );
                        crate::execution::binance_wire::format_ticks(
                            notional_ticks,
                            self.price_scale,
                        )
                    }),
                    position: state.position,
                    fills: state.fills,
                    winning_fills: state.winning_fills,
                    losing_fills: state.losing_fills,
                    realized_pnl_ticks: state.realized_pnl_ticks,
                    unrealized_pnl_ticks: unrealized_pnl(state, self.quantity_scale).unwrap_or(0),
                    market_pnl_ticks: state.market_pnl_ticks,
                    strategy_pnl_ticks: state.strategy_pnl_ticks,
                    funding_pnl_ticks: state.funding_pnl_ticks,
                    fees_ticks: state.fees_ticks,
                    net_pnl_ticks: state
                        .market_pnl_ticks
                        .saturating_add(state.strategy_pnl_ticks)
                        .saturating_add(state.funding_pnl_ticks)
                        .saturating_sub(state.fees_ticks),
                    risk_metrics: None,
                    anchor_age_ms,
                    anchor_final_close: state.anchor.observed_at_ms == 0
                        || anchor_refresh_allowed(symbol, state.anchor.observed_at_ms),
                    calendar_state: calendar_state.to_owned(),
                    next_funding_time_ms: state.next_funding_time_ms,
                    latest_funding_rate_e8: state.latest_funding_rate_e8,
                    funding_flatten_deadline_ms: (self.strategy_variant
                        != SimulationPolicyVariant::M8FundingAware)
                        .then(|| funding_flatten_deadline(state.next_funding_time_ms))
                        .flatten(),
                    funding_action: format!("{:?}", funding_decision.action),
                    funding_carry_bps: funding_decision.funding_carry_bps,
                    funding_net_edge_bps: funding_decision.net_edge_bps,
                    risk_state: risk_state.label().to_owned(),
                    entry_block_reason: entry_block_reason.to_owned(),
                    data_quality,
                    mark_age_ms,
                    bid_price_ticks,
                    ask_price_ticks,
                    anchor_price_ticks: state.anchor.close_price_ticks,
                    mark_price_ticks: state.mark_price_ticks,
                    index_price_ticks: state.index_price_ticks,
                    ewma_abs_return_bps: state.ewma_abs_return_bps,
                    ewma_spread_bps: state.ewma_spread_bps,
                    ewma_abs_return_micro_bps: state.ewma_abs_return_micro_bps,
                    ewma_spread_micro_bps: state.ewma_spread_micro_bps,
                    ewma_abs_return_pico_bps: state.ewma_abs_return_pico_bps,
                    ewma_spread_pico_bps: state.ewma_spread_pico_bps,
                    ewma_adverse_markout_bps: pico_bps_to_bps(state.ewma_adverse_markout_pico_bps),
                    ewma_adverse_markout_micro_bps: pico_bps_to_micro(
                        state.ewma_adverse_markout_pico_bps,
                    ),
                    ewma_adverse_markout_pico_bps: state.ewma_adverse_markout_pico_bps,
                    evaluated_markouts: state.evaluated_markouts,
                    adverse_markouts: state.adverse_markouts,
                    adaptive_relief_bps: state.adaptive_relief_bps,
                    adaptive_relief_micro_bps: state.adaptive_relief_micro_bps,
                    adaptive_relief_pico_bps: state.adaptive_relief_pico_bps,
                    buy_edge_bps,
                    sell_edge_bps,
                    buy_edge_micro_bps,
                    sell_edge_micro_bps,
                    buy_edge_pico_bps,
                    sell_edge_pico_bps,
                    liquidity_ratio_bps: state.book.map(|book| {
                        liquidity_ratio_bps(quote_quantity, book.bid_quantity, book.ask_quantity)
                    }),
                    liquidity_penalty_bps: state.book.map(|book| {
                        liquidity_penalty_bps(quote_quantity, book.bid_quantity, book.ask_quantity)
                    }),
                    liquidity_fill_probability_bps: state.book.map(|book| {
                        fill_probability_bps(quote_quantity, book.bid_quantity, book.ask_quantity)
                    }),
                    fair_value_ticks: fair_value.map(|estimate| estimate.price.0),
                    fair_value_confidence_bps: fair_value.map(|estimate| estimate.confidence_bps),
                    market_regime: fair_value.map(|estimate| estimate.regime.label().to_owned()),
                    threshold_status: threshold_diagnostic.status.label().to_owned(),
                    threshold_prior_used: threshold_diagnostic.prior_used,
                    threshold_missing_component: threshold_diagnostic
                        .missing_component
                        .map(str::to_owned),
                    m9_calibration: state
                        .calibration
                        .snapshot(ppm_to_pico_bps(self.fee_ppm.saturating_mul(2))),
                    threshold: threshold.map(threshold_metrics),
                }
            })
            .collect();
        MetricsSnapshot {
            observed_at_ms,
            strategy_variant: self.strategy_variant.label().to_owned(),
            last_market_event_at_ms: self.last_event_at_ms,
            last_received_at_ms,
            summary: self.summary(),
            symbols,
            history: Vec::new(),
            risk_metrics: None,
            calendar_snapshot: "sse-hkex-2026".to_owned(),
            maker_fee_source: "binance_usdm_base_maker_schedule".to_owned(),
            funding_model: "m8_exact_mark_settlement_plus_strategy_funding_controller".to_owned(),
            capital_usdt_ticks: self.capital_usdt_ticks,
            capital_usdt: self.capital_usdt_ticks.map(|capital| {
                crate::execution::binance_wire::format_ticks(capital, self.price_scale)
            }),
            model_assumptions: ModelAssumptions {
                fill_model: "local_depth_when_seeded_else_top_of_book_plus_aggregate_trade_queue"
                    .to_owned(),
                queue_ahead: self.realism.queue.visible_ahead,
                trade_through: self.realism.queue.trade_through,
                market_to_decision_ms: self.realism.latency.market_to_decision_ms,
                decision_to_exchange_ms: self.realism.latency.decision_to_exchange_ms,
                cancel_to_exchange_ms: self.realism.latency.cancel_to_exchange_ms,
            },
        }
    }

    pub fn performance_point(&self, observed_at_ms: u64) -> PerformancePoint {
        let summary = self.summary();
        PerformancePoint {
            observed_at_ms,
            market_pnl_ticks: summary.market_pnl_ticks,
            strategy_pnl_ticks: summary.strategy_pnl_ticks,
            funding_pnl_ticks: summary.funding_pnl_ticks,
            fees_ticks: summary.fees_ticks,
            gross_pnl_ticks: summary.gross_pnl_ticks,
            net_pnl_ticks: summary.net_pnl_ticks,
            current_absolute_position: summary.current_absolute_position,
            symbols: self
                .states
                .iter()
                .map(|(symbol, state)| SymbolPerformancePoint {
                    symbol: symbol.clone(),
                    position: state.position,
                    market_pnl_ticks: state.market_pnl_ticks,
                    strategy_pnl_ticks: state.strategy_pnl_ticks,
                    funding_pnl_ticks: state.funding_pnl_ticks,
                    fees_ticks: state.fees_ticks,
                    net_pnl_ticks: state
                        .market_pnl_ticks
                        .saturating_add(state.strategy_pnl_ticks)
                        .saturating_add(state.funding_pnl_ticks)
                        .saturating_sub(state.fees_ticks),
                })
                .collect(),
        }
    }

    pub fn metrics_snapshot_with_history(
        &self,
        observed_at_ms: u64,
        last_received_at_ms: u64,
        history: &[PerformancePoint],
    ) -> MetricsSnapshot {
        let mut snapshot = self.metrics_snapshot(observed_at_ms, last_received_at_ms);
        snapshot.history = history.to_vec();
        if let Some(capital_ticks) = snapshot.capital_usdt_ticks.filter(|capital| *capital > 0) {
            let portfolio_points = history
                .iter()
                .map(|point| (point.observed_at_ms, point.net_pnl_ticks))
                .collect::<Vec<_>>();
            snapshot.risk_metrics = Some(calculate_risk_metrics(&portfolio_points, capital_ticks));

            let mut symbol_points = BTreeMap::<String, Vec<(u64, i64)>>::new();
            for point in history {
                for symbol_point in &point.symbols {
                    symbol_points
                        .entry(symbol_point.symbol.clone())
                        .or_default()
                        .push((point.observed_at_ms, symbol_point.net_pnl_ticks));
                }
            }
            for symbol in &mut snapshot.symbols {
                let points = symbol_points
                    .get(&symbol.symbol)
                    .cloned()
                    .unwrap_or_default();
                let symbol_capital = symbol
                    .allocated_capital_usdt_ticks
                    .filter(|capital| *capital > 0)
                    .unwrap_or(capital_ticks);
                symbol.risk_metrics = Some(calculate_risk_metrics(&points, symbol_capital));
            }
        }
        snapshot
    }

    /// Seeds this ledger's local book from the same REST snapshot used by
    /// SimulationBatch. Replay callers may omit it and use top-of-book fallback.
    pub fn load_depth_snapshot(
        &mut self,
        symbol: &str,
        last_update_id: u64,
        bids: &[(i64, i64)],
        asks: &[(i64, i64)],
    ) -> Result<(), SimulationError> {
        let symbol = symbol.to_ascii_uppercase();
        let state = self
            .states
            .get_mut(&symbol)
            .ok_or(SimulationError::ReplaySymbolNotConfigured(symbol))?;
        state
            .local_book
            .load_snapshot(last_update_id, bids, asks)
            .map_err(|_| SimulationError::InvalidConfig("invalid local depth snapshot"))
    }

    fn on_depth_update(
        &mut self,
        depth: &crate::market::binance::DepthUpdate,
    ) -> Vec<SimulationRecord> {
        let symbol = depth.symbol.to_ascii_uppercase();
        if let Some(state) = self.states.get_mut(&symbol) {
            // SimulationBatch validates the shared stream before dispatch. Keeping the
            // ledger copy synchronized lets fills use quantity at the order's
            // actual price rather than whichever level is best now.
            let _ = state.local_book.apply_diff(depth);
        }
        Vec::new()
    }

    fn on_book_ticker(&mut self, ticker: &BookTicker) -> Vec<SimulationRecord> {
        let symbol = ticker.symbol.to_ascii_uppercase();
        if let Some(state) = self.states.get_mut(&symbol) {
            if state
                .last_book_update_id
                .is_some_and(|last| ticker.update_id <= last)
            {
                return Vec::new();
            }
            state.last_book_update_id = Some(ticker.update_id);
            state.last_book_event_at_ms = ticker.event_time_ms;
            state.book = Some(BookState {
                bid_price_ticks: ticker.bid_price.0,
                bid_quantity: ticker.bid_quantity.0,
                ask_price_ticks: ticker.ask_price.0,
                ask_quantity: ticker.ask_quantity.0,
            });
            let mid = (i128::from(ticker.bid_price.0) + i128::from(ticker.ask_price.0)) / 2;
            let spread_pico_bps =
                (mid > 0 && ticker.ask_price.0 >= ticker.bid_price.0).then(|| {
                    ((i128::from(ticker.ask_price.0) - i128::from(ticker.bid_price.0))
                        * 10_000
                        * i128::from(PICO_BPS_SCALE)
                        / mid)
                        .clamp(0, i128::from(i64::MAX)) as i64
                });
            if let Some(spread_pico_bps) = spread_pico_bps {
                state.ewma_spread_pico_bps =
                    ewma_scaled(state.ewma_spread_pico_bps, spread_pico_bps);
                state.ewma_spread_micro_bps = pico_bps_to_micro(state.ewma_spread_pico_bps);
                state.ewma_spread_bps = pico_bps_to_bps(state.ewma_spread_pico_bps);
            }
            state.calibration.observe_market(
                ticker.event_time_ms,
                None,
                spread_pico_bps,
                residual_pico_bps_for_state(state),
            );
        } else {
            return Vec::new();
        }
        self.rebalance_symbol(&symbol, ticker.event_time_ms)
    }

    fn on_mark_price(&mut self, mark: &MarkPrice, received_at_ms: u64) -> Vec<SimulationRecord> {
        let symbol = mark.symbol.to_ascii_uppercase();
        let funding_settled = {
            let Some(state) = self.states.get_mut(&symbol) else {
                return Vec::new();
            };
            if state.last_mark_time_ms > 0 && mark.event_time_ms < state.last_mark_time_ms {
                return Vec::new();
            }
            update_markout_feedback(state, mark.mark_price.0, mark.event_time_ms);
            let return_sample = state.last_mark_price_ticks.and_then(|previous| {
                if previous > 0 && mark.mark_price.0 > 0 {
                    let change = i128::from(mark.mark_price.0) - i128::from(previous);
                    Some(
                        (change.abs() * 10_000 * i128::from(PICO_BPS_SCALE) / i128::from(previous))
                            .clamp(0, i128::from(i64::MAX)) as i64,
                    )
                } else {
                    None
                }
            });
            if let Some(previous) = state.last_mark_price_ticks {
                if previous > 0 && mark.mark_price.0 > 0 {
                    let change = i128::from(mark.mark_price.0) - i128::from(previous);
                    let market_pnl = change * i128::from(state.position)
                        / quantity_scale_multiplier(self.quantity_scale);
                    state.market_pnl_ticks = state
                        .market_pnl_ticks
                        .saturating_add(clamp_i128(market_pnl));
                    let change_pico_bps =
                        (change.abs() * 10_000 * i128::from(PICO_BPS_SCALE) / i128::from(previous))
                            .clamp(0, i128::from(i64::MAX)) as i64;
                    state.ewma_abs_return_pico_bps =
                        ewma_scaled(state.ewma_abs_return_pico_bps, change_pico_bps);
                    state.ewma_abs_return_micro_bps =
                        pico_bps_to_micro(state.ewma_abs_return_pico_bps);
                    state.ewma_abs_return_bps = pico_bps_to_bps(state.ewma_abs_return_pico_bps);
                }
            }
            let due = state.next_funding_time_ms > 0
                && mark.event_time_ms >= state.next_funding_time_ms
                && state.last_settled_funding_time_ms < state.next_funding_time_ms;
            let funding_pnl = if due {
                state.latest_funding_rate_e8.map(|rate| {
                    let value = -i128::from(mark.mark_price.0)
                        * i128::from(state.position)
                        * i128::from(rate)
                        / 100_000_000
                        / quantity_scale_multiplier(self.quantity_scale);
                    let value = clamp_i128(value);
                    state.funding_pnl_ticks = state.funding_pnl_ticks.saturating_add(value);
                    state.last_settled_funding_time_ms = state.next_funding_time_ms;
                    value
                })
            } else {
                None
            };
            state.last_mark_price_ticks = Some(mark.mark_price.0);
            state.mark_price_ticks = Some(mark.mark_price.0);
            state.index_price_ticks = Some(mark.index_price.0);
            let residual = residual_pico_bps_for_state(state);
            state
                .calibration
                .observe_market(mark.event_time_ms, return_sample, None, residual);
            state.latest_funding_rate_e8 = mark.latest_funding_rate_e8;
            state.next_funding_time_ms = mark.next_funding_time_ms;
            state.last_mark_time_ms = mark.event_time_ms;
            state.last_mark_received_at_ms = received_at_ms;
            funding_pnl
        };
        let mut records = self.rebalance_symbol(&symbol, mark.event_time_ms);
        if funding_settled.is_some() {
            let state = self.states.get(&symbol).expect("symbol state exists");
            records.push(self.record(
                &symbol,
                state,
                mark.event_time_ms,
                RecordFields {
                    kind: "funding_settlement",
                    decision_id: None,
                    client_id: None,
                    side: None,
                    price_ticks: Some(mark.mark_price.0),
                    quantity: Some(state.position.checked_abs().unwrap_or(i64::MAX)),
                    order_age_ms: None,
                    queue_ahead_quantity: None,
                    quote_distance_bps: None,
                    detail: Some("estimated funding settlement from mark stream"),
                },
            ));
        }
        records
    }

    fn on_agg_trade(&mut self, trade: &AggTrade) -> Vec<SimulationRecord> {
        let symbol = trade.symbol.to_ascii_uppercase();
        let fee_ppm = self.fee_ppm;
        let quantity_scale = self.quantity_scale;
        let realism = self.realism;
        let (quantity, order) = {
            let Some(state) = self.states.get_mut(&symbol) else {
                return Vec::new();
            };
            if state
                .last_trade_id
                .is_some_and(|last| trade.aggregate_trade_id <= last)
            {
                return Vec::new();
            }
            state.last_trade_id = Some(trade.aggregate_trade_id);
            let Some(order) = state.working else {
                return Vec::new();
            };
            if order.reduce_only
                && ((order.side == Side::Buy && state.position >= 0)
                    || (order.side == Side::Sell && state.position <= 0))
            {
                return Vec::new();
            }
            let compatible = match order.side {
                Side::Buy => trade.buyer_is_maker && trade.price.0 == order.price_ticks,
                Side::Sell => !trade.buyer_is_maker && trade.price.0 == order.price_ticks,
            };
            if !compatible {
                return Vec::new();
            }
            if trade.event_time_ms < order.exchange_arrival_at_ms {
                return Vec::new();
            }
            let Some(book) = state.book else {
                return Vec::new();
            };
            let local_quantity = if state.local_book.is_valid() {
                state
                    .local_book
                    .quantity_at(order.side == Side::Buy, order.price_ticks)
            } else {
                0
            };
            let book = if state.local_book.is_valid() {
                match order.side {
                    Side::Buy => TopOfBook {
                        bid_quantity: local_quantity,
                        ..TopOfBook {
                            bid_price_ticks: book.bid_price_ticks,
                            ask_price_ticks: book.ask_price_ticks,
                            bid_quantity: book.bid_quantity,
                            ask_quantity: book.ask_quantity,
                        }
                    },
                    Side::Sell => TopOfBook {
                        ask_quantity: local_quantity,
                        ..TopOfBook {
                            bid_price_ticks: book.bid_price_ticks,
                            ask_price_ticks: book.ask_price_ticks,
                            bid_quantity: book.bid_quantity,
                            ask_quantity: book.ask_quantity,
                        }
                    },
                }
            } else {
                TopOfBook {
                    bid_price_ticks: book.bid_price_ticks,
                    ask_price_ticks: book.ask_price_ticks,
                    bid_quantity: book.bid_quantity,
                    ask_quantity: book.ask_quantity,
                }
            };
            let fill_quantity = realism.evaluate_after_latency(
                MakerQuote {
                    side: order.side,
                    price_ticks: order.price_ticks,
                    quantity: order.remaining_quantity,
                },
                book,
                trade.quantity.0,
            );
            let quantity = match fill_quantity {
                crate::backtest::FillDecision::Fill { quantity } => quantity,
                crate::backtest::FillDecision::NoFill => 0,
            };
            if quantity <= 0 {
                return Vec::new();
            }
            let mut updated_order = order;
            updated_order.remaining_quantity -= quantity;
            state.working = (updated_order.remaining_quantity > 0).then_some(updated_order);
            apply_position_fill(
                state,
                order.side,
                order.price_ticks,
                quantity,
                fee_ppm,
                quantity_scale,
            );
            if !order.reduce_only {
                state.pending_markouts.push_back(PendingMarkout {
                    side: order.side,
                    fill_price_ticks: order.price_ticks,
                    due_at_ms: trade.event_time_ms.saturating_add(MARKOUT_HORIZON_MS),
                });
                while state.pending_markouts.len() > 256 {
                    state.pending_markouts.pop_front();
                }
            }
            let displayed_depth = match order.side {
                Side::Buy => book.bid_quantity,
                Side::Sell => book.ask_quantity,
            };
            state.calibration.observe_fill(
                trade.event_time_ms.max(trade.trade_time_ms),
                order.placed_at_ms,
                quantity,
                displayed_depth,
            );
            (quantity, order)
        };
        self.fill_count = self.fill_count.saturating_add(1);
        self.filled_quantity = self.filled_quantity.saturating_add(quantity);
        let state = self.states.get(&symbol).expect("symbol state exists");
        self.peak_absolute_position = self
            .peak_absolute_position
            .max(state.position.checked_abs().unwrap_or(i64::MAX));
        vec![self.record(
            &symbol,
            state,
            trade.trade_time_ms.max(trade.event_time_ms),
            RecordFields {
                kind: "fill",
                decision_id: order.decision_id,
                client_id: Some(order.client_id),
                side: Some(order.side),
                price_ticks: Some(order.price_ticks),
                quantity: Some(quantity),
                order_age_ms: Some(
                    trade
                        .trade_time_ms
                        .max(trade.event_time_ms)
                        .saturating_sub(order.placed_at_ms),
                ),
                queue_ahead_quantity: Some(order.queue_ahead_quantity),
                quote_distance_bps: Some(order.quote_distance_bps),
                detail: Some(if state.working.is_some() {
                    "partial maker fill"
                } else {
                    "complete maker fill"
                }),
            },
        )]
    }

    fn update_adaptive_threshold_controller(
        &mut self,
        symbol: &str,
        timestamp_ms: u64,
        max_position: i64,
        requested_quantity: i64,
    ) {
        let variant = self.strategy_variant;
        let floor_bps = self.strategy.entry_threshold_bps;
        let fee_ppm = self.fee_ppm;
        let threshold_scale_ppm = self.threshold_scale_ppm;
        let Some(state) = self.states.get_mut(symbol) else {
            return;
        };
        let Some(book) = state.book else {
            return;
        };
        let quantity =
            liquidity_adjusted_quantity(requested_quantity, book.bid_quantity, book.ask_quantity);
        let Some(base_threshold) = dynamic_threshold_for(
            state,
            variant,
            floor_bps,
            fee_ppm,
            quantity,
            max_position,
            timestamp_ms,
        )
        .map(|threshold| scale_threshold_non_fee(threshold, threshold_scale_ppm)) else {
            return;
        };
        let Some(required_pico_bps) = base_threshold.required_pico_bps() else {
            return;
        };
        let fair_value_ticks = fair_value_for_state(state)
            .map(|estimate| estimate.price.0)
            .unwrap_or(state.anchor.close_price_ticks);
        let buy_edge = edge_pico_bps(fair_value_ticks, book.bid_price_ticks)
            .unwrap_or(0)
            .max(0);
        let sell_edge = edge_pico_bps(book.ask_price_ticks, fair_value_ticks)
            .unwrap_or(0)
            .max(0);
        let best_edge = buy_edge.max(sell_edge);
        let low_volatility = state.ewma_abs_return_pico_bps <= 5 * PICO_BPS_SCALE;
        let stable_inventory = state.position == 0;
        let fresh_market = data_quality_for(state, timestamp_ms, self.max_mark_index_gap_bps)
            == DataQualityStatus::Fresh;
        let gap = i128::from(required_pico_bps)
            .saturating_sub(i128::from(best_edge))
            .max(0) as i64;
        let eligible = low_volatility
            && stable_inventory
            && fresh_market
            && gap > 0
            && gap <= ADAPTIVE_NEAR_MISS_WINDOW_PICO_BPS;
        let target_relief = if eligible {
            gap.min(ADAPTIVE_RELIEF_MAX_PICO_BPS)
        } else {
            0
        };
        let current = state.adaptive_relief_pico_bps;
        let next = if target_relief > current {
            current
                .saturating_add(ADAPTIVE_RELIEF_STEP_PICO_BPS)
                .min(target_relief)
        } else {
            current
                .saturating_sub(ADAPTIVE_RELIEF_STEP_PICO_BPS)
                .max(target_relief)
        }
        .clamp(0, ADAPTIVE_RELIEF_MAX_PICO_BPS);
        state.adaptive_relief_pico_bps = next;
        state.adaptive_relief_micro_bps = pico_bps_to_micro(next);
        state.adaptive_relief_bps = pico_bps_to_bps(next);
        state.near_miss_count = if eligible {
            state.near_miss_count.saturating_add(1)
        } else {
            0
        };
    }

    fn rebalance_symbol(&mut self, symbol: &str, timestamp_ms: u64) -> Vec<SimulationRecord> {
        let decision_id = self.next_decision_id;
        self.next_decision_id = self.next_decision_id.saturating_add(1);
        let allocation = self.position_allocations.get(symbol);
        let max_position = allocation
            .map(|allocation| allocation.max_position)
            .unwrap_or(self.max_position);
        let requested_quantity = allocation
            .map(|allocation| allocation.requested_quantity)
            .unwrap_or(self.requested_quantity);
        let strategy_variant = self.strategy_variant;
        self.update_adaptive_threshold_controller(
            symbol,
            timestamp_ms,
            max_position,
            requested_quantity,
        );
        let (desired, reduce_only, has_working, decision_reason) = {
            let state = self.states.get(symbol).expect("symbol state exists");
            let Some(book) = state.book else {
                return Vec::new();
            };
            let session_allowed =
                !self.live_risk_gates || simulation_session_allows_entry(symbol, timestamp_ms);
            let funding_decision = (self.strategy_variant
                == SimulationPolicyVariant::M8FundingAware)
                .then(|| m8_funding_decision(state, timestamp_ms, max_position, self.fee_ppm));
            // Funding is an incremental overlay. Neutral/zero funding delegates
            // admission back to the inherited M7 signal and risk layers.
            let funding_overlay = funding_decision.as_ref().map(|decision| {
                evaluate_funding_overlay(
                    decision.action,
                    if state.latest_funding_rate_e8.is_some() {
                        crate::m8::FundingRateStatus::Observed
                    } else {
                        crate::m8::FundingRateStatus::Missing
                    },
                    decision.funding_carry_bps,
                    state.position,
                )
            });
            let funding_allowed = !self.live_risk_gates
                || funding_decision.as_ref().map_or_else(
                    || {
                        funding_entry_allowed_variant(
                            state,
                            timestamp_ms,
                            self.strategy_variant,
                            self.fee_ppm,
                        )
                    },
                    |decision| {
                        funding_overlay
                            .as_ref()
                            .is_some_and(|overlay| overlay.allow_base_strategy)
                            && (decision.allow_entry
                                || decision.action == crate::m8::FundingAction::Avoid
                                || decision.action == crate::m8::FundingAction::NoAction)
                    },
                );
            let funding_reduce_only = funding_overlay
                .as_ref()
                .is_some_and(|overlay| overlay.reduce_only);
            let entries_allowed = session_allowed && funding_allowed;
            let tail_reduce_only = strategy_variant.uses_tail_guard() && m5_tail_reduce_only(state);
            if !entries_allowed || tail_reduce_only {
                let should_reduce = !session_allowed
                    || (!funding_allowed && state.position != 0)
                    || funding_reduce_only
                    || tail_reduce_only;
                if !should_reduce {
                    (
                        None,
                        true,
                        state.working.is_some(),
                        "entry_restricted_without_position_reduction",
                    )
                } else {
                    let (desired, exit_reason) =
                        maker_exit_intent_for_state(state, timestamp_ms, self.quantity_scale);
                    (desired, true, state.working.is_some(), exit_reason)
                }
            } else if strategy_variant == SimulationPolicyVariant::M9DeadlineCausalDroMpc {
                let (intent, reduce_only, reason) = m9_intent_for_state(
                    state,
                    timestamp_ms,
                    max_position,
                    requested_quantity,
                    self.max_mark_index_gap_bps,
                    self.fee_ppm,
                );
                (intent, reduce_only, state.working.is_some(), reason)
            } else {
                let mark_index_ok = match (state.mark_price_ticks, state.index_price_ticks) {
                    (Some(mark), Some(index)) => {
                        let gap = (i128::from(mark) - i128::from(index)).abs() * 10_000;
                        gap <= i128::from(self.max_mark_index_gap_bps) * i128::from(index.max(1))
                    }
                    _ => false,
                };
                let signal_age_ms = timestamp_ms.saturating_sub(state.last_mark_time_ms);
                let valid = book.bid_price_ticks > 0
                    && book.ask_price_ticks >= book.bid_price_ticks
                    && book.bid_quantity > 0
                    && book.ask_quantity > 0
                    && mark_index_ok
                    && signal_age_ms <= 5_000
                    && state.anchor.valid_at(timestamp_ms, self.max_anchor_age_ms)
                    && (!self.live_risk_gates
                        || state.anchor.observed_at_ms == 0
                        || simulation_anchor_usable(
                            symbol,
                            state.anchor.observed_at_ms,
                            timestamp_ms,
                        ));
                if !valid {
                    let reason =
                        match data_quality_for(state, timestamp_ms, self.max_mark_index_gap_bps) {
                            DataQualityStatus::Missing => "market_data_missing",
                            DataQualityStatus::Contradictory => "market_data_contradictory",
                            DataQualityStatus::Stale => "market_data_not_fresh",
                            DataQualityStatus::Fresh => "anchor_or_signal_gate",
                            DataQualityStatus::Unknown => "market_data_unknown",
                        };
                    (None, false, state.working.is_some(), reason)
                } else {
                    let quantity = liquidity_adjusted_quantity(
                        requested_quantity,
                        book.bid_quantity,
                        book.ask_quantity,
                    );
                    let quantity = if strategy_variant.uses_tail_guard() {
                        m5_quote_quantity(state, quantity)
                    } else {
                        quantity
                    };
                    let threshold = dynamic_threshold_for(
                        state,
                        strategy_variant,
                        self.strategy.entry_threshold_bps,
                        self.fee_ppm,
                        quantity,
                        max_position,
                        timestamp_ms,
                    )
                    .map(|threshold| scale_threshold_non_fee(threshold, self.threshold_scale_ppm));
                    let m7_required_pico_bps = threshold
                        .and_then(|value| value.required_pico_bps())
                        .map(|required| required.saturating_sub(state.adaptive_relief_pico_bps))
                        .unwrap_or(0);
                    let m7_blocked = strategy_variant == SimulationPolicyVariant::M7EvidenceGated
                        && !m7_entry_admissible(state, m7_required_pico_bps);
                    let intent = if strategy_variant == SimulationPolicyVariant::M0Fixed {
                        if m7_blocked {
                            None
                        } else {
                            self.strategy.generate_intent(
                                state.symbol_id,
                                book.bid_price_ticks,
                                book.ask_price_ticks,
                                state.anchor.close_price_ticks,
                                quantity,
                            )
                        }
                    } else if m7_blocked {
                        None
                    } else {
                        let fill_probability_bps =
                            fill_probability_bps(quantity, book.bid_quantity, book.ask_quantity);
                        let (buy_micro_adverse_bps, sell_micro_adverse_bps) =
                            side_adverse_selection_bps(book.bid_quantity, book.ask_quantity);
                        let (buy_pico_adverse_bps, sell_pico_adverse_bps) =
                            side_adverse_selection_pico_bps(book.bid_quantity, book.ask_quantity);
                        let fair_value = fair_value_for_state(state);
                        let signal_reference = fair_value
                            .map(|estimate| estimate.price.0)
                            .unwrap_or(state.anchor.close_price_ticks);
                        let fair_value_confidence_bps = fair_value
                            .map(|estimate| estimate.confidence_bps)
                            .unwrap_or(0);
                        let input = threshold.map(|threshold| SignalInput {
                            symbol: state.symbol_id,
                            anchor: crate::strategy::PriceTicks(signal_reference),
                            best_bid: crate::strategy::PriceTicks(book.bid_price_ticks),
                            best_ask: crate::strategy::PriceTicks(book.ask_price_ticks),
                            index_price: crate::strategy::PriceTicks(
                                state.index_price_ticks.unwrap_or(0),
                            ),
                            mark_price: crate::strategy::PriceTicks(
                                state.mark_price_ticks.unwrap_or(0),
                            ),
                            position: state.position,
                            max_position,
                            requested_quantity: quantity,
                            threshold,
                            threshold_relief_micro_bps: state.adaptive_relief_micro_bps,
                            threshold_relief_pico_bps: state.adaptive_relief_pico_bps,
                            inventory_skew_bps: 50,
                            inventory_skew_pico_bps: 50 * PICO_BPS_SCALE,
                            buy_adverse_selection_bps: if strategy_variant.uses_microstructure() {
                                buy_micro_adverse_bps
                            } else {
                                0
                            },
                            sell_adverse_selection_bps: if strategy_variant.uses_microstructure() {
                                sell_micro_adverse_bps
                            } else {
                                0
                            },
                            buy_adverse_selection_pico_bps: if strategy_variant
                                .uses_microstructure()
                            {
                                buy_pico_adverse_bps
                            } else {
                                0
                            },
                            sell_adverse_selection_pico_bps: if strategy_variant
                                .uses_microstructure()
                            {
                                sell_pico_adverse_bps
                            } else {
                                0
                            },
                            fill_probability_bps,
                            confidence_bps: 10_000_i64
                                .saturating_sub(fair_value_confidence_bps.saturating_mul(50))
                                .clamp(1_000, 10_000)
                                .min(if signal_age_ms <= 1_000 { 9_000 } else { 7_000 })
                                as u16,
                            fill_aware: strategy_variant.uses_fill_gate(),
                            max_mark_index_gap_bps: self.max_mark_index_gap_bps,
                            signal_age_ms,
                            max_signal_age_ms: 5_000,
                        });
                        input.and_then(AnchorMakerStrategy::generate_adaptive_intent)
                    };
                    let reason = if intent.is_some() {
                        "admissible"
                    } else if strategy_variant == SimulationPolicyVariant::M7EvidenceGated {
                        "m7_evidence_gate"
                    } else {
                        "signal_below_threshold"
                    };
                    (intent, false, state.working.is_some(), reason)
                }
            }
        };
        if desired.is_none() {
            let outcome = if has_working {
                "cancel_pending"
            } else {
                "rejected"
            };
            let decision_record = {
                let state = self.states.get(symbol).expect("symbol state exists");
                let mut record = self.record(
                    symbol,
                    state,
                    timestamp_ms,
                    RecordFields {
                        kind: "decision",
                        decision_id: Some(decision_id),
                        client_id: None,
                        side: None,
                        price_ticks: None,
                        quantity: None,
                        order_age_ms: None,
                        queue_ahead_quantity: None,
                        quote_distance_bps: None,
                        detail: Some(decision_reason),
                    },
                );
                record.decision_audit = Some(decision_audit(
                    self,
                    symbol,
                    state,
                    timestamp_ms,
                    decision_id,
                    outcome,
                    decision_reason,
                    max_position,
                    requested_quantity,
                ));
                record
            };
            self.reject_entry(decision_reason);
            if has_working {
                let mut records = vec![decision_record];
                records.extend(self.cancel_symbol(
                    symbol,
                    timestamp_ms,
                    "session, signal, or data gate blocked",
                ));
                return records;
            }
            return vec![decision_record];
        }
        let desired = desired.expect("desired intent exists");
        let decision_record = {
            let state = self.states.get(symbol).expect("symbol state exists");
            let outcome = if reduce_only {
                "reduce_only"
            } else {
                "admissible"
            };
            let mut record = self.record(
                symbol,
                state,
                timestamp_ms,
                RecordFields {
                    kind: "decision",
                    decision_id: Some(decision_id),
                    client_id: None,
                    side: Some(desired.side),
                    price_ticks: Some(desired.price),
                    quantity: Some(desired.quantity),
                    order_age_ms: None,
                    queue_ahead_quantity: None,
                    quote_distance_bps: None,
                    detail: Some(decision_reason),
                },
            );
            record.decision_audit = Some(decision_audit(
                self,
                symbol,
                state,
                timestamp_ms,
                decision_id,
                outcome,
                decision_reason,
                max_position,
                requested_quantity,
            ));
            record
        };
        let same_order = self.states[symbol].working.is_some_and(|order| {
            order.side == desired.side
                && order.price_ticks == desired.price
                && order.remaining_quantity >= desired.quantity
                && order.reduce_only == reduce_only
        });
        if same_order {
            return vec![decision_record];
        }
        // Preserve queue priority unless the desired price moved materially.
        // A timer-only reprice needlessly cancels a live maker order and loses
        // its place in queue; a one-bps move is the minimum economic reason to
        // pay that queue-loss cost.
        let hold_existing_quote = has_working
            && !reduce_only
            && self.states[symbol].working.as_ref().is_some_and(|order| {
                order.side == desired.side
                    && order.remaining_quantity >= desired.quantity
                    && (timestamp_ms.saturating_sub(order.placed_at_ms)
                        < self.quote_reprice_min_interval_ms
                        || bps_between(order.price_ticks, desired.price).abs() < 1)
            });
        if hold_existing_quote {
            return vec![decision_record];
        }
        let mut records = vec![decision_record];
        if has_working {
            records.extend(self.cancel_symbol(symbol, timestamp_ms, "quote replacement"));
        }
        if self.states[symbol].working.is_some() {
            return records;
        }
        records.extend(self.place_symbol(
            symbol,
            desired,
            timestamp_ms,
            reduce_only,
            Some(decision_id),
        ));
        records
    }

    fn place_symbol(
        &mut self,
        symbol: &str,
        intent: OrderIntent,
        timestamp_ms: u64,
        reduce_only: bool,
        decision_id: Option<u64>,
    ) -> Vec<SimulationRecord> {
        if !intent.post_only || intent.price <= 0 || intent.quantity <= 0 {
            return Vec::new();
        }
        let client_id = self.next_client_id;
        self.next_client_id = self.next_client_id.saturating_add(1);
        let state = self.states.get_mut(symbol).expect("symbol state exists");
        let Some(book) = state.book else {
            return Vec::new();
        };
        let maker_valid = match intent.side {
            Side::Buy => intent.price <= book.bid_price_ticks,
            Side::Sell => intent.price >= book.ask_price_ticks,
        };
        if !maker_valid {
            self.reject_entry("execution_maker_validation");
            return Vec::new();
        }
        if reduce_only
            && ((intent.side == Side::Buy && state.position >= 0)
                || (intent.side == Side::Sell && state.position <= 0))
        {
            return Vec::new();
        }
        let mid_ticks = book.bid_price_ticks.saturating_add(book.ask_price_ticks) / 2;
        let quote_distance_bps = bps_between(intent.price, mid_ticks);
        let queue_ahead_quantity = if state.local_book.is_valid() {
            state
                .local_book
                .quantity_at(intent.side == Side::Buy, intent.price)
        } else {
            match intent.side {
                Side::Buy => book.bid_quantity.max(0),
                Side::Sell => book.ask_quantity.max(0),
            }
        };
        state.working = Some(WorkingOrder {
            client_id,
            decision_id,
            side: intent.side,
            price_ticks: intent.price,
            remaining_quantity: intent.quantity,
            reduce_only,
            queue_ahead_quantity,
            quote_distance_bps,
            placed_at_ms: timestamp_ms,
            exchange_arrival_at_ms: self
                .last_received_at_ms
                .saturating_add(self.realism.latency.total_entry_ms()),
            cancel_requested_at_ms: None,
        });
        state.calibration.observe_order_placed(timestamp_ms);
        self.order_count = self.order_count.saturating_add(1);
        let state = self.states.get(symbol).expect("symbol state exists");
        vec![self.record(
            symbol,
            state,
            timestamp_ms,
            RecordFields {
                kind: "order_placed",
                decision_id,
                client_id: Some(client_id),
                side: Some(intent.side),
                price_ticks: Some(intent.price),
                quantity: Some(intent.quantity),
                order_age_ms: Some(0),
                queue_ahead_quantity: Some(queue_ahead_quantity),
                quote_distance_bps: Some(quote_distance_bps),
                detail: Some(if reduce_only {
                    "reduce-only maker simulation order"
                } else {
                    "maker-only simulation order"
                }),
            },
        )]
    }

    fn cancel_symbol(
        &mut self,
        symbol: &str,
        timestamp_ms: u64,
        detail: &str,
    ) -> Vec<SimulationRecord> {
        let cancel_latency = self.realism.latency.cancel_to_exchange_ms;
        if cancel_latency > 0 {
            let Some(state) = self.states.get_mut(symbol) else {
                return Vec::new();
            };
            let Some(order) = state.working.as_mut() else {
                return Vec::new();
            };
            if order.cancel_requested_at_ms.is_none() {
                order.cancel_requested_at_ms = Some(self.last_received_at_ms.max(timestamp_ms));
            }
            // The exchange keeps the order live until the cancel reaches it.
            // The next market event will acknowledge it after the configured
            // latency, allowing fills during the in-flight cancel window.
            return Vec::new();
        }
        let canceled = self
            .states
            .get_mut(symbol)
            .and_then(|state| state.working.take());
        let Some(order) = canceled else {
            return Vec::new();
        };
        if let Some(state) = self.states.get_mut(symbol) {
            state
                .calibration
                .observe_order_terminal(timestamp_ms, order.placed_at_ms);
        }
        let state = self.states.get(symbol).expect("symbol state exists");
        vec![self.record(
            symbol,
            state,
            timestamp_ms,
            RecordFields {
                kind: "order_canceled",
                decision_id: order.decision_id,
                client_id: Some(order.client_id),
                side: Some(order.side),
                price_ticks: Some(order.price_ticks),
                quantity: Some(order.remaining_quantity),
                order_age_ms: Some(timestamp_ms.saturating_sub(order.placed_at_ms)),
                queue_ahead_quantity: Some(order.queue_ahead_quantity),
                quote_distance_bps: Some(order.quote_distance_bps),
                detail: Some(detail),
            },
        )]
    }

    fn record(
        &self,
        symbol: &str,
        state: &SimulationSymbolState,
        timestamp_ms: u64,
        fields: RecordFields<'_>,
    ) -> SimulationRecord {
        SimulationRecord {
            timestamp_ms,
            exchange_event_time_ms: self.last_event_at_ms,
            received_at_ms: self.last_received_at_ms,
            decision_id: fields.decision_id,
            strategy_variant: self.strategy_variant.label().to_owned(),
            kind: fields.kind.to_owned(),
            symbol: symbol.to_owned(),
            client_id: fields.client_id,
            side: fields.side.map(side_name),
            price_ticks: fields.price_ticks,
            quantity: fields.quantity,
            order_age_ms: fields.order_age_ms,
            queue_ahead_quantity: fields.queue_ahead_quantity,
            quote_distance_bps: fields.quote_distance_bps,
            position: state.position,
            realized_pnl_ticks: state.realized_pnl_ticks,
            market_pnl_ticks: state.market_pnl_ticks,
            strategy_pnl_ticks: state.strategy_pnl_ticks,
            funding_pnl_ticks: state.funding_pnl_ticks,
            fees_ticks: state.fees_ticks,
            net_pnl_ticks: state
                .market_pnl_ticks
                .saturating_add(state.strategy_pnl_ticks)
                .saturating_add(state.funding_pnl_ticks)
                .saturating_sub(state.fees_ticks),
            detail: fields.detail.map(str::to_owned),
            decision_audit: None,
        }
    }
}

fn decision_audit(
    engine: &SimulationEngine,
    symbol: &str,
    state: &SimulationSymbolState,
    timestamp_ms: u64,
    decision_id: u64,
    outcome: &str,
    final_gate: &str,
    max_position: i64,
    requested_quantity: i64,
) -> DecisionAudit {
    let book = state.book;
    let book_valid = book.is_some_and(|book| {
        book.bid_price_ticks > 0
            && book.ask_price_ticks >= book.bid_price_ticks
            && book.bid_quantity > 0
            && book.ask_quantity > 0
    });
    let mark_index_ok = match (state.mark_price_ticks, state.index_price_ticks) {
        (Some(mark), Some(index)) if mark > 0 && index > 0 => {
            (i128::from(mark) - i128::from(index)).abs() * 10_000
                <= i128::from(engine.max_mark_index_gap_bps.max(0)) * i128::from(index)
        }
        _ => false,
    };
    let mark_age_ms =
        (state.last_mark_time_ms > 0).then(|| timestamp_ms.saturating_sub(state.last_mark_time_ms));
    let signal_fresh = state.last_mark_time_ms > 0
        && timestamp_ms >= state.last_mark_time_ms
        && timestamp_ms.saturating_sub(state.last_mark_time_ms) <= 5_000;
    let anchor_valid = state
        .anchor
        .valid_at(timestamp_ms, engine.max_anchor_age_ms);
    let anchor_age_ms = (state.anchor.observed_at_ms > 0)
        .then(|| timestamp_ms.saturating_sub(state.anchor.observed_at_ms));
    let session_allowed =
        !engine.live_risk_gates || simulation_session_allows_entry(symbol, timestamp_ms);
    let funding_allowed = !engine.live_risk_gates
        || funding_entry_allowed_variant(
            state,
            timestamp_ms,
            engine.strategy_variant,
            engine.fee_ppm,
        );
    let effective_quantity = book
        .map(|book| {
            liquidity_adjusted_quantity(requested_quantity, book.bid_quantity, book.ask_quantity)
        })
        .unwrap_or(requested_quantity);
    let threshold_diagnostic = dynamic_threshold_diagnostic_for(
        state,
        engine.strategy_variant,
        engine.strategy.entry_threshold_bps,
        engine.fee_ppm,
        effective_quantity,
        max_position,
        timestamp_ms,
    );
    let threshold = threshold_diagnostic
        .threshold
        .map(|value| scale_threshold_non_fee(value, engine.threshold_scale_ppm));
    let fair_value = fair_value_for_state(state);
    let liquidity_ratio_bps = book
        .map(|book| liquidity_ratio_bps(effective_quantity, book.bid_quantity, book.ask_quantity));
    let signal_abs_pico_bps = match (book, fair_value) {
        (Some(book), Some(fair_value)) => {
            let mid = book.bid_price_ticks.saturating_add(book.ask_price_ticks) / 2;
            (mid > 0).then(|| bps_between_pico(fair_value.price.0, mid))
        }
        _ => None,
    };
    let position_capacity = state.position.checked_abs().unwrap_or(i64::MAX) < max_position;
    let gates = vec![
        DecisionGateAudit {
            name: "session_calendar".to_owned(),
            passed: session_allowed,
            reason: if session_allowed {
                "session permits entry"
            } else {
                "exchange-clock session gate"
            }
            .to_owned(),
        },
        DecisionGateAudit {
            name: "funding".to_owned(),
            passed: funding_allowed,
            reason: if funding_allowed {
                "funding gate permits entry"
            } else {
                "funding deadline/rate gate"
            }
            .to_owned(),
        },
        DecisionGateAudit {
            name: "market_book".to_owned(),
            passed: book_valid,
            reason: if book_valid {
                "valid bid/ask and positive depth"
            } else {
                "book missing, crossed, or empty"
            }
            .to_owned(),
        },
        DecisionGateAudit {
            name: "mark_index".to_owned(),
            passed: mark_index_ok,
            reason: if mark_index_ok {
                "mark/index complete and within configured gap"
            } else {
                "mark/index missing or beyond configured gap"
            }
            .to_owned(),
        },
        DecisionGateAudit {
            name: "signal_freshness".to_owned(),
            passed: signal_fresh,
            reason: if signal_fresh {
                "exchange-event signal age <= 5000ms"
            } else {
                "exchange-event signal is stale or unavailable"
            }
            .to_owned(),
        },
        DecisionGateAudit {
            name: "anchor".to_owned(),
            passed: anchor_valid,
            reason: if anchor_valid {
                "anchor valid at exchange event time"
            } else {
                "anchor unavailable or expired"
            }
            .to_owned(),
        },
        DecisionGateAudit {
            name: "threshold".to_owned(),
            passed: threshold.is_some(),
            reason: threshold_diagnostic.status.label().to_owned(),
        },
        DecisionGateAudit {
            name: "inventory_capacity".to_owned(),
            passed: position_capacity,
            reason: if position_capacity {
                "position remains below allocation"
            } else {
                "position allocation exhausted"
            }
            .to_owned(),
        },
        DecisionGateAudit {
            name: "policy_terminal".to_owned(),
            passed: outcome == "admissible" || outcome == "reduce_only",
            reason: final_gate.to_owned(),
        },
    ];
    DecisionAudit {
        schema_version: DECISION_AUDIT_SCHEMA_VERSION,
        decision_id,
        exchange_event_time_ms: timestamp_ms,
        received_at_ms: engine.last_received_at_ms,
        outcome: outcome.to_owned(),
        final_gate: final_gate.to_owned(),
        gates,
        book_bid_ticks: book.map(|book| book.bid_price_ticks),
        book_ask_ticks: book.map(|book| book.ask_price_ticks),
        book_bid_quantity: book.map(|book| book.bid_quantity),
        book_ask_quantity: book.map(|book| book.ask_quantity),
        mark_ticks: state.mark_price_ticks,
        index_ticks: state.index_price_ticks,
        anchor_ticks: state.anchor.close_price_ticks,
        position: state.position,
        mark_age_ms,
        anchor_age_ms,
        threshold_status: threshold_diagnostic.status.label().to_owned(),
        threshold_pico_bps: threshold.and_then(|value| value.required_pico_bps()),
        fair_value_ticks: fair_value.map(|estimate| estimate.price.0),
        liquidity_ratio_bps,
        signal_abs_pico_bps,
        adaptive_relief_pico_bps: state.adaptive_relief_pico_bps,
        threshold_components_pico_bps: threshold.map(|value| value.components_pico_bps()),
    }
}

fn m9_intent_for_state(
    state: &SimulationSymbolState,
    timestamp_ms: u64,
    max_position: i64,
    requested_quantity: i64,
    max_mark_index_gap_bps: i64,
    fee_ppm: i64,
) -> (Option<OrderIntent>, bool, &'static str) {
    let Some(book) = state.book else {
        return (None, false, "m9_market_book_unavailable");
    };
    let Some(fair_value) = fair_value_for_state(state) else {
        return (None, false, "m9_fair_value_unavailable");
    };
    let Some(deadline_ms) = funding_flatten_deadline(state.next_funding_time_ms) else {
        return (None, false, "m9_deadline_unavailable");
    };
    let mark = state.mark_price_ticks.unwrap_or(0);
    let index = state.index_price_ticks.unwrap_or(0);
    let mark_index_ok = mark > 0
        && index > 0
        && (i128::from(mark) - i128::from(index)).abs() * 10_000
            <= i128::from(max_mark_index_gap_bps.max(0)) * i128::from(index);
    let data_valid = mark_index_ok
        && data_quality_for(state, timestamp_ms, max_mark_index_gap_bps)
            == DataQualityStatus::Fresh
        && state.anchor.valid_at(timestamp_ms, u64::MAX);
    let funding_valid =
        state.next_funding_time_ms > timestamp_ms && state.latest_funding_rate_e8.is_some();
    let mid = (book.bid_price_ticks + book.ask_price_ticks) / 2;
    // Queue is side-specific: a buy joins the bid queue and a sell joins the ask queue.
    // Using max(bid, ask) here systematically understates M9 fill probability.
    let buy_signal = fair_value.price.0 > mid;
    let queue_ahead = if state.local_book.is_valid() {
        if buy_signal {
            state.local_book.quantity_at(true, book.bid_price_ticks)
        } else {
            state.local_book.quantity_at(false, book.ask_price_ticks)
        }
    } else if buy_signal {
        book.bid_quantity
    } else {
        book.ask_quantity
    };
    let entry_quantity_cap = if buy_signal {
        book.bid_quantity
    } else {
        book.ask_quantity
    };
    let calibration_snapshot = state
        .calibration
        .snapshot(ppm_to_pico_bps(fee_ppm.saturating_mul(2)));
    let Some(calibration) = calibration_snapshot.calibration else {
        return (None, false, "m9_calibration_unavailable");
    };
    let decision = decide_m9(
        M9Input {
            now_ms: timestamp_ms,
            deadline_ms,
            fair_value_ticks: crate::strategy::PriceTicks(fair_value.price.0),
            mid_ticks: crate::strategy::PriceTicks(mid),
            bid_ticks: crate::strategy::PriceTicks(book.bid_price_ticks),
            ask_ticks: crate::strategy::PriceTicks(book.ask_price_ticks),
            mark_ticks: crate::strategy::PriceTicks(mark),
            index_ticks: crate::strategy::PriceTicks(index),
            bid_quantity: book.bid_quantity,
            ask_quantity: book.ask_quantity,
            queue_ahead_quantity: queue_ahead,
            position: state.position,
            max_position,
            requested_quantity: requested_quantity.min(entry_quantity_cap.max(1)),
            volatility_bps: state.ewma_abs_return_bps.saturating_mul(3),
            spread_bps: state.ewma_spread_bps,
            funding_carry_bps: state
                .latest_funding_rate_e8
                .map(|rate| -rate / 10_000)
                .unwrap_or(0),
            fee_bps: ppm_to_bps(fee_ppm.saturating_mul(2)),
            markout_bps: pico_bps_to_bps(state.ewma_adverse_markout_pico_bps),
            volatility_pico_bps: state.ewma_abs_return_pico_bps.saturating_mul(3),
            spread_pico_bps: state.ewma_spread_pico_bps,
            funding_carry_pico_bps: state
                .latest_funding_rate_e8
                .map(|rate| {
                    ((-i128::from(rate) * i128::from(PICO_BPS_SCALE)) / 10_000)
                        .clamp(i128::from(i64::MIN), i128::from(i64::MAX))
                        as i64
                })
                .unwrap_or(0),
            fee_pico_bps: ppm_to_pico_bps(fee_ppm.saturating_mul(2)),
            markout_pico_bps: state.ewma_adverse_markout_pico_bps,
            data_valid,
            funding_valid,
        },
        calibration,
    );
    let reduce_only = matches!(decision.action, M9Action::ReduceBuy | M9Action::ReduceSell);
    let intent = match decision.action {
        M9Action::BuyMaker => Some(OrderIntent {
            symbol: state.symbol_id,
            side: Side::Buy,
            price: book.bid_price_ticks,
            quantity: decision.quantity,
            post_only: true,
        }),
        M9Action::SellMaker => Some(OrderIntent {
            symbol: state.symbol_id,
            side: Side::Sell,
            price: book.ask_price_ticks,
            quantity: decision.quantity,
            post_only: true,
        }),
        M9Action::ReduceBuy => Some(OrderIntent {
            symbol: state.symbol_id,
            side: Side::Buy,
            price: book.bid_price_ticks,
            quantity: decision.quantity,
            post_only: true,
        }),
        M9Action::ReduceSell => Some(OrderIntent {
            symbol: state.symbol_id,
            side: Side::Sell,
            price: book.ask_price_ticks,
            quantity: decision.quantity,
            post_only: true,
        }),
        M9Action::NoAction => None,
    };
    (intent, reduce_only, decision.reason)
}

fn maker_exit_intent_for_state(
    state: &SimulationSymbolState,
    timestamp_ms: u64,
    quantity_scale: u32,
) -> (Option<OrderIntent>, &'static str) {
    let Some(position_quantity) = state.position.checked_abs() else {
        return (None, "maker_exit_position_overflow");
    };
    let Some(book) = state.book else {
        return (None, "maker_exit_invalid_book");
    };
    if position_quantity == 0 {
        return (None, "maker_exit_flat");
    }
    let funding = if state.next_funding_time_ms > timestamp_ms {
        FundingSchedule::new(
            Some(state.next_funding_time_ms),
            Some(8),
            state.latest_funding_rate_e8.map(|rate| rate / 100),
            FundingRateKind::Regular,
            timestamp_ms,
        )
    } else {
        FundingSchedule::new(None, None, None, FundingRateKind::Unknown, timestamp_ms)
    };
    let Some(funding) = funding else {
        return (None, "maker_exit_funding_schedule_invalid");
    };
    let Some(plan) =
        DualFlattenPlan::new(timestamp_ms, None, funding, 30 * 60 * 1_000, 5 * 60 * 1_000)
    else {
        return (None, "maker_exit_plan_invalid");
    };
    let position_quantity = position_quantity.min(i64::MAX);
    let working = match state.working {
        None => ExitWorkingOrder::None,
        Some(order) if order.cancel_requested_at_ms.is_some() => ExitWorkingOrder::Pending,
        Some(order) => ExitWorkingOrder::Confirmed {
            side: order.side,
            price: order.price_ticks,
            remaining: order.remaining_quantity,
            reduce_only: order.reduce_only,
        },
    };
    let decision = decide_maker_exit(MakerExitInput {
        symbol: state.symbol_id,
        position: state.position,
        position_confirmed: true,
        now_ms: timestamp_ms,
        max_book_age_ms: 5_000,
        plan,
        book: Some(ExitBook {
            bid: book.bid_price_ticks,
            ask: book.ask_price_ticks,
            observed_at_ms: state.last_book_event_at_ms,
        }),
        constraints: Some(ExitConstraints {
            min_price: 1,
            max_price: i64::MAX,
            price_tick: 1,
            min_quantity: 1,
            max_quantity: position_quantity,
            quantity_step: 1,
            min_notional: 1,
            quantity_scale,
            observed_at_ms: timestamp_ms,
            max_age_ms: 5_000,
        }),
        working,
    });
    match decision {
        MakerExitDecision::Submit(intent) => (Some(intent), "maker_exit_submit"),
        MakerExitDecision::KeepWorking => (
            state.working.map(|order| OrderIntent {
                symbol: state.symbol_id,
                side: order.side,
                price: order.price_ticks,
                quantity: order.remaining_quantity,
                post_only: true,
            }),
            "maker_exit_keep_working",
        ),
        MakerExitDecision::CancelWorking => (None, "maker_exit_cancel_working"),
        MakerExitDecision::WaitForReconciliation => (None, "maker_exit_wait_reconciliation"),
        MakerExitDecision::ResidualExposure => (None, "maker_exit_hard_deadline"),
        MakerExitDecision::Flat => (None, "maker_exit_flat"),
        MakerExitDecision::Trading => (None, "maker_exit_not_in_window"),
        MakerExitDecision::Blocked(_) => (None, "maker_exit_blocked"),
    }
}

pub fn event_time_ms(event: &BinanceMarketEvent) -> u64 {
    match event {
        BinanceMarketEvent::BookTicker(value) => value.event_time_ms,
        BinanceMarketEvent::MarkPrice(value) => value.event_time_ms,
        BinanceMarketEvent::AggTrade(value) => value.event_time_ms,
        BinanceMarketEvent::DepthUpdate(value) => value.event_time_ms,
    }
}

fn funding_flatten_deadline(next_funding_time_ms: u64) -> Option<u64> {
    (next_funding_time_ms > 0).then(|| next_funding_time_ms.saturating_sub(FUNDING_FLATTEN_LEAD_MS))
}

fn funding_entry_allowed(state: &SimulationSymbolState, now_ms: u64) -> bool {
    state.next_funding_time_ms > now_ms
        && funding_flatten_deadline(state.next_funding_time_ms)
            .is_some_and(|deadline| now_ms < deadline)
}

fn m8_funding_decision(
    state: &SimulationSymbolState,
    now_ms: u64,
    max_position: i64,
    fee_ppm: i64,
) -> crate::m8::M8Decision {
    let Some(mid) = state
        .book
        .map(|book| (book.bid_price_ticks + book.ask_price_ticks) / 2)
    else {
        return crate::m8::decide(crate::m8::M8Input {
            now_ms,
            anchor_ticks: state.anchor.close_price_ticks,
            mid_ticks: 0,
            mark_ticks: state.mark_price_ticks.unwrap_or(0),
            index_ticks: state.index_price_ticks.unwrap_or(0),
            position: state.position,
            max_position: max_position.max(1),
            funding_rate_e8: state.latest_funding_rate_e8,
            next_funding_ms: None,
            funding_rate_status: crate::m8::FundingRateStatus::Missing,
            fee_ppm,
            volatility_bps: 0,
            spread_bps: 0,
            model_uncertainty_bps: 0,
            liquidation_buffer_bps: 5,
            volatility_pico_bps: 0,
            spread_pico_bps: 0,
            model_uncertainty_pico_bps: 0,
            liquidation_buffer_pico_bps: 5 * PICO_BPS_SCALE,
        });
    };
    crate::m8::decide(crate::m8::M8Input {
        now_ms,
        anchor_ticks: state.anchor.close_price_ticks,
        mid_ticks: mid,
        mark_ticks: state.mark_price_ticks.unwrap_or(0),
        index_ticks: state.index_price_ticks.unwrap_or(0),
        position: state.position,
        max_position: max_position.max(1),
        funding_rate_e8: state.latest_funding_rate_e8,
        next_funding_ms: (state.next_funding_time_ms > now_ms)
            .then_some(state.next_funding_time_ms),
        funding_rate_status: if state.latest_funding_rate_e8.is_some() {
            crate::m8::FundingRateStatus::Observed
        } else {
            crate::m8::FundingRateStatus::Missing
        },
        fee_ppm,
        volatility_bps: state.ewma_abs_return_bps.saturating_mul(3),
        spread_bps: state.ewma_spread_bps,
        model_uncertainty_bps: bps_between(
            state.mark_price_ticks.unwrap_or(0),
            state.index_price_ticks.unwrap_or(0),
        ) / 2,
        liquidation_buffer_bps: 5,
        volatility_pico_bps: state.ewma_abs_return_pico_bps.saturating_mul(3),
        spread_pico_bps: state.ewma_spread_pico_bps,
        model_uncertainty_pico_bps: bps_between_pico(
            state.mark_price_ticks.unwrap_or(0),
            state.index_price_ticks.unwrap_or(0),
        ) / 2,
        liquidation_buffer_pico_bps: 5 * PICO_BPS_SCALE,
    })
}

fn funding_entry_allowed_variant(
    state: &SimulationSymbolState,
    now_ms: u64,
    variant: SimulationPolicyVariant,
    fee_ppm: i64,
) -> bool {
    if variant != SimulationPolicyVariant::M8FundingAware {
        return funding_entry_allowed(state, now_ms);
    }
    m8_funding_decision(
        state,
        now_ms,
        state.position.checked_abs().unwrap_or(i64::MAX).max(1),
        fee_ppm,
    )
    .allow_entry
}

fn entry_block_reason_for(
    state: &SimulationSymbolState,
    risk_state: SimulationRiskState,
    threshold: Option<AdaptiveThreshold>,
    threshold_status: ThresholdStatus,
    relief_pico_bps: i64,
    buy_edge_pico_bps: Option<i64>,
    sell_edge_pico_bps: Option<i64>,
) -> &'static str {
    match risk_state {
        SimulationRiskState::ReduceOnlyEquitySession => "equity_session_open",
        SimulationRiskState::ReduceOnlyFundingDeadline => "funding_deadline",
        SimulationRiskState::ReduceOnlyFundingRisk => "funding_cost_exceeds_edge",
        SimulationRiskState::NoEntryFunding => "funding_entry_blocked",
        SimulationRiskState::ReduceOnlyTailRisk => "tail_risk_guard",
        SimulationRiskState::HaltFundingMetadata => "funding_metadata_missing",
        SimulationRiskState::HaltMarketData => "market_data_not_fresh",
        SimulationRiskState::HaltAnchor => "anchor_not_usable",
        SimulationRiskState::Trading => {
            if state.book.is_none() {
                "quote_missing"
            } else {
                let Some(required_pico_bps) = threshold
                    .and_then(AdaptiveThreshold::required_pico_bps)
                    .map(|required| required.saturating_sub(relief_pico_bps.max(0)))
                else {
                    return match threshold_status {
                        ThresholdStatus::WarmingUp => "threshold_warming_up",
                        ThresholdStatus::InsufficientData => "threshold_insufficient_data",
                        ThresholdStatus::InvalidInput => "threshold_invalid_input",
                        ThresholdStatus::ModelFailure => "threshold_model_failure",
                        ThresholdStatus::Ready => "threshold_unavailable",
                    };
                };
                let edge_reaches_threshold = buy_edge_pico_bps
                    .is_some_and(|edge| edge >= required_pico_bps)
                    || sell_edge_pico_bps.is_some_and(|edge| edge >= required_pico_bps);
                if edge_reaches_threshold {
                    "signal_not_admissible"
                } else {
                    "signal_below_threshold"
                }
            }
        }
    }
}

fn data_quality_for(
    state: &SimulationSymbolState,
    now_ms: u64,
    max_mark_index_gap_bps: i64,
) -> DataQualityStatus {
    let Some(book) = state.book else {
        return DataQualityStatus::Missing;
    };
    let (Some(mark), Some(index)) = (state.mark_price_ticks, state.index_price_ticks) else {
        return DataQualityStatus::Missing;
    };
    if book.bid_price_ticks <= 0
        || book.ask_price_ticks < book.bid_price_ticks
        || book.bid_quantity <= 0
        || book.ask_quantity <= 0
        || mark <= 0
        || index <= 0
        || max_mark_index_gap_bps < 0
    {
        return DataQualityStatus::Contradictory;
    }
    let gap = (i128::from(mark) - i128::from(index)).abs() * 10_000;
    if gap > i128::from(max_mark_index_gap_bps) * i128::from(index) {
        return DataQualityStatus::Contradictory;
    }
    // Freshness is evaluated only on the exchange clock. Local receipt time
    // can jump, be adjusted, or belong to another timezone and is therefore
    // retained only for latency diagnostics.
    if state.last_mark_time_ms == 0
        || now_ms < state.last_mark_time_ms
        || now_ms.saturating_sub(state.last_mark_time_ms) > 5_000
    {
        return DataQualityStatus::Stale;
    }
    if state.anchor.close_price_ticks <= 0 {
        return DataQualityStatus::Missing;
    }
    DataQualityStatus::Fresh
}

fn edge_pico_bps(numerator_price: i64, denominator_price: i64) -> Option<i64> {
    if numerator_price <= 0 || denominator_price <= 0 {
        return None;
    }
    Some(
        ((i128::from(numerator_price) - i128::from(denominator_price))
            * 10_000
            * i128::from(PICO_BPS_SCALE)
            / i128::from(denominator_price))
        .clamp(i128::from(i64::MIN), i128::from(i64::MAX)) as i64,
    )
}

/// Compatibility diagnostic; admission uses edge_pico_bps directly.
fn edge_micro_bps(numerator_price: i64, denominator_price: i64) -> Option<i64> {
    edge_pico_bps(numerator_price, denominator_price).map(pico_bps_to_micro)
}

fn edge_bps(numerator_price: i64, denominator_price: i64) -> Option<i64> {
    edge_pico_bps(numerator_price, denominator_price).map(pico_bps_to_bps)
}

fn fair_value_for_state(state: &SimulationSymbolState) -> Option<FairValueEstimate> {
    let book = state.book?;
    let index = state.index_price_ticks?;
    let mark = state.mark_price_ticks?;
    let mid = book.bid_price_ticks.checked_add(book.ask_price_ticks)? / 2;
    FairValueEstimate::from_market_precise(
        crate::strategy::PriceTicks(state.anchor.close_price_ticks),
        crate::strategy::PriceTicks(index),
        crate::strategy::PriceTicks(mark),
        crate::strategy::PriceTicks(mid),
        state.ewma_abs_return_pico_bps,
        state.ewma_spread_pico_bps,
    )
}

fn dynamic_threshold_diagnostic_for(
    state: &SimulationSymbolState,
    variant: SimulationPolicyVariant,
    floor_bps: i64,
    fee_ppm: i64,
    requested_quantity: i64,
    max_position: i64,
    timestamp_ms: u64,
) -> ThresholdDiagnostic {
    let Some(book) = state.book else {
        return ThresholdDiagnostic {
            status: ThresholdStatus::WarmingUp,
            threshold: None,
            prior_used: true,
            missing_component: Some("book"),
        };
    };
    let Some(mark) = state.mark_price_ticks else {
        return ThresholdDiagnostic {
            status: ThresholdStatus::InsufficientData,
            threshold: None,
            prior_used: true,
            missing_component: Some("mark_price"),
        };
    };
    let Some(index) = state.index_price_ticks else {
        return ThresholdDiagnostic {
            status: ThresholdStatus::InsufficientData,
            threshold: None,
            prior_used: true,
            missing_component: Some("index_price"),
        };
    };
    if book.bid_price_ticks <= 0
        || book.ask_price_ticks < book.bid_price_ticks
        || book.bid_quantity <= 0
        || book.ask_quantity <= 0
        || mark <= 0
        || index <= 0
        || floor_bps < 0
        || fee_ppm < 0
        || requested_quantity <= 0
        || max_position <= 0
    {
        return ThresholdDiagnostic {
            status: ThresholdStatus::InvalidInput,
            threshold: None,
            prior_used: false,
            missing_component: Some("validated_components"),
        };
    }
    let gap_pico_bps = edge_pico_bps(mark, index)
        .map(|value| i128::from(value.unsigned_abs()))
        .unwrap_or(i128::from(i64::MAX))
        .min(i128::from(i64::MAX)) as i64;
    let prior_used = variant != SimulationPolicyVariant::M0Fixed
        && (state.ewma_abs_return_pico_bps == 0 || state.ewma_spread_pico_bps == 0);
    let volatility_pico_bps = if state.ewma_abs_return_pico_bps == 0 {
        THRESHOLD_PRIOR_VOLATILITY_PICO_BPS
    } else {
        state.ewma_abs_return_pico_bps.saturating_mul(3)
    };
    let cost_pico_bps = ppm_to_pico_bps(fee_ppm.saturating_mul(2));
    let fair_value_confidence_pico_bps = fair_value_for_state(state)
        .map(|estimate| {
            i128::from(estimate.confidence_bps).saturating_mul(i128::from(PICO_BPS_SCALE))
        })
        .unwrap_or(i128::from(gap_pico_bps))
        .clamp(0, i128::from(i64::MAX)) as i64;
    let confidence_component_pico_bps = 5 * i128::from(PICO_BPS_SCALE)
        + i128::from(fair_value_confidence_pico_bps).min(50 * i128::from(PICO_BPS_SCALE));
    let uncertainty_pico_bps = (i128::from(gap_pico_bps) / 2)
        .max(confidence_component_pico_bps)
        .clamp(0, i128::from(i64::MAX)) as i64;
    let spread_pico_bps = if state.ewma_spread_pico_bps == 0 {
        THRESHOLD_PRIOR_SPREAD_PICO_BPS
    } else {
        state.ewma_spread_pico_bps / 2
    };
    let liquidity_pico_bps =
        liquidity_penalty_pico_bps(requested_quantity, book.bid_quantity, book.ask_quantity);
    let baseline_adverse_selection_pico_bps = if variant.uses_microstructure() {
        state.ewma_abs_return_pico_bps.saturating_mul(2)
    } else {
        0
    };
    let fill_feedback_pico_bps = if variant == SimulationPolicyVariant::M0Fixed {
        0
    } else {
        state.ewma_adverse_markout_pico_bps
    };
    // Favorable markout feedback may reduce the estimated cost, but a cost
    // component must never become negative and invalidate the full model.
    let adverse_selection_pico_bps = baseline_adverse_selection_pico_bps
        .saturating_add(fill_feedback_pico_bps)
        .max(0);
    let statistical_pico_bps = if variant.uses_statistical_term() {
        state.ewma_abs_return_pico_bps.saturating_mul(8)
    } else {
        0
    };
    let tail_risk_pico_bps = if variant.uses_tail_guard() {
        m5_tail_risk_pico(state)
    } else {
        0
    };
    let inventory_pico_bps = if max_position > 0 {
        let ratio_pico = i128::from(state.position).abs() * 10_000 * i128::from(PICO_BPS_SCALE)
            / i128::from(max_position);
        (ratio_pico.saturating_mul(ratio_pico) / (1_000_000_i128 * i128::from(PICO_BPS_SCALE)))
            .clamp(0, i128::from(i64::MAX)) as i64
    } else {
        100 * PICO_BPS_SCALE
    };
    let funding_remaining_ms = state.next_funding_time_ms.saturating_sub(timestamp_ms);
    let deadline_risk_pico_bps =
        if state.next_funding_time_ms > timestamp_ms && state.latest_funding_rate_e8.is_some() {
            if funding_remaining_ms <= 10 * 60 * 1_000 {
                50 * PICO_BPS_SCALE
            } else if funding_remaining_ms <= 30 * 60 * 1_000 {
                25 * PICO_BPS_SCALE
            } else if funding_remaining_ms <= 60 * 60 * 1_000 {
                10 * PICO_BPS_SCALE
            } else {
                0
            }
        } else {
            0
        };
    let floor_pico_bps = i128::from(floor_bps.max(0))
        .saturating_mul(i128::from(PICO_BPS_SCALE))
        .clamp(0, i128::from(i64::MAX)) as i64;
    let threshold = AdaptiveThreshold::from_pico_components(
        floor_pico_bps,
        if variant == SimulationPolicyVariant::M0Fixed {
            0
        } else {
            volatility_pico_bps
        },
        if variant == SimulationPolicyVariant::M0Fixed {
            0
        } else {
            cost_pico_bps
        },
        if variant == SimulationPolicyVariant::M0Fixed {
            0
        } else {
            uncertainty_pico_bps
        },
        if variant == SimulationPolicyVariant::M0Fixed {
            0
        } else {
            deadline_risk_pico_bps
        },
        if variant == SimulationPolicyVariant::M0Fixed {
            0
        } else {
            5 * PICO_BPS_SCALE
        },
        if variant == SimulationPolicyVariant::M0Fixed {
            0
        } else {
            spread_pico_bps
        },
        adverse_selection_pico_bps,
        if variant == SimulationPolicyVariant::M0Fixed {
            0
        } else {
            liquidity_pico_bps
        },
        if variant == SimulationPolicyVariant::M0Fixed {
            0
        } else {
            inventory_pico_bps
        },
        statistical_pico_bps,
        tail_risk_pico_bps,
    );
    ThresholdDiagnostic {
        status: if threshold.is_some() {
            if prior_used {
                ThresholdStatus::WarmingUp
            } else {
                ThresholdStatus::Ready
            }
        } else {
            ThresholdStatus::ModelFailure
        },
        threshold,
        prior_used,
        missing_component: None,
    }
}

fn dynamic_threshold_for(
    state: &SimulationSymbolState,
    variant: SimulationPolicyVariant,
    floor_bps: i64,
    fee_ppm: i64,
    requested_quantity: i64,
    max_position: i64,
    timestamp_ms: u64,
) -> Option<AdaptiveThreshold> {
    dynamic_threshold_diagnostic_for(
        state,
        variant,
        floor_bps,
        fee_ppm,
        requested_quantity,
        max_position,
        timestamp_ms,
    )
    .threshold
}

fn scale_threshold_non_fee(threshold: AdaptiveThreshold, scale_ppm: i64) -> AdaptiveThreshold {
    let scale = |value: i64| {
        (i128::from(value) * i128::from(scale_ppm.clamp(0, 1_000_000)) / 1_000_000)
            .clamp(0, i128::from(i64::MAX)) as i64
    };
    let values = threshold.components_pico_bps();
    AdaptiveThreshold::from_pico_components(
        scale(values[0]),
        scale(values[1]),
        values[2],
        values[3].min(i64::MAX),
        values[4],
        scale(values[5]),
        scale(values[6]),
        scale(values[7]),
        scale(values[8]),
        scale(values[9]),
        scale(values[10]),
        scale(values[11]),
    )
    .expect("scaled threshold components are non-negative")
}

fn threshold_metrics(threshold: AdaptiveThreshold) -> ThresholdMetrics {
    let exact = threshold.components_pico_bps();
    ThresholdMetrics {
        floor_bps: threshold.floor_bps,
        residual_volatility_bps: threshold.residual_volatility_bps,
        cost_bps: threshold.cost_bps,
        uncertainty_bps: threshold.uncertainty_bps,
        deadline_risk_bps: threshold.deadline_risk_bps,
        safety_margin_bps: threshold.safety_margin_bps,
        spread_bps: threshold.spread_bps,
        adverse_selection_bps: threshold.adverse_selection_bps,
        liquidity_bps: threshold.liquidity_bps,
        inventory_bps: threshold.inventory_bps,
        statistical_bps: threshold.statistical_bps,
        tail_risk_bps: threshold.tail_risk_bps,
        floor_pico_bps: exact[0],
        residual_volatility_pico_bps: exact[1],
        cost_pico_bps: exact[2],
        uncertainty_pico_bps: exact[3],
        deadline_risk_pico_bps: exact[4],
        safety_margin_pico_bps: exact[5],
        spread_pico_bps: exact[6],
        adverse_selection_pico_bps: exact[7],
        liquidity_pico_bps: exact[8],
        inventory_pico_bps: exact[9],
        statistical_pico_bps: exact[10],
        tail_risk_pico_bps: exact[11],
        required_bps: threshold.required_bps(),
        required_pico_bps: threshold.required_pico_bps(),
        required_micro_bps: threshold.required_micro_bps(),
    }
}

fn ewma_scaled(previous: i64, sample: i64) -> i64 {
    if previous <= 0 {
        sample.max(0)
    } else {
        ((i128::from(previous) * i128::from(EWMA_PREVIOUS_WEIGHT_PPM)
            + i128::from(sample.max(0)) * i128::from(EWMA_SAMPLE_WEIGHT_PPM))
            / i128::from(1_000_000_i64))
        .clamp(0, i128::from(i64::MAX)) as i64
    }
}

fn ewma_micro(previous: i64, sample: i64) -> i64 {
    ewma_scaled(previous, sample)
}

fn pico_bps_to_micro(value: i64) -> i64 {
    if value == 0 {
        return 0;
    }
    let magnitude = i128::from(value.unsigned_abs());
    let rounded = ((magnitude + i128::from(PICO_BPS_SCALE / MICRO_BPS_SCALE / 2))
        / i128::from(PICO_BPS_SCALE / MICRO_BPS_SCALE))
    .clamp(0, i128::from(i64::MAX)) as i64;
    if value >= 0 {
        rounded
    } else {
        rounded.saturating_neg()
    }
}

fn pico_bps_to_bps(value: i64) -> i64 {
    if value == 0 {
        return 0;
    }
    let magnitude = i128::from(value.unsigned_abs());
    let rounded = ((magnitude + i128::from(PICO_BPS_SCALE / 2)) / i128::from(PICO_BPS_SCALE))
        .clamp(0, i128::from(i64::MAX)) as i64;
    if value >= 0 {
        rounded
    } else {
        rounded.saturating_neg()
    }
}

fn micro_bps_to_bps(value: i64) -> i64 {
    if value == 0 {
        return 0;
    }
    let magnitude = i128::from(value.unsigned_abs());
    let rounded = ((magnitude + i128::from(MICRO_BPS_SCALE / 2)) / i128::from(MICRO_BPS_SCALE))
        .clamp(0, i128::from(i64::MAX)) as i64;
    if value >= 0 {
        rounded
    } else {
        rounded.saturating_neg()
    }
}

fn ppm_to_pico_bps(ppm: i64) -> i64 {
    if ppm <= 0 {
        return 0;
    }
    (i128::from(ppm) * i128::from(PICO_BPS_SCALE) / 100).clamp(0, i128::from(i64::MAX)) as i64
}

fn bps_between(left: i64, right: i64) -> i64 {
    edge_pico_bps(left, right)
        .map(pico_bps_to_bps)
        .unwrap_or(i64::MAX)
}

fn bps_between_pico(left: i64, right: i64) -> i64 {
    edge_pico_bps(left, right)
        .map(|value| i128::from(value.unsigned_abs()).min(i128::from(i64::MAX)) as i64)
        .unwrap_or(i64::MAX)
}

const M5_TAIL_CAUTION_BPS: i64 = 35;
const M5_TAIL_REDUCE_ONLY_BPS: i64 = 60;
const M5_TAIL_HALT_BPS: i64 = 100;

fn m5_tail_stress_pico(state: &SimulationSymbolState) -> i64 {
    let volatility = state.ewma_abs_return_pico_bps.saturating_mul(4);
    let mark_index = match (state.mark_price_ticks, state.index_price_ticks) {
        (Some(mark), Some(index)) => bps_between_pico(mark, index).saturating_mul(2),
        _ => i64::MAX,
    };
    let spread = state.ewma_spread_pico_bps.saturating_mul(4);
    volatility.max(mark_index).max(spread)
}

fn m5_tail_stress_bps(state: &SimulationSymbolState) -> i64 {
    pico_bps_to_bps(m5_tail_stress_pico(state))
}

fn m5_tail_risk_pico(state: &SimulationSymbolState) -> i64 {
    // Keep the tail premium finite. A halted quote is a risk decision, not an
    // arithmetic failure; the signal remains explainable as a large hurdle.
    m5_tail_stress_pico(state)
        .saturating_sub(M5_TAIL_CAUTION_BPS.saturating_mul(PICO_BPS_SCALE))
        .max(0)
        .saturating_mul(2)
        .min(10_000 * PICO_BPS_SCALE)
}

fn m5_tail_risk_bps(state: &SimulationSymbolState) -> i64 {
    pico_bps_to_bps(m5_tail_risk_pico(state))
}

fn m5_quote_quantity(state: &SimulationSymbolState, requested_quantity: i64) -> i64 {
    match m5_tail_stress_bps(state) {
        stress if stress >= M5_TAIL_HALT_BPS => 0,
        stress if stress >= M5_TAIL_REDUCE_ONLY_BPS => requested_quantity / 4,
        stress if stress >= M5_TAIL_CAUTION_BPS => requested_quantity / 2,
        _ => requested_quantity,
    }
}

fn m5_tail_reduce_only(state: &SimulationSymbolState) -> bool {
    m5_tail_stress_bps(state) >= M5_TAIL_REDUCE_ONLY_BPS
}

fn m7_entry_admissible(state: &SimulationSymbolState, threshold_pico_bps: i64) -> bool {
    let Some(book) = state.book else {
        return false;
    };
    let mid = book.bid_price_ticks.saturating_add(book.ask_price_ticks) / 2;
    let fair_value_ticks = fair_value_for_state(state)
        .map(|estimate| estimate.price.0)
        .unwrap_or(0);
    if mid <= 0 || fair_value_ticks <= 0 || threshold_pico_bps <= 0 {
        return false;
    }
    let residual_pico_bps = bps_between_pico(mid, fair_value_ticks);
    // M7 treats very large residuals under elevated stress as repricing,
    // not a free mean-reversion edge, while preserving ordinary opportunities.
    residual_pico_bps >= threshold_pico_bps
        && !(residual_pico_bps >= 500 * PICO_BPS_SCALE
            && m5_tail_stress_pico(state) >= M5_TAIL_CAUTION_BPS * PICO_BPS_SCALE)
}

fn ppm_to_bps(ppm: i64) -> i64 {
    if ppm <= 0 {
        return 0;
    }
    ((i128::from(ppm) + 99) / 100).clamp(0, i128::from(i64::MAX)) as i64
}

fn liquidity_adjusted_quantity(
    requested_quantity: i64,
    bid_quantity: i64,
    ask_quantity: i64,
) -> i64 {
    if requested_quantity <= 0 || bid_quantity <= 0 || ask_quantity <= 0 {
        return 0;
    }
    let depth = bid_quantity.min(ask_quantity);
    // Limit participation to 10% of the thinner side. Consuming an entire
    // displayed level is not a realistic passive execution assumption and
    // would make the liquidity penalty dominate the economic hurdle.
    // Synthetic unit-test books use tiny integer quantities; preserve their
    // exact fill semantics instead of collapsing a 10% cap to one unit.
    if depth < 10_000 {
        return requested_quantity.min(depth).max(1);
    }
    let participation_cap =
        (i128::from(depth) * 1_000 / 10_000).clamp(1, i128::from(i64::MAX)) as i64;
    requested_quantity.min(participation_cap).max(1)
}

fn liquidity_ratio_pico_bps(quantity: i64, bid_quantity: i64, ask_quantity: i64) -> i64 {
    if quantity <= 0 || bid_quantity <= 0 || ask_quantity <= 0 {
        return 10_000 * PICO_BPS_SCALE;
    }
    let depth = bid_quantity.min(ask_quantity);
    (i128::from(quantity).max(0) * 10_000 * i128::from(PICO_BPS_SCALE) / i128::from(depth))
        .clamp(0, 10_000 * i128::from(PICO_BPS_SCALE)) as i64
}

fn liquidity_ratio_bps(quantity: i64, bid_quantity: i64, ask_quantity: i64) -> i64 {
    pico_bps_to_bps(liquidity_ratio_pico_bps(
        quantity,
        bid_quantity,
        ask_quantity,
    ))
}

fn liquidity_penalty_pico_bps(quantity: i64, bid_quantity: i64, ask_quantity: i64) -> i64 {
    let participation_pico_bps = liquidity_ratio_pico_bps(quantity, bid_quantity, ask_quantity);
    if participation_pico_bps <= 1_000 * PICO_BPS_SCALE {
        0
    } else {
        ((participation_pico_bps - 1_000 * PICO_BPS_SCALE) * 6 / 1_000)
            .clamp(0, 100 * PICO_BPS_SCALE)
    }
}

fn liquidity_penalty_bps(quantity: i64, bid_quantity: i64, ask_quantity: i64) -> i64 {
    pico_bps_to_bps(liquidity_penalty_pico_bps(
        quantity,
        bid_quantity,
        ask_quantity,
    ))
}

fn fill_probability_bps(quantity: i64, bid_quantity: i64, ask_quantity: i64) -> u16 {
    if quantity <= 0 || bid_quantity <= 0 || ask_quantity <= 0 {
        return 0;
    }
    let participation_bps = liquidity_ratio_bps(quantity, bid_quantity, ask_quantity);
    // This is an explicitly conservative top-of-book proxy. It is not claimed
    // to be a calibrated fill hazard until completed fills are observed.
    (10_000_i64 - participation_bps * 8 / 10).clamp(500, 9_500) as u16
}

fn local_day(timestamp_ms: u64) -> u64 {
    (timestamp_ms / 1_000 + 8 * 3_600) / 86_400
}

fn local_weekday(timestamp_ms: u64) -> u8 {
    ((local_day(timestamp_ms) + 3) % 7 + 1) as u8
}

fn local_minute(timestamp_ms: u64) -> u16 {
    let local_seconds = timestamp_ms / 1_000 + 8 * 3_600;
    (local_seconds % 86_400 / 60) as u16
}

fn calendar_state_for(symbol: &str, timestamp_ms: u64) -> &'static str {
    let Some(profile) = profile_for(symbol) else {
        return "unknown_symbol";
    };
    let calendar = calendar_for(profile.region);
    let date_key = EquitySessionCalendar::date_key_from_timestamp(timestamp_ms);
    if !EquitySessionCalendar::calendar_snapshot_supported(date_key) {
        return "unsupported_snapshot";
    }
    if calendar.is_holiday(date_key) {
        return "holiday";
    }
    let weekday = local_weekday(timestamp_ms);
    if weekday > 5 {
        return "weekend";
    }
    let minute = local_minute(timestamp_ms);
    if calendar.after_final_close(date_key, weekday, minute) {
        return "final_close_anchor_window";
    }
    match calendar.detailed_state_at(weekday, minute, false, 30, true) {
        VenueSessionState::Closed => "closed",
        VenueSessionState::PreOpenFlatten => "pre_open_flatten",
        VenueSessionState::PreOpenAuction => "pre_open_auction",
        VenueSessionState::Open => "open",
        VenueSessionState::MiddayBreak => "midday_break",
        VenueSessionState::ClosingAuction => "closing_auction",
        VenueSessionState::Weekend => "weekend",
        VenueSessionState::Holiday => "holiday",
        VenueSessionState::Unknown => "unknown",
    }
}

fn simulation_anchor_usable(symbol: &str, anchor_observed_at_ms: u64, now_ms: u64) -> bool {
    if anchor_observed_at_ms == 0 || !anchor_reference_allowed(symbol, anchor_observed_at_ms) {
        return false;
    }
    let Some(profile) = profile_for(symbol) else {
        return false;
    };
    let calendar = calendar_for(profile.region);
    let anchor_day = local_day(anchor_observed_at_ms);
    let current_day = local_day(now_ms);
    if anchor_day > current_day {
        return false;
    }

    let mut day = anchor_day.saturating_add(1);
    while day <= current_day {
        let day_start_ms = day.saturating_mul(86_400_000).saturating_sub(8 * 3_600_000);
        let date_key = EquitySessionCalendar::date_key_from_timestamp(day_start_ms);
        if !EquitySessionCalendar::calendar_snapshot_supported(date_key) {
            return false;
        }
        let weekday = ((day + 3) % 7 + 1) as u8;
        if weekday <= 5 && !calendar.is_holiday(date_key) {
            let day_end_ms = day
                .saturating_add(1)
                .saturating_mul(86_400_000)
                .saturating_sub(8 * 3_600_000)
                .saturating_sub(1);
            let finalized = if day == current_day {
                calendar.after_final_close(date_key, weekday, local_minute(now_ms))
            } else {
                anchor_refresh_allowed(symbol, day_end_ms)
            };
            if finalized {
                return false;
            }
        }
        day = day.saturating_add(1);
    }
    true
}

fn anchor_refresh_allowed(symbol: &str, timestamp_ms: u64) -> bool {
    let Some(profile) = profile_for(symbol) else {
        return false;
    };
    let weekday = local_weekday(timestamp_ms);
    let minute = local_minute(timestamp_ms);
    let calendar = calendar_for(profile.region);
    let date_key = EquitySessionCalendar::date_key_from_timestamp(timestamp_ms);
    if !EquitySessionCalendar::calendar_snapshot_supported(date_key) {
        return false;
    }

    // During weekends and exchange holidays Binance's TradFi index is carried
    // forward from the last completed equity close. Treat that observation as
    // the immutable closed-session anchor; do not refresh it intra-session.
    if weekday > 5 || calendar.is_holiday(date_key) {
        return true;
    }
    if calendar.after_final_close(date_key, weekday, minute) {
        return true;
    }

    // A simulation run may start during the overnight/pre-open window. In that
    // case the current timestamp belongs to the next local date, while the
    // usable anchor is the most recent prior trading day's final close.
    // Resolve that prior close explicitly instead of rejecting a valid
    // restart merely because the process started after midnight.
    if minute < 540 {
        let current_day = local_day(timestamp_ms);
        for offset in 1..=7 {
            let prior_day = current_day.saturating_sub(offset);
            let prior_day_start_ms = prior_day
                .saturating_mul(86_400_000)
                .saturating_sub(8 * 3_600_000);
            let prior_date_key = EquitySessionCalendar::date_key_from_timestamp(prior_day_start_ms);
            let prior_weekday = ((prior_day + 3) % 7 + 1) as u8;
            if calendar.after_final_close(prior_date_key, prior_weekday, 1_439) {
                return true;
            }
        }
    }
    false
}

fn anchor_reference_allowed(symbol: &str, timestamp_ms: u64) -> bool {
    if anchor_refresh_allowed(symbol, timestamp_ms) {
        return true;
    }
    let Some(profile) = profile_for(symbol) else {
        return false;
    };
    let weekday = local_weekday(timestamp_ms);
    let minute = local_minute(timestamp_ms);
    let calendar = calendar_for(profile.region);
    let date_key = EquitySessionCalendar::date_key_from_timestamp(timestamp_ms);
    EquitySessionCalendar::calendar_snapshot_supported(date_key)
        && weekday <= 5
        && !calendar.is_holiday(date_key)
        && matches!(
            calendar.detailed_state_at(weekday, minute, false, 30, true),
            VenueSessionState::MiddayBreak
        )
}

fn simulation_session_allows_entry(symbol: &str, timestamp_ms: u64) -> bool {
    let Some(profile) = profile_for(symbol) else {
        return false;
    };
    let weekday = local_weekday(timestamp_ms);
    let minute = local_minute(timestamp_ms);
    let calendar = calendar_for(profile.region);
    let date_key = EquitySessionCalendar::date_key_from_timestamp(timestamp_ms);

    // Unit-level replay fixtures use small synthetic timestamps rather than
    // real epoch milliseconds; keep their deterministic closed-session path.
    if timestamp_ms < 10_000_000_000 {
        return matches!(
            calendar.detailed_state_at(weekday, minute, false, 30, true),
            VenueSessionState::Closed | VenueSessionState::MiddayBreak
        );
    }
    if !EquitySessionCalendar::calendar_snapshot_supported(date_key) {
        return false;
    }

    // The underlying equity venue is closed on weekends and holidays while
    // Binance perpetuals remain live. Those are valid static-anchor windows;
    // the generic calendar helper intentionally reports them as non-entry
    // states for ordinary equity execution, so this strategy handles them
    // explicitly.
    if weekday > 5 || calendar.is_holiday(date_key) {
        return true;
    }

    matches!(
        calendar.detailed_state_at(weekday, minute, false, 30, true),
        VenueSessionState::Closed | VenueSessionState::MiddayBreak
    )
}

fn residual_pico_bps_for_state(state: &SimulationSymbolState) -> Option<i64> {
    let book = state.book?;
    let fair_value = fair_value_for_state(state)?;
    let mid = clamp_i128((i128::from(book.bid_price_ticks) + i128::from(book.ask_price_ticks)) / 2);
    (mid > 0).then(|| bps_between_pico(fair_value.price.0, mid))
}

fn update_markout_feedback(
    state: &mut SimulationSymbolState,
    mark_price_ticks: i64,
    timestamp_ms: u64,
) {
    if mark_price_ticks <= 0 {
        return;
    }
    while let Some(observation) = state.pending_markouts.front().copied() {
        if timestamp_ms < observation.due_at_ms {
            break;
        }
        state.pending_markouts.pop_front();
        let adverse_ticks = match observation.side {
            Side::Buy => i128::from(observation.fill_price_ticks) - i128::from(mark_price_ticks),
            Side::Sell => i128::from(mark_price_ticks) - i128::from(observation.fill_price_ticks),
        };
        let sample_micro_bps = if adverse_ticks > 0 && observation.fill_price_ticks > 0 {
            (adverse_ticks * 10_000 * i128::from(MICRO_BPS_SCALE)
                / i128::from(observation.fill_price_ticks))
            .clamp(0, i128::from(i64::MAX)) as i64
        } else {
            0
        };
        let sample_pico_bps = (i128::from(sample_micro_bps)
            * i128::from(PICO_BPS_SCALE / MICRO_BPS_SCALE))
        .clamp(0, i128::from(i64::MAX)) as i64;
        state
            .calibration
            .observe_markout(timestamp_ms, sample_pico_bps);
        state.ewma_adverse_markout_micro_bps =
            ewma_micro(state.ewma_adverse_markout_micro_bps, sample_micro_bps);
        state.evaluated_markouts = state.evaluated_markouts.saturating_add(1);
        if sample_micro_bps > 0 {
            state.adverse_markouts = state.adverse_markouts.saturating_add(1);
        }
    }
}

fn apply_position_fill(
    state: &mut SimulationSymbolState,
    side: Side,
    price_ticks: i64,
    quantity: i64,
    fee_ppm: i64,
    quantity_scale: u32,
) {
    let delta = match side {
        Side::Buy => quantity,
        Side::Sell => -quantity,
    };
    let execution_alpha = state.mark_price_ticks.map(|mark| match side {
        Side::Buy => i128::from(mark) - i128::from(price_ticks),
        Side::Sell => i128::from(price_ticks) - i128::from(mark),
    });
    if let Some(alpha) = execution_alpha {
        let alpha_ticks = alpha * i128::from(quantity) / quantity_scale_multiplier(quantity_scale);
        let alpha_ticks = clamp_i128(alpha_ticks);
        state.strategy_pnl_ticks = state.strategy_pnl_ticks.saturating_add(alpha_ticks);
        if alpha_ticks > 0 {
            state.winning_fills = state.winning_fills.saturating_add(1);
        } else if alpha_ticks < 0 {
            state.losing_fills = state.losing_fills.saturating_add(1);
        }
    }
    state.fills = state.fills.saturating_add(1);
    let old_position = state.position;
    let same_direction =
        old_position == 0 || (old_position > 0 && delta > 0) || (old_position < 0 && delta < 0);
    if same_direction {
        let old_abs = i128::from(old_position).abs();
        let delta_abs = i128::from(delta).abs();
        let total = old_abs + delta_abs;
        let weighted = (old_abs * i128::from(state.average_entry_ticks)
            + delta_abs * i128::from(price_ticks))
            / total.max(1);
        state.average_entry_ticks = clamp_i128(weighted);
    } else {
        let close_quantity = i128::from(old_position).abs().min(i128::from(delta).abs());
        let pnl_per_unit = if old_position > 0 {
            i128::from(price_ticks) - i128::from(state.average_entry_ticks)
        } else {
            i128::from(state.average_entry_ticks) - i128::from(price_ticks)
        };
        state.realized_pnl_ticks = state.realized_pnl_ticks.saturating_add(clamp_i128(
            pnl_per_unit * close_quantity / quantity_scale_multiplier(quantity_scale),
        ));
        if i128::from(delta).abs() > close_quantity {
            state.average_entry_ticks = price_ticks;
        } else if i128::from(old_position) + i128::from(delta) == 0 {
            state.average_entry_ticks = 0;
        }
    }
    state.position = clamp_i128(i128::from(old_position) + i128::from(delta));
    let notional = i128::from(price_ticks).abs() * i128::from(quantity).abs();
    state.fees_ticks = state.fees_ticks.saturating_add(clamp_i128(
        notional * i128::from(fee_ppm) / 1_000_000 / quantity_scale_multiplier(quantity_scale),
    ));
}

fn quantity_scale_multiplier(quantity_scale: u32) -> i128 {
    10_i128.pow(quantity_scale)
}

fn unrealized_pnl(state: &SimulationSymbolState, quantity_scale: u32) -> Option<i64> {
    if state.position == 0 {
        return Some(0);
    }
    let mark_price_ticks = i128::from(state.mark_price_ticks?);
    let entry_price_ticks = i128::from(state.average_entry_ticks);
    let position = i128::from(state.position);
    let pnl_per_unit = if position > 0 {
        mark_price_ticks - entry_price_ticks
    } else {
        entry_price_ticks - mark_price_ticks
    };
    Some(clamp_i128(
        pnl_per_unit * position.abs() / quantity_scale_multiplier(quantity_scale),
    ))
}

fn clamp_i128(value: i128) -> i64 {
    value.clamp(i128::from(i64::MIN), i128::from(i64::MAX)) as i64
}

fn side_name(side: Side) -> String {
    match side {
        Side::Buy => "BUY".to_owned(),
        Side::Sell => "SELL".to_owned(),
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

#[derive(Debug, Clone)]
pub struct SimulationConfig {
    pub environment: BinanceEnvironment,
    pub strategy_variant: SimulationPolicyVariant,
    pub threshold_scale_ppm: i64,
    pub symbols: Vec<String>,
    pub price_scale: u32,
    pub quantity_scale: u32,
    pub position_allocations: Option<BTreeMap<String, PositionAllocation>>,
    pub max_subscriptions_per_shard: usize,
    pub connect_timeout_ms: u64,
    pub read_timeout_ms: u64,
    pub duration_secs: u64,
    pub index_anchor_refresh_ms: u64,
    pub http_proxy: Option<String>,
    pub market_output_path: Option<PathBuf>,
    pub fx_output_path: Option<PathBuf>,
    pub metrics_output_path: Option<PathBuf>,
    pub metrics_refresh_ms: u64,
    pub fx_refresh_ms: u64,
    pub fx_max_age_ms: u64,
    pub quote_reprice_min_interval_ms: u64,
}

#[derive(Debug, Clone)]
pub struct SimulationResult {
    pub summary: SimulationSummary,
    pub records_written: u64,
    pub records_dropped: u64,
    pub market_records_written: u64,
    pub market_records_dropped: u64,
    pub fx_records_written: u64,
    pub fx_records_dropped: u64,
    pub fx_last_update_at_ms: u64,
    pub fx_fresh_at_end: bool,
    pub stopped_by_duration: bool,
}

// The explicit arguments keep the simulation run's strategy assumptions visible at the call site.
#[allow(clippy::too_many_arguments)]
pub async fn run_simulation(
    config: SimulationConfig,
    anchors: BTreeMap<String, AnchorSnapshot>,
    entry_threshold_bps: i64,
    max_position: i64,
    requested_quantity: i64,
    max_mark_index_gap_bps: i64,
    max_anchor_age_ms: u64,
    fee_ppm: i64,
    output_path: Option<PathBuf>,
) -> Result<SimulationResult, SimulationError> {
    if config.symbols.is_empty() {
        return Err(SimulationError::InvalidConfig("symbols are required"));
    }
    // Zero is the explicit continuous-simulation mode. The long timeout keeps the
    // existing cleanup/reporting path while making the process effectively
    // unbounded until an operator stops it or a feed fails.
    let run_duration = if config.duration_secs == 0 {
        Duration::from_secs(10_000 * 365 * 24 * 60 * 60)
    } else {
        Duration::from_secs(config.duration_secs)
    };
    if config
        .symbols
        .iter()
        .any(|symbol| instrument_for(symbol).is_none())
    {
        return Err(SimulationError::InvalidConfig(
            "symbols must be selected execution-universe TradFi instruments",
        ));
    }
    let mut fx_currencies = Vec::new();
    for symbol in &config.symbols {
        let currency = profile_for(symbol)
            .map(|profile| profile.anchor_currency)
            .ok_or(SimulationError::InvalidConfig("missing instrument profile"))?;
        if !fx_currencies.contains(&currency) {
            fx_currencies.push(currency);
        }
    }
    let fx_client = BinanceC2cFxClient::new(config.http_proxy.as_deref())
        .map_err(|error| SimulationError::Market(format!("FX client: {error}")))?;
    let fx_poller = BinanceC2cFxPoller::new(
        fx_client,
        &fx_currencies,
        FxPollerConfig {
            refresh_interval_ms: config.fx_refresh_ms,
            max_stale_ms: config.fx_max_age_ms,
            max_backoff_ms: FxPollerConfig::high_frequency().max_backoff_ms,
        },
    )
    .map_err(|error| SimulationError::Market(format!("FX poller: {error}")))?;
    let endpoints = config.environment.endpoints();
    let mut shard_configs = BinanceMarketConfig::for_symbols(
        endpoints.public_market_ws_base,
        &config.symbols,
        BinanceMarketFeed::BookTicker,
        config.price_scale,
        config.quantity_scale,
        1_048_576,
        config.connect_timeout_ms,
        config.read_timeout_ms,
        config.http_proxy.clone(),
        ReconnectPolicy::default(),
        config.max_subscriptions_per_shard,
    )
    .map_err(|error| SimulationError::Market(error.to_string()))?;
    shard_configs.extend(
        BinanceMarketConfig::for_symbols(
            endpoints.market_ws_base,
            &config.symbols,
            BinanceMarketFeed::ReferenceAndTrades,
            config.price_scale,
            config.quantity_scale,
            1_048_576,
            config.connect_timeout_ms,
            config.read_timeout_ms,
            config.http_proxy.clone(),
            ReconnectPolicy::default(),
            config.max_subscriptions_per_shard,
        )
        .map_err(|error| SimulationError::Market(error.to_string()))?,
    );
    let mut engine = SimulationEngine::new(
        anchors,
        entry_threshold_bps,
        max_position,
        requested_quantity,
        max_mark_index_gap_bps,
        max_anchor_age_ms,
        fee_ppm,
        config.quantity_scale,
    )?
    .with_price_scale(config.price_scale)
    .with_live_risk_gates()
    .with_strategy_variant(config.strategy_variant)
    .with_quote_reprice_min_interval_ms(config.quote_reprice_min_interval_ms)
    .with_threshold_scale_ppm(config.threshold_scale_ppm);
    if let Some(allocations) = config.position_allocations.clone() {
        engine = engine.with_position_allocations(allocations)?;
    }
    let AsyncLineWriter {
        sender: record_tx,
        task: record_writer,
        written,
        dropped,
    } = spawn_line_writer(output_path, 4_096, 1 << 20, 64).await;
    let AsyncLineWriter {
        sender: market_tx,
        task: market_writer,
        written: market_written,
        dropped: market_dropped,
    } = spawn_line_writer(config.market_output_path.clone(), 4_096, 1 << 20, 64).await;
    let AsyncLineWriter {
        sender: fx_record_tx,
        task: fx_record_writer,
        written: fx_written,
        dropped: fx_dropped,
    } = spawn_line_writer(config.fx_output_path.clone(), 4_096, 1 << 20, 64).await;
    let metrics_output_path = config.metrics_output_path.clone();
    let mut metrics_interval =
        tokio::time::interval(Duration::from_millis(config.metrics_refresh_ms.max(250)));
    let mut last_received_at_ms = 0_u64;
    let mut event_sequence = 0_u64;
    let (fx_tx, mut fx_rx) = mpsc::channel::<FxUpdate>(128);
    let mut fx_task = tokio::spawn(fx_poller.run(fx_tx));
    let (anchor_tx, mut anchor_rx) = mpsc::channel::<BTreeMap<String, AnchorSnapshot>>(1);
    let mut anchor_task = if config.index_anchor_refresh_ms > 0 {
        let environment = config.environment;
        let symbols = config.symbols.clone();
        let price_scale = config.price_scale;
        let http_proxy = config.http_proxy.clone();
        let refresh_ms = config.index_anchor_refresh_ms;
        Some(tokio::spawn(async move {
            loop {
                tokio::time::sleep(Duration::from_millis(refresh_ms.max(1_000))).await;
                if let Ok(anchor_set) = load_index_anchor_set_internal(
                    environment,
                    &symbols,
                    price_scale,
                    http_proxy.as_deref(),
                )
                .await
                {
                    if anchor_tx.send(anchor_set.anchors).await.is_err() {
                        break;
                    }
                }
            }
        }))
    } else {
        None
    };
    let (event_tx, mut event_rx) = mpsc::unbounded_channel::<BinanceMarketEvent>();
    let event_dropped = Arc::new(AtomicU64::new(0));
    let mut shard_tasks = tokio::task::JoinSet::new();
    for shard_config in shard_configs {
        let event_tx = event_tx.clone();
        let event_dropped = Arc::clone(&event_dropped);
        shard_tasks.spawn(async move {
            BinanceMarketStream::run_forever(shard_config, |event| {
                if event_tx.send(event).is_err() {
                    event_dropped.fetch_add(1, Ordering::Relaxed);
                }
            })
            .await;
        });
    }
    drop(event_tx);

    let price_scale = config.price_scale;
    let quantity_scale = config.quantity_scale;
    let mut fx_latest_by_currency = BTreeMap::<String, FxUpdate>::new();
    let mut fx_last_update_at_ms = 0_u64;
    let mut performance_history = VecDeque::with_capacity(900);
    let run_result = tokio::time::timeout(run_duration, async {
        loop {
            tokio::select! {
                event = event_rx.recv() => {
                    let Some(event) = event else {
                        return Err(SimulationError::Market(
                            "all market shards stopped".to_owned(),
                        ));
                    };
                    let received_at_ms = now_ms();
                    last_received_at_ms = received_at_ms;
                    event_sequence = event_sequence.saturating_add(1);
                    let event_envelope = EventEnvelope {
                        event_id: format!("simulation-{event_sequence}").into(),
                        run_id: "simulation".into(),
                        causality_id: format!("simulation-cause-{event_sequence}").into(),
                        source: EventSource::Simulation,
                        observed_at_ms: event_time_ms(&event),
                        received_at_ms,
                        sequence: event_sequence,
                        state_version: event_sequence,
                        quality: DataQuality::Trusted,
                        payload: event.clone(),
                    };
                    let mut market_value = market_event_to_json(
                        &event,
                        price_scale,
                        quantity_scale,
                        Some(received_at_ms),
                    );
                    add_event_lineage(&mut market_value, &event_envelope);
                    let market_line = serde_json::to_string(&market_value)
                        .unwrap_or_else(|_| "{}".to_owned());
                    market_tx
                        .send(market_line)
                        .await
                        .map_err(|_| SimulationError::Io("market writer stopped".to_owned()))?;
                    for record in engine.on_enveloped_event(&event_envelope)? {
                        let line = serde_json::to_string(&record)
                            .unwrap_or_else(|_| "{}".to_owned());
                        record_tx
                            .send(line)
                            .await
                            .map_err(|_| SimulationError::Io("record writer stopped".to_owned()))?;
                    }
                }
                _ = metrics_interval.tick(), if metrics_output_path.is_some() => {
                    if let Some(path) = metrics_output_path.as_deref() {
                        let observed_at_ms = now_ms();
                        performance_history.push_back(engine.performance_point(observed_at_ms));
                        while performance_history.len() > 900 {
                            performance_history.pop_front();
                        }
                        write_json_atomic(
                            path,
                            &engine.metrics_snapshot_with_history(
                                observed_at_ms,
                                last_received_at_ms,
                                performance_history.make_contiguous(),
                            ),
                        ).await?;
                    }
                }
                anchor_update = anchor_rx.recv(), if config.index_anchor_refresh_ms > 0 => {
                    if let Some(anchors) = anchor_update {
                        engine.refresh_anchors(anchors, now_ms());
                    }
                }
                fx_update = fx_rx.recv() => {
                    let Some(update) = fx_update else {
                        return Err(SimulationError::Market(
                            "FX feed stopped".to_owned(),
                        ));
                    };
                    fx_last_update_at_ms = fx_last_update_at_ms.max(update.observed_at_ms);
                    fx_latest_by_currency.insert(update.currency.clone(), update.clone());
                    let line = serde_json::to_string(&update)
                        .unwrap_or_else(|_| "{}".to_owned());
                    fx_record_tx
                        .send(line)
                        .await
                        .map_err(|_| SimulationError::Io("FX writer stopped".to_owned()))?;
                }
                fx_joined = &mut fx_task => {
                    match fx_joined {
                        Ok(Ok(())) => {
                            return Err(SimulationError::Market(
                                "FX feed stopped".to_owned(),
                            ));
                        }
                        Ok(Err(error)) => {
                            return Err(SimulationError::Market(format!(
                                "FX feed failed: {error}"
                            )));
                        }
                        Err(error) => {
                            return Err(SimulationError::Market(format!(
                                "FX feed task failed: {error}"
                            )));
                        }
                    }
                }
                joined = shard_tasks.join_next() => {
                    match joined {
                        Some(Ok(())) => {
                            eprintln!("market shard supervisor ended unexpectedly; feed remains gated");
                        }
                        Some(Err(error)) => {
                            return Err(SimulationError::Market(format!(
                                "market shard task failed: {error}"
                            )));
                        }
                        None => {
                            return Err(SimulationError::Market(
                                "all market shard supervisors stopped".to_owned(),
                            ));
                        }
                    }
                }
            }
        }
    })
    .await;

    shard_tasks.abort_all();
    while shard_tasks.join_next().await.is_some() {}
    fx_task.abort();
    let _ = fx_task.await;
    if let Some(anchor_task) = anchor_task.take() {
        anchor_task.abort();
        let _ = anchor_task.await;
    }

    for record in engine.cancel_all(now_ms(), "simulation run stopped") {
        let line = serde_json::to_string(&record)?;
        record_tx
            .send(line)
            .await
            .map_err(|_| SimulationError::Io("record writer stopped".to_owned()))?;
    }
    if let Some(path) = metrics_output_path.as_deref() {
        let observed_at_ms = now_ms();
        performance_history.push_back(engine.performance_point(observed_at_ms));
        while performance_history.len() > 900 {
            performance_history.pop_front();
        }
        write_json_atomic(
            path,
            &engine.metrics_snapshot_with_history(
                observed_at_ms,
                last_received_at_ms,
                performance_history.make_contiguous(),
            ),
        )
        .await?;
    }
    drop(record_tx);
    drop(market_tx);
    drop(fx_record_tx);
    let records_written = record_writer
        .await
        .map_err(|error| SimulationError::Io(error.to_string()))??;
    let market_records_written = market_writer
        .await
        .map_err(|error| SimulationError::Io(error.to_string()))??;
    let fx_records_written = fx_record_writer
        .await
        .map_err(|error| SimulationError::Io(error.to_string()))??;
    let fx_fresh_at_end = !fx_currencies.is_empty()
        && fx_currencies.iter().all(|currency| {
            fx_latest_by_currency
                .get(currency.as_str())
                .is_some_and(|update| update.is_fresh_at(now_ms(), config.fx_max_age_ms))
        });
    let stopped_by_duration = match run_result {
        Err(_) if config.duration_secs != 0 => true,
        Err(_) => {
            return Err(SimulationError::Market(
                "continuous simulation timeout".to_owned(),
            ))
        }
        Ok(Ok(())) => false,
        Ok(Err(error)) => return Err(error),
    };
    let event_dropped = event_dropped.load(Ordering::Relaxed);
    if event_dropped != 0 {
        return Err(SimulationError::Market(format!(
            "market event queue dropped {event_dropped} events"
        )));
    }
    Ok(SimulationResult {
        summary: engine.summary(),
        records_written: records_written.max(written.load(Ordering::Relaxed)),
        records_dropped: dropped.load(Ordering::Relaxed),
        market_records_written: market_records_written.max(market_written.load(Ordering::Relaxed)),
        market_records_dropped: market_dropped.load(Ordering::Relaxed),
        fx_records_written: fx_records_written.max(fx_written.load(Ordering::Relaxed)),
        fx_records_dropped: fx_dropped.load(Ordering::Relaxed),
        fx_last_update_at_ms,
        fx_fresh_at_end,
        stopped_by_duration,
    })
}

const RISK_SAMPLE_INTERVAL_MS: u64 = 30_000;

fn calculate_risk_metrics(points: &[(u64, i64)], capital_ticks: i64) -> RiskMetrics {
    let capital = capital_ticks.max(1) as f64;
    let total_return_pct = points
        .last()
        .map(|(_, pnl)| (*pnl as f64 / capital) * 100.0)
        .unwrap_or(0.0);
    let mut max_drawdown_pct = 0.0_f64;
    let mut peak_equity = 1.0_f64;
    for (_, pnl) in points {
        let equity = 1.0 + (*pnl as f64 / capital);
        peak_equity = peak_equity.max(equity);
        if peak_equity > 0.0 {
            max_drawdown_pct = max_drawdown_pct.max((peak_equity - equity) / peak_equity * 100.0);
        }
    }

    let mut sampled_points = Vec::with_capacity(points.len());
    for point in points.iter().copied() {
        if sampled_points.last().is_none_or(|last: &(u64, i64)| {
            point.0.saturating_sub(last.0) >= RISK_SAMPLE_INTERVAL_MS
        }) {
            sampled_points.push(point);
        }
    }
    let mut returns = Vec::with_capacity(sampled_points.len().saturating_sub(1));
    let mut observed_seconds = 0.0_f64;
    for pair in sampled_points.windows(2) {
        let dt = (pair[1].0.saturating_sub(pair[0].0) as f64 / 1_000.0).max(0.001);
        observed_seconds += dt;
        returns.push((pair[1].1.saturating_sub(pair[0].1)) as f64 / capital);
    }
    let sample_count = returns.len();
    let mean = if sample_count == 0 {
        0.0
    } else {
        returns.iter().sum::<f64>() / sample_count as f64
    };
    let variance = if sample_count > 1 {
        returns
            .iter()
            .map(|value| (value - mean).powi(2))
            .sum::<f64>()
            / (sample_count - 1) as f64
    } else {
        0.0
    };
    let standard_deviation = variance.sqrt();
    let downside_deviation = if sample_count == 0 {
        0.0
    } else {
        (returns
            .iter()
            .map(|value| if *value < 0.0 { value.powi(2) } else { 0.0 })
            .sum::<f64>()
            / sample_count as f64)
            .sqrt()
    };
    let annualization = if sample_count > 0 && observed_seconds > 0.0 {
        (365.0 * 24.0 * 60.0 * 60.0 / (observed_seconds / sample_count as f64)).sqrt()
    } else {
        0.0
    };
    let positive = returns.iter().filter(|value| **value > 0.0).count();
    let gross_profit = returns.iter().filter(|value| **value > 0.0).sum::<f64>();
    let gross_loss = returns
        .iter()
        .filter(|value| **value < 0.0)
        .map(|value| value.abs())
        .sum::<f64>();

    RiskMetrics {
        status: if sample_count >= 30 {
            "ok".to_owned()
        } else {
            "insufficient_history".to_owned()
        },
        sample_count,
        observed_seconds,
        total_return_pct,
        max_drawdown_pct,
        win_rate_pct: if sample_count == 0 {
            0.0
        } else {
            positive as f64 / sample_count as f64 * 100.0
        },
        average_return_bps: mean * 10_000.0,
        profit_factor: (gross_loss > 0.0).then_some(gross_profit / gross_loss),
        sharpe_ratio: (sample_count >= 30 && standard_deviation > 0.0)
            .then_some(mean / standard_deviation * annualization),
        sortino_ratio: (sample_count >= 30 && downside_deviation > 0.0)
            .then_some(mean / downside_deviation * annualization),
    }
}

fn event_symbol(event: &BinanceMarketEvent) -> &str {
    match event {
        BinanceMarketEvent::BookTicker(value) => &value.symbol,
        BinanceMarketEvent::MarkPrice(value) => &value.symbol,
        BinanceMarketEvent::AggTrade(value) => &value.symbol,
        BinanceMarketEvent::DepthUpdate(value) => &value.symbol,
    }
}

// The explicit arguments keep replay assumptions visible and deterministic.
#[allow(clippy::too_many_arguments)]
pub fn replay_jsonl(
    input_path: &Path,
    output_path: Option<&Path>,
    anchors: BTreeMap<String, AnchorSnapshot>,
    price_scale: u32,
    quantity_scale: u32,
    entry_threshold_bps: i64,
    max_position: i64,
    requested_quantity: i64,
    max_mark_index_gap_bps: i64,
    max_anchor_age_ms: u64,
    fee_ppm: i64,
) -> Result<SimulationSummary, SimulationError> {
    replay_jsonl_with_realism(
        input_path,
        output_path,
        anchors,
        price_scale,
        quantity_scale,
        entry_threshold_bps,
        max_position,
        requested_quantity,
        max_mark_index_gap_bps,
        max_anchor_age_ms,
        fee_ppm,
        crate::backtest::realism::RealisticFillModel::default(),
    )
}

#[allow(clippy::too_many_arguments)]
pub fn replay_jsonl_with_realism(
    input_path: &Path,
    output_path: Option<&Path>,
    anchors: BTreeMap<String, AnchorSnapshot>,
    price_scale: u32,
    quantity_scale: u32,
    entry_threshold_bps: i64,
    max_position: i64,
    requested_quantity: i64,
    max_mark_index_gap_bps: i64,
    max_anchor_age_ms: u64,
    fee_ppm: i64,
    realism: crate::backtest::realism::RealisticFillModel,
) -> Result<SimulationSummary, SimulationError> {
    let mut engine = SimulationEngine::new(
        anchors,
        entry_threshold_bps,
        max_position,
        requested_quantity,
        max_mark_index_gap_bps,
        max_anchor_age_ms,
        fee_ppm,
        quantity_scale,
    )?
    .with_price_scale(price_scale)
    .with_strategy_variant(SimulationPolicyVariant::M0Fixed)
    .with_realism(realism);
    let reader = BufReader::new(File::open(input_path)?);
    if let Some(parent) = output_path
        .and_then(Path::parent)
        .filter(|path| !path.as_os_str().is_empty())
    {
        std::fs::create_dir_all(parent)?;
    }
    let mut output = output_path
        .map(|path| File::create(path).map(BufWriter::new))
        .transpose()?;
    let mut previous_ms = None;
    let mut event_sequence = 0_u64;
    for (index, line) in reader.lines().enumerate() {
        let line_number = index + 1;
        let line = line?;
        if line.trim().is_empty() {
            continue;
        }
        let envelope = serde_json::from_str::<serde_json::Value>(&line).ok();
        let payload = envelope
            .as_ref()
            .and_then(|value| value.get("payload"))
            .and_then(serde_json::Value::as_str)
            .unwrap_or(line.as_str());
        if payload.contains("\"id\"") && payload.contains("\"result\"") {
            continue;
        }
        let received_at_ms = envelope
            .as_ref()
            .and_then(|value| value.get("received_at_ms"))
            .and_then(serde_json::Value::as_i64)
            .and_then(|value| u64::try_from(value).ok())
            .or_else(|| {
                envelope
                    .as_ref()
                    .and_then(|value| value.get("_anchorbell_received_at_ms"))
                    .and_then(serde_json::Value::as_u64)
            });
        let event = crate::market::binance::parse_market_message(
            payload.as_bytes(),
            price_scale,
            quantity_scale,
        )
        .map_err(|error| SimulationError::ReplayParse {
            line: line_number,
            error,
        })?;
        let symbol = event_symbol(&event).to_ascii_uppercase();
        if !engine.states.contains_key(&symbol) {
            return Err(SimulationError::ReplaySymbolNotConfigured(symbol));
        }
        let event_timestamp_ms = match &event {
            BinanceMarketEvent::BookTicker(value) => value.event_time_ms,
            BinanceMarketEvent::MarkPrice(value) => value.event_time_ms,
            BinanceMarketEvent::AggTrade(value) => value.event_time_ms,
            BinanceMarketEvent::DepthUpdate(value) => value.event_time_ms,
        };
        let timestamp_ms = received_at_ms.unwrap_or(event_timestamp_ms);
        if previous_ms.is_some_and(|previous| timestamp_ms < previous) {
            return Err(SimulationError::ReplayOutOfOrder {
                previous_ms: previous_ms.unwrap(),
                current_ms: timestamp_ms,
            });
        }
        previous_ms = Some(timestamp_ms);
        event_sequence = event_sequence.saturating_add(1);
        let event_envelope = EventEnvelope {
            event_id: format!("replay-{event_sequence}").into(),
            run_id: "replay".into(),
            causality_id: format!("replay-cause-{event_sequence}").into(),
            source: EventSource::Replay,
            observed_at_ms: event_timestamp_ms,
            received_at_ms: timestamp_ms,
            sequence: event_sequence,
            state_version: event_sequence,
            quality: DataQuality::Trusted,
            payload: event,
        };
        for record in engine.on_enveloped_event(&event_envelope)? {
            if let Some(output) = output.as_mut() {
                serde_json::to_writer(&mut *output, &record)?;
                output.write_all(b"\n")?;
            }
        }
    }
    // A replay window cannot assume an order remains live after its last event.
    // Cancel working quotes at EOF, but never synthesize a position flatten fill.
    let final_timestamp_ms = previous_ms.unwrap_or(0);
    for record in engine.cancel_all(final_timestamp_ms, "replay window ended") {
        if let Some(output) = output.as_mut() {
            serde_json::to_writer(&mut *output, &record)?;
            output.write_all(b"\n")?;
        }
    }
    if let Some(output) = output.as_mut() {
        output.flush()?;
    }
    Ok(engine.summary())
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_millis() as u64)
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::market::binance::parse_market_message;

    fn anchors() -> BTreeMap<String, AnchorSnapshot> {
        [(
            "CXMTUSDT".to_owned(),
            AnchorSnapshot {
                close_price_ticks: 100,
                observed_at_ms: 0,
                valid_until_ms: 0,
            },
        )]
        .into_iter()
        .collect()
    }

    fn engine() -> SimulationEngine {
        SimulationEngine::new(anchors(), 100, 100, 10, 20, 0, 0, 0)
            .unwrap()
            .with_strategy_variant(SimulationPolicyVariant::M0Fixed)
    }

    fn feed(engine: &mut SimulationEngine, raw: &[u8]) -> Vec<SimulationRecord> {
        let event = parse_market_message(raw, 0, 0).unwrap();
        engine.on_event(event)
    }

    #[test]
    fn replay_realism_applies_queue_and_entry_latency() {
        let mut engine = engine().with_realism(crate::backtest::realism::RealisticFillModel {
            queue: crate::backtest::realism::QueueModel {
                visible_ahead: 2,
                trade_through: 0,
            },
            latency: crate::backtest::realism::LatencyModel {
                market_to_decision_ms: 5,
                decision_to_exchange_ms: 5,
                cancel_to_exchange_ms: 0,
            },
        });
        feed(
            &mut engine,
            br#"{"e":"markPriceUpdate","E":1,"s":"CXMTUSDT","p":"100","i":"100","T":600000,"r":"0"}"#,
        );
        feed(
            &mut engine,
            br#"{"e":"bookTicker","u":1,"E":2,"T":2,"s":"CXMTUSDT","b":"98","B":"10","a":"99","A":"10"}"#,
        );
        let too_early = feed(
            &mut engine,
            br#"{"e":"aggTrade","E":4,"s":"CXMTUSDT","a":1,"p":"98","q":"3","T":4,"m":true}"#,
        );
        assert!(too_early.is_empty());
        let queued = feed(
            &mut engine,
            br#"{"e":"aggTrade","E":12,"s":"CXMTUSDT","a":2,"p":"98","q":"3","T":12,"m":true}"#,
        );
        assert_eq!(queued[0].quantity, Some(1));
        assert_eq!(engine.summary().current_absolute_position, 1);
    }

    #[test]
    fn exchange_arrival_uses_local_receipt_time_not_exchange_event_time() {
        let mut engine = engine().with_realism(crate::backtest::realism::RealisticFillModel {
            queue: crate::backtest::realism::QueueModel::default(),
            latency: crate::backtest::realism::LatencyModel {
                market_to_decision_ms: 5,
                decision_to_exchange_ms: 5,
                cancel_to_exchange_ms: 0,
            },
        });
        let mark = parse_market_message(
            br#"{"e":"markPriceUpdate","E":100,"s":"CXMTUSDT","p":"100","i":"100","T":600000,"r":"0"}"#,
            0,
            0,
        )
        .unwrap();
        engine.on_event_at_ref(&mark, 100);
        let book = parse_market_message(
            br#"{"e":"bookTicker","u":1,"E":1000,"T":1000,"s":"CXMTUSDT","b":"98","B":"10","a":"99","A":"10"}"#,
            0,
            0,
        )
        .unwrap();
        engine.on_event_at_ref(&book, 3_000);
        let too_early = parse_market_message(
            br#"{"e":"aggTrade","E":2050,"s":"CXMTUSDT","a":1,"p":"98","q":"3","T":2050,"m":true}"#,
            0,
            0,
        )
        .unwrap();
        assert!(engine.on_event_at_ref(&too_early, 3_050).is_empty());
        assert_eq!(engine.summary().current_absolute_position, 0);
        let available = parse_market_message(
            br#"{"e":"aggTrade","E":3020,"s":"CXMTUSDT","a":2,"p":"98","q":"3","T":3020,"m":true}"#,
            0,
            0,
        )
        .unwrap();
        assert_eq!(engine.on_event_at_ref(&available, 3_020).len(), 1);
        assert_eq!(engine.summary().current_absolute_position, 3);
    }

    #[test]
    fn seeded_local_depth_caps_fill_at_the_order_price() {
        let mut engine = engine();
        engine
            .load_depth_snapshot("CXMTUSDT", 10, &[(98, 1)], &[(99, 10)])
            .unwrap();
        feed(
            &mut engine,
            br#"{"e":"markPriceUpdate","E":1,"s":"CXMTUSDT","p":"100","i":"100","T":600000,"r":"0"}"#,
        );
        feed(
            &mut engine,
            br#"{"e":"bookTicker","u":1,"E":2,"T":2,"s":"CXMTUSDT","b":"98","B":"10","a":"99","A":"10"}"#,
        );
        feed(
            &mut engine,
            br#"{"e":"depthUpdate","E":3,"T":3,"s":"CXMTUSDT","U":11,"u":11,"pu":10,"b":[["98","1"]],"a":[]}"#,
        );
        let filled = feed(
            &mut engine,
            br#"{"e":"aggTrade","E":12,"s":"CXMTUSDT","a":1,"p":"98","q":"5","T":12,"m":true}"#,
        );
        assert_eq!(filled[0].quantity, Some(1));
    }

    #[test]
    fn simulation_order_fills_only_on_compatible_aggressor_at_exact_price() {
        let mut engine = engine();
        feed(
            &mut engine,
            br#"{"e":"markPriceUpdate","E":1,"s":"CXMTUSDT","p":"100","i":"100","T":600000,"r":"0"}"#,
        );
        let placed = feed(
            &mut engine,
            br#"{"e":"bookTicker","u":1,"E":2,"T":2,"s":"CXMTUSDT","b":"98","B":"10","a":"99","A":"10"}"#,
        );
        assert!(placed.iter().any(|record| record.kind == "decision"));
        assert!(placed.iter().any(|record| record.kind == "order_placed"));
        let wrong_side = feed(
            &mut engine,
            br#"{"e":"aggTrade","E":3,"s":"CXMTUSDT","a":1,"p":"98","q":"3","T":3,"m":false}"#,
        );
        assert!(wrong_side.is_empty());
        let filled = feed(
            &mut engine,
            br#"{"e":"aggTrade","E":4,"s":"CXMTUSDT","a":2,"p":"98","q":"3","T":4,"m":true}"#,
        );
        assert_eq!(filled.len(), 1);
        assert_eq!(filled[0].quantity, Some(3));
        assert_eq!(engine.summary().current_absolute_position, 3);
    }

    #[test]
    fn funding_deadline_cancels_new_risk_before_settlement() {
        let mut engine = engine().with_live_risk_gates();
        feed(
            &mut engine,
            br#"{"e":"markPriceUpdate","E":1,"s":"CXMTUSDT","p":"100","i":"100","T":600000,"r":"0"}"#,
        );
        let placed = feed(
            &mut engine,
            br#"{"e":"bookTicker","u":1,"E":2,"T":2,"s":"CXMTUSDT","b":"98","B":"10","a":"99","A":"10"}"#,
        );
        assert!(placed.iter().any(|record| record.kind == "order_placed"));
        feed(
            &mut engine,
            br#"{"e":"markPriceUpdate","E":299999,"s":"CXMTUSDT","p":"100","i":"100","T":600000,"r":"0"}"#,
        );
        let canceled = feed(
            &mut engine,
            br#"{"e":"bookTicker","u":2,"E":300001,"T":300001,"s":"CXMTUSDT","b":"98","B":"10","a":"99","A":"10"}"#,
        );
        assert!(canceled.iter().any(|record| record.kind == "decision"));
        assert!(canceled
            .iter()
            .any(|record| record.kind == "order_canceled"));
        assert_eq!(engine.summary().working_orders, 0);
    }

    #[test]
    fn summary_includes_mark_to_market_for_open_position() {
        let mut engine = engine();
        feed(
            &mut engine,
            br#"{"e":"markPriceUpdate","E":1,"s":"CXMTUSDT","p":"100","i":"100","T":600000,"r":"0"}"#,
        );
        feed(
            &mut engine,
            br#"{"e":"bookTicker","u":1,"E":2,"T":2,"s":"CXMTUSDT","b":"98","B":"10","a":"99","A":"10"}"#,
        );
        feed(
            &mut engine,
            br#"{"e":"aggTrade","E":3,"s":"CXMTUSDT","a":1,"p":"98","q":"3","T":3,"m":true}"#,
        );
        let summary = engine.summary();
        assert_eq!(summary.current_absolute_position, 3);
        assert_eq!(summary.unrealized_pnl_ticks, 6);
        assert_eq!(summary.net_pnl_ticks, 6);
        assert!(summary.unrealized_valuation_complete);
        assert!(!summary.flat_at_end);
    }

    #[test]
    fn quantity_precision_is_applied_to_mark_to_market_pnl() {
        let mut engine = SimulationEngine::new(anchors(), 100, 1_000, 300, 20, 0, 0, 2)
            .unwrap()
            .with_strategy_variant(SimulationPolicyVariant::M0Fixed);
        let feed_scaled = |engine: &mut SimulationEngine, raw: &[u8]| {
            let event = parse_market_message(raw, 0, 2).unwrap();
            engine.on_event(event)
        };
        feed_scaled(
            &mut engine,
            br#"{"e":"markPriceUpdate","E":1,"s":"CXMTUSDT","p":"100","i":"100","T":600000,"r":"0"}"#,
        );
        feed_scaled(
            &mut engine,
            br#"{"e":"bookTicker","u":1,"E":2,"T":2,"s":"CXMTUSDT","b":"98","B":"10","a":"99","A":"10"}"#,
        );
        feed_scaled(
            &mut engine,
            br#"{"e":"aggTrade","E":3,"s":"CXMTUSDT","a":1,"p":"98","q":"3","T":3,"m":true}"#,
        );
        let summary = engine.summary();
        assert_eq!(summary.current_absolute_position, 300);
        assert_eq!(summary.unrealized_pnl_ticks, 6);
    }

    #[test]
    fn replay_cancels_working_quotes_at_window_end_without_faking_a_fill() {
        let path = std::env::temp_dir().join("anchorbell-simulation-replay-eof.jsonl");
        std::fs::write(
            &path,
            "{\"e\":\"markPriceUpdate\",\"E\":1,\"s\":\"CXMTUSDT\",\"p\":\"100\",\"i\":\"100\",\"T\":1000,\"r\":\"0\"}\n{\"e\":\"bookTicker\",\"u\":1,\"E\":2,\"T\":2,\"s\":\"CXMTUSDT\",\"b\":\"98\",\"B\":\"10\",\"a\":\"99\",\"A\":\"10\"}\n",
        )
        .unwrap();
        let result = replay_jsonl(&path, None, anchors(), 0, 0, 100, 100, 10, 20, 0, 0).unwrap();
        assert_eq!(result.order_count, 1);
        assert_eq!(result.fill_count, 0);
        assert_eq!(result.working_orders, 0);
        assert!(result.flat_at_end);
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn simulation_replay_rejects_symbols_without_configured_state() {
        let path = std::env::temp_dir().join("anchorbell-simulation-replay-symbol.jsonl");
        std::fs::write(
            &path,
            "{\"e\":\"markPriceUpdate\",\"E\":1,\"s\":\"XYZUSDT\",\"p\":\"100\",\"i\":\"100\",\"T\":1000,\"r\":\"0\"}\n",
        )
        .unwrap();
        let result = replay_jsonl(&path, None, anchors(), 0, 0, 100, 100, 10, 20, 0, 0);
        assert!(matches!(
            result,
            Err(SimulationError::ReplaySymbolNotConfigured(symbol)) if symbol == "XYZUSDT"
        ));
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn simulation_replay_rejects_out_of_order_events() {
        let path = std::env::temp_dir().join("anchorbell-simulation-replay-order.jsonl");
        std::fs::write(
            &path,
            "{\"e\":\"markPriceUpdate\",\"E\":2,\"s\":\"CXMTUSDT\",\"p\":\"100\",\"i\":\"100\",\"T\":2,\"r\":\"0\"}\n{\"e\":\"markPriceUpdate\",\"E\":1,\"s\":\"CXMTUSDT\",\"p\":\"100\",\"i\":\"100\",\"T\":1,\"r\":\"0\"}\n",
        )
        .unwrap();
        let result = replay_jsonl(&path, None, anchors(), 0, 0, 100, 100, 10, 20, 0, 0);
        assert!(matches!(
            result,
            Err(SimulationError::ReplayOutOfOrder { .. })
        ));
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn simulation_marks_beijing_overnight_as_closed_and_usable() {
        let overnight = 1_788_377_934_321_u64;
        let previous_close = overnight - 12 * 60 * 60 * 1_000;
        let next_close = overnight + 12 * 60 * 60 * 1_000;
        assert_eq!(calendar_state_for("CXMTUSDT", overnight), "closed");
        assert!(simulation_session_allows_entry("CXMTUSDT", overnight));
        assert!(simulation_anchor_usable(
            "CXMTUSDT",
            previous_close,
            overnight
        ));
        assert!(!simulation_anchor_usable(
            "CXMTUSDT",
            previous_close,
            next_close
        ));
        assert!(anchor_refresh_allowed("CXMTUSDT", overnight));
    }

    #[test]
    fn simulation_allows_static_anchor_entries_on_weekends() {
        // 2026-09-05 12:00 Asia/Shanghai (Saturday), within the supported
        // 2026 calendar snapshot.
        let saturday_midday = 1_788_580_800_000_u64;
        assert_eq!(calendar_state_for("CXMTUSDT", saturday_midday), "weekend");
        assert!(simulation_session_allows_entry("CXMTUSDT", saturday_midday));
        assert!(anchor_refresh_allowed("CXMTUSDT", saturday_midday));
        assert!(simulation_anchor_usable(
            "CXMTUSDT",
            saturday_midday,
            saturday_midday + 60 * 60 * 1_000
        ));
    }

    #[test]
    fn capital_allocation_respects_fixed_and_weighted_modes() {
        let anchors = [
            (
                "CXMTUSDT".to_owned(),
                AnchorSnapshot {
                    close_price_ticks: 100 * 100_000_000,
                    observed_at_ms: 0,
                    valid_until_ms: 0,
                },
            ),
            (
                "UNITREEUSDT".to_owned(),
                AnchorSnapshot {
                    close_price_ticks: 200 * 100_000_000,
                    observed_at_ms: 0,
                    valid_until_ms: 0,
                },
            ),
        ]
        .into_iter()
        .collect();
        let modes = [(
            "CXMTUSDT".to_owned(),
            PositionMode::FixedUsdt(2 * 100_000_000),
        )]
        .into_iter()
        .collect();
        let allocations = allocate_positions(&anchors, 10 * 100_000_000, &modes, 8).unwrap();
        assert_eq!(allocations["CXMTUSDT"].budget_usdt_ticks, 2 * 100_000_000);
        assert_eq!(
            allocations["UNITREEUSDT"].budget_usdt_ticks,
            8 * 100_000_000
        );
        assert_eq!(
            allocations
                .values()
                .map(|allocation| allocation.budget_usdt_ticks)
                .sum::<i64>(),
            10 * 100_000_000
        );
        assert_eq!(allocations["CXMTUSDT"].requested_quantity, 2_000_000);
        assert_eq!(allocations["UNITREEUSDT"].requested_quantity, 4_000_000);
    }

    #[test]
    fn simulation_allows_midday_break_anchor_but_not_open_anchor() {
        let midday_break = 1_788_408_258_130_u64;
        let morning_open = 1_788_404_658_130_u64;
        assert!(anchor_reference_allowed("CXMTUSDT", midday_break));
        assert!(anchor_reference_allowed("HK0625USDT", midday_break));
        assert!(!anchor_reference_allowed("CXMTUSDT", morning_open));
        assert!(!anchor_reference_allowed("HK0625USDT", morning_open));
    }

    #[test]
    fn cancel_latency_keeps_order_fillable_until_exchange_ack() {
        let mut engine = engine().with_realism(crate::backtest::realism::RealisticFillModel {
            queue: crate::backtest::realism::QueueModel::default(),
            latency: crate::backtest::realism::LatencyModel {
                market_to_decision_ms: 0,
                decision_to_exchange_ms: 0,
                cancel_to_exchange_ms: 5,
            },
        });
        feed(
            &mut engine,
            br#"{"e":"markPriceUpdate","E":1,"s":"CXMTUSDT","p":"100","i":"100","T":600000,"r":"0"}"#,
        );
        feed(
            &mut engine,
            br#"{"e":"bookTicker","u":1,"E":2,"T":2,"s":"CXMTUSDT","b":"98","B":"10","a":"99","A":"10"}"#,
        );
        let in_flight = feed(
            &mut engine,
            br#"{"e":"bookTicker","u":2,"E":3,"T":3,"s":"CXMTUSDT","b":"97","B":"10","a":"99","A":"10"}"#,
        );
        assert!(in_flight.iter().any(|record| record.kind == "decision"));
        assert!(!in_flight
            .iter()
            .any(|record| record.kind == "order_canceled"));
        assert_eq!(engine.summary().working_orders, 1);
        let filled = feed(
            &mut engine,
            br#"{"e":"aggTrade","E":4,"s":"CXMTUSDT","a":1,"p":"98","q":"3","T":4,"m":true}"#,
        );
        assert_eq!(filled.len(), 1);
        assert_eq!(engine.summary().current_absolute_position, 3);
        let acknowledged = feed(
            &mut engine,
            br#"{"e":"bookTicker","u":3,"E":8,"T":8,"s":"CXMTUSDT","b":"97","B":"10","a":"99","A":"10"}"#,
        );
        assert!(acknowledged
            .iter()
            .any(|record| record.kind == "order_canceled"));
    }

    #[test]
    fn micro_ewma_preserves_sub_basis_point_samples() {
        assert_eq!(ewma_micro(0, 250_000), 250_000);
        assert_eq!(ewma_micro(1_000_000, 500_000), 850_000);
        assert_eq!(micro_bps_to_bps(499_999), 0);
        assert_eq!(micro_bps_to_bps(500_000), 1);
    }

    #[test]
    fn adaptive_relief_never_removes_hard_cost_or_deadline_risk() {
        let base = AdaptiveThreshold::from_components(5, 4, 3, 2, 7, 6, 5, 4, 3, 2, 8, 1).unwrap();
        let hard_cost_micro =
            (base.floor_bps + base.cost_bps + base.deadline_risk_bps) * MICRO_BPS_SCALE;
        let relaxed_required = base
            .required_micro_bps()
            .unwrap()
            .saturating_sub(3 * MICRO_BPS_SCALE);
        assert!(relaxed_required >= hard_cost_micro);
        assert!(relaxed_required < base.required_micro_bps().unwrap());
    }

    #[test]
    fn edge_micro_bps_preserves_fractional_basis_points() {
        let edge = edge_micro_bps(100_001, 100_000).unwrap();
        assert_eq!(edge, 100_000);
        assert_eq!(micro_bps_to_bps(edge), 0);
        assert!(edge_micro_bps(100_000, 100_001).unwrap() < 0);
    }

    #[test]
    fn exchange_clock_freshness_ignores_local_receipt_clock() {
        let mut engine = engine();
        let mark = parse_market_message(
            br#"{"e":"markPriceUpdate","E":100,"s":"CXMTUSDT","p":"100","i":"100","T":600000,"r":"0"}"#,
            0,
            0,
        )
        .unwrap();
        engine.on_event_at_ref(&mark, 1_000_000);
        let book = parse_market_message(
            br#"{"e":"bookTicker","u":1,"E":200,"T":200,"s":"CXMTUSDT","b":"98","B":"100","a":"99","A":"100"}"#,
            0,
            0,
        )
        .unwrap();
        engine.on_event_at_ref(&book, 1);
        let metrics = engine.metrics_snapshot(200, 1).symbols[0].clone();
        assert_eq!(metrics.data_quality, DataQualityStatus::Fresh);
        assert_eq!(metrics.mark_age_ms, Some(100));
    }

    #[test]
    fn liquidity_controls_are_continuous_and_monotonic() {
        assert_eq!(liquidity_ratio_bps(10, 100, 100), 1_000);
        assert_eq!(liquidity_penalty_bps(10, 100, 100), 0);
        assert!(liquidity_penalty_bps(50, 100, 100) > liquidity_penalty_bps(25, 100, 100));
        assert!(fill_probability_bps(10, 100, 100) > fill_probability_bps(50, 100, 100));
        assert_eq!(liquidity_adjusted_quantity(100, 100, 100), 100);
    }

    #[test]
    fn threshold_status_explains_warmup_and_uses_a_conservative_prior() {
        let mut engine = engine().with_strategy_variant(SimulationPolicyVariant::M7EvidenceGated);
        let initial = engine.metrics_snapshot(1, 1).symbols[0].clone();
        assert_eq!(initial.threshold_status, "warming_up");
        assert_eq!(initial.threshold_missing_component.as_deref(), Some("book"));
        feed(
            &mut engine,
            br#"{"e":"bookTicker","u":1,"E":2,"T":2,"s":"CXMTUSDT","b":"98","B":"10","a":"99","A":"10"}"#,
        );
        let missing_mark = engine.metrics_snapshot(2, 2).symbols[0].clone();
        assert_eq!(missing_mark.threshold_status, "insufficient_data");
        assert_eq!(
            missing_mark.threshold_missing_component.as_deref(),
            Some("mark_price")
        );
        feed(
            &mut engine,
            br#"{"e":"markPriceUpdate","E":3,"s":"CXMTUSDT","p":"100","i":"100","T":600000,"r":"0"}"#,
        );
        let warmed = engine.metrics_snapshot(3, 3).symbols[0].clone();
        assert_eq!(warmed.threshold_status, "warming_up");
        assert!(warmed.threshold_prior_used);
        assert!(warmed.threshold.is_some());
    }

    #[test]
    fn shutdown_requests_reduce_only_flatten_without_faking_a_fill() {
        let mut engine = engine();
        feed(
            &mut engine,
            br#"{"e":"markPriceUpdate","E":1,"s":"CXMTUSDT","p":"100","i":"100","T":600000,"r":"0"}"#,
        );
        feed(
            &mut engine,
            br#"{"e":"bookTicker","u":1,"E":2,"T":2,"s":"CXMTUSDT","b":"98","B":"10","a":"99","A":"10"}"#,
        );
        feed(
            &mut engine,
            br#"{"e":"aggTrade","E":3,"s":"CXMTUSDT","a":1,"p":"98","q":"3","T":3,"m":true}"#,
        );
        assert_eq!(engine.summary().current_absolute_position, 3);
        let settlement = engine.shutdown(4, "test shutdown");
        assert!(settlement.flatten_requested);
        assert_eq!(settlement.summary.current_absolute_position, 3);
        assert_eq!(settlement.settlement_status, "flatten_orders_working");
        assert!(settlement
            .records
            .iter()
            .any(|record| record.kind == "order_placed"));
    }
}
