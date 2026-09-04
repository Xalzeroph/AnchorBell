//! Shared paper-trading and replay execution engine.
//!
//! The paper path consumes the same Binance bookTicker, markPrice, and
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
        recorder::market_event_to_json,
        BinanceC2cFxClient, BinanceC2cFxPoller, BinanceMarketConfig, BinanceMarketFeed,
        BinanceMarketStream, FxPollerConfig, FxUpdate, PublicMarketMetadataClient, ReconnectPolicy,
    },
    runtime::io::{spawn_line_writer, write_json_atomic, AsyncLineWriter},
    strategy::{
        calendar::{calendar_for, EquitySessionCalendar},
        decide_maker_exit, profile_for, side_adverse_selection_bps,
        universe::instrument_for,
        AdaptiveThreshold, AnchorCurrency, AnchorMakerStrategy, DataQualityStatus, DualFlattenPlan,
        ExitBook, ExitConstraints, ExitWorkingOrder, FundingRateKind, FundingSchedule,
        FundingScheduleStatus, MakerExitDecision, MakerExitInput, SignalInput, VenueSessionState,
    },
};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub struct PaperAnchor {
    pub close_price_ticks: i64,
    pub observed_at_ms: u64,
    pub valid_until_ms: u64,
}

impl PaperAnchor {
    pub fn valid_at(self, now_ms: u64, max_age_ms: u64) -> bool {
        self.close_price_ticks > 0
            && (self.observed_at_ms == 0 || now_ms >= self.observed_at_ms)
            && (self.valid_until_ms == 0 || now_ms < self.valid_until_ms)
            && (self.observed_at_ms == 0
                || max_age_ms == 0
                || now_ms.saturating_sub(self.observed_at_ms) <= max_age_ms)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize)]
pub enum PaperStrategyVariant {
    M0Fixed,
    M1AdaptiveRisk,
    M2Microstructure,
    M3FillAware,
    M4Statistical,
}

impl PaperStrategyVariant {
    pub fn label(self) -> &'static str {
        match self {
            Self::M0Fixed => "m0_fixed",
            Self::M1AdaptiveRisk => "m1_adaptive_risk",
            Self::M2Microstructure => "m2_microstructure",
            Self::M3FillAware => "m3_fill_aware",
            Self::M4Statistical => "m4_statistical",
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
}

#[derive(Debug, Clone)]
pub enum PositionMode {
    Equal,
    Weight(u64),
    FixedUsdt(i64),
}

impl PositionMode {
    fn label(&self) -> String {
        match self {
            Self::Equal => "equal".to_owned(),
            Self::Weight(weight) => format!("weight:{weight}"),
            Self::FixedUsdt(_) => "fixed_usdt".to_owned(),
        }
    }

    fn weight(&self) -> Option<u64> {
        match self {
            Self::Equal => Some(1),
            Self::Weight(weight) => Some(*weight),
            Self::FixedUsdt(_) => None,
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
    anchors: &BTreeMap<String, PaperAnchor>,
    total_capital_usdt_ticks: i64,
    modes: &BTreeMap<String, PositionMode>,
    quantity_scale: u32,
) -> Result<BTreeMap<String, PositionAllocation>, PaperError> {
    if anchors.is_empty() || total_capital_usdt_ticks <= 0 || quantity_scale > 18 {
        return Err(PaperError::InvalidConfig(
            "capital allocation requires anchors, positive capital, and a valid quantity scale",
        ));
    }
    if modes.keys().any(|symbol| !anchors.contains_key(symbol)) {
        return Err(PaperError::InvalidConfig(
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
                return Err(PaperError::InvalidConfig(
                    "fixed position capital must be positive",
                ));
            }
            PositionMode::Equal | PositionMode::Weight(_) => {
                let weight = mode.weight().unwrap_or(0);
                if weight == 0 {
                    return Err(PaperError::InvalidConfig(
                        "position mode weight must be positive",
                    ));
                }
                variable_weight = variable_weight.saturating_add(u128::from(weight));
            }
        }
        resolved_modes.insert(symbol.clone(), mode);
    }
    if fixed_total > i128::from(total_capital_usdt_ticks) {
        return Err(PaperError::InvalidConfig(
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
            .ok_or(PaperError::InvalidConfig(
                "capital allocation requires positive anchor prices",
            ))?;
        let requested_quantity =
            budget.saturating_mul(10_i128.pow(quantity_scale)) / i128::from(anchor_price);
        if requested_quantity <= 0 || requested_quantity > i128::from(i64::MAX) {
            return Err(PaperError::InvalidConfig(
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
pub enum PaperError {
    #[error("invalid paper configuration: {0}")]
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

impl From<std::io::Error> for PaperError {
    fn from(error: std::io::Error) -> Self {
        Self::Io(error.to_string())
    }
}

impl From<serde_json::Error> for PaperError {
    fn from(error: serde_json::Error) -> Self {
        Self::Json(error.to_string())
    }
}

pub fn load_anchors(path: &Path) -> Result<BTreeMap<String, PaperAnchor>, PaperError> {
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
            return Err(PaperError::InvalidAnchorRow {
                row,
                reason: "expected symbol,close_price_ticks,observed_at_ms,valid_until_ms",
            });
        }
        let symbol = normalize_symbol(fields[0]).ok_or(PaperError::InvalidAnchorRow {
            row,
            reason: "symbol must be non-empty ASCII alphanumeric text",
        })?;
        let close_price_ticks =
            fields[1]
                .parse::<i64>()
                .map_err(|_| PaperError::InvalidAnchorRow {
                    row,
                    reason: "close_price_ticks must be an integer",
                })?;
        let observed_at_ms =
            fields[2]
                .parse::<u64>()
                .map_err(|_| PaperError::InvalidAnchorRow {
                    row,
                    reason: "observed_at_ms must be an unsigned integer",
                })?;
        let valid_until_ms =
            fields[3]
                .parse::<u64>()
                .map_err(|_| PaperError::InvalidAnchorRow {
                    row,
                    reason: "valid_until_ms must be an unsigned integer",
                })?;
        if close_price_ticks <= 0
            || (valid_until_ms != 0 && observed_at_ms != 0 && valid_until_ms <= observed_at_ms)
        {
            return Err(PaperError::InvalidAnchorRow {
                row,
                reason: "anchor price must be positive and validity must be ordered",
            });
        }
        if anchors
            .insert(
                symbol.clone(),
                PaperAnchor {
                    close_price_ticks,
                    observed_at_ms,
                    valid_until_ms,
                },
            )
            .is_some()
        {
            return Err(PaperError::DuplicateAnchor(symbol));
        }
    }
    if anchors.is_empty() {
        return Err(PaperError::NoAnchors);
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
    pub anchors: BTreeMap<String, PaperAnchor>,
    pub conversions: BTreeMap<String, IndexAnchorConversion>,
}

pub async fn load_binance_index_anchor_set(
    environment: BinanceEnvironment,
    symbols: &[String],
    price_scale: u32,
    http_proxy: Option<&str>,
) -> Result<BinanceIndexAnchorSet, PaperError> {
    if symbols.is_empty() {
        return Err(PaperError::InvalidConfig(
            "index anchors require at least one symbol",
        ));
    }
    let mut seen = BTreeMap::new();
    let mut selected_metadata = Vec::with_capacity(symbols.len());
    let client = PublicMarketMetadataClient::new(environment.endpoints().rest_base, http_proxy)
        .map_err(|error| PaperError::Market(format!("index anchor client: {error}")))?;
    let exchange_info = client
        .exchange_info()
        .await
        .map_err(|error| PaperError::Market(format!("index anchor exchangeInfo: {error}")))?;

    for symbol in symbols {
        let normalized = normalize_symbol(symbol).ok_or(PaperError::InvalidConfig(
            "index anchor symbol must be non-empty ASCII alphanumeric text",
        ))?;
        if instrument_for(&normalized).is_none() {
            return Err(PaperError::InvalidConfig(
                "index anchor symbols must be selected TradFi instruments",
            ));
        }
        if seen.insert(normalized.clone(), ()).is_some() {
            return Err(PaperError::DuplicateAnchor(normalized));
        }
        let metadata = exchange_info
            .iter()
            .find(|metadata| metadata.symbol == normalized)
            .cloned()
            .ok_or_else(|| {
                PaperError::Market(format!(
                    "Binance exchangeInfo has no selected symbol {normalized}"
                ))
            })?;
        if !metadata.is_trading_tradifi_perpetual() {
            return Err(PaperError::Market(format!(
                "selected symbol {normalized} is not a trading TradFi perpetual"
            )));
        }
        selected_metadata.push(metadata);
    }

    let snapshots = client.symbol_snapshots(selected_metadata.clone(), 4).await;
    let observed_now_ms = now_ms();
    let mut fx_quotes = BTreeMap::new();
    let fx_client = BinanceC2cFxClient::new(http_proxy)
        .map_err(|error| PaperError::Market(format!("index anchor FX client: {error}")))?;
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
            .map_err(|error| PaperError::Market(format!("index anchor CNY/USDT FX: {error}")))?;
        fx_quotes.insert(AnchorCurrency::Cny.as_str().to_owned(), quote);
    }
    if needs_hkd {
        let quote = fx_client
            .midpoint(AnchorCurrency::Hkd)
            .await
            .map_err(|error| PaperError::Market(format!("index anchor HKD/USDT FX: {error}")))?;
        fx_quotes.insert(AnchorCurrency::Hkd.as_str().to_owned(), quote);
    }

    let mut anchors = BTreeMap::new();
    let mut conversions = BTreeMap::new();
    for snapshot in snapshots {
        let snapshot = snapshot
            .map_err(|error| PaperError::Market(format!("index anchor snapshot: {error}")))?;
        snapshot
            .validate_for_runtime(observed_now_ms)
            .map_err(|error| PaperError::Market(format!("index anchor validation: {error}")))?;
        let symbol = snapshot.metadata.symbol.clone();
        let profile = profile_for(&symbol).ok_or_else(|| {
            PaperError::Market(format!("no anchor currency profile for {symbol}"))
        })?;
        let fx_quote = fx_quotes
            .get(profile.anchor_currency.as_str())
            .ok_or_else(|| {
                PaperError::Market(format!(
                    "missing {}/USDT FX quote for {symbol}",
                    profile.anchor_currency.as_str()
                ))
            })?;
        let index_price = crate::market::binance::parse_price_ticks(
            &snapshot.premium_index.index_price,
            price_scale,
        )
        .map_err(|error| {
            PaperError::Market(format!(
                "index anchor price for {symbol} is invalid: {error:?}"
            ))
        })?;
        if index_price.0 <= 0 {
            return Err(PaperError::Market(format!(
                "index anchor price for {symbol} is not positive"
            )));
        }
        let local_price = fx_quote
            .convert_usdt_ticks_to_local(index_price.0)
            .ok_or_else(|| {
                PaperError::Market(format!(
                    "local FX conversion overflow for {symbol} at {}",
                    profile.anchor_currency.as_str()
                ))
            })?;
        anchors.insert(
            symbol.clone(),
            PaperAnchor {
                close_price_ticks: index_price.0,
                observed_at_ms: snapshot.observed_at_ms,
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
                index_observed_at_ms: snapshot.observed_at_ms,
            },
        );
    }
    if anchors.len() != seen.len() || conversions.len() != seen.len() {
        return Err(PaperError::Market(
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
) -> Result<BTreeMap<String, PaperAnchor>, PaperError> {
    Ok(
        load_binance_index_anchor_set(environment, symbols, price_scale, http_proxy)
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
    /// The book clock is deliberately not inferred from mark updates.
    observed_at_ms: u64,
}

#[derive(Debug, Clone, Copy)]
struct WorkingOrder {
    client_id: u64,
    side: Side,
    price_ticks: i64,
    remaining_quantity: i64,
    reduce_only: bool,
    placed_at_ms: u64,
    cancel_requested_at_ms: Option<u64>,
}

#[derive(Debug, Clone, Copy)]
enum ExitConstraintSource {
    /// Baseline paper assumptions, never presented as downloaded filters.
    Simulated,
    Explicit(ExitConstraints),
}

#[derive(Debug, Clone)]
struct PaperSymbolState {
    symbol_id: u32,
    anchor: PaperAnchor,
    book: Option<BookState>,
    mark_price_ticks: Option<i64>,
    index_price_ticks: Option<i64>,
    next_funding_time_ms: u64,
    last_mark_time_ms: u64,
    last_trade_id: Option<u64>,
    last_mark_price_ticks: Option<i64>,
    ewma_abs_return_bps: i64,
    ewma_spread_bps: i64,
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
    exit_cutoff: bool,
    exit_session_plan: Option<DualFlattenPlan>,
}

#[derive(Debug, Clone, Serialize)]
pub struct PaperRecord {
    pub timestamp_ms: u64,
    /// Self-describing ledger tag for multi-strategy paper labs.
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
    pub position: i64,
    pub realized_pnl_ticks: i64,
    pub market_pnl_ticks: i64,
    pub strategy_pnl_ticks: i64,
    pub funding_pnl_ticks: i64,
    pub fees_ticks: i64,
    pub net_pnl_ticks: i64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub detail: Option<String>,
}

struct RecordFields<'a> {
    kind: &'a str,
    client_id: Option<u64>,
    side: Option<Side>,
    price_ticks: Option<i64>,
    quantity: Option<i64>,
    detail: Option<&'a str>,
}

#[derive(Debug, Clone, Serialize)]
pub struct PaperSummary {
    pub event_count: u64,
    pub order_count: u64,
    pub fill_count: u64,
    pub filled_quantity: i64,
    pub rejected_entries: u64,
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
pub struct PaperThresholdMetrics {
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
    pub required_bps: Option<i64>,
}

const FUNDING_FLATTEN_LEAD_MS: u64 = 5 * 60 * 1_000;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PaperRiskState {
    Trading,
    ReduceOnlyEquitySession,
    ReduceOnlyFundingDeadline,
    HaltFundingMetadata,
    HaltMarketData,
    HaltAnchor,
}

impl PaperRiskState {
    fn label(self) -> &'static str {
        match self {
            Self::Trading => "trading",
            Self::ReduceOnlyEquitySession => "reduce_only_equity_session",
            Self::ReduceOnlyFundingDeadline => "reduce_only_funding_deadline",
            Self::HaltFundingMetadata => "halt_funding_metadata",
            Self::HaltMarketData => "halt_market_data",
            Self::HaltAnchor => "halt_anchor",
        }
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct PaperSymbolMetrics {
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
    pub anchor_age_ms: Option<u64>,
    pub anchor_final_close: bool,
    pub calendar_state: String,
    pub next_funding_time_ms: u64,
    pub latest_funding_rate_e8: Option<i64>,
    pub funding_flatten_deadline_ms: Option<u64>,
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
    pub buy_edge_bps: Option<i64>,
    pub sell_edge_bps: Option<i64>,
    pub threshold: Option<PaperThresholdMetrics>,
}

#[derive(Debug, Clone, Serialize)]
pub struct PaperSymbolPerformancePoint {
    pub symbol: String,
    pub position: i64,
    pub market_pnl_ticks: i64,
    pub strategy_pnl_ticks: i64,
    pub funding_pnl_ticks: i64,
    pub fees_ticks: i64,
    pub net_pnl_ticks: i64,
}

#[derive(Debug, Clone, Serialize)]
pub struct PaperPerformancePoint {
    pub observed_at_ms: u64,
    pub market_pnl_ticks: i64,
    pub strategy_pnl_ticks: i64,
    pub funding_pnl_ticks: i64,
    pub fees_ticks: i64,
    pub gross_pnl_ticks: i64,
    pub net_pnl_ticks: i64,
    pub current_absolute_position: i64,
    pub symbols: Vec<PaperSymbolPerformancePoint>,
}

#[derive(Debug, Clone, Serialize)]
pub struct PaperModelAssumptions {
    pub fill_model: String,
    pub queue_ahead: i64,
    pub trade_through: i64,
    pub market_to_decision_ms: u64,
    pub decision_to_exchange_ms: u64,
    pub cancel_to_exchange_ms: u64,
    pub exit_constraints: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct PaperMetricsSnapshot {
    pub observed_at_ms: u64,
    pub strategy_variant: String,
    pub last_market_event_at_ms: u64,
    pub last_received_at_ms: u64,
    pub summary: PaperSummary,
    pub symbols: Vec<PaperSymbolMetrics>,
    pub history: Vec<PaperPerformancePoint>,
    pub calendar_snapshot: String,
    pub maker_fee_source: String,
    pub funding_model: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub capital_usdt_ticks: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub capital_usdt: Option<String>,
    pub model_assumptions: PaperModelAssumptions,
}

#[derive(Debug, Clone)]
pub struct PaperEngine {
    strategy: AnchorMakerStrategy,
    strategy_variant: PaperStrategyVariant,
    max_position: i64,
    requested_quantity: i64,
    max_mark_index_gap_bps: i64,
    max_anchor_age_ms: u64,
    fee_ppm: i64,
    price_scale: u32,
    quantity_scale: u32,
    realism: crate::backtest::realism::RealisticFillModel,
    live_risk_gates: bool,
    threshold_scale_ppm: i64,
    position_allocations: BTreeMap<String, PositionAllocation>,
    capital_usdt_ticks: Option<i64>,
    exit_constraints: BTreeMap<String, ExitConstraintSource>,
    states: BTreeMap<String, PaperSymbolState>,
    next_client_id: u64,
    event_count: u64,
    order_count: u64,
    fill_count: u64,
    filled_quantity: i64,
    rejected_entries: u64,
    peak_absolute_position: i64,
    last_event_at_ms: u64,
}

impl PaperEngine {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        anchors: BTreeMap<String, PaperAnchor>,
        entry_threshold_bps: i64,
        max_position: i64,
        requested_quantity: i64,
        max_mark_index_gap_bps: i64,
        max_anchor_age_ms: u64,
        fee_ppm: i64,
        quantity_scale: u32,
    ) -> Result<Self, PaperError> {
        if anchors.is_empty()
            || entry_threshold_bps < 0
            || max_position <= 0
            || requested_quantity <= 0
            || max_mark_index_gap_bps < 0
            || fee_ppm < 0
            || quantity_scale > 18
        {
            return Err(PaperError::InvalidConfig(
                "anchors, position, quantity, thresholds, and fee must be valid",
            ));
        }
        let states: BTreeMap<String, PaperSymbolState> = anchors
            .into_iter()
            .map(|(symbol, anchor)| {
                (
                    symbol.clone(),
                    PaperSymbolState {
                        symbol_id: stable_symbol_id(&symbol),
                        anchor,
                        book: None,
                        mark_price_ticks: None,
                        index_price_ticks: None,
                        next_funding_time_ms: 0,
                        last_mark_time_ms: 0,
                        last_trade_id: None,
                        last_mark_price_ticks: None,
                        ewma_abs_return_bps: 0,
                        ewma_spread_bps: 0,
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
                        exit_cutoff: false,
                        exit_session_plan: None,
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
                        mode: "legacy_global".to_owned(),
                        budget_usdt_ticks: 0,
                        max_position,
                        requested_quantity,
                    },
                )
            })
            .collect();
        let exit_constraints = states
            .keys()
            .map(|symbol| (symbol.clone(), ExitConstraintSource::Simulated))
            .collect();
        Ok(Self {
            strategy: AnchorMakerStrategy::new(entry_threshold_bps, 0),
            strategy_variant: PaperStrategyVariant::M4Statistical,
            max_position,
            requested_quantity,
            max_mark_index_gap_bps,
            max_anchor_age_ms,
            fee_ppm,
            price_scale: 8,
            quantity_scale,
            realism: crate::backtest::realism::RealisticFillModel::default(),
            live_risk_gates: false,
            threshold_scale_ppm: 1_000_000,
            position_allocations,
            capital_usdt_ticks: None,
            exit_constraints,
            states,
            next_client_id: 1,
            event_count: 0,
            order_count: 0,
            fill_count: 0,
            filled_quantity: 0,
            rejected_entries: 0,
            peak_absolute_position: 0,
            last_event_at_ms: 0,
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

    pub fn with_threshold_scale_ppm(mut self, scale_ppm: i64) -> Self {
        self.threshold_scale_ppm = scale_ppm.clamp(0, 1_000_000);
        self
    }

    pub fn with_strategy_variant(mut self, variant: PaperStrategyVariant) -> Self {
        self.strategy_variant = variant;
        self
    }

    pub fn with_price_scale(mut self, price_scale: u32) -> Self {
        self.price_scale = price_scale;
        self
    }

    pub fn with_position_allocations(
        mut self,
        allocations: BTreeMap<String, PositionAllocation>,
    ) -> Result<Self, PaperError> {
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
            return Err(PaperError::InvalidConfig(
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

    /// Supply actual exchange or fixture filters. Unlike the baseline paper
    /// assumptions, this timestamp is retained and may expire in the exit core.
    pub fn set_exit_constraints(
        &mut self,
        symbol: &str,
        constraints: ExitConstraints,
    ) -> Result<(), PaperError> {
        let symbol = normalize_symbol(symbol)
            .filter(|symbol| self.states.contains_key(symbol))
            .ok_or(PaperError::InvalidConfig(
                "exit constraints require a configured symbol",
            ))?;
        if constraints.min_price <= 0
            || constraints.max_price < constraints.min_price
            || constraints.price_tick <= 0
            || constraints.min_quantity <= 0
            || constraints.max_quantity < constraints.min_quantity
            || constraints.quantity_step <= 0
            || constraints.min_notional <= 0
            || 10_i128.checked_pow(constraints.quantity_scale).is_none()
        {
            return Err(PaperError::InvalidConfig("invalid exit constraints"));
        }
        self.exit_constraints
            .insert(symbol, ExitConstraintSource::Explicit(constraints));
        Ok(())
    }

    pub fn on_event(&mut self, event: BinanceMarketEvent) -> Vec<PaperRecord> {
        self.on_event_ref(&event)
    }

    /// Borrowed event path for multi-strategy paper labs. The market event is
    /// parsed once and then evaluated by every isolated ledger without cloning
    /// symbol strings or payload fields.
    pub fn on_event_ref(&mut self, event: &BinanceMarketEvent) -> Vec<PaperRecord> {
        self.event_count = self.event_count.saturating_add(1);
        let timestamp_ms = event_time_ms(event);
        self.last_event_at_ms = timestamp_ms;
        let mut records = self.complete_pending_cancels(timestamp_ms);
        records.extend(match event {
            BinanceMarketEvent::BookTicker(ticker) => self.on_book_ticker(ticker),
            BinanceMarketEvent::MarkPrice(mark) => self.on_mark_price(mark),
            BinanceMarketEvent::AggTrade(trade) => self.on_agg_trade(trade),
        });
        records
    }

    pub fn refresh_anchors(&mut self, anchors: BTreeMap<String, PaperAnchor>, timestamp_ms: u64) {
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
                // A completed exit session is reopened only by an explicit new
                // trading-day anchor while flat and without a tracked order.
                if candidate_day > current_day && state.position == 0 && state.working.is_none() {
                    state.exit_cutoff = false;
                    state.exit_session_plan = None;
                }
                state.anchor = anchor;
            }
        }
    }

    pub fn cancel_all(&mut self, timestamp_ms: u64, detail: &str) -> Vec<PaperRecord> {
        let symbols = self.states.keys().cloned().collect::<Vec<_>>();
        symbols
            .into_iter()
            .flat_map(|symbol| self.teardown_symbol(&symbol, timestamp_ms, detail))
            .collect()
    }

    /// Clock-driven exit processing used by replay ticks. It only records
    /// decisions/cancel acknowledgements; fills remain aggregate-trade facts.
    pub fn evaluate_exits_at(&mut self, timestamp_ms: u64) -> Result<Vec<PaperRecord>, PaperError> {
        let mut records = self.complete_pending_cancels(timestamp_ms);
        let symbols = self.states.keys().cloned().collect::<Vec<_>>();
        for symbol in symbols {
            records.extend(self.evaluate_exit_symbol(&symbol, timestamp_ms));
        }
        Ok(records)
    }

    pub fn summary(&self) -> PaperSummary {
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
        PaperSummary {
            event_count: self.event_count,
            order_count: self.order_count,
            fill_count: self.fill_count,
            filled_quantity: self.filled_quantity,
            rejected_entries: self.rejected_entries,
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

    pub fn metrics_snapshot(
        &self,
        observed_at_ms: u64,
        last_received_at_ms: u64,
    ) -> PaperMetricsSnapshot {
        let symbols = self
            .states
            .iter()
            .map(|(symbol, state)| {
                let requested_quantity = self
                    .position_allocations
                    .get(symbol)
                    .map(|allocation| allocation.requested_quantity)
                    .unwrap_or(self.requested_quantity);
                let (bid_price_ticks, ask_price_ticks) = state
                    .book
                    .map(|book| (Some(book.bid_price_ticks), Some(book.ask_price_ticks)))
                    .unwrap_or((None, None));
                let threshold = dynamic_threshold_for(
                    state,
                    self.strategy_variant,
                    self.strategy.entry_threshold_bps,
                    self.fee_ppm,
                    requested_quantity,
                )
                .map(|threshold| scale_threshold_non_fee(threshold, self.threshold_scale_ppm));
                let calendar_state = calendar_state_for(symbol, observed_at_ms);
                let data_quality =
                    data_quality_for(state, observed_at_ms, self.max_mark_index_gap_bps);
                let equity_entry_allowed = paper_session_allows_entry(symbol, observed_at_ms);
                let funding_known = !self.live_risk_gates
                    || (state.next_funding_time_ms > observed_at_ms
                        && state.latest_funding_rate_e8.is_some());
                let funding_allowed =
                    !self.live_risk_gates || funding_entry_allowed(state, observed_at_ms);
                let anchor_allowed = state
                    .anchor
                    .valid_at(observed_at_ms, self.max_anchor_age_ms)
                    && (!self.live_risk_gates
                        || state.anchor.observed_at_ms == 0
                        || paper_anchor_usable(
                            symbol,
                            state.anchor.observed_at_ms,
                            observed_at_ms,
                        ));
                let risk_state = if !equity_entry_allowed {
                    PaperRiskState::ReduceOnlyEquitySession
                } else if !funding_known {
                    PaperRiskState::HaltFundingMetadata
                } else if !funding_allowed {
                    PaperRiskState::ReduceOnlyFundingDeadline
                } else if !matches!(data_quality, DataQualityStatus::Fresh) {
                    PaperRiskState::HaltMarketData
                } else if !anchor_allowed {
                    PaperRiskState::HaltAnchor
                } else {
                    PaperRiskState::Trading
                };
                let anchor_age_ms = (state.anchor.observed_at_ms > 0)
                    .then(|| observed_at_ms.saturating_sub(state.anchor.observed_at_ms));
                let mark_age_ms = (state.last_mark_time_ms > 0)
                    .then(|| observed_at_ms.saturating_sub(state.last_mark_time_ms));
                let buy_edge_bps = bid_price_ticks
                    .and_then(|price| edge_bps(state.anchor.close_price_ticks, price));
                let sell_edge_bps = ask_price_ticks
                    .and_then(|price| edge_bps(price, state.anchor.close_price_ticks));
                let entry_block_reason = entry_block_reason_for(
                    state,
                    risk_state,
                    threshold,
                    buy_edge_bps,
                    sell_edge_bps,
                );
                PaperSymbolMetrics {
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
                    anchor_age_ms,
                    anchor_final_close: state.anchor.observed_at_ms == 0
                        || anchor_refresh_allowed(symbol, state.anchor.observed_at_ms),
                    calendar_state: calendar_state.to_owned(),
                    next_funding_time_ms: state.next_funding_time_ms,
                    latest_funding_rate_e8: state.latest_funding_rate_e8,
                    funding_flatten_deadline_ms: funding_flatten_deadline(
                        state.next_funding_time_ms,
                    ),
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
                    buy_edge_bps: bid_price_ticks
                        .and_then(|price| edge_bps(state.anchor.close_price_ticks, price)),
                    sell_edge_bps: ask_price_ticks
                        .and_then(|price| edge_bps(price, state.anchor.close_price_ticks)),
                    threshold: threshold.map(threshold_metrics),
                }
            })
            .collect();
        PaperMetricsSnapshot {
            observed_at_ms,
            strategy_variant: self.strategy_variant.label().to_owned(),
            last_market_event_at_ms: self.last_event_at_ms,
            last_received_at_ms,
            summary: self.summary(),
            symbols,
            history: Vec::new(),
            calendar_snapshot: "sse-hkex-2026".to_owned(),
            maker_fee_source: "binance_usdm_base_maker_schedule".to_owned(),
            funding_model: "mark_price_at_next_funding_time".to_owned(),
            capital_usdt_ticks: self.capital_usdt_ticks,
            capital_usdt: self.capital_usdt_ticks.map(|capital| {
                crate::execution::binance_wire::format_ticks(capital, self.price_scale)
            }),
            model_assumptions: PaperModelAssumptions {
                fill_model: "top_of_book_plus_aggregate_trade_queue".to_owned(),
                queue_ahead: self.realism.queue.visible_ahead,
                trade_through: self.realism.queue.trade_through,
                market_to_decision_ms: self.realism.latency.market_to_decision_ms,
                decision_to_exchange_ms: self.realism.latency.decision_to_exchange_ms,
                cancel_to_exchange_ms: self.realism.latency.cancel_to_exchange_ms,
                exit_constraints: "simulation: price_step=1, quantity_step=1, minimum_quantity=1, minimum_notional=1 quote tick; explicit constraints retain their source timestamp".to_owned(),
            },
        }
    }

    pub fn performance_point(&self, observed_at_ms: u64) -> PaperPerformancePoint {
        let summary = self.summary();
        PaperPerformancePoint {
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
                .map(|(symbol, state)| PaperSymbolPerformancePoint {
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
        history: &[PaperPerformancePoint],
    ) -> PaperMetricsSnapshot {
        let mut snapshot = self.metrics_snapshot(observed_at_ms, last_received_at_ms);
        snapshot.history = history.to_vec();
        snapshot
    }

    fn on_book_ticker(&mut self, ticker: &BookTicker) -> Vec<PaperRecord> {
        let symbol = ticker.symbol.to_ascii_uppercase();
        if let Some(state) = self.states.get_mut(&symbol) {
            state.book = Some(BookState {
                bid_price_ticks: ticker.bid_price.0,
                bid_quantity: ticker.bid_quantity.0,
                ask_price_ticks: ticker.ask_price.0,
                ask_quantity: ticker.ask_quantity.0,
                observed_at_ms: ticker.event_time_ms,
            });
            let mid = (i128::from(ticker.bid_price.0) + i128::from(ticker.ask_price.0)) / 2;
            if mid > 0 && ticker.ask_price.0 >= ticker.bid_price.0 {
                let spread_bps = ((i128::from(ticker.ask_price.0) - i128::from(ticker.bid_price.0))
                    * 10_000
                    / mid) as i64;
                state.ewma_spread_bps = ewma(state.ewma_spread_bps, spread_bps);
            }
        } else {
            return Vec::new();
        }
        self.rebalance_symbol(&symbol, ticker.event_time_ms)
    }

    fn on_mark_price(&mut self, mark: &MarkPrice) -> Vec<PaperRecord> {
        let symbol = mark.symbol.to_ascii_uppercase();
        let funding_settled = {
            let Some(state) = self.states.get_mut(&symbol) else {
                return Vec::new();
            };
            if let Some(previous) = state.last_mark_price_ticks {
                if previous > 0 && mark.mark_price.0 > 0 {
                    let change = i128::from(mark.mark_price.0) - i128::from(previous);
                    let market_pnl = change * i128::from(state.position)
                        / quantity_scale_multiplier(self.quantity_scale);
                    state.market_pnl_ticks = state
                        .market_pnl_ticks
                        .saturating_add(clamp_i128(market_pnl));
                    let change_bps = (change.abs() * 10_000 / i128::from(previous)) as i64;
                    state.ewma_abs_return_bps = ewma(state.ewma_abs_return_bps, change_bps);
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
            state.latest_funding_rate_e8 = mark.latest_funding_rate_e8;
            state.next_funding_time_ms = mark.next_funding_time_ms;
            state.last_mark_time_ms = mark.event_time_ms;
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
                    client_id: None,
                    side: None,
                    price_ticks: Some(mark.mark_price.0),
                    quantity: Some(state.position.checked_abs().unwrap_or(i64::MAX)),
                    detail: Some("estimated funding settlement from mark stream"),
                },
            ));
        }
        records
    }

    fn on_agg_trade(&mut self, trade: &AggTrade) -> Vec<PaperRecord> {
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
            if trade.event_time_ms
                < order
                    .placed_at_ms
                    .saturating_add(realism.latency.total_entry_ms())
            {
                return Vec::new();
            }
            let Some(book) = state.book else {
                return Vec::new();
            };
            let fill_quantity = realism.evaluate_after_latency(
                MakerQuote {
                    side: order.side,
                    price_ticks: order.price_ticks,
                    quantity: order.remaining_quantity,
                },
                TopOfBook {
                    bid_price_ticks: book.bid_price_ticks,
                    ask_price_ticks: book.ask_price_ticks,
                    bid_quantity: book.bid_quantity,
                    ask_quantity: book.ask_quantity,
                },
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
                client_id: Some(order.client_id),
                side: Some(order.side),
                price_ticks: Some(order.price_ticks),
                quantity: Some(quantity),
                detail: Some(if state.working.is_some() {
                    "partial maker fill"
                } else {
                    "complete maker fill"
                }),
            },
        )]
    }

    fn exit_constraints_for(
        &self,
        symbol: &str,
        max_position: i64,
        now_ms: u64,
    ) -> Option<ExitConstraints> {
        match self.exit_constraints.get(symbol).copied()? {
            ExitConstraintSource::Simulated => Some(ExitConstraints {
                min_price: 1,
                // This is deliberately finite; paper does not claim fetched filters.
                max_price: i64::MAX,
                price_tick: 1,
                min_quantity: 1,
                max_quantity: max_position,
                quantity_step: 1,
                min_notional: 1,
                quantity_scale: self.quantity_scale,
                observed_at_ms: now_ms,
                max_age_ms: 0,
            }),
            ExitConstraintSource::Explicit(value) => Some(value),
        }
    }

    fn exit_plan_for(
        state: &PaperSymbolState,
        symbol: &str,
        now_ms: u64,
        live_risk_gates: bool,
    ) -> DualFlattenPlan {
        let equity_open_at_ms = profile_for(symbol)
            .and_then(|profile| calendar_for(profile.region).exit_deadline_at(now_ms));
        let funding = if state.next_funding_time_ms > 0 {
            // A received schedule remains meaningful at its deadline even when
            // replay has already passed it, so do not turn it into "unknown".
            FundingSchedule {
                next_funding_at_ms: Some(state.next_funding_time_ms),
                funding_interval_hours: None,
                estimated_rate_ppm: state.latest_funding_rate_e8,
                rate_kind: FundingRateKind::Regular,
                status: FundingScheduleStatus::Scheduled,
                observed_at_ms: state.last_mark_time_ms,
            }
        } else if !live_risk_gates && now_ms < 10_000_000_000 {
            // A missing epoch fixture is explicitly simulated as no-funding;
            // positive supplied deadlines are always honored above.
            FundingSchedule::no_event(None, None, FundingRateKind::Regular, now_ms)
                .expect("no-event funding schedule is structurally valid")
        } else {
            FundingSchedule::new(None, None, None, FundingRateKind::Unknown, now_ms)
                .expect("unknown funding schedule is structurally valid")
        };
        DualFlattenPlan::new(
            now_ms,
            equity_open_at_ms,
            funding,
            30 * 60 * 1_000,
            FUNDING_FLATTEN_LEAD_MS,
        )
        .expect("fixed nonzero flatten windows")
    }

    /// Returns `Some` when the shared core owns this tick; `None` leaves the
    /// legacy entry alpha path untouched while trading is still admissible.
    fn evaluate_exit_symbol(&mut self, symbol: &str, timestamp_ms: u64) -> Vec<PaperRecord> {
        let allocation = self.position_allocations.get(symbol);
        let max_position = allocation
            .map(|allocation| allocation.max_position)
            .unwrap_or(self.max_position);
        let constraints = self.exit_constraints_for(symbol, max_position, timestamp_ms);
        let (input, cutoff) = {
            let state = self.states.get_mut(symbol).expect("symbol state exists");
            let current_plan =
                Self::exit_plan_for(state, symbol, timestamp_ms, self.live_risk_gates);
            if current_plan.phase_at(timestamp_ms, true) != crate::strategy::FlattenPhase::Trading {
                state.exit_cutoff = true;
                state.exit_session_plan = Some(current_plan);
            }
            let plan = state.exit_session_plan.unwrap_or(current_plan);
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
            let book = state.book.map(|book| ExitBook {
                bid: book.bid_price_ticks,
                ask: book.ask_price_ticks,
                observed_at_ms: book.observed_at_ms,
            });
            (
                MakerExitInput {
                    symbol: state.symbol_id,
                    position: state.position,
                    position_confirmed: true,
                    now_ms: timestamp_ms,
                    max_book_age_ms: 5_000,
                    plan,
                    book,
                    constraints,
                    working,
                },
                state.exit_cutoff,
            )
        };
        let pending_residual = input.position != 0
            && matches!(input.working, ExitWorkingOrder::Pending)
            && input.plan.phase_at(timestamp_ms, true)
                == crate::strategy::FlattenPhase::ResidualExposure;
        let decision = decide_maker_exit(input);
        match decision {
            MakerExitDecision::Trading if !cutoff => Vec::new(),
            MakerExitDecision::Trading
            | MakerExitDecision::Flat
            | MakerExitDecision::KeepWorking => Vec::new(),
            MakerExitDecision::WaitForReconciliation if pending_residual => {
                let state = &self.states[symbol];
                vec![self.record(
                    symbol,
                    state,
                    timestamp_ms,
                    RecordFields {
                        kind: "residual_exposure",
                        client_id: None,
                        side: None,
                        price_ticks: None,
                        quantity: Some(state.position.checked_abs().unwrap_or(i64::MAX)),
                        detail: Some("hard deadline reached while cancellation is pending"),
                    },
                )]
            }
            MakerExitDecision::WaitForReconciliation => Vec::new(),
            MakerExitDecision::Submit(intent) => {
                self.place_symbol(symbol, intent, timestamp_ms, true)
            }
            MakerExitDecision::CancelWorking => {
                self.cancel_symbol(symbol, timestamp_ms, "shared maker exit cancellation")
            }
            MakerExitDecision::ResidualExposure => {
                let risk_increasing = self.states[symbol]
                    .working
                    .is_some_and(|order| !order.reduce_only);
                let mut records = if risk_increasing {
                    self.cancel_symbol(
                        symbol,
                        timestamp_ms,
                        "hard deadline cancels risk-increasing order",
                    )
                } else {
                    Vec::new()
                };
                let state = &self.states[symbol];
                records.push(self.record(
                    symbol,
                    state,
                    timestamp_ms,
                    RecordFields {
                        kind: "residual_exposure",
                        client_id: None,
                        side: None,
                        price_ticks: None,
                        quantity: Some(state.position.checked_abs().unwrap_or(i64::MAX)),
                        detail: Some("hard deadline reached; no replacement order submitted"),
                    },
                ));
                records
            }
            MakerExitDecision::Blocked(reason) => {
                let mut records = if self.states[symbol].working.is_some() {
                    self.cancel_symbol(symbol, timestamp_ms, "shared maker exit blocked")
                } else {
                    Vec::new()
                };
                let state = &self.states[symbol];
                records.push(self.record(
                    symbol,
                    state,
                    timestamp_ms,
                    RecordFields {
                        kind: "exit_blocked",
                        client_id: None,
                        side: None,
                        price_ticks: None,
                        quantity: Some(state.position.checked_abs().unwrap_or(i64::MAX)),
                        detail: Some(match reason {
                            crate::strategy::ExitBlockReason::InvalidInput => "invalid exit input",
                            crate::strategy::ExitBlockReason::InvalidBook => "invalid exit book",
                            crate::strategy::ExitBlockReason::StaleBook => "stale exit book",
                            crate::strategy::ExitBlockReason::MissingConstraints => {
                                "missing exit constraints"
                            }
                            crate::strategy::ExitBlockReason::InvalidConstraints => {
                                "invalid exit constraints"
                            }
                            crate::strategy::ExitBlockReason::StaleConstraints => {
                                "stale exit constraints"
                            }
                            crate::strategy::ExitBlockReason::Dust => "exit residual is dust",
                        }),
                    },
                ));
                records
            }
        }
    }

    fn rebalance_symbol(&mut self, symbol: &str, timestamp_ms: u64) -> Vec<PaperRecord> {
        let exit_records = self.evaluate_exit_symbol(symbol, timestamp_ms);
        let exit_active = self.states[symbol].exit_cutoff || !exit_records.is_empty();
        if exit_active {
            return exit_records;
        }
        let allocation = self.position_allocations.get(symbol);
        let max_position = allocation
            .map(|allocation| allocation.max_position)
            .unwrap_or(self.max_position);
        let requested_quantity = allocation
            .map(|allocation| allocation.requested_quantity)
            .unwrap_or(self.requested_quantity);
        let strategy_variant = self.strategy_variant;
        let (desired, reduce_only, has_working) = {
            let state = self.states.get(symbol).expect("symbol state exists");
            let Some(book) = state.book else {
                return Vec::new();
            };
            let entries_allowed = paper_session_allows_entry(symbol, timestamp_ms)
                && (!self.live_risk_gates || funding_entry_allowed(state, timestamp_ms));
            if !entries_allowed {
                let side = if state.position > 0 {
                    Some(Side::Sell)
                } else if state.position < 0 {
                    Some(Side::Buy)
                } else {
                    None
                };
                let desired = side.map(|side| OrderIntent {
                    symbol: state.symbol_id,
                    side,
                    price: if side == Side::Sell {
                        book.bid_price_ticks
                    } else {
                        book.ask_price_ticks
                    },
                    quantity: state.position.checked_abs().unwrap_or(i64::MAX),
                    post_only: true,
                });
                (desired, true, state.working.is_some())
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
                        || paper_anchor_usable(symbol, state.anchor.observed_at_ms, timestamp_ms));
                if !valid {
                    (None, false, state.working.is_some())
                } else {
                    let quantity = requested_quantity
                        .min(book.bid_quantity.max(1))
                        .min(book.ask_quantity.max(1));
                    let threshold = dynamic_threshold_for(
                        state,
                        strategy_variant,
                        self.strategy.entry_threshold_bps,
                        self.fee_ppm,
                        quantity,
                    )
                    .map(|threshold| scale_threshold_non_fee(threshold, self.threshold_scale_ppm));
                    let intent = if strategy_variant == PaperStrategyVariant::M0Fixed {
                        self.strategy.generate_intent(
                            state.symbol_id,
                            book.bid_price_ticks,
                            book.ask_price_ticks,
                            state.anchor.close_price_ticks,
                            quantity,
                        )
                    } else {
                        let fill_probability_bps =
                            fill_probability_bps(quantity, book.bid_quantity, book.ask_quantity);
                        let (buy_micro_adverse_bps, sell_micro_adverse_bps) =
                            side_adverse_selection_bps(book.bid_quantity, book.ask_quantity);
                        let input = threshold.map(|threshold| SignalInput {
                            symbol: state.symbol_id,
                            anchor: crate::strategy::PriceTicks(state.anchor.close_price_ticks),
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
                            inventory_skew_bps: 50,
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
                            fill_probability_bps,
                            confidence_bps: if signal_age_ms <= 1_000 { 9_000 } else { 7_000 },
                            fill_aware: strategy_variant.uses_fill_gate(),
                            max_mark_index_gap_bps: self.max_mark_index_gap_bps,
                            signal_age_ms,
                            max_signal_age_ms: 5_000,
                        });
                        input.and_then(AnchorMakerStrategy::generate_adaptive_intent)
                    };
                    (intent, false, state.working.is_some())
                }
            }
        };
        if desired.is_none() {
            if has_working {
                return self.cancel_symbol(
                    symbol,
                    timestamp_ms,
                    "session, signal, or data gate blocked",
                );
            }
            self.rejected_entries = self.rejected_entries.saturating_add(1);
            return Vec::new();
        }
        let desired = desired.expect("desired intent exists");
        let same_order = self.states[symbol].working.is_some_and(|order| {
            order.side == desired.side
                && order.price_ticks == desired.price
                && order.remaining_quantity >= desired.quantity
                && order.reduce_only == reduce_only
        });
        if same_order {
            return Vec::new();
        }
        if has_working {
            // A replacement waits for the modeled cancel acknowledgement.
            return self.cancel_symbol(symbol, timestamp_ms, "quote replacement");
        }
        self.place_symbol(symbol, desired, timestamp_ms, reduce_only)
    }

    fn place_symbol(
        &mut self,
        symbol: &str,
        intent: OrderIntent,
        timestamp_ms: u64,
        reduce_only: bool,
    ) -> Vec<PaperRecord> {
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
            self.rejected_entries = self.rejected_entries.saturating_add(1);
            return Vec::new();
        }
        if reduce_only
            && ((intent.side == Side::Buy && state.position >= 0)
                || (intent.side == Side::Sell && state.position <= 0))
        {
            return Vec::new();
        }
        state.working = Some(WorkingOrder {
            client_id,
            side: intent.side,
            price_ticks: intent.price,
            remaining_quantity: intent.quantity,
            reduce_only,
            placed_at_ms: timestamp_ms,
            cancel_requested_at_ms: None,
        });
        self.order_count = self.order_count.saturating_add(1);
        let state = self.states.get(symbol).expect("symbol state exists");
        vec![self.record(
            symbol,
            state,
            timestamp_ms,
            RecordFields {
                kind: "order_placed",
                client_id: Some(client_id),
                side: Some(intent.side),
                price_ticks: Some(intent.price),
                quantity: Some(intent.quantity),
                detail: Some(if reduce_only {
                    "reduce-only maker paper order"
                } else {
                    "maker-only paper order"
                }),
            },
        )]
    }

    fn cancel_symbol(&mut self, symbol: &str, timestamp_ms: u64, detail: &str) -> Vec<PaperRecord> {
        let cancel_delay_ms = self.realism.latency.cancel_to_exchange_ms;
        let order = {
            let Some(state) = self.states.get_mut(symbol) else {
                return Vec::new();
            };
            let Some(order) = state.working else {
                return Vec::new();
            };
            if order.cancel_requested_at_ms.is_some() {
                return Vec::new();
            }
            if cancel_delay_ms == 0 {
                state.working.take().expect("working order was checked")
            } else {
                state
                    .working
                    .as_mut()
                    .expect("working order was checked")
                    .cancel_requested_at_ms = Some(timestamp_ms);
                order
            }
        };
        let state = self.states.get(symbol).expect("symbol state exists");
        vec![self.record(
            symbol,
            state,
            timestamp_ms,
            RecordFields {
                kind: if cancel_delay_ms == 0 {
                    "order_canceled"
                } else {
                    "cancel_requested"
                },
                client_id: Some(order.client_id),
                side: Some(order.side),
                price_ticks: Some(order.price_ticks),
                quantity: Some(order.remaining_quantity),
                detail: Some(detail),
            },
        )]
    }

    fn complete_pending_cancels(&mut self, timestamp_ms: u64) -> Vec<PaperRecord> {
        let cancel_delay_ms = self.realism.latency.cancel_to_exchange_ms;
        if cancel_delay_ms == 0 {
            return Vec::new();
        }
        let symbols = self.states.keys().cloned().collect::<Vec<_>>();
        let mut completed = Vec::new();
        for symbol in symbols {
            let Some(order) = self.states.get(&symbol).and_then(|state| state.working) else {
                continue;
            };
            let Some(requested_at_ms) = order.cancel_requested_at_ms else {
                continue;
            };
            let acknowledged_at_ms = requested_at_ms.saturating_add(cancel_delay_ms);
            if timestamp_ms < acknowledged_at_ms {
                continue;
            }
            let order = self
                .states
                .get_mut(&symbol)
                .and_then(|state| state.working.take())
                .expect("pending cancellation retained its working order");
            let state = &self.states[&symbol];
            completed.push(self.record(
                &symbol,
                state,
                acknowledged_at_ms,
                RecordFields {
                    kind: "order_canceled",
                    client_id: Some(order.client_id),
                    side: Some(order.side),
                    price_ticks: Some(order.price_ticks),
                    quantity: Some(order.remaining_quantity),
                    detail: Some("modeled cancel acknowledgement"),
                },
            ));
        }
        completed
    }

    fn teardown_symbol(
        &mut self,
        symbol: &str,
        timestamp_ms: u64,
        detail: &str,
    ) -> Vec<PaperRecord> {
        let order = self
            .states
            .get_mut(symbol)
            .and_then(|state| state.working.take());
        let Some(order) = order else {
            return Vec::new();
        };
        let state = &self.states[symbol];
        vec![self.record(
            symbol,
            state,
            timestamp_ms,
            RecordFields {
                kind: "order_teardown",
                client_id: Some(order.client_id),
                side: Some(order.side),
                price_ticks: Some(order.price_ticks),
                quantity: Some(order.remaining_quantity),
                detail: Some(detail),
            },
        )]
    }

    fn record(
        &self,
        symbol: &str,
        state: &PaperSymbolState,
        timestamp_ms: u64,
        fields: RecordFields<'_>,
    ) -> PaperRecord {
        PaperRecord {
            timestamp_ms,
            strategy_variant: self.strategy_variant.label().to_owned(),
            kind: fields.kind.to_owned(),
            symbol: symbol.to_owned(),
            client_id: fields.client_id,
            side: fields.side.map(side_name),
            price_ticks: fields.price_ticks,
            quantity: fields.quantity,
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
        }
    }
}

fn event_time_ms(event: &BinanceMarketEvent) -> u64 {
    match event {
        BinanceMarketEvent::BookTicker(value) => value.event_time_ms,
        BinanceMarketEvent::MarkPrice(value) => value.event_time_ms,
        BinanceMarketEvent::AggTrade(value) => value.event_time_ms,
    }
}

fn funding_flatten_deadline(next_funding_time_ms: u64) -> Option<u64> {
    (next_funding_time_ms > 0).then(|| next_funding_time_ms.saturating_sub(FUNDING_FLATTEN_LEAD_MS))
}

fn funding_entry_allowed(state: &PaperSymbolState, now_ms: u64) -> bool {
    state.next_funding_time_ms > now_ms
        && state.latest_funding_rate_e8.is_some()
        && funding_flatten_deadline(state.next_funding_time_ms)
            .is_some_and(|deadline| now_ms < deadline)
}

fn entry_block_reason_for(
    state: &PaperSymbolState,
    risk_state: PaperRiskState,
    threshold: Option<AdaptiveThreshold>,
    buy_edge_bps: Option<i64>,
    sell_edge_bps: Option<i64>,
) -> &'static str {
    match risk_state {
        PaperRiskState::ReduceOnlyEquitySession => "equity_session_open",
        PaperRiskState::ReduceOnlyFundingDeadline => "funding_deadline",
        PaperRiskState::HaltFundingMetadata => "funding_metadata_missing",
        PaperRiskState::HaltMarketData => "market_data_not_fresh",
        PaperRiskState::HaltAnchor => "anchor_not_usable",
        PaperRiskState::Trading => {
            if state.book.is_none() {
                "quote_missing"
            } else {
                let Some(required_bps) = threshold.and_then(AdaptiveThreshold::required_bps) else {
                    return "threshold_unavailable";
                };
                let edge_reaches_threshold = buy_edge_bps.is_some_and(|edge| edge >= required_bps)
                    || sell_edge_bps.is_some_and(|edge| edge >= required_bps);
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
    state: &PaperSymbolState,
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

fn edge_bps(numerator_price: i64, denominator_price: i64) -> Option<i64> {
    if numerator_price <= 0 || denominator_price <= 0 {
        return None;
    }
    Some(
        ((i128::from(numerator_price) - i128::from(denominator_price)) * 10_000
            / i128::from(denominator_price))
        .clamp(i128::from(i64::MIN), i128::from(i64::MAX)) as i64,
    )
}

fn dynamic_threshold_for(
    state: &PaperSymbolState,
    variant: PaperStrategyVariant,
    floor_bps: i64,
    fee_ppm: i64,
    requested_quantity: i64,
) -> Option<AdaptiveThreshold> {
    let book = state.book?;
    let mark = state.mark_price_ticks?;
    let index = state.index_price_ticks?;
    let gap_bps = bps_between(mark, index);
    let volatility_bps = state.ewma_abs_return_bps.saturating_mul(3);
    let cost_bps = ppm_to_bps(fee_ppm.saturating_mul(2));
    let uncertainty_bps = gap_bps / 2 + 5;
    let spread_bps = state.ewma_spread_bps / 2;
    let liquidity_bps =
        liquidity_penalty_bps(requested_quantity, book.bid_quantity, book.ask_quantity);
    let adverse_selection_bps = if variant.uses_microstructure() {
        state.ewma_abs_return_bps.saturating_mul(2)
    } else {
        0
    };
    let statistical_bps = if variant.uses_statistical_term() {
        state.ewma_abs_return_bps.saturating_mul(8)
    } else {
        0
    };
    AdaptiveThreshold::from_components(
        floor_bps,
        if variant == PaperStrategyVariant::M0Fixed {
            0
        } else {
            volatility_bps
        },
        if variant == PaperStrategyVariant::M0Fixed {
            0
        } else {
            cost_bps
        },
        if variant == PaperStrategyVariant::M0Fixed {
            0
        } else {
            uncertainty_bps
        },
        0,
        if variant == PaperStrategyVariant::M0Fixed {
            0
        } else {
            5
        },
        if variant == PaperStrategyVariant::M0Fixed {
            0
        } else {
            spread_bps
        },
        adverse_selection_bps,
        if variant == PaperStrategyVariant::M0Fixed {
            0
        } else {
            liquidity_bps
        },
        0,
        statistical_bps,
    )
}

fn scale_threshold_non_fee(threshold: AdaptiveThreshold, scale_ppm: i64) -> AdaptiveThreshold {
    let scale = |value: i64| value.saturating_mul(scale_ppm.clamp(0, 1_000_000)) / 1_000_000;
    AdaptiveThreshold {
        floor_bps: scale(threshold.floor_bps),
        residual_volatility_bps: scale(threshold.residual_volatility_bps),
        cost_bps: threshold.cost_bps,
        uncertainty_bps: scale(threshold.uncertainty_bps),
        deadline_risk_bps: threshold.deadline_risk_bps,
        safety_margin_bps: scale(threshold.safety_margin_bps),
        spread_bps: scale(threshold.spread_bps),
        adverse_selection_bps: scale(threshold.adverse_selection_bps),
        liquidity_bps: scale(threshold.liquidity_bps),
        inventory_bps: scale(threshold.inventory_bps),
        statistical_bps: scale(threshold.statistical_bps),
    }
}

fn threshold_metrics(threshold: AdaptiveThreshold) -> PaperThresholdMetrics {
    PaperThresholdMetrics {
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
        required_bps: threshold.required_bps(),
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

fn bps_between(left: i64, right: i64) -> i64 {
    if left <= 0 || right <= 0 {
        return i64::MAX;
    }
    (((i128::from(left) - i128::from(right)).abs() * 10_000) / i128::from(right))
        .clamp(0, i128::from(i64::MAX)) as i64
}

fn ppm_to_bps(ppm: i64) -> i64 {
    if ppm <= 0 {
        return 0;
    }
    ((i128::from(ppm) + 99) / 100).clamp(0, i128::from(i64::MAX)) as i64
}

fn liquidity_penalty_bps(quantity: i64, bid_quantity: i64, ask_quantity: i64) -> i64 {
    if quantity <= 0 || bid_quantity <= 0 || ask_quantity <= 0 {
        return i64::MAX;
    }
    let worst_depth = bid_quantity.min(ask_quantity);
    if quantity > worst_depth {
        100
    } else if i128::from(quantity) * 2 > i128::from(worst_depth) {
        25
    } else if i128::from(quantity) * 10 > i128::from(worst_depth) {
        10
    } else {
        0
    }
}

fn fill_probability_bps(quantity: i64, bid_quantity: i64, ask_quantity: i64) -> u16 {
    if quantity <= 0 || bid_quantity <= 0 || ask_quantity <= 0 {
        return 0;
    }
    let depth = bid_quantity.min(ask_quantity);
    if i128::from(quantity) * 10 <= i128::from(depth) {
        8_000
    } else if i128::from(quantity) * 2 <= i128::from(depth) {
        6_000
    } else if quantity <= depth {
        4_000
    } else {
        2_000
    }
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

fn paper_anchor_usable(symbol: &str, anchor_observed_at_ms: u64, now_ms: u64) -> bool {
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
                anchor_refresh_allowed(symbol, now_ms)
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
    calendar.after_final_close(date_key, weekday, minute)
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

fn paper_session_allows_entry(symbol: &str, timestamp_ms: u64) -> bool {
    let Some(profile) = profile_for(symbol) else {
        return false;
    };
    let weekday = local_weekday(timestamp_ms);
    let minute = local_minute(timestamp_ms);
    let calendar = calendar_for(profile.region);
    let date_key = EquitySessionCalendar::date_key_from_timestamp(timestamp_ms);
    if !crate::strategy::calendar::EquitySessionCalendar::calendar_snapshot_supported(date_key)
        && timestamp_ms >= 10_000_000_000
    {
        return false;
    }
    calendar.entry_allowed_on_date(date_key, weekday, minute, 30, true, true)
        || (timestamp_ms < 10_000_000_000
            && matches!(
                calendar.detailed_state_at(weekday, minute, false, 30, true),
                VenueSessionState::Closed | VenueSessionState::MiddayBreak
            ))
}

fn apply_position_fill(
    state: &mut PaperSymbolState,
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

fn unrealized_pnl(state: &PaperSymbolState, quantity_scale: u32) -> Option<i64> {
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
pub struct PaperRunConfig {
    pub environment: BinanceEnvironment,
    pub strategy_variant: PaperStrategyVariant,
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
}

#[derive(Debug, Clone)]
pub struct PaperRunResult {
    pub summary: PaperSummary,
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

// The explicit arguments keep the paper run's strategy assumptions visible at the call site.
#[allow(clippy::too_many_arguments)]
pub async fn run_live(
    config: PaperRunConfig,
    anchors: BTreeMap<String, PaperAnchor>,
    entry_threshold_bps: i64,
    max_position: i64,
    requested_quantity: i64,
    max_mark_index_gap_bps: i64,
    max_anchor_age_ms: u64,
    fee_ppm: i64,
    output_path: Option<PathBuf>,
) -> Result<PaperRunResult, PaperError> {
    if config.symbols.is_empty() {
        return Err(PaperError::InvalidConfig("symbols are required"));
    }
    // Zero is the explicit continuous-paper mode. The long timeout keeps the
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
        return Err(PaperError::InvalidConfig(
            "symbols must be selected execution-universe TradFi instruments",
        ));
    }
    let mut fx_currencies = Vec::new();
    for symbol in &config.symbols {
        let currency = profile_for(symbol)
            .map(|profile| profile.anchor_currency)
            .ok_or(PaperError::InvalidConfig("missing instrument profile"))?;
        if !fx_currencies.contains(&currency) {
            fx_currencies.push(currency);
        }
    }
    let fx_client = BinanceC2cFxClient::new(config.http_proxy.as_deref())
        .map_err(|error| PaperError::Market(format!("FX client: {error}")))?;
    let fx_poller = BinanceC2cFxPoller::new(
        fx_client,
        &fx_currencies,
        FxPollerConfig {
            refresh_interval_ms: config.fx_refresh_ms,
            max_stale_ms: config.fx_max_age_ms,
            max_backoff_ms: 30_000,
        },
    )
    .map_err(|error| PaperError::Market(format!("FX poller: {error}")))?;
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
    .map_err(|error| PaperError::Market(error.to_string()))?;
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
        .map_err(|error| PaperError::Market(error.to_string()))?,
    );
    let mut engine = PaperEngine::new(
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
    let (fx_tx, mut fx_rx) = mpsc::channel::<FxUpdate>(128);
    let mut fx_task = tokio::spawn(fx_poller.run(fx_tx));
    let (anchor_tx, mut anchor_rx) = mpsc::channel::<BTreeMap<String, PaperAnchor>>(1);
    let mut anchor_task = if config.index_anchor_refresh_ms > 0 {
        let environment = config.environment;
        let symbols = config.symbols.clone();
        let price_scale = config.price_scale;
        let http_proxy = config.http_proxy.clone();
        let refresh_ms = config.index_anchor_refresh_ms;
        Some(tokio::spawn(async move {
            loop {
                tokio::time::sleep(Duration::from_millis(refresh_ms.max(1_000))).await;
                if let Ok(anchor_set) = load_binance_index_anchor_set(
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
    let (event_tx, mut event_rx) = mpsc::channel::<BinanceMarketEvent>(4096);
    let event_dropped = Arc::new(AtomicU64::new(0));
    let mut shard_tasks = tokio::task::JoinSet::new();
    for shard_config in shard_configs {
        let event_tx = event_tx.clone();
        let event_dropped = Arc::clone(&event_dropped);
        shard_tasks.spawn(async move {
            let mut stream = BinanceMarketStream::new(shard_config);
            stream
                .run_until_error(|event| {
                    if event_tx.try_send(event).is_err() {
                        event_dropped.fetch_add(1, Ordering::Relaxed);
                    }
                })
                .await
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
                        return Err(PaperError::Market(
                            "all market shards stopped".to_owned(),
                        ));
                    };
                    let received_at_ms = now_ms();
                    last_received_at_ms = received_at_ms;
                    let market_line = serde_json::to_string(&market_event_to_json(
                        &event,
                        price_scale,
                        quantity_scale,
                        Some(received_at_ms),
                    ))
                    .unwrap_or_else(|_| "{}".to_owned());
                    if market_tx.try_send(market_line).is_err() {
                        market_dropped.fetch_add(1, Ordering::Relaxed);
                    }
                    for record in engine.on_event(event) {
                        let line = serde_json::to_string(&record)
                            .unwrap_or_else(|_| "{}".to_owned());
                        if record_tx.try_send(line).is_err() {
                            dropped.fetch_add(1, Ordering::Relaxed);
                        }
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
                        return Err(PaperError::Market(
                            "FX feed stopped".to_owned(),
                        ));
                    };
                    fx_last_update_at_ms = fx_last_update_at_ms.max(update.observed_at_ms);
                    fx_latest_by_currency.insert(update.currency.clone(), update.clone());
                    let line = serde_json::to_string(&update)
                        .unwrap_or_else(|_| "{}".to_owned());
                    if fx_record_tx.try_send(line).is_err() {
                        fx_dropped.fetch_add(1, Ordering::Relaxed);
                    }
                }
                fx_joined = &mut fx_task => {
                    match fx_joined {
                        Ok(Ok(())) => {
                            return Err(PaperError::Market(
                                "FX feed stopped".to_owned(),
                            ));
                        }
                        Ok(Err(error)) => {
                            return Err(PaperError::Market(format!(
                                "FX feed failed: {error}"
                            )));
                        }
                        Err(error) => {
                            return Err(PaperError::Market(format!(
                                "FX feed task failed: {error}"
                            )));
                        }
                    }
                }
                joined = shard_tasks.join_next() => {
                    match joined {
                        Some(Ok(Ok(()))) => {
                            return Err(PaperError::Market(
                                "market shard stopped".to_owned(),
                            ));
                        }
                        Some(Ok(Err(error))) => {
                            return Err(PaperError::Market(error.to_string()));
                        }
                        Some(Err(error)) => {
                            return Err(PaperError::Market(format!(
                                "market shard task failed: {error}"
                            )));
                        }
                        None => {
                            return Err(PaperError::Market(
                                "all market shards stopped".to_owned(),
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

    for record in engine.cancel_all(now_ms(), "paper run stopped") {
        let line = serde_json::to_string(&record)?;
        if record_tx.try_send(line).is_err() {
            dropped.fetch_add(1, Ordering::Relaxed);
        }
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
        .map_err(|error| PaperError::Io(error.to_string()))??;
    let market_records_written = market_writer
        .await
        .map_err(|error| PaperError::Io(error.to_string()))??;
    let fx_records_written = fx_record_writer
        .await
        .map_err(|error| PaperError::Io(error.to_string()))??;
    let fx_fresh_at_end = !fx_currencies.is_empty()
        && fx_currencies.iter().all(|currency| {
            fx_latest_by_currency
                .get(currency.as_str())
                .is_some_and(|update| update.is_fresh_at(now_ms(), config.fx_max_age_ms))
        });
    let stopped_by_duration = match run_result {
        Err(_) if config.duration_secs != 0 => true,
        Err(_) => return Err(PaperError::Market("continuous paper timeout".to_owned())),
        Ok(Ok(())) => false,
        Ok(Err(error)) => return Err(error),
    };
    let event_dropped = event_dropped.load(Ordering::Relaxed);
    if event_dropped != 0 {
        return Err(PaperError::Market(format!(
            "market event queue dropped {event_dropped} events"
        )));
    }
    Ok(PaperRunResult {
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

fn event_symbol(event: &BinanceMarketEvent) -> &str {
    match event {
        BinanceMarketEvent::BookTicker(value) => &value.symbol,
        BinanceMarketEvent::MarkPrice(value) => &value.symbol,
        BinanceMarketEvent::AggTrade(value) => &value.symbol,
    }
}

// The explicit arguments keep replay assumptions visible and deterministic.
#[allow(clippy::too_many_arguments)]
pub fn replay_jsonl(
    input_path: &Path,
    output_path: Option<&Path>,
    anchors: BTreeMap<String, PaperAnchor>,
    price_scale: u32,
    quantity_scale: u32,
    entry_threshold_bps: i64,
    max_position: i64,
    requested_quantity: i64,
    max_mark_index_gap_bps: i64,
    max_anchor_age_ms: u64,
    fee_ppm: i64,
) -> Result<PaperSummary, PaperError> {
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
    anchors: BTreeMap<String, PaperAnchor>,
    price_scale: u32,
    quantity_scale: u32,
    entry_threshold_bps: i64,
    max_position: i64,
    requested_quantity: i64,
    max_mark_index_gap_bps: i64,
    max_anchor_age_ms: u64,
    fee_ppm: i64,
    realism: crate::backtest::realism::RealisticFillModel,
) -> Result<PaperSummary, PaperError> {
    let mut engine = PaperEngine::new(
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
        .map_err(|error| PaperError::ReplayParse {
            line: line_number,
            error,
        })?;
        let symbol = event_symbol(&event).to_ascii_uppercase();
        if !engine.states.contains_key(&symbol) {
            return Err(PaperError::ReplaySymbolNotConfigured(symbol));
        }
        let event_timestamp_ms = match &event {
            BinanceMarketEvent::BookTicker(value) => value.event_time_ms,
            BinanceMarketEvent::MarkPrice(value) => value.event_time_ms,
            BinanceMarketEvent::AggTrade(value) => value.event_time_ms,
        };
        let timestamp_ms = received_at_ms.unwrap_or(event_timestamp_ms);
        if previous_ms.is_some_and(|previous| timestamp_ms < previous) {
            return Err(PaperError::ReplayOutOfOrder {
                previous_ms: previous_ms.unwrap(),
                current_ms: timestamp_ms,
            });
        }
        previous_ms = Some(timestamp_ms);
        for record in engine.on_event(event) {
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

    fn anchors() -> BTreeMap<String, PaperAnchor> {
        [(
            "CXMTUSDT".to_owned(),
            PaperAnchor {
                close_price_ticks: 100,
                observed_at_ms: 0,
                valid_until_ms: 0,
            },
        )]
        .into_iter()
        .collect()
    }

    fn engine() -> PaperEngine {
        PaperEngine::new(anchors(), 100, 100, 10, 20, 0, 0, 0).unwrap()
    }

    fn feed(engine: &mut PaperEngine, raw: &[u8]) -> Vec<PaperRecord> {
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
    fn paper_order_fills_only_on_compatible_aggressor_at_exact_price() {
        let mut engine = engine();
        feed(
            &mut engine,
            br#"{"e":"markPriceUpdate","E":1,"s":"CXMTUSDT","p":"100","i":"100","T":600000,"r":"0"}"#,
        );
        let placed = feed(
            &mut engine,
            br#"{"e":"bookTicker","u":1,"E":2,"T":2,"s":"CXMTUSDT","b":"98","B":"10","a":"99","A":"10"}"#,
        );
        assert_eq!(placed.len(), 1);
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
    fn funding_reduction_places_passive_exit_without_a_fill_until_a_compatible_trade() {
        let mut engine = engine().with_live_risk_gates();
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

        feed(
            &mut engine,
            br#"{"e":"bookTicker","u":2,"E":299999,"T":299999,"s":"CXMTUSDT","b":"9880","B":"10","a":"9890","A":"10"}"#,
        );
        let before = engine.summary().fill_count;
        let records = engine.evaluate_exits_at(300_000).unwrap();
        assert!(
            records.iter().any(|record| record.kind == "order_placed"
                && record.side.as_deref() == Some("SELL")
                && record.price_ticks == Some(9890)),
            "records: {records:?}"
        );
        assert_eq!(engine.summary().fill_count, before);

        let not_aggressive = feed(
            &mut engine,
            br#"{"e":"aggTrade","E":300002,"s":"CXMTUSDT","a":2,"p":"9890","q":"3","T":300002,"m":true}"#,
        );
        assert!(not_aggressive.is_empty());
        let filled = feed(
            &mut engine,
            br#"{"e":"aggTrade","E":300003,"s":"CXMTUSDT","a":3,"p":"9890","q":"3","T":300003,"m":false}"#,
        );
        assert_eq!(filled[0].kind, "fill");
        assert_eq!(engine.summary().current_absolute_position, 0);
    }

    #[test]
    fn event_driven_reduction_quotes_long_exit_at_ask() {
        let mut engine = engine().with_live_risk_gates();
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
            br#"{"e":"aggTrade","E":3,"s":"CXMTUSDT","a":1,"p":"98","q":"10","T":3,"m":true}"#,
        );
        assert_eq!(engine.summary().current_absolute_position, 10);
        let records = feed(
            &mut engine,
            br#"{"e":"bookTicker","u":2,"E":300000,"T":300000,"s":"CXMTUSDT","b":"9880","B":"10","a":"9890","A":"10"}"#,
        );
        assert!(
            records.iter().any(|record| record.kind == "order_placed"
                && record.side.as_deref() == Some("SELL")
                && record.price_ticks == Some(9_890)),
            "records: {records:?}"
        );
        assert_eq!(engine.summary().fill_count, 1);
    }

    fn set_exit_fixture(engine: &mut PaperEngine, position: i64, book_time_ms: u64) {
        let state = engine.states.get_mut("CXMTUSDT").unwrap();
        state.position = position;
        state.next_funding_time_ms = 600_000;
        state.last_mark_time_ms = 1;
        state.book = Some(BookState {
            bid_price_ticks: 9_880,
            bid_quantity: 20,
            ask_price_ticks: 9_890,
            ask_quantity: 20,
            observed_at_ms: book_time_ms,
        });
    }

    #[test]
    fn exit_core_uses_bid_for_short_and_rejects_stale_book_without_fills() {
        let mut short_engine = engine().with_live_risk_gates();
        set_exit_fixture(&mut short_engine, -3, 300_000);
        let short = short_engine.evaluate_exits_at(300_000).unwrap();
        assert!(short.iter().any(|record| record.kind == "order_placed"
            && record.side.as_deref() == Some("BUY")
            && record.price_ticks == Some(9_880)));
        assert_eq!(short_engine.summary().fill_count, 0);

        let mut stale = engine().with_live_risk_gates();
        set_exit_fixture(&mut stale, 3, 294_999);
        stale.states.get_mut("CXMTUSDT").unwrap().last_mark_time_ms = 300_000;
        let records = stale.evaluate_exits_at(300_000).unwrap();
        assert!(records.iter().any(|record| record.kind == "exit_blocked"));
        assert!(!records.iter().any(|record| record.kind == "order_placed"));
        assert_eq!(stale.summary().fill_count, 0);
    }

    #[test]
    fn explicit_exit_constraints_keep_their_timestamp_and_expire() {
        let mut engine = engine().with_live_risk_gates();
        set_exit_fixture(&mut engine, 3, 300_000);
        engine
            .set_exit_constraints(
                "CXMTUSDT",
                ExitConstraints {
                    min_price: 1,
                    max_price: 20_000,
                    price_tick: 1,
                    min_quantity: 1,
                    max_quantity: 100,
                    quantity_step: 1,
                    min_notional: 1,
                    quantity_scale: 0,
                    observed_at_ms: 1,
                    max_age_ms: 10,
                },
            )
            .unwrap();
        let records = engine.evaluate_exits_at(300_000).unwrap();
        assert!(records.iter().any(|record| record.kind == "exit_blocked"));
        assert!(!records.iter().any(|record| record.kind == "order_placed"));
        assert_eq!(engine.summary().fill_count, 0);
    }

    #[test]
    fn exit_core_handles_i64_min_and_keeps_safe_partial_reduce_order() {
        let mut invalid = engine().with_live_risk_gates();
        set_exit_fixture(&mut invalid, i64::MIN, 300_000);
        let records = invalid.evaluate_exits_at(300_000).unwrap();
        assert!(records.iter().any(|record| record.kind == "exit_blocked"));
        assert_eq!(invalid.summary().fill_count, 0);

        let mut retained = engine().with_live_risk_gates();
        set_exit_fixture(&mut retained, 3, 300_000);
        retained.states.get_mut("CXMTUSDT").unwrap().working = Some(WorkingOrder {
            client_id: 9,
            side: Side::Sell,
            price_ticks: 9_890,
            remaining_quantity: 2,
            reduce_only: true,
            placed_at_ms: 299_999,
            cancel_requested_at_ms: None,
        });
        assert!(retained.evaluate_exits_at(300_000).unwrap().is_empty());
        assert_eq!(
            retained.states["CXMTUSDT"]
                .working
                .unwrap()
                .remaining_quantity,
            2
        );
    }

    #[test]
    fn hard_deadline_reports_residual_and_never_replaces_risk_increasing_order() {
        let mut engine = engine().with_live_risk_gates();
        set_exit_fixture(&mut engine, 3, 600_000);
        engine.states.get_mut("CXMTUSDT").unwrap().working = Some(WorkingOrder {
            client_id: 10,
            side: Side::Buy,
            price_ticks: 9_880,
            remaining_quantity: 3,
            reduce_only: false,
            placed_at_ms: 1,
            cancel_requested_at_ms: None,
        });
        let records = engine.evaluate_exits_at(600_000).unwrap();
        assert!(records.iter().any(|record| record.kind == "order_canceled"));
        assert!(records
            .iter()
            .any(|record| record.kind == "residual_exposure"));
        assert!(!records.iter().any(|record| record.kind == "order_placed"));
        assert_eq!(engine.summary().current_absolute_position, 3);
        assert_eq!(engine.summary().fill_count, 0);
    }

    #[test]
    fn hard_deadline_reports_residual_while_cancel_reconciliation_is_pending() {
        let mut engine = engine().with_live_risk_gates().with_realism(
            crate::backtest::realism::RealisticFillModel {
                queue: crate::backtest::realism::QueueModel::default(),
                latency: crate::backtest::realism::LatencyModel {
                    market_to_decision_ms: 0,
                    decision_to_exchange_ms: 0,
                    cancel_to_exchange_ms: 1,
                },
            },
        );
        set_exit_fixture(&mut engine, 3, 600_000);
        engine.states.get_mut("CXMTUSDT").unwrap().working = Some(WorkingOrder {
            client_id: 12,
            side: Side::Buy,
            price_ticks: 9_880,
            remaining_quantity: 3,
            reduce_only: false,
            placed_at_ms: 1,
            cancel_requested_at_ms: Some(600_000),
        });
        let records = engine.evaluate_exits_at(600_000).unwrap();
        assert!(records
            .iter()
            .any(|record| record.kind == "residual_exposure"));
        assert!(!records.iter().any(|record| record.kind == "order_placed"));
        assert_eq!(engine.summary().current_absolute_position, 3);
        assert_eq!(engine.summary().fill_count, 0);
    }

    #[test]
    fn cancel_latency_keeps_compatible_fills_possible_until_acknowledged() {
        let mut engine = engine().with_live_risk_gates().with_realism(
            crate::backtest::realism::RealisticFillModel {
                queue: crate::backtest::realism::QueueModel::default(),
                latency: crate::backtest::realism::LatencyModel {
                    market_to_decision_ms: 0,
                    decision_to_exchange_ms: 0,
                    cancel_to_exchange_ms: 10,
                },
            },
        );
        set_exit_fixture(&mut engine, 3, 300_000);
        engine.states.get_mut("CXMTUSDT").unwrap().working = Some(WorkingOrder {
            client_id: 11,
            side: Side::Buy,
            price_ticks: 9_880,
            remaining_quantity: 2,
            reduce_only: false,
            placed_at_ms: 1,
            cancel_requested_at_ms: None,
        });
        let requested = engine.evaluate_exits_at(300_000).unwrap();
        assert!(requested
            .iter()
            .any(|record| record.kind == "cancel_requested"));
        let fill = feed(
            &mut engine,
            br#"{"e":"aggTrade","E":300005,"s":"CXMTUSDT","a":99,"p":"9880","q":"1","T":300005,"m":true}"#,
        );
        assert!(fill.iter().any(|record| record.kind == "fill"));
        let acknowledged = engine.evaluate_exits_at(300_010).unwrap();
        assert!(acknowledged
            .iter()
            .any(|record| record.kind == "order_canceled"));
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
        assert_eq!(placed.len(), 1);
        feed(
            &mut engine,
            br#"{"e":"markPriceUpdate","E":299999,"s":"CXMTUSDT","p":"100","i":"100","T":600000,"r":"0"}"#,
        );
        let canceled = feed(
            &mut engine,
            br#"{"e":"bookTicker","u":2,"E":300001,"T":300001,"s":"CXMTUSDT","b":"98","B":"10","a":"99","A":"10"}"#,
        );
        assert_eq!(canceled.len(), 1);
        assert_eq!(canceled[0].kind, "order_canceled");
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
        let mut engine = PaperEngine::new(anchors(), 100, 1_000, 300, 20, 0, 0, 2).unwrap();
        let feed_scaled = |engine: &mut PaperEngine, raw: &[u8]| {
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
        let path = std::env::temp_dir().join("anchorbell-paper-replay-eof.jsonl");
        std::fs::write(
            &path,
            "{\"e\":\"markPriceUpdate\",\"E\":1,\"s\":\"CXMTUSDT\",\"p\":\"100\",\"i\":\"100\",\"T\":600000,\"r\":\"0\"}\n{\"e\":\"bookTicker\",\"u\":1,\"E\":2,\"T\":2,\"s\":\"CXMTUSDT\",\"b\":\"98\",\"B\":\"10\",\"a\":\"99\",\"A\":\"10\"}\n",
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
    fn paper_replay_rejects_symbols_without_configured_state() {
        let path = std::env::temp_dir().join("anchorbell-paper-replay-symbol.jsonl");
        std::fs::write(
            &path,
            "{\"e\":\"markPriceUpdate\",\"E\":1,\"s\":\"XYZUSDT\",\"p\":\"100\",\"i\":\"100\",\"T\":1000,\"r\":\"0\"}\n",
        )
        .unwrap();
        let result = replay_jsonl(&path, None, anchors(), 0, 0, 100, 100, 10, 20, 0, 0);
        assert!(matches!(
            result,
            Err(PaperError::ReplaySymbolNotConfigured(symbol)) if symbol == "XYZUSDT"
        ));
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn paper_replay_rejects_out_of_order_events() {
        let path = std::env::temp_dir().join("anchorbell-paper-replay-order.jsonl");
        std::fs::write(
            &path,
            "{\"e\":\"markPriceUpdate\",\"E\":2,\"s\":\"CXMTUSDT\",\"p\":\"100\",\"i\":\"100\",\"T\":2,\"r\":\"0\"}\n{\"e\":\"markPriceUpdate\",\"E\":1,\"s\":\"CXMTUSDT\",\"p\":\"100\",\"i\":\"100\",\"T\":1,\"r\":\"0\"}\n",
        )
        .unwrap();
        let result = replay_jsonl(&path, None, anchors(), 0, 0, 100, 100, 10, 20, 0, 0);
        assert!(matches!(result, Err(PaperError::ReplayOutOfOrder { .. })));
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn paper_marks_beijing_overnight_as_closed_and_usable() {
        let overnight = 1_788_377_934_321_u64;
        let previous_close = overnight - 12 * 60 * 60 * 1_000;
        let next_close = overnight + 12 * 60 * 60 * 1_000;
        assert_eq!(calendar_state_for("CXMTUSDT", overnight), "closed");
        assert!(paper_session_allows_entry("CXMTUSDT", overnight));
        assert!(paper_anchor_usable("CXMTUSDT", previous_close, overnight));
        assert!(!paper_anchor_usable("CXMTUSDT", previous_close, next_close));
        assert!(!anchor_refresh_allowed("CXMTUSDT", overnight));
    }

    #[test]
    fn capital_allocation_respects_fixed_and_weighted_modes() {
        let anchors = [
            (
                "CXMTUSDT".to_owned(),
                PaperAnchor {
                    close_price_ticks: 100 * 100_000_000,
                    observed_at_ms: 0,
                    valid_until_ms: 0,
                },
            ),
            (
                "UNITREEUSDT".to_owned(),
                PaperAnchor {
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
    fn paper_allows_midday_break_anchor_but_not_open_anchor() {
        let midday_break = 1_788_408_258_130_u64;
        let morning_open = 1_788_404_658_130_u64;
        assert!(anchor_reference_allowed("CXMTUSDT", midday_break));
        assert!(anchor_reference_allowed("HK0625USDT", midday_break));
        assert!(!anchor_reference_allowed("CXMTUSDT", morning_open));
        assert!(!anchor_reference_allowed("HK0625USDT", morning_open));
    }
}
