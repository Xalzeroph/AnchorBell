//! Shared simulation-trading and replay execution engine.

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

mod decision_audit;

use super::portfolio_guard::{
    PortfolioDrawdownAction, PortfolioDrawdownGuard, PortfolioDrawdownSnapshot,
};
pub use super::risk_metrics::RiskMetrics;
use super::risk_metrics::{calculate_risk_metrics, RISK_SAMPLE_INTERVAL_MS};
use decision_audit::DecisionAuditContext;

use crate::{
    backtest::TopOfBook,
    execution::{binance_runtime_config, BinanceEnvironment},
    execution::{
        decide_adaptive_taker, AdaptiveTakerDecision, AdaptiveTakerInput, EmergencyExecutionPolicy,
        OrderIntent, Side,
    },
    market::{
        binance::{AggTrade, BinanceMarketEvent, BookTicker, MarkPrice},
        recorder::{add_event_lineage, market_event_to_json},
        BinanceC2cFxClient, BinanceC2cFxPoller, BinanceMarketConfig, BinanceMarketFeed,
        BinanceMarketStream, BinanceScaledExecutionFilters, FxPollerConfig, FxUpdate,
        PublicMarketMetadataClient, ReconnectPolicy,
    },
    observability::{DecisionAudit, DecisionGateAudit, DECISION_AUDIT_SCHEMA_VERSION},
    orderbook::LocalOrderBook,
    risk::evaluate_funding_overlay,
    runtime::{
        io::{send_line, spawn_line_writer, write_json_atomic, AsyncLineWriter},
        CausalLedger, DataQuality, EventEnvelope, EventSource,
    },
    strategy::{
        calendar::{calendar_for, EquitySessionCalendar},
        capital::{dynamic_weights, CapitalRiskInput},
        decide_m9, decide_maker_exit, profile_for, side_adverse_selection_pico_bps,
        universe::instrument_for,
        AdaptiveThreshold, AnchorCurrency, AnchorMakerStrategy, CalibrationSnapshot,
        CalibrationState, CalibrationStatus, DataQualityStatus, DualFlattenPlan, ExitBook, ExitConstraints,
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
    /// Permanent production core: M1 adaptive risk plus proven safety overlays.
    CoreV1,
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
    /// M8 without its funding controller, used only for a valid funding ablation.
    M8FundingDisabled,
    /// M8 funding control plus deadline-constrained causal residual DRO-MPC.
    M9DeadlineCausalDroMpc,
}

impl SimulationPolicyVariant {
    pub fn label(self) -> &'static str {
        match self {
            Self::M0Fixed => "m0_fixed",
            Self::M1AdaptiveRisk => "m1_adaptive_risk",
            Self::CoreV1 => "core_v1",
            Self::M2Microstructure => "m2_microstructure",
            Self::M3FillAware => "m3_fill_aware",
            Self::M4Statistical => "m4_statistical",
            Self::M5Robust => "m5_robust",
            Self::M6DynamicCapital => "m6_dynamic_capital",
            Self::M7EvidenceGated => "m7_evidence_gated",
            Self::M8FundingAware => "m8_funding_aware",
            Self::M8FundingDisabled => "m8_funding_disabled",
            Self::M9DeadlineCausalDroMpc => "m9_deadline_causal_dro_mpc",
        }
    }

    fn uses_microstructure(self) -> bool {
        matches!(
            self,
            Self::M2Microstructure
                | Self::M3FillAware
                | Self::M4Statistical
                | Self::M5Robust
                | Self::M6DynamicCapital
                | Self::M7EvidenceGated
                | Self::M8FundingAware
                | Self::M8FundingDisabled
                | Self::M9DeadlineCausalDroMpc
        )
    }

    fn uses_fill_gate(self) -> bool {
        matches!(
            self,
            Self::M3FillAware
                | Self::M4Statistical
                | Self::M5Robust
                | Self::M6DynamicCapital
                | Self::M7EvidenceGated
                | Self::M8FundingAware
                | Self::M8FundingDisabled
                | Self::M9DeadlineCausalDroMpc
        )
    }

    fn uses_statistical_term(self) -> bool {
        matches!(
            self,
            Self::M4Statistical
                | Self::M5Robust
                | Self::M6DynamicCapital
                | Self::M7EvidenceGated
                | Self::M8FundingAware
                | Self::M8FundingDisabled
                | Self::M9DeadlineCausalDroMpc
        )
    }

    fn uses_tail_guard(self) -> bool {
        matches!(
            self,
            Self::CoreV1
                | Self::M5Robust
                | Self::M6DynamicCapital
                | Self::M7EvidenceGated
                | Self::M8FundingAware
                | Self::M8FundingDisabled
                | Self::M9DeadlineCausalDroMpc
        )
    }

    fn uses_dynamic_capital(self) -> bool {
        matches!(
            self,
            Self::M6DynamicCapital
                | Self::M7EvidenceGated
                | Self::M8FundingAware
                | Self::M8FundingDisabled
                | Self::M9DeadlineCausalDroMpc
        )
    }

    fn uses_evidence_gate(self) -> bool {
        matches!(
            self,
            Self::CoreV1
                | Self::M7EvidenceGated
                | Self::M8FundingAware
                | Self::M8FundingDisabled
                | Self::M9DeadlineCausalDroMpc
        )
    }

    fn uses_funding_controller(self) -> bool {
        matches!(self, Self::M8FundingAware | Self::M9DeadlineCausalDroMpc)
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
    #[error("calibration seed time {seed_ms} is not strictly before replay start {replay_ms}")]
    CalibrationSeedNotPrior { seed_ms: u64, replay_ms: u64 },
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

pub const INDEX_ANCHOR_SOURCE: &str = "binance_index_price_klines";

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
    pub index_source: String,
    pub index_open_time_ms: u64,
    pub index_close_time_ms: u64,
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
    anchor_kline_interval: &str,
    anchor_kline_lookback_ms: u64,
    anchor_kline_limit: usize,
    http_proxy: Option<&str>,
) -> Result<BinanceIndexAnchorSet, SimulationError> {
    if symbols.is_empty() {
        return Err(SimulationError::InvalidConfig(
            "index anchors require at least one symbol",
        ));
    }
    let mut seen = BTreeMap::new();
    let mut selected_metadata = Vec::with_capacity(symbols.len());
    let client =
        PublicMarketMetadataClient::new(environment.endpoints().rest_base.as_str(), http_proxy)
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

    let observed_now_ms = now_ms();
    let mut historical_anchors = BTreeMap::new();
    for metadata in &selected_metadata {
        let symbol = metadata.symbol.clone();
        let profile = profile_for(&symbol).ok_or_else(|| {
            SimulationError::Market(format!("no anchor currency profile for {symbol}"))
        })?;
        let calendar = calendar_for(profile.region);
        let close_at_ms = calendar
            .latest_completed_close_before(observed_now_ms)
            .ok_or_else(|| {
                SimulationError::Market(format!(
                    "no completed exchange close available for {symbol}"
                ))
            })?;
        let klines = client
            .index_price_klines(
                &symbol,
                anchor_kline_interval,
                close_at_ms.saturating_sub(anchor_kline_lookback_ms),
                close_at_ms,
                anchor_kline_limit,
            )
            .await
            .map_err(|error| {
                SimulationError::Market(format!(
                    "index price kline for {symbol} at close {close_at_ms}: {error}"
                ))
            })?;
        let kline = klines
            .into_iter()
            .filter(|kline| kline.open_time_ms < close_at_ms && kline.close_time_ms <= close_at_ms)
            .max_by_key(|kline| kline.close_time_ms)
            .ok_or_else(|| {
                SimulationError::Market(format!(
                    "no completed historical close kline for {symbol} at {close_at_ms}"
                ))
            })?;
        historical_anchors.insert(symbol, (profile, kline));
    }

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
    for (symbol, (profile, kline)) in historical_anchors {
        let fx_quote = fx_quotes
            .get(profile.anchor_currency.as_str())
            .ok_or_else(|| {
                SimulationError::Market(format!(
                    "missing {}/USDT FX quote for {symbol}",
                    profile.anchor_currency.as_str()
                ))
            })?;
        let index_price =
            crate::market::binance::parse_price_ticks(&kline.close_price, price_scale).map_err(
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
                observed_at_ms: kline.close_time_ms,
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
                index_source: INDEX_ANCHOR_SOURCE.to_owned(),
                index_open_time_ms: kline.open_time_ms,
                index_close_time_ms: kline.close_time_ms,
                index_observed_at_ms: kline.close_time_ms,
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
    anchor_kline_interval: &str,
    anchor_kline_lookback_ms: u64,
    anchor_kline_limit: usize,
    http_proxy: Option<&str>,
) -> Result<BTreeMap<String, AnchorSnapshot>, SimulationError> {
    Ok(load_index_anchor_set_internal(
        environment,
        symbols,
        price_scale,
        anchor_kline_interval,
        anchor_kline_lookback_ms,
        anchor_kline_limit,
        http_proxy,
    )
    .await?
    .anchors)
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
    /// Initial modeled quantity resting ahead of this order. When local depth is
    /// seeded this includes the observed quantity at our price plus any explicit
    /// synthetic queue/trade-through stress.
    queue_ahead_quantity: i64,
    /// Stateful queue barrier still requiring compatible aggressor volume before
    /// this maker order may fill. This can only decrease after exchange arrival.
    queue_ahead_remaining: i64,
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
    /// Contract-level settlement interval fetched from Binance fundingInfo.
    funding_interval_hours: u32,
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
    ewma_signed_return_pico_bps: i64,
    near_miss_count: u64,
    adaptive_relief_bps: i64,
    adaptive_relief_micro_bps: i64,
    adaptive_relief_pico_bps: i64,
    working: Option<WorkingOrder>,
    last_taker_at_ms: Option<u64>,
    position: i64,
    average_entry_ticks: i64,
    realized_pnl_ticks: i64,
    market_pnl_ticks: i64,
    strategy_pnl_ticks: i64,
    funding_pnl_ticks: i64,
    fees_ticks: i64,
    /// High-water mark of the symbol net-PnL path, updated on the causal
    /// decision path so future observations cannot leak into sizing.
    peak_net_pnl_ticks: i64,
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
    pub gate_rejection_records: Vec<GateRejectionRecord>,
    pub realized_pnl_ticks: i64,
    pub unrealized_pnl_ticks: i64,
    pub market_pnl_ticks: i64,
    pub strategy_pnl_ticks: i64,
    pub funding_pnl_ticks: i64,
    pub gross_pnl_ticks: i64,
    pub fees_ticks: i64,
    pub net_pnl_ticks: i64,
    pub maker_fee_ppm: i64,
    pub taker_fee_ppm: i64,
    pub unrealized_valuation_complete: bool,
    pub current_absolute_position: i64,
    /// Signed inventory imbalance across symbols, normalized by each symbol's
    /// configured maximum position. Positive values are net long; negative
    /// values are net short. This exposes common-mode concentration directly
    /// instead of hiding it behind gross position.
    pub portfolio_inventory_imbalance_bps: i64,
    pub peak_absolute_position: i64,
    pub working_orders: u64,
    pub flat_at_end: bool,
}

#[derive(Debug, Clone, Serialize)]
pub struct GateRejectionRecord {
    pub reason: String,
    pub symbol: String,
    pub market: String,
    pub method: String,
    pub source: String,
    pub threshold: Option<i64>,
    pub observed_value: Option<i64>,
    pub timestamp_ms: u64,
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
/// Do not infer a tail probability from a handful of fills. This is a policy
/// sample floor, not an exchange/trading-rule constant.
const MIN_MARKOUT_FEEDBACK_SAMPLES: usize = 8;
/// Pseudo-observations for the zero-trend prior. A short burst of one-sided
/// prints must not immediately veto a mean-reversion quote.
const TREND_PRIOR_OBSERVATIONS: i128 = 32;
const TREND_CONFLICT_CAP_PICO_BPS: i64 = 75 * PICO_BPS_SCALE;
const MIN_EVIDENCE_SCALE_PPM: i64 = 250_000;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SimulationRiskState {
    Trading,
    ReduceOnlyEquitySession,
    ReduceOnlySymbolDrawdown,
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
            Self::ReduceOnlySymbolDrawdown => "reduce_only_symbol_drawdown",
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
    /// Drawdown of this symbol's own net-PnL path from its observed peak,
    /// measured against its current allocated capital. This is separate from
    /// portfolio drawdown so one damaged symbol cannot consume the whole
    /// portfolio risk budget while the aggregate still looks calm.
    pub symbol_drawdown_bps: i64,
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
    pub book_age_ms: Option<u64>,
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
    /// Signed EWMA return used to distinguish mean-reversion from a moving
    /// dislocation. Positive values indicate upward pressure.
    pub ewma_signed_return_pico_bps: i64,
    /// Directional persistence after shrinkage toward a zero-trend prior.
    pub trend_persistence_bps: i64,
    pub buy_trend_conflict_pico_bps: i64,
    pub sell_trend_conflict_pico_bps: i64,
    pub buy_market_trend_conflict_pico_bps: i64,
    pub sell_market_trend_conflict_pico_bps: i64,
    pub reversion_evidence_lower_bps: i64,
    pub reversion_evidence_scale_ppm: i64,
    pub ewma_adverse_markout_bps: i64,
    pub ewma_adverse_markout_micro_bps: i64,
    pub ewma_adverse_markout_pico_bps: i64,
    /// Conservative upper estimate used by admission and allocation. The raw
    /// EWMA remains visible for diagnosing adaptation lag.
    pub adver