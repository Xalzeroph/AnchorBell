use std::{
    path::PathBuf,
    sync::{Arc, OnceLock},
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use futures_util::{stream, StreamExt};
use reqwest::Client;
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use serde_json::Value;
use thiserror::Error;

use crate::execution::binance_runtime_config;
use crate::network::{RequestClass, RequestCoordinator};

use super::freshness::{FreshnessClass, FreshnessPolicy, FreshnessState};

#[derive(Debug, Error, PartialEq, Eq)]
pub enum PublicMetadataError {
    #[error("invalid HTTP proxy configuration")]
    InvalidProxy,
    #[error("public metadata client construction failed")]
    ClientBuild,
    #[error("public metadata transport failed")]
    Transport,
    #[error("public metadata endpoint returned HTTP status {status}")]
    HttpStatus { status: u16 },
    #[error("public metadata response could not be decoded")]
    Decode,
    #[error("index price kline response is invalid")]
    InvalidIndexPriceKline,
    #[error("symbol metadata is not present in exchangeInfo: {0}")]
    SymbolNotFound(String),
    #[error("metadata snapshot has inconsistent symbols")]
    SymbolMismatch,
    #[error("symbol is not a trading TradFi perpetual")]
    NotTradingTradFiPerpetual,
    #[error("required exchange filter is missing: {0}")]
    MissingExchangeFilter(&'static str),
    #[error("exchange filter is invalid: {filter}.{field}")]
    InvalidExchangeFilter {
        filter: &'static str,
        field: &'static str,
    },
    #[error("metadata snapshot has no complete two-sided quote")]
    IncompleteQuote,
    #[error("metadata snapshot contains a non-positive market value")]
    NonPositiveMarketValue,
    #[error("metadata snapshot contains an invalid funding rate")]
    InvalidFundingRate,
    #[error("metadata snapshot contains an expired funding time")]
    ExpiredFundingTime,
    #[error("metadata snapshot is stale or has a future observation timestamp")]
    StaleSnapshot,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct BinanceSymbolFilter {
    #[serde(rename = "filterType")]
    pub filter_type: String,
    #[serde(rename = "minPrice")]
    pub min_price: Option<String>,
    #[serde(rename = "maxPrice")]
    pub max_price: Option<String>,
    #[serde(rename = "tickSize")]
    pub tick_size: Option<String>,
    #[serde(rename = "minQty")]
    pub min_quantity: Option<String>,
    #[serde(rename = "maxQty")]
    pub max_quantity: Option<String>,
    #[serde(rename = "stepSize")]
    pub step_size: Option<String>,
    pub notional: Option<String>,
    #[serde(rename = "multiplierUp")]
    pub multiplier_up: Option<String>,
    #[serde(rename = "multiplierDown")]
    pub multiplier_down: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BinanceExecutionFilters {
    pub min_price: String,
    pub max_price: String,
    pub price_tick: String,
    pub min_quantity: String,
    pub max_quantity: String,
    pub quantity_step: String,
    pub min_notional: String,
    pub multiplier_up: String,
    pub multiplier_down: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct BinanceScaledExecutionFilters {
    pub min_price_ticks: i64,
    pub max_price_ticks: i64,
    pub price_tick: i64,
    pub min_quantity_units: i64,
    pub max_quantity_units: i64,
    pub quantity_step: i64,
    pub min_notional_price_ticks: i64,
    pub multiplier_up_ppm: i64,
    pub multiplier_down_ppm: i64,
}

impl BinanceExecutionFilters {
    pub fn scaled(
        &self,
        price_scale: u32,
        quantity_scale: u32,
    ) -> Result<BinanceScaledExecutionFilters, PublicMetadataError> {
        let price = |value: &str| {
            super::binance::parse_price_ticks(value, price_scale)
                .map(|value| value.0)
                .map_err(|_| PublicMetadataError::InvalidExchangeFilter {
                    filter: "PRICE_FILTER",
                    field: "scaled_value",
                })
        };
        let quantity = |value: &str| {
            super::binance::parse_quantity(value, quantity_scale)
                .map(|value| value.0)
                .map_err(|_| PublicMetadataError::InvalidExchangeFilter {
                    filter: "LOT_SIZE",
                    field: "scaled_value",
                })
        };
        let multiplier = |value: &str| {
            super::binance::parse_price_ticks(value, 6)
                .map(|value| value.0)
                .map_err(|_| PublicMetadataError::InvalidExchangeFilter {
                    filter: "PERCENT_PRICE",
                    field: "scaled_value",
                })
        };
        Ok(BinanceScaledExecutionFilters {
            min_price_ticks: price(&self.min_price)?,
            max_price_ticks: price(&self.max_price)?,
            price_tick: price(&self.price_tick)?,
            min_quantity_units: quantity(&self.min_quantity)?,
            max_quantity_units: quantity(&self.max_quantity)?,
            quantity_step: quantity(&self.quantity_step)?,
            min_notional_price_ticks: price(&self.min_notional)?,
            multiplier_up_ppm: multiplier(&self.multiplier_up)?,
            multiplier_down_ppm: multiplier(&self.multiplier_down)?,
        })
    }
}

impl BinanceScaledExecutionFilters {
    /// Normalize a limit price to the exchange tick in a direction that keeps
    /// the order's execution intent intact. Passive buys/sells are rounded
    /// toward the book; aggressive reduce-only orders are rounded toward the
    /// taker side. This prevents a valid signal from becoming a rejected order
    /// solely because an arithmetic price was between two exchange ticks.
    pub fn normalize_price(
        self,
        requested_price_ticks: i64,
        is_buy: bool,
        post_only: bool,
    ) -> Result<i64, &'static str> {
        if requested_price_ticks <= 0 || self.price_tick <= 0 {
            return Err("exchange_price_rule_invalid");
        }
        let remainder = requested_price_ticks % self.price_tick;
        let price = if remainder == 0 {
            requested_price_ticks
        } else if post_only == is_buy {
            requested_price_ticks.saturating_sub(remainder)
        } else {
            requested_price_ticks.saturating_add(self.price_tick - remainder)
        };
        if price < self.min_price_ticks {
            return Err("exchange_price_below_min");
        }
        if price > self.max_price_ticks {
            return Err("exchange_price_above_max");
        }
        Ok(price)
    }

    /// Returns the exchange-admissible quantity closest to the requested
    /// quantity without exceeding the caller's risk capacity.  If the
    /// configured quantity is below MIN_NOTIONAL, the quantity is increased
    /// only when the caller explicitly provides enough remaining capacity.
    pub fn normalize_quantity(
        self,
        requested_quantity: i64,
        price_ticks: i64,
        maximum_quantity: i64,
        quantity_scale: u32,
    ) -> Result<i64, &'static str> {
        if requested_quantity <= 0 || price_ticks <= 0 || maximum_quantity <= 0 {
            return Err("exchange_quantity_non_positive");
        }
        let maximum_quantity = maximum_quantity.min(self.max_quantity_units);
        if self.quantity_step <= 0 || self.min_quantity_units <= 0 {
            return Err("exchange_quantity_rule_invalid");
        }
        let mut quantity = requested_quantity.min(maximum_quantity);
        quantity -= quantity % self.quantity_step;
        if quantity < self.min_quantity_units {
            quantity = self.min_quantity_units;
        }
        let quantity_factor = 10_i128
            .checked_pow(quantity_scale)
            .ok_or("exchange_quantity_scale")?;
        let minimum_notional_quantity = (i128::from(self.min_notional_price_ticks)
            .checked_mul(quantity_factor)
            .ok_or("exchange_notional_overflow")?
            .saturating_add(i128::from(price_ticks).saturating_sub(1)))
            / i128::from(price_ticks);
        let required = i64::try_from(minimum_notional_quantity)
            .map_err(|_| "exchange_min_notional_overflow")?;
        if quantity < required {
            quantity = required;
        }
        let remainder = quantity % self.quantity_step;
        if remainder != 0 {
            quantity = quantity
                .checked_add(self.quantity_step - remainder)
                .ok_or("exchange_quantity_overflow")?;
        }
        if quantity < self.min_quantity_units {
            quantity = self.min_quantity_units;
        }
        if quantity > maximum_quantity || quantity > self.max_quantity_units {
            return Err("exchange_min_notional_exceeds_risk_capacity");
        }
        Ok(quantity)
    }

    pub fn validate_order(
        self,
        price_ticks: i64,
        quantity_units: i64,
        mark_price_ticks: i64,
        is_buy: bool,
        quantity_scale: u32,
    ) -> Result<(), &'static str> {
        if price_ticks <= 0 || quantity_units <= 0 || mark_price_ticks <= 0 {
            return Err("exchange_order_non_positive");
        }
        if price_ticks < self.min_price_ticks {
            return Err("exchange_price_below_min");
        }
        if price_ticks > self.max_price_ticks {
            return Err("exchange_price_above_max");
        }
        if self.price_tick <= 0 || price_ticks % self.price_tick != 0 {
            return Err("exchange_price_tick");
        }
        if quantity_units < self.min_quantity_units {
            return Err("exchange_quantity_below_min");
        }
        if quantity_units > self.max_quantity_units {
            return Err("exchange_quantity_above_max");
        }
        if self.quantity_step <= 0 || quantity_units % self.quantity_step != 0 {
            return Err("exchange_quantity_step");
        }
        let quantity_factor = 10_i128
            .checked_pow(quantity_scale)
            .ok_or("exchange_quantity_scale")?;
        let notional = i128::from(price_ticks)
            .checked_mul(i128::from(quantity_units))
            .ok_or("exchange_notional_overflow")?;
        let minimum = i128::from(self.min_notional_price_ticks)
            .checked_mul(quantity_factor)
            .ok_or("exchange_notional_overflow")?;
        if notional < minimum {
            return Err("exchange_min_notional");
        }
        let price_factor = i128::from(price_ticks)
            .checked_mul(1_000_000)
            .ok_or("exchange_percent_price_overflow")?;
        let mark_limit = i128::from(mark_price_ticks)
            .checked_mul(i128::from(if is_buy {
                self.multiplier_up_ppm
            } else {
                self.multiplier_down_ppm
            }))
            .ok_or("exchange_percent_price_overflow")?;
        if (is_buy && price_factor > mark_limit) || (!is_buy && price_factor < mark_limit) {
            return Err("exchange_percent_price");
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct BinanceSymbolMetadata {
    pub symbol: String,
    pub status: String,
    #[serde(rename = "contractType")]
    pub contract_type: String,
    #[serde(rename = "underlyingType", default)]
    pub underlying_type: Option<String>,
    #[serde(rename = "baseAsset")]
    pub base_asset: String,
    #[serde(rename = "quoteAsset")]
    pub quote_asset: String,
    #[serde(rename = "marginAsset")]
    pub margin_asset: String,
    #[serde(rename = "pricePrecision")]
    pub price_precision: u32,
    #[serde(rename = "quantityPrecision")]
    pub quantity_precision: u32,
    #[serde(rename = "onboardDate")]
    pub onboard_date_ms: u64,
    #[serde(rename = "deliveryDate")]
    pub delivery_date_ms: u64,
    pub filters: Vec<BinanceSymbolFilter>,
}
#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
struct ExchangeInfoWire {
    symbols: Vec<BinanceSymbolMetadata>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct BinanceBookTickerSnapshot {
    pub symbol: String,
    #[serde(rename = "bidPrice")]
    pub bid_price: String,
    #[serde(rename = "bidQty")]
    pub bid_quantity: String,
    #[serde(rename = "askPrice")]
    pub ask_price: String,
    #[serde(rename = "askQty")]
    pub ask_quantity: String,
}

#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
pub struct BinanceDepthSnapshot {
    #[serde(rename = "lastUpdateId")]
    pub last_update_id: u64,
    pub bids: Vec<[String; 2]>,
    pub asks: Vec<[String; 2]>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct BinancePremiumIndexSnapshot {
    pub symbol: String,
    #[serde(rename = "markPrice")]
    pub mark_price: String,
    #[serde(rename = "indexPrice")]
    pub index_price: String,
    #[serde(rename = "lastFundingRate")]
    pub last_funding_rate: String,
    #[serde(rename = "nextFundingTime")]
    pub next_funding_time_ms: u64,
    /// Binance exchange-side observation time, not local receipt time.
    #[serde(default)]
    pub time: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BinanceTimedPremiumIndexSnapshot {
    pub snapshot: BinancePremiumIndexSnapshot,
    pub observed_at_ms: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BinanceIndexPriceKline {
    pub open_time_ms: u64,
    pub close_price: String,
    pub close_time_ms: u64,
}

/// Funding history returned by Binance's USD-M fundingRate endpoint.
/// rate_type defaults to Regular when the exchange omits rateType.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct BinanceFundingRateSnapshot {
    pub symbol: String,
    #[serde(rename = "fundingRate")]
    pub funding_rate: String,
    #[serde(rename = "fundingTime")]
    pub funding_time_ms: u64,
    #[serde(rename = "markPrice")]
    pub mark_price: String,
    #[serde(rename = "rateType", default = "default_funding_rate_type")]
    pub rate_type: String,
}

fn default_funding_rate_type() -> String {
    "Regular".to_owned()
}

/// Contract-level funding bounds/interval returned by /fapi/v1/fundingInfo.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct BinanceFundingInfo {
    pub symbol: String,
    #[serde(rename = "adjustedFundingRateCap")]
    pub adjusted_funding_rate_cap: String,
    #[serde(rename = "adjustedFundingRateFloor")]
    pub adjusted_funding_rate_floor: String,
    #[serde(rename = "fundingIntervalHours")]
    pub funding_interval_hours: u32,
}

impl BinanceFundingInfo {
    pub fn validate(&self) -> Result<(), PublicMetadataError> {
        if self.symbol.trim().is_empty()
            || self.funding_interval_hours == 0
            || !is_positive_decimal(&self.adjusted_funding_rate_cap)
            || !is_signed_decimal(&self.adjusted_funding_rate_floor)
        {
            return Err(PublicMetadataError::InvalidFundingRate);
        }
        Ok(())
    }
}

pub const PUBLIC_SNAPSHOT_MAX_AGE_MS: u64 = 5_000;
pub const PUBLIC_SNAPSHOT_POLICY: FreshnessPolicy =
    FreshnessPolicy::new(FreshnessClass::Quote, 1_000, PUBLIC_SNAPSHOT_MAX_AGE_MS);

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BinanceSymbolSnapshot {
    pub metadata: BinanceSymbolMetadata,
    pub book_ticker: BinanceBookTickerSnapshot,
    pub premium_index: BinancePremiumIndexSnapshot,
    pub observed_at_ms: u64,
}
impl BinanceSymbolMetadata {
    pub fn is_trading_tradifi_perpetual(&self) -> bool {
        self.status == "TRADING" && self.contract_type == "TRADIFI_PERPETUAL"
    }

    /// Extracts the filters required for a passive limit order.
    ///
    /// The exchange's precision fields are display hints; these filters are
    /// the authoritative order-admission contract.
    pub fn execution_filters(&self) -> Result<BinanceExecutionFilters, PublicMetadataError> {
        let price = self.required_filter("PRICE_FILTER")?;
        let lot = self.required_filter("LOT_SIZE")?;
        let notional = self.required_filter("MIN_NOTIONAL")?;
        let percent = self.required_filter("PERCENT_PRICE")?;
        let values = BinanceExecutionFilters {
            min_price: required_field(price, "PRICE_FILTER", "minPrice")?,
            max_price: required_field(price, "PRICE_FILTER", "maxPrice")?,
            price_tick: required_field(price, "PRICE_FILTER", "tickSize")?,
            min_quantity: required_field(lot, "LOT_SIZE", "minQty")?,
            max_quantity: required_field(lot, "LOT_SIZE", "maxQty")?,
            quantity_step: required_field(lot, "LOT_SIZE", "stepSize")?,
            min_notional: required_field(notional, "MIN_NOTIONAL", "notional")?,
            multiplier_up: required_field(percent, "PERCENT_PRICE", "multiplierUp")?,
            multiplier_down: required_field(percent, "PERCENT_PRICE", "multiplierDown")?,
        };
        for (filter, field, value) in [
            ("PRICE_FILTER", "minPrice", &values.min_price),
            ("PRICE_FILTER", "maxPrice", &values.max_price),
            ("PRICE_FILTER", "tickSize", &values.price_tick),
            ("LOT_SIZE", "minQty", &values.min_quantity),
            ("LOT_SIZE", "maxQty", &values.max_quantity),
            ("LOT_SIZE", "stepSize", &values.quantity_step),
            ("MIN_NOTIONAL", "notional", &values.min_notional),
            ("PERCENT_PRICE", "multiplierUp", &values.multiplier_up),
            ("PERCENT_PRICE", "multiplierDown", &values.multiplier_down),
        ] {
            if !is_positive_decimal(value) {
                return Err(PublicMetadataError::InvalidExchangeFilter { filter, field });
            }
        }
        Ok(values)
    }

    fn required_filter(
        &self,
        filter_type: &'static str,
    ) -> Result<&BinanceSymbolFilter, PublicMetadataError> {
        self.filters
            .iter()
            .find(|filter| filter.filter_type == filter_type)
            .ok_or(PublicMetadataError::MissingExchangeFilter(filter_type))
    }
}

fn required_field(
    filter: &BinanceSymbolFilter,
    filter_name: &'static str,
    field: &'static str,
) -> Result<String, PublicMetadataError> {
    let value = match field {
        "minPrice" => filter.min_price.as_ref(),
        "maxPrice" => filter.max_price.as_ref(),
        "tickSize" => filter.tick_size.as_ref(),
        "minQty" => filter.min_quantity.as_ref(),
        "maxQty" => filter.max_quantity.as_ref(),
        "stepSize" => filter.step_size.as_ref(),
        "notional" => filter.notional.as_ref(),
        "multiplierUp" => filter.multiplier_up.as_ref(),
        "multiplierDown" => filter.multiplier_down.as_ref(),
        _ => None,
    };
    value
        .cloned()
        .ok_or(PublicMetadataError::InvalidExchangeFilter {
            filter: filter_name,
            field,
        })
}

impl BinanceBookTickerSnapshot {
    pub fn has_two_sided_quote(&self) -> bool {
        !self.bid_price.is_empty()
            && !self.ask_price.is_empty()
            && !self.bid_quantity.is_empty()
            && !self.ask_quantity.is_empty()
    }
}

impl BinancePremiumIndexSnapshot {
    pub fn validate_for_anchor(
        &self,
        observed_at_ms: u64,
        now_ms: u64,
    ) -> Result<(), PublicMetadataError> {
        // Anchor lifetime is determined by the equity-session lifecycle and
        // refresh events, not by a generic short market-data TTL.
        if observed_at_ms > now_ms {
            return Err(PublicMetadataError::StaleSnapshot);
        }
        if !is_positive_decimal(&self.mark_price) || !is_positive_decimal(&self.index_price) {
            return Err(PublicMetadataError::NonPositiveMarketValue);
        }
        if !is_signed_decimal(&self.last_funding_rate) {
            return Err(PublicMetadataError::InvalidFundingRate);
        }
        if self.next_funding_time_ms != 0 && self.next_funding_time_ms <= now_ms {
            return Err(PublicMetadataError::ExpiredFundingTime);
        }
        Ok(())
    }

    /// Validates only the price observations needed to construct an equity
    /// anchor. Funding schedule metadata is intentionally excluded: it is
    /// refreshed from the mark stream and must not make an otherwise usable
    /// anchor unavailable during a REST cooldown.
    pub fn validate_for_anchor_price(
        &self,
        observed_at_ms: u64,
        now_ms: u64,
    ) -> Result<(), PublicMetadataError> {
        if observed_at_ms > now_ms {
            return Err(PublicMetadataError::StaleSnapshot);
        }
        if !is_positive_decimal(&self.mark_price) || !is_positive_decimal(&self.index_price) {
            return Err(PublicMetadataError::NonPositiveMarketValue);
        }
        Ok(())
    }
}

impl BinanceSymbolSnapshot {
    /// Validates the public snapshot before it can enter a live decision path.
    /// This is a data-quality gate, not a profitability signal.
    pub fn validate_for_runtime(&self, now_ms: u64) -> Result<(), PublicMetadataError> {
        if self.observed_at_ms != 0
            && matches!(
                PUBLIC_SNAPSHOT_POLICY.validate(self.observed_at_ms, now_ms),
                FreshnessState::Expired | FreshnessState::Invalid
            )
        {
            return Err(PublicMetadataError::StaleSnapshot);
        }
        if self.metadata.symbol != self.book_ticker.symbol
            || self.metadata.symbol != self.premium_index.symbol
        {
            return Err(PublicMetadataError::SymbolMismatch);
        }
        if !self.metadata.is_trading_tradifi_perpetual() {
            return Err(PublicMetadataError::NotTradingTradFiPerpetual);
        }
        self.metadata.execution_filters()?;
        if !self.book_ticker.has_two_sided_quote()
            || !is_positive_decimal(&self.book_ticker.bid_price)
            || !is_positive_decimal(&self.book_ticker.ask_price)
            || !is_positive_decimal(&self.book_ticker.bid_quantity)
            || !is_positive_decimal(&self.book_ticker.ask_quantity)
            || !is_positive_decimal(&self.premium_index.mark_price)
            || !is_positive_decimal(&self.premium_index.index_price)
        {
            return Err(if self.book_ticker.has_two_sided_quote() {
                PublicMetadataError::NonPositiveMarketValue
            } else {
                PublicMetadataError::IncompleteQuote
            });
        }
        if !is_signed_decimal(&self.premium_index.last_funding_rate) {
            return Err(PublicMetadataError::InvalidFundingRate);
        }
        if self.premium_index.next_funding_time_ms != 0
            && self.premium_index.next_funding_time_ms <= now_ms
        {
            return Err(PublicMetadataError::ExpiredFundingTime);
        }
        Ok(())
    }
}

fn current_time_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock must be after UNIX epoch")
        .as_millis() as u64
}

fn is_positive_decimal(value: &str) -> bool {
    !value.starts_with('-')
        && is_signed_decimal(value)
        && value
            .bytes()
            .any(|byte| byte.is_ascii_digit() && byte != b'0')
}

fn is_signed_decimal(value: &str) -> bool {
    let unsigned = value.strip_prefix('-').unwrap_or(value);
    let (whole, fraction) = unsigned.split_once('.').unwrap_or((unsigned, ""));
    !whole.is_empty()
        && whole.bytes().all(|byte| byte.is_ascii_digit())
        && fraction.bytes().all(|byte| byte.is_ascii_digit())
}

const PUBLIC_REST_MIN_INTERVAL: Duration = Duration::from_millis(100);
const PUBLIC_REST_WINDOW: Duration = Duration::from_secs(60);
const PUBLIC_REST_WEIGHT_CAPACITY: f64 = 1_200.0;
const PUBLIC_REST_WEIGHT_PER_SECOND: f64 =
    PUBLIC_REST_WEIGHT_CAPACITY / PUBLIC_REST_WINDOW.as_secs_f64();
const PUBLIC_REST_SOFT_LIMIT: u32 = 1_000;
const RATE_LIMIT_FALLBACK_DELAY: Duration = Duration::from_secs(60);
const CROSS_PROCESS_LEASE_MAX_AGE: Duration = Duration::from_secs(90);
const PERSISTED_COOLDOWN_FILE: &str = "anchorbell-public-rest.cooldown";
const EXCHANGE_INFO_CACHE_TTL_MS: u64 = 6 * 60 * 60 * 1_000;
const EXCHANGE_INFO_CACHE_FALLBACK_TTL_MS: u64 = 24 * 60 * 60 * 1_000;
const EXCHANGE_INFO_CACHE_SCHEMA_VERSION: u16 = 1;
const PREMIUM_INDEX_CACHE_TTL_MS: u64 = 45_000;
const PREMIUM_INDEX_CACHE_FALLBACK_TTL_MS: u64 = 24 * 60 * 60 * 1_000;
const PREMIUM_INDEX_CACHE_SCHEMA_VERSION: u16 = 1;
const FUNDING_HISTORY_CACHE_TTL_MS: u64 = 5 * 60 * 1_000;
const FUNDING_HISTORY_CACHE_FALLBACK_TTL_MS: u64 = 60 * 60 * 1_000;
const FUNDING_INFO_CACHE_TTL_MS: u64 = 6 * 60 * 60 * 1_000;
const FUNDING_INFO_CACHE_FALLBACK_TTL_MS: u64 = 24 * 60 * 60 * 1_000;
const GENERIC_REST_CACHE_SCHEMA_VERSION: u16 = 1;
static PUBLIC_REST_GOVERNOR: OnceLock<Arc<tokio::sync::Mutex<PublicRestGovernor>>> =
    OnceLock::new();

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PublicRestRequestClass {
    Generic,
    Depth(usize),
    Funding,
    ExchangeInfo,
    IndexPriceKline,
}

impl PublicRestRequestClass {
    fn query_limit(path: &str) -> usize {
        path.split('&')
            .find_map(|part| part.strip_prefix("limit="))
            .and_then(|value| value.parse::<usize>().ok())
            .unwrap_or(100)
    }

    fn from_path(path: &str) -> Self {
        if path.contains("/depth") {
            Self::Depth(Self::query_limit(path))
        } else if path.contains("/fundingRate") || path.contains("/fundingInfo") {
            Self::Funding
        } else if path.contains("/exchangeInfo") {
            Self::ExchangeInfo
        } else if path.contains("/indexPriceKlines") {
            Self::IndexPriceKline
        } else {
            Self::Generic
        }
    }

    fn weight(self) -> f64 {
        let configured = &binance_runtime_config().request_weights;
        match self {
            Self::Depth(limit) => configured
                .depth_by_limit
                .get(&limit.to_string())
                .copied()
                .unwrap_or(configured.depth_default) as f64,
            Self::IndexPriceKline => configured.index_price_klines as f64,
            Self::Funding => configured.funding as f64,
            Self::ExchangeInfo => configured.exchange_info as f64,
            Self::Generic => configured.generic as f64,
        }
    }
}

#[derive(Debug)]
struct PublicRestGovernor {
    available_weight: f64,
    last_refill: Instant,
    next_allowed: Instant,
    cooldown_until: Instant,
    observed_used_weight: Option<u32>,
    rate_limited_responses: u64,
}

impl PublicRestGovernor {
    fn new() -> Self {
        let now = Instant::now();
        Self {
            available_weight: PUBLIC_REST_WEIGHT_CAPACITY,
            last_refill: now,
            next_allowed: now,
            cooldown_until: now,
            observed_used_weight: None,
            rate_limited_responses: 0,
        }
    }

    fn refill(&mut self, now: Instant) {
        let elapsed = now
            .saturating_duration_since(self.last_refill)
            .as_secs_f64();
        if elapsed > 0.0 {
            self.available_weight = (self.available_weight
                + elapsed * PUBLIC_REST_WEIGHT_PER_SECOND)
                .min(PUBLIC_REST_WEIGHT_CAPACITY);
            self.last_refill = now;
        }
    }

    fn wait_for(&mut self, weight: f64) -> Option<Duration> {
        let now = Instant::now();
        self.refill(now);
        let cooldown_wait = self.cooldown_until.saturating_duration_since(now);
        let pace_wait = self.next_allowed.saturating_duration_since(now);
        let weight_wait = if self.available_weight >= weight {
            Duration::ZERO
        } else {
            Duration::from_secs_f64(
                (weight - self.available_weight) / PUBLIC_REST_WEIGHT_PER_SECOND,
            )
        };
        let wait = cooldown_wait.max(pace_wait).max(weight_wait);
        if wait.is_zero() {
            self.available_weight -= weight;
            self.next_allowed = now + PUBLIC_REST_MIN_INTERVAL;
            None
        } else {
            Some(wait)
        }
    }
}

fn public_rest_governor() -> Arc<tokio::sync::Mutex<PublicRestGovernor>> {
    PUBLIC_REST_GOVERNOR
        .get_or_init(|| Arc::new(tokio::sync::Mutex::new(PublicRestGovernor::new())))
        .clone()
}

pub(crate) async fn pace_public_rest_request(path: &str) {
    let governor = public_rest_governor();
    let weight = PublicRestRequestClass::from_path(path).weight();
    if let Some(deadline) = persisted_cooldown_deadline().await {
        let mut state = governor.lock().await;
        state.cooldown_until = state.cooldown_until.max(deadline);
    }
    loop {
        let wait = {
            let mut state = governor.lock().await;
            state.wait_for(weight)
        };
        if let Some(delay) = wait {
            tokio::time::sleep(delay).await;
        } else {
            break;
        }
    }
}

/// Coordinate REST calls made by multiple AnchorBell processes sharing one
/// machine and one proxy/IP. The lease is short-lived and stale leases are
/// recoverable after a process crash.
pub(crate) struct CrossProcessRestLease {
    path: PathBuf,
}

impl Drop for CrossProcessRestLease {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.path);
    }
}

pub(crate) async fn acquire_cross_process_rest_lease() -> CrossProcessRestLease {
    let path = std::env::temp_dir().join("anchorbell-public-rest.lease");
    loop {
        match tokio::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&path)
            .await
        {
            Ok(mut file) => {
                use tokio::io::AsyncWriteExt;
                let _ = file
                    .write_all(format!("pid={}\\n", std::process::id()).as_bytes())
                    .await;
                return CrossProcessRestLease { path };
            }
            Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {
                let stale = tokio::fs::metadata(&path)
                    .await
                    .ok()
                    .and_then(|metadata| metadata.modified().ok())
                    .and_then(|modified| modified.elapsed().ok())
                    .is_some_and(|age| age > CROSS_PROCESS_LEASE_MAX_AGE);
                if stale {
                    let _ = tokio::fs::remove_file(&path).await;
                } else {
                    tokio::time::sleep(Duration::from_millis(25)).await;
                }
            }
            Err(_) => {
                tokio::time::sleep(Duration::from_millis(25)).await;
            }
        }
    }
}

/// Extends the process-wide and cross-process cooldown after Binance tells us
/// to back off. Binance's Retry-After header is authoritative when present.
fn retry_after_delay(headers: &reqwest::header::HeaderMap) -> Duration {
    headers
        .get(reqwest::header::RETRY_AFTER)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.trim().parse::<u64>().ok())
        .and_then(|seconds| seconds.checked_mul(1_000))
        .map(Duration::from_millis)
        .unwrap_or(RATE_LIMIT_FALLBACK_DELAY)
}

pub(crate) async fn note_public_rest_response(status: u16, headers: &reqwest::header::HeaderMap) {
    let observed_weight = headers
        .get("x-mbx-used-weight-1m")
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.parse::<u32>().ok());
    let governor = public_rest_governor();
    let mut state = governor.lock().await;
    state.observed_used_weight = observed_weight.or(state.observed_used_weight);
    if let Some(used) = observed_weight {
        if used >= PUBLIC_REST_SOFT_LIMIT {
            state.available_weight = state.available_weight.min(1.0);
        }
    }
    if !matches!(status, 418 | 429) {
        return;
    }
    state.rate_limited_responses = state.rate_limited_responses.saturating_add(1);
    let deadline = Instant::now() + retry_after_delay(headers);
    if state.cooldown_until < deadline {
        state.cooldown_until = deadline;
    }
    let cooldown_ms =
        current_time_ms().saturating_add(retry_after_delay(headers).as_millis() as u64);
    drop(state);
    let _ = persist_cooldown_deadline(cooldown_ms).await;
}

pub(crate) async fn public_rest_cooldown_active() -> bool {
    persisted_cooldown_deadline().await.is_some()
}

async fn persisted_cooldown_deadline() -> Option<Instant> {
    let path = std::env::temp_dir().join(PERSISTED_COOLDOWN_FILE);
    let text = tokio::fs::read_to_string(path).await.ok()?;
    let deadline_ms = text.trim().parse::<u64>().ok()?;
    let now_ms = current_time_ms();
    if deadline_ms <= now_ms {
        return None;
    }
    Some(Instant::now() + Duration::from_millis(deadline_ms - now_ms))
}

async fn persist_cooldown_deadline(deadline_ms: u64) -> std::io::Result<()> {
    let path = std::env::temp_dir().join(PERSISTED_COOLDOWN_FILE);
    let temp = path.with_extension("tmp");
    tokio::fs::write(&temp, deadline_ms.to_string()).await?;
    match tokio::fs::rename(&temp, &path).await {
        Ok(()) => Ok(()),
        Err(error) if path.exists() => {
            let _ = tokio::fs::remove_file(&path).await;
            tokio::fs::rename(temp, path).await.map_err(|_| error)
        }
        Err(error) => Err(error),
    }
}

fn exchange_info_cache_path(rest_base: &str) -> PathBuf {
    let key = rest_base
        .chars()
        .map(|character| {
            if character.is_ascii_alphanumeric() {
                character
            } else {
                '_'
            }
        })
        .collect::<String>();
    std::env::temp_dir()
        .join("anchorbell")
        .join(format!("exchange-info-{key}.json"))
}

async fn read_exchange_info_cache(
    rest_base: &str,
    max_age_ms: u64,
) -> Option<Vec<BinanceSymbolMetadata>> {
    let path = exchange_info_cache_path(rest_base);
    let bytes = tokio::fs::read(path).await.ok()?;
    let cache = serde_json::from_slice::<ExchangeInfoCache>(&bytes).ok()?;
    if cache.schema_version != EXCHANGE_INFO_CACHE_SCHEMA_VERSION
        || cache.rest_base != rest_base
        || current_time_ms().saturating_sub(cache.fetched_at_ms) > max_age_ms
    {
        return None;
    }
    Some(cache.symbols)
}

async fn write_exchange_info_cache(
    rest_base: &str,
    symbols: &[BinanceSymbolMetadata],
) -> std::io::Result<()> {
    let path = exchange_info_cache_path(rest_base);
    if let Some(parent) = path.parent() {
        tokio::fs::create_dir_all(parent).await?;
    }
    let cache = ExchangeInfoCache {
        schema_version: EXCHANGE_INFO_CACHE_SCHEMA_VERSION,
        rest_base: rest_base.to_owned(),
        fetched_at_ms: current_time_ms(),
        symbols: symbols.to_owned(),
    };
    let temp = path.with_extension(format!("tmp.{}.{}", std::process::id(), current_time_ms()));
    let bytes =
        serde_json::to_vec(&cache).map_err(|error| std::io::Error::other(error.to_string()))?;
    tokio::fs::write(&temp, bytes).await?;
    for attempt in 0..5 {
        match tokio::fs::rename(&temp, &path).await {
            Ok(()) => return Ok(()),
            Err(_error) if attempt < 4 => {
                if path.exists() {
                    let _ = tokio::fs::remove_file(&path).await;
                }
                tokio::time::sleep(Duration::from_millis(25)).await;
            }
            Err(error) => return Err(error),
        }
    }
    Err(std::io::Error::other(
        "exchange info cache replace exhausted",
    ))
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct ExchangeInfoCache {
    schema_version: u16,
    rest_base: String,
    fetched_at_ms: u64,
    symbols: Vec<BinanceSymbolMetadata>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct PremiumIndexCache {
    schema_version: u16,
    rest_base: String,
    symbol: String,
    fetched_at_ms: u64,
    snapshot: BinancePremiumIndexSnapshot,
}

fn generic_rest_cache_path(rest_base: &str, key: &str) -> PathBuf {
    let key = format!("{rest_base}-{key}")
        .chars()
        .map(|character| {
            if character.is_ascii_alphanumeric() {
                character
            } else {
                '_'
            }
        })
        .collect::<String>();
    std::env::temp_dir()
        .join("anchorbell")
        .join(format!("rest-{key}.json"))
}

async fn read_generic_rest_cache<T: DeserializeOwned>(
    rest_base: &str,
    key: &str,
    max_age_ms: u64,
) -> Option<T> {
    let path = generic_rest_cache_path(rest_base, key);
    let bytes = tokio::fs::read(path).await.ok()?;
    let cache = serde_json::from_slice::<GenericRestCache<T>>(&bytes).ok()?;
    if cache.schema_version != GENERIC_REST_CACHE_SCHEMA_VERSION
        || cache.rest_base != rest_base
        || cache.key != key
        || current_time_ms().saturating_sub(cache.fetched_at_ms) > max_age_ms
    {
        return None;
    }
    Some(cache.value)
}

async fn write_generic_rest_cache<T: Serialize>(
    rest_base: &str,
    key: &str,
    value: &T,
) -> std::io::Result<()> {
    let path = generic_rest_cache_path(rest_base, key);
    if let Some(parent) = path.parent() {
        tokio::fs::create_dir_all(parent).await?;
    }
    let cache = GenericRestCache {
        schema_version: GENERIC_REST_CACHE_SCHEMA_VERSION,
        rest_base: rest_base.to_owned(),
        key: key.to_owned(),
        fetched_at_ms: current_time_ms(),
        value,
    };
    let temp = path.with_extension(format!("tmp.{}.{}", std::process::id(), current_time_ms()));
    let bytes =
        serde_json::to_vec(&cache).map_err(|error| std::io::Error::other(error.to_string()))?;
    tokio::fs::write(&temp, bytes).await?;
    for attempt in 0..5 {
        match tokio::fs::rename(&temp, &path).await {
            Ok(()) => return Ok(()),
            Err(_error) if attempt < 4 => {
                if path.exists() {
                    let _ = tokio::fs::remove_file(&path).await;
                }
                tokio::time::sleep(Duration::from_millis(25)).await;
            }
            Err(error) => return Err(error),
        }
    }
    Err(std::io::Error::other(
        "generic REST cache replace exhausted",
    ))
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct GenericRestCache<T> {
    schema_version: u16,
    rest_base: String,
    key: String,
    fetched_at_ms: u64,
    value: T,
}

fn premium_index_cache_path(rest_base: &str, symbol: &str) -> PathBuf {
    let key = format!("{rest_base}-{symbol}")
        .chars()
        .map(|character| {
            if character.is_ascii_alphanumeric() {
                character
            } else {
                '_'
            }
        })
        .collect::<String>();
    std::env::temp_dir()
        .join("anchorbell")
        .join(format!("premium-index-{key}.json"))
}

async fn read_premium_index_cache(
    rest_base: &str,
    symbol: &str,
    max_age_ms: u64,
) -> Option<BinanceTimedPremiumIndexSnapshot> {
    let path = premium_index_cache_path(rest_base, symbol);
    let bytes = tokio::fs::read(path).await.ok()?;
    let cache = serde_json::from_slice::<PremiumIndexCache>(&bytes).ok()?;
    if cache.schema_version != PREMIUM_INDEX_CACHE_SCHEMA_VERSION
        || cache.rest_base != rest_base
        || cache.symbol != symbol
        || current_time_ms().saturating_sub(cache.fetched_at_ms) > max_age_ms
    {
        return None;
    }
    Some(BinanceTimedPremiumIndexSnapshot {
        observed_at_ms: if cache.snapshot.time > 0 {
            cache.snapshot.time
        } else {
            cache.fetched_at_ms
        },
        snapshot: cache.snapshot,
    })
}

async fn write_premium_index_cache(
    rest_base: &str,
    symbol: &str,
    snapshot: &BinancePremiumIndexSnapshot,
) -> std::io::Result<()> {
    let path = premium_index_cache_path(rest_base, symbol);
    if let Some(parent) = path.parent() {
        tokio::fs::create_dir_all(parent).await?;
    }
    let cache = PremiumIndexCache {
        schema_version: PREMIUM_INDEX_CACHE_SCHEMA_VERSION,
   