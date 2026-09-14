use serde::{Deserialize, Serialize};
use std::cmp::min;

pub const BPS_SCALE: i128 = 10_000;
pub const PICO_BPS_SCALE: i128 = 1_000_000_000_000;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct PriceTicks(pub i64);

impl PriceTicks {
    pub fn new(value: i64) -> Option<Self> {
        (value > 0).then_some(Self(value))
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct Quantity(pub i64);

impl Quantity {
    pub fn new(value: i64) -> Option<Self> {
        (value > 0).then_some(Self(value))
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum Side {
    Buy,
    Sell,
}

impl Side {
    pub fn sign(self) -> i64 {
        match self {
            Self::Buy => 1,
            Self::Sell => -1,
        }
    }
    pub fn opposite(self) -> Self {
        match self {
            Self::Buy => Self::Sell,
            Self::Sell => Self::Buy,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct Book {
    pub bid: PriceTicks,
    pub ask: PriceTicks,
    pub bid_quantity: Quantity,
    pub ask_quantity: Quantity,
    pub observed_at_ms: u64,
    pub sequence: u64,
}

impl Book {
    pub fn validate(self) -> Result<(), ModelError> {
        if self.bid.0 <= 0
            || self.ask.0 < self.bid.0
            || self.bid_quantity.0 <= 0
            || self.ask_quantity.0 <= 0
        {
            return Err(ModelError::InvalidMarket);
        }
        Ok(())
    }
    pub fn passive_price(self, side: Side) -> PriceTicks {
        match side {
            Side::Buy => self.bid,
            Side::Sell => self.ask,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Anchor {
    pub id: String,
    pub instrument: String,
    pub price: PriceTicks,
    pub observed_at_ms: u64,
    pub valid_until_ms: u64,
    pub source_digest: String,
}

impl Anchor {
    pub fn new(
        id: impl Into<String>,
        instrument: impl Into<String>,
        price: PriceTicks,
        observed_at_ms: u64,
        valid_until_ms: u64,
        source_digest: impl Into<String>,
    ) -> Result<Self, ModelError> {
        let value = Self {
            id: id.into(),
            instrument: instrument.into(),
            price,
            observed_at_ms,
            valid_until_ms,
            source_digest: source_digest.into(),
        };
        if value.id.is_empty()
            || value.instrument.is_empty()
            || value.source_digest.is_empty()
            || price.0 <= 0
            || valid_until_ms <= observed_at_ms
        {
            return Err(ModelError::InvalidAnchor);
        }
        Ok(value)
    }
    pub fn valid_at(&self, now_ms: u64) -> bool {
        now_ms >= self.observed_at_ms && now_ms < self.valid_until_ms
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct ClosedWindow {
    pub closed_at_ms: u64,
    pub entry_deadline_ms: u64,
    pub external_open_ms: u64,
    pub hard_flatten_ms: u64,
}

impl ClosedWindow {
    pub fn new(
        closed_at_ms: u64,
        entry_deadline_ms: u64,
        external_open_ms: u64,
        hard_flatten_ms: u64,
    ) -> Result<Self, ModelError> {
        if !(closed_at_ms < entry_deadline_ms
            && entry_deadline_ms <= external_open_ms
            && external_open_ms <= hard_flatten_ms)
        {
            return Err(ModelError::InvalidWindow);
        }
        Ok(Self {
            closed_at_ms,
            entry_deadline_ms,
            external_open_ms,
            hard_flatten_ms,
        })
    }
    pub fn entry_allowed(&self, now_ms: u64) -> bool {
        now_ms >= self.closed_at_ms && now_ms < self.entry_deadline_ms
    }
    pub fn reduce_only(&self, now_ms: u64) -> bool {
        now_ms >= self.entry_deadline_ms && now_ms < self.hard_flatten_ms
    }
    pub fn hard_deadline(&self, now_ms: u64) -> bool {
        now_ms >= self.hard_flatten_ms
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum EpisodePhase {
    Unborn,
    Eligible,
    Building,
    Harvesting,
    Reducing,
    Flat,
    Closed,
    Reconcile,
    Halt,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AnchorEpisode {
    pub id: String,
    pub anchor: Anchor,
    pub window: ClosedWindow,
    pub funding_deadline_ms: Option<u64>,
    pub phase: EpisodePhase,
}

impl AnchorEpisode {
    pub fn new(
        id: impl Into<String>,
        anchor: Anchor,
        window: ClosedWindow,
        funding_deadline_ms: Option<u64>,
    ) -> Result<Self, ModelError> {
        let id = id.into();
        if id.is_empty() || funding_deadline_ms.is_some_and(|v| v <= window.closed_at_ms) {
            return Err(ModelError::InvalidEpisode);
        }
        Ok(Self {
            id,
            anchor,
            window,
            funding_deadline_ms,
            phase: EpisodePhase::Unborn,
        })
    }
    pub fn phase_at(&self, now_ms: u64, position: i64) -> EpisodePhase {
        if self.window.hard_deadline(now_ms) {
            return EpisodePhase::Closed;
        }
        if position != 0
            && (self.window.reduce_only(now_ms)
                || self.funding_deadline_ms.is_some_and(|d| now_ms >= d))
        {
            return EpisodePhase::Reducing;
        }
        if self.window.entry_allowed(now_ms) {
            EpisodePhase::Eligible
        } else {
            EpisodePhase::Harvesting
        }
    }
    pub fn entry_allowed(&self, now_ms: u64) -> bool {
        self.anchor.valid_at(now_ms) && self.window.entry_allowed(now_ms)
    }
    pub fn earliest_exit_ms(&self) -> u64 {
        self.funding_deadline_ms
            .map_or(self.window.external_open_ms, |d| {
                min(d, self.window.external_open_ms)
            })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ContractStatus {
    Trading,
    Halted,
    Unknown,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum PositionMode {
    OneWay,
    Hedge,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct PriceFilter {
    pub min: PriceTicks,
    pub max: PriceTicks,
    pub tick: i64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct QuantityFilter {
    pub min: Quantity,
    pub max: Quantity,
    pub step: i64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BinanceContract {
    pub symbol: String,
    pub status: ContractStatus,
    pub contract_type: String,
    pub price_filter: PriceFilter,
    pub quantity_filter: QuantityFilter,
    pub min_notional_ticks: i128,
    pub position_mode: PositionMode,
    pub post_only_supported: bool,
    pub observed_at_ms: u64,
    pub max_age_ms: u64,
    pub digest: String,
}

impl BinanceContract {
    pub fn validate(
        &self,
        order: CandidateOrder,
        book: Book,
        account: AccountSnapshot,
        now_ms: u64,
    ) -> Result<ValidatedOrder, ModelError> {
        book.validate()?;
        if self.symbol != order.symbol
            || self.status != ContractStatus::Trading
            || !self.post_only_supported
            || self.digest.is_empty()
            || now_ms < self.observed_at_ms
            || now_ms - self.observed_at_ms > self.max_age_ms
        {
            return Err(ModelError::ContractUnavailable);
        }
        if order.price.0 < self.price_filter.min.0
            || order.price.0 > self.price_filter.max.0
            || self.price_filter.tick <= 0
            || order.price.0 % self.price_filter.tick != 0
        {
            return Err(ModelError::PriceFilter);
        }
        if order.quantity.0 < self.quantity_filter.min.0
            || order.quantity.0 > self.quantity_filter.max.0
            || self.quantity_filter.step <= 0
            || order.quantity.0 % self.quantity_filter.step != 0
        {
            return Err(ModelError::QuantityFilter);
        }
        let notional = i128::from(order.price.0)
            .checked_mul(i128::from(order.quantity.0))
            .ok_or(ModelError::NotionalFilter)?;
        if notional < self.min_notional_ticks {
            return Err(ModelError::NotionalFilter);
        }
        if (order.side == Side::Buy && order.price.0 >= book.ask.0)
            || (order.side == Side::Sell && order.price.0 <= book.bid.0)
        {
            return Err(ModelError::WouldTakeLiquidity);
        }
        if order.reduce_only {
            let reducing = (order.side == Side::Sell && account.position > 0)
                || (order.side == Side::Buy && account.position < 0);
            if !reducing {
                return Err(ModelError::InvalidReduceOnly);
            }
        } else {
            let next = i128::from(account.position)
                + i128::from(order.side.sign()) * i128::from(order.quantity.0);
            if next.abs() > i128::from(account.max_position) {
                return Err(ModelError::PositionLimit);
            }
        }
        Ok(ValidatedOrder {
            order,
            contract_digest: self.digest.clone(),
            validated_at_ms: now_ms,
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct MarketSnapshot {
    pub book: Book,
    pub index: PriceTicks,
    pub mark: PriceTicks,
    pub server_time_ms: u64,
    pub max_age_ms: u64,
}

impl MarketSnapshot {
    pub fn validate(&self, now_ms: u64) -> Result<(), ModelError> {
        self.book.validate()?;
        if self.index.0 <= 0
            || self.mark.0 <= 0
            || now_ms < self.server_time_ms
            || now_ms - self.server_time_ms > self.max_age_ms
        {
            return Err(ModelError::StaleMarket);
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct AccountSnapshot {
    pub position: i64,
    pub max_position: i64,
    pub reconciled: bool,
    pub margin_available_ppm: i64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CandidateOrder {
    pub symbol: String,
    pub side: Side,
    pub price: PriceTicks,
    pub quantity: Quantity,
    pub reduce_only: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ValidatedOrder {
    pub order: CandidateOrder,
    pub contract_digest: String,
    pub validated_at_ms: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct OutcomeScenario {
    pub weight_bps: u16,
    pub pnl_pico_bps: i64,
    pub terminal: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct OutcomeDistribution {
    pub scenarios: Vec<OutcomeScenario>,
    pub uncertainty_pico_bps: i64,
}

impl OutcomeDistribution {
    pub fn lower_value(&self) -> Option<i64> {
        if self.scenarios.is_empty()
            || self.uncertainty_pico_bps < 0
            || self
                .scenarios
                .iter()
                .any(|s| s.weight_bps == 0 || s.pnl_pico_bps == i64::MIN)
        {
            return None;
        }
        let total: u32 = self.scenarios.iter().map(|s| u32::from(s.weight_bps)).sum();
        if total != 10_000 {
            return None;
        }
        self.scenarios
            .iter()
            .map(|s| s.pnl_pico_bps)
            .min()
            .map(|v| v.saturating_sub(self.uncertainty_pico_bps))
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EvidenceFrame {
    pub now_ms: u64,
    pub episode: AnchorEpisode,
    pub contract: BinanceContract,
    pub market: MarketSnapshot,
    pub account: AccountSnapshot,
    pub external_closed: bool,
    pub calendar_known: bool,
    pub funding_known: bool,
    pub model_version: String,
    pub buy_outcome: OutcomeDistribution,
    pub sell_outcome: OutcomeDistribution,
}

impl EvidenceFrame {
    pub fn complete_for_entry(&self) -> bool {
        self.calendar_known
            && self.external_closed
            && self.funding_known
            && self.account.reconciled
            && !self.model_version.is_empty()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ModelError {
    InvalidAnchor,
    InvalidWindow,
    InvalidEpisode,
    InvalidMarket,
    StaleMarket,
    ContractUnavailable,
    PriceFilter,
    QuantityFilter,
    NotionalFilter,
    WouldTakeLiquidity,
    InvalidReduceOnly,
    PositionLimit,
    IncompleteEvidence,
}
