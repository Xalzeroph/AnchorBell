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
    pub fn validate(&self) -> Result<(), ModelError> {
        if self.bid.0 <= 0
            || self.ask.0 <= self.bid.0
            || self.bid_quantity.0 <= 0
            || self.ask_quantity.0 <= 0
            || self.sequence == 0
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

    pub fn queue_ahead_quantity(self, side: Side) -> Quantity {
        match side {
            Side::Buy => self.bid_quantity,
            Side::Sell => self.ask_quantity,
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
    pub fn residual_pico_bps(&self, fair_price: PriceTicks) -> Option<i64> {
        if fair_price.0 <= 0 {
            return None;
        }
        let residual =
            (i128::from(self.price.0) - i128::from(fair_price.0)).checked_mul(PICO_BPS_SCALE)?;
        let residual = floor_div_positive(residual, i128::from(self.price.0))?;
        i64::try_from(residual).ok()
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
pub struct FundingSchedule {
    pub next_funding_at_ms: u64,
    pub interval_ms: u64,
    pub observed_at_ms: u64,
    pub max_age_ms: u64,
    pub source_digest: String,
}

impl FundingSchedule {
    pub fn new(
        next_funding_at_ms: u64,
        interval_ms: u64,
        observed_at_ms: u64,
        max_age_ms: u64,
        source_digest: impl Into<String>,
    ) -> Result<Self, ModelError> {
        let schedule = Self {
            next_funding_at_ms,
            interval_ms,
            observed_at_ms,
            max_age_ms,
            source_digest: source_digest.into(),
        };
        if schedule.next_funding_at_ms <= schedule.observed_at_ms
            || schedule.interval_ms == 0
            || schedule.max_age_ms == 0
            || schedule.source_digest.is_empty()
        {
            return Err(ModelError::InvalidFundingSchedule);
        }
        Ok(schedule)
    }

    pub fn valid_at(&self, now_ms: u64) -> bool {
        now_ms >= self.observed_at_ms && now_ms - self.observed_at_ms <= self.max_age_ms
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AnchorEpisode {
    pub id: String,
    pub anchor: Anchor,
    pub window: ClosedWindow,
    pub funding: Option<FundingSchedule>,
    pub phase: EpisodePhase,
}

impl AnchorEpisode {
    pub fn new(
        id: impl Into<String>,
        anchor: Anchor,
        window: ClosedWindow,
        funding: Option<FundingSchedule>,
    ) -> Result<Self, ModelError> {
        let id = id.into();
        if id.is_empty()
            || funding
                .as_ref()
                .is_some_and(|schedule| schedule.next_funding_at_ms <= window.closed_at_ms)
        {
            return Err(ModelError::InvalidEpisode);
        }
        Ok(Self {
            id,
            anchor,
            window,
            funding,
            phase: EpisodePhase::Unborn,
        })
    }
    pub fn phase_at(&self, now_ms: u64, has_position: bool) -> EpisodePhase {
        if self.window.hard_deadline(now_ms) {
            return EpisodePhase::Closed;
        }
        if has_position
            && (self.window.reduce_only(now_ms)
                || self
                    .funding
                    .as_ref()
                    .is_some_and(|schedule| now_ms >= schedule.next_funding_at_ms))
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
    pub fn funding_valid_at(&self, now_ms: u64) -> bool {
        self.funding
            .as_ref()
            .is_some_and(|schedule| schedule.valid_at(now_ms))
    }
    pub fn earliest_exit_ms(&self) -> u64 {
        self.funding
            .as_ref()
            .map_or(self.window.external_open_ms, |schedule| {
                min(schedule.next_funding_at_ms, self.window.external_open_ms)
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
pub enum PositionSide {
    Both,
    Long,
    Short,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum TimeInForce {
    Gtc,
    Ioc,
    Fok,
    Gtx,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum WorkingType {
    MarkPrice,
    ContractPrice,
    IndexPrice,
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
    pub max_notional_ticks: Option<i128>,
    pub position_mode: PositionMode,
    pub post_only_supported: bool,
    pub reduce_only_supported: bool,
    pub close_position_supported: bool,
    pub conditional_orders_supported: bool,
    pub trigger_protect_bps: u16,
    pub rate_limit_remaining: u32,
    pub max_open_orders: u32,
    pub open_orders: u32,
    pub self_trade_prevention_enabled: bool,
    pub observed_at_ms: u64,
    pub max_age_ms: u64,
    pub digest: String,
}

impl BinanceContract {
    pub fn validate(
        &self,
        order: CandidateOrder,
        book: Book,
        account: &AccountSnapshot,
        now_ms: u64,
    ) -> Result<ValidatedOrder, ModelError> {
        book.validate()?;
        account.validate_for(self.position_mode, now_ms)?;
        if !account.reconciled {
            return Err(ModelError::IncompleteEvidence);
        }
        if !order.reduce_only && account.margin_available_ppm == 0 {
            return Err(ModelError::InsufficientMargin);
        }
        if self.symbol != order.symbol
            || self.status != ContractStatus::Trading
            || !self.post_only_supported
            || self.rate_limit_remaining == 0
            || self.max_open_orders == 0
            || self.open_orders >= self.max_open_orders
            || !self.self_trade_prevention_enabled
            || self.contract_type.trim().is_empty()
            || self.digest.is_empty()
            || self.min_notional_ticks <= 0
            || self
                .max_notional_ticks
                .is_some_and(|value| value <= 0 || value < self.min_notional_ticks)
            || self.max_age_ms == 0
            || now_ms < self.observed_at_ms
            || now_ms - self.observed_at_ms > self.max_age_ms
        {
            return Err(ModelError::ContractUnavailable);
        }
        let valid_position_side = match self.position_mode {
            PositionMode::OneWay => order.position_side == PositionSide::Both,
            PositionMode::Hedge => order.position_side != PositionSide::Both,
        };
        if !valid_position_side
            || order.time_in_force != TimeInForce::Gtx
            || (order.reduce_only && !self.reduce_only_supported)
            || (order.reduce_only && self.position_mode == PositionMode::Hedge)
            || (order.close_position && !self.close_position_supported)
            || (order.close_position && !order.reduce_only)
            || (order.close_position && order.trigger_price.is_none())
        {
            return Err(ModelError::InvalidOrderSemantics);
        }
        if let Some(trigger_price) = order.trigger_price {
            if !self.conditional_orders_supported
                || trigger_price.0 <= 0
                || (order.price_protect && self.trigger_protect_bps == 0)
            {
                return Err(ModelError::InvalidOrderSemantics);
            }
        } else if order.price_protect {
            return Err(ModelError::InvalidOrderSemantics);
        }
        if !price_filter_contains(self.price_filter, order.price) {
            return Err(ModelError::PriceFilter);
        }
        if !quantity_filter_contains(self.quantity_filter, order.quantity) {
            return Err(ModelError::QuantityFilter);
        }
        let notional = i128::from(order.price.0)
            .checked_mul(i128::from(order.quantity.0))
            .ok_or(ModelError::NotionalFilter)?;
        if notional < self.min_notional_ticks
            || self
                .max_notional_ticks
                .is_some_and(|maximum| notional > maximum)
        {
            return Err(ModelError::NotionalFilter);
        }
        if (order.side == Side::Buy && order.price.0 >= book.ask.0)
            || (order.side == Side::Sell && order.price.0 <= book.bid.0)
        {
            return Err(ModelError::WouldTakeLiquidity);
        }
        let is_reducing = match self.position_mode {
            PositionMode::OneWay => {
                (order.side == Side::Sell && account.position > 0)
                    || (order.side == Side::Buy && account.position < 0)
            }
            PositionMode::Hedge => {
                (order.side == Side::Sell && order.position_side == PositionSide::Long)
                    || (order.side == Side::Buy && order.position_side == PositionSide::Short)
            }
        };
        if order.reduce_only && !is_reducing {
            return Err(ModelError::InvalidReduceOnly);
        }
        if self.position_mode == PositionMode::OneWay && is_reducing && !order.reduce_only {
            return Err(ModelError::InvalidReduceOnly);
        }
        if is_reducing {
            let current = match self.position_mode {
                PositionMode::OneWay => account.position.checked_abs(),
                PositionMode::Hedge => match order.position_side {
                    PositionSide::Long => Some(account.long_position),
                    PositionSide::Short => Some(account.short_position),
                    PositionSide::Both => None,
                },
            }
            .ok_or(ModelError::InvalidReduceOnly)?;
            if order.quantity.0 > current {
                return Err(ModelError::PositionLimit);
            }
        } else {
            let current = match self.position_mode {
                PositionMode::OneWay => i128::from(account.position)
                    .checked_add(i128::from(order.side.sign()) * i128::from(order.quantity.0))
                    .ok_or(ModelError::PositionLimit)?
                    .abs(),
                PositionMode::Hedge => match order.position_side {
                    PositionSide::Long => i128::from(account.long_position)
                        .checked_add(i128::from(order.quantity.0))
                        .ok_or(ModelError::PositionLimit)?,
                    PositionSide::Short => i128::from(account.short_position)
                        .checked_add(i128::from(order.quantity.0))
                        .ok_or(ModelError::PositionLimit)?,
                    PositionSide::Both => return Err(ModelError::InvalidOrderSemantics),
                },
            };
            if current > i128::from(account.max_position) {
                return Err(ModelError::PositionLimit);
            }
        }
        let route = if is_reducing {
            OrderRoute::PassiveReduceOnly
        } else {
            OrderRoute::PassiveMaker
        };
        Ok(ValidatedOrder {
            queue_ahead_quantity: book.queue_ahead_quantity(order.side).0,
            order,
            route,
            contract_digest: self.digest.clone(),
            validated_at_ms: now_ms,
        })
    }

    pub fn validate_emergency_reduce_only(
        &self,
        order: CandidateOrder,
        book: Book,
        account: &AccountSnapshot,
        now_ms: u64,
    ) -> Result<ValidatedOrder, ModelError> {
        book.validate()?;
        account.validate_for(self.position_mode, now_ms)?;
        if !account.reconciled {
            return Err(ModelError::IncompleteEvidence);
        }
        if self.symbol != order.symbol
            || self.status != ContractStatus::Trading
            || self.rate_limit_remaining == 0
            || self.max_open_orders == 0
            || self.open_orders >= self.max_open_orders
            || self.contract_type.trim().is_empty()
            || self.digest.is_empty()
            || self.min_notional_ticks <= 0
            || self
                .max_notional_ticks
                .is_some_and(|value| value <= 0 || value < self.min_notional_ticks)
            || self.max_age_ms == 0
            || now_ms < self.observed_at_ms
            || now_ms - self.observed_at_ms > self.max_age_ms
            || order.time_in_force != TimeInForce::Ioc
            || order.close_position
            || order.trigger_price.is_some()
            || order.price_protect
            || (self.position_mode == PositionMode::OneWay
                && (!order.reduce_only || order.position_side != PositionSide::Both))
            || (self.position_mode == PositionMode::Hedge
                && (order.reduce_only || order.position_side == PositionSide::Both))
            || (self.position_mode == PositionMode::OneWay && !self.reduce_only_supported)
        {
            return Err(ModelError::InvalidOrderSemantics);
        }
        if !price_filter_contains(self.price_filter, order.price) {
            return Err(ModelError::PriceFilter);
        }
        let Some((expected_side, expected_position_side, available)) =
            account.reduction(self.position_mode)
        else {
            return Err(ModelError::InvalidReduceOnly);
        };
        if !quantity_filter_contains(self.quantity_filter, order.quantity)
            || order.side != expected_side
            || order.position_side != expected_position_side
            || order.quantity.0 > available
        {
            return Err(ModelError::QuantityFilter);
        }
        let notional = i128::from(order.price.0)
            .checked_mul(i128::from(order.quantity.0))
            .ok_or(ModelError::NotionalFilter)?;
        if notional < self.min_notional_ticks
            || self
                .max_notional_ticks
                .is_some_and(|maximum| notional > maximum)
        {
            return Err(ModelError::NotionalFilter);
        }
        let reducing = match self.position_mode {
            PositionMode::OneWay => {
                order.reduce_only
                    && ((order.side == Side::Sell && account.position > 0)
                        || (order.side == Side::Buy && account.position < 0))
            }
            PositionMode::Hedge => {
                !order.reduce_only
                    && ((order.side == Side::Sell && order.position_side == PositionSide::Long)
                        || (order.side == Side::Buy && order.position_side == PositionSide::Short))
            }
        };
        if !reducing {
            return Err(ModelError::InvalidReduceOnly);
        }
        Ok(ValidatedOrder {
            queue_ahead_quantity: book.queue_ahead_quantity(order.side).0,
            order,
            route: OrderRoute::EmergencyReduceOnly,
            contract_digest: self.digest.clone(),
            validated_at_ms: now_ms,
        })
    }
}

fn price_filter_contains(filter: PriceFilter, price: PriceTicks) -> bool {
    filter.min.0 > 0
        && filter.max.0 >= filter.min.0
        && filter.tick > 0
        && price.0 >= filter.min.0
        && price.0 <= filter.max.0
        && price.0 % filter.tick == 0
}

fn quantity_filter_contains(filter: QuantityFilter, quantity: Quantity) -> bool {
    filter.min.0 > 0
        && filter.max.0 >= filter.min.0
        && filter.step > 0
        && quantity.0 >= filter.min.0
        && quantity.0 <= filter.max.0
        && quantity.0 % filter.step == 0
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MarketSnapshot {
    pub book: Book,
    pub index: PriceTicks,
    pub index_observed_at_ms: u64,
    pub index_max_age_ms: u64,
    pub mark: PriceTicks,
    pub mark_observed_at_ms: u64,
    pub mark_max_age_ms: u64,
    pub server_time_ms: u64,
    pub max_age_ms: u64,
    pub source_digest: String,
}

impl MarketSnapshot {
    pub fn validate(&self, now_ms: u64) -> Result<(), ModelError> {
        self.book.validate()?;
        if self.max_age_ms == 0
            || self.index_max_age_ms == 0
            || self.mark_max_age_ms == 0
            || self.index.0 <= 0
            || self.mark.0 <= 0
            || self.source_digest.is_empty()
            || now_ms < self.server_time_ms
            || now_ms - self.server_time_ms > self.max_age_ms
            || now_ms < self.book.observed_at_ms
            || now_ms - self.book.observed_at_ms > self.max_age_ms
            || now_ms < self.index_observed_at_ms
            || now_ms - self.index_observed_at_ms > self.index_max_age_ms
            || now_ms < self.mark_observed_at_ms
            || now_ms - self.mark_observed_at_ms > self.mark_max_age_ms
        {
            return Err(ModelError::StaleMarket);
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AccountSnapshot {
    /// Signed net position used by Binance One-way mode.
    pub position: i64,
    /// Long and short legs used by Binance Hedge mode. Both may be non-zero.
    pub long_position: i64,
    pub short_position: i64,
    pub max_position: i64,
    pub reconciled: bool,
    pub margin_available_ppm: i64,
    pub observed_at_ms: u64,
    pub max_age_ms: u64,
    pub source_digest: String,
}

impl AccountSnapshot {
    pub fn validate(&self) -> Result<(), ModelError> {
        if self.max_position < 0
            || self.position.unsigned_abs() > self.max_position as u64
            || self.long_position < 0
            || self.short_position < 0
            || self.long_position > self.max_position
            || self.short_position > self.max_position
            || !(0..=1_000_000).contains(&self.margin_available_ppm)
        {
            return Err(ModelError::InvalidAccount);
        }
        Ok(())
    }

    pub fn has_position(&self, mode: PositionMode) -> bool {
        match mode {
            PositionMode::OneWay => self.position != 0,
            PositionMode::Hedge => self.long_position != 0 || self.short_position != 0,
        }
    }

    pub fn reduction(&self, mode: PositionMode) -> Option<(Side, PositionSide, i64)> {
        match mode {
            PositionMode::OneWay if self.position > 0 => {
                Some((Side::Sell, PositionSide::Both, self.position))
            }
            PositionMode::OneWay if self.position < 0 => {
                Some((Side::Buy, PositionSide::Both, self.position.checked_abs()?))
            }
            PositionMode::OneWay => None,
            // A single order cannot safely flatten both hedge legs. Fail closed and
            // require the authoritative executor to issue two separately audited orders.
            PositionMode::Hedge if self.long_position > 0 && self.short_position == 0 => {
                Some((Side::Sell, PositionSide::Long, self.long_position))
            }
            PositionMode::Hedge if self.short_position > 0 && self.long_position == 0 => {
                Some((Side::Buy, PositionSide::Short, self.short_position))
            }
            PositionMode::Hedge => None,
        }
    }

    pub fn leg_quantity(
        &self,
        mode: PositionMode,
        side: Side,
        position_side: PositionSide,
    ) -> Option<i64> {
        match mode {
            PositionMode::OneWay if position_side == PositionSide::Both => {
                let remaining = match side {
                    Side::Buy => i128::from(self.max_position) - i128::from(self.position),
                    Side::Sell => i128::from(self.max_position) + i128::from(self.position),
                };
                (remaining >= 0)
                    .then(|| i64::try_from(remaining).ok())
                    .flatten()
            }
            PositionMode::Hedge => match position_side {
                PositionSide::Long if side == Side::Buy => {
                    self.max_position.checked_sub(self.long_position)
                }
                PositionSide::Long if side == Side::Sell => Some(self.long_position),
                PositionSide::Short if side == Side::Sell => {
                    self.max_position.checked_sub(self.short_position)
                }
                PositionSide::Short if side == Side::Buy => Some(self.short_position),
                PositionSide::Both | PositionSide::Long | PositionSide::Short => None,
            },
            PositionMode::OneWay => None,
        }
    }

    pub fn validate_at(&self, now_ms: u64) -> Result<(), ModelError> {
        self.validate()?;
        if self.observed_at_ms == 0
            || self.max_age_ms == 0
            || now_ms < self.observed_at_ms
            || now_ms - self.observed_at_ms > self.max_age_ms
            || self.source_digest.is_empty()
        {
            return Err(ModelError::InvalidAccount);
        }
        Ok(())
    }

    pub fn validate_for(&self, mode: PositionMode, now_ms: u64) -> Result<(), ModelError> {
        self.validate_at(now_ms)?;
        match mode {
            PositionMode::OneWay if self.long_position == 0 && self.short_position == 0 => Ok(()),
            PositionMode::Hedge if self.position == 0 => Ok(()),
            _ => Err(ModelError::InvalidAccount),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CandidateOrder {
    pub symbol: String,
    pub side: Side,
    pub price: PriceTicks,
    pub quantity: Quantity,
    pub reduce_only: bool,
    pub position_side: PositionSide,
    pub time_in_force: TimeInForce,
    pub working_type: WorkingType,
    pub price_protect: bool,
    pub trigger_price: Option<PriceTicks>,
    pub close_position: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum OrderRoute {
    PassiveMaker,
    PassiveReduceOnly,
    EmergencyReduceOnly,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ValidatedOrder {
    pub queue_ahead_quantity: i64,
    pub order: CandidateOrder,
    pub route: OrderRoute,
    pub contract_digest: String,
    pub validated_at_ms: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct QueueFillEstimate {
    pub queue_ahead_quantity: i64,
    pub order_quantity: i64,
    pub traded_through_quantity: i64,
    pub observed_latency_ms: u64,
}

impl QueueFillEstimate {
    pub fn causal_fill_quantity(&self) -> Option<i64> {
        if self.queue_ahead_quantity < 0
            || self.order_quantity <= 0
            || self.traded_through_quantity < 0
        {
            return None;
        }
        Some(
            self.traded_through_quantity
                .saturating_sub(self.queue_ahead_quantity)
                .min(self.order_quantity),
        )
    }

    pub fn fill_probability_bps(&self) -> Option<u16> {
        let available = self.causal_fill_quantity()?;
        let probability = i128::from(available)
            .checked_mul(BPS_SCALE)?
            .checked_div(i128::from(self.order_quantity))?;
        u16::try_from(probability).ok()
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ExecutionCycle {
    pub side: Side,
    pub anchor_price: PriceTicks,
    pub entry_price: PriceTicks,
    pub exit_price: PriceTicks,
    pub requested_quantity: Quantity,
    pub entry_filled_quantity: i64,
    pub exit_filled_quantity: i64,
    pub entry_queue_ahead_quantity: i64,
    pub exit_queue_ahead_quantity: i64,
    pub entry_traded_through_quantity: i64,
    pub exit_traded_through_quantity: i64,
    pub entry_latency_ms: u64,
    pub exit_latency_ms: u64,
    pub entry_fee_pico_bps: i64,
    pub exit_fee_pico_bps: i64,
    pub exit_cost_pico_bps: i64,
    pub funding_cost_pico_bps: i64,
    pub deadline_risk_pico_bps: i64,
    pub entry_at_ms: u64,
    pub exit_at_ms: u64,
    pub deadline_ms: u64,
}

impl ExecutionCycle {
    fn validate(&self) -> Option<()> {
        if self.anchor_price.0 <= 0
            || self.entry_price.0 <= 0
            || self.exit_price.0 <= 0
            || self.requested_quantity.0 <= 0
            || self.entry_filled_quantity <= 0
            || self.exit_filled_quantity < 0
            || self.entry_filled_quantity > self.requested_quantity.0
            || self.exit_filled_quantity > self.entry_filled_quantity
            || self.entry_queue_ahead_quantity < 0
            || self.exit_queue_ahead_quantity < 0
            || self.entry_traded_through_quantity < 0
            || self.exit_traded_through_quantity < 0
            || self.entry_at_ms == 0
            || self.exit_at_ms <= self.entry_at_ms
            || self.deadline_ms <= self.exit_at_ms
            || self.entry_fee_pico_bps < 0
            || self.exit_fee_pico_bps < 0
            || self.exit_cost_pico_bps < 0
            || self.funding_cost_pico_bps < 0
            || self.deadline_risk_pico_bps < 0
        {
            return None;
        }
        let entry_causal_fill = QueueFillEstimate {
            queue_ahead_quantity: self.entry_queue_ahead_quantity,
            order_quantity: self.requested_quantity.0,
            traded_through_quantity: self.entry_traded_through_quantity,
            observed_latency_ms: self.entry_latency_ms,
        }
        .causal_fill_quantity()?;
        if self.entry_filled_quantity > entry_causal_fill {
            return None;
        }
        let exit_causal_fill = QueueFillEstimate {
            queue_ahead_quantity: self.exit_queue_ahead_quantity,
            order_quantity: self.entry_filled_quantity,
            traded_through_quantity: self.exit_traded_through_quantity,
            observed_latency_ms: self.exit_latency_ms,
        }
        .causal_fill_quantity()?;
        if self.exit_filled_quantity > exit_causal_fill {
            return None;
        }
        Some(())
    }

    pub fn terminal(&self) -> bool {
        self.exit_filled_quantity == self.entry_filled_quantity
    }

    pub fn gross_anchor_pnl_pico_bps(&self) -> Option<i128> {
        self.validate()?;
        let signed_delta = (i128::from(self.exit_price.0) - i128::from(self.entry_price.0))
            * i128::from(self.side.sign());
        let numerator = signed_delta
            .checked_mul(PICO_BPS_SCALE)?
            .checked_mul(i128::from(self.exit_filled_quantity))?;
        let denominator =
            i128::from(self.anchor_price.0).checked_mul(i128::from(self.requested_quantity.0))?;
        floor_div_positive(numerator, denominator)
    }

    pub fn net_value(&self) -> Option<i128> {
        let gross = self.gross_anchor_pnl_pico_bps()?;
        let fee =
            i128::from(self.entry_fee_pico_bps).checked_add(i128::from(self.exit_fee_pico_bps))?;
        Some(
            gross
                - fee
                - i128::from(self.exit_cost_pico_bps)
                - i128::from(self.funding_cost_pico_bps)
                - i128::from(self.deadline_risk_pico_bps),
        )
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum OutcomeScenario {
    Cycle {
        weight_bps: u16,
        cycle: ExecutionCycle,
    },
    Wait {
        weight_bps: u16,
        value_pico_bps: i64,
    },
}

impl OutcomeScenario {
    pub fn cycle(weight_bps: u16, cycle: ExecutionCycle) -> Self {
        Self::Cycle { weight_bps, cycle }
    }

    pub fn wait(weight_bps: u16, value_pico_bps: i64) -> Self {
        Self::Wait {
            weight_bps,
            value_pico_bps,
        }
    }

    fn weight_bps(&self) -> u16 {
        match self {
            Self::Cycle { weight_bps, .. } | Self::Wait { weight_bps, .. } => *weight_bps,
        }
    }

    fn terminal(&self) -> bool {
        match self {
            Self::Cycle { cycle, .. } => cycle.terminal(),
            Self::Wait { .. } => true,
        }
    }

    pub fn net_value(&self) -> Option<i128> {
        match self {
            Self::Cycle { cycle, .. } => cycle.net_value(),
            Self::Wait { value_pico_bps, .. } => {
                (*value_pico_bps != i64::MIN).then_some(i128::from(*value_pico_bps))
            }
        }
    }
}

pub fn floor_div_positive(numerator: i128, denominator: i128) -> Option<i128> {
    if denominator <= 0 {
        return None;
    }
    let quotient = numerator / denominator;
    let remainder = numerator % denominator;
    if remainder != 0 && numerator < 0 {
        quotient.checked_sub(1)
    } else {
        Some(quotient)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct UncertaintyBudget {
    pub anchor_pico_bps: i64,
    pub execution_pico_bps: i64,
    pub timing_pico_bps: i64,
    pub model_pico_bps: i64,
}

impl UncertaintyBudget {
    pub fn total(self) -> Option<i64> {
        let mut total = 0_i128;
        for component in [
            self.anchor_pico_bps,
            self.execution_pico_bps,
            self.timing_pico_bps,
            self.model_pico_bps,
        ] {
            if component < 0 {
                return None;
            }
            total = total.checked_add(i128::from(component))?;
        }
        i64::try_from(total).ok()
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct OutcomeDistribution {
    pub scenarios: Vec<OutcomeScenario>,
    pub uncertainty: UncertaintyBudget,
}

impl OutcomeDistribution {
    pub fn lower_value(&self) -> Option<i64> {
        let uncertainty = self.uncertainty.total()?;
        let weighted_sum = self.weighted_net_sum(true)?;
        let weighted_value = floor_div_positive(weighted_sum, 10_000)?;
        let lower = weighted_value.checked_sub(i128::from(uncertainty))?;
        i64::try_from(lower).ok()
    }

    pub fn expected_net_value(&self) -> Option<i64> {
        let weighted_sum = self.weighted_net_sum(true)?;
        i64::try_from(floor_div_positive(weighted_sum, 10_000)?).ok()
    }

    fn weighted_net_sum(&self, require_terminal: bool) -> Option<i128> {
        if self.scenarios.is_empty() {
            return None;
        }
        let total = self.scenarios.iter().try_fold(0_u32, |sum, scenario| {
            sum.checked_add(u32::from(scenario.weight_bps()))
        })?;
        if total != 10_000
            || self.scenarios.iter().any(|scenario| {
                scenario.weight_bps() == 0 || (require_terminal && !scenario.terminal())
            })
        {
            return None;
        }
        self.scenarios.iter().try_fold(0_i128, |sum, scenario| {
            let value = scenario.net_value()?;
            sum.checked_add(i128::from(scenario.weight_bps()) * value)
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ClosureEvidence {
    pub event_id: String,
    pub closed_at_ms: u64,
    pub observed_at_ms: u64,
    pub calendar_known: bool,
    pub source_digest: String,
}

impl ClosureEvidence {
    pub fn valid_for(&self, expected_closed_at_ms: u64, now_ms: u64) -> bool {
        !self.event_id.is_empty()
            && self.closed_at_ms == expected_closed_at_ms
            && self.closed_at_ms > 0
            && self.observed_at_ms >= self.closed_at_ms
            && self.observed_at_ms <= now_ms
            && self.calendar_known
            && !self.source_digest.is_empty()
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EvidenceFrame {
    pub now_ms: u64,
    pub episode: AnchorEpisode,
    pub contract: BinanceContract,
    pub market: MarketSnapshot,
    pub account: AccountSnapshot,
    pub portfolio: crate::portfolio::PortfolioSnapshot,
    pub entry_margin_quote_ticks: i128,
    pub entry_stress_quote_ticks: i128,
    pub closure: ClosureEvidence,
    pub model_version: String,
    pub calibration: crate::calibration::CalibrationSnapshot,
    pub buy_outcome: OutcomeDistribution,
    pub sell_outcome: OutcomeDistribution,
    pub wait_outcome: OutcomeDistribution,
}

impl EvidenceFrame {
    pub fn structurally_valid(&self) -> bool {
        self.now_ms > 0
            && self.entry_margin_quote_ticks >= 0
            && self.entry_stress_quote_ticks >= 0
            && !self.episode.id.is_empty()
            && self.contract.symbol == self.episode.anchor.instrument
            && self.calibration.instrument == self.contract.symbol
            && self
                .closure
                .valid_for(self.episode.window.closed_at_ms, self.now_ms)
    }

    pub fn complete_for_entry(&self) -> bool {
        self.structurally_valid()
            && self.calibration.phase == crate::calibration::CalibrationPhase::Admitted
            && self.episode.anchor.valid_at(self.now_ms)
            && self.episode.funding_valid_at(self.now_ms)
            && self.account.reconciled
            && self
                .account
                .validate_for(self.contract.position_mode, self.now_ms)
                .is_ok()
            && self.portfolio.validate_at(self.now_ms).is_ok()
            && self.market.validate(self.now_ms).is_ok()
            && !self.model_version.is_empty()
            && self.calibration.validate().is_ok()
            && self.wait_outcome.lower_value().is_some()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ModelError {
    InvalidAnchor,
    InvalidWindow,
    InvalidEpisode,
    InvalidFundingSchedule,
    InvalidMarket,
    StaleMarket,
    ContractUnavailable,
    PriceFilter,
    QuantityFilter,
    NotionalFilter,
    WouldTakeLiquidity,
    InvalidReduceOnly,
    PositionLimit,
    InvalidOrderSemantics,
    InvalidAccount,
    InsufficientMargin,
    RateLimit,
    IncompleteEvidence,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn fixtures() -> (BinanceContract, Book, AccountSnapshot, CandidateOrder) {
        (
            BinanceContract {
                symbol: "BTCUSDT".into(),
                status: ContractStatus::Trading,
                contract_type: "PERPETUAL".into(),
                price_filter: PriceFilter {
                    min: PriceTicks(1),
                    max: PriceTicks(1_000_000),
                    tick: 1,
                },
                quantity_filter: QuantityFilter {
                    min: Quantity(1),
                    max: Quantity(1_000),
                    step: 1,
                },
                min_notional_ticks: 1,
                max_notional_ticks: None,
                position_mode: PositionMode::OneWay,
                post_only_supported: true,
                reduce_only_supported: true,
                close_position_supported: true,
                conditional_orders_supported: false,
                trigger_protect_bps: 0,
                rate_limit_remaining: 10,
                max_open_orders: 100,
                open_orders: 0,
                self_trade_prevention_enabled: true,
                observed_at_ms: 1,
                max_age_ms: 100,
                digest: "contract-digest".into(),
            },
            Book {
                bid: PriceTicks(99),
                ask: PriceTicks(101),
                bid_quantity: Quantity(100),
                ask_quantity: Quantity(100),
                observed_at_ms: 1,
                sequence: 1,
            },
            AccountSnapshot {
                position: 0,
                long_position: 0,
                short_position: 0,
                max_position: 100,
                reconciled: true,
                margin_available_ppm: 1_000_000,
                observed_at_ms: 1,
                max_age_ms: 100,
                source_digest: "account-digest".into(),
            },
            CandidateOrder {
                symbol: "BTCUSDT".into(),
                side: Side::Buy,
                price: PriceTicks(99),
                quantity: Quantity(1),
                reduce_only: false,
                position_side: PositionSide::Both,
                time_in_force: TimeInForce::Gtx,
                working_type: WorkingType::ContractPrice,
                price_protect: false,
                trigger_price: None,
                close_position: false,
            },
        )
    }

    #[test]
    fn binance_order_semantics_are_hard_gates() {
        let (contract, book, account, order) = fixtures();
        assert!(contract.validate(order.clone(), book, &account, 1).is_ok());

        let mut non_post_only = order.clone();
        non_post_only.time_in_force = TimeInForce::Gtc;
        assert_eq!(
            contract.validate(non_post_only, book, &account, 1),
            Err(ModelError::InvalidOrderSemantics)
        );

        let mut wrong_position_side = order.clone();
        wrong_position_side.position_side = PositionSide::Long;
        assert_eq!(
            contract.validate(wrong_position_side, book, &account, 1),
            Err(ModelError::InvalidOrderSemantics)
        );

        let mut unsupported_trigger = order;
        unsupported_trigger.trigger_price = Some(PriceTicks(100));
        assert_eq!(
            contract.validate(unsupported_trigger, book, &account, 1),
            Err(ModelError::InvalidOrderSemantics)
        );
    }
    #[test]
    fn price_filter_uses_exchange_tick_origin() {
        let (mut contract, book, account, mut order) = fixtures();
        contract.price_filter = PriceFilter {
            min: PriceTicks(99),
            max: PriceTicks(1_000_000),
            tick: 10,
        };
        order.price = PriceTicks(99);
        assert_eq!(
            contract.validate(order.clone(), book, &account, 1),
            Err(ModelError::PriceFilter)
        );
        order.price = PriceTicks(100);
        assert!(contract.validate(order, book, &account, 1).is_ok());
    }

    #[test]
    fn entry_rejects_full_open_order_capacity_and_missing_stp() {
        let (mut contract, book, account, order) = fixtures();
        contract.open_orders = contract.max_open_orders;
        assert_eq!(
            contract.validate(order.clone(), book, &account, 1),
            Err(ModelError::ContractUnavailable)
        );
        contract.open_orders = 0;
        contract.self_trade_prevention_enabled = false;
        assert_eq!(
            contract.validate(order, book, &account, 1),
            Err(ModelError::ContractUnavailable)
        );
    }

    #[test]
    fn entry_rejects_notional_above_exchange_maximum() {
        let (mut contract, book, account, mut order) = fixtures();
        contract.max_notional_ticks = Some(99);
        order.quantity = Quantity(2);
        assert_eq!(
            contract.validate(order, book, &account, 1),
            Err(ModelError::NotionalFilter)
        );
    }

    #[test]
    fn entry_rejects_zero_available_margin() {
        let (contract, book, mut account, order) = fixtures();
        account.margin_available_ppm = 0;
        assert_eq!(
            contract.validate(order, book, &account, 1),
            Err(ModelError::InsufficientMargin)
        );
    }

    #[test]
    fn contract_rejects_a_stale_account_snapshot() {
        let (contract, book, mut account, order) = fixtures();
        account.observed_at_ms = 0;
        assert_eq!(
            contract.validate(order, book, &account, 1),
            Err(ModelError::InvalidAccount)
        );
    }

    #[test]
    fn contract_rejects_an_unreconciled_account() {
        let (contract, book, mut account, order) = fixtures();
        account.reconciled = false;
        assert_eq!(
            contract.validate(order, book, &account, 1),
            Err(ModelError::IncompleteEvidence)
        );
    }
    #[test]
    fn book_rejects_locked_or_crossed_quotes() {
        let book = Book {
            bid: PriceTicks(100),
            ask: PriceTicks(100),
            bid_quantity: Quantity(10),
            ask_quantity: Quantity(10),
            observed_at_ms: 1,
            sequence: 1,
        };
        assert_eq!(book.validate(), Err(ModelError::InvalidMarket));
    }

    #[test]
    fn account_position_representation_must_match_contract_mode() {
        let (mut contract, book, mut account, order) = fixtures();
        account.long_position = 1;
        assert_eq!(
            contract.validate(order.clone(), book, &account, 1),
            Err(ModelError::InvalidAccount)
        );

        contract.position_mode = PositionMode::Hedge;
        account.long_position = 0;
        account.position = 1;
        assert_eq!(
            contract.validate(order, book, &account, 1),
            Err(ModelError::InvalidAccount)
        );
    }

    #[test]
    fn hedge_mode_tracks_legs_and_rejects_ambiguous_emergency_flatten() {
        let (mut contract, book, mut account, mut order) = fixtures();
        contract.position_mode = PositionMode::Hedge;
        account.position = 0;
        account.long_position = 3;
        account.short_position = 2;
        order.side = Side::Sell;
        order.price = PriceTicks(101);
        order.quantity = Quantity(1);
        order.reduce_only = false;
        order.position_side = PositionSide::Long;

        let validated = contract
            .validate(order.clone(), book, &account, 1)
            .expect("single hedge-leg reduction is valid");
        assert_eq!(validated.route, OrderRoute::PassiveReduceOnly);

        let emergency = CandidateOrder {
            time_in_force: TimeInForce::Ioc,
            price: book.ask,
            ..order
        };
        assert_eq!(
            contract.validate_emergency_reduce_only(emergency, book, &account, 1),
            Err(ModelError::InvalidReduceOnly)
        );
    }
}

#[cfg(test)]
mod outcome_tests {
    use super::*;

    fn cycle(
        weight_bps: u16,
        entry_price: i64,
        exit_price: i64,
        entry_fee_pico_bps: i64,
        entry_filled_quantity: i64,
        exit_filled_quantity: i64,
    ) -> OutcomeScenario {
        OutcomeScenario::cycle(
            weight_bps,
            ExecutionCycle {
                side: Side::Buy,
                anchor_price: PriceTicks(1_000_000_000_000),
                entry_price: PriceTicks(entry_price),
                exit_price: PriceTicks(exit_price),
                requested_quantity: Quantity(1),
                entry_filled_quantity,
                exit_filled_quantity,
                entry_queue_ahead_quantity: 0,
                exit_queue_ahead_quantity: 0,
                entry_traded_through_quantity: entry_filled_quantity,
                exit_traded_through_quantity: exit_filled_quantity,
                entry_latency_ms: 0,
                exit_latency_ms: 0,
                entry_fee_pico_bps,
                exit_fee_pico_bps: 0,
                exit_cost_pico_bps: 0,
                funding_cost_pico_bps: 0,
                deadline_risk_pico_bps: 0,
                entry_at_ms: 1,
                exit_at_ms: 2,
                deadline_ms: 3,
            },
        )
    }

    #[test]
    fn execution_cycle_rejects_fill_without_causal_queue_throughput() {
        let scenario = OutcomeScenario::cycle(
            10_000,
            ExecutionCycle {
                side: Side::Buy,
                anchor_price: PriceTicks(1_000),
                entry_price: PriceTicks(990),
                exit_price: PriceTicks(1_000),
                requested_quantity: Quantity(1),
                entry_filled_quantity: 1,
                exit_filled_quantity: 1,
                entry_queue_ahead_quantity: 100,
                exit_queue_ahead_quantity: 100,
                entry_traded_through_quantity: 100,
                exit_traded_through_quantity: 100,
                entry_latency_ms: 10,
                exit_latency_ms: 10,
                entry_fee_pico_bps: 0,
                exit_fee_pico_bps: 0,
                exit_cost_pico_bps: 0,
                funding_cost_pico_bps: 0,
                deadline_risk_pico_bps: 0,
                entry_at_ms: 1,
                exit_at_ms: 2,
                deadline_ms: 3,
            },
        );
        assert_eq!(scenario.net_value(), None);
    }

    #[test]
    fn queue_fill_probability_uses_only_observed_throughput() {
        let estimate = QueueFillEstimate {
            queue_ahead_quantity: 100,
            order_quantity: 10,
            traded_through_quantity: 105,
            observed_latency_ms: 50,
        };
        assert_eq!(estimate.fill_probability_bps(), Some(5_000));
        assert_eq!(
            QueueFillEstimate {
                queue_ahead_quantity: -1,
                order_quantity: 10,
                traded_through_quantity: 10,
                observed_latency_ms: 0,
            }
            .fill_probability_bps(),
            None
        );
    }

    #[test]
    fn floor_division_is_a_true_lower_bound_for_negative_values() {
        assert_eq!(floor_div_positive(-1, 10_000), Some(-1));
        assert_eq!(floor_div_positive(-10_000, 10_000), Some(-1));
        assert_eq!(floor_div_positive(10_001, 10_000), Some(1));
        assert_eq!(floor_div_positive(1, 0), None);
    }

    #[test]
    fn uncertainty_budget_is_decomposable_and_rejects_negative() {
        let budget = UncertaintyBudget {
            anchor_pico_bps: 2,
            execution_pico_bps: 3,
            timing_pico_bps: 5,
            model_pico_bps: 7,
        };
        assert_eq!(budget.total(), Some(17));
        assert_eq!(
            UncertaintyBudget {
                anchor_pico_bps: -1,
                execution_pico_bps: 0,
                timing_pico_bps: 0,
                model_pico_bps: 0,
            }
            .total(),
            None
        );
    }

    #[test]
    fn lower_value_uses_all_probabilities_and_costs() {
        let distribution = OutcomeDistribution {
            scenarios: vec![
                cycle(5_000, 1_000_000_000_000, 1_000_000_000_100, 10, 1, 1),
                cycle(5_000, 1_000_000_000_000, 1_000_000_000_000, 0, 1, 1),
            ],
            uncertainty: UncertaintyBudget {
                anchor_pico_bps: 2,
                execution_pico_bps: 3,
                timing_pico_bps: 1,
                model_pico_bps: 4,
            },
        };
        assert_eq!(distribution.expected_net_value(), Some(45));
        assert_eq!(distribution.lower_value(), Some(35));
    }

    #[test]
    fn incomplete_path_is_not_silently_zero() {
        let distribution = OutcomeDistribution {
            scenarios: vec![cycle(10_000, 1_000_000_000_000, 1_000_000_000_100, 0, 1, 0)],
            uncertainty: UncertaintyBudget {
                anchor_pico_bps: 0,
                execution_pico_bps: 0,
                timing_pico_bps: 0,
                model_pico_bps: 0,
            },
        };
        assert_eq!(distribution.lower_value(), None);
        assert_eq!(distribution.expected_net_value(), None);
    }

    #[test]
    fn anchor_residual_is_signed_against_fair_price() {
        let anchor = Anchor::new("a", "equity", PriceTicks(110), 1, 10, "digest").unwrap();
        assert_eq!(
            anchor.residual_pico_bps(PriceTicks(100)),
            Some(90_909_090_909)
        );
        assert_eq!(
            anchor.residual_pico_bps(PriceTicks(120)),
            Some(-90_909_090_910)
        );
    }

    #[test]
    fn market_rejects_a_stale_mark_even_when_book_is_fresh() {
        let market = MarketSnapshot {
            book: Book {
                bid: PriceTicks(99),
                ask: PriceTicks(101),
                bid_quantity: Quantity(10),
                ask_quantity: Quantity(10),
                observed_at_ms: 100,
                sequence: 1,
            },
            index: PriceTicks(100),
            index_observed_at_ms: 100,
            index_max_age_ms: 10,
            mark: PriceTicks(100),
            mark_observed_at_ms: 1,
            mark_max_age_ms: 10,
            server_time_ms: 100,
            max_age_ms: 10,
            source_digest: "market-digest".into(),
        };
        assert_eq!(market.validate(100), Err(ModelError::StaleMarket));
    }

    #[test]
    fn market_rejects_a_stale_book_even_when_server_time_is_fresh() {
        let market = MarketSnapshot {
            book: Book {
                bid: PriceTicks(99),
                ask: PriceTicks(101),
                bid_quantity: Quantity(10),
                ask_quantity: Quantity(10),
                observed_at_ms: 1,
                sequence: 1,
            },
            index: PriceTicks(100),
            index_observed_at_ms: 100,
            index_max_age_ms: 10,
            mark: PriceTicks(100),
            mark_observed_at_ms: 100,
            mark_max_age_ms: 10,
            server_time_ms: 100,
            max_age_ms: 10,
            source_digest: "market-digest".into(),
        };
        assert_eq!(market.validate(100), Err(ModelError::StaleMarket));
    }
}
