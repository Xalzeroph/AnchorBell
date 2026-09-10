//! Cost-aware, volatility-aware maker admission policy.
use super::{
    risk_contracts::{ConditionalOrderValue, ConfidenceInterval},
    PriceTicks,
};
use crate::execution::{OrderIntent, Side};

const MICRO_BPS_SCALE: i128 = 1_000_000;
/// One pico-bps is 1e-12 bps. With the engine's 1e-8 price tick this preserves
/// every representable one-tick relative move across the supported price range.
const PICO_BPS_SCALE: i128 = 1_000_000_000_000;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AdaptiveThreshold {
    /// Rounded display values retained for human-readable reports and the
    /// existing configuration surface. Decisions use the exact pico fields.
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
    /// Tail-risk surcharge used by the M5 robust challenger. This is zero
    /// for M1-M4 and is intentionally additive to the auditable hurdle.
    pub tail_risk_bps: i64,
    exact_pico_bps: [i64; 12],
}

impl AdaptiveThreshold {
    fn bps_to_pico(value: i64) -> i64 {
        i128::from(value)
            .saturating_mul(PICO_BPS_SCALE)
            .clamp(i128::from(i64::MIN), i128::from(i64::MAX)) as i64
    }

    fn pico_to_bps(value: i64) -> i64 {
        if value <= 0 {
            return 0;
        }
        ((i128::from(value) + PICO_BPS_SCALE / 2) / PICO_BPS_SCALE).clamp(0, i128::from(i64::MAX))
            as i64
    }

    #[allow(clippy::too_many_arguments)]
    pub fn from_pico_components(
        floor_pico_bps: i64,
        residual_volatility_pico_bps: i64,
        cost_pico_bps: i64,
        uncertainty_pico_bps: i64,
        deadline_risk_pico_bps: i64,
        safety_margin_pico_bps: i64,
        spread_pico_bps: i64,
        adverse_selection_pico_bps: i64,
        liquidity_pico_bps: i64,
        inventory_pico_bps: i64,
        statistical_pico_bps: i64,
        tail_risk_pico_bps: i64,
    ) -> Option<Self> {
        let exact_pico_bps = [
            floor_pico_bps,
            residual_volatility_pico_bps,
            cost_pico_bps,
            uncertainty_pico_bps,
            deadline_risk_pico_bps,
            safety_margin_pico_bps,
            spread_pico_bps,
            adverse_selection_pico_bps,
            liquidity_pico_bps,
            inventory_pico_bps,
            statistical_pico_bps,
            tail_risk_pico_bps,
        ];
        if exact_pico_bps.iter().any(|value| *value < 0) {
            return None;
        }
        Some(Self {
            floor_bps: Self::pico_to_bps(floor_pico_bps),
            residual_volatility_bps: Self::pico_to_bps(residual_volatility_pico_bps),
            cost_bps: Self::pico_to_bps(cost_pico_bps),
            uncertainty_bps: Self::pico_to_bps(uncertainty_pico_bps),
            deadline_risk_bps: Self::pico_to_bps(deadline_risk_pico_bps),
            safety_margin_bps: Self::pico_to_bps(safety_margin_pico_bps),
            spread_bps: Self::pico_to_bps(spread_pico_bps),
            adverse_selection_bps: Self::pico_to_bps(adverse_selection_pico_bps),
            liquidity_bps: Self::pico_to_bps(liquidity_pico_bps),
            inventory_bps: Self::pico_to_bps(inventory_pico_bps),
            statistical_bps: Self::pico_to_bps(statistical_pico_bps),
            tail_risk_bps: Self::pico_to_bps(tail_risk_pico_bps),
            exact_pico_bps,
        })
    }

    pub fn components_pico_bps(self) -> [i64; 12] {
        self.exact_pico_bps
    }

    /// Builds the documented adaptive hurdle. Additive terms represent
    /// independently paid risks; statistical_bps is an alternative empirical
    /// hurdle and therefore competes with the sum via max().
    // The named constructor mirrors the independently auditable hurdle components.
    #[allow(clippy::too_many_arguments)]
    pub fn from_components(
        floor_bps: i64,
        residual_volatility_bps: i64,
        cost_bps: i64,
        uncertainty_bps: i64,
        deadline_risk_bps: i64,
        safety_margin_bps: i64,
        spread_bps: i64,
        adverse_selection_bps: i64,
        liquidity_bps: i64,
        inventory_bps: i64,
        statistical_bps: i64,
        tail_risk_bps: i64,
    ) -> Option<Self> {
        Self::from_pico_components(
            Self::bps_to_pico(floor_bps),
            Self::bps_to_pico(residual_volatility_bps),
            Self::bps_to_pico(cost_bps),
            Self::bps_to_pico(uncertainty_bps),
            Self::bps_to_pico(deadline_risk_bps),
            Self::bps_to_pico(safety_margin_bps),
            Self::bps_to_pico(spread_bps),
            Self::bps_to_pico(adverse_selection_bps),
            Self::bps_to_pico(liquidity_bps),
            Self::bps_to_pico(inventory_bps),
            Self::bps_to_pico(statistical_bps),
            Self::bps_to_pico(tail_risk_bps),
        )
    }

    pub fn with_adverse_selection(self, extra_bps: i64) -> Option<Self> {
        if extra_bps < 0 {
            return None;
        }
        self.with_adverse_selection_pico(Self::bps_to_pico(extra_bps))
    }

    pub fn with_adverse_selection_pico(self, extra_pico_bps: i64) -> Option<Self> {
        if extra_pico_bps < 0 || self.required_pico_bps().is_none() {
            return None;
        }
        let mut values = self.exact_pico_bps;
        values[7] = i128::from(values[7])
            .checked_add(i128::from(extra_pico_bps))?
            .clamp(0, i128::from(i64::MAX)) as i64;
        Self::from_pico_components(
            values[0], values[1], values[2], values[3], values[4], values[5], values[6], values[7],
            values[8], values[9], values[10], values[11],
        )
    }

    pub fn required_pico_bps(self) -> Option<i64> {
        if self.exact_pico_bps.iter().any(|value| *value < 0)
            || [
                self.floor_bps,
                self.residual_volatility_bps,
                self.cost_bps,
                self.uncertainty_bps,
                self.deadline_risk_bps,
                self.safety_margin_bps,
                self.spread_bps,
                self.adverse_selection_bps,
                self.liquidity_bps,
                self.inventory_bps,
                self.statistical_bps,
                self.tail_risk_bps,
            ]
            .iter()
            .any(|value| *value < 0)
        {
            return None;
        }
        let additive = self
            .exact_pico_bps
            .iter()
            .enumerate()
            .filter(|(index, _)| *index != 10)
            .fold(0_i128, |total, (_, value)| {
                total.saturating_add(i128::from(*value))
            });
        Some(
            additive
                .max(i128::from(self.exact_pico_bps[10]))
                .clamp(0, i128::from(i64::MAX)) as i64,
        )
    }

    /// Compatibility view only; admission logic uses required_pico_bps.
    pub fn required_micro_bps(self) -> Option<i64> {
        self.required_pico_bps().map(|value| {
            ((i128::from(value) + MICRO_BPS_SCALE / 2) / MICRO_BPS_SCALE)
                .clamp(0, i128::from(i64::MAX)) as i64
        })
    }

    pub fn required_bps(self) -> Option<i64> {
        self.required_pico_bps().map(Self::pico_to_bps)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SignalInput {
    pub symbol: u32,
    pub anchor: PriceTicks,
    pub best_bid: PriceTicks,
    pub best_ask: PriceTicks,
    pub index_price: PriceTicks,
    pub mark_price: PriceTicks,
    pub position: i64,
    pub max_position: i64,
    pub requested_quantity: i64,
    pub threshold: AdaptiveThreshold,
    /// Maximum extra hurdle, in bps, applied to the risk-increasing side
    /// at a full position. The reducing side receives no inventory surcharge.
    pub inventory_skew_bps: i64,
    /// Exact forms used by admission. The integer fields above are display and
    /// legacy inputs only.
    pub inventory_skew_pico_bps: i64,
    /// Directional microstructure penalty. A buy is penalized when the ask
    /// queue is thinner (downward pressure), and a sell when the bid queue is
    /// thinner (upward pressure).
    pub buy_adverse_selection_bps: i64,
    pub sell_adverse_selection_bps: i64,
    pub buy_adverse_selection_pico_bps: i64,
    pub sell_adverse_selection_pico_bps: i64,
    /// Direction-specific queue-survival estimates. A shared minimum lets a
    /// congested queue on one side erase an otherwise valid opportunity on the
    /// other side.
    pub buy_fill_probability_bps: u16,
    pub sell_fill_probability_bps: u16,
    pub fill_probability_bps: u16,
    pub confidence_bps: u16,
    /// Enables the conditional-value gate for fill-aware challengers.
    /// Core price/risk admission remains active for every variant.
    pub fill_aware: bool,
    /// Compatibility relief field in micro-bps.
    pub threshold_relief_micro_bps: i64,
    /// Exact relief field in pico-bps.
    pub threshold_relief_pico_bps: i64,
    pub max_mark_index_gap_bps: i64,
    pub signal_age_ms: u64,
    pub max_signal_age_ms: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SignalBlockReason {
    InvalidPrices,
    InvalidPositionLimit,
    StaleSignal,
    MarkIndexDisagreement,
    ThresholdUnavailable,
    NoEdge,
    PositionLimit,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SignalDecision {
    BuyMaker { price: PriceTicks, quantity: i64 },
    SellMaker { price: PriceTicks, quantity: i64 },
    Blocked(SignalBlockReason),
}

pub fn decide(input: SignalInput) -> SignalDecision {
    if input.anchor.0 <= 0
        || input.best_bid.0 <= 0
        || input.best_ask.0 < input.best_bid.0
        || input.index_price.0 <= 0
        || input.mark_price.0 <= 0
    {
        return SignalDecision::Blocked(SignalBlockReason::InvalidPrices);
    }
    if input.max_position <= 0 || input.requested_quantity <= 0 {
        return SignalDecision::Blocked(SignalBlockReason::InvalidPositionLimit);
    }
    if input.signal_age_ms > input.max_signal_age_ms {
        return SignalDecision::Blocked(SignalBlockReason::StaleSignal);
    }
    if input.max_mark_index_gap_bps < 0 {
        return SignalDecision::Blocked(SignalBlockReason::MarkIndexDisagreement);
    }
    let mark_index_gap_numerator =
        (i128::from(input.mark_price.0) - i128::from(input.index_price.0)).abs() * 10_000;
    let mark_index_limit_numerator =
        i128::from(input.max_mark_index_gap_bps) * i128::from(input.index_price.0);
    if mark_index_gap_numerator > mark_index_limit_numerator {
        return SignalDecision::Blocked(SignalBlockReason::MarkIndexDisagreement);
    }
    let buy_extra_pico_bps = if input.buy_adverse_selection_pico_bps != 0 {
        input.buy_adverse_selection_pico_bps
    } else {
        AdaptiveThreshold::bps_to_pico(input.buy_adverse_selection_bps)
    };
    let sell_extra_pico_bps = if input.sell_adverse_selection_pico_bps != 0 {
        input.sell_adverse_selection_pico_bps
    } else {
        AdaptiveThreshold::bps_to_pico(input.sell_adverse_selection_bps)
    };
    let buy_threshold = match input
        .threshold
        .with_adverse_selection_pico(buy_extra_pico_bps)
    {
        Some(value) => value,
        None => return SignalDecision::Blocked(SignalBlockReason::ThresholdUnavailable),
    };
    let sell_threshold = match input
        .threshold
        .with_adverse_selection_pico(sell_extra_pico_bps)
    {
        Some(value) => value,
        None => return SignalDecision::Blocked(SignalBlockReason::ThresholdUnavailable),
    };
    let buy_required_pico_bps = match buy_threshold.required_pico_bps() {
        Some(value) => value,
        None => return SignalDecision::Blocked(SignalBlockReason::ThresholdUnavailable),
    };
    let sell_required_pico_bps = match sell_threshold.required_pico_bps() {
        Some(value) => value,
        None => return SignalDecision::Blocked(SignalBlockReason::ThresholdUnavailable),
    };
    let inventory_skew_pico_bps = if input.inventory_skew_pico_bps != 0 {
        input.inventory_skew_pico_bps
    } else {
        AdaptiveThreshold::bps_to_pico(input.inventory_skew_bps)
    };
    let side_specific_fill =
        input.buy_fill_probability_bps != 0 || input.sell_fill_probability_bps != 0;
    let buy_fill_probability_bps = if side_specific_fill {
        input.buy_fill_probability_bps
    } else {
        input.fill_probability_bps
    };
    let sell_fill_probability_bps = if side_specific_fill {
        input.sell_fill_probability_bps
    } else {
        input.fill_probability_bps
    };
    if inventory_skew_pico_bps < 0
        || (buy_fill_probability_bps == 0 && sell_fill_probability_bps == 0)
        || input.confidence_bps == 0
    {
        return SignalDecision::Blocked(SignalBlockReason::ThresholdUnavailable);
    }
    let inventory_ratio_bps =
        (i128::from(input.position).abs() * 10_000 / i128::from(input.max_position)).min(10_000);
    let inventory_surcharge_pico_bps =
        inventory_ratio_bps.saturating_mul(i128::from(inventory_skew_pico_bps)) / 10_000;
    let relief_pico_bps = if input.threshold_relief_pico_bps != 0 {
        i128::from(input.threshold_relief_pico_bps.max(0))
    } else {
        i128::from(input.threshold_relief_micro_bps.max(0)) * (PICO_BPS_SCALE / MICRO_BPS_SCALE)
    };
    let buy_required_with_inventory = i128::from(buy_required_pico_bps)
        .saturating_sub(relief_pico_bps)
        + if input.position > 0 {
            inventory_surcharge_pico_bps
        } else {
            0
        };
    let sell_required_with_inventory = i128::from(sell_required_pico_bps)
        .saturating_sub(relief_pico_bps)
        + if input.position < 0 {
            inventory_surcharge_pico_bps
        } else {
            0
        };
    let buy_edge_numerator = (i128::from(input.anchor.0) - i128::from(input.best_bid.0)).max(0)
        * 10_000
        * PICO_BPS_SCALE;
    let sell_edge_numerator = (i128::from(input.best_ask.0) - i128::from(input.anchor.0)).max(0)
        * 10_000
        * PICO_BPS_SCALE;
    let buy_threshold_numerator = buy_required_with_inventory * i128::from(input.anchor.0);
    let sell_threshold_numerator = sell_required_with_inventory * i128::from(input.anchor.0);
    let quantity = input.requested_quantity.min(input.max_position);
    if quantity <= 0 {
        return SignalDecision::Blocked(SignalBlockReason::PositionLimit);
    }
    if buy_edge_numerator >= buy_threshold_numerator
        && (!input.fill_aware
            || conditionally_admissible(
                buy_edge_numerator,
                input.anchor.0,
                buy_threshold,
                buy_fill_probability_bps,
                input.confidence_bps,
            ))
    {
        let remaining = (i128::from(input.max_position) - i128::from(input.position)).max(0);
        let capped_quantity = i128::from(quantity).min(remaining);
        if capped_quantity > 0 {
            return SignalDecision::BuyMaker {
                price: input.best_bid,
                quantity: capped_quantity as i64,
            };
        }
        return SignalDecision::Blocked(SignalBlockReason::PositionLimit);
    }
    if sell_edge_numerator >= sell_threshold_numerator
        && (!input.fill_aware
            || conditionally_admissible(
                sell_edge_numerator,
                input.anchor.0,
                sell_threshold,
                sell_fill_probability_bps,
                input.confidence_bps,
            ))
    {
        let remaining = (i128::from(input.max_position) + i128::from(input.position)).max(0);
        let capped_quantity = i128::from(quantity).min(remaining);
        if capped_quantity > 0 {
            return SignalDecision::SellMaker {
                price: input.best_ask,
                quantity: capped_quantity as i64,
            };
        }
        return SignalDecision::Blocked(SignalBlockReason::PositionLimit);
    }
    SignalDecision::Blocked(SignalBlockReason::NoEdge)
}

#[allow(clippy::too_many_arguments)]
pub fn adaptive_intent_from_market(
    symbol: u32,
    bid: PriceTicks,
    bid_quantity: i64,
    ask: PriceTicks,
    ask_quantity: i64,
    anchor: PriceTicks,
    index: PriceTicks,
    mark: PriceTicks,
    position: i64,
    max_position: i64,
    requested_quantity: i64,
    // Rolling absolute-return EWMA supplied by the caller. This keeps
    // live/testnet and simulation on the same adaptive contract.
    volatility_bps: i64,
    floor_bps: i64,
    fee_bps: i64,
    max_mark_index_gap_bps: i64,
    signal_age_ms: u64,
    max_signal_age_ms: u64,
) -> Option<OrderIntent> {
    if bid.0 <= 0
        || ask.0 < bid.0
        || bid_quantity <= 0
        || ask_quantity <= 0
        || anchor.0 <= 0
        || index.0 <= 0
        || mark.0 <= 0
    {
        return None;
    }

    let gap_bps = (((i128::from(mark.0) - i128::from(index.0)).abs() * 10_000)
        / i128::from(index.0))
    .clamp(0, i128::from(i64::MAX)) as i64;
    let midpoint = i128::from(bid.0) + (i128::from(ask.0) - i128::from(bid.0)) / 2;
    let spread_bps = if midpoint > 0 {
        ((i128::from(ask.0) - i128::from(bid.0)) * 10_000 / midpoint / 2)
            .clamp(0, i128::from(i64::MAX)) as i64
    } else {
        i64::MAX
    };
    let depth = bid_quantity.min(ask_quantity);
    let participation_bps = if requested_quantity <= 0 {
        10_000
    } else {
        (i128::from(requested_quantity).max(0) * 10_000 / i128::from(depth)).clamp(0, 10_000) as i64
    };
    let liquidity_bps = if requested_quantity <= 0 {
        i64::MAX
    } else if participation_bps <= 1_000 {
        0
    } else {
        ((participation_bps - 1_000) * 6 / 1_000).clamp(0, 100)
    };
    let fill_probability_bps = if requested_quantity <= 0 {
        0
    } else {
        (10_000_i64 - participation_bps * 8 / 10).clamp(500, 9_500) as u16
    };
    let effective_quantity = i128::from(requested_quantity)
        .min(i128::from(depth))
        .clamp(0, i128::from(i64::MAX)) as i64;
    let confidence_bps = if signal_age_ms <= 1_000 { 9_000 } else { 7_000 };
    let volatility_bps = volatility_bps.max(0);
    let (buy_micro_adverse_bps, sell_micro_adverse_bps) =
        side_adverse_selection_bps(bid_quantity, ask_quantity);
    let threshold = AdaptiveThreshold::from_components(
        floor_bps,
        volatility_bps.saturating_mul(3),
        fee_bps,
        gap_bps / 2 + 5,
        0,
        5,
        spread_bps,
        volatility_bps.saturating_mul(2),
        liquidity_bps,
        0,
        volatility_bps.saturating_mul(8),
        0,
    )?;
    decide(SignalInput {
        symbol,
        anchor,
        best_bid: bid,
        best_ask: ask,
        index_price: index,
        mark_price: mark,
        position,
        max_position,
        requested_quantity: effective_quantity,
        threshold,
        inventory_skew_bps: 50,
        buy_adverse_selection_bps: buy_micro_adverse_bps,
        sell_adverse_selection_bps: sell_micro_adverse_bps,
        fill_probability_bps,
        confidence_bps,
        fill_aware: true,
        threshold_relief_micro_bps: 0,
        threshold_relief_pico_bps: 0,
        inventory_skew_pico_bps: 0,
        buy_adverse_selection_pico_bps: buy_micro_adverse_bps
            .saturating_mul((PICO_BPS_SCALE / MICRO_BPS_SCALE) as i64),
        sell_adverse_selection_pico_bps: sell_micro_adverse_bps
            .saturating_mul((PICO_BPS_SCALE / MICRO_BPS_SCALE) as i64),
        buy_fill_probability_bps: fill_probability_bps,
        sell_fill_probability_bps: fill_probability_bps,
        max_mark_index_gap_bps,
        signal_age_ms,
        max_signal_age_ms,
    })
    .into_intent(symbol)
}

/// Converts top-of-book imbalance into a directional adverse-selection
/// surcharge. The exact calculation is retained in pico-bps.
pub fn side_adverse_selection_pico_bps(bid_quantity: i64, ask_quantity: i64) -> (i64, i64) {
    if bid_quantity <= 0 || ask_quantity <= 0 {
        return (i64::MAX, i64::MAX);
    }
    let total = i128::from(bid_quantity) + i128::from(ask_quantity);
    if total <= 0 {
        return (i64::MAX, i64::MAX);
    }
    let imbalance_pico_bps =
        ((i128::from(bid_quantity) - i128::from(ask_quantity)) * 10_000 * PICO_BPS_SCALE / total)
            .clamp(-10_000 * PICO_BPS_SCALE, 10_000 * PICO_BPS_SCALE);
    let buy = (-imbalance_pico_bps).max(0) * 25 / 10_000;
    let sell = imbalance_pico_bps.max(0) * 25 / 10_000;
    (
        buy.clamp(0, i128::from(i64::MAX)) as i64,
        sell.clamp(0, i128::from(i64::MAX)) as i64,
    )
}

pub fn side_adverse_selection_bps(bid_quantity: i64, ask_quantity: i64) -> (i64, i64) {
    let (buy, sell) = side_adverse_selection_pico_bps(bid_quantity, ask_quantity);
    let round = |value: i64| {
        ((i128::from(value.max(0)) + PICO_BPS_SCALE / 2) / PICO_BPS_SCALE)
            .clamp(0, i128::from(i64::MAX)) as i64
    };
    (round(buy), round(sell))
}

fn conditionally_admissible(
    edge_numerator: i128,
    anchor: i64,
    threshold: AdaptiveThreshold,
    fill_probability_bps: u16,
    confidence_bps: u16,
) -> bool {
    if anchor <= 0 {
        return false;
    }
    let edge_ppm = (edge_numerator * 100 / (i128::from(anchor) * PICO_BPS_SCALE))
        .clamp(0, i128::from(i64::MAX)) as i64;
    // The threshold already prices spread, adverse selection, liquidity, and
    // uncertainty. The conditional-value gate must not subtract those terms a
    // second time; it only checks direct cash costs and explicit penalties.
    let components = threshold.components_pico_bps();
    let to_ppm = |pico_bps: i64| {
        (i128::from(pico_bps) * 100 / PICO_BPS_SCALE).clamp(0, i128::from(i64::MAX)) as i64
    };
    let inventory_ppm = to_ppm(components[9]);
    let deadline_ppm = to_ppm(components[4]);
    let cost_ppm = to_ppm(components[2]);
    let Some(gross_edge) = ConfidenceInterval::new(edge_ppm, edge_ppm, edge_ppm, 1, confidence_bps)
    else {
        return false;
    };
    let Some(value) = ConditionalOrderValue::new(
        gross_edge,
        fill_probability_bps,
        confidence_bps,
        cost_ppm,
        inventory_ppm,
        deadline_ppm,
    ) else {
        return false;
    };
    value.is_admissible(0)
}

impl SignalDecision {
    pub fn into_intent(self, symbol: u32) -> Option<OrderIntent> {
        match self {
            SignalDecision::BuyMaker { price, quantity } => Some(OrderIntent {
                symbol,
                side: Side::Buy,
                price: price.0,
                quantity,
                post_only: true,
                reduce_only: false,
            }),
            SignalDecision::SellMaker { price, quantity } => Some(OrderIntent {
                symbol,
                side: Side::Sell,
                price: price.0,
                quantity,
                post_only: true,
                reduce_only: false,
            }),
            SignalDecision::Blocked(_) => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn input() -> SignalInput {
        SignalInput {
            symbol: 7,
            anchor: PriceTicks(100_000),
            best_bid: PriceTicks(98_000),
            best_ask: PriceTicks(98_100),
            index_price: PriceTicks(98_050),
            mark_price: PriceTicks(98_050),
            position: 0,
            max_position: 1_000,
            requested_quantity: 100,
            threshold: AdaptiveThreshold {
                floor_bps: 50,
                residual_volatility_bps: 25,
                cost_bps: 10,
                uncertainty_bps: 10,
                deadline_risk_bps: 0,
                safety_margin_bps: 10,
                spread_bps: 0,
                adverse_selection_bps: 0,
                liquidity_bps: 0,
                inventory_bps: 0,
                statistical_bps: 0,
                tail_risk_bps: 0,
                exact_pico_bps: [
                    50 * PICO_BPS_SCALE as i64,
                    25 * PICO_BPS_SCALE as i64,
                    10 * PICO_BPS_SCALE as i64,
                    10 * PICO_BPS_SCALE as i64,
                    0,
                    10 * PICO_BPS_SCALE as i64,
                    0,
                    0,
                    0,
                    0,
                    0,
                    0,
                ],
            },
            inventory_skew_bps: 0,
            buy_adverse_selection_bps: 0,
            sell_adverse_selection_bps: 0,
            fill_probability_bps: 10_000,
            confidence_bps: 10_000,
            fill_aware: true,
            threshold_relief_micro_bps: 0,
            threshold_relief_pico_bps: 0,
            inventory_skew_pico_bps: 0,
            buy_adverse_selection_pico_bps: 0,
            sell_adverse_selection_pico_bps: 0,
            buy_fill_probability_bps: 10_000,
            sell_fill_probability_bps: 10_000,
            max_mark_index_gap_bps: 20,
            signal_age_ms: 10,
            max_signal_age_ms: 100,
        }
    }

    #[test]
    fn requires_edge_above_all_dynamic_components() {
        assert_eq!(
            decide(input()),
            SignalDecision::BuyMaker {
                price: PriceTicks(98_000),
                quantity: 100
            }
        );
    }

    #[test]
    fn blocks_when_mark_and_index_disagree() {
        let mut value = input();
        value.mark_price = PriceTicks(99_000);
        assert_eq!(
            decide(value),
            SignalDecision::Blocked(SignalBlockReason::MarkIndexDisagreement)
        );
    }

    #[test]
    fn applies_directional_adverse_selection_penalty() {
        let mut value = input();
        value.best_bid = PriceTicks(98_950);
        value.best_ask = PriceTicks(101_050);
        value.buy_adverse_selection_bps = 24;
        value.sell_adverse_selection_bps = 0;
        assert_eq!(
            decide(value),
            SignalDecision::SellMaker {
                price: PriceTicks(101_050),
                quantity: 100
            }
        );
    }

    #[test]
    fn side_specific_fill_probability_blocks_only_the_unfillable_side() {
        let mut value = input();
        value.buy_fill_probability_bps = 0;
        value.sell_fill_probability_bps = 10_000;
        assert_eq!(
            decide(value),
            SignalDecision::Blocked(SignalBlockReason::NoEdge)
        );
    }

    #[test]
    fn caps_both_sides_by_remaining_position() {
        let mut value = input();
        value.position = 950;
        assert_eq!(
            decide(value),
            SignalDecision::BuyMaker {
                price: PriceTicks(98_000),
                quantity: 50
            }
        );
        value.position = -950;
        value.best_bid = PriceTicks(102_000);
        value.best_ask = PriceTicks(102_100);
        assert_eq!(
            decide(value),
            SignalDecision::SellMaker {
                price: PriceTicks(102_100),
                quantity: 50
            }
        );
    }

    #[test]
    fn extreme_prices_are_evaluated_without_overflow() {
        let mut value = input();
        value.anchor = PriceTicks(i64::MAX);
        value.best_bid = PriceTicks(i64::MAX - 100);
        value.best_ask = PriceTicks(i64::MAX);
        value.index_price = PriceTicks(i64::MAX);
        value.mark_price = PriceTicks(i64::MAX);
        assert_eq!(
            decide(value),
            SignalDecision::Blocked(SignalBlockReason::NoEdge)
        );
    }

    #[test]
    fn stale_and_invalid_inputs_fail_closed() {
        let mut value = input();
        value.signal_age_ms = 101;
        assert_eq!(
            decide(value),
            SignalDecision::Blocked(SignalBlockReason::StaleSignal)
        );
        value = input();
        value.threshold.cost_bps = -1;
        assert_eq!(
            decide(value),
            SignalDecision::Blocked(SignalBlockReason::ThresholdUnavailable)
        );
    }
}
