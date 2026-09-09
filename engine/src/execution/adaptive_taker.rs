use serde::{Deserialize, Serialize};

use super::Side;

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub struct EmergencyExecutionPolicy {
    pub max_slippage_bps: i64,
    pub max_participation_bps: u16,
    pub minimum_maker_confidence_bps: u16,
    pub cooldown_ms: u64,
    pub safety_buffer_ms: u64,
    pub taker_fee_ppm: i64,
    pub urgency_cost_bps_per_second: i64,
    pub deadline_penalty_bps: i64,
    pub cost_margin_bps: i64,
}

impl Default for EmergencyExecutionPolicy {
    fn default() -> Self {
        Self {
            max_slippage_bps: 25,
            max_participation_bps: 5_000,
            minimum_maker_confidence_bps: 7_000,
            cooldown_ms: 1_000,
            safety_buffer_ms: 1_000,
            taker_fee_ppm: 400,
            urgency_cost_bps_per_second: 2,
            deadline_penalty_bps: 100,
            cost_margin_bps: 1,
        }
    }
}

impl EmergencyExecutionPolicy {
    pub fn validate(&self) -> Result<(), &'static str> {
        if self.max_slippage_bps <= 0
            || self.max_participation_bps == 0
            || self.max_participation_bps > 10_000
            || self.minimum_maker_confidence_bps > 10_000
            || self.taker_fee_ppm < 0
            || self.urgency_cost_bps_per_second < 0
            || self.deadline_penalty_bps < 0
            || self.cost_margin_bps < 0
        {
            return Err("invalid emergency execution policy");
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TakerTrigger {
    MakerCannotMeetDeadline,
    WaitingCostExceedsTakerCost,
}

impl TakerTrigger {
    pub const fn label(self) -> &'static str {
        match self {
            Self::MakerCannotMeetDeadline => "maker_cannot_meet_deadline",
            Self::WaitingCostExceedsTakerCost => "waiting_cost_exceeds_taker_cost",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TakerBlockReason {
    Flat,
    UnknownRemoteState,
    StaleMarket,
    InvalidBook,
    MarkIndexDivergence,
    NoDeadline,
    Cooldown,
    NoSafeQuantity,
    KeepMakerWorking,
}

impl TakerBlockReason {
    pub const fn label(self) -> &'static str {
        match self {
            Self::Flat => "flat",
            Self::UnknownRemoteState => "unknown_remote_state",
            Self::StaleMarket => "stale_market",
            Self::InvalidBook => "invalid_book",
            Self::MarkIndexDivergence => "mark_index_divergence",
            Self::NoDeadline => "no_deadline",
            Self::Cooldown => "cooldown",
            Self::NoSafeQuantity => "no_safe_quantity",
            Self::KeepMakerWorking => "keep_maker_working",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AdaptiveTakerInput {
    pub now_ms: u64,
    pub deadline_ms: Option<u64>,
    pub position: i64,
    pub maker_remaining_quantity: i64,
    pub maker_estimated_time_ms: u64,
    pub maker_confidence_bps: u16,
    pub bid_price_ticks: i64,
    pub ask_price_ticks: i64,
    pub bid_quantity: i64,
    pub ask_quantity: i64,
    pub market_age_ms: u64,
    pub book_age_ms: u64,
    pub remote_state_known: bool,
    pub anchor_valid: bool,
    pub mark_index_gap_bps: Option<i64>,
    pub max_mark_index_gap_bps: i64,
    pub volatility_bps: i64,
    pub last_taker_at_ms: Option<u64>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TakerDecision {
    pub side: Side,
    pub price_ticks: i64,
    pub quantity: i64,
    pub trigger: TakerTrigger,
    pub maker_slack_ms: i64,
    pub taker_cost_bps: i64,
    pub waiting_cost_bps: i64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AdaptiveTakerDecision {
    Submit(TakerDecision),
    Hold(TakerBlockReason),
}

pub fn decide(
    policy: EmergencyExecutionPolicy,
    input: AdaptiveTakerInput,
) -> AdaptiveTakerDecision {
    if policy.validate().is_err() {
        return AdaptiveTakerDecision::Hold(TakerBlockReason::NoSafeQuantity);
    }
    if input.position == 0 {
        return AdaptiveTakerDecision::Hold(TakerBlockReason::Flat);
    }
    if !input.remote_state_known {
        return AdaptiveTakerDecision::Hold(TakerBlockReason::UnknownRemoteState);
    }
    if !input.anchor_valid || input.market_age_ms > policy.safety_buffer_ms.saturating_mul(5) {
        return AdaptiveTakerDecision::Hold(TakerBlockReason::StaleMarket);
    }
    if input.book_age_ms > policy.safety_buffer_ms.saturating_mul(5)
        || input.bid_price_ticks <= 0
        || input.ask_price_ticks <= input.bid_price_ticks
        || input.bid_quantity <= 0
        || input.ask_quantity <= 0
    {
        return AdaptiveTakerDecision::Hold(TakerBlockReason::InvalidBook);
    }
    if input.max_mark_index_gap_bps < 0
        || input
            .mark_index_gap_bps
            .is_none_or(|gap| gap < 0 || gap > input.max_mark_index_gap_bps)
    {
        return AdaptiveTakerDecision::Hold(TakerBlockReason::MarkIndexDivergence);
    }
    let Some(deadline_ms) = input.deadline_ms else {
        return AdaptiveTakerDecision::Hold(TakerBlockReason::NoDeadline);
    };
    if input
        .last_taker_at_ms
        .is_some_and(|last| input.now_ms.saturating_sub(last) < policy.cooldown_ms)
    {
        return AdaptiveTakerDecision::Hold(TakerBlockReason::Cooldown);
    }

    let remaining_ms = deadline_ms.saturating_sub(input.now_ms);
    let maker_slack = i128::from(remaining_ms)
        - i128::from(input.maker_estimated_time_ms)
        - i128::from(policy.safety_buffer_ms);
    let maker_slack_ms = maker_slack.clamp(i128::from(i64::MIN), i128::from(i64::MAX)) as i64;
    let mid = (i128::from(input.bid_price_ticks) + i128::from(input.ask_price_ticks)) / 2;
    if mid <= 0 {
        return AdaptiveTakerDecision::Hold(TakerBlockReason::InvalidBook);
    }
    let spread_bps = ((i128::from(input.ask_price_ticks - input.bid_price_ticks) * 10_000) / mid)
        .clamp(0, i128::from(i64::MAX)) as i64;
    let opposing_depth = if input.position > 0 {
        input.bid_quantity
    } else {
        input.ask_quantity
    };
    let position_quantity = input.position.checked_abs().unwrap_or(i64::MAX);
    let depth_impact_bps = (i128::from(position_quantity.max(1)) * 10_000
        / i128::from(opposing_depth.max(1)))
    .clamp(0, i128::from(i64::MAX)) as i64;
    let taker_cost_bps = spread_bps
        .saturating_add(policy.taker_fee_ppm / 100)
        .saturating_add(depth_impact_bps)
        .saturating_add(input.volatility_bps.max(0));
    let waiting_cost_bps = ((input.maker_estimated_time_ms / 1_000).max(1) as i64)
        .saturating_mul(policy.urgency_cost_bps_per_second)
        .saturating_add(if maker_slack < 0 {
            policy.deadline_penalty_bps
        } else {
            0
        });
    let maker_not_feasible =
        maker_slack < 0 || input.maker_confidence_bps < policy.minimum_maker_confidence_bps;
    if !maker_not_feasible
        && waiting_cost_bps <= taker_cost_bps.saturating_add(policy.cost_margin_bps)
    {
        return AdaptiveTakerDecision::Hold(TakerBlockReason::KeepMakerWorking);
    }

    let capped_by_participation =
        i128::from(opposing_depth) * i128::from(policy.max_participation_bps) / 10_000;
    let quantity = position_quantity
        .min(if input.maker_remaining_quantity > 0 {
            input.maker_remaining_quantity
        } else {
            position_quantity
        })
        .min(capped_by_participation.clamp(0, i128::from(i64::MAX)) as i64);
    if quantity <= 0 {
        return AdaptiveTakerDecision::Hold(TakerBlockReason::NoSafeQuantity);
    }

    let aggressive_price = |price: i64, buy: bool| {
        let delta = (i128::from(price) * i128::from(policy.max_slippage_bps) / 10_000)
            .clamp(1, i128::from(i64::MAX)) as i64;
        if buy {
            price.saturating_add(delta)
        } else {
            price.saturating_sub(delta).max(1)
        }
    };
    let (side, price_ticks) = if input.position > 0 {
        (Side::Sell, aggressive_price(input.bid_price_ticks, false))
    } else {
        (Side::Buy, aggressive_price(input.ask_price_ticks, true))
    };
    AdaptiveTakerDecision::Submit(TakerDecision {
        side,
        price_ticks,
        quantity,
        trigger: if maker_not_feasible {
            TakerTrigger::MakerCannotMeetDeadline
        } else {
            TakerTrigger::WaitingCostExceedsTakerCost
        },
        maker_slack_ms,
        taker_cost_bps,
        waiting_cost_bps,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn policy() -> EmergencyExecutionPolicy {
        EmergencyExecutionPolicy::default()
    }

    fn input() -> AdaptiveTakerInput {
        AdaptiveTakerInput {
            now_ms: 9_000,
            deadline_ms: Some(11_000),
            position: 100,
            maker_remaining_quantity: 100,
            maker_estimated_time_ms: 250,
            maker_confidence_bps: 9_000,
            bid_price_ticks: 990,
            ask_price_ticks: 1_000,
            bid_quantity: 1_000,
            ask_quantity: 1_000,
            market_age_ms: 10,
            book_age_ms: 10,
            remote_state_known: true,
            anchor_valid: true,
            mark_index_gap_bps: Some(1),
            max_mark_index_gap_bps: 50,
            volatility_bps: 0,
            last_taker_at_ms: None,
        }
    }

    #[test]
    fn keeps_maker_when_it_is_feasible_and_cheaper() {
        assert_eq!(
            decide(policy(), input()),
            AdaptiveTakerDecision::Hold(TakerBlockReason::KeepMakerWorking)
        );
    }

    #[test]
    fn escalates_when_maker_cannot_meet_deadline() {
        let mut value = input();
        value.maker_estimated_time_ms = 2_000;
        assert!(matches!(
            decide(policy(), value),
            AdaptiveTakerDecision::Submit(TakerDecision {
                trigger: TakerTrigger::MakerCannotMeetDeadline,
                side: Side::Sell,
                quantity: 100,
                ..
            })
        ));
    }

    #[test]
    fn hard_safety_gates_fail_closed_and_cooldown_is_adaptive_state() {
        let mut value = input();
        value.remote_state_known = false;
        assert_eq!(
            decide(policy(), value),
            AdaptiveTakerDecision::Hold(TakerBlockReason::UnknownRemoteState)
        );
        value.remote_state_known = true;
        value.last_taker_at_ms = Some(8_500);
        assert_eq!(
            decide(policy(), value),
            AdaptiveTakerDecision::Hold(TakerBlockReason::Cooldown)
        );
    }
}
