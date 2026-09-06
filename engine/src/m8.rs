//! M8 Funding-Aware Robust Anchor Control.
//! Pure strategy math: no exchange, simulator, or persistence dependency.
use serde::Serialize;

const PICO_BPS_SCALE: i128 = 1_000_000_000_000;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub enum FundingAction {
    NoAction,
    Collect,
    Tolerate,
    Avoid,
    Exit,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub enum FundingRateStatus {
    Observed,
    Missing,
    Stale,
    Special,
    Unknown,
}

#[derive(Debug, Clone, Copy)]
pub struct M8Input {
    pub now_ms: u64,
    pub anchor_ticks: i64,
    pub mid_ticks: i64,
    pub mark_ticks: i64,
    pub index_ticks: i64,
    pub position: i64,
    pub max_position: i64,
    pub funding_rate_e8: Option<i64>,
    pub next_funding_ms: Option<u64>,
    pub funding_rate_status: FundingRateStatus,
    pub fee_ppm: i64,
    /// Compatibility display fields; exact calculations use pico-bps.
    pub volatility_bps: i64,
    pub spread_bps: i64,
    pub model_uncertainty_bps: i64,
    pub liquidation_buffer_bps: i64,
    pub volatility_pico_bps: i64,
    pub spread_pico_bps: i64,
    pub model_uncertainty_pico_bps: i64,
    pub liquidation_buffer_pico_bps: i64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub struct M8Decision {
    pub action: FundingAction,
    pub allow_entry: bool,
    pub reduce_only: bool,
    pub side: i8,
    pub anchor_edge_bps: i64,
    pub funding_carry_bps: i64,
    pub total_cost_bps: i64,
    pub net_edge_bps: i64,
    pub safety_margin_bps: i64,
    pub anchor_edge_pico_bps: i64,
    pub funding_carry_pico_bps: i64,
    pub total_cost_pico_bps: i64,
    pub net_edge_pico_bps: i64,
    pub safety_margin_pico_bps: i64,
    pub reason: &'static str,
}

fn pico_bps(num: i64, den: i64) -> i64 {
    if num <= 0 || den <= 0 {
        return 0;
    }
    ((i128::from(num) * 10_000 * PICO_BPS_SCALE) / i128::from(den)).clamp(0, i128::from(i64::MAX))
        as i64
}

fn pico_to_bps(value: i64) -> i64 {
    if value == 0 {
        return 0;
    }
    let magnitude = i128::from(value.unsigned_abs());
    let rounded =
        ((magnitude + PICO_BPS_SCALE / 2) / PICO_BPS_SCALE).clamp(0, i128::from(i64::MAX)) as i64;
    if value >= 0 {
        rounded
    } else {
        rounded.saturating_neg()
    }
}

fn bps_to_pico(value: i64) -> i64 {
    (i128::from(value).saturating_mul(PICO_BPS_SCALE))
        .clamp(i128::from(i64::MIN), i128::from(i64::MAX)) as i64
}

fn ppm_to_pico_bps(ppm: i64) -> i64 {
    if ppm <= 0 {
        return 0;
    }
    (i128::from(ppm) * PICO_BPS_SCALE / 100).clamp(0, i128::from(i64::MAX)) as i64
}

fn exact_or_bps(pico: i64, bps: i64) -> i64 {
    if pico != 0 {
        pico
    } else {
        bps_to_pico(bps)
    }
}

fn signed_edge(anchor: i64, mid: i64) -> (i8, i64) {
    if mid < anchor {
        (1, pico_bps(anchor - mid, mid))
    } else if mid > anchor {
        (-1, pico_bps(mid - anchor, mid))
    } else {
        (0, 0)
    }
}

fn funding_carry(side: i8, rate_e8: Option<i64>) -> i64 {
    rate_e8
        .map(|rate| {
            ((-i128::from(side) * i128::from(rate) * PICO_BPS_SCALE) / 10_000)
                .clamp(i128::from(i64::MIN), i128::from(i64::MAX)) as i64
        })
        .unwrap_or(0)
}

pub fn decide(input: M8Input) -> M8Decision {
    let zero = M8Decision {
        action: FundingAction::NoAction,
        allow_entry: false,
        reduce_only: false,
        side: 0,
        anchor_edge_bps: 0,
        funding_carry_bps: 0,
        total_cost_bps: i64::MAX,
        net_edge_bps: i64::MIN,
        safety_margin_bps: 0,
        anchor_edge_pico_bps: 0,
        funding_carry_pico_bps: 0,
        total_cost_pico_bps: i64::MAX,
        net_edge_pico_bps: i64::MIN,
        safety_margin_pico_bps: 0,
        reason: "invalid_or_unknown_state",
    };
    if input.anchor_ticks <= 0
        || input.mid_ticks <= 0
        || input.mark_ticks <= 0
        || input.index_ticks <= 0
        || input.max_position <= 0
        || input.fee_ppm < 0
        || input.volatility_bps < 0
        || input.spread_bps < 0
        || input.model_uncertainty_bps < 0
        || input.liquidation_buffer_bps < 0
        || input.volatility_pico_bps < 0
        || input.spread_pico_bps < 0
        || input.model_uncertainty_pico_bps < 0
        || input.liquidation_buffer_pico_bps < 0
    {
        return zero;
    }
    if input.funding_rate_status == FundingRateStatus::Unknown
        || input.funding_rate_status == FundingRateStatus::Missing
        || input.next_funding_ms.is_none()
    {
        return zero;
    }
    let (signal_side, anchor_edge_pico) = signed_edge(input.anchor_ticks, input.mid_ticks);
    let anchor_edge = pico_to_bps(anchor_edge_pico);
    let held_side = input.position.signum() as i8;
    let side = if held_side != 0 {
        held_side
    } else {
        signal_side
    };
    let carry_pico = funding_carry(side, input.funding_rate_e8);
    let carry = pico_to_bps(carry_pico);
    let volatility_pico = exact_or_bps(input.volatility_pico_bps, input.volatility_bps).max(0);
    let spread_pico = exact_or_bps(input.spread_pico_bps, input.spread_bps).max(0);
    let uncertainty_pico = exact_or_bps(
        input.model_uncertainty_pico_bps,
        input.model_uncertainty_bps,
    )
    .max(0);
    let liquidation_buffer_pico = exact_or_bps(
        input.liquidation_buffer_pico_bps,
        input.liquidation_buffer_bps,
    )
    .max(0);
    let costs_pico = ppm_to_pico_bps(input.fee_ppm.saturating_mul(2))
        .saturating_add(volatility_pico)
        .saturating_add(spread_pico / 2)
        .saturating_add(uncertainty_pico)
        .saturating_add(liquidation_buffer_pico);
    let costs = pico_to_bps(costs_pico);
    let net_pico = anchor_edge_pico
        .saturating_add(carry_pico)
        .saturating_sub(costs_pico);
    let net = pico_to_bps(net_pico);
    let remaining = input
        .next_funding_ms
        .unwrap_or(0)
        .saturating_sub(input.now_ms);
    let near_funding = remaining <= 5 * 60 * 1_000;
    let safety_pico = costs_pico.saturating_add(bps_to_pico(5));
    let safety = pico_to_bps(safety_pico);
    if input.position != 0
        && (net_pico <= 0 || input.funding_rate_status == FundingRateStatus::Special)
    {
        return M8Decision {
            action: FundingAction::Exit,
            allow_entry: false,
            reduce_only: true,
            side,
            anchor_edge_bps: anchor_edge,
            funding_carry_bps: carry,
            total_cost_bps: costs,
            net_edge_bps: net,
            safety_margin_bps: safety,
            anchor_edge_pico_bps: anchor_edge_pico,
            funding_carry_pico_bps: carry_pico,
            total_cost_pico_bps: costs_pico,
            net_edge_pico_bps: net_pico,
            safety_margin_pico_bps: safety_pico,
            reason: "held_edge_not_compensating_funding_or_special",
        };
    }
    if signal_side == 0 || anchor_edge_pico <= 0 {
        return M8Decision {
            action: FundingAction::NoAction,
            allow_entry: false,
            reduce_only: false,
            side,
            anchor_edge_bps: anchor_edge,
            funding_carry_bps: carry,
            total_cost_bps: costs,
            net_edge_bps: net,
            safety_margin_bps: safety,
            anchor_edge_pico_bps: anchor_edge_pico,
            funding_carry_pico_bps: carry_pico,
            total_cost_pico_bps: costs_pico,
            net_edge_pico_bps: net_pico,
            safety_margin_pico_bps: safety_pico,
            reason: "anchor_edge_not_observable",
        };
    }
    if near_funding && carry_pico > 0 && net_pico > safety_pico {
        return M8Decision {
            action: FundingAction::Collect,
            allow_entry: true,
            reduce_only: false,
            side,
            anchor_edge_bps: anchor_edge,
            funding_carry_bps: carry,
            total_cost_bps: costs,
            net_edge_bps: net,
            safety_margin_bps: safety,
            anchor_edge_pico_bps: anchor_edge_pico,
            funding_carry_pico_bps: carry_pico,
            total_cost_pico_bps: costs_pico,
            net_edge_pico_bps: net_pico,
            safety_margin_pico_bps: safety_pico,
            reason: "carry_and_anchor_edge_cover_tail_cost",
        };
    }
    if net_pico > safety_pico {
        return M8Decision {
            action: FundingAction::Tolerate,
            allow_entry: true,
            reduce_only: false,
            side,
            anchor_edge_bps: anchor_edge,
            funding_carry_bps: carry,
            total_cost_bps: costs,
            net_edge_bps: net,
            safety_margin_bps: safety,
            anchor_edge_pico_bps: anchor_edge_pico,
            funding_carry_pico_bps: carry_pico,
            total_cost_pico_bps: costs_pico,
            net_edge_pico_bps: net_pico,
            safety_margin_pico_bps: safety_pico,
            reason: "anchor_edge_covers_funding_and_cost",
        };
    }
    M8Decision {
        action: FundingAction::Avoid,
        allow_entry: false,
        reduce_only: input.position != 0,
        side,
        anchor_edge_bps: anchor_edge,
        funding_carry_bps: carry,
        total_cost_bps: costs,
        net_edge_bps: net,
        safety_margin_bps: safety,
        anchor_edge_pico_bps: anchor_edge_pico,
        funding_carry_pico_bps: carry_pico,
        total_cost_pico_bps: costs_pico,
        net_edge_pico_bps: net_pico,
        safety_margin_pico_bps: safety_pico,
        reason: "funding_or_tail_cost_exceeds_conservative_edge",
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    fn input(rate: Option<i64>) -> M8Input {
        M8Input {
            now_ms: 0,
            anchor_ticks: 100_000,
            mid_ticks: 99_000,
            mark_ticks: 99_000,
            index_ticks: 99_000,
            position: 0,
            max_position: 1_000,
            funding_rate_e8: rate,
            next_funding_ms: Some(60_000),
            funding_rate_status: FundingRateStatus::Observed,
            fee_ppm: 4,
            volatility_bps: 1,
            spread_bps: 1,
            model_uncertainty_bps: 1,
            liquidation_buffer_bps: 1,
            volatility_pico_bps: 1_000_000_000_000,
            spread_pico_bps: 1_000_000_000_000,
            model_uncertainty_pico_bps: 1_000_000_000_000,
            liquidation_buffer_pico_bps: 1_000_000_000_000,
        }
    }
    #[test]
    fn positive_short_funding_can_be_collected_with_edge() {
        let d = decide(input(Some(-20_000)));
        assert!(d.allow_entry);
        assert_eq!(d.action, FundingAction::Collect);
    }
    #[test]
    fn unknown_rate_fails_closed() {
        let mut x = input(None);
        x.funding_rate_status = FundingRateStatus::Missing;
        assert_eq!(decide(x).action, FundingAction::NoAction);
    }
    #[test]
    fn zero_funding_does_not_create_a_deadline_exit_when_edge_remains() {
        let d = decide(input(Some(0)));
        assert_eq!(d.action, FundingAction::Tolerate);
        assert!(d.allow_entry);
        assert_ne!(d.reason, "held_edge_not_compensating_funding_or_special");
    }
    #[test]
    fn expensive_carry_is_avoided() {
        let mut x = input(Some(200_000));
        x.mid_ticks = 99_950;
        assert_eq!(decide(x).action, FundingAction::Avoid);
    }
}
