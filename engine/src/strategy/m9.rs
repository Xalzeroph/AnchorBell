//! M9 deadline-constrained causal residual / joint-outcome / DRO-MPC policy.
use super::PriceTicks;
use serde::{Deserialize, Serialize};

pub const M9_MODEL_VERSION: &str = "m9-dcr-dro-mpc-v1";
const PICO_BPS_SCALE: i128 = 1_000_000_000_000;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub enum M9Phase {
    Build,
    Harvest,
    Exit,
    Reconcile,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub enum M9Action {
    NoAction,
    BuyMaker,
    SellMaker,
    ReduceBuy,
    ReduceSell,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct M9Calibration {
    pub half_life_ms: u64,
    pub exit_lead_ms: u64,
    pub fill_horizon_ms: u64,
    pub fill_hazard_bps: i64,
    pub null_rw_min_weight_bps: i64,
    pub uncertainty_bps: i64,
    pub adverse_selection_bps: i64,
    pub exit_participation_bps: i64,
    pub min_residual_pico_bps: i64,
    pub min_robust_lcb_pico_bps: i64,
}

impl M9Calibration {
    #[cfg(test)]
    /// Explicit conservative bootstrap prior. It is a versioned prior, not
    /// evidence of alpha; production calibration must replace this bundle.
    #[cfg(test)]
    pub const fn bootstrap() -> Self {
        Self {
            half_life_ms: 4 * 60 * 60 * 1_000,
            exit_lead_ms: 30 * 60 * 1_000,
            fill_horizon_ms: 5 * 60 * 1_000,
            fill_hazard_bps: 2_500,
            null_rw_min_weight_bps: 3_500,
            uncertainty_bps: 8,
            adverse_selection_bps: 4,
            exit_participation_bps: 5_000,
            min_residual_pico_bps: PICO_BPS_SCALE as i64,
            min_robust_lcb_pico_bps: 2 * PICO_BPS_SCALE as i64,
        }
    }

    pub(crate) fn valid(self) -> bool {
        self.half_life_ms > 0
            && self.exit_lead_ms > 0
            && self.fill_horizon_ms > 0
            && (0..=10_000).contains(&self.fill_hazard_bps)
            && (0..=10_000).contains(&self.null_rw_min_weight_bps)
            && self.uncertainty_bps >= 0
            && self.adverse_selection_bps >= 0
            && (0..=10_000).contains(&self.exit_participation_bps)
            && self.min_residual_pico_bps >= 0
            && self.min_robust_lcb_pico_bps >= 0
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct M9Input {
    pub now_ms: u64,
    pub deadline_ms: u64,
    pub fair_value_ticks: PriceTicks,
    pub mid_ticks: PriceTicks,
    pub bid_ticks: PriceTicks,
    pub ask_ticks: PriceTicks,
    pub mark_ticks: PriceTicks,
    pub index_ticks: PriceTicks,
    pub bid_quantity: i64,
    pub ask_quantity: i64,
    pub queue_ahead_quantity: i64,
    pub position: i64,
    pub max_position: i64,
    pub requested_quantity: i64,
    /// Compatibility display fields; exact calculations use the pico fields.
    pub volatility_bps: i64,
    pub spread_bps: i64,
    pub funding_carry_bps: i64,
    pub fee_bps: i64,
    pub markout_bps: i64,
    pub volatility_pico_bps: i64,
    pub spread_pico_bps: i64,
    pub funding_carry_pico_bps: i64,
    pub fee_pico_bps: i64,
    pub markout_pico_bps: i64,
    pub data_valid: bool,
    pub funding_valid: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub struct M9Decision {
    pub action: M9Action,
    pub phase: M9Phase,
    pub fair_value_ticks: i64,
    pub residual_bps: i64,
    pub expected_reversion_bps: i64,
    pub residual_pico_bps: i64,
    pub expected_reversion_pico_bps: i64,
    pub fill_probability_lcb_bps: i64,
    pub exit_capacity_quantity: i64,
    pub robust_lcb_bps: i64,
    pub robust_lcb_pico_bps: i64,
    pub quantity: i64,
    pub reason: &'static str,
}

impl M9Decision {
    fn blocked(input: M9Input, phase: M9Phase, reason: &'static str) -> Self {
        Self {
            action: M9Action::NoAction,
            phase,
            fair_value_ticks: input.fair_value_ticks.0,
            residual_bps: 0,
            expected_reversion_bps: 0,
            residual_pico_bps: 0,
            expected_reversion_pico_bps: 0,
            fill_probability_lcb_bps: 0,
            exit_capacity_quantity: 0,
            robust_lcb_bps: i64::MIN,
            robust_lcb_pico_bps: i64::MIN,
            quantity: 0,
            reason,
        }
    }
}

fn pico_bps(delta: i64, base: i64) -> i64 {
    if base <= 0 {
        return 0;
    }
    ((i128::from(delta) * 10_000 * PICO_BPS_SCALE) / i128::from(base))
        .clamp(i128::from(i64::MIN), i128::from(i64::MAX)) as i64
}

fn pico_to_bps(value: i64) -> i64 {
    ((i128::from(value)
        + if value >= 0 {
            PICO_BPS_SCALE / 2
        } else {
            -(PICO_BPS_SCALE / 2)
        })
        / PICO_BPS_SCALE)
        .clamp(i128::from(i64::MIN), i128::from(i64::MAX)) as i64
}

fn bps_to_pico(value: i64) -> i64 {
    (i128::from(value).saturating_mul(PICO_BPS_SCALE))
        .clamp(i128::from(i64::MIN), i128::from(i64::MAX)) as i64
}

fn exact_input(value_pico: i64, value_bps: i64) -> i64 {
    if value_pico != 0 {
        value_pico
    } else {
        bps_to_pico(value_bps)
    }
}

fn phase(remaining_ms: u64, calibration: M9Calibration) -> M9Phase {
    if remaining_ms == 0 {
        M9Phase::Reconcile
    } else if remaining_ms <= calibration.exit_lead_ms {
        M9Phase::Exit
    } else if remaining_ms <= calibration.exit_lead_ms.saturating_mul(3) {
        M9Phase::Harvest
    } else {
        M9Phase::Build
    }
}

fn positive_capacity(
    input: M9Input,
    calibration: M9Calibration,
    exposure: i64,
    exit_buy: bool,
) -> i64 {
    let depth = if exit_buy {
        input.bid_quantity
    } else {
        input.ask_quantity
    }
    .max(0);
    let depth_cap = i128::from(depth) * i128::from(calibration.exit_participation_bps) / 10_000;
    exposure
        .max(0)
        .min(depth_cap.clamp(0, i128::from(i64::MAX)) as i64)
}

fn fill_lower_bound(input: M9Input, calibration: M9Calibration, remaining_ms: u64) -> i64 {
    let horizon = remaining_ms.min(calibration.fill_horizon_ms.saturating_mul(8));
    let time_factor = i128::from(horizon) * 10_000
        / i128::from(horizon.saturating_add(calibration.fill_horizon_ms).max(1));
    let size = input.requested_quantity.max(1);
    let queue = input.queue_ahead_quantity.max(0);
    let queue_factor = i128::from(size) * 10_000 / i128::from(queue.saturating_add(size).max(1));
    (i128::from(calibration.fill_hazard_bps) * time_factor * queue_factor / 100_000_000)
        .clamp(0, 10_000) as i64
}

pub fn decide(input: M9Input, calibration: M9Calibration) -> M9Decision {
    let remaining = input.deadline_ms.saturating_sub(input.now_ms);
    let current_phase = phase(remaining, calibration);
    if !calibration.valid() {
        return M9Decision::blocked(input, current_phase, "calibration_invalid");
    }
    if input.deadline_ms == 0 {
        return M9Decision::blocked(input, current_phase, "deadline_unknown");
    }
    if !input.data_valid || !input.funding_valid {
        return M9Decision::blocked(input, current_phase, "market_or_funding_unusable");
    }
    if input.fair_value_ticks.0 <= 0
        || input.mid_ticks.0 <= 0
        || input.bid_ticks.0 <= 0
        || input.ask_ticks.0 < input.bid_ticks.0
        || input.mark_ticks.0 <= 0
        || input.index_ticks.0 <= 0
        || input.max_position <= 0
        || input.requested_quantity <= 0
    {
        return M9Decision::blocked(input, current_phase, "invalid_m9_input");
    }
    let residual_pico = pico_bps(
        input.fair_value_ticks.0 - input.mid_ticks.0,
        input.mid_ticks.0,
    );
    let residual = pico_to_bps(residual_pico);
    let signal = residual_pico.signum();
    let abs_residual_pico = residual_pico.unsigned_abs().min(i64::MAX as u64) as i64;
    let current_exposure = input.position.unsigned_abs().min(i64::MAX as u64) as i64;
    let candidate_exposure = current_exposure.saturating_add(input.requested_quantity.max(0));
    let exit_buy = if input.position != 0 {
        input.position < 0
    } else {
        signal < 0
    };
    let exit_capacity = positive_capacity(input, calibration, candidate_exposure, exit_buy);
    if input.position != 0
        && (current_phase == M9Phase::Exit || (signal != 0 && signal != input.position.signum()))
    {
        let side = if input.position > 0 {
            M9Action::ReduceSell
        } else {
            M9Action::ReduceBuy
        };
        let quantity = exit_capacity.min(input.position.unsigned_abs().min(i64::MAX as u64) as i64);
        if quantity <= 0 {
            return M9Decision::blocked(input, current_phase, "exit_capacity_unavailable");
        }
        return M9Decision {
            action: side,
            phase: current_phase,
            fair_value_ticks: input.fair_value_ticks.0,
            residual_bps: residual,
            expected_reversion_bps: 0,
            residual_pico_bps: residual_pico,
            expected_reversion_pico_bps: 0,
            fill_probability_lcb_bps: 0,
            exit_capacity_quantity: exit_capacity,
            robust_lcb_bps: 0,
            robust_lcb_pico_bps: 0,
            quantity,
            reason: "deadline_or_residual_direction_requires_reduction",
        };
    }
    if current_phase == M9Phase::Exit || current_phase == M9Phase::Reconcile {
        return M9Decision::blocked(input, current_phase, "entry_deadline_passed");
    }
    if signal == 0 || abs_residual_pico < calibration.min_residual_pico_bps {
        return M9Decision::blocked(input, current_phase, "residual_below_identification_floor");
    }
    let decay = i128::from(remaining.min(calibration.half_life_ms.saturating_mul(8))) * 10_000
        / i128::from(remaining.saturating_add(calibration.half_life_ms).max(1));
    let expected_pico = (i128::from(abs_residual_pico) * decay / 10_000
        * i128::from(10_000 - calibration.null_rw_min_weight_bps)
        / 10_000)
        .clamp(0, i128::from(i64::MAX)) as i64;
    let expected = pico_to_bps(expected_pico);
    let fill_lcb = fill_lower_bound(input, calibration, remaining);
    let exposure_limit = if signal > 0 {
        (input.max_position - input.position).max(0)
    } else {
        (input.max_position + input.position).max(0)
    };
    let quantity = input
        .requested_quantity
        .min(exposure_limit)
        .min(exit_capacity.max(0));
    let volatility_pico = exact_input(input.volatility_pico_bps, input.volatility_bps).max(0);
    let spread_pico = exact_input(input.spread_pico_bps, input.spread_bps).max(0);
    let fee_pico = exact_input(input.fee_pico_bps, input.fee_bps).max(0);
    let markout_pico = exact_input(input.markout_pico_bps, input.markout_bps).max(0);
    let funding_carry_pico = exact_input(input.funding_carry_pico_bps, input.funding_carry_bps);
    let costs_pico = fee_pico
        .saturating_add(spread_pico)
        .saturating_add(volatility_pico / 2)
        .saturating_add(markout_pico)
        .saturating_add(funding_carry_pico.saturating_neg().max(0))
        .saturating_add(bps_to_pico(calibration.uncertainty_bps))
        .saturating_add(bps_to_pico(calibration.adverse_selection_bps));
    let deadline_penalty_pico = if remaining < calibration.exit_lead_ms.saturating_mul(2) {
        bps_to_pico(5)
    } else {
        0
    };
    let robust_lcb_pico = (i128::from(expected_pico) * i128::from(fill_lcb) / 10_000)
        .saturating_sub(i128::from(costs_pico))
        .saturating_sub(i128::from(deadline_penalty_pico))
        .clamp(i128::from(i64::MIN), i128::from(i64::MAX)) as i64;
    let robust_lcb = pico_to_bps(robust_lcb_pico);
    if quantity <= 0 {
        return M9Decision::blocked(input, current_phase, "exit_capacity_or_position_limit");
    }
    if robust_lcb_pico < calibration.min_robust_lcb_pico_bps {
        return M9Decision {
            action: M9Action::NoAction,
            phase: current_phase,
            fair_value_ticks: input.fair_value_ticks.0,
            residual_bps: residual,
            expected_reversion_bps: expected,
            residual_pico_bps: residual_pico,
            expected_reversion_pico_bps: expected_pico,
            fill_probability_lcb_bps: fill_lcb,
            exit_capacity_quantity: exit_capacity,
            robust_lcb_bps: robust_lcb,
            robust_lcb_pico_bps: robust_lcb_pico,
            quantity: 0,
            reason: "robust_lcb_below_required_edge",
        };
    }
    let action = if signal > 0 {
        M9Action::BuyMaker
    } else {
        M9Action::SellMaker
    };
    M9Decision {
        action,
        phase: current_phase,
        fair_value_ticks: input.fair_value_ticks.0,
        residual_bps: residual,
        expected_reversion_bps: expected,
        residual_pico_bps: residual_pico,
        expected_reversion_pico_bps: expected_pico,
        fill_probability_lcb_bps: fill_lcb,
        exit_capacity_quantity: exit_capacity,
        robust_lcb_bps: robust_lcb,
        robust_lcb_pico_bps: robust_lcb_pico,
        quantity,
        reason: "robust_deadline_action_admissible",
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn input() -> M9Input {
        M9Input {
            now_ms: 0,
            deadline_ms: 10 * 60 * 60 * 1_000,
            fair_value_ticks: PriceTicks(101_000),
            mid_ticks: PriceTicks(100_000),
            bid_ticks: PriceTicks(99_999),
            ask_ticks: PriceTicks(100_001),
            mark_ticks: PriceTicks(100_000),
            index_ticks: PriceTicks(100_000),
            bid_quantity: 1_000,
            ask_quantity: 1_000,
            queue_ahead_quantity: 10,
            position: 0,
            max_position: 1_000,
            requested_quantity: 100,
            volatility_bps: 1,
            spread_bps: 1,
            funding_carry_bps: 0,
            fee_bps: 4,
            markout_bps: 0,
            volatility_pico_bps: 1_000_000_000_000,
            spread_pico_bps: 1_000_000_000_000,
            funding_carry_pico_bps: 0,
            fee_pico_bps: 4_000_000_000_000,
            markout_pico_bps: 0,
            data_valid: true,
            funding_valid: true,
        }
    }
    #[test]
    fn null_weight_and_costs_can_block_a_weak_edge() {
        let mut x = input();
        x.fair_value_ticks = PriceTicks(100_080);
        assert_eq!(
            decide(x, M9Calibration::bootstrap()).action,
            M9Action::NoAction
        );
    }
    #[test]
    fn deadline_reduces_existing_inventory() {
        let mut x = input();
        x.position = 100;
        x.deadline_ms = 20 * 60 * 1_000;
        assert_eq!(
            decide(x, M9Calibration::bootstrap()).action,
            M9Action::ReduceSell
        );
    }
    #[test]
    fn flat_entry_has_capacity_when_candidate_is_exitable() {
        let mut x = input();
        x.fair_value_ticks = PriceTicks(105_000);
        x.bid_quantity = 10_000;
        x.ask_quantity = 10_000;
        x.queue_ahead_quantity = 0;
        x.volatility_bps = 0;
        x.spread_bps = 0;
        x.fee_bps = 0;
        x.markout_bps = 0;
        x.volatility_pico_bps = 0;
        x.spread_pico_bps = 0;
        x.fee_pico_bps = 0;
        x.markout_pico_bps = 0;
        let mut calibration = M9Calibration::bootstrap();
        calibration.fill_hazard_bps = 10_000;
        calibration.null_rw_min_weight_bps = 0;
        calibration.uncertainty_bps = 0;
        calibration.adverse_selection_bps = 0;
        calibration.min_robust_lcb_pico_bps = 0;
        let decision = decide(x, calibration);
        assert!(decision.exit_capacity_quantity > 0);
        assert!(
            decision.quantity > 0,
            "flat entry was incorrectly capacity-blocked"
        );
    }
    #[test]
    fn invalid_data_fails_closed() {
        let mut x = input();
        x.data_valid = false;
        assert_eq!(
            decide(x, M9Calibration::bootstrap()).reason,
            "market_or_funding_unusable"
        );
    }
}
