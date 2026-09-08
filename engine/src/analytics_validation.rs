use serde::Serialize;
use std::collections::BTreeMap;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub enum AnchorKind {
    FrozenClose,
    SessionOpen,
    ExternalReference,
}

#[derive(Debug, Clone, Serialize)]
pub struct ContractTransform {
    pub price_scale: u32,
    pub quote_currency: String,
    pub multiplier: i64,
    pub inverse: bool,
}

impl ContractTransform {
    pub fn validate(&self) -> Result<(), &'static str> {
        if self.price_scale > 18 || self.multiplier <= 0 || self.quote_currency.is_empty() {
            return Err("invalid contract transform");
        }
        Ok(())
    }

    pub fn transform_ticks(&self, value: i64) -> Option<i64> {
        self.validate().ok()?;
        let scaled = i128::from(value).checked_mul(i128::from(self.multiplier))?;
        let value = if self.inverse {
            if scaled == 0 {
                return None;
            }
            i128::from(1_000_000_000_000_000_000_i64).checked_mul(i128::from(self.multiplier))?
                / scaled
        } else {
            scaled
        };
        value
            .clamp(i128::from(i64::MIN), i128::from(i64::MAX))
            .try_into()
            .ok()
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct RuleRegime {
    pub regime_id: String,
    pub venue: String,
    pub contract_type: String,
    pub fee_ppm: i64,
    pub tick_size_ticks: i64,
    pub lot_size: i64,
    pub maker_only: bool,
    pub effective_from_ms: u64,
    pub effective_to_ms: Option<u64>,
}

impl RuleRegime {
    pub fn valid_at(&self, timestamp_ms: u64) -> bool {
        !self.regime_id.is_empty()
            && !self.venue.is_empty()
            && self.fee_ppm >= 0
            && self.tick_size_ticks > 0
            && self.lot_size > 0
            && timestamp_ms >= self.effective_from_ms
            && self.effective_to_ms.is_none_or(|end| timestamp_ms < end)
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct AnchorDefinition {
    pub symbol: String,
    pub kind: AnchorKind,
    pub close_price_ticks: i64,
    pub observed_at_ms: u64,
    pub valid_until_ms: u64,
    pub transform: ContractTransform,
    pub rule_regime_id: String,
    pub immutable: bool,
}

impl AnchorDefinition {
    pub fn validate(&self) -> Result<(), &'static str> {
        if self.symbol.is_empty()
            || self.close_price_ticks <= 0
            || self.valid_until_ms <= self.observed_at_ms
            || self.rule_regime_id.is_empty()
        {
            return Err("invalid anchor definition");
        }
        self.transform.validate()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub enum EpisodeOutcome {
    Converged,
    Adverse,
    Expired,
    Censored,
}

#[derive(Debug, Clone, Serialize)]
pub struct EpisodeRecord {
    pub episode_id: String,
    pub cluster_id: String,
    pub start_event_time_ms: u64,
    pub end_event_time_ms: u64,
    pub outcome: EpisodeOutcome,
    pub signed_improvement_bps: i64,
}

#[derive(Debug, Clone, Default, Serialize)]
pub struct CompetingRiskSummary {
    pub total: u64,
    pub converged: u64,
    pub adverse: u64,
    pub expired: u64,
    pub censored: u64,
    pub convergence_rate_ppm: i64,
    pub adverse_rate_ppm: i64,
}

pub fn competing_risk(records: &[EpisodeRecord]) -> CompetingRiskSummary {
    let mut summary = CompetingRiskSummary::default();
    for record in records {
        summary.total = summary.total.saturating_add(1);
        match record.outcome {
            EpisodeOutcome::Converged => summary.converged = summary.converged.saturating_add(1),
            EpisodeOutcome::Adverse => summary.adverse = summary.adverse.saturating_add(1),
            EpisodeOutcome::Expired => summary.expired = summary.expired.saturating_add(1),
            EpisodeOutcome::Censored => summary.censored = summary.censored.saturating_add(1),
        }
    }
    if summary.total > 0 {
        summary.convergence_rate_ppm =
            ((i128::from(summary.converged) * 1_000_000) / i128::from(summary.total)) as i64;
        summary.adverse_rate_ppm =
            ((i128::from(summary.adverse) * 1_000_000) / i128::from(summary.total)) as i64;
    }
    summary
}
#[derive(Debug, Clone, Default, Serialize)]
pub struct BootstrapSummary {
    pub replicates: u32,
    pub cluster_count: u64,
    pub mean_bps: i64,
    pub lower_bps: i64,
    pub upper_bps: i64,
}

pub fn cluster_bootstrap(
    records: &[EpisodeRecord],
    replicates: u32,
    seed: u64,
) -> BootstrapSummary {
    let mut clusters: BTreeMap<&str, Vec<i64>> = BTreeMap::new();
    for record in records {
        clusters
            .entry(&record.cluster_id)
            .or_default()
            .push(record.signed_improvement_bps);
    }
    let cluster_values: Vec<&Vec<i64>> = clusters.values().collect();
    let cluster_count = cluster_values.len() as u64;
    if cluster_values.is_empty() || replicates == 0 {
        return BootstrapSummary {
            replicates,
            cluster_count,
            ..BootstrapSummary::default()
        };
    }
    let reps = replicates.max(1);
    let mut rng = seed.max(1);
    let mut means = Vec::with_capacity(reps as usize);
    for _ in 0..reps {
        let mut sum = 0_i128;
        let mut count = 0_u64;
        for _ in 0..cluster_values.len() {
            rng = xorshift64(rng);
            let cluster = cluster_values[(rng as usize) % cluster_values.len()];
            for value in cluster {
                sum += i128::from(*value);
                count = count.saturating_add(1);
            }
        }
        means.push(if count == 0 {
            0
        } else {
            (sum / i128::from(count)).clamp(i128::from(i64::MIN), i128::from(i64::MAX)) as i64
        });
    }
    means.sort_unstable();
    let mean_bps =
        means.iter().map(|v| i128::from(*v)).sum::<i128>() / i128::from(means.len() as u64);
    let at = |ppm: u64| -> i64 {
        let idx = (((means.len() - 1) as u128 * ppm as u128) / 1_000_000) as usize;
        means[idx]
    };
    BootstrapSummary {
        replicates: reps,
        cluster_count,
        mean_bps: mean_bps as i64,
        lower_bps: at(25_000),
        upper_bps: at(975_000),
    }
}

fn xorshift64(mut value: u64) -> u64 {
    value ^= value << 13;
    value ^= value >> 7;
    value ^ (value << 17)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub enum EvidenceState {
    X0NoData,
    X1Observed,
    X2Eligible,
    X3Convergent,
    X4EconomicallyPositive,
    X5SurvivesStress,
    X6Validated,
}

impl EvidenceState {
    pub fn can_transition(self, next: Self) -> bool {
        (self as u8) + 1 >= next as u8 && (self as u8) <= next as u8
    }
}

#[derive(Debug, Clone, Copy, Serialize)]
pub struct EvidenceStateMachine {
    pub state: EvidenceState,
}

impl Default for EvidenceStateMachine {
    fn default() -> Self {
        Self {
            state: EvidenceState::X0NoData,
        }
    }
}

impl EvidenceStateMachine {
    pub fn transition(&mut self, next: EvidenceState) -> bool {
        if self.state.can_transition(next) {
            self.state = next;
            true
        } else {
            false
        }
    }
}

#[derive(Debug, Clone, Copy, Default, Serialize)]
pub struct LatentStateEstimate {
    pub residual_bps: i64,
    pub velocity_bps_per_s: i64,
    pub uncertainty_bps: i64,
}

impl LatentStateEstimate {
    pub fn update(
        &mut self,
        observed_residual_bps: i64,
        dt_ms: u64,
        process_noise_bps: i64,
        measurement_noise_bps: i64,
    ) {
        let dt = dt_ms.min(60_000) as i128;
        let predicted =
            i128::from(self.residual_bps) + i128::from(self.velocity_bps_per_s) * dt / 1_000;
        let prior_unc =
            i128::from(self.uncertainty_bps.max(0)) + i128::from(process_noise_bps.max(0));
        let measurement = i128::from(measurement_noise_bps.max(1));
        let gain_num = prior_unc;
        let gain_den = prior_unc + measurement;
        let gain = if gain_den == 0 {
            0
        } else {
            gain_num * 1_000 / gain_den
        };
        let innovation = i128::from(observed_residual_bps) - predicted;
        let filtered = predicted + innovation * gain / 1_000;
        self.velocity_bps_per_s = (innovation * 1_000 / dt.max(1))
            .clamp(i128::from(i64::MIN), i128::from(i64::MAX))
            as i64;
        self.residual_bps = filtered.clamp(i128::from(i64::MIN), i128::from(i64::MAX)) as i64;
        self.uncertainty_bps =
            ((prior_unc * (1_000 - gain)) / 1_000).clamp(0, i128::from(i64::MAX)) as i64;
    }
}
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn contract_transform_is_checked_and_deterministic() {
        let transform = ContractTransform {
            price_scale: 2,
            quote_currency: "USDT".to_owned(),
            multiplier: 2,
            inverse: false,
        };
        assert_eq!(transform.transform_ticks(7), Some(14));
    }

    #[test]
    fn competing_risk_keeps_censoring_separate() {
        let records = vec![
            EpisodeRecord {
                episode_id: "a".into(),
                cluster_id: "d1".into(),
                start_event_time_ms: 0,
                end_event_time_ms: 1,
                outcome: EpisodeOutcome::Converged,
                signed_improvement_bps: 10,
            },
            EpisodeRecord {
                episode_id: "b".into(),
                cluster_id: "d1".into(),
                start_event_time_ms: 0,
                end_event_time_ms: 1,
                outcome: EpisodeOutcome::Censored,
                signed_improvement_bps: -2,
            },
        ];
        let summary = competing_risk(&records);
        assert_eq!(summary.total, 2);
        assert_eq!(summary.convergence_rate_ppm, 500_000);
        assert_eq!(summary.adverse, 0);
    }

    #[test]
    fn bootstrap_is_seeded_and_clustered() {
        let records = vec![
            EpisodeRecord {
                episode_id: "a".into(),
                cluster_id: "d1".into(),
                start_event_time_ms: 0,
                end_event_time_ms: 1,
                outcome: EpisodeOutcome::Converged,
                signed_improvement_bps: 10,
            },
            EpisodeRecord {
                episode_id: "b".into(),
                cluster_id: "d2".into(),
                start_event_time_ms: 0,
                end_event_time_ms: 1,
                outcome: EpisodeOutcome::Adverse,
                signed_improvement_bps: -4,
            },
        ];
        let one = cluster_bootstrap(&records, 32, 7);
        let two = cluster_bootstrap(&records, 32, 7);
        assert_eq!(one.mean_bps, two.mean_bps);
        assert_eq!(one.cluster_count, 2);
    }

    #[test]
    fn state_machine_does_not_skip_evidence_levels() {
        let mut machine = EvidenceStateMachine::default();
        assert!(!machine.transition(EvidenceState::X4EconomicallyPositive));
        assert!(machine.transition(EvidenceState::X1Observed));
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub enum MethodAvailability {
    Available,
    Unavailable,
    Indeterminate,
}

#[derive(Debug, Clone, Serialize)]
pub struct PriceDiscoveryObservation {
    pub symbol: String,
    pub anchor_ticks: i64,
    pub opening_reference_ticks: Option<i64>,
    pub first_trade_ticks: Option<i64>,
    pub label: MethodAvailability,
    pub opening_gap_bps: Option<i64>,
}

pub fn classify_price_discovery(
    symbol: impl Into<String>,
    anchor_ticks: i64,
    opening_reference_ticks: Option<i64>,
    first_trade_ticks: Option<i64>,
) -> PriceDiscoveryObservation {
    let opening_gap_bps = opening_reference_ticks
        .filter(|value| *value > 0 && anchor_ticks > 0)
        .map(|value| (i128::from(value - anchor_ticks) * 10_000 / i128::from(anchor_ticks)) as i64);
    let label = if anchor_ticks <= 0 {
        MethodAvailability::Indeterminate
    } else if opening_reference_ticks.is_some() && first_trade_ticks.is_some() {
        MethodAvailability::Available
    } else {
        MethodAvailability::Unavailable
    };
    PriceDiscoveryObservation {
        symbol: symbol.into(),
        anchor_ticks,
        opening_reference_ticks,
        first_trade_ticks,
        label,
        opening_gap_bps,
    }
}

#[derive(Debug, Clone, Copy, Serialize)]
pub struct MakerProbeInput {
    pub bid_ticks: i64,
    pub ask_ticks: i64,
    pub bid_quantity: i64,
    pub ask_quantity: i64,
    pub own_bid_quantity: i64,
    pub own_ask_quantity: i64,
}

#[derive(Debug, Clone, Copy, Serialize)]
pub struct MakerProbeResult {
    pub self_excluded_bid_quantity: i64,
    pub self_excluded_ask_quantity: i64,
    pub mid_ticks: Option<i64>,
    pub availability: MethodAvailability,
}

pub fn self_excluded_lob_probe(input: MakerProbeInput) -> MakerProbeResult {
    let bid = input
        .bid_quantity
        .saturating_sub(input.own_bid_quantity.max(0));
    let ask = input
        .ask_quantity
        .saturating_sub(input.own_ask_quantity.max(0));
    let mid_ticks = if input.bid_ticks > 0 && input.ask_ticks >= input.bid_ticks {
        Some(input.bid_ticks.saturating_add(input.ask_ticks) / 2)
    } else {
        None
    };
    let availability = if mid_ticks.is_some() {
        MethodAvailability::Available
    } else {
        MethodAvailability::Indeterminate
    };
    MakerProbeResult {
        self_excluded_bid_quantity: bid.max(0),
        self_excluded_ask_quantity: ask.max(0),
        mid_ticks,
        availability,
    }
}

#[derive(Debug, Clone, Copy, Serialize)]
pub struct MarkoutPoint {
    pub horizon_ms: u64,
    pub mark_ticks: i64,
}

#[derive(Debug, Clone, Copy, Serialize)]
pub struct PostFillEdgeResult {
    pub gross_edge_bps: Option<i64>,
    pub net_edge_bps: Option<i64>,
    pub adverse_selection_bps: Option<i64>,
    pub markout_samples: u64,
    pub availability: MethodAvailability,
}

pub fn evaluate_post_fill_edge(
    buy_side: bool,
    fill_ticks: i64,
    anchor_ticks: i64,
    fee_bps: i64,
    funding_bps: i64,
    markouts: &[MarkoutPoint],
) -> PostFillEdgeResult {
    if fill_ticks <= 0 || anchor_ticks <= 0 {
        return PostFillEdgeResult {
            gross_edge_bps: None,
            net_edge_bps: None,
            adverse_selection_bps: None,
            markout_samples: 0,
            availability: MethodAvailability::Indeterminate,
        };
    }
    let gross = if buy_side {
        (i128::from(anchor_ticks - fill_ticks) * 10_000 / i128::from(fill_ticks)) as i64
    } else {
        (i128::from(fill_ticks - anchor_ticks) * 10_000 / i128::from(fill_ticks)) as i64
    };
    let net = gross.saturating_sub(fee_bps).saturating_sub(funding_bps);
    let adverse = markouts
        .iter()
        .filter(|point| point.mark_ticks > 0)
        .map(|point| {
            let move_bps = if buy_side {
                (i128::from(point.mark_ticks - fill_ticks) * 10_000 / i128::from(fill_ticks)) as i64
            } else {
                (i128::from(fill_ticks - point.mark_ticks) * 10_000 / i128::from(fill_ticks)) as i64
            };
            -move_bps
        })
        .min();
    PostFillEdgeResult {
        gross_edge_bps: Some(gross),
        net_edge_bps: Some(net),
        adverse_selection_bps: adverse,
        markout_samples: markouts.len() as u64,
        availability: if markouts.is_empty() {
            MethodAvailability::Indeterminate
        } else {
            MethodAvailability::Available
        },
    }
}

#[derive(Debug, Clone, Copy, Serialize)]
pub struct CausalContrast {
    pub treated_n: u64,
    pub control_n: u64,
    pub treated_mean_bps: Option<i64>,
    pub control_mean_bps: Option<i64>,
    pub difference_bps: Option<i64>,
    pub availability: MethodAvailability,
}

pub fn causal_contrast(treated: &[i64], control: &[i64]) -> CausalContrast {
    let mean = |values: &[i64]| -> Option<i64> {
        if values.is_empty() {
            None
        } else {
            Some(
                (values.iter().map(|v| i128::from(*v)).sum::<i128>()
                    / i128::from(values.len() as u64)) as i64,
            )
        }
    };
    let treated_mean_bps = mean(treated);
    let control_mean_bps = mean(control);
    let difference_bps = treated_mean_bps
        .zip(control_mean_bps)
        .map(|(a, b)| a.saturating_sub(b));
    CausalContrast {
        treated_n: treated.len() as u64,
        control_n: control.len() as u64,
        treated_mean_bps,
        control_mean_bps,
        difference_bps,
        availability: if difference_bps.is_some() {
            MethodAvailability::Available
        } else {
            MethodAvailability::Unavailable
        },
    }
}

#[derive(Debug, Clone, Copy, Serialize)]
pub struct SurvivalSummary {
    pub observations: u64,
    pub terminal_equity_bps: i64,
    pub max_drawdown_bps: i64,
    pub ruin: bool,
    pub availability: MethodAvailability,
}

pub fn evaluate_survival(
    initial_equity_bps: i64,
    equity_path: &[i64],
    ruin_floor_bps: i64,
) -> SurvivalSummary {
    if equity_path.is_empty() || initial_equity_bps <= 0 {
        return SurvivalSummary {
            observations: 0,
            terminal_equity_bps: initial_equity_bps,
            max_drawdown_bps: 0,
            ruin: false,
            availability: MethodAvailability::Unavailable,
        };
    }
    let mut peak = initial_equity_bps;
    let mut max_drawdown = 0_i64;
    for equity in equity_path {
        peak = peak.max(*equity);
        max_drawdown = max_drawdown.max(peak.saturating_sub(*equity));
    }
    let terminal = *equity_path.last().unwrap_or(&initial_equity_bps);
    SurvivalSummary {
        observations: equity_path.len() as u64,
        terminal_equity_bps: terminal,
        max_drawdown_bps: max_drawdown,
        ruin: equity_path.iter().any(|value| *value <= ruin_floor_bps),
        availability: MethodAvailability::Available,
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub enum ValidationVerdict {
    Supported,
    Falsified,
    Indeterminate,
}

#[derive(Debug, Clone, Copy, Serialize)]
pub struct VerdictInputs {
    pub h_r_positive: bool,
    pub h_m_positive: bool,
    pub h_d_consistent: bool,
    pub h_e_positive: bool,
    pub h_s_survives: bool,
    pub labels_complete: bool,
}

pub fn adjudicate_verdict(input: VerdictInputs) -> ValidationVerdict {
    if !input.labels_complete {
        return ValidationVerdict::Indeterminate;
    }
    if input.h_r_positive
        && input.h_m_positive
        && input.h_d_consistent
        && input.h_e_positive
        && input.h_s_survives
    {
        ValidationVerdict::Supported
    } else {
        ValidationVerdict::Falsified
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct ValidationSummary {
    pub methodology_id: String,
    pub price_discovery: MethodAvailability,
    pub self_excluded_lob: MethodAvailability,
    pub post_fill_edge: MethodAvailability,
    pub causal_contrast: MethodAvailability,
    pub survival: MethodAvailability,
    pub verdict: ValidationVerdict,
}

impl Default for ValidationSummary {
    fn default() -> Self {
        Self {
            methodology_id: "anchorbell-analytics-methods-v2".to_owned(),
            price_discovery: MethodAvailability::Unavailable,
            self_excluded_lob: MethodAvailability::Unavailable,
            post_fill_edge: MethodAvailability::Unavailable,
            causal_contrast: MethodAvailability::Unavailable,
            survival: MethodAvailability::Unavailable,
            verdict: ValidationVerdict::Indeterminate,
        }
    }
}

/// Inputs used by the automatic promotion gate.  The gate is deliberately
/// conservative: a live run is never promoted from order count alone.
#[derive(Debug, Clone, Copy, Serialize)]
pub struct SimulationPromotionInput {
    pub ledger_count: u64,
    pub orders: u64,
    pub fills: u64,
    pub records_dropped: u64,
    pub valuation_incomplete_ledgers: u64,
    pub non_flat_ledgers: u64,
    pub total_net_pnl_ticks: i64,
}

#[derive(Debug, Clone, Serialize)]
pub struct SimulationPromotionGate {
    pub methodology_id: String,
    pub minimum_fills: u64,
    pub integrity_passed: bool,
    pub evidence_sufficient: bool,
    pub economic_passed: bool,
    pub survival_passed: bool,
    pub verdict: ValidationVerdict,
    pub reason: String,
}

pub fn evaluate_simulation_promotion(input: SimulationPromotionInput) -> SimulationPromotionGate {
    const MINIMUM_FILLS: u64 = 100;
    if input.ledger_count > 1 {
        return SimulationPromotionGate {
            methodology_id: "anchorbell-automatic-promotion-v2".to_owned(),
            minimum_fills: MINIMUM_FILLS,
            integrity_passed: input.records_dropped == 0,
            evidence_sufficient: false,
            economic_passed: false,
            survival_passed: input.valuation_incomplete_ledgers == 0 && input.non_flat_ledgers == 0,
            verdict: ValidationVerdict::Indeterminate,
            reason: "candidate_level_oos_validation_required".to_owned(),
        };
    }
    let integrity_passed = input.ledger_count > 0 && input.records_dropped == 0;
    let evidence_sufficient = input.fills >= MINIMUM_FILLS && input.orders >= input.fills;
    let economic_passed = evidence_sufficient && input.total_net_pnl_ticks > 0;
    let survival_passed =
        integrity_passed && input.valuation_incomplete_ledgers == 0 && input.non_flat_ledgers == 0;
    let verdict = if integrity_passed && evidence_sufficient && economic_passed && survival_passed {
        ValidationVerdict::Supported
    } else if integrity_passed && evidence_sufficient && (!economic_passed || !survival_passed) {
        ValidationVerdict::Falsified
    } else {
        ValidationVerdict::Indeterminate
    };
    let reason = if !integrity_passed {
        "data_integrity_failed".to_owned()
    } else if !evidence_sufficient {
        "insufficient_fill_evidence".to_owned()
    } else if !economic_passed {
        "net_pnl_not_positive".to_owned()
    } else if !survival_passed {
        "open_risk_or_incomplete_valuation".to_owned()
    } else {
        "all_promotion_conditions_passed".to_owned()
    };
    SimulationPromotionGate {
        methodology_id: "anchorbell-automatic-promotion-v2".to_owned(),
        minimum_fills: MINIMUM_FILLS,
        integrity_passed,
        evidence_sufficient,
        economic_passed,
        survival_passed,
        verdict,
        reason,
    }
}

#[cfg(test)]
mod method_tests {
    use super::*;

    #[test]
    fn price_discovery_requires_opening_and_trade_labels() {
        let observation = classify_price_discovery("X", 100, Some(102), Some(103));
        assert_eq!(observation.label, MethodAvailability::Available);
        assert_eq!(observation.opening_gap_bps, Some(200));
    }

    #[test]
    fn post_fill_net_edge_subtracts_costs() {
        let result = evaluate_post_fill_edge(
            true,
            100,
            102,
            5,
            2,
            &[MarkoutPoint {
                horizon_ms: 1_000,
                mark_ticks: 101,
            }],
        );
        assert_eq!(result.net_edge_bps, Some(193));
        assert_eq!(result.markout_samples, 1);
    }

    #[test]
    fn multi_ledger_matrix_requires_candidate_level_oos_validation() {
        let gate = evaluate_simulation_promotion(SimulationPromotionInput {
            ledger_count: 10,
            orders: 1_000,
            fills: 500,
            records_dropped: 0,
            valuation_incomplete_ledgers: 0,
            non_flat_ledgers: 0,
            total_net_pnl_ticks: 1_000_000,
        });
        assert_eq!(gate.verdict, ValidationVerdict::Indeterminate);
        assert_eq!(gate.reason, "candidate_level_oos_validation_required");
        assert!(!gate.economic_passed);
    }

    #[test]
    fn incomplete_verdict_is_indeterminate() {
        assert_eq!(
            adjudicate_verdict(VerdictInputs {
                h_r_positive: true,
                h_m_positive: true,
                h_d_consistent: true,
                h_e_positive: true,
                h_s_survives: true,
                labels_complete: false,
            }),
            ValidationVerdict::Indeterminate
        );
    }
}
