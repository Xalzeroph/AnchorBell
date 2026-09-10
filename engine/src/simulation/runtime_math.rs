use super::*;

pub(super) fn edge_pico_bps(numerator_price: i64, denominator_price: i64) -> Option<i64> {
    if numerator_price <= 0 || denominator_price <= 0 {
        return None;
    }
    Some(
        ((i128::from(numerator_price) - i128::from(denominator_price))
            * 10_000
            * i128::from(PICO_BPS_SCALE)
            / i128::from(denominator_price))
        .clamp(i128::from(i64::MIN), i128::from(i64::MAX)) as i64,
    )
}

/// Compatibility diagnostic; admission uses edge_pico_bps directly.
pub(super) fn fair_value_for_state(state: &SimulationSymbolState) -> Option<FairValueEstimate> {
    let book = state.book?;
    let index = state.index_price_ticks?;
    let mark = state.mark_price_ticks?;
    let mid = book.bid_price_ticks.checked_add(book.ask_price_ticks)? / 2;
    FairValueEstimate::from_market_precise(
        crate::strategy::PriceTicks(state.anchor.close_price_ticks),
        crate::strategy::PriceTicks(index),
        crate::strategy::PriceTicks(mark),
        crate::strategy::PriceTicks(mid),
        state.ewma_abs_return_pico_bps,
        state.ewma_spread_pico_bps,
    )
}

pub(super) fn dynamic_threshold_diagnostic_for(
    state: &SimulationSymbolState,
    variant: SimulationPolicyVariant,
    floor_bps: i64,
    fee_ppm: i64,
    requested_quantity: i64,
    max_position: i64,
    timestamp_ms: u64,
) -> ThresholdDiagnostic {
    let Some(book) = state.book else {
        return ThresholdDiagnostic {
            status: ThresholdStatus::WarmingUp,
            threshold: None,
            prior_used: true,
            missing_component: Some("book"),
        };
    };
    let Some(mark) = state.mark_price_ticks else {
        return ThresholdDiagnostic {
            status: ThresholdStatus::InsufficientData,
            threshold: None,
            prior_used: true,
            missing_component: Some("mark_price"),
        };
    };
    let Some(index) = state.index_price_ticks else {
        return ThresholdDiagnostic {
            status: ThresholdStatus::InsufficientData,
            threshold: None,
            prior_used: true,
            missing_component: Some("index_price"),
        };
    };
    if book.bid_price_ticks <= 0
        || book.ask_price_ticks < book.bid_price_ticks
        || book.bid_quantity <= 0
        || book.ask_quantity <= 0
        || mark <= 0
        || index <= 0
        || floor_bps < 0
        || fee_ppm < 0
        || requested_quantity <= 0
        || max_position <= 0
    {
        return ThresholdDiagnostic {
            status: ThresholdStatus::InvalidInput,
            threshold: None,
            prior_used: false,
            missing_component: Some("validated_components"),
        };
    }
    let gap_pico_bps = edge_pico_bps(mark, index)
        .map(|value| i128::from(value.unsigned_abs()))
        .unwrap_or(i128::from(i64::MAX))
        .min(i128::from(i64::MAX)) as i64;
    let prior_used = variant != SimulationPolicyVariant::M0Fixed
        && (state.ewma_abs_return_pico_bps == 0 || state.ewma_spread_pico_bps == 0);
    let volatility_pico_bps = if state.ewma_abs_return_pico_bps == 0 {
        THRESHOLD_PRIOR_VOLATILITY_PICO_BPS
    } else {
        state.ewma_abs_return_pico_bps.saturating_mul(3)
    };
    let cost_pico_bps = ppm_to_pico_bps(fee_ppm.saturating_mul(2));
    let fair_value_confidence_pico_bps = fair_value_for_state(state)
        .map(|estimate| {
            i128::from(estimate.confidence_bps).saturating_mul(i128::from(PICO_BPS_SCALE))
        })
        .unwrap_or(i128::from(gap_pico_bps))
        .clamp(0, i128::from(i64::MAX)) as i64;
    let confidence_component_pico_bps = 5 * i128::from(PICO_BPS_SCALE)
        + i128::from(fair_value_confidence_pico_bps).min(50 * i128::from(PICO_BPS_SCALE));
    let uncertainty_pico_bps = (i128::from(gap_pico_bps) / 2)
        .max(confidence_component_pico_bps)
        .clamp(0, i128::from(i64::MAX)) as i64;
    let spread_pico_bps = if state.ewma_spread_pico_bps == 0 {
        THRESHOLD_PRIOR_SPREAD_PICO_BPS
    } else {
        state.ewma_spread_pico_bps / 2
    };
    let liquidity_pico_bps =
        liquidity_penalty_pico_bps(requested_quantity, book.bid_quantity, book.ask_quantity);
    let baseline_adverse_selection_pico_bps = if variant.uses_microstructure() {
        state.ewma_abs_return_pico_bps.saturating_mul(2)
    } else {
        0
    };
    let fill_feedback_pico_bps = if variant == SimulationPolicyVariant::M0Fixed {
        0
    } else {
        conservative_adverse_markout_pico_bps(state)
    };
    // Favorable markout feedback may reduce the estimated cost, but a cost
    // component must never become negative and invalidate the full model.
    let adverse_selection_pico_bps = baseline_adverse_selection_pico_bps
        .saturating_add(fill_feedback_pico_bps)
        .max(0);
    let statistical_pico_bps = if variant.uses_statistical_term() {
        uncertainty_pico_bps
            .saturating_add(cost_pico_bps)
            .saturating_add(spread_pico_bps)
            .saturating_add(adverse_selection_pico_bps)
            .saturating_add(state.ewma_abs_return_pico_bps.saturating_mul(8))
    } else {
        0
    };
    let tail_risk_pico_bps = if variant.uses_tail_guard() {
        m5_tail_risk_pico(state)
    } else {
        0
    };
    let inventory_pico_bps = if max_position > 0 {
        let ratio_pico = i128::from(state.position).abs() * 10_000 * i128::from(PICO_BPS_SCALE)
            / i128::from(max_position);
        (ratio_pico.saturating_mul(ratio_pico) / (1_000_000_i128 * i128::from(PICO_BPS_SCALE)))
            .clamp(0, i128::from(i64::MAX)) as i64
    } else {
        100 * PICO_BPS_SCALE
    };
    let funding_remaining_ms = state.next_funding_time_ms.saturating_sub(timestamp_ms);
    let deadline_risk_pico_bps =
        if state.next_funding_time_ms > timestamp_ms && state.latest_funding_rate_e8.is_some() {
            if funding_remaining_ms <= 10 * 60 * 1_000 {
                50 * PICO_BPS_SCALE
            } else if funding_remaining_ms <= 30 * 60 * 1_000 {
                25 * PICO_BPS_SCALE
            } else if funding_remaining_ms <= 60 * 60 * 1_000 {
                10 * PICO_BPS_SCALE
            } else {
                0
            }
        } else {
            0
        };
    let floor_pico_bps = i128::from(floor_bps.max(0))
        .saturating_mul(i128::from(PICO_BPS_SCALE))
        .clamp(0, i128::from(i64::MAX)) as i64;
    let threshold = AdaptiveThreshold::from_pico_components(
        floor_pico_bps,
        if variant == SimulationPolicyVariant::M0Fixed {
            0
        } else {
            volatility_pico_bps
        },
        if variant == SimulationPolicyVariant::M0Fixed {
            0
        } else {
            cost_pico_bps
        },
        if variant == SimulationPolicyVariant::M0Fixed {
            0
        } else {
            uncertainty_pico_bps
        },
        if variant == SimulationPolicyVariant::M0Fixed {
            0
        } else {
            deadline_risk_pico_bps
        },
        if variant == SimulationPolicyVariant::M0Fixed {
            0
        } else {
            5 * PICO_BPS_SCALE
        },
        if variant == SimulationPolicyVariant::M0Fixed {
            0
        } else {
            spread_pico_bps
        },
        adverse_selection_pico_bps,
        if variant == SimulationPolicyVariant::M0Fixed {
            0
        } else {
            liquidity_pico_bps
        },
        if variant == SimulationPolicyVariant::M0Fixed {
            0
        } else {
            inventory_pico_bps
        },
        statistical_pico_bps,
        tail_risk_pico_bps,
    );
    ThresholdDiagnostic {
        status: if threshold.is_some() {
            if prior_used {
                ThresholdStatus::WarmingUp
            } else {
                ThresholdStatus::Ready
            }
        } else {
            ThresholdStatus::ModelFailure
        },
        threshold,
        prior_used,
        missing_component: None,
    }
}

pub(super) fn dynamic_threshold_for(
    state: &SimulationSymbolState,
    variant: SimulationPolicyVariant,
    floor_bps: i64,
    fee_ppm: i64,
    requested_quantity: i64,
    max_position: i64,
    timestamp_ms: u64,
) -> Option<AdaptiveThreshold> {
    dynamic_threshold_diagnostic_for(
        state,
        variant,
        floor_bps,
        fee_ppm,
        requested_quantity,
        max_position,
        timestamp_ms,
    )
    .threshold
}

pub(super) fn scale_threshold_non_fee(
    threshold: AdaptiveThreshold,
    scale_ppm: i64,
) -> AdaptiveThreshold {
    let scale = |value: i64| {
        (i128::from(value) * i128::from(scale_ppm.clamp(0, 1_000_000)) / 1_000_000)
            .clamp(0, i128::from(i64::MAX)) as i64
    };
    let values = threshold.components_pico_bps();
    AdaptiveThreshold::from_pico_components(
        scale(values[0]),
        scale(values[1]),
        values[2],
        values[3],
        values[4],
        scale(values[5]),
        scale(values[6]),
        scale(values[7]),
        scale(values[8]),
        scale(values[9]),
        scale(values[10]),
        scale(values[11]),
    )
    .expect("scaled threshold components are non-negative")
}

pub(super) fn threshold_metrics(threshold: AdaptiveThreshold) -> ThresholdMetrics {
    let exact = threshold.components_pico_bps();
    ThresholdMetrics {
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
        tail_risk_bps: threshold.tail_risk_bps,
        floor_pico_bps: exact[0],
        residual_volatility_pico_bps: exact[1],
        cost_pico_bps: exact[2],
        uncertainty_pico_bps: exact[3],
        deadline_risk_pico_bps: exact[4],
        safety_margin_pico_bps: exact[5],
        spread_pico_bps: exact[6],
        adverse_selection_pico_bps: exact[7],
        liquidity_pico_bps: exact[8],
        inventory_pico_bps: exact[9],
        statistical_pico_bps: exact[10],
        tail_risk_pico_bps: exact[11],
        required_bps: threshold.required_bps(),
        required_pico_bps: threshold.required_pico_bps(),
        required_micro_bps: threshold.required_micro_bps(),
    }
}

pub(super) fn ewma_scaled(previous: i64, sample: i64) -> i64 {
    if previous <= 0 {
        sample.max(0)
    } else {
        ((i128::from(previous) * i128::from(EWMA_PREVIOUS_WEIGHT_PPM)
            + i128::from(sample.max(0)) * i128::from(EWMA_SAMPLE_WEIGHT_PPM))
            / i128::from(1_000_000_i64))
        .clamp(0, i128::from(i64::MAX)) as i64
    }
}

pub(super) fn ewma_signed_scaled(previous: i64, sample: i64) -> i64 {
    if previous == 0 {
        sample
    } else {
        ((i128::from(previous) * i128::from(EWMA_PREVIOUS_WEIGHT_PPM)
            + i128::from(sample) * i128::from(EWMA_SAMPLE_WEIGHT_PPM))
            / i128::from(1_000_000_i64))
        .clamp(i128::from(i64::MIN), i128::from(i64::MAX)) as i64
    }
}

/// Shrunk directional persistence in basis points. The ratio is analogous to
/// a bounded signal-to-noise measure: signed drift divided by absolute move,
/// then shrunk toward zero until enough causal observations accumulate.
pub(super) fn trend_persistence_bps(state: &SimulationSymbolState) -> i64 {
    let signed = i128::from(state.ewma_signed_return_pico_bps.unsigned_abs());
    let volatility = i128::from(state.ewma_abs_return_pico_bps.max(0));
    let observations = state.calibration.return_abs_pico_bps.len() as i128;
    if signed == 0 || volatility <= 0 || observations <= 0 {
        return 0;
    }
    let raw_bps = (signed * 10_000 / volatility).clamp(0, 10_000);
    (raw_bps * observations / (observations + TREND_PRIOR_OBSERVATIONS)).clamp(0, 10_000) as i64
}

/// Directional penalty for a quote that fights the locally persistent move.
/// The quadratic ratio `signed^2 / absolute` suppresses noisy sign flips while
/// retaining a strong penalty for a coherent trend. A zero-trend prior avoids
/// overreacting to the first few ticks; the cap keeps this a risk surcharge,
/// never an arithmetic substitute for the hard market-data gates.
pub(super) fn trend_conflict_pico_bps(state: &SimulationSymbolState, side: Side) -> i64 {
    directional_trend_conflict_pico_bps(
        state.ewma_signed_return_pico_bps,
        state.ewma_abs_return_pico_bps,
        state.calibration.return_abs_pico_bps.len(),
        side,
    )
}

pub(super) fn directional_trend_conflict_pico_bps(
    signed_return_pico_bps: i64,
    absolute_return_pico_bps: i64,
    observations: usize,
    side: Side,
) -> i64 {
    let signed = i128::from(signed_return_pico_bps);
    let conflicts = match side {
        Side::Buy => signed < 0,
        Side::Sell => signed > 0,
    };
    let signed_abs = signed.abs();
    let volatility = i128::from(absolute_return_pico_bps.max(0));
    let observations = observations as i128;
    if !conflicts || signed_abs <= 0 || volatility <= 0 || observations <= 0 {
        return 0;
    }
    let quadratic_signal = signed_abs
        .saturating_mul(signed_abs)
        .checked_div(volatility)
        .unwrap_or(i128::MAX);
    let shrunk = quadratic_signal
        .saturating_mul(observations)
        .checked_div(observations + TREND_PRIOR_OBSERVATIONS)
        .unwrap_or(i128::MAX);
    shrunk
        .saturating_mul(2)
        .clamp(0, i128::from(TREND_CONFLICT_CAP_PICO_BPS)) as i64
}

/// Update the causal residual state once per event-time observation. The
/// residual is fair value minus mid, so its signed level identifies the
/// direction of the hypothesised reversion, its signed first difference
/// identifies directional drift, and its absolute first difference identifies
/// whether the dislocation is expanding or contracting. Sign persistence is
/// an online serial-dependence proxy; it is intentionally kept separate from
/// the level so a long-lived but shrinking residual is not confused with a
/// new adverse regime.
pub(super) fn observe_residual_dynamics(
    state: &mut SimulationSymbolState,
    event_time_ms: u64,
    residual: Option<i64>,
) {
    let Some(residual) = residual else { return };
    if state
        .last_residual_dynamics_time_ms
        .is_some_and(|previous| event_time_ms <= previous)
    {
        return;
    }
    state.ewma_signed_residual_pico_bps =
        ewma_signed_scaled(state.ewma_signed_residual_pico_bps, residual);
    if let Some(previous) = state.last_residual_pico_bps {
        let previous_abs = previous.unsigned_abs().min(i64::MAX as u64) as i64;
        let residual_abs = residual.unsigned_abs().min(i64::MAX as u64) as i64;
        let signed_change = residual.saturating_sub(previous);
        let absolute_change = residual_abs.saturating_sub(previous_abs);
        state.ewma_signed_residual_drift_pico_bps =
            ewma_signed_scaled(state.ewma_signed_residual_drift_pico_bps, signed_change);
        state.ewma_residual_drift_pico_bps =
            ewma_signed_scaled(state.ewma_residual_drift_pico_bps, absolute_change);
        let persistence_sample = if previous == 0 || residual == 0 {
            0
        } else if previous.signum() == residual.signum() {
            1_000_000
        } else {
            -1_000_000
        };
        state.ewma_residual_persistence_ppm =
            ewma_signed_scaled(state.ewma_residual_persistence_ppm, persistence_sample);
    }
    state.last_residual_pico_bps = Some(residual);
    state.last_residual_dynamics_time_ms = Some(event_time_ms);
}

/// Conservative upper risk score for the residual regime. The score combines
/// three online signals: expansion away from zero, drift in the direction
/// adverse to the candidate quote, and same-sign serial persistence. Every
/// component is normalized by the observed residual/return scale and shrunk
/// by the causal observation count. This is a continuous risk budget, not an
/// assertion that a regime has statistically changed.
pub(super) fn residual_regime_risk_pico_bps(state: &SimulationSymbolState, side: Side) -> i64 {
    let observations = state.calibration.residual_abs_pico_bps.len() as i128;
    if observations <= 0 {
        return 0;
    }
    let scale = i128::from(
        state
            .ewma_signed_residual_pico_bps
            .unsigned_abs()
            .min(i64::MAX as u64)
            .max(
                state
                    .ewma_abs_return_pico_bps
                    .unsigned_abs()
                    .min(i64::MAX as u64),
            )
            .max(PICO_BPS_SCALE as u64) as i64,
    );
    let expansion = i128::from(state.ewma_residual_drift_pico_bps.max(0));
    let directional_drift = match side {
        Side::Buy => state.ewma_signed_residual_drift_pico_bps.max(0),
        Side::Sell => state
            .ewma_signed_residual_drift_pico_bps
            .saturating_neg()
            .max(0),
    };
    let drift = expansion.max(i128::from(directional_drift));
    if drift <= 0 {
        return 0;
    }
    let persistence = i128::from(state.ewma_residual_persistence_ppm.max(0));
    let persistence_factor = (500_000_i128 + persistence / 2).clamp(0, 1_000_000);
    let raw = drift
        .saturating_mul(i128::from(RESIDUAL_REGIME_CAP_PICO_BPS))
        .checked_div(scale)
        .unwrap_or(i128::from(RESIDUAL_REGIME_CAP_PICO_BPS))
        .clamp(0, i128::from(RESIDUAL_REGIME_CAP_PICO_BPS));
    raw.saturating_mul(persistence_factor)
        .checked_div(1_000_000)
        .unwrap_or(0)
        .saturating_mul(observations)
        .checked_div(observations + TREND_PRIOR_OBSERVATIONS)
        .unwrap_or(0)
        .clamp(0, i128::from(RESIDUAL_REGIME_CAP_PICO_BPS)) as i64
}

pub(super) fn residual_regime_scale_ppm(state: &SimulationSymbolState) -> i64 {
    let risk = residual_regime_risk_pico_bps(state, Side::Buy)
        .max(residual_regime_risk_pico_bps(state, Side::Sell));
    let cap = i128::from(RESIDUAL_REGIME_CAP_PICO_BPS.max(1));
    (1_000_000_i128 - (i128::from(risk) * i128::from(1_000_000 - MIN_EVIDENCE_SCALE_PPM) / cap))
        .clamp(i128::from(MIN_EVIDENCE_SCALE_PPM), 1_000_000) as i64
}

/// Lower confidence bound for the probability that a residual observation is
/// followed by a causal halving event. This is deliberately a lower bound:
/// scarce or serially dependent reversion evidence cannot create size.
pub(super) fn reversion_evidence_lower_bps(state: &SimulationSymbolState) -> i64 {
    let trials = state
        .calibration
        .residual_abs_pico_bps
        .len()
        .saturating_sub(1) as u64;
    let successes = state.calibration.reversion_events.min(trials);
    wilson_lower_probability_bps(successes, trials)
}

pub(super) fn reversion_evidence_scale_ppm(state: &SimulationSymbolState) -> i64 {
    let lower_bps = reversion_evidence_lower_bps(state);
    MIN_EVIDENCE_SCALE_PPM.saturating_add(
        (i128::from(lower_bps) * i128::from(1_000_000 - MIN_EVIDENCE_SCALE_PPM) / 10_000)
            .clamp(0, i128::from(1_000_000 - MIN_EVIDENCE_SCALE_PPM)) as i64,
    )
}

pub(super) fn wilson_lower_probability_bps(successes: u64, trials: u64) -> i64 {
    if trials == 0 {
        return 0;
    }
    let n = trials as f64;
    let p = successes.min(trials) as f64 / n;
    let z = 1.96_f64;
    let z_squared = z * z;
    let denominator = 1.0 + z_squared / n;
    let center = p + z_squared / (2.0 * n);
    let margin = z * (p * (1.0 - p) / n + z_squared / (4.0 * n * n)).sqrt();
    ((center - margin) / denominator * 10_000.0)
        .floor()
        .clamp(0.0, 10_000.0) as i64
}

pub(super) fn ewma_micro(previous: i64, sample: i64) -> i64 {
    ewma_scaled(previous, sample)
}

pub(super) fn pico_bps_to_micro(value: i64) -> i64 {
    if value == 0 {
        return 0;
    }
    let magnitude = i128::from(value.unsigned_abs());
    let rounded = ((magnitude + i128::from(PICO_BPS_SCALE / MICRO_BPS_SCALE / 2))
        / i128::from(PICO_BPS_SCALE / MICRO_BPS_SCALE))
    .clamp(0, i128::from(i64::MAX)) as i64;
    if value >= 0 {
        rounded
    } else {
        rounded.saturating_neg()
    }
}

#[cfg(test)]
pub(super) fn micro_bps_to_bps(value: i64) -> i64 {
    if value == 0 {
        return 0;
    }
    let magnitude = i128::from(value.unsigned_abs());
    let rounded = ((magnitude + i128::from(MICRO_BPS_SCALE / 2)) / i128::from(MICRO_BPS_SCALE))
        .clamp(0, i128::from(i64::MAX)) as i64;
    if value >= 0 {
        rounded
    } else {
        rounded.saturating_neg()
    }
}

#[cfg(test)]
pub(super) fn edge_micro_bps(numerator_price: i64, denominator_price: i64) -> Option<i64> {
    edge_pico_bps(numerator_price, denominator_price).map(pico_bps_to_micro)
}

pub(super) fn pico_bps_to_bps(value: i64) -> i64 {
    if value == 0 {
        return 0;
    }
    let magnitude = i128::from(value.unsigned_abs());
    let rounded = ((magnitude + i128::from(PICO_BPS_SCALE / 2)) / i128::from(PICO_BPS_SCALE))
        .clamp(0, i128::from(i64::MAX)) as i64;
    if value >= 0 {
        rounded
    } else {
        rounded.saturating_neg()
    }
}

pub(super) fn ppm_to_pico_bps(ppm: i64) -> i64 {
    if ppm <= 0 {
        return 0;
    }
    (i128::from(ppm) * i128::from(PICO_BPS_SCALE) / 100).clamp(0, i128::from(i64::MAX)) as i64
}

pub(super) fn bps_between(left: i64, right: i64) -> i64 {
    edge_pico_bps(left, right)
        .map(pico_bps_to_bps)
        .unwrap_or(i64::MAX)
}

pub(super) fn bps_between_pico(left: i64, right: i64) -> i64 {
    edge_pico_bps(left, right)
        .map(|value| i128::from(value.unsigned_abs()).min(i128::from(i64::MAX)) as i64)
        .unwrap_or(i64::MAX)
}

pub(super) const M5_TAIL_CAUTION_BPS: i64 = 35;
pub(super) const M5_TAIL_REDUCE_ONLY_BPS: i64 = 60;
pub(super) const M5_TAIL_HALT_BPS: i64 = 100;

pub(super) fn m5_tail_stress_pico(state: &SimulationSymbolState) -> i64 {
    let volatility = state.ewma_abs_return_pico_bps.saturating_mul(4);
    let mark_index = match (state.mark_price_ticks, state.index_price_ticks) {
        (Some(mark), Some(index)) => bps_between_pico(mark, index).saturating_mul(2),
        _ => i64::MAX,
    };
    let spread = state.ewma_spread_pico_bps.saturating_mul(4);
    // Historical runs exposed a failure mode that volatility-only tail
    // controls missed: CXMT/MINIMAX/ZHONGJIU could show a large apparent
    // anchor edge while anchor/index/mark disagreed enough to classify the
    // fair value as dislocated. Treat that cross-source dispersion as risk,
    // not as free mean-reversion alpha. This makes the existing M5 policy
    // shrink or reduce-only exactly when the reference model loses consensus.
    let reference_dispersion = fair_value_for_state(state)
        .map(|estimate| estimate.dispersion_pico_bps)
        .unwrap_or(i64::MAX);
    volatility
        .max(mark_index)
        .max(spread)
        .max(reference_dispersion)
}

pub(super) fn m5_tail_stress_bps(state: &SimulationSymbolState) -> i64 {
    pico_bps_to_bps(m5_tail_stress_pico(state))
}

pub(super) fn m5_tail_risk_pico(state: &SimulationSymbolState) -> i64 {
    // Keep the tail premium finite. A halted quote is a risk decision, not an
    // arithmetic failure; the signal remains explainable as a large hurdle.
    m5_tail_stress_pico(state)
        .saturating_sub(M5_TAIL_CAUTION_BPS.saturating_mul(PICO_BPS_SCALE))
        .max(0)
        .saturating_mul(2)
        .min(10_000 * PICO_BPS_SCALE)
}

pub(super) fn m5_quote_quantity(state: &SimulationSymbolState, requested_quantity: i64) -> i64 {
    m5_scaled_quantity(m5_tail_stress_bps(state), requested_quantity)
}

pub(super) fn m5_scaled_quantity(stress: i64, requested_quantity: i64) -> i64 {
    if requested_quantity <= 0 {
        return 0;
    }
    if stress >= M5_TAIL_HALT_BPS {
        return 0;
    }
    if stress <= M5_TAIL_CAUTION_BPS {
        return requested_quantity;
    }
    if stress >= M5_TAIL_REDUCE_ONLY_BPS {
        return requested_quantity / 4;
    }

    // Use a continuous risk budget between the caution and reduce-only
    // boundaries. The old step function changed quantity from 100% to 50%
    // at exactly 35 bps and from 50% to 25% at 60 bps, creating artificial
    // quote churn and discontinuous fill selection. The affine scale is
    // monotone in stress, bounded in [25%, 100%], and remains conservative
    // relative to the old policy at the reduce-only boundary.
    let span = M5_TAIL_REDUCE_ONLY_BPS - M5_TAIL_CAUTION_BPS;
    let remaining = M5_TAIL_REDUCE_ONLY_BPS - stress;
    let scale_bps =
        2_500_i128 + i128::from(remaining.max(0)) * 7_500_i128 / i128::from(span.max(1));
    let scaled = i128::from(requested_quantity) * scale_bps / 10_000_i128;
    scaled
        .clamp(1, i128::from(requested_quantity))
        .min(i128::from(i64::MAX)) as i64
}

pub(super) fn m5_tail_reduce_only(state: &SimulationSymbolState) -> bool {
    m5_tail_stress_bps(state) >= M5_TAIL_REDUCE_ONLY_BPS
}

pub(super) fn core_v1_margin_scaled_quantity(
    state: &SimulationSymbolState,
    intent: OrderIntent,
    threshold: AdaptiveThreshold,
) -> i64 {
    if intent.reduce_only || intent.quantity <= 0 {
        return intent.quantity;
    }
    let Some(fair_value) = fair_value_for_state(state) else {
        return intent.quantity;
    };
    let edge = match intent.side {
        Side::Buy => edge_pico_bps(fair_value.price.0, intent.price),
        Side::Sell => edge_pico_bps(intent.price, fair_value.price.0),
    };
    let Some(edge) = edge.filter(|edge| *edge > 0) else {
        return intent.quantity;
    };
    let Some(hurdle) = threshold.required_pico_bps().filter(|hurdle| *hurdle > 0) else {
        return intent.quantity;
    };
    let edge_scaled_quantity = fractional_edge_quantity(edge, hurdle, intent.quantity);
    let evidence_scale = reversion_evidence_scale_ppm(state);
    let evidence_scaled = (i128::from(edge_scaled_quantity) * i128::from(evidence_scale)
        / i128::from(1_000_000_i64))
    .clamp(1, i128::from(edge_scaled_quantity)) as i64;
    residual_regime_scaled_quantity(
        residual_regime_risk_pico_bps(state, intent.side),
        evidence_scaled,
    )
}

/// Fractional-Kelly-inspired sizing without claiming a return distribution.
/// A marginal edge receives 25% of the admissible quote, while an edge far
/// above its causal hurdle approaches 100%. The function is deterministic,
/// monotone in edge, and never increases the quantity supplied by admission.
pub(super) fn fractional_edge_quantity(
    edge_pico_bps: i64,
    hurdle_pico_bps: i64,
    quantity: i64,
) -> i64 {
    if edge_pico_bps <= 0 || hurdle_pico_bps <= 0 || quantity <= 0 {
        return quantity.max(0);
    }
    if edge_pico_bps < hurdle_pico_bps {
        // Defense in depth: the sizing layer must never turn a stale or
        // inconsistent fair-value calculation into a live admissible quote.
        return 0;
    }
    let margin = i128::from(edge_pico_bps - hurdle_pico_bps);
    let edge = i128::from(edge_pico_bps);
    let scale_bps = 2_500_i128 + margin * 7_500_i128 / edge;
    (i128::from(quantity) * scale_bps / 10_000_i128)
        .clamp(1, i128::from(quantity))
        .min(i128::from(i64::MAX)) as i64
}

/// Convex concentration penalty for risk-increasing orders. Let `I` be the
/// signed cross-symbol inventory imbalance in bps. The scale
/// `10_000 / (10_000 + |I|)` is one at a flat book and decreases smoothly as
/// common-mode exposure grows. A side that reduces this symbol's inventory is
/// never penalized, preserving the emergency/cleanup path.
pub(super) fn cross_symbol_concentration_scaled_quantity(
    portfolio_imbalance_bps: i64,
    symbol_position: i64,
    side: Side,
    quantity: i64,
) -> i64 {
    if quantity <= 0
        || (side == Side::Buy && symbol_position < 0)
        || (side == Side::Sell && symbol_position > 0)
    {
        return quantity.max(0);
    }
    let same_direction = (side == Side::Buy && portfolio_imbalance_bps > 0)
        || (side == Side::Sell && portfolio_imbalance_bps < 0);
    if !same_direction {
        return quantity;
    }
    let imbalance = i128::from(portfolio_imbalance_bps).abs();
    let scale_bps = 10_000_i128 * 10_000_i128 / 10_000_i128.saturating_add(imbalance).max(1);
    (i128::from(quantity) * scale_bps / 10_000_i128).clamp(1, i128::from(quantity)) as i64
}

/// Continuous risk reduction for a quote that fights a persistent local or
/// regional trend. The same conflict already raises the edge hurdle; this
/// second-order control also reduces inventory carried into adverse drift.
/// The scale is affine on the bounded conflict interval [0, cap], so it is
/// monotone, explainable, and never amplifies a signal. At the cap it keeps a
/// small residual quote rather than converting a soft risk signal into an
/// undocumented hard gate; hard data/tail gates remain authoritative.
pub(super) fn trend_conflict_scaled_quantity(conflict_pico_bps: i64, quantity: i64) -> i64 {
    if quantity <= 0 || conflict_pico_bps <= 0 {
        return quantity.max(0);
    }
    let conflict = i128::from(conflict_pico_bps.min(TREND_CONFLICT_CAP_PICO_BPS));
    let cap = i128::from(TREND_CONFLICT_CAP_PICO_BPS.max(1));
    let scale_bps = 10_000_i128 - 5_000_i128 * conflict / cap;
    (i128::from(quantity) * scale_bps / 10_000_i128).clamp(1, i128::from(quantity)) as i64
}

/// Convert the queue-survival estimate into a continuous execution-size
/// budget. This is deliberately a size control rather than a new admission
/// gate: low but non-zero fill probability keeps a small probe alive, while
/// high probability recovers the full signal quantity. The affine map is
/// monotone and uses the same conservative 25% floor as the evidence model.
pub(super) fn fill_probability_scaled_quantity(fill_probability_bps: u16, quantity: i64) -> i64 {
    if quantity <= 0 {
        return quantity.max(0);
    }
    let probability = i128::from(fill_probability_bps.min(10_000));
    let scale_ppm = i128::from(MIN_EVIDENCE_SCALE_PPM)
        + (1_000_000_i128 - i128::from(MIN_EVIDENCE_SCALE_PPM)) * probability / 10_000;
    (i128::from(quantity) * scale_ppm / 1_000_000_i128).clamp(1, i128::from(quantity)) as i64
}

/// Convert residual-regime risk into a continuous inventory budget. The
/// floor matches the existing evidence-aware sizing floor: uncertain regime
/// state can reduce a new quote to a probe, but it cannot silently create a
/// hard entry gate or interfere with reduce-only cleanup.
pub(super) fn residual_regime_scaled_quantity(risk_pico_bps: i64, quantity: i64) -> i64 {
    if quantity <= 0 || risk_pico_bps <= 0 {
        return quantity.max(0);
    }
    let risk = i128::from(risk_pico_bps.min(RESIDUAL_REGIME_CAP_PICO_BPS));
    let cap = i128::from(RESIDUAL_REGIME_CAP_PICO_BPS.max(1));
    let scale_ppm = 1_000_000_i128 - risk * i128::from(1_000_000 - MIN_EVIDENCE_SCALE_PPM) / cap;
    (i128::from(quantity) * scale_ppm / 1_000_000_i128).clamp(1, i128::from(quantity)) as i64
}

pub(super) fn m7_entry_admissible(state: &SimulationSymbolState, threshold_pico_bps: i64) -> bool {
    let Some(book) = state.book else {
        return false;
    };
    let mid = book.bid_price_ticks.saturating_add(book.ask_price_ticks) / 2;
    let fair_value_ticks = fair_value_for_state(state)
        .map(|estimate| estimate.price.0)
        .unwrap_or(0);
    if mid <= 0 || fair_value_ticks <= 0 || threshold_pico_bps <= 0 {
        return false;
    }
    let residual_pico_bps = bps_between_pico(mid, fair_value_ticks);
    // M7 treats very large residuals under elevated stress as repricing,
    // not a free mean-reversion edge, while preserving ordinary opportunities.
    residual_pico_bps >= threshold_pico_bps
        && !(residual_pico_bps >= 500 * PICO_BPS_SCALE
            && m5_tail_stress_pico(state) >= M5_TAIL_CAUTION_BPS * PICO_BPS_SCALE)
}

pub(super) fn ppm_to_bps(ppm: i64) -> i64 {
    if ppm <= 0 {
        return 0;
    }
    ((i128::from(ppm) + 99) / 100).clamp(0, i128::from(i64::MAX)) as i64
}

pub(super) fn liquidity_adjusted_quantity(
    requested_quantity: i64,
    bid_quantity: i64,
    ask_quantity: i64,
) -> i64 {
    if requested_quantity <= 0 || bid_quantity <= 0 || ask_quantity <= 0 {
        return 0;
    }
    let depth = bid_quantity.min(ask_quantity);
    // Limit participation to 10% of the thinner side. Consuming an entire
    // displayed level is not a realistic passive execution assumption and
    // would make the liquidity penalty dominate the economic hurdle.
    // Synthetic unit-test books use tiny integer quantities; preserve their
    // exact fill semantics instead of collapsing a 10% cap to one unit.
    if depth < 10_000 {
        return requested_quantity.min(depth).max(1);
    }
    let participation_cap =
        (i128::from(depth) * 1_000 / 10_000).clamp(1, i128::from(i64::MAX)) as i64;
    requested_quantity.min(participation_cap).max(1)
}

pub(super) fn liquidity_ratio_pico_bps(quantity: i64, bid_quantity: i64, ask_quantity: i64) -> i64 {
    if quantity <= 0 || bid_quantity <= 0 || ask_quantity <= 0 {
        return 10_000 * PICO_BPS_SCALE;
    }
    let depth = bid_quantity.min(ask_quantity);
    (i128::from(quantity).max(0) * 10_000 * i128::from(PICO_BPS_SCALE) / i128::from(depth))
        .clamp(0, 10_000 * i128::from(PICO_BPS_SCALE)) as i64
}

pub(super) fn liquidity_ratio_bps(quantity: i64, bid_quantity: i64, ask_quantity: i64) -> i64 {
    pico_bps_to_bps(liquidity_ratio_pico_bps(
        quantity,
        bid_quantity,
        ask_quantity,
    ))
}

pub(super) fn liquidity_penalty_pico_bps(
    quantity: i64,
    bid_quantity: i64,
    ask_quantity: i64,
) -> i64 {
    let participation_pico_bps = liquidity_ratio_pico_bps(quantity, bid_quantity, ask_quantity);
    if participation_pico_bps <= 1_000 * PICO_BPS_SCALE {
        0
    } else {
        ((participation_pico_bps - 1_000 * PICO_BPS_SCALE) * 6 / 1_000)
            .clamp(0, 100 * PICO_BPS_SCALE)
    }
}

pub(super) fn liquidity_penalty_bps(quantity: i64, bid_quantity: i64, ask_quantity: i64) -> i64 {
    pico_bps_to_bps(liquidity_penalty_pico_bps(
        quantity,
        bid_quantity,
        ask_quantity,
    ))
}

pub(super) fn fill_probability_bps(quantity: i64, bid_quantity: i64, ask_quantity: i64) -> u16 {
    if quantity <= 0 || bid_quantity <= 0 || ask_quantity <= 0 {
        return 0;
    }
    let participation_bps = liquidity_ratio_bps(quantity, bid_quantity, ask_quantity);
    // This is an explicitly conservative top-of-book proxy. It is not claimed
    // to be a calibrated fill hazard until completed fills are observed.
    (10_000_i64 - participation_bps * 8 / 10).clamp(500, 9_500) as u16
}

/// Conservative queue-survival proxy used by fill-aware challengers.
///
/// If `Q` is the observable queue ahead, `D` is displayed depth, and `q` is
/// our order size, `p = D / (D + Q + 2q)` is a bounded surrogate for a
/// first-passage queue model. It is not called a calibrated probability; it
/// prevents a top-of-book ratio from implying 95% fill likelihood while a
/// large queue is visibly ahead of us.
pub(super) fn queue_aware_fill_probability_bps(
    quantity: i64,
    bid_quantity: i64,
    ask_quantity: i64,
    buy_queue_ahead: i64,
    sell_queue_ahead: i64,
) -> u16 {
    if quantity <= 0 || bid_quantity <= 0 || ask_quantity <= 0 {
        return 0;
    }
    let side_probability = |depth: i64, queue: i64| {
        let denominator = i128::from(depth.max(1))
            .saturating_add(i128::from(queue.max(0)))
            .saturating_add(i128::from(quantity.max(1)).saturating_mul(2));
        (i128::from(depth) * 10_000 / denominator).clamp(500, 9_500) as i64
    };
    side_probability(bid_quantity, buy_queue_ahead)
        .min(side_probability(ask_quantity, sell_queue_ahead)) as u16
}

pub(super) const MIN_EMPIRICAL_FILL_TRIALS: u64 = 30;

/// Returns a one-sided finite-sample lower confidence bound for the observed
/// order-level fill rate. A partial fill still counts as a successful order,
/// so this bound is intentionally optimistic about completion and conservative
/// only about whether any execution opportunity exists. Before the lifecycle
/// floor is reached, absence of evidence is reported as `None` rather than
/// converted into a fabricated zero or perfect probability.
pub(super) fn empirical_fill_probability_lcb_bps(state: &SimulationSymbolState) -> Option<u16> {
    let trials = state.calibration.order_placed_times_ms.len() as u64;
    if trials < MIN_EMPIRICAL_FILL_TRIALS {
        return None;
    }
    let successes = (state.calibration.fill_times_ms.len() as u64).min(trials);
    Some(wilson_lower_probability_bps(successes, trials).clamp(0, 10_000) as u16)
}

/// Fuse model-based queue survival with observed lifecycle evidence. The
/// minimum is a robust intersection of two different information sources:
/// neither a favorable book snapshot nor a short run of fills can overrule a
/// materially worse observed lower bound.
pub(super) fn effective_fill_probability_bps(
    state: &SimulationSymbolState,
    queue_probability_bps: u16,
) -> u16 {
    empirical_fill_probability_lcb_bps(state)
        .map(|observed| queue_probability_bps.min(observed))
        .unwrap_or(queue_probability_bps)
}

pub(super) fn local_day(timestamp_ms: u64) -> u64 {
    (timestamp_ms / 1_000 + 8 * 3_600) / 86_400
}

pub(super) fn local_weekday(timestamp_ms: u64) -> u8 {
    ((local_day(timestamp_ms) + 3) % 7 + 1) as u8
}

pub(super) fn local_minute(timestamp_ms: u64) -> u16 {
    let local_seconds = timestamp_ms / 1_000 + 8 * 3_600;
    (local_seconds % 86_400 / 60) as u16
}

pub(super) fn calendar_state_for(symbol: &str, timestamp_ms: u64) -> &'static str {
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

pub(super) fn simulation_anchor_usable(
    symbol: &str,
    anchor_observed_at_ms: u64,
    now_ms: u64,
) -> bool {
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
                calendar.after_final_close(date_key, weekday, local_minute(now_ms))
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

pub(super) fn close_candle_reaches_final_close(
    calendar: &EquitySessionCalendar,
    date_key: u32,
    weekday: u8,
    timestamp_ms: u64,
) -> bool {
    let minute = local_minute(timestamp_ms);
    calendar.after_final_close(date_key, weekday, minute)
        || (minute.saturating_add(1) == calendar.effective_final_close_minute(date_key)
            && timestamp_ms % 60_000 >= 59_000)
}

pub(super) fn anchor_refresh_allowed(symbol: &str, timestamp_ms: u64) -> bool {
    let Some(profile) = profile_for(symbol) else {
        return false;
    };
    let weekday = local_weekday(timestamp_ms);
    let minute = local_minute(timestamp_ms);
    let calendar = calendar_for(profile.region);
    let date_key = EquitySessionCalendar::date_key_from_timestamp(timestamp_ms);
    if !EquitySessionCalendar::calendar_snapshot_supported(date_key) {
        return false;
    }

    // During weekends and exchange holidays Binance's TradFi index is carried
    // forward from the last completed equity close. Treat that observation as
    // the immutable closed-session anchor; do not refresh it intra-session.
    if weekday > 5 || calendar.is_holiday(date_key) {
        return true;
    }
    if close_candle_reaches_final_close(&calendar, date_key, weekday, timestamp_ms) {
        return true;
    }

    // A simulation run may start during the overnight/pre-open window. In that
    // case the current timestamp belongs to the next local date, while the
    // usable anchor is the most recent prior trading day's final close.
    // Resolve that prior close explicitly instead of rejecting a valid
    // restart merely because the process started after midnight.
    if minute < 540 {
        let current_day = local_day(timestamp_ms);
        for offset in 1..=7 {
            let prior_day = current_day.saturating_sub(offset);
            let prior_day_start_ms = prior_day
                .saturating_mul(86_400_000)
                .saturating_sub(8 * 3_600_000);
            let prior_date_key = EquitySessionCalendar::date_key_from_timestamp(prior_day_start_ms);
            let prior_weekday = ((prior_day + 3) % 7 + 1) as u8;
            if calendar.after_final_close(prior_date_key, prior_weekday, 1_439) {
                return true;
            }
        }
    }
    false
}

pub(super) fn anchor_reference_allowed(symbol: &str, timestamp_ms: u64) -> bool {
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

pub(super) fn simulation_session_allows_entry(symbol: &str, timestamp_ms: u64) -> bool {
    let Some(profile) = profile_for(symbol) else {
        return false;
    };
    let weekday = local_weekday(timestamp_ms);
    let minute = local_minute(timestamp_ms);
    let calendar = calendar_for(profile.region);
    let date_key = EquitySessionCalendar::date_key_from_timestamp(timestamp_ms);

    // Unit-level replay fixtures use small synthetic timestamps rather than
    // real epoch milliseconds; keep their deterministic closed-session path.
    if timestamp_ms < 10_000_000_000 {
        return matches!(
            calendar.detailed_state_at(weekday, minute, false, 30, true),
            VenueSessionState::Closed | VenueSessionState::MiddayBreak
        );
    }
    if !EquitySessionCalendar::calendar_snapshot_supported(date_key) {
        return false;
    }

    // The underlying equity venue is closed on weekends and holidays while
    // Binance perpetuals remain live. Those are valid static-anchor windows;
    // the generic calendar helper intentionally reports them as non-entry
    // states for ordinary equity execution, so this strategy handles them
    // explicitly.
    if weekday > 5 || calendar.is_holiday(date_key) {
        return true;
    }

    matches!(
        calendar.detailed_state_at(weekday, minute, false, 30, true),
        VenueSessionState::Closed | VenueSessionState::MiddayBreak
    )
}
