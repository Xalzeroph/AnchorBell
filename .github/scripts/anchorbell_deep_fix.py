from pathlib import Path


def replace_once(path: str, old: str, new: str) -> None:
    p = Path(path)
    text = p.read_text()
    count = text.count(old)
    if count == 0:
        if new in text:
            print(f"already patched: {path}")
            return
        raise SystemExit(f"anchor not found in {path}: {old[:120]!r}")
    if count != 1:
        raise SystemExit(f"expected one anchor in {path}, found {count}")
    p.write_text(text.replace(old, new, 1))
    print(f"patched: {path}")


# 1) M8 no-funding is an actual M8 ablation identity. Runtime maps only the
# funding-disabled M8 challenger to inherited M7 behavior, which is exactly
# M8-minus-funding today while keeping experiment identity/audit semantics.
replace_once(
    "engine/src/simulation/experiment_plan.rs",
    '''        experiments.push(ExperimentSpec {\n            label: "M8_no_funding".into(),\n            strategy: "m7".into(),\n            ablations: vec!["funding".into()],\n        });''',
    '''        experiments.push(ExperimentSpec {\n            label: "M8_no_funding".into(),\n            strategy: "m8".into(),\n            ablations: vec!["funding".into()],\n        });''',
)
replace_once(
    "engine/src/simulation/experiment_plan.rs",
    '''    pub fn runtime_specs(&self) -> Result<Vec<(String, SimulationPolicyVariant)>, &'static str> {\n        self.validate()?;\n        self.experiments\n            .iter()\n            .map(|experiment| {\n                let variant = match experiment.strategy.as_str() {\n                    "m1" => SimulationPolicyVariant::M1AdaptiveRisk,\n                    "m2" => SimulationPolicyVariant::M2Microstructure,\n                    "m3" => SimulationPolicyVariant::M3FillAware,\n                    "m4" => SimulationPolicyVariant::M4Statistical,\n                    "m5" => SimulationPolicyVariant::M5Robust,\n                    "m6" => SimulationPolicyVariant::M6DynamicCapital,\n                    "m7" => SimulationPolicyVariant::M7EvidenceGated,\n                    "m8" => SimulationPolicyVariant::M8FundingAware,\n                    "m9" => SimulationPolicyVariant::M9DeadlineCausalDroMpc,\n                    _ => return Err("unknown experiment strategy"),\n                };\n                Ok((experiment.label.clone(), variant))\n            })\n            .collect()\n    }''',
    '''    pub fn runtime_specs_with_ablations(\n        &self,\n    ) -> Result<Vec<(String, SimulationPolicyVariant, Vec<String>)>, &'static str> {\n        self.validate()?;\n        self.experiments\n            .iter()\n            .map(|experiment| {\n                let funding_disabled = experiment\n                    .ablations\n                    .iter()\n                    .any(|ablation| ablation == "funding");\n                let variant = match experiment.strategy.as_str() {\n                    "m1" => SimulationPolicyVariant::M1AdaptiveRisk,\n                    "m2" => SimulationPolicyVariant::M2Microstructure,\n                    "m3" => SimulationPolicyVariant::M3FillAware,\n                    "m4" => SimulationPolicyVariant::M4Statistical,\n                    "m5" => SimulationPolicyVariant::M5Robust,\n                    "m6" => SimulationPolicyVariant::M6DynamicCapital,\n                    "m7" => SimulationPolicyVariant::M7EvidenceGated,\n                    "m8" if funding_disabled => SimulationPolicyVariant::M7EvidenceGated,\n                    "m8" => SimulationPolicyVariant::M8FundingAware,\n                    "m9" => SimulationPolicyVariant::M9DeadlineCausalDroMpc,\n                    _ => return Err("unknown experiment strategy"),\n                };\n                Ok((\n                    experiment.label.clone(),\n                    variant,\n                    experiment.ablations.clone(),\n                ))\n            })\n            .collect()\n    }\n\n    pub fn runtime_specs(&self) -> Result<Vec<(String, SimulationPolicyVariant)>, &'static str> {\n        self.runtime_specs_with_ablations().map(|specs| {\n            specs\n                .into_iter()\n                .map(|(label, variant, _)| (label, variant))\n                .collect()\n        })\n    }''',
)
replace_once(
    "engine/src/simulation/experiment_plan.rs",
    '''        assert_eq!(plan.runtime_specs().unwrap().len(), 9);''',
    '''        assert_eq!(plan.runtime_specs().unwrap().len(), 9);\n        let no_funding = plan\n            .experiments\n            .iter()\n            .find(|experiment| experiment.label == "M8_no_funding")\n            .unwrap();\n        assert_eq!(no_funding.strategy, "m8");\n        let runtime = plan.runtime_specs_with_ablations().unwrap();\n        let (_, variant, ablations) = runtime\n            .iter()\n            .find(|(label, _, _)| label == "M8_no_funding")\n            .unwrap();\n        assert_eq!(*variant, SimulationPolicyVariant::M7EvidenceGated);\n        assert_eq!(ablations, &vec!["funding".to_owned()]);''',
)

# Carry ablation metadata into the batch ledger and manifest.
replace_once(
    "engine/src/bin/anchorbell_simulation_batch.rs",
    '''        let specs = experiment_plan\n            .runtime_specs()\n            .unwrap_or_else(|error| fail(format!("invalid experiment plan: {error}")))\n            .into_iter()\n            .map(|(label, variant)| SimulationBatchSpec { label, variant })\n            .collect();''',
    '''        let specs = experiment_plan\n            .runtime_specs_with_ablations()\n            .unwrap_or_else(|error| fail(format!("invalid experiment plan: {error}")))\n            .into_iter()\n            .map(|(label, variant, ablations)| SimulationBatchSpec {\n                label,\n                variant,\n                ablations,\n            })\n            .collect();''',
)
replace_once(
    "engine/src/simulation_batch.rs",
    '''pub struct SimulationBatchSpec {\n    pub label: String,\n    pub variant: SimulationPolicyVariant,\n}''',
    '''pub struct SimulationBatchSpec {\n    pub label: String,\n    pub variant: SimulationPolicyVariant,\n    pub ablations: Vec<String>,\n}''',
)
replace_once(
    "engine/src/simulation_batch.rs",
    '''pub struct SimulationLedgerResult {\n    pub label: String,\n    pub strategy_variant: String,''',
    '''pub struct SimulationLedgerResult {\n    pub label: String,\n    pub strategy_variant: String,\n    pub ablations: Vec<String>,''',
)
replace_once(
    "engine/src/simulation_batch.rs",
    '''        "spec_labels": config.specs.iter().map(|spec| spec.label.as_str()).collect::<Vec<_>>(),\n        "symbols": config.symbols,''',
    '''        "spec_labels": config.specs.iter().map(|spec| spec.label.as_str()).collect::<Vec<_>>(),\n        "spec_ablations": config.specs.iter().map(|spec| &spec.ablations).collect::<Vec<_>>(),\n        "symbols": config.symbols,''',
)
replace_once(
    "engine/src/simulation_batch.rs",
    '''            label: ledger.spec.label,\n            strategy_variant: ledger.spec.variant.label().to_owned(),\n            evidence_record_id: evidence.evidence_id(),''',
    '''            label: ledger.spec.label,\n            strategy_variant: ledger.spec.variant.label().to_owned(),\n            ablations: ledger.spec.ablations,\n            evidence_record_id: evidence.evidence_id(),''',
)

# 2) M9 calibration: different streams have different natural sampling rates.
# Convert each stream's minimum evidence requirement into a common ESS scale
# instead of taking a raw min across heterogeneous counts.
replace_once(
    "engine/src/strategy/calibration.rs",
    '''/// A calibration is not usable merely because every field has one sample.\n/// This floor matches the minimum history required before the runtime reports\n/// risk statistics as usable and keeps M9 fail-closed during warm-up.\nconst MIN_CALIBRATION_EFFECTIVE_SAMPLE_SIZE: u64 = 30;''',
    '''/// Common effective-sample scale used in calibration reports. Individual\n/// evidence streams have different natural frequencies, so readiness is based\n/// on component-specific floors rather than the raw minimum count.\nconst MIN_CALIBRATION_EFFECTIVE_SAMPLE_SIZE: u64 = 30;\nconst MIN_MARKET_SAMPLES: u64 = 30;\nconst MIN_REVERSION_SAMPLES: u64 = 8;\nconst MIN_ORDER_LIFECYCLE_SAMPLES: u64 = 30;\nconst MIN_FILL_PARTICIPATION_SAMPLES: u64 = 10;\nconst MIN_MARKOUT_SAMPLES: u64 = 10;''',
)
replace_once(
    "engine/src/strategy/calibration.rs",
    '''        let residual_count = state.residual_abs_pico_bps.len() as u64;\n        let effective = [\n            state.return_abs_pico_bps.len(),\n            state.spread_pico_bps.len(),\n            residual_count as usize,\n            state.reversion_half_life_ms.len(),\n            state.order_wait_ms.len(),\n            state.fill_participation_bps.len(),\n            state.adverse_markout_pico_bps.len(),\n        ]\n        .into_iter()\n        .filter(|count| *count > 0)\n        .min()\n        .unwrap_or(0) as u64;''',
    '''        let residual_count = state.residual_abs_pico_bps.len() as u64;\n        let market_samples = (state.return_abs_pico_bps.len() as u64)\n            .min(state.spread_pico_bps.len() as u64)\n            .min(residual_count);\n        let reversion_samples = state.reversion_half_life_ms.len() as u64;\n        let order_lifecycle_samples = state.order_wait_ms.len() as u64;\n        let fill_participation_samples = state.fill_participation_bps.len() as u64;\n        let markout_samples = state.adverse_markout_pico_bps.len() as u64;\n        let scaled = |count: u64, required: u64| {\n            count\n                .saturating_mul(MIN_CALIBRATION_EFFECTIVE_SAMPLE_SIZE)\n                / required.max(1)\n        };\n        let effective = [\n            scaled(market_samples, MIN_MARKET_SAMPLES),\n            scaled(reversion_samples, MIN_REVERSION_SAMPLES),\n            scaled(order_lifecycle_samples, MIN_ORDER_LIFECYCLE_SAMPLES),\n            scaled(\n                fill_participation_samples,\n                MIN_FILL_PARTICIPATION_SAMPLES,\n            ),\n            scaled(markout_samples, MIN_MARKOUT_SAMPLES),\n        ]\n        .into_iter()\n        .min()\n        .unwrap_or(0);\n        let component_history_ready = market_samples >= MIN_MARKET_SAMPLES\n            && reversion_samples >= MIN_REVERSION_SAMPLES\n            && order_lifecycle_samples >= MIN_ORDER_LIFECYCLE_SAMPLES\n            && fill_participation_samples >= MIN_FILL_PARTICIPATION_SAMPLES\n            && markout_samples >= MIN_MARKOUT_SAMPLES;''',
)
replace_once(
    "engine/src/strategy/calibration.rs",
    '''        if effective < MIN_CALIBRATION_EFFECTIVE_SAMPLE_SIZE {\n            missing.push("effective_sample_size");\n        }''',
    '''        if market_samples < MIN_MARKET_SAMPLES {\n            missing.push("market_samples");\n        }\n        if reversion_samples < MIN_REVERSION_SAMPLES {\n            missing.push("reversion_samples");\n        }\n        if order_lifecycle_samples < MIN_ORDER_LIFECYCLE_SAMPLES {\n            missing.push("order_lifecycle_samples");\n        }\n        if fill_participation_samples < MIN_FILL_PARTICIPATION_SAMPLES {\n            missing.push("fill_participation_samples");\n        }\n        if markout_samples < MIN_MARKOUT_SAMPLES {\n            missing.push("markout_samples");\n        }\n        if !component_history_ready {\n            missing.push("effective_sample_size");\n        }''',
)
replace_once(
    "engine/src/strategy/calibration.rs",
    '''            ) if fill_hazard > 0 && effective >= MIN_CALIBRATION_EFFECTIVE_SAMPLE_SIZE => {''',
    '''            ) if fill_hazard > 0 && component_history_ready => {''',
)
replace_once(
    "engine/src/strategy/calibration.rs",
    '''    #[test]\n    fn one_fill_is_not_enough_to_calibrate() {''',
    '''    #[test]\n    fn heterogeneous_stream_rates_can_calibrate_safely() {\n        let mut state = CalibrationState::new("TESTUSDT");\n        for time in 1..=30 {\n            state.observe_market(\n                time,\n                Some(2_000_000_000_000),\n                Some(1_000_000_000_000),\n                Some(if time % 2 == 0 {\n                    4_000_000_000_000\n                } else {\n                    8_000_000_000_000\n                }),\n            );\n        }\n        for index in 0..30 {\n            let placed_at = 100 + index * 3;\n            state.observe_order_placed(placed_at);\n            if index < 10 {\n                state.observe_fill(placed_at + 1, placed_at, 10, 100);\n                state.observe_markout(placed_at + 2, 1_000_000_000_000);\n            }\n            state.observe_order_terminal(placed_at + 1, placed_at);\n        }\n        let snapshot = state.snapshot(2_000_000_000_000);\n        assert_eq!(snapshot.status, CalibrationStatus::Calibrated);\n        assert!(snapshot.effective_sample_size >= MIN_CALIBRATION_EFFECTIVE_SAMPLE_SIZE);\n    }\n\n    #[test]\n    fn one_fill_is_not_enough_to_calibrate() {''',
)

# 3) M4: preserve max(structural hurdle, empirical hurdle) architecture, but
# build a complete empirical LCB hurdle instead of comparing the structural
# sum against volatility alone (which was mathematically dominated in practice).
replace_once(
    "engine/src/simulation/runtime.rs",
    '''    let statistical_pico_bps = if variant.uses_statistical_term() {\n        state.ewma_abs_return_pico_bps.saturating_mul(8)\n    } else {\n        0\n    };''',
    '''    let statistical_pico_bps = if variant.uses_statistical_term() {\n        uncertainty_pico_bps\n            .saturating_add(cost_pico_bps)\n            .saturating_add(spread_pico_bps)\n            .saturating_add(adverse_selection_pico_bps)\n            .saturating_add(state.ewma_abs_return_pico_bps.saturating_mul(8))\n    } else {\n        0\n    };''',
)

# 4) M9 warm start from the declared F3 source during the same continuous run.
# Seeds are applied only while the target remains uncalibrated; once M9 is
# calibrated it owns and evolves its local state without periodic overwrite.
replace_once(
    "engine/src/simulation/runtime.rs",
    '''    pub fn restore_calibration_states(&mut self, seeds: &BTreeMap<String, CalibrationState>) {\n        for (symbol, seed) in seeds {\n            if seed.instrument != symbol.as_str() {\n                continue;\n            }\n            if let Some(state) = self.states.get_mut(symbol) {\n                state.calibration = seed.clone();\n            }\n        }\n    }''',
    '''    pub fn restore_calibration_states(&mut self, seeds: &BTreeMap<String, CalibrationState>) {\n        for (symbol, seed) in seeds {\n            if seed.instrument != symbol.as_str() {\n                continue;\n            }\n            if let Some(state) = self.states.get_mut(symbol) {\n                state.calibration = seed.clone();\n            }\n        }\n    }\n\n    pub fn restore_calibration_states_if_unavailable(\n        &mut self,\n        seeds: &BTreeMap<String, CalibrationState>,\n    ) {\n        for (symbol, seed) in seeds {\n            if seed.instrument != symbol.as_str()\n                || seed.snapshot(0).calibration.is_none()\n            {\n                continue;\n            }\n            if let Some(state) = self.states.get_mut(symbol) {\n                if state.calibration.snapshot(0).calibration.is_none() {\n                    state.calibration = seed.clone();\n                }\n            }\n        }\n    }''',
)
replace_once(
    "engine/src/simulation_batch.rs",
    '''async fn persist_calibration_store(path: &Path, ledgers: &[Ledger]) -> Result<(), SimulationError> {\n    let mut snapshots = BTreeMap::<String, CalibrationSnapshot>::new();''',
    '''fn warm_start_m9_from_source(ledgers: &mut [Ledger]) {\n    let seeds = ledgers\n        .iter()\n        .find(|ledger| ledger.spec.label == M9_CALIBRATION_SOURCE_LABEL)\n        .map(|ledger| {\n            ledger\n                .engine\n                .calibration_snapshots(0)\n                .into_iter()\n                .filter_map(|(symbol, snapshot)| {\n                    snapshot.calibration.map(|_| (symbol, snapshot.state))\n                })\n                .collect::<BTreeMap<_, _>>()\n        })\n        .unwrap_or_default();\n    if seeds.is_empty() {\n        return;\n    }\n    for ledger in ledgers\n        .iter_mut()\n        .filter(|ledger| ledger.spec.variant == SimulationPolicyVariant::M9DeadlineCausalDroMpc)\n    {\n        ledger\n            .engine\n            .restore_calibration_states_if_unavailable(&seeds);\n    }\n}\n\nasync fn persist_calibration_store(path: &Path, ledgers: &[Ledger]) -> Result<(), SimulationError> {\n    let mut snapshots = BTreeMap::<String, CalibrationSnapshot>::new();''',
)
replace_once(
    "engine/src/simulation_batch.rs",
    '''                    write_json_atomic(&evidence_summary_path, &evidence.summary()).await?;\n                    for ledger in &mut ledgers {''',
    '''                    write_json_atomic(&evidence_summary_path, &evidence.summary()).await?;\n                    warm_start_m9_from_source(&mut ledgers);\n                    for ledger in &mut ledgers {''',
)

# 5) Decouple the compact JSON history from the longer risk-statistics window.
# 900 visible points remain for observability; 7201 points allow >= 30 independent
# 30-second returns without exploding metrics.json size.
replace_once(
    "engine/src/simulation/runtime.rs",
    '''    pub fn metrics_snapshot_with_history(\n        &self,\n        observed_at_ms: u64,\n        last_received_at_ms: u64,\n        history: &[PerformancePoint],\n    ) -> MetricsSnapshot {\n        let mut snapshot = self.metrics_snapshot(observed_at_ms, last_received_at_ms);\n        snapshot.history = history.to_vec();\n        if let Some(capital_ticks) = snapshot.capital_usdt_ticks.filter(|capital| *capital > 0) {\n            let portfolio_points = history\n                .iter()\n                .map(|point| (point.observed_at_ms, point.net_pnl_ticks))\n                .collect::<Vec<_>>();\n            snapshot.risk_metrics = Some(calculate_risk_metrics(&portfolio_points, capital_ticks));\n\n            let mut symbol_points = BTreeMap::<String, Vec<(u64, i64)>>::new();\n            for point in history {\n                for symbol_point in &point.symbols {\n                    symbol_points\n                        .entry(symbol_point.symbol.clone())\n                        .or_default()\n                        .push((point.observed_at_ms, symbol_point.net_pnl_ticks));\n                }\n            }\n            for symbol in &mut snapshot.symbols {\n                let points = symbol_points\n                    .get(&symbol.symbol)\n                    .cloned()\n                    .unwrap_or_default();\n                let symbol_capital = symbol\n                    .allocated_capital_usdt_ticks\n                    .filter(|capital| *capital > 0)\n                    .unwrap_or(capital_ticks);\n                symbol.risk_metrics = Some(calculate_risk_metrics(&points, symbol_capital));\n            }\n        }\n        snapshot\n    }''',
    '''    pub fn metrics_snapshot_with_history(\n        &self,\n        observed_at_ms: u64,\n        last_received_at_ms: u64,\n        history: &[PerformancePoint],\n    ) -> MetricsSnapshot {\n        self.metrics_snapshot_with_histories(\n            observed_at_ms,\n            last_received_at_ms,\n            history,\n            history,\n        )\n    }\n\n    pub fn metrics_snapshot_with_histories(\n        &self,\n        observed_at_ms: u64,\n        last_received_at_ms: u64,\n        display_history: &[PerformancePoint],\n        risk_history: &[PerformancePoint],\n    ) -> MetricsSnapshot {\n        let mut snapshot = self.metrics_snapshot(observed_at_ms, last_received_at_ms);\n        snapshot.history = display_history.to_vec();\n        if let Some(capital_ticks) = snapshot.capital_usdt_ticks.filter(|capital| *capital > 0) {\n            let portfolio_points = risk_history\n                .iter()\n                .map(|point| (point.observed_at_ms, point.net_pnl_ticks))\n                .collect::<Vec<_>>();\n            snapshot.risk_metrics = Some(calculate_risk_metrics(&portfolio_points, capital_ticks));\n\n            let mut symbol_points = BTreeMap::<String, Vec<(u64, i64)>>::new();\n            for point in risk_history {\n                for symbol_point in &point.symbols {\n                    symbol_points\n                        .entry(symbol_point.symbol.clone())\n                        .or_default()\n                        .push((point.observed_at_ms, symbol_point.net_pnl_ticks));\n                }\n            }\n            for symbol in &mut snapshot.symbols {\n                let points = symbol_points\n                    .get(&symbol.symbol)\n                    .cloned()\n                    .unwrap_or_default();\n                let symbol_capital = symbol\n                    .allocated_capital_usdt_ticks\n                    .filter(|capital| *capital > 0)\n                    .unwrap_or(capital_ticks);\n                symbol.risk_metrics = Some(calculate_risk_metrics(&points, symbol_capital));\n            }\n        }\n        snapshot\n    }''',
)
replace_once(
    "engine/src/simulation/runtime.rs",
    '''    let mut performance_history = VecDeque::with_capacity(900);''',
    '''    let mut performance_history = VecDeque::with_capacity(DISPLAY_HISTORY_CAPACITY);\n    let mut risk_history = VecDeque::with_capacity(RISK_HISTORY_CAPACITY);''',
)
replace_once(
    "engine/src/simulation/runtime.rs",
    '''                        performance_history.push_back(engine.performance_point(observed_at_ms));\n                        while performance_history.len() > 900 {\n                            performance_history.pop_front();\n                        }\n                        write_json_atomic(\n                            path,\n                            &engine.metrics_snapshot_with_history(\n                                observed_at_ms,\n                                last_received_at_ms,\n                                performance_history.make_contiguous(),\n                            ),\n                        ).await?;''',
    '''                        let point = engine.performance_point(observed_at_ms);\n                        performance_history.push_back(point.clone());\n                        risk_history.push_back(point);\n                        while performance_history.len() > DISPLAY_HISTORY_CAPACITY {\n                            performance_history.pop_front();\n                        }\n                        while risk_history.len() > RISK_HISTORY_CAPACITY {\n                            risk_history.pop_front();\n                        }\n                        let display = performance_history.make_contiguous().to_vec();\n                        let risk = risk_history.make_contiguous().to_vec();\n                        write_json_atomic(\n                            path,\n                            &engine.metrics_snapshot_with_histories(\n                                observed_at_ms,\n                                last_received_at_ms,\n                                &display,\n                                &risk,\n                            ),\n                        ).await?;''',
)
replace_once(
    "engine/src/simulation/runtime.rs",
    '''        performance_history.push_back(engine.performance_point(observed_at_ms));\n        while performance_history.len() > 900 {\n            performance_history.pop_front();\n        }\n        write_json_atomic(\n            path,\n            &engine.metrics_snapshot_with_history(\n                observed_at_ms,\n                last_received_at_ms,\n                performance_history.make_contiguous(),\n            ),\n        )''',
    '''        let point = engine.performance_point(observed_at_ms);\n        performance_history.push_back(point.clone());\n        risk_history.push_back(point);\n        while performance_history.len() > DISPLAY_HISTORY_CAPACITY {\n            performance_history.pop_front();\n        }\n        while risk_history.len() > RISK_HISTORY_CAPACITY {\n            risk_history.pop_front();\n        }\n        let display = performance_history.make_contiguous().to_vec();\n        let risk = risk_history.make_contiguous().to_vec();\n        write_json_atomic(\n            path,\n            &engine.metrics_snapshot_with_histories(\n                observed_at_ms,\n                last_received_at_ms,\n                &display,\n                &risk,\n            ),\n        )''',
)
replace_once(
    "engine/src/simulation/runtime.rs",
    '''const RISK_SAMPLE_INTERVAL_MS: u64 = 30_000;''',
    '''const DISPLAY_HISTORY_CAPACITY: usize = 900;\nconst RISK_HISTORY_CAPACITY: usize = 7_201;\nconst RISK_SAMPLE_INTERVAL_MS: u64 = 30_000;\nconst MIN_RISK_RETURN_SAMPLES: usize = 30;''',
)
replace_once(
    "engine/src/simulation/runtime.rs",
    '''        status: if sample_count >= 30 {''',
    '''        status: if sample_count >= MIN_RISK_RETURN_SAMPLES {''',
)
replace_once(
    "engine/src/simulation/runtime.rs",
    '''        sharpe_ratio: (sample_count >= 30 && standard_deviation > 0.0)''',
    '''        sharpe_ratio: (sample_count >= MIN_RISK_RETURN_SAMPLES && standard_deviation > 0.0)''',
)
replace_once(
    "engine/src/simulation/runtime.rs",
    '''        sortino_ratio: (sample_count >= 30 && downside_deviation > 0.0)''',
    '''        sortino_ratio: (sample_count >= MIN_RISK_RETURN_SAMPLES && downside_deviation > 0.0)''',
)

replace_once(
    "engine/src/simulation_batch.rs",
    '''const M9_CALIBRATION_SOURCE_LABEL: &str = "F3_m3";''',
    '''const M9_CALIBRATION_SOURCE_LABEL: &str = "F3_m3";\nconst DISPLAY_HISTORY_CAPACITY: usize = 900;\nconst RISK_HISTORY_CAPACITY: usize = 7_201;''',
)
replace_once(
    "engine/src/simulation_batch.rs",
    '''    metrics_path: PathBuf,\n    history: VecDeque<PerformancePoint>,\n    settlement_status: String,''',
    '''    metrics_path: PathBuf,\n    history: VecDeque<PerformancePoint>,\n    risk_history: VecDeque<PerformancePoint>,\n    settlement_status: String,''',
)
replace_once(
    "engine/src/simulation_batch.rs",
    '''            metrics_path: dir.join("metrics.json"),\n            history: VecDeque::with_capacity(900),\n            settlement_status: "not_started".to_owned(),''',
    '''            metrics_path: dir.join("metrics.json"),\n            history: VecDeque::with_capacity(DISPLAY_HISTORY_CAPACITY),\n            risk_history: VecDeque::with_capacity(RISK_HISTORY_CAPACITY),\n            settlement_status: "not_started".to_owned(),''',
)
replace_once(
    "engine/src/simulation_batch.rs",
    '''                    for ledger in &mut ledgers {\n                        ledger.history.push_back(ledger.engine.performance_point(observed_at));\n                        while ledger.history.len() > 900 { ledger.history.pop_front(); }\n                        let snapshot = ledger.engine.metrics_snapshot_with_history(observed_at, last_received_at_ms, ledger.history.make_contiguous());\n                        write_json_atomic(&ledger.metrics_path, &snapshot).await?;\n                    }''',
    '''                    for ledger in &mut ledgers {\n                        let point = ledger.engine.performance_point(observed_at);\n                        ledger.history.push_back(point.clone());\n                        ledger.risk_history.push_back(point);\n                        while ledger.history.len() > DISPLAY_HISTORY_CAPACITY {\n                            ledger.history.pop_front();\n                        }\n                        while ledger.risk_history.len() > RISK_HISTORY_CAPACITY {\n                            ledger.risk_history.pop_front();\n                        }\n                        let display = ledger.history.make_contiguous().to_vec();\n                        let risk = ledger.risk_history.make_contiguous().to_vec();\n                        let snapshot = ledger.engine.metrics_snapshot_with_histories(\n                            observed_at,\n                            last_received_at_ms,\n                            &display,\n                            &risk,\n                        );\n                        write_json_atomic(&ledger.metrics_path, &snapshot).await?;\n                    }''',
)
replace_once(
    "engine/src/simulation_batch.rs",
    '''        ledger\n            .history\n            .push_back(ledger.engine.performance_point(observed_at));\n        while ledger.history.len() > 900 {\n            ledger.history.pop_front();\n        }\n        let snapshot = ledger.engine.metrics_snapshot_with_history(\n            observed_at,\n            last_received_at_ms,\n            ledger.history.make_contiguous(),\n        );''',
    '''        let point = ledger.engine.performance_point(observed_at);\n        ledger.history.push_back(point.clone());\n        ledger.risk_history.push_back(point);\n        while ledger.history.len() > DISPLAY_HISTORY_CAPACITY {\n            ledger.history.pop_front();\n        }\n        while ledger.risk_history.len() > RISK_HISTORY_CAPACITY {\n            ledger.risk_history.pop_front();\n        }\n        let display = ledger.history.make_contiguous().to_vec();\n        let risk = ledger.risk_history.make_contiguous().to_vec();\n        let snapshot = ledger.engine.metrics_snapshot_with_histories(\n            observed_at,\n            last_received_at_ms,\n            &display,\n            &risk,\n        );''',
)

print("AnchorBell deep-fix transformation complete")
