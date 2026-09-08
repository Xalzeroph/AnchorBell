from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]


def replace_once(text: str, old: str, new: str, label: str) -> str:
    if old not in text:
        raise SystemExit(f"missing anchor: {label}")
    return text.replace(old, new, 1)


# 1) OOS domain: standard fold bundle + deterministic merge.
oos_path = ROOT / "engine" / "src" / "oos_validation.rs"
oos = oos_path.read_text(encoding="utf-8")
if "pub struct OosFoldBundle" not in oos:
    oos = replace_once(
        oos,
        "use std::cmp::Ordering;",
        "use std::{cmp::Ordering, collections::{BTreeMap, BTreeSet}};",
        "oos imports",
    )
    anchor = '''impl OosFoldMetrics {
    fn valid(&self) -> bool {
        !self.fold_id.trim().is_empty()
            && self.net_return_bps.is_finite()
            && self.max_drawdown_pct.is_finite()
            && self.max_drawdown_pct >= 0.0
            && self.fee_drag_bps.is_finite()
            && self.fee_drag_bps >= 0.0
            && self.sharpe_ratio.is_none_or(f64::is_finite)
            && self.sortino_ratio.is_none_or(f64::is_finite)
    }
}

'''
    addition = anchor + '''#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct OosFoldBundle {
    pub methodology_id: String,
    pub fold_id: String,
    pub stress: bool,
    pub candidates: BTreeMap<String, OosFoldMetrics>,
}

impl OosFoldBundle {
    pub fn validate(&self) -> Result<(), &'static str> {
        if self.methodology_id != "anchorbell-oos-fold-bundle-v1"
            || self.fold_id.trim().is_empty()
            || self.candidates.is_empty()
        {
            return Err("invalid_fold_bundle_identity");
        }
        if self.candidates.iter().any(|(candidate_id, metrics)| {
            candidate_id.trim().is_empty()
                || !metrics.valid()
                || metrics.fold_id != self.fold_id
                || metrics.stress != self.stress
        }) {
            return Err("invalid_fold_bundle_metrics");
        }
        Ok(())
    }
}

pub fn merge_fold_bundles(
    bundles: &[OosFoldBundle],
) -> Result<BTreeMap<String, Vec<OosFoldMetrics>>, &'static str> {
    if bundles.is_empty() {
        return Err("fold_bundles_required");
    }
    let mut seen_folds = BTreeSet::new();
    let mut candidates = BTreeMap::<String, Vec<OosFoldMetrics>>::new();
    for bundle in bundles {
        bundle.validate()?;
        if !seen_folds.insert(bundle.fold_id.clone()) {
            return Err("duplicate_fold_id");
        }
        for (candidate_id, metrics) in &bundle.candidates {
            candidates
                .entry(candidate_id.clone())
                .or_default()
                .push(metrics.clone());
        }
    }
    for folds in candidates.values_mut() {
        folds.sort_by(|left, right| left.fold_id.cmp(&right.fold_id));
    }
    Ok(candidates)
}

'''
    oos = replace_once(oos, anchor, addition, "fold bundle insertion")

if "merge_fold_bundles_rejects_duplicate_fold_identity" not in oos:
    test_anchor = '''    #[test]
    fn stable_candidate_passes_hard_oos_and_stress_gates() {
'''
    tests = '''    #[test]
    fn merge_fold_bundles_groups_candidates_and_rejects_duplicate_fold_identity() {
        let mut first_candidates = BTreeMap::new();
        first_candidates.insert("m7|".to_owned(), fold("o1", false, 5.0, 1.0, 1.0));
        let first = OosFoldBundle {
            methodology_id: "anchorbell-oos-fold-bundle-v1".to_owned(),
            fold_id: "o1".to_owned(),
            stress: false,
            candidates: first_candidates,
        };
        let mut second_candidates = BTreeMap::new();
        second_candidates.insert("m7|".to_owned(), fold("s1", true, -2.0, 0.2, 2.0));
        let second = OosFoldBundle {
            methodology_id: "anchorbell-oos-fold-bundle-v1".to_owned(),
            fold_id: "s1".to_owned(),
            stress: true,
            candidates: second_candidates,
        };
        let merged = merge_fold_bundles(&[first.clone(), second]).unwrap();
        assert_eq!(merged["m7|"].len(), 2);
        assert_eq!(
            merge_fold_bundles(&[first.clone(), first]).unwrap_err(),
            "duplicate_fold_id"
        );
    }

'''
    oos = replace_once(oos, test_anchor, tests + test_anchor, "fold bundle tests")

oos_path.write_text(oos, encoding="utf-8")


# 2) Batch runtime: retain final risk metrics and emit a fold bundle when requested.
batch_path = ROOT / "engine" / "src" / "simulation_batch.rs"
batch = batch_path.read_text(encoding="utf-8")

old_import = '''    orderbook::{LocalOrderBook, OrderBookError},
    runtime::{
'''
new_import = '''    oos_validation::{OosFoldBundle, OosFoldMetrics},
    orderbook::{LocalOrderBook, OrderBookError},
    runtime::{
'''
if old_import in batch:
    batch = replace_once(batch, old_import, new_import, "oos imports in batch")
elif "oos_validation::{OosFoldBundle, OosFoldMetrics}" not in batch:
    raise SystemExit("batch oos imports are neither original nor repaired")

old_engine_import = '''        AnchorSnapshot, PerformancePoint, PositionAllocation, SimulationEngine, SimulationError,
        SimulationPolicyVariant, SimulationSummary,
'''
new_engine_import = '''        AnchorSnapshot, PerformancePoint, PositionAllocation, RiskMetrics, SimulationEngine,
        SimulationError, SimulationPolicyVariant, SimulationSummary,
'''
if old_engine_import in batch:
    batch = replace_once(batch, old_engine_import, new_engine_import, "risk metrics import")
elif "PositionAllocation, RiskMetrics, SimulationEngine" not in batch:
    raise SystemExit("risk metrics import is neither original nor repaired")

if "pub validation_fold_id: Option<String>" not in batch:
    batch = replace_once(
        batch,
        '''    pub duration_secs: u64,
    /// Shared, deterministic evidence test fed exactly once per public event.
''',
        '''    pub duration_secs: u64,
    /// Optional reproducible OOS/stress fold identity. Formal folds must be finite runs.
    pub validation_fold_id: Option<String>,
    pub validation_stress: bool,
    /// Shared, deterministic evidence test fed exactly once per public event.
''',
        "batch fold config",
    )

if "pub risk_metrics: Option<RiskMetrics>" not in batch:
    batch = replace_once(
        batch,
        '''    pub summary: SimulationSummary,
    pub settlement_status: String,
''',
        '''    pub summary: SimulationSummary,
    pub risk_metrics: Option<RiskMetrics>,
    pub settlement_status: String,
''',
        "ledger risk metrics result",
    )

if "pub oos_fold_bundle: Option<OosFoldBundle>" not in batch:
    batch = replace_once(
        batch,
        '''    pub candidate_readiness: BTreeMap<String, CandidateReadinessGate>,
    pub evidence_records_written: u64,
''',
        '''    pub candidate_readiness: BTreeMap<String, CandidateReadinessGate>,
    pub oos_fold_bundle: Option<OosFoldBundle>,
    pub evidence_records_written: u64,
''',
        "batch result fold bundle",
    )

if "final_risk_metrics: Option<RiskMetrics>" not in batch:
    batch = replace_once(
        batch,
        '''    risk_history: VecDeque<PerformancePoint>,
    settlement_status: String,
''',
        '''    risk_history: VecDeque<PerformancePoint>,
    final_risk_metrics: Option<RiskMetrics>,
    settlement_status: String,
''',
        "ledger final risk field",
    )
    batch = replace_once(
        batch,
        '''            risk_history: VecDeque::with_capacity(RISK_HISTORY_CAPACITY),
            settlement_status: "not_started".to_owned(),
''',
        '''            risk_history: VecDeque::with_capacity(RISK_HISTORY_CAPACITY),
            final_risk_metrics: None,
            settlement_status: "not_started".to_owned(),
''',
        "ledger final risk init",
    )

if "validation folds require a non-empty fold id" not in batch:
    validate_anchor = '''    if config
        .specs
        .iter()
        .map(candidate_identity)
        .any(|identity| !candidate_identities.insert(identity))
    {
        return Err(SimulationError::InvalidConfig(
            "batch execution contains duplicate candidate semantics",
        ));
    }
    Ok(())
'''
    validate_replacement = '''    if config
        .specs
        .iter()
        .map(candidate_identity)
        .any(|identity| !candidate_identities.insert(identity))
    {
        return Err(SimulationError::InvalidConfig(
            "batch execution contains duplicate candidate semantics",
        ));
    }
    if let Some(fold_id) = config.validation_fold_id.as_deref() {
        if fold_id.trim().is_empty() {
            return Err(SimulationError::InvalidConfig(
                "validation folds require a non-empty fold id",
            ));
        }
        if config.duration_secs == 0 {
            return Err(SimulationError::InvalidConfig(
                "validation folds require a finite duration",
            ));
        }
        if config.position_allocations.is_none() {
            return Err(SimulationError::InvalidConfig(
                "validation folds require explicit capital allocations",
            ));
        }
    } else if config.validation_stress {
        return Err(SimulationError::InvalidConfig(
            "stress validation requires a fold id",
        ));
    }
    Ok(())
'''
    batch = replace_once(batch, validate_anchor, validate_replacement, "fold validation")

if '"validation_fold_id": config.validation_fold_id' not in batch:
    batch = replace_once(
        batch,
        '''        "duration_secs": config.duration_secs,
        "evidence": config.evidence.clone(),
''',
        '''        "duration_secs": config.duration_secs,
        "validation_fold_id": config.validation_fold_id,
        "validation_stress": config.validation_stress,
        "evidence": config.evidence.clone(),
''',
        "manifest validation fold",
    )

if "ledger.final_risk_metrics = snapshot.risk_metrics.clone();" not in batch:
    final_snapshot_anchor = '''        let snapshot = ledger.engine.metrics_snapshot_with_histories(
            observed_at,
            last_received_at_ms,
            &display,
            &risk,
        );
        write_json_atomic(&ledger.metrics_path, &snapshot).await?;
'''
    final_snapshot_replacement = '''        let snapshot = ledger.engine.metrics_snapshot_with_histories(
            observed_at,
            last_received_at_ms,
            &display,
            &risk,
        );
        ledger.final_risk_metrics = snapshot.risk_metrics.clone();
        write_json_atomic(&ledger.metrics_path, &snapshot).await?;
'''
    # Only the shutdown/final snapshot should populate the retained final metric.
    pos = batch.rfind(final_snapshot_anchor)
    if pos < 0:
        raise SystemExit("missing anchor: final risk snapshot")
    batch = batch[:pos] + final_snapshot_replacement + batch[pos + len(final_snapshot_anchor):]

if "risk_metrics: ledger.final_risk_metrics" not in batch:
    batch = replace_once(
        batch,
        '''            summary: ledger.engine.summary(),
            settlement_status: ledger.settlement_status,
''',
        '''            summary: ledger.engine.summary(),
            risk_metrics: ledger.final_risk_metrics,
            settlement_status: ledger.settlement_status,
''',
        "ledger result risk metrics",
    )

if 'join("oos-fold-bundle.json")' not in batch:
    promotion_anchor = '''    let promotion_input = SimulationPromotionInput {
        ledger_count: ledger_results.len() as u64,
'''
    fold_block = '''    let oos_fold_bundle = if let Some(fold_id) = config.validation_fold_id.as_deref() {
        let capital_ticks = config
            .position_allocations
            .as_ref()
            .map(|allocations| {
                allocations
                    .values()
                    .map(|allocation| allocation.budget_usdt_ticks)
                    .sum::<i64>()
            })
            .unwrap_or(0);
        if capital_ticks <= 0 {
            return Err(SimulationError::InvalidConfig(
                "validation fold capital must be positive",
            ));
        }
        let mut candidates = BTreeMap::new();
        for ledger in &ledger_results {
            let risk = ledger.risk_metrics.as_ref().ok_or(SimulationError::InvalidConfig(
                "validation fold requires complete risk metrics",
            ))?;
            let mut ablations = ledger.ablations.clone();
            ablations.sort();
            ablations.dedup();
            let candidate_id = format!("{}|{}", ledger.strategy_variant, ablations.join(","));
            let fee_drag_bps = (ledger.summary.fees_ticks.max(0) as f64) * 10_000.0
                / capital_ticks as f64;
            candidates.insert(
                candidate_id,
                OosFoldMetrics {
                    fold_id: fold_id.to_owned(),
                    stress: config.validation_stress,
                    net_return_bps: risk.total_return_pct * 100.0,
                    sharpe_ratio: risk.sharpe_ratio,
                    sortino_ratio: risk.sortino_ratio,
                    max_drawdown_pct: risk.max_drawdown_pct,
                    fee_drag_bps,
                    trades: ledger.summary.fill_count,
                },
            );
        }
        let bundle = OosFoldBundle {
            methodology_id: "anchorbell-oos-fold-bundle-v1".to_owned(),
            fold_id: fold_id.to_owned(),
            stress: config.validation_stress,
            candidates,
        };
        bundle
            .validate()
            .map_err(|_| SimulationError::InvalidConfig("generated validation fold is invalid"))?;
        write_json_atomic(
            &config.output_root.join("oos-fold-bundle.json"),
            &bundle,
        )
        .await?;
        Some(bundle)
    } else {
        None
    };
'''
    batch = replace_once(batch, promotion_anchor, fold_block + promotion_anchor, "fold bundle output")

if "        oos_fold_bundle,\n" not in batch:
    batch = replace_once(
        batch,
        '''        promotion_gate,
        candidate_readiness,
        evidence_records_written:''',
        '''        promotion_gate,
        candidate_readiness,
        oos_fold_bundle,
        evidence_records_written:''',
        "fold bundle result assignment",
    )

batch_path.write_text(batch, encoding="utf-8")


# 3) Batch CLI: explicit finite fold identity and stress tag.
batch_cli_path = ROOT / "engine" / "src" / "bin" / "anchorbell_simulation_batch.rs"
batch_cli = batch_cli_path.read_text(encoding="utf-8")

if "fold_id: Option<String>" not in batch_cli:
    batch_cli = replace_once(
        batch_cli,
        '''    duration_secs: u64,
    include_m9: bool,
''',
        '''    duration_secs: u64,
    fold_id: Option<String>,
    stress_fold: bool,
    include_m9: bool,
''',
        "batch cli fold args",
    )
    batch_cli = replace_once(
        batch_cli,
        '''            duration_secs: args.duration_secs,
            evidence: EvidenceConfig::default(),
''',
        '''            duration_secs: args.duration_secs,
            validation_fold_id: args.fold_id,
            validation_stress: args.stress_fold,
            evidence: EvidenceConfig::default(),
''',
        "batch config fold args",
    )
    batch_cli = replace_once(
        batch_cli,
        '''    let mut duration_secs = 0;
    let mut include_m9 = false;
''',
        '''    let mut duration_secs = 0;
    let mut fold_id = None;
    let mut stress_fold = false;
    let mut include_m9 = false;
''',
        "batch cli fold defaults",
    )
    batch_cli = replace_once(
        batch_cli,
        '''            "--duration-secs" => duration_secs = parse(&mut args, &flag)?,
            "--include-m9" => include_m9 = true,
''',
        '''            "--duration-secs" => duration_secs = parse(&mut args, &flag)?,
            "--fold-id" => fold_id = Some(next(&mut args, &flag)?),
            "--stress-fold" => stress_fold = true,
            "--include-m9" => include_m9 = true,
''',
        "batch cli fold parsing",
    )
    batch_cli = replace_once(
        batch_cli,
        '''    if symbols.is_empty() {
        return Err("--symbols cannot be empty".to_owned());
    }
    Ok(Args {
''',
        '''    if symbols.is_empty() {
        return Err("--symbols cannot be empty".to_owned());
    }
    if stress_fold && fold_id.is_none() {
        return Err("--stress-fold requires --fold-id".to_owned());
    }
    if fold_id.as_ref().is_some_and(|value| value.trim().is_empty()) {
        return Err("--fold-id cannot be empty".to_owned());
    }
    if fold_id.is_some() && duration_secs == 0 {
        return Err("--fold-id requires finite --duration-secs".to_owned());
    }
    Ok(Args {
''',
        "batch cli fold validation",
    )
    batch_cli = replace_once(
        batch_cli,
        '''        capital_usdt,
        duration_secs,
        include_m9,
''',
        '''        capital_usdt,
        duration_secs,
        fold_id,
        stress_fold,
        include_m9,
''',
        "batch cli fold return",
    )

batch_cli_path.write_text(batch_cli, encoding="utf-8")


# 4) OOS selector: accept one or more fold bundles directly; no manual JSON assembly.
selector_path = ROOT / "engine" / "src" / "bin" / "anchorbell_oos_select.rs"
selector = selector_path.read_text(encoding="utf-8")

if "enum SelectionSource" not in selector:
    selector = replace_once(
        selector,
        '''use anchorbell_engine::oos_validation::{
    compare_robust_candidates, evaluate_robust_candidate, OosFoldMetrics,
    RobustCandidateEvaluation, RobustSelectionConstraints,
};
''',
        '''use anchorbell_engine::oos_validation::{
    compare_robust_candidates, evaluate_robust_candidate, merge_fold_bundles, OosFoldBundle,
    OosFoldMetrics, RobustCandidateEvaluation, RobustSelectionConstraints,
};
''',
        "selector bundle imports",
    )
    selector = replace_once(
        selector,
        '''fn main() {
    let input = parse_args().unwrap_or_else(|message| fail(message));
    let bytes = fs::read(&input)
        .unwrap_or_else(|error| fail(format!("cannot read {}: {error}", input.display())));
    let request: SelectionInput = serde_json::from_slice(&bytes)
        .unwrap_or_else(|error| fail(format!("invalid selection input: {error}")));
''',
        '''enum SelectionSource {
    Input(PathBuf),
    Bundles(Vec<PathBuf>),
}

fn main() {
    let source = parse_args().unwrap_or_else(|message| fail(message));
    let request = match source {
        SelectionSource::Input(input) => {
            let bytes = fs::read(&input)
                .unwrap_or_else(|error| fail(format!("cannot read {}: {error}", input.display())));
            serde_json::from_slice::<SelectionInput>(&bytes)
                .unwrap_or_else(|error| fail(format!("invalid selection input: {error}")))
        }
        SelectionSource::Bundles(paths) => {
            let bundles = paths
                .iter()
                .map(|path| {
                    let bytes = fs::read(path).unwrap_or_else(|error| {
                        fail(format!("cannot read {}: {error}", path.display()))
                    });
                    serde_json::from_slice::<OosFoldBundle>(&bytes).unwrap_or_else(|error| {
                        fail(format!("invalid fold bundle {}: {error}", path.display()))
                    })
                })
                .collect::<Vec<_>>();
            let grouped = merge_fold_bundles(&bundles)
                .unwrap_or_else(|reason| fail(format!("cannot merge fold bundles: {reason}")));
            SelectionInput {
                constraints: None,
                candidates: grouped
                    .into_iter()
                    .map(|(candidate_id, folds)| CandidateInput { candidate_id, folds })
                    .collect(),
            }
        }
    };
''',
        "selector source main",
    )
    old_parse = '''fn parse_args() -> Result<PathBuf, String> {
    let mut args = env::args().skip(1);
    match (args.next().as_deref(), args.next()) {
        (Some("--input"), Some(path)) if args.next().is_none() => Ok(PathBuf::from(path)),
        (Some("--help" | "-h"), None) => {
            eprintln!("usage: anchorbell_oos_select --input CANDIDATES.json");
            process::exit(0);
        }
        _ => Err("usage: anchorbell_oos_select --input CANDIDATES.json".to_owned()),
    }
}
'''
    new_parse = '''fn parse_args() -> Result<SelectionSource, String> {
    let mut input = None;
    let mut bundles = Vec::new();
    let mut args = env::args().skip(1);
    while let Some(flag) = args.next() {
        match flag.as_str() {
            "--input" => {
                let path = args.next().ok_or("--input requires a path")?;
                input = Some(PathBuf::from(path));
            }
            "--bundle" => {
                let path = args.next().ok_or("--bundle requires a path")?;
                bundles.push(PathBuf::from(path));
            }
            "--help" | "-h" => {
                eprintln!(
                    "usage: anchorbell_oos_select --input CANDIDATES.json | --bundle FOLD.json [--bundle FOLD.json ...]"
                );
                process::exit(0);
            }
            _ => return Err(format!("unknown option {flag}")),
        }
    }
    match (input, bundles.is_empty()) {
        (Some(path), true) => Ok(SelectionSource::Input(path)),
        (None, false) => Ok(SelectionSource::Bundles(bundles)),
        _ => Err(
            "use exactly one --input or one-or-more --bundle arguments".to_owned(),
        ),
    }
}
'''
    selector = replace_once(selector, old_parse, new_parse, "selector parse args")

selector_path.write_text(selector, encoding="utf-8")
print("reproducible OOS fold pipeline repair complete")
