from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
OOS = ROOT / "engine" / "src" / "oos_validation.rs"
BATCH = ROOT / "engine" / "src" / "simulation_batch.rs"

oos = OOS.read_text(encoding="utf-8")
batch = BATCH.read_text(encoding="utf-8")

# OOS schema v3: candidate policy parameters are first-class provenance.
old_metric = '''pub struct OosFoldMetrics {
    pub fold_id: String,
    /// SHA-256 of the exact shared market-event ledger used by this fold.
    pub data_digest: String,
'''
new_metric = '''pub struct OosFoldMetrics {
    pub fold_id: String,
    /// SHA-256 of the exact shared market-event ledger used by this fold.
    pub data_digest: String,
    /// SHA-256 of candidate behavior parameters before any stress transformation.
    pub parameter_digest: String,
'''
if new_metric not in oos:
    if old_metric not in oos:
        raise SystemExit("missing anchor: OOS parameter digest field")
    oos = oos.replace(old_metric, new_metric, 1)

old_valid = '''        !self.fold_id.trim().is_empty()
            && valid_sha256_digest(&self.data_digest)
            && valid_stress_profile(self.stress, self.stress_profile.as_deref())
'''
new_valid = '''        !self.fold_id.trim().is_empty()
            && valid_sha256_digest(&self.data_digest)
            && valid_sha256_digest(&self.parameter_digest)
            && valid_stress_profile(self.stress, self.stress_profile.as_deref())
'''
if new_valid not in oos:
    if old_valid not in oos:
        raise SystemExit("missing anchor: OOS digest validation")
    oos = oos.replace(old_valid, new_valid, 1)

oos = oos.replace('"anchorbell-oos-fold-bundle-v2"', '"anchorbell-oos-fold-bundle-v3"')

old_fold = '''        OosFoldMetrics {
            fold_id: id.to_owned(),
            data_digest: format!("sha256:{digest_seed:064x}"),
            stress,
'''
new_fold = '''        OosFoldMetrics {
            fold_id: id.to_owned(),
            data_digest: format!("sha256:{digest_seed:064x}"),
            parameter_digest: format!("sha256:{:064x}", 7_u64),
            stress,
'''
if new_fold not in oos:
    if old_fold not in oos:
        raise SystemExit("missing anchor: OOS test fold constructor")
    oos = oos.replace(old_fold, new_fold, 1)

# Candidate behavior digest deliberately excludes fold/run identity and stress mutation.
anchor = '''fn candidate_identity(spec: &SimulationBatchSpec) -> String {
    let mut ablations = spec.ablations.clone();
    ablations.sort();
    ablations.dedup();
    format!("{}|{}", spec.variant.label(), ablations.join(","))
}
'''
helper = anchor + '''
fn candidate_behavior_digest(
    config: &SimulationBatchConfig,
    spec: &SimulationBatchSpec,
) -> Result<String, SimulationError> {
    let mut symbols = config.symbols.clone();
    symbols.sort();
    symbols.dedup();
    let source_semantics = config
        .specs
        .iter()
        .find(|candidate| candidate.label == config.m9_calibration_source_label)
        .map(candidate_identity);
    let mut ablations = spec.ablations.clone();
    ablations.sort();
    ablations.dedup();
    let material = serde_json::json!({
        "strategy_variant": spec.variant.label(),
        "ablations": ablations,
        "symbols": symbols,
        "m9_calibration_source_semantics": if spec.variant == SimulationPolicyVariant::M9DeadlineCausalDroMpc { source_semantics } else { None },
        "entry_threshold_bps": config.entry_threshold_bps,
        "threshold_scale_ppm": config.threshold_scale_ppm,
        "max_position": config.max_position,
        "requested_quantity": config.requested_quantity,
        "max_mark_index_gap_bps": config.max_mark_index_gap_bps,
        "max_anchor_age_ms": config.max_anchor_age_ms,
        "fee_ppm": config.fee_ppm,
        "quantity_scale": config.quantity_scale,
        "price_scale": config.price_scale,
        "position_allocations": config.position_allocations,
        "portfolio_drawdown_soft_limit_bps": config.portfolio_drawdown_soft_limit_bps,
        "portfolio_drawdown_hard_limit_bps": config.portfolio_drawdown_hard_limit_bps,
        "queue_ahead": config.queue_ahead,
        "trade_through": config.trade_through,
        "market_to_decision_ms": config.market_to_decision_ms,
        "decision_to_exchange_ms": config.decision_to_exchange_ms,
        "cancel_to_exchange_ms": config.cancel_to_exchange_ms,
        "quote_reprice_min_interval_ms": config.quote_reprice_min_interval_ms,
        "dynamic_capital_refresh_ms": config.dynamic_capital_refresh_ms,
        "depth_snapshot_limit": config.depth_snapshot_limit,
        "index_anchor_refresh_ms": config.index_anchor_refresh_ms,
        "fx_refresh_ms": config.fx_refresh_ms,
        "fx_max_age_ms": config.fx_max_age_ms,
    });
    let encoded = serde_json::to_vec(&material)
        .map_err(|_| SimulationError::InvalidConfig("cannot encode candidate behavior digest"))?;
    Ok(format!("sha256:{}", hex::encode(Sha256::digest(encoded))))
}
'''
if 'fn candidate_behavior_digest(' not in batch:
    if anchor not in batch:
        raise SystemExit("missing anchor: candidate identity")
    batch = batch.replace(anchor, helper, 1)

old_run = '''    validate(&config)?;
    apply_validation_stress_profile(&mut config)?;
    if config.policy_id.trim().is_empty() {
'''
new_run = '''    validate(&config)?;
    // Candidate identity must describe the unstressed policy so ordinary OOS
    // and its synthetic stress fold remain the same candidate.
    let candidate_parameter_digests = config
        .specs
        .iter()
        .map(|spec| Ok((candidate_identity(spec), candidate_behavior_digest(&config, spec)?)))
        .collect::<Result<BTreeMap<_, _>, SimulationError>>()?;
    apply_validation_stress_profile(&mut config)?;
    if config.policy_id.trim().is_empty() {
'''
if new_run not in batch:
    if old_run not in batch:
        raise SystemExit("missing anchor: pre-stress candidate digest")
    batch = batch.replace(old_run, new_run, 1)

old_candidate = '''            let candidate_id = format!("{}|{}", ledger.strategy_variant, ablations.join(","));
            let fee_drag_bps =
'''
new_candidate = '''            let semantics = format!("{}|{}", ledger.strategy_variant, ablations.join(","));
            let parameter_digest = candidate_parameter_digests
                .get(&semantics)
                .ok_or(SimulationError::InvalidConfig("candidate behavior digest missing"))?
                .clone();
            let candidate_id = format!("{semantics}|{parameter_digest}");
            let fee_drag_bps =
'''
if new_candidate not in batch:
    if old_candidate not in batch:
        raise SystemExit("missing anchor: candidate key provenance")
    batch = batch.replace(old_candidate, new_candidate, 1)

old_metrics = '''                    data_digest: validation_market_data_digest
                        .as_ref()
                        .expect("validation digest exists when fold id is configured")
                        .clone(),
                    stress: is_stress,
'''
new_metrics = '''                    data_digest: validation_market_data_digest
                        .as_ref()
                        .expect("validation digest exists when fold id is configured")
                        .clone(),
                    parameter_digest,
                    stress: is_stress,
'''
if new_metrics not in batch:
    if old_metrics not in batch:
        raise SystemExit("missing anchor: fold parameter provenance")
    batch = batch.replace(old_metrics, new_metrics, 1)

batch = batch.replace('"anchorbell-oos-fold-bundle-v2"', '"anchorbell-oos-fold-bundle-v3"')

# Add explicit regression: parameter mismatch cannot masquerade as one candidate.
needle = '''    #[test]
    fn candidate_fold_coverage_mismatch_is_rejected() {
'''
test = '''    #[test]
    fn candidate_parameter_digest_is_required_and_distinguishes_policy_identity() {
        let mut value = fold("o1", false, 1.0, 1.0, 1.0);
        assert!(value.valid());
        value.parameter_digest = "sha256:bad".to_owned();
        assert!(!value.valid());
    }

''' + needle
if 'fn candidate_parameter_digest_is_required_and_distinguishes_policy_identity()' not in oos:
    if needle not in oos:
        raise SystemExit("missing anchor: provenance regression test")
    oos = oos.replace(needle, test, 1)

OOS.write_text(oos, encoding="utf-8")
BATCH.write_text(batch, encoding="utf-8")
print("OOS candidate parameter provenance v3 repair applied")
