from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]


def replace_once(text: str, old: str, new: str, label: str) -> str:
    if old not in text:
        raise SystemExit(f"missing anchor: {label}")
    return text.replace(old, new, 1)


analytics_path = ROOT / "engine" / "src" / "analytics_validation.rs"
analytics = analytics_path.read_text(encoding="utf-8")

if "pub struct CandidateReadinessGate" not in analytics:
    anchor = '''#[derive(Debug, Clone, Serialize)]
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

'''
    addition = anchor + '''/// Candidate-scoped prequalification for the final OOS/stress selector.
/// A Supported verdict here means only "ready for OOS validation"; it is not
/// a deployment or production-promotion decision.
#[derive(Debug, Clone, Serialize)]
pub struct CandidateReadinessGate {
    pub methodology_id: String,
    pub minimum_fills: u64,
    pub integrity_passed: bool,
    pub evidence_sufficient: bool,
    pub economic_passed: bool,
    pub survival_passed: bool,
    pub ready_for_oos_validation: bool,
    pub verdict: ValidationVerdict,
    pub reason: String,
}

pub fn evaluate_candidate_readiness(input: SimulationPromotionInput) -> CandidateReadinessGate {
    const MINIMUM_FILLS: u64 = 100;
    if input.ledger_count != 1 {
        return CandidateReadinessGate {
            methodology_id: "anchorbell-candidate-readiness-v1".to_owned(),
            minimum_fills: MINIMUM_FILLS,
            integrity_passed: false,
            evidence_sufficient: false,
            economic_passed: false,
            survival_passed: false,
            ready_for_oos_validation: false,
            verdict: ValidationVerdict::Indeterminate,
            reason: "candidate_scope_must_contain_exactly_one_ledger".to_owned(),
        };
    }
    let integrity_passed = input.records_dropped == 0;
    let evidence_sufficient = input.fills >= MINIMUM_FILLS && input.orders >= input.fills;
    let economic_passed = evidence_sufficient && input.total_net_pnl_ticks > 0;
    let survival_passed =
        integrity_passed && input.valuation_incomplete_ledgers == 0 && input.non_flat_ledgers == 0;
    let ready_for_oos_validation =
        integrity_passed && evidence_sufficient && economic_passed && survival_passed;
    let verdict = if ready_for_oos_validation {
        ValidationVerdict::Supported
    } else if integrity_passed && evidence_sufficient && (!economic_passed || !survival_passed) {
        ValidationVerdict::Falsified
    } else {
        ValidationVerdict::Indeterminate
    };
    let reason = if !integrity_passed {
        "data_integrity_failed"
    } else if !evidence_sufficient {
        "insufficient_fill_evidence"
    } else if !economic_passed {
        "net_pnl_not_positive"
    } else if !survival_passed {
        "open_risk_or_incomplete_valuation"
    } else {
        "ready_for_oos_validation"
    }
    .to_owned();
    CandidateReadinessGate {
        methodology_id: "anchorbell-candidate-readiness-v1".to_owned(),
        minimum_fills: MINIMUM_FILLS,
        integrity_passed,
        evidence_sufficient,
        economic_passed,
        survival_passed,
        ready_for_oos_validation,
        verdict,
        reason,
    }
}

'''
    analytics = replace_once(analytics, anchor, addition, "candidate readiness insertion")

analytics = analytics.replace("anchorbell-automatic-promotion-v2", "anchorbell-automatic-promotion-v3")

old_verdict = '''    let verdict = if integrity_passed && evidence_sufficient && economic_passed && survival_passed {
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
'''
new_verdict = '''    let readiness = evaluate_candidate_readiness(input);
    let verdict = if readiness.verdict == ValidationVerdict::Falsified {
        ValidationVerdict::Falsified
    } else {
        // One in-sample run can prequalify a candidate, but it cannot promote it.
        // Final selection must be made by the OOS/stress candidate selector.
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
        "oos_validation_required".to_owned()
    };
'''
if old_verdict in analytics:
    analytics = replace_once(analytics, old_verdict, new_verdict, "automatic promotion OOS requirement")
elif "let readiness = evaluate_candidate_readiness(input);" not in analytics:
    raise SystemExit("automatic promotion verdict is neither original nor repaired")

if "single_ledger_positive_run_still_requires_oos_validation" not in analytics:
    test_anchor = '''    #[test]
    fn incomplete_verdict_is_indeterminate() {
'''
    tests = '''    #[test]
    fn candidate_readiness_is_scoped_and_prequalifies_only_one_ledger() {
        let ready = evaluate_candidate_readiness(SimulationPromotionInput {
            ledger_count: 1,
            orders: 200,
            fills: 120,
            records_dropped: 0,
            valuation_incomplete_ledgers: 0,
            non_flat_ledgers: 0,
            total_net_pnl_ticks: 10,
        });
        assert!(ready.ready_for_oos_validation);
        assert_eq!(ready.verdict, ValidationVerdict::Supported);
        let mixed = evaluate_candidate_readiness(SimulationPromotionInput {
            ledger_count: 2,
            orders: 400,
            fills: 240,
            records_dropped: 0,
            valuation_incomplete_ledgers: 0,
            non_flat_ledgers: 0,
            total_net_pnl_ticks: 20,
        });
        assert!(!mixed.ready_for_oos_validation);
        assert_eq!(mixed.verdict, ValidationVerdict::Indeterminate);
    }

    #[test]
    fn single_ledger_positive_run_still_requires_oos_validation() {
        let gate = evaluate_simulation_promotion(SimulationPromotionInput {
            ledger_count: 1,
            orders: 200,
            fills: 120,
            records_dropped: 0,
            valuation_incomplete_ledgers: 0,
            non_flat_ledgers: 0,
            total_net_pnl_ticks: 10,
        });
        assert_eq!(gate.verdict, ValidationVerdict::Indeterminate);
        assert_eq!(gate.reason, "oos_validation_required");
        assert!(gate.economic_passed);
        assert!(gate.survival_passed);
    }

'''
    analytics = replace_once(analytics, test_anchor, tests + test_anchor, "promotion tests")

analytics_path.write_text(analytics, encoding="utf-8")

batch_path = ROOT / "engine" / "src" / "simulation_batch.rs"
batch = batch_path.read_text(encoding="utf-8")

batch = batch.replace(
    "collections::{BTreeMap, VecDeque}",
    "collections::{BTreeMap, BTreeSet, VecDeque}",
    1,
)

old_import = '''    analytics_validation::{
        evaluate_simulation_promotion, SimulationPromotionGate, SimulationPromotionInput,
        ValidationSummary,
    },
'''
new_import = '''    analytics_validation::{
        evaluate_candidate_readiness, evaluate_simulation_promotion, CandidateReadinessGate,
        SimulationPromotionGate, SimulationPromotionInput, ValidationSummary,
    },
'''
if old_import in batch:
    batch = replace_once(batch, old_import, new_import, "candidate readiness imports")
elif "CandidateReadinessGate" not in batch:
    raise SystemExit("candidate readiness imports are neither original nor repaired")

if "pub candidate_readiness: BTreeMap<String, CandidateReadinessGate>" not in batch:
    batch = replace_once(
        batch,
        "    pub promotion_gate: SimulationPromotionGate,\n",
        "    pub promotion_gate: SimulationPromotionGate,\n    pub candidate_readiness: BTreeMap<String, CandidateReadinessGate>,\n",
        "batch result candidate readiness field",
    )

if "fn candidate_identity(spec: &SimulationBatchSpec)" not in batch:
    validate_anchor = "fn validate(config: &SimulationBatchConfig) -> Result<(), SimulationError> {\n"
    helper = '''fn candidate_identity(spec: &SimulationBatchSpec) -> String {
    let mut ablations = spec.ablations.clone();
    ablations.sort();
    ablations.dedup();
    format!("{}|{}", spec.variant.label(), ablations.join(","))
}

'''
    batch = replace_once(batch, validate_anchor, helper + validate_anchor, "candidate identity helper")

old_label_validation = '''    if config.specs.iter().any(|spec| spec.label.trim().is_empty()) {
        return Err(SimulationError::InvalidConfig(
            "batch execution labels must be non-empty",
        ));
    }
    if config
        .specs
        .windows(2)
        .any(|pair| pair[0].label == pair[1].label)
    {
        return Err(SimulationError::InvalidConfig(
            "batch execution labels must be unique",
        ));
    }
'''
new_label_validation = '''    let mut labels = BTreeSet::new();
    if config
        .specs
        .iter()
        .any(|spec| spec.label.trim().is_empty() || !labels.insert(spec.label.as_str()))
    {
        return Err(SimulationError::InvalidConfig(
            "batch execution labels must be non-empty and unique",
        ));
    }
    let mut candidate_identities = BTreeSet::new();
    if config
        .specs
        .iter()
        .map(candidate_identity)
        .any(|identity| !candidate_identities.insert(identity))
    {
        return Err(SimulationError::InvalidConfig(
            "batch execution contains duplicate candidate semantics",
        ));
    }
'''
if old_label_validation in batch:
    batch = replace_once(batch, old_label_validation, new_label_validation, "batch candidate uniqueness")
elif "duplicate candidate semantics" not in batch:
    raise SystemExit("candidate uniqueness validation is neither original nor repaired")

if "candidate-readiness.json" not in batch:
    promotion_anchor = '''    let promotion_input = SimulationPromotionInput {
        ledger_count: ledger_results.len() as u64,
'''
    candidate_block = '''    let candidate_readiness = ledger_results
        .iter()
        .map(|ledger| {
            let input = SimulationPromotionInput {
                ledger_count: 1,
                orders: ledger.summary.order_count,
                fills: ledger.summary.fill_count,
                records_dropped: ledger.records_dropped,
                valuation_incomplete_ledgers: if ledger.summary.unrealized_valuation_complete {
                    0
                } else {
                    1
                },
                non_flat_ledgers: if ledger.summary.flat_at_end { 0 } else { 1 },
                total_net_pnl_ticks: ledger.summary.net_pnl_ticks,
            };
            (ledger.label.clone(), evaluate_candidate_readiness(input))
        })
        .collect::<BTreeMap<_, _>>();
    write_json_atomic(
        &config.output_root.join("candidate-readiness.json"),
        &candidate_readiness,
    )
    .await?;
'''
    batch = replace_once(batch, promotion_anchor, candidate_block + promotion_anchor, "candidate readiness output")

if "        candidate_readiness,\n" not in batch:
    batch = replace_once(
        batch,
        "        promotion_gate,\n        evidence_records_written:",
        "        promotion_gate,\n        candidate_readiness,\n        evidence_records_written:",
        "candidate readiness result assignment",
    )

if "candidate_identity_normalizes_ablation_order_and_ignores_label" not in batch:
    old_test_import = "    use super::calibration_key;\n"
    new_test_import = '''    use super::{candidate_identity, calibration_key, SimulationBatchSpec};
    use crate::simulation::engine::SimulationPolicyVariant;
'''
    batch = replace_once(batch, old_test_import, new_test_import, "simulation batch test imports")
    test_end = '''    fn calibration_store_keys_are_strategy_scoped() {
        assert_eq!(calibration_key("F3_m3", "CXMTUSDT"), "F3_m3::CXMTUSDT");
        assert_ne!(
            calibration_key("F3_m3", "CXMTUSDT"),
            calibration_key("F4_m4", "CXMTUSDT")
        );
    }
'''
    replacement = test_end + '''
    #[test]
    fn candidate_identity_normalizes_ablation_order_and_ignores_label() {
        let left = SimulationBatchSpec {
            label: "candidate-a".to_owned(),
            variant: SimulationPolicyVariant::M8FundingAware,
            ablations: vec!["funding".to_owned(), "tail".to_owned()],
        };
        let right = SimulationBatchSpec {
            label: "candidate-b".to_owned(),
            variant: SimulationPolicyVariant::M8FundingAware,
            ablations: vec!["tail".to_owned(), "funding".to_owned(), "funding".to_owned()],
        };
        assert_eq!(candidate_identity(&left), candidate_identity(&right));
    }
'''
    batch = replace_once(batch, test_end, replacement, "candidate identity test")

batch_path.write_text(batch, encoding="utf-8")
print("candidate-level promotion repair complete")
