from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
OOS = ROOT / "engine" / "src" / "oos_validation.rs"

text = OOS.read_text(encoding="utf-8")

old_bundle = '''            candidate_id.trim().is_empty()
                || !metrics.valid()
                || metrics.fold_id != self.fold_id
'''
new_bundle = '''            candidate_id.trim().is_empty()
                || !metrics.valid()
                || !candidate_id.ends_with(&format!("|{}", metrics.parameter_digest))
                || metrics.fold_id != self.fold_id
'''
if new_bundle not in text:
    if old_bundle not in text:
        raise SystemExit("missing anchor: bundle candidate parameter binding")
    text = text.replace(old_bundle, new_bundle, 1)

old_coverage = '''        let mut fold_ids = BTreeSet::new();
        let mut oos_data = BTreeSet::new();
'''
new_coverage = '''        let parameter_digest = folds[0].parameter_digest.as_str();
        if folds
            .iter()
            .any(|fold| fold.parameter_digest.as_str() != parameter_digest)
        {
            return Err("candidate_parameter_digest_mismatch");
        }
        let mut fold_ids = BTreeSet::new();
        let mut oos_data = BTreeSet::new();
'''
if new_coverage not in text:
    if old_coverage not in text:
        raise SystemExit("missing anchor: cross-fold parameter digest binding")
    text = text.replace(old_coverage, new_coverage, 1)

needle = '''    #[test]
    fn candidate_fold_coverage_mismatch_is_rejected() {
'''
test = '''    #[test]
    fn parameter_digest_cannot_change_within_one_candidate() {
        let first = fold("o1", false, 1.0, 1.0, 1.0);
        let mut second = fold("o2", false, 1.0, 1.0, 1.0);
        second.parameter_digest = format!("sha256:{:064x}", 8_u64);
        let candidates = [("candidate".to_owned(), vec![first, second])]
            .into_iter()
            .collect();
        assert_eq!(
            validate_candidate_fold_coverage(&candidates).unwrap_err(),
            "candidate_parameter_digest_mismatch"
        );
    }

''' + needle
if 'fn parameter_digest_cannot_change_within_one_candidate()' not in text:
    if needle not in text:
        raise SystemExit("missing anchor: parameter binding regression")
    text = text.replace(needle, test, 1)

OOS.write_text(text, encoding="utf-8")
print("OOS candidate parameter binding hardening applied")
