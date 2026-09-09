use std::{collections::BTreeSet, env, fs, path::PathBuf, process};

use anchorbell_engine::{
    oos_validation::{
        merge_fold_bundles, validate_candidate_fold_coverage, OosFoldBundle, OosFoldMetrics,
    },
    promotion_policy::{
        compare_risk_adjusted_candidates, RiskAdjustedCandidateEvaluation,
        RiskAdjustedPromotionPolicy,
    },
};
use serde::{Deserialize, Serialize};

#[derive(Debug, Deserialize)]
struct CandidateInput {
    candidate_id: String,
    folds: Vec<OosFoldMetrics>,
}

#[derive(Debug, Deserialize)]
struct SelectionInput {
    candidates: Vec<CandidateInput>,
}

#[derive(Debug, Serialize)]
struct CandidateOutput {
    candidate_id: String,
    evaluation: RiskAdjustedCandidateEvaluation,
}

#[derive(Debug, Serialize)]
struct SelectionOutput {
    methodology_id: &'static str,
    policy_id: String,
    policy_digest: String,
    best_candidate_id: Option<String>,
    candidates: Vec<CandidateOutput>,
}

enum SelectionSource {
    Input(PathBuf),
    Bundles(Vec<PathBuf>),
}

fn main() {
    let (source, policy_path) = parse_args().unwrap_or_else(|message| fail(message));
    let policy =
        RiskAdjustedPromotionPolicy::load(&policy_path).unwrap_or_else(|message| fail(message));

    let candidates = match source {
        SelectionSource::Input(input) => {
            let bytes = fs::read(&input)
                .unwrap_or_else(|error| fail(format!("cannot read {}: {error}", input.display())));
            serde_json::from_slice::<SelectionInput>(&bytes)
                .unwrap_or_else(|error| fail(format!("invalid selection input: {error}")))
                .candidates
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
            merge_fold_bundles(&bundles)
                .unwrap_or_else(|reason| fail(format!("cannot merge fold bundles: {reason}")))
                .into_iter()
                .map(|(candidate_id, folds)| CandidateInput {
                    candidate_id,
                    folds,
                })
                .collect()
        }
    };

    if candidates.is_empty() {
        fail("candidate list cannot be empty");
    }
    let mut ids = BTreeSet::new();
    if candidates.iter().any(|candidate| {
        candidate.candidate_id.trim().is_empty() || !ids.insert(&candidate.candidate_id)
    }) {
        fail("candidate ids must be non-empty and unique");
    }

    let candidate_coverage = candidates
        .iter()
        .map(|candidate| (candidate.candidate_id.clone(), candidate.folds.clone()))
        .collect::<std::collections::BTreeMap<_, _>>();
    validate_candidate_fold_coverage(&candidate_coverage)
        .unwrap_or_else(|reason| fail(format!("invalid candidate fold coverage: {reason}")));

    let mut evaluated = candidates
        .into_iter()
        .map(|candidate| CandidateOutput {
            candidate_id: candidate.candidate_id,
            evaluation: policy.evaluate(&candidate.folds),
        })
        .collect::<Vec<_>>();
    evaluated.sort_by(|left, right| {
        compare_risk_adjusted_candidates(&right.evaluation, &left.evaluation)
            .then_with(|| left.candidate_id.cmp(&right.candidate_id))
    });
    let best_candidate_id = evaluated
        .iter()
        .find(|candidate| candidate.evaluation.eligible)
        .map(|candidate| candidate.candidate_id.clone());

    let result = SelectionOutput {
        methodology_id: "anchorbell-oos-risk-adjusted-selector-v2",
        policy_id: policy.policy_id.clone(),
        policy_digest: policy.digest().unwrap_or_else(|message| fail(message)),
        best_candidate_id,
        candidates: evaluated,
    };
    println!(
        "{}",
        serde_json::to_string_pretty(&result).expect("selection output is serializable")
    );
}

fn parse_args() -> Result<(SelectionSource, PathBuf), String> {
    let mut input = None;
    let mut bundles = Vec::new();
    let mut policy = None;
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
            "--policy" => {
                let path = args.next().ok_or("--policy requires a path")?;
                policy = Some(PathBuf::from(path));
            }
            "--help" | "-h" => {
                eprintln!(
                    "usage: anchorbell_oos_select --policy POLICY.json --input CANDIDATES.json | --bundle FOLD.json [--bundle FOLD.json ...]"
                );
                process::exit(0);
            }
            _ => return Err(format!("unknown option {flag}")),
        }
    }
    let policy = policy.ok_or("--policy is required")?;
    let source = match (input, bundles.is_empty()) {
        (Some(path), true) => SelectionSource::Input(path),
        (None, false) => SelectionSource::Bundles(bundles),
        _ => Err("use exactly one --input or one-or-more --bundle arguments")?,
    };
    Ok((source, policy))
}

fn fail(message: impl std::fmt::Display) -> ! {
    eprintln!("{message}");
    process::exit(2);
}
