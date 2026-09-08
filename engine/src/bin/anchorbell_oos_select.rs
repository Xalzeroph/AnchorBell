use std::{collections::BTreeSet, env, fs, path::PathBuf, process};

use anchorbell_engine::oos_validation::{
    compare_robust_candidates, evaluate_robust_candidate, merge_fold_bundles, OosFoldBundle,
    OosFoldMetrics, RobustCandidateEvaluation, RobustSelectionConstraints,
};
use serde::{Deserialize, Serialize};

#[derive(Debug, Deserialize)]
struct CandidateInput {
    candidate_id: String,
    folds: Vec<OosFoldMetrics>,
}

#[derive(Debug, Deserialize)]
struct SelectionInput {
    constraints: Option<RobustSelectionConstraints>,
    candidates: Vec<CandidateInput>,
}

#[derive(Debug, Serialize)]
struct CandidateOutput {
    candidate_id: String,
    evaluation: RobustCandidateEvaluation,
}

#[derive(Debug, Serialize)]
struct SelectionOutput {
    methodology_id: &'static str,
    constraints: RobustSelectionConstraints,
    best_candidate_id: Option<String>,
    candidates: Vec<CandidateOutput>,
}

enum SelectionSource {
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
                    .map(|(candidate_id, folds)| CandidateInput {
                        candidate_id,
                        folds,
                    })
                    .collect(),
            }
        }
    };
    if request.candidates.is_empty() {
        fail("candidate list cannot be empty");
    }
    let mut ids = BTreeSet::new();
    if request.candidates.iter().any(|candidate| {
        candidate.candidate_id.trim().is_empty() || !ids.insert(&candidate.candidate_id)
    }) {
        fail("candidate ids must be non-empty and unique");
    }

    let constraints = request.constraints.unwrap_or_default();
    let mut candidates = request
        .candidates
        .into_iter()
        .map(|candidate| CandidateOutput {
            candidate_id: candidate.candidate_id,
            evaluation: evaluate_robust_candidate(&candidate.folds, constraints),
        })
        .collect::<Vec<_>>();
    candidates.sort_by(|left, right| {
        compare_robust_candidates(&right.evaluation, &left.evaluation)
            .then_with(|| left.candidate_id.cmp(&right.candidate_id))
    });
    let best_candidate_id = candidates
        .iter()
        .find(|candidate| candidate.evaluation.eligible)
        .map(|candidate| candidate.candidate_id.clone());

    let result = SelectionOutput {
        methodology_id: "anchorbell-oos-robust-selector-v1",
        constraints,
        best_candidate_id,
        candidates,
    };
    println!(
        "{}",
        serde_json::to_string_pretty(&result).expect("selection output is serializable")
    );
}

fn parse_args() -> Result<SelectionSource, String> {
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
        _ => Err("use exactly one --input or one-or-more --bundle arguments".to_owned()),
    }
}

fn fail(message: impl std::fmt::Display) -> ! {
    eprintln!("{message}");
    process::exit(2);
}
