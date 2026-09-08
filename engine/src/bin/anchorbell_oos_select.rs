use std::{collections::BTreeSet, env, fs, path::PathBuf, process};

use anchorbell_engine::oos_validation::{
    compare_robust_candidates, evaluate_robust_candidate, OosFoldMetrics,
    RobustCandidateEvaluation, RobustSelectionConstraints,
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

fn main() {
    let input = parse_args().unwrap_or_else(|message| fail(message));
    let bytes = fs::read(&input)
        .unwrap_or_else(|error| fail(format!("cannot read {}: {error}", input.display())));
    let request: SelectionInput = serde_json::from_slice(&bytes)
        .unwrap_or_else(|error| fail(format!("invalid selection input: {error}")));
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

fn parse_args() -> Result<PathBuf, String> {
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

fn fail(message: impl std::fmt::Display) -> ! {
    eprintln!("{message}");
    process::exit(2);
}
