use anchorbell_engine::{policy::StrategyPlan, ANCHORBELL_BUILD_SHA};

fn main() {
    println!(
        "anchorbell {} {}",
        ANCHORBELL_BUILD_SHA,
        StrategyPlan::anchor_closed_maker("plan-v1").version
    );
}
