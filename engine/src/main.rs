use anchorbell_engine::{
    policy::StrategyPlan, ANCHORBELL_BUILD_DIRTY, ANCHORBELL_BUILD_SHA, ANCHORBELL_RUSTC,
};

fn main() {
    println!(
        "anchorbell {} {} dirty={} rustc={}",
        ANCHORBELL_BUILD_SHA,
        StrategyPlan::anchor_closed_maker("plan-v1").version,
        ANCHORBELL_BUILD_DIRTY,
        ANCHORBELL_RUSTC
    );
}
