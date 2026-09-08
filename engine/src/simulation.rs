//! Operational simulation facade.
//!
//! Simulation is an execution environment with explicit runtime, batch,
//! replay, accounting, and validation boundaries.

#[path = "simulation/contract.rs"]
pub(crate) mod contract;
#[path = "simulation/engine.rs"]
pub(crate) mod engine;
#[path = "simulation/experiment_plan.rs"]
pub(crate) mod experiment_plan;
#[path = "simulation/portfolio_guard.rs"]
pub(crate) mod portfolio_guard;
#[path = "simulation/replay_config.rs"]
pub(crate) mod replay_config;
#[path = "simulation/risk_metrics.rs"]
pub(crate) mod risk_metrics;
#[path = "simulation/runtime.rs"]
pub(crate) mod runtime;

pub mod batch {
    pub use crate::simulation_batch::*;
}

pub use crate::runtime::load_index_anchor_set;
pub use crate::simulation_batch::{
    run, SimulationBatchConfig, SimulationBatchResult, SimulationBatchSpec,
};
pub use contract::{SimulationRunManifest, SIMULATION_MANIFEST_SCHEMA_VERSION};

pub fn compiled_build_identity() -> String {
    SimulationRunManifest::compiled_build_identity()
}
pub use engine::*;
pub use experiment_plan::{ExperimentPlan, ExperimentSpec};
pub use replay_config::ReplayConfig;
