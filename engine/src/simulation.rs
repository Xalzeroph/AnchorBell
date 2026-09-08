//! Operational simulation facade.
//!
//! Simulation is an execution environment with explicit runtime, batch,
//! replay, accounting, and validation boundaries.

#[path = "simulation/contract.rs"]
pub mod contract;
#[path = "simulation/engine.rs"]
pub mod engine;
#[path = "simulation/experiment_plan.rs"]
pub mod experiment_plan;
#[path = "simulation/orchestration.rs"]
pub mod orchestration;
#[path = "simulation/profile.rs"]
pub mod profile;
#[path = "simulation/replay.rs"]
pub mod replay;
#[path = "simulation/runtime.rs"]
pub mod runtime;

pub mod batch {
    pub use crate::simulation_batch::*;
}

pub use crate::runtime::reference_authority::fetch as load_index_anchor_set;
pub use crate::simulation_batch::{
    SimulationBatchConfig, SimulationBatchResult, SimulationBatchSpec,
};
pub use contract::{SimulationRunManifest, SIMULATION_MANIFEST_SCHEMA_VERSION};
pub use engine::*;
pub use experiment_plan::{ExperimentPlan, ExperimentSpec};
pub use orchestration::*;
pub use profile::*;
