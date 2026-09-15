pub mod action;
pub mod calibration;
pub mod calibration_store;
pub mod evolution;
pub mod execution;
pub mod ledger;
pub mod model;
pub mod policy;
pub mod portfolio;
pub mod runtime;

pub const ANCHORBELL_BUILD_SHA: &str = env!("ANCHORBELL_GIT_SHA");
pub const ANCHORBELL_BUILD_DIRTY: &str = env!("ANCHORBELL_GIT_DIRTY");
pub const ANCHORBELL_RUSTC: &str = env!("ANCHORBELL_RUSTC");

pub use action::{ActionChoice, ActionEconomics, ActionKind, ActionValueError};
pub use calibration_store::{CalibrationStoreError, FileCalibrationStore};
pub use model::{
    Anchor, AnchorEpisode, BinanceContract, ClosedWindow, EvidenceFrame, ValidatedOrder,
};
pub use policy::{Decision, DecisionEngine, StrategyPlan};
pub use portfolio::{PortfolioError, PortfolioSnapshot};
