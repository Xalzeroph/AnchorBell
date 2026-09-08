pub(crate) mod audit;
pub(crate) mod channels;
pub(crate) mod control_plane;
pub(crate) mod event_envelope;
pub(crate) mod event_loop;
pub(crate) mod health_reporter;
pub(crate) mod io;
pub(crate) mod reference_authority;
pub(crate) mod run_registry;
#[path = "supervisor.rs"]
pub(crate) mod supervisor;

pub use audit::AuditSink;
pub use channels::RuntimeChannels;
pub use control_plane::RuntimeControlPlane;
pub use event_envelope::{CausalLedger, DataQuality, EventEnvelope, EventSource};
pub use event_loop::{DispatchError, RuntimeEventHandler, TradingRuntime};
pub use health_reporter::{timestamp_ms, RuntimeHealthReporter};
pub use reference_authority::fetch as load_index_anchor_set;
pub use run_registry::{
    RunHeartbeat, RunMode, RunRecord, RunRegistry, RunRegistryError, RunSpec, RunStatus,
    RUN_REGISTRY_SCHEMA_VERSION,
};
pub use supervisor::{RuntimeBus, RuntimeCapacities, RuntimeHandles, RuntimeSignal};
