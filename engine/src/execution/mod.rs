pub mod adaptive_taker;
pub mod binance;
#[path = "binance_wire.rs"]
pub mod binance_wire;
pub mod credential_store;
pub mod credentials;
pub mod deployment;
pub mod environment;
pub mod funding_risk;
pub mod gateway;
pub mod intent;
pub mod lifecycle;
pub mod lifecycle_contract;
#[path = "limits.rs"]
pub mod limits;
pub mod order_api;
pub mod order_manager;
#[path = "order_ws.rs"]
pub mod order_ws;
pub mod pnl;
pub mod ports;
#[path = "reconciliation.rs"]
pub mod reconciliation;
#[path = "recovery.rs"]
pub mod recovery;
pub mod rest;
pub mod risk;
pub mod safety;
pub mod session_checkpoint;
pub mod signing;
#[path = "spot.rs"]
pub mod spot;
pub mod supervisor;
pub mod trading_permission;
pub mod user_data;

pub use order_ws::{BinanceOrderWebSocket, OrderTransportError};

pub use limits::{LimitError, OrderLimits};

pub use pnl::{PnlBreakdown, PnlLedger, PnlObservation, PnlSource};
pub use ports::{
    AccountAuthority, AccountId, AccountSnapshot, AdapterError, AdapterFuture, ExecutionAdapter,
    MarketDataAdapter, ReadOnlyMarketAdapter, VenueId,
};
pub use reconciliation::{reconcile, ReconciliationAction, ReconciliationInput};
pub use recovery::{RecoveryEpoch, RecoveryEvent, RecoveryMachine, RecoveryState};
pub use rest::{
    BinanceAccountSnapshot, BinanceEmergencyReduceOnlyTakerRequest, BinanceMakerOrderRequest,
    BinanceOpenOrder, BinanceOrderResponse, BinancePositionRisk, BinanceRestClient,
    BinanceRestError, BinanceTradFiContractResponse,
};

pub use signing::{canonical_query, sign_query, signed_params, SigningError};

pub use adaptive_taker::{
    decide as decide_adaptive_taker, AdaptiveTakerDecision, AdaptiveTakerInput,
    EmergencyExecutionPolicy, TakerBlockReason, TakerDecision, TakerTrigger,
};
pub use binance::BinanceGateway;
pub use binance_wire::{
    BinanceAccountStatusResponse, BinanceAccountStatusResult, BinanceAccountStatusWire,
    BinanceOrderStatusResponse, BinanceOrderStatusResult, BinanceOrderStatusWire,
    BinancePositionSnapshot, BinancePositionStatusResponse, BinancePositionStatusWire,
};
pub use credential_store::{CredentialStoreError, PersistentCredentialStore};
pub use credentials::{BinanceCredentials, CredentialsError};
pub use deployment::{
    DeploymentConfig, DeploymentConfigError, ENABLE_ORDER_SUBMISSION_VAR, ENABLE_PRODUCTION_VAR,
    ENVIRONMENT_VAR, LIVE_TRADING_CONFIRMATION, LIVE_TRADING_CONFIRMATION_VAR,
};
pub use environment::{BinanceEndpoints, BinanceEnvironment, EnvironmentParseError};
pub use funding_risk::{FundingAwareRiskGate, FundingRiskAction, FundingRiskInput};
pub use gateway::{ExchangeOrder, ExecutionGateway, GatewayResult, SimulationGateway};
pub use intent::{OrderIntent, Side};
pub use lifecycle::{LifecycleError, LifecycleEvent, MakerOrder, OrderStatus};
pub use lifecycle_contract::{
    ExecutionMode, LifecycleAuthority, OrderLifecycleState, UnifiedOrderEvent, UnifiedOrderState,
};
pub use order_api::{
    BinanceOrderClient, SignedBinanceTransport, SignedCancelRequest, SignedOrderRequest,
};
pub use order_manager::{OrderManager, OrderState};
pub use risk::{RiskAction, RiskInput, SessionRiskGate};
pub use safety::{DeploymentPolicy, SafetyError};
pub use session_checkpoint::{
    CheckpointError, SessionCheckpoint, SESSION_CHECKPOINT_SCHEMA_VERSION,
};
pub use spot::{SpotDemoEndpoints, SpotOrderWire};
pub use supervisor::{
    ExecutionSupervisor, GateDecision, GateReason, SupervisorConfig, SupervisorState,
};
pub use trading_permission::TradingPermission;
pub use user_data::{
    parse_user_data_message, AccountUpdate, BinanceUserDataStream, OrderUpdate, PositionUpdate,
    UserDataError, UserDataEvent,
};
