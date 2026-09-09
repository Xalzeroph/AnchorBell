#[path = "market/binance.rs"]
pub mod binance;
#[path = "market/binance_adapter.rs"]
pub mod binance_adapter;
#[path = "market/capability.rs"]
pub mod capability;
#[path = "market/connection.rs"]
pub mod connection;
#[path = "market/freshness.rs"]
pub mod freshness;
#[path = "market/fx.rs"]
pub mod fx;
#[path = "market/instrument_registry.rs"]
pub mod instrument_registry;
#[path = "market/live.rs"]
pub mod live;
#[path = "market/metadata.rs"]
pub mod metadata;
#[path = "market/recorder.rs"]
pub mod recorder;
#[path = "market/shared.rs"]
pub mod shared;
#[path = "market/subscription.rs"]
pub mod subscription;
#[path = "market/truth.rs"]
pub mod truth;
pub use binance_adapter::BinanceMarketDataAdapter;
pub use capability::{CapabilityGateError, MarketCapabilityGate};
pub use connection::{ConnectionAction, ConnectionState, ConnectionSupervisor, ReconnectPolicy};
pub use freshness::{FreshnessClass, FreshnessPolicy, FreshnessState};
pub use fx::{BinanceC2cFxClient, BinanceC2cFxPoller, FxError, FxPollerConfig, FxQuote, FxUpdate};
pub use instrument_registry::{
    AssetClass, InstrumentClassification, InstrumentRegistryConfig, InstrumentRegistryError,
    InstrumentRegistrySnapshot, ManagedInstrument, MarketRegion,
    INSTRUMENT_REGISTRY_SCHEMA_VERSION,
};
pub use live::{BinanceMarketConfig, BinanceMarketFeed, BinanceMarketStream, MarketStreamError};
pub use metadata::{
    BinanceBookTickerSnapshot, BinanceDepthSnapshot, BinanceExecutionFilters, BinanceFundingInfo,
    BinanceFundingRateSnapshot, BinancePremiumIndexSnapshot, BinanceScaledExecutionFilters,
    BinanceSymbolFilter, BinanceSymbolMetadata, BinanceSymbolSnapshot,
    BinanceTimedPremiumIndexSnapshot, PublicMarketMetadataClient, PublicMetadataError,
    PUBLIC_SNAPSHOT_MAX_AGE_MS, PUBLIC_SNAPSHOT_POLICY,
};
pub use shared::{MarketSnapshot, SharedMarketDataPlane};
pub use subscription::{
    BinanceSubscription, SubscriptionError, SubscriptionPlan, SubscriptionPlanError,
};
pub use truth::{
    quote_event, MarketEventKind, MarketTruthError, MarketTruthSnapshot, MarketTruthState,
    StandardMarketEvent,
};

#[derive(Debug)]
pub struct MarketState {
    pub symbol: String,
    pub index_price: f64,
    pub bid: f64,
    pub ask: f64,
}

impl MarketState {
    pub fn mid(&self) -> f64 {
        (self.bid + self.ask) * 0.5
    }
}
