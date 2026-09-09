use crate::event::EngineEvent;
use crate::execution::{OrderIntent, OrderIntentError};
use crate::platform::{HealthSnapshot, ReadinessReport, SystemRegistry, SystemRole};
use crate::runtime::RuntimeChannels;
use std::time::{SystemTime, UNIX_EPOCH};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DispatchError {
    NonMakerIntent,
    InvalidIntent,
    OrderQueueClosed,
    Halted,
    SystemNotReady,
}

pub trait RuntimeEventHandler {
    fn on_event(&mut self, event: EngineEvent) -> Option<OrderIntent>;
}

impl<F> RuntimeEventHandler for F
where
    F: FnMut(EngineEvent) -> Option<OrderIntent>,
{
    fn on_event(&mut self, event: EngineEvent) -> Option<OrderIntent> {
        self(event)
    }
}

#[derive(Debug, Default)]
pub struct TradingRuntime {
    running: bool,
    halted: bool,
    processed_events: u64,
    /// Runtime topology and health registry. It is initialized before any
    /// event can create risk and is never used to bypass execution gates.
    registry: SystemRegistry,
}

impl TradingRuntime {
    pub fn new() -> Self {
        let mut runtime = Self::default();
        runtime.registry.bootstrap_health(now_ms());
        runtime
    }

    /// Exposes the authoritative topology to supervisors and diagnostics.
    pub fn system_registry(&self) -> &SystemRegistry {
        &self.registry
    }

    /// Allows asynchronous health reporters to publish validated snapshots.
    pub fn system_registry_mut(&mut self) -> &mut SystemRegistry {
        &mut self.registry
    }

    /// A runtime is composition-ready only when its dependency graph is valid.
    pub fn topology_ready(&self) -> bool {
        self.registry.validate_topology().is_ok()
    }

    /// Refresh health expiry without requiring an operator-maintained checklist.
    pub fn refresh_system_health(&mut self, now_ms: u64) -> Vec<String> {
        self.registry.mark_stale_at(now_ms)
    }

    pub fn report_system_health(
        &mut self,
        snapshot: HealthSnapshot,
    ) -> Result<(), crate::platform::RegistryError> {
        self.registry.report_health(snapshot)
    }

    pub fn live_readiness(&self, now_ms: u64) -> ReadinessReport {
        self.registry
            .readiness_for_role(SystemRole::ExecutionGateway, now_ms)
    }

    /// Live entrypoints must opt into this admission check before new risk.
    pub fn require_live_execution(&self, now_ms: u64) -> Result<(), DispatchError> {
        self.registry
            .require_role(SystemRole::ExecutionGateway, now_ms)
            .map_err(|_| DispatchError::SystemNotReady)
    }

    /// Keeps the zero-configuration entry point for composition tests.
    pub async fn run(&mut self) {
        self.running = true;
    }

    pub async fn run_channels<H>(
        &mut self,
        channels: RuntimeChannels,
        handler: &mut H,
    ) -> Result<(), DispatchError>
    where
        H: RuntimeEventHandler,
    {
        self.running = true;
        self.halted = false;
        let mut market_rx = channels.market_rx;
        let order_tx = channels.order_tx;

        while let Some(event) = market_rx.recv().await {
            self.processed_events = self.processed_events.saturating_add(1);
            let Some(intent) = handler.on_event(event) else {
                continue;
            };
            validate_intent(&intent).map_err(|error| {
                self.halted = true;
                self.running = false;
                error
            })?;
            order_tx.send(intent).await.map_err(|_| {
                self.halted = true;
                self.running = false;
                DispatchError::OrderQueueClosed
            })?;
        }

        self.running = false;
        Ok(())
    }

    pub fn is_running(&self) -> bool {
        self.running
    }

    pub fn is_halted(&self) -> bool {
        self.halted
    }

    pub fn processed_events(&self) -> u64 {
        self.processed_events
    }

    pub fn dispatch_event<H>(
        &mut self,
        handler: &mut H,
        event: EngineEvent,
    ) -> Result<Option<OrderIntent>, DispatchError>
    where
        H: RuntimeEventHandler,
    {
        if self.halted {
            return Err(DispatchError::Halted);
        }
        self.processed_events = self.processed_events.saturating_add(1);
        let Some(intent) = handler.on_event(event) else {
            return Ok(None);
        };
        if let Err(error) = validate_intent(&intent) {
            self.halted = true;
            return Err(error);
        }
        Ok(Some(intent))
    }
}

fn validate_intent(intent: &OrderIntent) -> Result<(), DispatchError> {
    match intent.validate() {
        Ok(()) => Ok(()),
        Err(OrderIntentError::UnscopedAggressor) => Err(DispatchError::NonMakerIntent),
        Err(OrderIntentError::InvalidShape) => Err(DispatchError::InvalidIntent),
    }
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_millis() as u64)
        .unwrap_or_default()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::event::MarketTick;
    use tokio::sync::mpsc;

    fn tick() -> EngineEvent {
        EngineEvent::MarketTick(
            MarketTick {
                symbol: 7,
                timestamp_ns: 1,
                bid: 100,
                ask: 101,
                index_price: 100,
                mark_price: 100,
            }
            .enveloped("test-run", 1),
        )
    }

    #[test]
    fn dispatches_only_valid_maker_intents() {
        let mut runtime = TradingRuntime::new();
        let mut handler = |_event| Some(OrderIntent::maker_buy(7, 100, 2));

        assert_eq!(
            runtime.dispatch_event(&mut handler, tick()).unwrap(),
            Some(OrderIntent::maker_buy(7, 100, 2))
        );
        assert_eq!(runtime.processed_events(), 1);
    }

    #[test]
    fn accepts_reduce_only_taker_intents() {
        let mut runtime = TradingRuntime::new();
        let mut handler = |_event| {
            Some(OrderIntent::emergency_reduce_only_taker(
                7,
                crate::execution::Side::Sell,
                99,
                2,
            ))
        };

        assert!(runtime.dispatch_event(&mut handler, tick()).is_ok());
        assert!(!runtime.is_halted());
    }

    #[test]
    fn rejects_unscoped_taker_intents_and_halts() {
        let mut runtime = TradingRuntime::new();
        let mut handler = |_event| {
            Some(OrderIntent {
                symbol: 7,
                side: crate::execution::Side::Buy,
                price: 100,
                quantity: 2,
                post_only: false,
                reduce_only: false,
            })
        };

        assert_eq!(
            runtime.dispatch_event(&mut handler, tick()),
            Err(DispatchError::NonMakerIntent)
        );
        assert!(runtime.is_halted());
        assert_eq!(
            runtime.dispatch_event(&mut handler, tick()),
            Err(DispatchError::Halted)
        );
    }

    #[tokio::test]
    async fn run_channels_forwards_to_bounded_order_queue() {
        let (market_tx, market_rx) = mpsc::channel(2);
        let (order_tx, mut order_rx) = mpsc::channel(1);
        market_tx.send(tick()).await.unwrap();
        drop(market_tx);

        let mut runtime = TradingRuntime::new();
        let mut handler = |_event| Some(OrderIntent::maker_sell(7, 101, 3));
        runtime
            .run_channels(RuntimeChannels::new(market_rx, order_tx), &mut handler)
            .await
            .unwrap();

        assert_eq!(runtime.processed_events(), 1);
        assert_eq!(
            order_rx.recv().await,
            Some(OrderIntent::maker_sell(7, 101, 3))
        );
        assert!(!runtime.is_running());
    }
}
