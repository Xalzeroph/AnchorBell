//! Shared public-market data plane.
//! One Binance public feed may serve many tenant runtimes; private account
//! state must never be stored here.
use serde::{Deserialize, Serialize};
use std::{collections::BTreeMap, sync::Arc};
use tokio::sync::{broadcast, RwLock};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct MarketSnapshot {
    pub symbol: String,
    pub observed_at_ms: u64,
    pub event_time_ms: u64,
    pub bid_price: String,
    pub ask_price: String,
    pub bid_quantity: String,
    pub ask_quantity: String,
    pub mark_price: Option<String>,
    pub index_price: Option<String>,
    pub funding_rate: Option<String>,
    pub next_funding_time_ms: Option<u64>,
}

#[derive(Clone)]
pub struct SharedMarketDataPlane {
    snapshots: Arc<RwLock<BTreeMap<String, MarketSnapshot>>>,
    updates: broadcast::Sender<MarketSnapshot>,
}

impl SharedMarketDataPlane {
    pub fn new(channel_capacity: usize) -> Self {
        let (updates, _) = broadcast::channel(channel_capacity.max(1));
        Self {
            snapshots: Arc::new(RwLock::new(BTreeMap::new())),
            updates,
        }
    }

    pub async fn publish(&self, snapshot: MarketSnapshot) {
        self.snapshots
            .write()
            .await
            .insert(snapshot.symbol.to_ascii_uppercase(), snapshot.clone());
        let _ = self.updates.send(snapshot);
    }

    pub async fn snapshot(&self, symbol: &str) -> Option<MarketSnapshot> {
        self.snapshots
            .read()
            .await
            .get(&symbol.to_ascii_uppercase())
            .cloned()
    }

    pub async fn all(&self) -> Vec<MarketSnapshot> {
        self.snapshots.read().await.values().cloned().collect()
    }

    pub fn subscribe(&self) -> broadcast::Receiver<MarketSnapshot> {
        self.updates.subscribe()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[tokio::test]
    async fn one_published_snapshot_is_reused_by_multiple_readers() {
        let plane = SharedMarketDataPlane::new(4);
        let mut a = plane.subscribe();
        let mut b = plane.subscribe();
        let value = MarketSnapshot {
            symbol: "abcusdt".into(),
            observed_at_ms: 1,
            event_time_ms: 1,
            bid_price: "1".into(),
            ask_price: "2".into(),
            bid_quantity: "3".into(),
            ask_quantity: "4".into(),
            mark_price: None,
            index_price: None,
            funding_rate: None,
            next_funding_time_ms: None,
        };
        plane.publish(value.clone()).await;
        assert_eq!(a.recv().await.unwrap(), value);
        assert_eq!(b.recv().await.unwrap().symbol, "abcusdt");
        assert_eq!(plane.snapshot("ABCUSDT").await.unwrap().bid_price, "1");
    }
}
