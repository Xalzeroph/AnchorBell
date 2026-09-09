use std::collections::{BTreeMap, BTreeSet};

use thiserror::Error;

use super::metadata::{BinanceSymbolSnapshot, PublicMetadataError};

#[derive(Debug, Error, PartialEq, Eq)]
pub enum CapabilityGateError {
    #[error("capability gate requires at least one symbol")]
    EmptyUniverse,
    #[error("capability universe contains an empty symbol")]
    EmptySymbol,
    #[error("capability universe contains a duplicate symbol: {0}")]
    DuplicateSymbol(String),
    #[error("snapshot symbol is not in the capability universe: {0}")]
    UnexpectedSymbol(String),
    #[error("snapshot rejected for {symbol}: {source:?}")]
    InvalidSnapshot {
        symbol: String,
        source: PublicMetadataError,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MarketCapabilityGate {
    required_symbols: BTreeSet<String>,
    ready_snapshots: BTreeMap<String, BinanceSymbolSnapshot>,
}

impl MarketCapabilityGate {
    pub fn new(
        symbols: impl IntoIterator<Item = impl Into<String>>,
    ) -> Result<Self, CapabilityGateError> {
        let mut required_symbols = BTreeSet::new();
        for symbol in symbols {
            let symbol = symbol.into().trim().to_ascii_uppercase();
            if symbol.is_empty() {
                return Err(CapabilityGateError::EmptySymbol);
            }
            if !required_symbols.insert(symbol.clone()) {
                return Err(CapabilityGateError::DuplicateSymbol(symbol));
            }
        }
        if required_symbols.is_empty() {
            return Err(CapabilityGateError::EmptyUniverse);
        }
        Ok(Self {
            required_symbols,
            ready_snapshots: BTreeMap::new(),
        })
    }

    pub fn accept(
        &mut self,
        snapshot: BinanceSymbolSnapshot,
        now_ms: u64,
    ) -> Result<(), CapabilityGateError> {
        let symbol = snapshot.metadata.symbol.to_ascii_uppercase();
        if !self.required_symbols.contains(&symbol) {
            return Err(CapabilityGateError::UnexpectedSymbol(symbol));
        }
        self.ready_snapshots.remove(&symbol);
        snapshot.validate_for_runtime(now_ms).map_err(|source| {
            CapabilityGateError::InvalidSnapshot {
                symbol: symbol.clone(),
                source,
            }
        })?;
        self.ready_snapshots.insert(symbol, snapshot);
        Ok(())
    }

    pub fn is_ready_at(&self, now_ms: u64) -> bool {
        self.ready_snapshots.len() == self.required_symbols.len()
            && self
                .ready_snapshots
                .values()
                .all(|snapshot| snapshot.validate_for_runtime(now_ms).is_ok())
    }

    pub fn required_symbols(&self) -> impl Iterator<Item = &str> {
        self.required_symbols.iter().map(String::as_str)
    }

    pub fn missing_symbols(&self) -> Vec<String> {
        self.required_symbols
            .difference(&self.ready_snapshots.keys().cloned().collect())
            .cloned()
            .collect()
    }

    pub fn snapshot(&self, symbol: &str) -> Option<&BinanceSymbolSnapshot> {
        self.ready_snapshots
            .get(&symbol.trim().to_ascii_uppercase())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::market::metadata::{
        BinanceBookTickerSnapshot, BinancePremiumIndexSnapshot, BinanceSymbolFilter,
        BinanceSymbolMetadata,
    };

    fn snapshot(symbol: &str, observed_at_ms: u64) -> BinanceSymbolSnapshot {
        BinanceSymbolSnapshot {
            metadata: BinanceSymbolMetadata {
                symbol: symbol.into(),
                status: "TRADING".into(),
                contract_type: "TRADIFI_PERPETUAL".into(),
                underlying_type: None,
                base_asset: symbol.trim_end_matches("USDT").into(),
                quote_asset: "USDT".into(),
                margin_asset: "USDT".into(),
                price_precision: 5,
                quantity_precision: 2,
                onboard_date_ms: 1,
                delivery_date_ms: 4_102_444_800_000,
                filters: vec![
                    BinanceSymbolFilter {
                        filter_type: "PRICE_FILTER".into(),
                        min_price: Some("0.001".into()),
                        max_price: Some("20000".into()),
                        tick_size: Some("0.001".into()),
                        min_quantity: None,
                        max_quantity: None,
                        step_size: None,
                        notional: None,
                        multiplier_up: None,
                        multiplier_down: None,
                    },
                    BinanceSymbolFilter {
                        filter_type: "LOT_SIZE".into(),
                        min_price: None,
                        max_price: None,
                        tick_size: None,
                        min_quantity: Some("0.01".into()),
                        max_quantity: Some("400000".into()),
                        step_size: Some("0.01".into()),
                        notional: None,
                        multiplier_up: None,
                        multiplier_down: None,
                    },
                    BinanceSymbolFilter {
                        filter_type: "MIN_NOTIONAL".into(),
                        min_price: None,
                        max_price: None,
                        tick_size: None,
                        min_quantity: None,
                        max_quantity: None,
                        step_size: None,
                        notional: Some("5".into()),
                        multiplier_up: None,
                        multiplier_down: None,
                    },
                    BinanceSymbolFilter {
                        filter_type: "PERCENT_PRICE".into(),
                        min_price: None,
                        max_price: None,
                        tick_size: None,
                        min_quantity: None,
                        max_quantity: None,
                        step_size: None,
                        notional: None,
                        multiplier_up: Some("1.03".into()),
                        multiplier_down: Some("0.97".into()),
                    },
                ],
            },
            book_ticker: BinanceBookTickerSnapshot {
                symbol: symbol.into(),
                bid_price: "8.27800".into(),
                bid_quantity: "22.46".into(),
                ask_price: "8.28100".into(),
                ask_quantity: "14.18".into(),
            },
            premium_index: BinancePremiumIndexSnapshot {
                symbol: symbol.into(),
                mark_price: "8.27900".into(),
                index_price: "8.27850".into(),
                last_funding_rate: "-0.00010000".into(),
                next_funding_time_ms: 2_000,
                time: 1_000,
            },
            observed_at_ms,
        }
    }

    #[test]
    fn requires_a_non_empty_unique_universe() {
        assert_eq!(
            MarketCapabilityGate::new(Vec::<String>::new()),
            Err(CapabilityGateError::EmptyUniverse)
        );
        assert_eq!(
            MarketCapabilityGate::new(vec!["CXMTUSDT", "cxmtusdt"]),
            Err(CapabilityGateError::DuplicateSymbol("CXMTUSDT".into()))
        );
    }

    #[test]
    fn becomes_ready_only_after_every_required_symbol_is_valid() {
        let mut gate = MarketCapabilityGate::new(vec!["CXMTUSDT", "UNITREEUSDT"]).unwrap();
        assert!(!gate.is_ready_at(1_000));
        assert_eq!(gate.missing_symbols(), vec!["CXMTUSDT", "UNITREEUSDT"]);
        gate.accept(snapshot("CXMTUSDT", 1_000), 1_000).unwrap();
        assert!(!gate.is_ready_at(1_000));
        gate.accept(snapshot("UNITREEUSDT", 1_000), 1_000).unwrap();
        assert!(gate.is_ready_at(1_000));
        assert!(gate.snapshot("cxmtusdt").is_some());
    }

    #[test]
    fn accepted_snapshot_expires_without_refresh() {
        let mut gate = MarketCapabilityGate::new(vec!["CXMTUSDT"]).unwrap();
        gate.accept(snapshot("CXMTUSDT", 1_000), 1_000).unwrap();
        assert!(gate.is_ready_at(1_000));
        assert!(!gate.is_ready_at(1_000 + super::super::metadata::PUBLIC_SNAPSHOT_MAX_AGE_MS + 1));
    }

    #[test]
    fn invalid_refresh_removes_previous_ready_snapshot() {
        let mut gate = MarketCapabilityGate::new(vec!["CXMTUSDT"]).unwrap();
        gate.accept(snapshot("CXMTUSDT", 1_000), 1_000).unwrap();
        let result = gate.accept(snapshot("CXMTUSDT", 1), 10_000);
        assert!(matches!(
            result,
            Err(CapabilityGateError::InvalidSnapshot {
                source: PublicMetadataError::StaleSnapshot,
                ..
            })
        ));
        assert!(!gate.is_ready_at(1_000));
        assert!(!gate.is_ready_at(10_000));
        assert_eq!(gate.missing_symbols(), vec!["CXMTUSDT"]);
    }

    #[test]
    fn rejects_symbols_outside_the_declared_universe() {
        let mut gate = MarketCapabilityGate::new(vec!["CXMTUSDT"]).unwrap();
        assert_eq!(
            gate.accept(snapshot("OTHERUSDT", 1_000), 1_000),
            Err(CapabilityGateError::UnexpectedSymbol("OTHERUSDT".into()))
        );
        assert!(!gate.is_ready_at(1_000));
    }

    #[test]
    fn rejects_empty_symbol_entries() {
        assert_eq!(
            MarketCapabilityGate::new(vec![" "]),
            Err(CapabilityGateError::EmptySymbol)
        );
    }
}
