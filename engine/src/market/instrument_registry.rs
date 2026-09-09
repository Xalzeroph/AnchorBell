//! Exchange-backed instrument registry. Unknown classifications fail closed.
use super::metadata::BinanceSymbolMetadata;
use serde::{Deserialize, Serialize};
use std::{
    collections::{BTreeMap, BTreeSet},
    path::Path,
};
use thiserror::Error;

pub const INSTRUMENT_REGISTRY_SCHEMA_VERSION: u16 = 1;

#[derive(Debug, Clone, Copy, Deserialize, Serialize, PartialEq, Eq, PartialOrd, Ord)]
#[serde(rename_all = "snake_case")]
pub enum MarketRegion {
    China,
    HongKong,
    Korea,
    Taiwan,
    Japan,
    UnitedStates,
    Global,
    Unknown,
}

#[derive(Debug, Clone, Copy, Deserialize, Serialize, PartialEq, Eq, PartialOrd, Ord)]
#[serde(rename_all = "snake_case")]
pub enum AssetClass {
    OrdinaryEquity,
    Adr,
    Etf,
    LeveragedEtf,
    Index,
    Commodity,
    PreMarketEquity,
    Unknown,
}

impl AssetClass {
    pub const fn is_equity_like(self) -> bool {
        matches!(
            self,
            Self::OrdinaryEquity | Self::Adr | Self::Etf | Self::LeveragedEtf | Self::Index
        )
    }
    pub const fn is_plain_ordinary_equity(self) -> bool {
        matches!(self, Self::OrdinaryEquity)
    }
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
pub struct InstrumentClassification {
    pub symbol: String,
    pub market_region: MarketRegion,
    pub reference_venue: String,
    pub asset_class: AssetClass,
    pub management_profile: String,
    #[serde(default)]
    pub source: Option<String>,
    #[serde(default)]
    pub simulation_enabled: bool,
    #[serde(default)]
    pub live_enabled: bool,
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
pub struct InstrumentRegistryConfig {
    pub schema_version: u16,
    pub source: String,
    pub instruments: Vec<InstrumentClassification>,
}

#[derive(Debug, Error)]
pub enum InstrumentRegistryError {
    #[error("instrument registry file could not be read: {0}")]
    Io(String),
    #[error("instrument registry JSON could not be decoded: {0}")]
    Decode(String),
    #[error("unsupported instrument registry schema version: {0}")]
    UnsupportedSchema(u16),
    #[error("duplicate instrument registry symbol: {0}")]
    DuplicateSymbol(String),
    #[error("instrument registry symbol is invalid: {0}")]
    InvalidSymbol(String),
    #[error("instrument registry venue is empty for {0}")]
    MissingVenue(String),
}

impl InstrumentRegistryConfig {
    pub fn from_json(json: &str) -> Result<Self, InstrumentRegistryError> {
        let mut config = serde_json::from_str::<Self>(json)
            .map_err(|error| InstrumentRegistryError::Decode(error.to_string()))?;
        if config.schema_version != INSTRUMENT_REGISTRY_SCHEMA_VERSION {
            return Err(InstrumentRegistryError::UnsupportedSchema(
                config.schema_version,
            ));
        }
        let mut seen = BTreeSet::new();
        for instrument in &mut config.instruments {
            instrument.symbol = instrument.symbol.trim().to_ascii_uppercase();
            if instrument.symbol.is_empty()
                || !instrument
                    .symbol
                    .bytes()
                    .all(|byte| byte.is_ascii_alphanumeric())
            {
                return Err(InstrumentRegistryError::InvalidSymbol(
                    instrument.symbol.clone(),
                ));
            }
            if !seen.insert(instrument.symbol.clone()) {
                return Err(InstrumentRegistryError::DuplicateSymbol(
                    instrument.symbol.clone(),
                ));
            }
            instrument.reference_venue = instrument.reference_venue.trim().to_owned();
            if instrument.reference_venue.is_empty() {
                return Err(InstrumentRegistryError::MissingVenue(
                    instrument.symbol.clone(),
                ));
            }
            instrument.management_profile = instrument.management_profile.trim().to_owned();
            instrument.source = instrument
                .source
                .take()
                .map(|s| s.trim().to_owned())
                .filter(|s| !s.is_empty());
        }
        Ok(config)
    }

    pub fn load(path: impl AsRef<Path>) -> Result<Self, InstrumentRegistryError> {
        let path = path.as_ref();
        let json = std::fs::read_to_string(path)
            .map_err(|error| InstrumentRegistryError::Io(format!("{}: {error}", path.display())))?;
        Self::from_json(&json)
    }

    pub fn embedded() -> Result<Self, InstrumentRegistryError> {
        Self::from_json(include_str!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../config/anchorbell-instrument-registry.json"
        )))
    }

    pub fn by_symbol(&self) -> BTreeMap<String, InstrumentClassification> {
        self.instruments
            .iter()
            .cloned()
            .map(|i| (i.symbol.clone(), i))
            .collect()
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct ManagedInstrument {
    pub symbol: String,
    pub exchange_status: String,
    pub contract_type: String,
    pub underlying_type: Option<String>,
    pub market_region: MarketRegion,
    pub reference_venue: Option<String>,
    pub asset_class: AssetClass,
    pub management_profile: Option<String>,
    pub simulation_enabled: bool,
    pub live_enabled: bool,
    pub strategy_eligible: bool,
    pub eligibility_reason: String,
    pub price_precision: u32,
    pub quantity_precision: u32,
    pub onboard_date_ms: u64,
    pub delivery_date_ms: u64,
}

#[derive(Debug, Clone, Serialize)]
pub struct InstrumentRegistrySnapshot {
    pub schema_version: u16,
    pub instruments: Vec<ManagedInstrument>,
    pub configured_missing_from_exchange: Vec<String>,
}

impl InstrumentRegistrySnapshot {
    pub fn from_exchange_info(
        exchange_info: Vec<BinanceSymbolMetadata>,
        config: &InstrumentRegistryConfig,
    ) -> Self {
        let classifications = config.by_symbol();
        let mut observed = BTreeSet::new();
        let mut instruments = Vec::new();
        for metadata in exchange_info
            .into_iter()
            .filter(|m| m.contract_type == "TRADIFI_PERPETUAL")
        {
            let c = classifications.get(&metadata.symbol);
            let (region, venue, class, profile) = c
                .map(|c| {
                    (
                        c.market_region,
                        Some(c.reference_venue.clone()),
                        c.asset_class,
                        Some(c.management_profile.clone()),
                    )
                })
                .unwrap_or((MarketRegion::Unknown, None, AssetClass::Unknown, None));
            let simulation_enabled = c.map(|c| c.simulation_enabled).unwrap_or(false);
            let live_enabled = c.map(|c| c.live_enabled).unwrap_or(false);
            let strategy_eligible =
                metadata.status == "TRADING" && class != AssetClass::Unknown && simulation_enabled;
            let reason = if metadata.status != "TRADING" {
                "exchange_status_not_trading"
            } else if class == AssetClass::Unknown {
                "missing_external_asset_classification"
            } else if !simulation_enabled && !live_enabled {
                "management_only_until_strategy_is_enabled"
            } else if live_enabled {
                "explicitly_enabled_for_live_and_simulation"
            } else {
                "explicitly_enabled_for_simulation_only"
            };
            observed.insert(metadata.symbol.clone());
            instruments.push(ManagedInstrument {
                symbol: metadata.symbol,
                exchange_status: metadata.status,
                contract_type: metadata.contract_type,
                underlying_type: metadata.underlying_type,
                market_region: region,
                reference_venue: venue,
                asset_class: class,
                management_profile: profile,
                simulation_enabled,
                live_enabled,
                strategy_eligible,
                eligibility_reason: reason.to_owned(),
                price_precision: metadata.price_precision,
                quantity_precision: metadata.quantity_precision,
                onboard_date_ms: metadata.onboard_date_ms,
                delivery_date_ms: metadata.delivery_date_ms,
            });
        }
        instruments.sort_by(|a, b| a.symbol.cmp(&b.symbol));
        let configured_missing_from_exchange = classifications
            .keys()
            .filter(|s| !observed.contains(*s))
            .cloned()
            .collect();
        Self {
            schema_version: INSTRUMENT_REGISTRY_SCHEMA_VERSION,
            instruments,
            configured_missing_from_exchange,
        }
    }

    pub fn by_asset_class(&self, class: AssetClass) -> impl Iterator<Item = &ManagedInstrument> {
        self.instruments
            .iter()
            .filter(move |i| i.asset_class == class)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::market::metadata::BinanceSymbolMetadata;

    fn metadata(symbol: &str, underlying_type: &str) -> BinanceSymbolMetadata {
        BinanceSymbolMetadata {
            symbol: symbol.to_owned(),
            status: "TRADING".to_owned(),
            contract_type: "TRADIFI_PERPETUAL".to_owned(),
            underlying_type: Some(underlying_type.to_owned()),
            base_asset: symbol.trim_end_matches("USDT").to_owned(),
            quote_asset: "USDT".to_owned(),
            margin_asset: "USDT".to_owned(),
            price_precision: 5,
            quantity_precision: 2,
            onboard_date_ms: 1,
            delivery_date_ms: 4_133_404_800_000,
            filters: Vec::new(),
        }
    }

    #[test]
    fn ordinary_equity_and_etf_are_distinct() {
        let config = InstrumentRegistryConfig::from_json(r#"{"schema_version":1,"source":"test","instruments":[
            {"symbol":"SAMSUNGUSDT","market_region":"korea","reference_venue":"KRX","asset_class":"ordinary_equity","management_profile":"tradfi_reference","simulation_enabled":true},
            {"symbol":"EWYUSDT","market_region":"korea","reference_venue":"NYSE_ARCA","asset_class":"etf","management_profile":"tradfi_reference","simulation_enabled":true}
        ]}"#).unwrap();
        let snapshot = InstrumentRegistrySnapshot::from_exchange_info(
            vec![
                metadata("SAMSUNGUSDT", "KR_EQUITY"),
                metadata("EWYUSDT", "EQUITY"),
            ],
            &config,
        );
        assert_eq!(
            snapshot
                .by_asset_class(AssetClass::OrdinaryEquity)
                .map(|i| i.symbol.as_str())
                .collect::<Vec<_>>(),
            vec!["SAMSUNGUSDT"]
        );
        assert_eq!(
            snapshot
                .by_asset_class(AssetClass::Etf)
                .map(|i| i.symbol.as_str())
                .collect::<Vec<_>>(),
            vec!["EWYUSDT"]
        );
    }

    #[test]
    fn unclassified_exchange_symbols_fail_closed() {
        let config = InstrumentRegistryConfig::from_json(
            r#"{"schema_version":1,"source":"test","instruments":[]}"#,
        )
        .unwrap();
        let snapshot = InstrumentRegistrySnapshot::from_exchange_info(
            vec![metadata("NEWUSDT", "KR_EQUITY")],
            &config,
        );
        let instrument = &snapshot.instruments[0];
        assert_eq!(instrument.asset_class, AssetClass::Unknown);
        assert!(!instrument.strategy_eligible);
        assert_eq!(
            instrument.eligibility_reason,
            "missing_external_asset_classification"
        );
    }
}
