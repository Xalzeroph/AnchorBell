//! The AnchorBell TradFi catalog and anchor-stability eligibility boundary.

use serde::Deserialize;
use std::sync::LazyLock;
//!
//! ADR/ADS presence is recorded as issuer evidence. It is not, by itself, a
//! veto: the FrozenClose strategy is blocked only when the ADR market provides
//! active price discovery during the Hong Kong close-to-open interval.

#[derive(Debug, Clone, Copy, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum EquityRegion {
    AShare,
    HongKong,
}

#[derive(Debug, Clone, Copy, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum AdrStatus {
    /// The issuer has an active or historical ADR/ADS program.
    ConfirmedPresent,
    /// The issuer was checked and no ADR/ADS was found in the reviewed sources.
    ConfirmedAbsent,
    /// Evidence is incomplete; this status is never tradable.
    Unknown,
}

#[derive(Debug, Clone, Copy, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum AdrPriceDiscovery {
    /// A sufficiently active external market can reprice the issuer after the
    /// Hong Kong close; this contaminates a frozen-close signal.
    Active,
    /// A program exists, but stale/thin OTC activity is not treated as an
    /// effective continuous reference for the frozen-close strategy.
    InactiveOrStale,
    /// A program exists but there is no effective current market or quote.
    NoEffectiveMarket,
    /// Market quality has not been established; live execution fails closed.
    Unknown,
    /// No ADR/ADS program is part of the reviewed evidence.
    NotApplicable,
}

impl AdrPriceDiscovery {
    pub const fn allows_frozen_close(self) -> bool {
        matches!(
            self,
            Self::InactiveOrStale | Self::NoEffectiveMarket | Self::NotApplicable
        )
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TradFiInstrument {
    pub symbol: &'static str,
    pub region: EquityRegion,
    pub adr_status: AdrStatus,
    pub adr_price_discovery: AdrPriceDiscovery,
}

impl TradFiInstrument {
    pub const fn is_execution_eligible(self) -> bool {
        match self.adr_status {
            AdrStatus::ConfirmedAbsent => true,
            AdrStatus::ConfirmedPresent => self.adr_price_discovery.allows_frozen_close(),
            AdrStatus::Unknown => false,
        }
    }
}

#[derive(Debug, Deserialize)]
struct UniverseConfig {
    schema_version: u16,
    instruments: Vec<UniverseInstrumentConfig>,
}

#[derive(Debug, Deserialize)]
struct UniverseInstrumentConfig {
    symbol: String,
    region: EquityRegion,
    adr_status: AdrStatus,
    adr_price_discovery: AdrPriceDiscovery,
}

fn load_instruments(region: EquityRegion) -> Vec<TradFiInstrument> {
    let config: UniverseConfig = serde_json::from_str(include_str!(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../config/anchorbell-universe.json"
    )))
    .expect("anchorbell universe config must be valid JSON");
    assert_eq!(config.schema_version, 1, "unsupported universe config schema");
    config
        .instruments
        .into_iter()
        .filter(|instrument| instrument.region == region)
        .map(|instrument| TradFiInstrument {
            symbol: Box::leak(instrument.symbol.into_boxed_str()),
            region: instrument.region,
            adr_status: instrument.adr_status,
            adr_price_discovery: instrument.adr_price_discovery,
        })
        .collect()
}

pub static A_SHARE_INSTRUMENTS: LazyLock<Vec<TradFiInstrument>> =
    LazyLock::new(|| load_instruments(EquityRegion::AShare));

pub static HONG_KONG_INSTRUMENTS: LazyLock<Vec<TradFiInstrument>> =
    LazyLock::new(|| load_instruments(EquityRegion::HongKong));

/// Returns the complete reviewed catalog, including hard-excluded instruments.
pub fn catalog_instruments() -> impl Iterator<Item = &'static TradFiInstrument> {
    A_SHARE_INSTRUMENTS
        .iter()
        .chain(HONG_KONG_INSTRUMENTS.iter())
}

/// Returns instruments whose frozen-close anchor is not contaminated by an
/// active ADR/ADS price-discovery venue.
pub fn all_instruments() -> impl Iterator<Item = &'static TradFiInstrument> {
    catalog_instruments().filter(|instrument| instrument.is_execution_eligible())
}

pub fn catalog_instrument_for(symbol: &str) -> Option<TradFiInstrument> {
    let normalized = symbol.trim().to_ascii_uppercase();
    catalog_instruments()
        .find(|instrument| instrument.symbol == normalized)
        .copied()
}

pub fn instrument_for(symbol: &str) -> Option<TradFiInstrument> {
    catalog_instrument_for(symbol).filter(|instrument| instrument.is_execution_eligible())
}

/// Returns only instruments rejected because an ADR/ADS venue currently
/// provides active price discovery during the frozen-close interval.
pub fn adr_excluded_instruments() -> impl Iterator<Item = &'static TradFiInstrument> {
    catalog_instruments()
        .filter(|instrument| matches!(instrument.adr_price_discovery, AdrPriceDiscovery::Active))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn separates_reviewed_catalog_from_execution_universe() {
        assert_eq!(catalog_instruments().count(), 13);
        assert_eq!(all_instruments().count(), 7);
        assert_eq!(adr_excluded_instruments().count(), 6);
    }

    #[test]
    fn maps_a_share_instruments_without_adr_exclusion() {
        assert_eq!(
            instrument_for(" cxmtusdt "),
            Some(TradFiInstrument {
                symbol: "CXMTUSDT",
                region: EquityRegion::AShare,
                adr_status: AdrStatus::ConfirmedAbsent,
                adr_price_discovery: AdrPriceDiscovery::NotApplicable,
            })
        );
        assert_eq!(
            instrument_for("UNITREEUSDT").map(|instrument| instrument.region),
            Some(EquityRegion::AShare)
        );
    }

    #[test]
    fn rejects_hong_kong_issuers_with_active_adr_price_discovery() {
        for symbol in [
            "HK0700USDT",
            "HK1810USDT",
            "KUAISHOUUSDT",
            "MEITUANUSDT",
            "POPMARTUSDT",
            "TENCENTUSDT",
        ] {
            assert_eq!(instrument_for(symbol), None, "{symbol} must be rejected");
            assert_eq!(
                catalog_instrument_for(symbol).map(|instrument| instrument.adr_status),
                Some(AdrStatus::ConfirmedPresent)
            );
        }
    }

    #[test]
    fn reviewed_thin_or_ineffective_adr_symbols_remain_available() {
        for symbol in [
            "GIGADEVUSDT",
            "HK0625USDT",
            "MINIMAXUSDT",
            "ZHIPUUSDT",
            "ZHONGJIUSDT",
        ] {
            let instrument = catalog_instrument_for(symbol).expect("reviewed symbol");
            assert_eq!(instrument.adr_status, AdrStatus::ConfirmedPresent);
            assert!(
                instrument.is_execution_eligible(),
                "{symbol} should remain eligible"
            );
            assert_ne!(instrument.adr_price_discovery, AdrPriceDiscovery::Active);
        }
    }

    #[test]
    fn unknown_and_external_symbols_fail_closed() {
        assert_eq!(instrument_for("BTCUSDT"), None);
        assert_eq!(instrument_for(""), None);
        assert!(!TradFiInstrument {
            symbol: "UNKNOWN",
            region: EquityRegion::HongKong,
            adr_status: AdrStatus::Unknown,
            adr_price_discovery: AdrPriceDiscovery::Unknown,
        }
        .is_execution_eligible());
    }

    #[test]
    fn active_adr_price_discovery_is_hard_excluded() {
        assert!(!TradFiInstrument {
            symbol: "ACTIVE",
            region: EquityRegion::HongKong,
            adr_status: AdrStatus::ConfirmedPresent,
            adr_price_discovery: AdrPriceDiscovery::Active,
        }
        .is_execution_eligible());
    }

    #[test]
    fn weak_otc_adr_does_not_contaminate_frozen_close_by_existence() {
        assert!(TradFiInstrument {
            symbol: "THIN_OTC",
            region: EquityRegion::HongKong,
            adr_status: AdrStatus::ConfirmedPresent,
            adr_price_discovery: AdrPriceDiscovery::InactiveOrStale,
        }
        .is_execution_eligible());
        assert!(TradFiInstrument {
            symbol: "NO_MARKET",
            region: EquityRegion::HongKong,
            adr_status: AdrStatus::ConfirmedPresent,
            adr_price_discovery: AdrPriceDiscovery::NoEffectiveMarket,
        }
        .is_execution_eligible());
    }

    #[test]
    fn execution_universe_contains_no_active_adr_price_discovery() {
        assert!(all_instruments()
            .all(|instrument| { instrument.adr_price_discovery.allows_frozen_close() }));
        assert!(A_SHARE_INSTRUMENTS
            .iter()
            .all(|a_share| HONG_KONG_INSTRUMENTS
                .iter()
                .all(|hong_kong| a_share.symbol != hong_kong.symbol)));
    }
}
