use std::{collections::BTreeMap, fmt, str::FromStr, sync::OnceLock};

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum BinanceEnvironment {
    Testnet,
    Production,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EnvironmentParseError {
    Unsupported,
}

impl BinanceEnvironment {
    pub fn endpoints(self) -> BinanceEndpoints {
        runtime_config()
            .environments
            .get(self.as_str())
            .cloned()
            .unwrap_or_else(|| panic!("missing Binance endpoint configuration for {}", self))
    }

    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Testnet => "testnet",
            Self::Production => "production",
        }
    }

    pub const fn credential_env_names(self) -> (&'static str, &'static str) {
        match self {
            Self::Testnet => (
                "ANCHORBELL_BINANCE_API_KEY",
                "ANCHORBELL_BINANCE_API_SECRET",
            ),
            Self::Production => (
                "ANCHORBELL_BINANCE_LIVE_API_KEY",
                "ANCHORBELL_BINANCE_LIVE_API_SECRET",
            ),
        }
    }
}

impl FromStr for BinanceEnvironment {
    type Err = EnvironmentParseError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value.trim().to_ascii_lowercase().as_str() {
            "testnet" | "demo" => Ok(Self::Testnet),
            "production" | "prod" | "live" | "mainnet" => Ok(Self::Production),
            _ => Err(EnvironmentParseError::Unsupported),
        }
    }
}

impl fmt::Display for BinanceEnvironment {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BinanceEndpoints {
    pub rest_base: String,
    pub market_ws_base: String,
    pub public_market_ws_base: String,
    pub order_ws_base: String,
    pub user_data_ws_base: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BinanceRequestWeights {
    pub generic: u32,
    pub exchange_info: u32,
    pub funding: u32,
    pub index_price_klines: u32,
    pub depth_by_limit: BTreeMap<String, u32>,
    pub depth_default: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BinanceOperationalConfig {
    pub max_frame_bytes: usize,
    pub default_recv_window_ms: u64,
    pub max_signal_age_ms: u64,
    pub default_funding_flatten_lead_ms: u64,
    pub default_maker_flatten_horizon_ms: u64,
    pub default_funding_flatten_horizon_ms: u64,
    pub default_inventory_skew_bps: i64,
    pub default_deadline_risk_bps: i64,
    pub listen_key_keepalive_interval_ms: u64,
}

#[derive(Debug, Clone, Deserialize)]
pub struct BinanceRuntimeConfig {
    schema_version: u16,
    environments: BTreeMap<String, BinanceEndpoints>,
    pub request_weights: BinanceRequestWeights,
    pub operational: BinanceOperationalConfig,
}

impl BinanceRuntimeConfig {
    fn validate(&self) -> Result<(), &'static str> {
        if self.environments.len() != 2
            || self
                .environments
                .values()
                .any(|endpoint| {
                    endpoint.rest_base.trim().is_empty()
                        || endpoint.market_ws_base.trim().is_empty()
                        || endpoint.public_market_ws_base.trim().is_empty()
                        || endpoint.order_ws_base.trim().is_empty()
                        || endpoint.user_data_ws_base.trim().is_empty()
                })
            || self.request_weights.generic == 0
            || self.request_weights.exchange_info == 0
            || self.request_weights.funding == 0
            || self.request_weights.index_price_klines == 0
            || self.request_weights.depth_default == 0
            || self.request_weights.depth_by_limit.values().any(|weight| *weight == 0)
            || self.operational.max_frame_bytes == 0
            || self.operational.default_recv_window_ms == 0
            || self.operational.max_signal_age_ms == 0
            || self.operational.listen_key_keepalive_interval_ms == 0
        {
            return Err("Binance runtime configuration contains missing or zero values");
        }
        Ok(())
    }
}

pub fn binance_runtime_config() -> &'static BinanceRuntimeConfig {
    static CONFIG: OnceLock<BinanceRuntimeConfig> = OnceLock::new();
    CONFIG.get_or_init(|| {
        let config: BinanceRuntimeConfig = serde_json::from_str(include_str!(
            concat!(env!("CARGO_MANIFEST_DIR"), "/../config/anchorbell-binance-runtime.json")
        ))
        .expect("embedded Binance runtime configuration must be valid JSON");
        assert_eq!(config.schema_version, 1, "unsupported Binance runtime config schema");
        config
            .validate()
            .expect("embedded Binance runtime configuration must pass validation");
        config
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn testnet_is_explicitly_separate_from_production() {
        let testnet = BinanceEnvironment::Testnet.endpoints();
        let production = BinanceEnvironment::Production.endpoints();
        assert_eq!(testnet.rest_base, "https://demo-fapi.binance.com");
        assert_eq!(
            testnet.market_ws_base,
            "wss://demo-fstream.binance.com/market"
        );
        assert_eq!(
            testnet.public_market_ws_base,
            "wss://demo-fstream.binance.com/public"
        );
        assert_eq!(
            testnet.order_ws_base,
            "wss://testnet.binancefuture.com/ws-fapi/v1"
        );
        assert_ne!(testnet.rest_base, production.rest_base);
        assert_ne!(testnet.market_ws_base, production.market_ws_base);
    }
}
