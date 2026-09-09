use std::{fmt, str::FromStr};

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
    pub const fn endpoints(self) -> BinanceEndpoints {
        match self {
            Self::Testnet => BinanceEndpoints {
                rest_base: "https://demo-fapi.binance.com",
                market_ws_base: "wss://demo-fstream.binance.com/market",
                public_market_ws_base: "wss://demo-fstream.binance.com/public",
                order_ws_base: "wss://testnet.binancefuture.com/ws-fapi/v1",
                user_data_ws_base: "wss://demo-fstream.binance.com/private",
            },
            Self::Production => BinanceEndpoints {
                rest_base: "https://fapi.binance.com",
                market_ws_base: "wss://fstream.binance.com/market",
                public_market_ws_base: "wss://fstream.binance.com/public",
                order_ws_base: "wss://ws-fapi.binance.com/ws-fapi/v1",
                user_data_ws_base: "wss://fstream.binance.com/private",
            },
        }
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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BinanceEndpoints {
    pub rest_base: &'static str,
    pub market_ws_base: &'static str,
    pub public_market_ws_base: &'static str,
    pub order_ws_base: &'static str,
    pub user_data_ws_base: &'static str,
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
