use std::{collections::BTreeMap, time::Duration};

use reqwest::Method;
use serde::Deserialize;
use thiserror::Error;

use crate::network::{RequestClass, RequestCoordinator};

use super::{
    signing::{canonical_query, sign_query, SigningError},
    BinanceCredentials, BinanceEnvironment, DeploymentPolicy, SafetyError, Side,
};

#[derive(Debug, Error)]
pub enum BinanceRestError {
    #[error("deployment policy rejected REST transport: {0:?}")]
    Policy(SafetyError),
    #[error("invalid HTTP proxy configuration")]
    InvalidProxy,
    #[error("REST client construction failed")]
    ClientBuild,
    #[error("REST request transport failed: {message}")]
    Transport { message: String },
    #[error("REST endpoint returned HTTP status {status}")]
    HttpStatus { status: u16 },
    #[error("REST response could not be decoded")]
    Decode,
    #[error("request signing failed: {0:?}")]
    Signing(SigningError),
    #[error("Binance returned application error {code}: {message}")]
    Exchange { code: i64, message: String },
    #[error("exchange execution status is unknown; reconcile before retrying (HTTP {status}): {message}")]
    UnknownExecution { status: u16, message: String },
    #[error("invalid order request: {0}")]
    InvalidOrderRequest(&'static str),
    #[error("order response identity mismatch: {0}")]
    InvalidOrderResponse(&'static str),
}

#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
pub struct BinanceOpenOrder {
    pub symbol: String,
    #[serde(rename = "clientOrderId")]
    pub client_order_id: String,
    pub status: String,
    #[serde(rename = "executedQty")]
    pub executed_quantity: String,
    #[serde(rename = "origQty", default)]
    pub original_quantity: String,
    #[serde(default)]
    pub price: String,
    #[serde(rename = "avgPrice", default)]
    pub average_price: String,
    #[serde(default)]
    pub side: String,
    #[serde(rename = "timeInForce", default)]
    pub time_in_force: String,
    #[serde(rename = "type", default)]
    pub order_type: String,
    #[serde(rename = "reduceOnly", default)]
    pub reduce_only: bool,
    #[serde(rename = "updateTime", default)]
    pub update_time_ms: u64,
    #[serde(rename = "orderId")]
    pub order_id: i64,
}

#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
pub struct BinanceOrderResponse {
    #[serde(rename = "orderId")]
    pub order_id: i64,
    pub symbol: String,
    pub status: String,
    #[serde(rename = "clientOrderId")]
    pub client_order_id: String,
    #[serde(default)]
    pub side: String,
    #[serde(rename = "timeInForce", default)]
    pub time_in_force: String,
    #[serde(rename = "type", default)]
    pub order_type: String,
    pub price: String,
    #[serde(rename = "origQty")]
    pub original_quantity: String,
    #[serde(rename = "executedQty")]
    pub executed_quantity: String,
    #[serde(rename = "avgPrice", default)]
    pub average_price: String,
    #[serde(rename = "updateTime", default)]
    pub update_time_ms: u64,
    #[serde(rename = "reduceOnly", default)]
    pub reduce_only: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BinanceMakerOrderRequest {
    pub symbol: String,
    pub side: Side,
    pub price: String,
    pub quantity: String,
    pub client_order_id: String,
    pub reduce_only: bool,
}

impl BinanceMakerOrderRequest {
    fn validate(&self) -> Result<(), BinanceRestError> {
        if !valid_symbol(&self.symbol) {
            return Err(BinanceRestError::InvalidOrderRequest("symbol"));
        }
        if !valid_positive_decimal(&self.price) {
            return Err(BinanceRestError::InvalidOrderRequest("price"));
        }
        if !valid_positive_decimal(&self.quantity) {
            return Err(BinanceRestError::InvalidOrderRequest("quantity"));
        }
        if !valid_client_order_id(&self.client_order_id) {
            return Err(BinanceRestError::InvalidOrderRequest("client_order_id"));
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
pub struct BinancePositionRisk {
    pub symbol: String,
    #[serde(rename = "positionAmt")]
    pub position_amount: String,
    #[serde(rename = "entryPrice")]
    pub entry_price: String,
    #[serde(rename = "markPrice")]
    pub mark_price: String,
    #[serde(rename = "unRealizedProfit")]
    pub unrealized_profit: String,
    #[serde(rename = "positionSide", default)]
    pub position_side: String,
    #[serde(rename = "updateTime", default)]
    pub update_time_ms: u64,
}

#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
pub struct BinanceAccountSnapshot {
    pub observed_at_ms: u64,
    pub open_orders: Vec<BinanceOpenOrder>,
    pub positions: Vec<BinancePositionRisk>,
}

#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
pub struct BinanceTradFiContractResponse {
    pub code: i64,
    pub msg: String,
}

pub struct BinanceRestClient {
    environment: BinanceEnvironment,
    policy: DeploymentPolicy,
    client: reqwest::Client,
    coordinator: RequestCoordinator,
}

impl BinanceRestClient {
    pub fn new(
        environment: BinanceEnvironment,
        policy: DeploymentPolicy,
        http_proxy: Option<&str>,
    ) -> Result<Self, BinanceRestError> {
        policy
            .validate_for(environment)
            .map_err(BinanceRestError::Policy)?;
        let mut builder = reqwest::Client::builder()
            .no_proxy()
            .timeout(Duration::from_secs(10));
        if let Some(proxy_url) = http_proxy {
            let proxy =
                reqwest::Proxy::http(proxy_url).map_err(|_| BinanceRestError::InvalidProxy)?;
            builder = builder.proxy(proxy);
        }
        let client = builder.build().map_err(|_| BinanceRestError::ClientBuild)?;
        Ok(Self {
            environment,
            policy,
            client,
            coordinator: RequestCoordinator::shared(),
        })
    }

    pub async fn sign_tradfi_contract(
        &self,
        credentials: &BinanceCredentials,
        timestamp_ms: u64,
        recv_window_ms: u64,
    ) -> Result<BinanceTradFiContractResponse, BinanceRestError> {
        let body =
            signed_tradfi_contract_body(&credentials.api_secret, timestamp_ms, recv_window_ms)
                .map_err(BinanceRestError::Signing)?;
        let url = format!(
            "{}/fapi/v1/stock/contract",
            self.environment.endpoints().rest_base
        );
        self.coordinator.acquire(RequestClass::Order).await;
        let response = self
            .client
            .post(url)
            .header("Content-Type", "application/x-www-form-urlencoded")
            .header("X-MBX-APIKEY", &credentials.api_key)
            .body(body)
            .send()
            .await
            .map_err(|error| BinanceRestError::Transport {
                message: error.to_string(),
            })?;
        let status = response.status().as_u16();
        self.coordinator
            .observe_status(RequestClass::Order, status, None)
            .await;
        let body = response
            .bytes()
            .await
            .map_err(|error| BinanceRestError::Transport {
                message: error.to_string(),
            })?;
        if status >= 400 {
            return Err(exchange_error(status, &body));
        }
        let result = serde_json::from_slice::<BinanceTradFiContractResponse>(&body)
            .map_err(|_| BinanceRestError::Decode)?;
        if result.code != 200 {
            return Err(BinanceRestError::Exchange {
                code: result.code,
                message: result.msg.clone(),
            });
        }
        Ok(result)
    }

    pub async fn current_open_orders(
        &self,
        credentials: &BinanceCredentials,
        symbol: Option<&str>,
        timestamp_ms: u64,
        recv_window_ms: u64,
    ) -> Result<Vec<BinanceOpenOrder>, BinanceRestError> {
        let query = signed_open_orders_query(
            symbol,
            &credentials.api_secret,
            timestamp_ms,
            recv_window_ms,
        )
        .map_err(BinanceRestError::Signing)?;
        let url = format!(
            "{}/fapi/v1/openOrders?{query}",
            self.environment.endpoints().rest_base
        );
        self.coordinator.acquire(RequestClass::Account).await;
        let response = self
            .client
            .get(url)
            .header("X-MBX-APIKEY", &credentials.api_key)
            .send()
            .await
            .map_err(|error| BinanceRestError::Transport {
                message: error.to_string(),
            })?;
        let status = response.status().as_u16();
        let body = response
            .bytes()
            .await
            .map_err(|error| BinanceRestError::Transport {
                message: error.to_string(),
            })?;
        if status >= 400 {
            return Err(exchange_error(status, &body));
        }
        serde_json::from_slice::<Vec<BinanceOpenOrder>>(&body).map_err(|_| BinanceRestError::Decode)
    }

    /// Starts the account user-data stream. The returned listen key must be
    /// kept alive by the caller and is never treated as an order acknowledgement.
    pub async fn start_user_data_stream(
        &self,
        credentials: &BinanceCredentials,
    ) -> Result<String, BinanceRestError> {
        self.policy
            .validate_for(self.environment)
            .map_err(BinanceRestError::Policy)?;
        let url = format!(
            "{}/fapi/v1/listenKey",
            self.environment.endpoints().rest_base
        );
        self.coordinator.acquire(RequestClass::UserStream).await;
        let response = self
            .client
            .post(url)
            .header("X-MBX-APIKEY", &credentials.api_key)
            .send()
            .await
            .map_err(|error| BinanceRestError::Transport {
                message: error.to_string(),
            })?;
        let status = response.status().as_u16();
        let body = response
            .bytes()
            .await
            .map_err(|error| BinanceRestError::Transport {
                message: error.to_string(),
            })?;
        if status >= 400 {
            return Err(exchange_error(status, &body));
        }
        #[derive(Deserialize)]
        struct ListenKeyResponse {
            #[serde(rename = "listenKey")]
            listen_key: String,
        }
        let value = serde_json::from_slice::<ListenKeyResponse>(&body)
            .map_err(|_| BinanceRestError::Decode)?;
        if value.listen_key.is_empty() {
            return Err(BinanceRestError::InvalidOrderResponse("empty listen key"));
        }
        Ok(value.listen_key)
    }

    /// Extends a user-data stream. Binance requires this at least once per
    /// hour; the supervisor refreshes it more frequently.
    pub async fn keepalive_user_data_stream(
        &self,
        credentials: &BinanceCredentials,
        listen_key: &str,
    ) -> Result<(), BinanceRestError> {
        if listen_key.is_empty() {
            return Err(BinanceRestError::InvalidOrderRequest("listen_key"));
        }
        self.policy
            .validate_for(self.environment)
            .map_err(BinanceRestError::Policy)?;
        let url = format!(
            "{}/fapi/v1/listenKey",
            self.environment.endpoints().rest_base
        );
        self.coordinator.acquire(RequestClass::UserStream).await;
        let response = self
            .client
            .put(url)
            .header("X-MBX-APIKEY", &credentials.api_key)
            .send()
            .await
            .map_err(|error| BinanceRestError::Transport {
                message: error.to_string(),
            })?;
        let status = response.status().as_u16();
        if status >= 400 {
            let body = response
                .bytes()
                .await
                .map_err(|error| BinanceRestError::Transport {
                    message: error.to_string(),
                })?;
            return Err(exchange_error(status, &body));
        }
        Ok(())
    }

    pub async fn server_time_ms(&self) -> Result<u64, BinanceRestError> {
        #[derive(Debug, Deserialize)]
        struct ServerTime {
            #[serde(rename = "serverTime")]
            server_time_ms: u64,
        }

        let url = format!("{}/fapi/v1/time", self.environment.endpoints().rest_base);
        self.coordinator.acquire(RequestClass::Public).await;
        let response =
            self.client
                .get(url)
                .send()
                .await
                .map_err(|error| BinanceRestError::Transport {
                    message: error.to_string(),
                })?;
        let status = response.status().as_u16();
        let body = response
            .bytes()
            .await
            .map_err(|error| BinanceRestError::Transport {
                message: error.to_string(),
            })?;
        if status >= 400 {
            return Err(exchange_error(status, &body));
        }
        serde_json::from_slice::<ServerTime>(&body)
            .map(|value| value.server_time_ms)
            .map_err(|_| BinanceRestError::Decode)
    }

    pub async fn place_maker_order(
        &self,
        credentials: &BinanceCredentials,
        request: BinanceMakerOrderRequest,
        timestamp_ms: u64,
        recv_window_ms: u64,
    ) -> Result<BinanceOrderResponse, BinanceRestError> {
        request.validate()?;
        let expected_symbol = request.symbol.clone();
        let expected_client_order_id = request.client_order_id.clone();
        let expected_side = match request.side {
            Side::Buy => "BUY",
            Side::Sell => "SELL",
        };
        let expected_reduce_only = request.reduce_only;
        let mut params = BTreeMap::new();
        params.insert("newClientOrderId".into(), request.client_order_id);
        params.insert("price".into(), request.price);
        params.insert("quantity".into(), request.quantity);
        params.insert(
            "side".into(),
            match request.side {
                Side::Buy => "BUY",
                Side::Sell => "SELL",
            }
            .into(),
        );
        params.insert("symbol".into(), request.symbol);
        params.insert("timeInForce".into(), "GTX".into());
        params.insert("type".into(), "LIMIT".into());
        params.insert("newOrderRespType".into(), "RESULT".into());
        if request.reduce_only {
            params.insert("reduceOnly".into(), "true".into());
        }
        let body = self
            .execute_signed(
                Method::POST,
                "/fapi/v1/order",
                params,
                credentials,
                timestamp_ms,
                recv_window_ms,
                true,
            )
            .await?;
        let response = serde_json::from_slice::<BinanceOrderResponse>(&body)
            .map_err(|_| BinanceRestError::Decode)?;
        if response.symbol != expected_symbol {
            return Err(BinanceRestError::InvalidOrderResponse("symbol"));
        }
        if response.client_order_id != expected_client_order_id {
            return Err(BinanceRestError::InvalidOrderResponse("client order id"));
        }
        if response.side != expected_side
            || response.time_in_force != "GTX"
            || response.order_type != "LIMIT"
            || response.reduce_only != expected_reduce_only
        {
            return Err(BinanceRestError::InvalidOrderResponse(
                "maker order semantics",
            ));
        }
        Ok(response)
    }

    pub async fn cancel_order(
        &self,
        credentials: &BinanceCredentials,
        symbol: &str,
        client_order_id: &str,
        timestamp_ms: u64,
        recv_window_ms: u64,
    ) -> Result<BinanceOrderResponse, BinanceRestError> {
        if !valid_symbol(symbol) {
            return Err(BinanceRestError::InvalidOrderRequest("symbol"));
        }
        if !valid_client_order_id(client_order_id) {
            return Err(BinanceRestError::InvalidOrderRequest("client_order_id"));
        }
        let mut params = BTreeMap::new();
        params.insert("origClientOrderId".into(), client_order_id.to_owned());
        params.insert("symbol".into(), symbol.to_owned());
        let body = self
            .execute_signed(
                Method::DELETE,
                "/fapi/v1/order",
                params,
                credentials,
                timestamp_ms,
                recv_window_ms,
                true,
            )
            .await?;
        let response = serde_json::from_slice::<BinanceOrderResponse>(&body)
            .map_err(|_| BinanceRestError::Decode)?;
        if response.symbol != symbol || response.client_order_id != client_order_id {
            return Err(BinanceRestError::InvalidOrderResponse("cancel identity"));
        }
        Ok(response)
    }

    pub async fn query_order(
        &self,
        credentials: &BinanceCredentials,
        symbol: &str,
        client_order_id: &str,
        timestamp_ms: u64,
        recv_window_ms: u64,
    ) -> Result<BinanceOrderResponse, BinanceRestError> {
        if !valid_symbol(symbol) {
            return Err(BinanceRestError::InvalidOrderRequest("symbol"));
        }
        if !valid_client_order_id(client_order_id) {
            return Err(BinanceRestError::InvalidOrderRequest("client_order_id"));
        }
        let mut params = BTreeMap::new();
        params.insert("origClientOrderId".into(), client_order_id.to_owned());
        params.insert("symbol".into(), symbol.to_owned());
        let body = self
            .execute_signed(
                Method::GET,
                "/fapi/v1/order",
                params,
                credentials,
                timestamp_ms,
                recv_window_ms,
                false,
            )
            .await?;
        let response = serde_json::from_slice::<BinanceOrderResponse>(&body)
            .map_err(|_| BinanceRestError::Decode)?;
        if response.symbol != symbol || response.client_order_id != client_order_id {
            return Err(BinanceRestError::InvalidOrderResponse("query identity"));
        }
        Ok(response)
    }

    pub async fn cancel_all_open_orders(
        &self,
        credentials: &BinanceCredentials,
        symbol: &str,
        timestamp_ms: u64,
        recv_window_ms: u64,
    ) -> Result<BinanceTradFiContractResponse, BinanceRestError> {
        if !valid_symbol(symbol) {
            return Err(BinanceRestError::InvalidOrderRequest("symbol"));
        }
        let mut params = BTreeMap::new();
        params.insert("symbol".into(), symbol.to_owned());
        let body = self
            .execute_signed(
                Method::DELETE,
                "/fapi/v1/allOpenOrders",
                params,
                credentials,
                timestamp_ms,
                recv_window_ms,
                true,
            )
            .await?;
        let response = serde_json::from_slice::<BinanceTradFiContractResponse>(&body)
            .map_err(|_| BinanceRestError::Decode)?;
        if response.code != 200 {
            return Err(BinanceRestError::Exchange {
                code: response.code,
                message: response.msg.clone(),
            });
        }
        Ok(response)
    }

    pub async fn authoritative_account_snapshot(
        &self,
        credentials: &BinanceCredentials,
        timestamp_ms: u64,
        recv_window_ms: u64,
    ) -> Result<BinanceAccountSnapshot, BinanceRestError> {
        let open_orders = self
            .current_open_orders(credentials, None, timestamp_ms, recv_window_ms)
            .await?;
        let positions = self
            .position_risk(credentials, None, timestamp_ms, recv_window_ms)
            .await?;
        Ok(BinanceAccountSnapshot {
            observed_at_ms: timestamp_ms,
            open_orders,
            positions,
        })
    }

    pub async fn position_risk(
        &self,
        credentials: &BinanceCredentials,
        symbol: Option<&str>,
        timestamp_ms: u64,
        recv_window_ms: u64,
    ) -> Result<Vec<BinancePositionRisk>, BinanceRestError> {
        if let Some(symbol) = symbol {
            if !valid_symbol(symbol) {
                return Err(BinanceRestError::InvalidOrderRequest("symbol"));
            }
        }
        let mut params = BTreeMap::new();
        if let Some(symbol) = symbol {
            params.insert("symbol".into(), symbol.to_owned());
        }
        let body = self
            .execute_signed(
                Method::GET,
                "/fapi/v2/positionRisk",
                params,
                credentials,
                timestamp_ms,
                recv_window_ms,
                false,
            )
            .await?;
        serde_json::from_slice::<Vec<BinancePositionRisk>>(&body)
            .map_err(|_| BinanceRestError::Decode)
    }

    // These fields intentionally mirror Binance's signed REST request contract.
    #[allow(clippy::too_many_arguments)]
    async fn execute_signed(
        &self,
        method: Method,
        path: &str,
        params: BTreeMap<String, String>,
        credentials: &BinanceCredentials,
        timestamp_ms: u64,
        recv_window_ms: u64,
        require_order_permission: bool,
    ) -> Result<Vec<u8>, BinanceRestError> {
        if require_order_permission {
            self.policy
                .validate_for_order(self.environment)
                .map_err(BinanceRestError::Policy)?;
        } else {
            self.policy
                .validate_for(self.environment)
                .map_err(BinanceRestError::Policy)?;
        }
        let query = signed_rest_query(
            params,
            &credentials.api_secret,
            timestamp_ms,
            recv_window_ms,
        )
        .map_err(BinanceRestError::Signing)?;
        let is_body = method == Method::POST;
        let url = if is_body {
            format!("{}{}", self.environment.endpoints().rest_base, path)
        } else {
            format!("{}{}?{query}", self.environment.endpoints().rest_base, path)
        };
        let mut request = self
            .client
            .request(method, url)
            .header("X-MBX-APIKEY", &credentials.api_key);
        if is_body {
            request = request
                .header("Content-Type", "application/x-www-form-urlencoded")
                .body(query);
        }
        self.coordinator
            .acquire(if require_order_permission {
                RequestClass::Order
            } else {
                RequestClass::Account
            })
            .await;
        let response = request.send().await.map_err(|error| {
            let message = error.to_string();
            if require_order_permission {
                BinanceRestError::UnknownExecution { status: 0, message }
            } else {
                BinanceRestError::Transport { message }
            }
        })?;
        let status = response.status().as_u16();
        self.coordinator
            .observe_status(
                if require_order_permission {
                    RequestClass::Order
                } else {
                    RequestClass::Account
                },
                status,
                None,
            )
            .await;
        let body = response.bytes().await.map_err(|error| {
            let message = error.to_string();
            if require_order_permission {
                BinanceRestError::UnknownExecution { status, message }
            } else {
                BinanceRestError::Transport { message }
            }
        })?;
        if status >= 400 {
            return Err(exchange_error(status, &body));
        }
        Ok(body.to_vec())
    }
}

fn signed_tradfi_contract_body(
    secret: &str,
    timestamp_ms: u64,
    recv_window_ms: u64,
) -> Result<String, SigningError> {
    signed_rest_query(BTreeMap::new(), secret, timestamp_ms, recv_window_ms)
}

fn signed_open_orders_query(
    symbol: Option<&str>,
    secret: &str,
    timestamp_ms: u64,
    recv_window_ms: u64,
) -> Result<String, SigningError> {
    let mut params = BTreeMap::new();
    if let Some(symbol) = symbol {
        params.insert("symbol".into(), symbol.to_owned());
    }
    signed_rest_query(params, secret, timestamp_ms, recv_window_ms)
}

fn exchange_error(status: u16, body: &[u8]) -> BinanceRestError {
    let message = serde_json::from_slice::<BinanceTradFiContractResponse>(body)
        .map(|result| result.msg)
        .unwrap_or_else(|_| String::from_utf8_lossy(body).trim().to_owned());
    if status == 503 {
        return BinanceRestError::UnknownExecution { status, message };
    }
    match serde_json::from_slice::<BinanceTradFiContractResponse>(body) {
        Ok(result) => BinanceRestError::Exchange {
            code: result.code,
            message: result.msg,
        },
        Err(_) => BinanceRestError::HttpStatus { status },
    }
}

fn valid_symbol(value: &str) -> bool {
    !value.is_empty() && value.bytes().all(|byte| byte.is_ascii_alphanumeric())
}

fn valid_positive_decimal(value: &str) -> bool {
    if value.is_empty() {
        return false;
    }
    let mut digits = 0_u32;
    let mut dots = 0_u32;
    let mut nonzero = false;
    for byte in value.bytes() {
        match byte {
            b'0'..=b'9' => {
                digits += 1;
                nonzero |= byte != b'0';
            }
            b'.' => dots += 1,
            _ => return false,
        }
    }
    digits > 0 && dots <= 1 && nonzero
}

fn valid_client_order_id(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 36
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || b"._:-".contains(&byte))
}

fn signed_rest_query(
    mut params: BTreeMap<String, String>,
    secret: &str,
    timestamp_ms: u64,
    recv_window_ms: u64,
) -> Result<String, SigningError> {
    params.insert("timestamp".into(), timestamp_ms.to_string());
    params.insert("recvWindow".into(), recv_window_ms.to_string());
    let signature = sign_query(&params, secret)?;
    params.insert("signature".into(), signature);
    Ok(canonical_query(&params))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn builds_signed_tradfi_contract_body_without_order_fields() {
        let body = signed_tradfi_contract_body("secret", 100, 5000).unwrap();
        assert!(body.contains("recvWindow=5000"));
        assert!(body.contains("timestamp=100"));
        assert!(body.contains("signature="));
        assert!(!body.contains("apiKey="));
        assert!(!body.contains("symbol="));
        assert!(!body.contains("secret"));
    }

    #[test]
    fn decodes_successful_tradfi_contract_response() {
        let response: BinanceTradFiContractResponse = serde_json::from_value(serde_json::json!({
            "code": 200,
            "msg": "success"
        }))
        .unwrap();
        assert_eq!(response.code, 200);
        assert_eq!(response.msg, "success");
    }

    #[test]
    fn builds_symbol_scoped_signed_open_orders_query() {
        let query = signed_open_orders_query(Some("BTCUSDT"), "secret", 100, 5000).unwrap();
        assert!(query.contains("symbol=BTCUSDT"));
        assert!(!query.contains("apiKey="));
        assert!(query.contains("recvWindow=5000"));
        assert!(query.contains("signature="));
        assert!(!query.contains("secret"));
    }

    #[test]
    fn rejects_production_mismatch_before_client_creation() {
        let policy = DeploymentPolicy {
            environment: BinanceEnvironment::Testnet,
            allow_live_orders: false,
            allow_production: false,
            credentials_loaded: true,
        };
        assert!(matches!(
            BinanceRestClient::new(BinanceEnvironment::Production, policy, None),
            Err(BinanceRestError::Policy(SafetyError::EnvironmentMismatch))
        ));
    }

    #[test]
    fn validates_maker_order_fields_before_transport() {
        let request = BinanceMakerOrderRequest {
            symbol: "BTCUSDT".into(),
            side: Side::Buy,
            price: "100.00".into(),
            quantity: "0.001".into(),
            client_order_id: "anchorbell-1".into(),
            reduce_only: false,
        };
        assert!(request.validate().is_ok());
        assert!(!valid_positive_decimal("0"));
        assert!(!valid_positive_decimal("-1"));
        assert!(!valid_client_order_id("has spaces"));
    }

    #[test]
    fn decodes_order_response_with_optional_fields() {
        let response: BinanceOrderResponse = serde_json::from_value(serde_json::json!({
            "orderId": 42,
            "symbol": "BTCUSDT",
            "status": "NEW",
            "clientOrderId": "anchorbell-1",
            "price": "100.00",
            "origQty": "0.001",
            "executedQty": "0"
        }))
        .unwrap();
        assert_eq!(response.order_id, 42);
        assert_eq!(response.average_price, "");
        assert_eq!(response.side, "");
        assert_eq!(response.time_in_force, "");
        assert_eq!(response.order_type, "");
        assert!(!response.reduce_only);
    }

    #[test]
    fn treats_http_503_as_unknown_execution() {
        assert!(matches!(
            exchange_error(503, br#"{\"code\":-1001,\"msg\":\"Unknown error, please check your request or account status.\"}"#),
            BinanceRestError::UnknownExecution { status: 503, .. }
        ));
    }
}
