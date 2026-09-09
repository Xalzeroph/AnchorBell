use futures_util::{SinkExt, StreamExt};
use serde::de::DeserializeOwned;
use serde_json::Value;
use thiserror::Error;
use tokio::{
    net::TcpStream,
    time::{timeout, Duration},
};
use tokio_tungstenite::{tungstenite::Message, MaybeTlsStream, WebSocketStream};

use crate::network::connect_websocket;

use super::{BinanceEnvironment, DeploymentPolicy};

fn validate_maker_order_payload(payload: &Value) -> Result<(), OrderTransportError> {
    let params = payload
        .get("params")
        .and_then(Value::as_object)
        .ok_or(OrderTransportError::NonMakerOrder)?;
    let is_string = |name: &str| {
        params
            .get(name)
            .and_then(Value::as_str)
            .is_some_and(|value| !value.is_empty())
    };
    if params.get("type").and_then(Value::as_str) != Some("LIMIT")
        || params.get("timeInForce").and_then(Value::as_str) != Some("GTX")
        || !is_string("symbol")
        || !is_string("side")
        || !is_string("price")
        || !is_string("quantity")
        || !is_string("newClientOrderId")
        || !matches!(
            params.get("side").and_then(Value::as_str),
            Some("BUY" | "SELL")
        )
    {
        return Err(OrderTransportError::NonMakerOrder);
    }
    Ok(())
}

fn validate_order_place_response(
    response: &Value,
    expected_symbol: &str,
    expected_client_order_id: &str,
) -> Result<(), OrderTransportError> {
    if response.get("status").and_then(Value::as_u64) != Some(200) {
        return Err(OrderTransportError::InvalidResponse(
            "order.place response status is not 200".into(),
        ));
    }
    let result = response
        .get("result")
        .and_then(Value::as_object)
        .ok_or_else(|| {
            OrderTransportError::InvalidResponse("order.place response has no result object".into())
        })?;
    for field in ["orderId", "symbol", "clientOrderId", "status"] {
        if result.get(field).is_none() {
            return Err(OrderTransportError::InvalidResponse(format!(
                "order.place response is missing {field}"
            )));
        }
    }
    for field in ["symbol", "clientOrderId", "status"] {
        if result
            .get(field)
            .and_then(Value::as_str)
            .is_none_or(str::is_empty)
        {
            return Err(OrderTransportError::InvalidResponse(format!(
                "order.place response has an invalid {field}"
            )));
        }
    }
    if result.get("symbol").and_then(Value::as_str) != Some(expected_symbol)
        || result.get("clientOrderId").and_then(Value::as_str) != Some(expected_client_order_id)
    {
        return Err(OrderTransportError::InvalidResponse(
            "order.place response identity does not match the request".into(),
        ));
    }
    Ok(())
}

#[derive(Debug, Error)]
pub enum OrderTransportError {
    #[error("deployment policy rejected order transport: {0:?}")]
    Policy(super::SafetyError),
    #[error("websocket error: {0}")]
    WebSocket(#[source] Box<tokio_tungstenite::tungstenite::Error>),
    #[error("network transport error: {0}")]
    Network(String),
    #[error("response stream closed")]
    Closed,
    #[error("exchange response timed out; execution state is unknown; reconcile before retrying")]
    UnknownExecution,
    #[error("response was not valid text")]
    NonTextResponse,
    #[error("response could not be decoded into the requested type: {0}")]
    ResponseDecode(String),
    #[error("response did not contain the requested id")]
    CorrelationMismatch,
    #[error("order.place payload is not maker-only LIMIT + GTX")]
    NonMakerOrder,
    #[error("exchange returned an invalid response: {0}")]
    InvalidResponse(String),
    #[error("exchange returned error {code}: {message}")]
    Exchange { code: i64, message: String },
}

pub struct BinanceOrderWebSocket {
    socket: WebSocketStream<MaybeTlsStream<TcpStream>>,
    policy: DeploymentPolicy,
}

impl BinanceOrderWebSocket {
    pub async fn connect(
        environment: BinanceEnvironment,
        policy: DeploymentPolicy,
    ) -> Result<Self, OrderTransportError> {
        Self::connect_with_proxy(environment, policy, None).await
    }

    pub async fn connect_with_proxy(
        environment: BinanceEnvironment,
        policy: DeploymentPolicy,
        http_proxy: Option<&str>,
    ) -> Result<Self, OrderTransportError> {
        policy
            .validate_for(environment)
            .map_err(OrderTransportError::Policy)?;
        let endpoint = environment.endpoints().order_ws_base;
        let socket = connect_websocket(endpoint.as_str(), 10_000, http_proxy)
            .await
            .map_err(|error| OrderTransportError::Network(error.to_string()))?;
        Ok(Self { socket, policy })
    }

    pub async fn request(&mut self, payload: Value) -> Result<Value, OrderTransportError> {
        if payload.get("method").and_then(Value::as_str) == Some("order.place") {
            self.policy
                .validate_for_order(self.policy.environment)
                .map_err(OrderTransportError::Policy)?;
            validate_maker_order_payload(&payload)?;
        }
        let request_id = payload
            .get("id")
            .and_then(Value::as_str)
            .ok_or(OrderTransportError::CorrelationMismatch)?
            .to_string();
        self.socket
            .send(Message::Text(payload.to_string()))
            .await
            .map_err(|error| OrderTransportError::WebSocket(Box::new(error)))?;

        let response = timeout(Duration::from_secs(10), async {
            while let Some(message) = self.socket.next().await {
                match message.map_err(|error| OrderTransportError::WebSocket(Box::new(error)))? {
                    Message::Text(text) => {
                        let response: Value = serde_json::from_str(&text)
                            .map_err(|_| OrderTransportError::NonTextResponse)?;
                        if response.get("id").and_then(Value::as_str) != Some(request_id.as_str()) {
                            continue;
                        }
                        let status =
                            response
                                .get("status")
                                .and_then(Value::as_u64)
                                .ok_or_else(|| {
                                    OrderTransportError::InvalidResponse(
                                        "response is missing numeric status".into(),
                                    )
                                })?;
                        if status >= 400 {
                            let code = response
                                .get("error")
                                .and_then(|error| error.get("code"))
                                .and_then(Value::as_i64)
                                .unwrap_or(-1);
                            let message = response
                                .get("error")
                                .and_then(|error| error.get("msg"))
                                .and_then(Value::as_str)
                                .unwrap_or("unknown exchange error")
                                .to_string();
                            return Err(OrderTransportError::Exchange { code, message });
                        }
                        if status != 200 {
                            return Err(OrderTransportError::InvalidResponse(
                                "successful response status is not 200".into(),
                            ));
                        }
                        if payload.get("method").and_then(Value::as_str) == Some("order.place") {
                            let params = payload
                                .get("params")
                                .and_then(Value::as_object)
                                .ok_or(OrderTransportError::NonMakerOrder)?;
                            let expected_symbol = params
                                .get("symbol")
                                .and_then(Value::as_str)
                                .ok_or(OrderTransportError::NonMakerOrder)?;
                            let expected_client_order_id = params
                                .get("newClientOrderId")
                                .and_then(Value::as_str)
                                .ok_or(OrderTransportError::NonMakerOrder)?;
                            validate_order_place_response(
                                &response,
                                expected_symbol,
                                expected_client_order_id,
                            )?;
                        }
                        return Ok(response);
                    }
                    Message::Ping(payload) => {
                        self.socket
                            .send(Message::Pong(payload))
                            .await
                            .map_err(|error| OrderTransportError::WebSocket(Box::new(error)))?;
                    }
                    Message::Close(_) => return Err(OrderTransportError::Closed),
                    _ => {}
                }
            }
            Err(OrderTransportError::Closed)
        })
        .await
        .map_err(|_| OrderTransportError::UnknownExecution)??;
        Ok(response)
    }

    pub async fn request_typed<T: DeserializeOwned>(
        &mut self,
        payload: Value,
    ) -> Result<T, OrderTransportError> {
        let response = self.request(payload).await?;
        serde_json::from_value(response)
            .map_err(|error| OrderTransportError::ResponseDecode(error.to_string()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn requires_a_request_id_at_transport_boundary() {
        let payload = serde_json::json!({"method": "order.place", "params": {}});
        assert!(payload.get("id").and_then(Value::as_str).is_none());
    }

    #[test]
    fn rejects_non_maker_order_payload_before_network_connect() {
        let payload = serde_json::json!({
            "id": "order-1",
            "method": "order.place",
            "params": {
                "symbol": "ABCUSDT",
                "side": "BUY",
                "type": "LIMIT",
                "timeInForce": "GTC",
                "price": "10",
                "quantity": "1",
                "newClientOrderId": "anchorbell-1"
            }
        });
        assert!(matches!(
            validate_maker_order_payload(&payload),
            Err(OrderTransportError::NonMakerOrder)
        ));
    }

    #[test]
    fn rejects_missing_order_semantics_before_network_connect() {
        let payload = serde_json::json!({
            "id": "order-1",
            "method": "order.place",
            "params": {}
        });
        assert!(matches!(
            validate_maker_order_payload(&payload),
            Err(OrderTransportError::NonMakerOrder)
        ));
    }

    #[test]
    fn accepts_only_identity_complete_order_place_response() {
        let response = serde_json::json!({
            "id": "order-1",
            "status": 200,
            "result": {
                "orderId": 42,
                "symbol": "ABCUSDT",
                "clientOrderId": "anchorbell-1",
                "status": "NEW"
            }
        });
        assert!(validate_order_place_response(&response, "ABCUSDT", "anchorbell-1").is_ok());
    }

    #[test]
    fn rejects_order_place_response_without_exchange_identity() {
        let response = serde_json::json!({
            "id": "order-1",
            "status": 200,
            "result": {"status": "NEW"}
        });
        assert!(matches!(
            validate_order_place_response(&response, "ABCUSDT", "anchorbell-1"),
            Err(OrderTransportError::InvalidResponse(_))
        ));
    }

    #[test]
    fn rejects_order_place_response_for_wrong_request_identity() {
        let response = serde_json::json!({
            "id": "order-1",
            "status": 200,
            "result": {
                "orderId": 42,
                "symbol": "OTHERUSDT",
                "clientOrderId": "other-order",
                "status": "NEW"
            }
        });
        assert!(matches!(
            validate_order_place_response(&response, "ABCUSDT", "anchorbell-1"),
            Err(OrderTransportError::InvalidResponse(_))
        ));
    }

    #[test]
    fn environment_mismatch_is_rejected_before_network_connect() {
        let policy = DeploymentPolicy {
            environment: BinanceEnvironment::Testnet,
            allow_live_orders: false,
            allow_production: false,
            credentials_loaded: true,
        };
        assert_eq!(
            policy.validate_for(BinanceEnvironment::Production),
            Err(super::super::SafetyError::EnvironmentMismatch)
        );
    }
}
