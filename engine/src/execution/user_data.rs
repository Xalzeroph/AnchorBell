use std::time::Duration;

use futures_util::{SinkExt, StreamExt};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use thiserror::Error;
use tokio_tungstenite::tungstenite::Message;

use super::BinanceEnvironment;
use crate::network::connect_websocket;

#[derive(Debug, Error)]
pub enum UserDataError {
    #[error("user data payload is not valid JSON")]
    Decode,
    #[error("user data event is missing field {0}")]
    Missing(&'static str),
    #[error("user data event is unsupported: {0}")]
    Unsupported(String),
    #[error("user data stream transport failed: {0}")]
    Transport(String),
    #[error("user data stream closed")]
    Closed,
    #[error("user data listen key expired")]
    ListenKeyExpired,
    #[error("user data frame exceeds configured limit")]
    FrameTooLarge,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub enum UserDataEvent {
    OrderUpdate(Box<OrderUpdate>),
    AccountUpdate(AccountUpdate),
    ListenKeyExpired,
    /// Preserve newly introduced Binance user events for audit without
    /// dropping the stream or pretending they changed trading state.
    Unmodeled {
        event_time_ms: u64,
        event_type: String,
        payload: Value,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct OrderUpdate {
    pub event_time_ms: u64,
    pub transaction_time_ms: u64,
    pub symbol: String,
    pub client_order_id: String,
    pub order_id: i64,
    pub side: String,
    pub order_type: String,
    pub time_in_force: String,
    pub status: String,
    pub execution_type: String,
    pub executed_quantity: String,
    pub last_filled_quantity: String,
    pub average_price: String,
    pub reduce_only: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct AccountUpdate {
    pub event_time_ms: u64,
    pub transaction_time_ms: u64,
    pub reason: String,
    pub positions: Vec<PositionUpdate>,
    pub balances: Vec<BalanceUpdate>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct PositionUpdate {
    pub symbol: String,
    pub position_amount: String,
    pub entry_price: String,
    pub unrealized_profit: String,
    pub position_side: String,
}

#[derive(Debug, Deserialize)]
struct OrderWire {
    #[serde(rename = "s")]
    symbol: String,
    #[serde(rename = "c")]
    client_order_id: String,
    #[serde(rename = "i")]
    order_id: i64,
    #[serde(rename = "S")]
    side: String,
    #[serde(rename = "o")]
    order_type: String,
    #[serde(rename = "f")]
    time_in_force: String,
    #[serde(rename = "X")]
    status: String,
    #[serde(rename = "x")]
    execution_type: String,
    #[serde(rename = "z")]
    executed_quantity: String,
    #[serde(rename = "l")]
    last_filled_quantity: String,
    #[serde(rename = "ap")]
    average_price: String,
    #[serde(rename = "R", default)]
    reduce_only: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct BalanceUpdate {
    pub asset: String,
    pub wallet_balance: String,
    pub cross_wallet_balance: String,
    pub balance_change: String,
}

#[derive(Debug, Deserialize)]
struct BalanceWire {
    #[serde(rename = "a")]
    asset: String,
    #[serde(rename = "wb")]
    wallet_balance: String,
    #[serde(rename = "cw")]
    cross_wallet_balance: String,
    #[serde(rename = "bc")]
    balance_change: String,
}

#[derive(Debug, Deserialize)]
struct PositionWire {
    #[serde(rename = "s")]
    symbol: String,
    #[serde(rename = "pa")]
    position_amount: String,
    #[serde(rename = "ep")]
    entry_price: String,
    #[serde(rename = "up")]
    unrealized_profit: String,
    #[serde(rename = "ps")]
    position_side: String,
}

#[derive(Debug, Deserialize)]
struct AccountWire {
    #[serde(rename = "T")]
    transaction_time_ms: u64,
    #[serde(rename = "m", default)]
    reason: String,
    #[serde(rename = "P", default)]
    positions: Vec<PositionWire>,
    #[serde(rename = "B", default)]
    balances: Vec<BalanceWire>,
}

pub fn parse_user_data_message(payload: &[u8]) -> Result<UserDataEvent, UserDataError> {
    let value: Value = serde_json::from_slice(payload).map_err(|_| UserDataError::Decode)?;
    let event = value
        .get("e")
        .and_then(Value::as_str)
        .ok_or(UserDataError::Missing("e"))?;
    let event_time_ms = value
        .get("E")
        .and_then(Value::as_u64)
        .ok_or(UserDataError::Missing("E"))?;
    match event {
        "ORDER_TRADE_UPDATE" => {
            let wire: OrderWire =
                serde_json::from_value(value.get("o").cloned().ok_or(UserDataError::Missing("o"))?)
                    .map_err(|_| UserDataError::Decode)?;
            if wire.symbol.is_empty() || wire.client_order_id.is_empty() {
                return Err(UserDataError::Missing("order identity"));
            }
            Ok(UserDataEvent::OrderUpdate(Box::new(OrderUpdate {
                event_time_ms,
                transaction_time_ms: value
                    .get("T")
                    .and_then(Value::as_u64)
                    .ok_or(UserDataError::Missing("T"))?,
                symbol: wire.symbol,
                client_order_id: wire.client_order_id,
                order_id: wire.order_id,
                side: wire.side,
                order_type: wire.order_type,
                time_in_force: wire.time_in_force,
                status: wire.status,
                execution_type: wire.execution_type,
                executed_quantity: wire.executed_quantity,
                last_filled_quantity: wire.last_filled_quantity,
                average_price: wire.average_price,
                reduce_only: wire.reduce_only,
            })))
        }
        "ACCOUNT_UPDATE" => {
            let wire: AccountWire =
                serde_json::from_value(value.get("a").cloned().ok_or(UserDataError::Missing("a"))?)
                    .map_err(|_| UserDataError::Decode)?;
            Ok(UserDataEvent::AccountUpdate(AccountUpdate {
                event_time_ms,
                transaction_time_ms: wire.transaction_time_ms,
                reason: wire.reason,
                positions: wire
                    .positions
                    .into_iter()
                    .map(|position| PositionUpdate {
                        symbol: position.symbol,
                        position_amount: position.position_amount,
                        entry_price: position.entry_price,
                        unrealized_profit: position.unrealized_profit,
                        position_side: position.position_side,
                    })
                    .collect(),
                balances: wire
                    .balances
                    .into_iter()
                    .map(|balance| BalanceUpdate {
                        asset: balance.asset,
                        wallet_balance: balance.wallet_balance,
                        cross_wallet_balance: balance.cross_wallet_balance,
                        balance_change: balance.balance_change,
                    })
                    .collect(),
            }))
        }
        "listenKeyExpired" => Ok(UserDataEvent::ListenKeyExpired),
        other => Ok(UserDataEvent::Unmodeled {
            event_time_ms,
            event_type: other.to_owned(),
            payload: value,
        }),
    }
}

pub struct BinanceUserDataStream {
    environment: BinanceEnvironment,
    listen_key: String,
    max_frame_bytes: usize,
    http_proxy: Option<String>,
}

impl BinanceUserDataStream {
    pub fn new(
        environment: BinanceEnvironment,
        listen_key: String,
        http_proxy: Option<String>,
    ) -> Result<Self, UserDataError> {
        if listen_key.is_empty() {
            return Err(UserDataError::Missing("listenKey"));
        }
        Ok(Self {
            environment,
            listen_key,
            max_frame_bytes: 1_048_576,
            http_proxy,
        })
    }

    pub async fn run<F>(&self, mut on_event: F) -> Result<(), UserDataError>
    where
        F: FnMut(UserDataEvent) + Send,
    {
        let base = self.environment.endpoints().user_data_ws_base;
        let url = format!("{}/ws/{}", base.trim_end_matches('/'), self.listen_key);
        let mut socket = connect_websocket(&url, 10_000, self.http_proxy.as_deref())
            .await
            .map_err(|error| UserDataError::Transport(error.to_string()))?;
        loop {
            let message = tokio::time::timeout(Duration::from_secs(30), socket.next())
                .await
                .map_err(|_| UserDataError::Transport("user data read timeout".into()))?;
            let Some(message) = message else {
                return Err(UserDataError::Closed);
            };
            match message.map_err(|error| UserDataError::Transport(error.to_string()))? {
                Message::Text(text) => {
                    if text.len() > self.max_frame_bytes {
                        return Err(UserDataError::FrameTooLarge);
                    }
                    let event = parse_user_data_message(text.as_bytes())?;
                    if matches!(event, UserDataEvent::ListenKeyExpired) {
                        on_event(event);
                        return Err(UserDataError::ListenKeyExpired);
                    }
                    on_event(event);
                }
                Message::Binary(bytes) => {
                    if bytes.len() > self.max_frame_bytes {
                        return Err(UserDataError::FrameTooLarge);
                    }
                    let event = parse_user_data_message(&bytes)?;
                    if matches!(event, UserDataEvent::ListenKeyExpired) {
                        on_event(event);
                        return Err(UserDataError::ListenKeyExpired);
                    }
                    on_event(event);
                }
                Message::Ping(bytes) => {
                    socket
                        .send(Message::Pong(bytes))
                        .await
                        .map_err(|error| UserDataError::Transport(error.to_string()))?;
                }
                Message::Close(_) => return Err(UserDataError::Closed),
                _ => {}
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn order_payload() -> Vec<u8> {
        br#"{"e":"ORDER_TRADE_UPDATE","E":100,"T":101,"o":{"s":"CXMTUSDT","c":"anchorbell-1","i":7,"S":"BUY","o":"LIMIT","f":"GTX","X":"NEW","x":"NEW","z":"0","l":"0","ap":"0","R":false}}"#.to_vec()
    }

    #[test]
    fn parses_identity_complete_order_update() {
        let event = parse_user_data_message(&order_payload()).unwrap();
        assert!(
            matches!(event, UserDataEvent::OrderUpdate(value) if value.symbol == "CXMTUSDT" && value.time_in_force == "GTX")
        );
    }

    #[test]
    fn rejects_order_update_without_client_identity() {
        let mut payload = order_payload();
        payload = String::from_utf8(payload)
            .unwrap()
            .replace("anchorbell-1", "")
            .into_bytes();
        assert!(matches!(
            parse_user_data_message(&payload),
            Err(UserDataError::Missing("order identity"))
        ));
    }

    #[test]
    fn parses_account_position_update() {
        let payload = br#"{"e":"ACCOUNT_UPDATE","E":100,"a":{"T":101,"m":"ORDER","B":[{"a":"USDT","wb":"100","cw":"100","bc":"0"}],"P":[{"s":"CXMTUSDT","pa":"2","ep":"8","up":"1","ps":"BOTH"}]}}"#;
        let event = parse_user_data_message(payload).unwrap();
        assert!(
            matches!(event, UserDataEvent::AccountUpdate(value) if value.positions.len() == 1 && value.positions[0].position_amount == "2" && value.balances.len() == 1)
        );
    }

    #[test]
    fn new_events_are_preserved_without_tearing_down_the_stream() {
        let payload = br#"{"e":"ACCOUNT_CONFIG_UPDATE","E":100,"T":101,"ac":{"s":"BOTH"}}"#;
        assert!(matches!(
            parse_user_data_message(payload),
            Ok(UserDataEvent::Unmodeled { event_type, .. }) if event_type == "ACCOUNT_CONFIG_UPDATE"
        ));
    }
}
