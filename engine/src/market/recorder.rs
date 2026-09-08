use std::io::{self, Write};

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RecordedMarketMessage {
    pub received_at_ms: i64,
    pub payload: String,
}

#[derive(Debug, PartialEq, Eq)]
pub enum RecorderError {
    Io,
    OutOfOrder { previous_ms: i64, current_ms: i64 },
}

pub struct JsonlRecorder<W> {
    writer: W,
    last_received_at_ms: Option<i64>,
}

impl<W: Write> JsonlRecorder<W> {
    pub fn new(writer: W) -> Self {
        Self {
            writer,
            last_received_at_ms: None,
        }
    }

    pub fn append(&mut self, record: RecordedMarketMessage) -> Result<(), RecorderError> {
        if let Some(previous_ms) = self.last_received_at_ms {
            if record.received_at_ms < previous_ms {
                return Err(RecorderError::OutOfOrder {
                    previous_ms,
                    current_ms: record.received_at_ms,
                });
            }
        }
        serde_json::to_writer(&mut self.writer, &record).map_err(|_| RecorderError::Io)?;
        self.writer
            .write_all(b"\n")
            .map_err(|_| RecorderError::Io)?;
        self.last_received_at_ms = Some(record.received_at_ms);
        Ok(())
    }

    pub fn finish(mut self) -> Result<W, RecorderError> {
        self.writer.flush().map_err(|_| RecorderError::Io)?;
        Ok(self.writer)
    }
}

impl From<io::Error> for RecorderError {
    fn from(_: io::Error) -> Self {
        Self::Io
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn writes_one_json_record_per_line() {
        let mut output = Vec::new();
        let mut recorder = JsonlRecorder::new(&mut output);
        recorder
            .append(RecordedMarketMessage {
                received_at_ms: 10,
                payload: "{}".to_string(),
            })
            .unwrap();
        recorder.finish().unwrap();
        assert!(String::from_utf8(output).unwrap().ends_with("\n"));
    }

    #[test]
    fn rejects_out_of_order_receipts() {
        let mut recorder = JsonlRecorder::new(Vec::new());
        recorder
            .append(RecordedMarketMessage {
                received_at_ms: 10,
                payload: "{}".to_string(),
            })
            .unwrap();
        assert!(matches!(
            recorder.append(RecordedMarketMessage {
                received_at_ms: 9,
                payload: "{}".to_string(),
            }),
            Err(RecorderError::OutOfOrder { .. })
        ));
    }
}

#[allow(clippy::items_after_test_module)]
/// Serializes parsed Binance events for both live recording and replay.
///
/// The event timestamp remains the exchange timestamp; the optional receipt
/// timestamp preserves the local observation time for latency-aware operations.
pub fn market_event_to_json(
    event: &crate::market::binance::BinanceMarketEvent,
    price_scale: u32,
    quantity_scale: u32,
    received_at_ms: Option<u64>,
) -> serde_json::Value {
    let mut value = match event {
        crate::market::binance::BinanceMarketEvent::BookTicker(book) => serde_json::json!({
            "e": "bookTicker",
            "E": book.event_time_ms,
            "T": book.transaction_time_ms,
            "u": book.update_id,
            "s": book.symbol,
            "b": crate::execution::binance_wire::format_ticks(book.bid_price.0, price_scale),
            "B": crate::execution::binance_wire::format_ticks(book.bid_quantity.0, quantity_scale),
            "a": crate::execution::binance_wire::format_ticks(book.ask_price.0, price_scale),
            "A": crate::execution::binance_wire::format_ticks(book.ask_quantity.0, quantity_scale),
        }),
        crate::market::binance::BinanceMarketEvent::MarkPrice(mark) => serde_json::json!({
            "e": "markPriceUpdate",
            "E": mark.event_time_ms,
            "s": mark.symbol,
            "p": crate::execution::binance_wire::format_ticks(mark.mark_price.0, price_scale),
            "i": crate::execution::binance_wire::format_ticks(mark.index_price.0, price_scale),
            "T": mark.next_funding_time_ms,
            "r": mark.latest_funding_rate_e8
                .map(|value| crate::execution::binance_wire::format_ticks(value, 8))
                .unwrap_or_else(|| "0".to_owned()),
        }),
        crate::market::binance::BinanceMarketEvent::AggTrade(trade) => {
            serde_json::json!({            "e": "aggTrade",
                "E": trade.event_time_ms,
                "s": trade.symbol,
                "a": trade.aggregate_trade_id,
                "p": crate::execution::binance_wire::format_ticks(trade.price.0, price_scale),
                "q": crate::execution::binance_wire::format_ticks(trade.quantity.0, quantity_scale),
                "T": trade.trade_time_ms,
                "m": trade.buyer_is_maker,
            })
        }
        crate::market::binance::BinanceMarketEvent::DepthUpdate(depth) => {
            let levels = |items: &[crate::market::binance::DepthLevel]| {
                items
                    .iter()
                    .map(|level| {
                        [
                            crate::execution::binance_wire::format_ticks(
                                level.price.0,
                                price_scale,
                            ),
                            crate::execution::binance_wire::format_ticks(
                                level.quantity.0,
                                quantity_scale,
                            ),
                        ]
                    })
                    .collect::<Vec<_>>()
            };
            serde_json::json!({
                "e": "depthUpdate",
                "E": depth.event_time_ms,
                "T": depth.transaction_time_ms,
                "s": depth.symbol,
                "U": depth.first_update_id,
                "u": depth.final_update_id,
                "pu": depth.previous_final_update_id,
                "b": levels(&depth.bids),
                "a": levels(&depth.asks),
            })
        }
    };
    if let Some(received_at_ms) = received_at_ms {
        value["_anchorbell_received_at_ms"] = serde_json::json!(received_at_ms);
    }
    value
}

/// Adds the deterministic lineage fields used to join raw market evidence to
/// decisions, fills, and PnL without requiring a second side-channel lookup.
pub fn add_event_lineage<T>(
    value: &mut serde_json::Value,
    envelope: &crate::runtime::EventEnvelope<T>,
) {
    value["_anchorbell_event_id"] = serde_json::json!(envelope.event_id.as_str());
    value["_anchorbell_run_id"] = serde_json::json!(envelope.run_id.as_str());
    value["_anchorbell_causality_id"] = serde_json::json!(envelope.causality_id.as_str());
    value["_anchorbell_source"] = serde_json::to_value(&envelope.source).unwrap_or_default();
    value["_anchorbell_observed_at_ms"] = serde_json::json!(envelope.observed_at_ms);
    value["_anchorbell_sequence"] = serde_json::json!(envelope.sequence);
    value["_anchorbell_state_version"] = serde_json::json!(envelope.state_version);
    value["_anchorbell_quality"] = serde_json::to_value(&envelope.quality).unwrap_or_default();
}
