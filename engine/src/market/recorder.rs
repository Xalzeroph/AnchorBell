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

/// Serializes parsed Binance events for both live recording and replay.
///
/// The event timestamp remains the exchange timestamp; the optional receipt
/// timestamp preserves the local observation time for latency-aware research.
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
    };
    if let Some(received_at_ms) = received_at_ms {
        value["_anchorbell_received_at_ms"] = serde_json::json!(received_at_ms);
    }
    value
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
