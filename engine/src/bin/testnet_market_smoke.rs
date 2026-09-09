use anchorbell_engine::execution::BinanceEnvironment;
use anchorbell_engine::market::{
    BinanceMarketConfig, BinanceMarketStream, BinanceSubscription, ReconnectPolicy,
};
use std::time::Duration;

#[tokio::main]
async fn main() {
    let symbol = std::env::var("ANCHORBELL_TESTNET_SYMBOL").unwrap_or_else(|_| "BTCUSDT".into());
    let subscription = BinanceSubscription::new(symbol).expect("valid symbol");
    let config = BinanceMarketConfig {
        market_ws_base: BinanceEnvironment::Testnet.endpoints().market_ws_base,
        subscriptions: vec![subscription],
        // Mark-price streams commonly expose eight decimal places; normalize both feeds to it.
        price_scale: std::env::var("ANCHORBELL_PRICE_SCALE")
            .ok()
            .and_then(|value| value.parse().ok())
            .unwrap_or(8),
        quantity_scale: 8,
        max_frame_bytes: 1_048_576,
        connect_timeout_ms: 5_000,
        read_timeout_ms: 15_000,
        http_proxy: std::env::var("ANCHORBELL_HTTP_PROXY").ok(),
        reconnect: ReconnectPolicy {
            max_attempts: Some(1),
            ..ReconnectPolicy::default()
        },
    };
    let mut stream = BinanceMarketStream::new(config);
    let mut count = 0_u32;
    let result = tokio::time::timeout(
        Duration::from_secs(12),
        stream.run_until_error(|event| {
            count += 1;
            println!("event={event:?}");
        }),
    )
    .await;
    println!("market_smoke_events={count} result={result:?}");
    if count == 0 {
        eprintln!("market smoke failed: no parsed events received");
        std::process::exit(2);
    }
}
