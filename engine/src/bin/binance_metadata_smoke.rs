use std::time::{SystemTime, UNIX_EPOCH};

use anchorbell_engine::execution::DeploymentConfig;
use anchorbell_engine::market::PublicMarketMetadataClient;
use anchorbell_engine::strategy::all_instruments;

#[tokio::main]
async fn main() {
    let deployment = match DeploymentConfig::from_process_environment() {
        Ok(config) => config,
        Err(error) => {
            eprintln!("metadata smoke configuration rejected before network: {error:?}");
            std::process::exit(2);
        }
    };
    let client = match PublicMarketMetadataClient::new(
        deployment.environment.endpoints().rest_base.as_str(),
        std::env::var("ANCHORBELL_HTTP_PROXY").ok().as_deref(),
    ) {
        Ok(client) => client,
        Err(error) => {
            eprintln!("metadata client construction failed: {error}");
            std::process::exit(2);
        }
    };

    let infos = match client.exchange_info().await {
        Ok(infos) => infos,
        Err(error) => {
            eprintln!("exchangeInfo failed: {error}");
            std::process::exit(2);
        }
    };
    let mut failures = 0_u32;
    let mut checked = 0_u32;
    let mut pending_symbols = Vec::new();
    let mut pending_metadata = Vec::new();
    for instrument in all_instruments() {
        checked += 1;
        let Some(metadata) = infos
            .iter()
            .find(|metadata| metadata.symbol == instrument.symbol)
            .cloned()
        else {
            failures += 1;
            println!("symbol={} state=missing_exchange_info", instrument.symbol);
            continue;
        };

        if !metadata.is_trading_tradifi_perpetual() {
            failures += 1;
            println!(
                "symbol={} state=not_trading_tradifi_perpetual status={} contract_type={}",
                instrument.symbol, metadata.status, metadata.contract_type
            );
            continue;
        }
        pending_symbols.push(instrument.symbol);
        pending_metadata.push(metadata);
    }

    for (symbol, result) in pending_symbols
        .into_iter()
        .zip(client.symbol_snapshots(pending_metadata, 8).await)
    {
        match result {
            Ok(snapshot) => match snapshot.validate_for_runtime(now_ms()) {
                Ok(()) => {
                    let filters = snapshot
                        .metadata
                        .execution_filters()
                        .expect("runtime validation already checked exchange filters");
                    println!(
                        "symbol={} state=ok bid={} ask={} mark={} index={} funding={} next_funding={} price_tick={} quantity_step={} min_notional={} two_sided_quote={}",
                        symbol,
                        snapshot.book_ticker.bid_price,
                        snapshot.book_ticker.ask_price,
                        snapshot.premium_index.mark_price,
                        snapshot.premium_index.index_price,
                        snapshot.premium_index.last_funding_rate,
                        snapshot.premium_index.next_funding_time_ms,
                        filters.price_tick,
                        filters.quantity_step,
                        filters.min_notional,
                        snapshot.book_ticker.has_two_sided_quote()
                    );
                }
                Err(error) => {
                    failures += 1;
                    println!(
                        "symbol={} state=invalid_runtime_metadata error={error}",
                        symbol
                    );
                }
            },
            Err(error) => {
                failures += 1;
                println!(
                    "symbol={} state=public_snapshot_error error={error}",
                    symbol
                );
            }
        }
    }
    println!(
        "metadata_smoke_environment={} checked={} failures={}",
        deployment.environment, checked, failures
    );
    if failures != 0 {
        std::process::exit(2);
    }
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock must be after UNIX epoch")
        .as_millis() as u64
}
