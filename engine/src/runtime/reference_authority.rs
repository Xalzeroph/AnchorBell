use crate::{
    execution::BinanceEnvironment,
    simulation::engine::{BinanceIndexAnchorSet, SimulationError},
};

/// Single authority for fetching and validating runtime anchor/reference data.
///
/// Consumers must use this port instead of reaching into a simulation or
/// execution implementation. The returned set is immutable for the caller's
/// run and carries the FX provenance used for local-currency reporting.
pub async fn fetch(
    environment: BinanceEnvironment,
    symbols: &[String],
    price_scale: u32,
    anchor_kline_interval: &str,
    http_proxy: Option<&str>,
) -> Result<BinanceIndexAnchorSet, SimulationError> {
    crate::simulation::engine::load_index_anchor_set_internal(
        environment,
        symbols,
        price_scale,
        anchor_kline_interval,
        http_proxy,
    )
    .await
}
