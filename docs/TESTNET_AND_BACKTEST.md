# Binance Testnet and Historical Replay

## Testnet boundary

AnchorBell now has an explicit Binance environment model. Testnet and production
carry different REST, market-data WebSocket, and order WebSocket endpoints.

The generic gateway remains a deterministic execution boundary. In addition, the
repository now has a concrete signed REST adapter used by the controlled
anchorbell_testnet runner for server time, open orders, position risk, order
status, post-only LIMIT + GTX placement, and cancellation. It never reads keys
from source files and never submits an order unless the explicit order policy
is enabled. The public paper runner consumes bookTicker, mark price, and
aggTrade streams without credentials.

Use testnet credentials only. Store API keys in environment variables or a
secret manager; never commit them, print them, or put them in a config file.
## What testnet can validate

Testnet is useful for checking authentication, symbol filters, order rejects,
post-only behavior, cancel/replace handling, reconnects, and the order lifecycle
against exchange acknowledgements.

It cannot prove profitability or reproduce historical fills. Testnet liquidity,
latency, matching, and available symbols can differ from production. Keep the
environment explicit in every run and require a deliberate production switch.

## Official Binance references

Binance's official `ai-trading-prototype-backtester` demonstrates configurable historical
runs, automatic `data.binance.vision` downloads, and durable text/HTML result reports.
It is Spot and kline-oriented, so it is a reference for ingestion and reporting rather
than the AnchorBell maker fill engine.

AnchorBell now exposes the same public-data boundary in Rust through
`historical::monthly_download_url` and the `binance_public_data_smoke` binary. The
adapter parses extracted Binance Futures UM CSV files for `klines`, `trades`, and
`aggTrades`; archive download and checksum verification remain an outer data-job
responsibility. It deliberately preserves decimal values as text until an instrument
metadata scale is known, preventing float conversion from changing prices or sizes.

The official Futures Demo endpoints remain useful for API lifecycle evidence, but a
symbol must be present and `TRADING` in Demo `exchangeInfo` before it can be used for
order testing. `/fapi/v1/order/test` only validates a request and never enters the
matching engine. Neither path replaces recorded production book data plus local maker
queue simulation for the A-share/Hong Kong target universe.

## Actual public archive verification (2026-09-02)

On the remote Windows host, the official CXMTUSDT UM Futures `1m` archive for
2026-08 was downloaded from `data.binance.vision`, verified against its SHA-256
checksum, extracted, and parsed by `binance_public_data_smoke`.

Result: `checksum_match=True`, archive size `547587` bytes, and `19860` valid
Kline rows covering `2026-08-18T05:00:00Z` through `2026-08-31T23:59:00Z`.
This proves the public-data adapter works against a real Binance archive; it is
not yet a strategy-profitability result because this archive contains Klines, not
the recorded book/anchor stream required by the maker replay contract.

## Replay and backtesting

The engine now accepts a deterministic, timestamp-ordered event stream containing
bookTicker, mark price, and anchor events. A recorded raw Binance WebSocket line
can be decoded into this stream through the replay boundary. EventReplay has no
network dependency: the same input must produce the same strategy decisions and
risk transitions.

The recording format is append-only JSONL: one raw WebSocket message per line,
with the local receipt timestamp retained by the recorder in the next adapter
layer. Files should be immutable once a replay run starts.
For this maker strategy, kline-only backtests are not sufficient. A useful first
dataset is recorded bookTicker plus mark price and anchor snapshots. A more
realistic fill model will additionally need trade/depth observations, local
receipt timestamps, order latency, queue position assumptions, and cancel timing.

The planned pipeline is:

1. Record public Binance market streams to immutable event files.
2. Normalize raw or combined WebSocket JSON into typed replay events.
3. Replay events through the same strategy, risk, and lifecycle code.
4. Report fills, inventory, exposure time, fees, rejects, and drawdown.
5. Compare replay decisions with testnet acknowledgements before any production
permission is considered.

Replay must fail fast on out-of-order timestamps or symbols that are not configured
for the run, so an accidental clock/file merge bug cannot silently change results.
At end of file it cancels remaining working quotes without synthesizing a fill. The
summary reports realized and mark-to-market PnL separately, includes
`unrealized_valuation_complete` and `flat_at_end`, and the CLI can enforce a complete
session with `--require-flat-at-end`.

## Read-only evidence (2026-09-01)

The following evidence was collected from the remote Windows host through the
repository's own smoke binaries. None of these runs loaded credentials or submitted
an order:

- Testnet public market stream: `BTCUSDT` produced 34 parsed `bookTicker` and
  `markPrice` events before the intentional 12-second timeout.
- Testnet public metadata: the nine reviewed A/H execution symbols were absent or
  `PENDING_TRADING`; all nine were rejected by the metadata diagnostic.
- Production public market stream: `CXMTUSDT` produced 11 parsed events. `UNITREEUSDT`
  connected but produced no WebSocket event in the 12-second window; its REST snapshot
  later had a two-sided quote, so the runtime must keep the stream-health gate.
- Production public metadata: eight of nine eligible symbols returned exchangeInfo,
  book ticker, mark/index, and funding snapshots. `CSOPSKHYNIX2LUSDT` returned HTTP
  418 during one run and was fail-closed rather than admitted.

These are connectivity and data-quality observations, not evidence of profitability,
fill quality, or permission to place orders.
## Safety gates

- Default environment: testnet.
- Production requires an explicit configuration change.
- Post-only is mandatory for entry orders.
- The session risk gate targets passive reduction before the earliest equity-open
  or funding deadline and reports any unfilled residual exposure.
- Stale market data, invalid anchors, position caps, or lifecycle rejects halt
  new entries.
- Backtest results must record assumptions; they are not live performance claims.

## Dual-environment gate and reporting

DeploymentConfig defaults to Testnet. Production read-only access requires
ANCHORBELL_ENABLE_PRODUCTION=1 and Production-specific credentials. Order submission
is a separate gate: ANCHORBELL_ENABLE_ORDER_SUBMISSION=1, plus the exact live-trading
confirmation string for Production. DeploymentPolicy fails closed before network
access when any required condition is absent. The runnable `anchorbell_backtest`
summary additionally reports mark-to-market PnL, valuation completeness, working
orders and `flat_at_end`; all prices, quantities and PnL remain integer ticks.
