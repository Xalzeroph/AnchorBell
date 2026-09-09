# AnchorBell release checklist

A release is eligible only when the exact commit, commands, artifacts, and
environment are recorded together.

## Automated gates

- [ ] portable Git status is clean
- [ ] cargo fmt --all -- --check
- [ ] cargo test --workspace --locked
- [ ] cargo clippy --workspace --all-targets --all-features -- -D warnings
- [ ] cargo run -p anchorbell-engine --bin market_throughput_smoke --locked
- [ ] cargo metadata --locked --format-version 1 is retained as the dependency SBOM input

## Runtime gates

- [ ] simulation run records market, strategy, funding, fee, queue, and latency assumptions
- [ ] /health, /live, and /ready return the expected state
- [ ] checkpoint restore enters RiskStopped before reconciliation
- [ ] invalid, stale, contradictory, and unknown state fails closed
- [ ] no risk-increasing taker intent can be produced; any emergency taker intent is reduce-only, policy-registered, and fully audited
- [ ] production remains disabled by default

## Evidence gates

- [ ] dataset and configuration SHA-256 values are recorded
- [ ] replay report declares time, fill, queue, latency, fee, funding, and flatness assumptions
- [ ] Testnet evidence is retained separately from simulation evidence
- [ ] no credentials or signed payloads are present in artifacts
- [ ] production safety review is separate and explicit
