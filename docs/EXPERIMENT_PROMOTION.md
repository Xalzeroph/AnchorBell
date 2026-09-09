# Risk-adjusted experiment promotion

AnchorBell treats an experiment result as evidence, not as an automatic deployment decision.

The selector requires an explicit policy file:

```bash
cargo run --release --bin anchorbell_oos_select -- \
  --policy config/anchorbell-promotion-policy.json \
  --input target/oos-candidates.json
```

The policy is part of the result contract. The selector emits its identifier and SHA-256 digest so a report can be reproduced after thresholds change.

## Decision path

- `insufficient_evidence`: the candidate does not yet have the configured OOS/stress folds, trades, or risk metrics.
- `rejected`: the candidate has enough data but violates a configured safety or economic gate.
- `paper_candidate`: the candidate passes the configured lower-tail return, drawdown, stress survival, risk-adjusted return, stability, and fee-drag gates.

A `paper_candidate` is not a production approval. Production execution remains a separate, manually authorized state and must add live/testnet evidence, operational readiness, and current Binance rule metadata.

## Why the policy is external

Thresholds are research and risk decisions. They belong in versioned JSON/TOML configuration, not in Rust defaults. This prevents a code release from silently changing the economic definition of “good enough” and makes policy changes reviewable, reproducible, and attributable.
