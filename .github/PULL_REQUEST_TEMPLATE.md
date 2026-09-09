## Change summary

<!-- State the behavior, ownership boundary, and invariant being changed. -->

## Architecture checklist

- [ ] The change has one clear ownership boundary.
- [ ] Strategy and risk remain independent of network, credentials, and persistence.
- [ ] Business values come from Binance metadata, explicit user input, or versioned JSON/TOML.
- [ ] No production business default is hidden in Rust source or Default implementations.
- [ ] Execution changes preserve maker-first behavior and make emergency taker use strictly reduce-only.
- [ ] Experiment changes preserve manifest, lineage, fee, latency, queue, and data-source provenance.
- [ ] Unknown, stale, contradictory, or unreconciled state still fails closed.
- [ ] No credentials, signed payloads, or sensitive account data are included.

## Verification

- Commands:
- Commit/config/data identifiers:
- Evidence or report location:

## Risk and rollback

- Operational risk:
- Rollback or recovery path:
- Production capability impact:
