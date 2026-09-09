# Contributing to AnchorBell

AnchorBell is a focused open-source industrial quantitative execution platform. Contributions
should preserve the separation between deterministic strategy/risk logic and
external exchange adapters.

## Before opening a pull request

- Explain the ownership boundary and invariant being changed.
- Add focused regression tests for behavioral changes.
- Update the owning documentation.
- Run `cargo fmt --all -- --check` and `cargo test --workspace --locked`.
- Never include API keys, account data, or authenticated payloads.

Keep commits narrow and describe the exact verification performed. Changes that weaken maker-first execution, session flattening, stale-data handling,
production safety, or the registered reduce-only emergency taker policy require
explicit design discussion.
