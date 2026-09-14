# Night simulation stability repair

## Runtime invariants

- An entry quantity reduced to zero by the edge hurdle remains zero. Both buy
  and sell paths reject it without panicking; reduce-only sizing is unchanged.
- Portfolio drawdown and performance sampling use accounting totals without
  cloning rejection diagnostics. Public summaries retain cumulative rejection
  counts and the recent 1024 diagnostic records.
- Live log rotation uses one lossless zstd level-3 pass. It retains the original
  SHA-256, byte count and line count metadata and removes the raw segment only
  after publishing the archive and metadata. No level-9 recompression runs on
  the live writer path. A level-3 pass can still briefly backpressure the writer.

## Automatic retention

Install `scripts/simulation_retention.py` to
`/opt/anchorbell-maintenance/simulation_retention.py`, install
`deploy/anchorbell-retention.service` and `.timer` into systemd and
enable the timer. It runs every 15 minutes as the simulation user, with bounded
CPU/memory and idle I/O priority. A manual read-only preview is:

```sh
python3 scripts/simulation_retention.py --root /var/lib/anchorbell
```

Only top-level, sealed `shared-market.jsonl.segment-*.zst` and
`evidence-opportunities.jsonl.segment-*.zst` files in simulation run directories
with a manifest are eligible. Their sidecar metadata must match the archive
size and expected schema. Symlinks, nested paths, active JSONL files, missing or
invalid sidecars are excluded. Defaults:

- Always retain at least 24 hours of raw archives.
- Prune oldest eligible archives when this archive category exceeds 8 GiB,
  disk free space is below 8 GiB, or an archive is over 7 days old.
- Preserve strategy records (including fills), summaries, manifests, calibration,
  FX records, archive metadata and retention audit records.
- Persist a per-run `retention-status.json` marking raw replay incomplete before
  deletion. `retention-audit.jsonl` records intent and completion. Cleanup never
  converts a partial history into a valid full replay claim.
- With no eligible files, report pressure and exit unsuccessfully. Never remove
  protected results to meet the budget. The existing 4 GiB simulation stop stays.

The 8 GiB budget covers only eligible archive categories, not all server data.
Protected result growth, young files, other applications and build caches may
still exhaust disk space. `retention-status.json` at the data root records the
latest outcome; `journalctl -u anchorbell-retention` records failures. This does
not configure external notifications.

## Build and deployment

Run workspace tests and clippy on a separate host where possible. On the small
ECS host, use `CARGO_BUILD_JOBS=1`, a disk-backed `TMPDIR` under `/var/tmp`, and
resource controls. `/tmp` is tmpfs here: large compiler temporary files consume
RAM. Do not run an unrestricted parallel compiler beside the simulation.

Keep the old binary and batch results when deploying. Build into a staging
target, stop the simulation gracefully, install the verified binary, then start
a new batch. Verify the manifest build identity, service restarts, event progress
and latency. Never delete old batches to make the latest run look clean.

### September 12 integration

Before deployment the server had newer uncommitted strategy changes on top of
`8f2c08d9`. A snapshot of those changes was merged into the repair branch, including
the newer economic-boundary rule: quantity is zero at or below the edge hurdle.
The regression preserves that rule. Metrics serialization was moved unchanged
into `runtime_metrics.rs` to keep the combined runtime within the source budget.

The deployed executable lives under `/opt/anchorbell-releases/<commit>/` and is
selected by `anchorbell-simulation.service.d/30-verified-release.conf`. The
server's uncommitted strategy changes are preserved.
Future deployments must update this override to the new verified release;
rebuilding the old checkout alone does not change the running version.

The stability patch is also synchronized into the original working tree while
preserving its existing uncommitted strategy edits, so subsequent builds keep
the fixes. The reviewed release branch records the combined source snapshot.

## Dashboard response budget

The run index retains financial totals for every run but includes only the
latest run's last 120 rejection details. Older details remain on disk; the API
sets its truncation flag and the UI explains the omission. This avoids returning
about 20 MB of repeated historical rejection records on every refresh. The
browser also skips refresh ticks while a previous request batch is pending. A
15-second abort deadline covers response headers and JSON bodies; timeout keeps
the last successful snapshot and permits the next refresh to retry.

Additional checks:

```sh
cargo test --locked --bin anchorbell_dashboard
node --test scripts/test_dashboard_refresh.cjs
node --check engine/web/app.js
```

## Verification

The new zero-quantity test reproduced `min > max` before the guard was added;
the archive test detected the old second compression pass. Retention fixtures
exercise budget ordering, expiry, dry-run behavior and protected data. Commands:

```sh
cargo fmt --all -- --check
cargo test --workspace --locked
cargo clippy --workspace --all-targets --all-features --locked -- -D warnings
python3 scripts/architecture_gate.py
python3 scripts/resource_gate.py
python3 -m unittest discover -s scripts -p test_simulation_retention.py -v
```
