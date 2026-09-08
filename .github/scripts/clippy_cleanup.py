from pathlib import Path


def replace_once(path: str, old: str, new: str) -> None:
    p = Path(path)
    text = p.read_text()
    count = text.count(old)
    if count != 1:
        raise SystemExit(f"expected one anchor in {path}, found {count}: {old[:100]!r}")
    p.write_text(text.replace(old, new, 1))
    print(f"patched {path}")

# Windows-only FFI must not create Linux dead-code/import warnings. Test-only
# serialization helpers remain available to platform-neutral unit tests.
p = Path("engine/src/execution/credential_store.rs")
text = p.read_text()
text = text.replace(
    'use std::{ffi::c_void, ptr};\n\nuse serde::{Deserialize, Serialize};',
    '#[cfg(windows)]\nuse std::{ffi::c_void, ptr};\n\n#[cfg(any(windows, test))]\nuse serde::{Deserialize, Serialize};',
    1,
)
text = text.replace(
    'const TARGET_PREFIX: &str = "AnchorBell/Binance/";\nconst MAX_CREDENTIAL_BLOB_BYTES: usize = 2_560;',
    '#[cfg(any(windows, test))]\nconst TARGET_PREFIX: &str = "AnchorBell/Binance/";\n#[cfg(windows)]\nconst MAX_CREDENTIAL_BLOB_BYTES: usize = 2_560;',
    1,
)
text = text.replace(
    '#[derive(Debug, Serialize, Deserialize)]\nstruct StoredCredentials {',
    '#[cfg(any(windows, test))]\n#[derive(Debug, Serialize, Deserialize)]\nstruct StoredCredentials {',
    1,
)
text = text.replace(
    'fn target_name(environment: BinanceEnvironment) -> String {',
    '#[cfg(any(windows, test))]\nfn target_name(environment: BinanceEnvironment) -> String {',
    1,
)
text = text.replace(
    'fn to_credentials(stored: StoredCredentials) -> Result<BinanceCredentials, CredentialStoreError> {',
    '#[cfg(windows)]\nfn to_credentials(stored: StoredCredentials) -> Result<BinanceCredentials, CredentialStoreError> {',
    1,
)
p.write_text(text)
print("patched engine/src/execution/credential_store.rs")

# The coordinator snapshot was a stale diagnostic API with no callers.
replace_once(
    "engine/src/network.rs",
    '''\n    pub async fn snapshot(&self) -> BTreeMap<RequestClass, (u64, u64, Option<u16>)> {\n        self.buckets\n            .lock()\n            .await\n            .iter()\n            .map(|(class, bucket)| {\n                (\n                    *class,\n                    (bucket.requests, bucket.throttled, bucket.last_status),\n                )\n            })\n            .collect()\n    }\n''',
    '\n',
)

# Name the runtime spec instead of exporting a complex nested tuple signature.
p = Path("engine/src/simulation/experiment_plan.rs")
text = p.read_text()
text = text.replace(
    'use std::collections::BTreeSet;\n',
    'use std::collections::BTreeSet;\n\npub type RuntimeExperimentSpec = (String, SimulationPolicyVariant, Vec<String>);\n',
    1,
)
old_signature = "Result<Vec<(String, SimulationPolicyVariant, Vec<String>)>, &'static str>"
new_signature = "Result<Vec<RuntimeExperimentSpec>, &'static str>"
if text.count(old_signature) != 1:
    raise SystemExit(
        f"expected one runtime spec return type, found {text.count(old_signature)}"
    )
text = text.replace(old_signature, new_signature, 1)
p.write_text(text)
print("patched engine/src/simulation/experiment_plan.rs")

# Keep test-only compatibility conversions out of the production lib, remove
# no-op integer clamps, and document the intentionally explicit audit signature.
p = Path("engine/src/simulation/runtime.rs")
text = p.read_text()
text = text.replace(
    '/// Compatibility diagnostic; admission uses edge_pico_bps directly.\nfn edge_micro_bps(',
    '/// Compatibility diagnostic used by precision regression tests.\n#[cfg(test)]\nfn edge_micro_bps(',
    1,
)
text = text.replace(
    '''\nfn edge_bps(numerator_price: i64, denominator_price: i64) -> Option<i64> {\n    edge_pico_bps(numerator_price, denominator_price).map(pico_bps_to_bps)\n}\n''',
    '\n',
    1,
)
text = text.replace(
    '\nfn micro_bps_to_bps(value: i64) -> i64 {',
    '\n#[cfg(test)]\nfn micro_bps_to_bps(value: i64) -> i64 {',
    1,
)
text = text.replace(
    '''\nfn m5_tail_risk_bps(state: &SimulationSymbolState) -> i64 {\n    pico_bps_to_bps(m5_tail_risk_pico(state))\n}\n''',
    '\n',
    1,
)
text = text.replace(
    '\nfn decision_audit(\n',
    '\n#[allow(clippy::too_many_arguments)]\nfn decision_audit(\n',
    1,
)
text = text.replace(
    '    let position_quantity = position_quantity.min(i64::MAX);\n',
    '',
    1,
)
text = text.replace(
    '        values[3].min(i64::MAX),\n',
    '        values[3],\n',
    1,
)
p.write_text(text)
print("patched engine/src/simulation/runtime.rs")

replace_once(
    "engine/src/validation_contracts.rs",
    '''    pub fn len(&self) -> usize {\n        self.episodes.len()\n    }\n\n    pub fn episodes(&self) -> &[ClosureEpisode] {''',
    '''    pub fn len(&self) -> usize {\n        self.episodes.len()\n    }\n\n    pub fn is_empty(&self) -> bool {\n        self.episodes.is_empty()\n    }\n\n    pub fn episodes(&self) -> &[ClosureEpisode] {''',
)

print("clippy cleanup complete")
