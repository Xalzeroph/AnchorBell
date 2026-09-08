use serde::Serialize;

pub const SIMULATION_MANIFEST_SCHEMA_VERSION: u16 = 2;

/// Stable identity shared by replay, single-run simulation and batch runs.
/// The manifest is operational evidence with explicit policy lineage.
#[derive(Debug, Clone, Serialize)]
pub struct SimulationRunManifest {
    pub schema_version: u16,
    pub run_id: String,
    pub mode: String,
    pub policy_id: String,
    pub parent_policy_id: Option<String>,
    pub parameter_digest: String,
    pub data_digest: String,
    pub created_at_ms: u64,
    pub effective_from_ms: u64,
    pub effective_until_ms: Option<u64>,
    pub approval_state: String,
    pub rollback_target: Option<String>,
    pub build_identity: String,
    pub symbols: Vec<String>,
    pub policy_variants: Vec<String>,
}

impl SimulationRunManifest {
    pub fn compiled_build_identity() -> String {
        let configured = std::env::var("ANCHORBELL_BUILD_IDENTITY").ok();
        configured.unwrap_or_else(|| {
            format!(
                "pkg:{};git:{};tree:{};rustc:{}",
                env!("CARGO_PKG_VERSION"),
                option_env!("ANCHORBELL_GIT_SHA").unwrap_or("unknown"),
                option_env!("ANCHORBELL_GIT_DIRTY").unwrap_or("unknown"),
                option_env!("ANCHORBELL_RUSTC").unwrap_or("unknown"),
            )
        })
    }

    pub fn new(
        run_id: impl Into<String>,
        mode: impl Into<String>,
        policy_id: impl Into<String>,
        created_at_ms: u64,
        symbols: Vec<String>,
        policy_variants: Vec<String>,
    ) -> Self {
        Self {
            schema_version: SIMULATION_MANIFEST_SCHEMA_VERSION,
            run_id: run_id.into(),
            mode: mode.into(),
            policy_id: policy_id.into(),
            parent_policy_id: None,
            parameter_digest: "sha256:uncomputed".to_owned(),
            data_digest: "sha256:uncomputed".to_owned(),
            created_at_ms,
            effective_from_ms: created_at_ms,
            effective_until_ms: None,
            approval_state: "isolated".to_owned(),
            rollback_target: None,
            build_identity: Self::compiled_build_identity(),
            symbols,
            policy_variants,
        }
    }

    pub fn with_lineage(
        mut self,
        parent_policy_id: Option<String>,
        parameter_digest: impl Into<String>,
        data_digest: impl Into<String>,
        approval_state: impl Into<String>,
        rollback_target: Option<String>,
    ) -> Self {
        self.parent_policy_id = parent_policy_id;
        self.parameter_digest = parameter_digest.into();
        self.data_digest = data_digest.into();
        self.approval_state = approval_state.into();
        self.rollback_target = rollback_target;
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn manifest_identity_is_explicit_and_versioned() {
        let manifest = SimulationRunManifest::new(
            "run-001",
            "batch",
            "policy-v1",
            42,
            vec!["CXMTUSDT".to_owned()],
            vec!["baseline".to_owned()],
        )
        .with_lineage(
            Some("policy-v0".to_owned()),
            "sha256:params",
            "sha256:data",
            "isolated",
            Some("policy-v0".to_owned()),
        );
        assert_eq!(manifest.schema_version, SIMULATION_MANIFEST_SCHEMA_VERSION);
        assert_eq!(manifest.policy_id, "policy-v1");
        assert_eq!(manifest.parent_policy_id.as_deref(), Some("policy-v0"));
        assert_eq!(manifest.policy_variants, vec!["baseline"]);
        assert!(!SimulationRunManifest::compiled_build_identity().is_empty());
        assert!(!SimulationRunManifest::compiled_build_identity().contains("unknown-build"));
    }
}
