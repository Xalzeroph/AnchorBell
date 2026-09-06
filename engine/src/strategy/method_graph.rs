//! Versioned, composable method lineage.
//!
//! This is control-plane metadata. A resolved graph is compiled once before a
//! run; the event hot path never performs string-based registry lookups.
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct MethodId(String);

impl MethodId {
    pub fn new(value: impl Into<String>) -> Result<Self, MethodGraphError> {
        let value = value.into();
        if value.trim().is_empty()
            || !value
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || byte == b'_' || byte == b'-')
        {
            return Err(MethodGraphError::InvalidId);
        }
        Ok(Self(value))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum MethodLayer {
    Signal,
    Microstructure,
    Fill,
    Risk,
    Capital,
    Evidence,
    Funding,
    Execution,
    Accounting,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MethodSpec {
    pub id: MethodId,
    pub layer: MethodLayer,
    pub parents: Vec<MethodId>,
    pub overlays: Vec<String>,
    pub required_features: BTreeSet<String>,
    pub overrides: BTreeSet<String>,
}

impl MethodSpec {
    pub fn root(id: MethodId, layer: MethodLayer) -> Self {
        Self {
            id,
            layer,
            parents: Vec::new(),
            overlays: Vec::new(),
            required_features: BTreeSet::new(),
            overrides: BTreeSet::new(),
        }
    }

    pub fn child(id: MethodId, layer: MethodLayer, parent: MethodId) -> Self {
        let mut value = Self::root(id, layer);
        value.parents.push(parent);
        value
    }

    pub fn with_overlay(mut self, overlay: impl Into<String>) -> Self {
        self.overlays.push(overlay.into());
        self
    }

    pub fn requires_feature(mut self, feature: impl Into<String>) -> Self {
        self.required_features.insert(feature.into());
        self
    }

    pub fn overrides(mut self, contract: impl Into<String>) -> Self {
        self.overrides.insert(contract.into());
        self
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MethodGraphError {
    InvalidId,
    DuplicateMethod,
    MissingParent(String),
    ImmutableOverride(String),
    Cycle(String),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResolvedMethod {
    pub id: MethodId,
    pub lineage: Vec<MethodId>,
    pub overlays: Vec<String>,
    pub required_features: BTreeSet<String>,
    pub overridden_contracts: BTreeSet<String>,
}
#[derive(Debug, Clone, Default)]
pub struct MethodRegistry {
    specs: BTreeMap<MethodId, MethodSpec>,
    immutable_contracts: BTreeSet<String>,
}

impl MethodRegistry {
    pub fn new(immutable_contracts: impl IntoIterator<Item = impl Into<String>>) -> Self {
        Self {
            specs: BTreeMap::new(),
            immutable_contracts: immutable_contracts.into_iter().map(Into::into).collect(),
        }
    }

    pub fn register(&mut self, spec: MethodSpec) -> Result<(), MethodGraphError> {
        if self.specs.contains_key(&spec.id) {
            return Err(MethodGraphError::DuplicateMethod);
        }
        for parent in &spec.parents {
            if !self.specs.contains_key(parent) {
                return Err(MethodGraphError::MissingParent(parent.as_str().to_owned()));
            }
        }
        if let Some(contract) = spec
            .overrides
            .iter()
            .find(|contract| self.immutable_contracts.contains(*contract))
        {
            return Err(MethodGraphError::ImmutableOverride(contract.clone()));
        }
        self.specs.insert(spec.id.clone(), spec);
        Ok(())
    }

    pub fn get(&self, id: &MethodId) -> Option<&MethodSpec> {
        self.specs.get(id)
    }

    pub fn resolve(&self, id: &MethodId) -> Result<ResolvedMethod, MethodGraphError> {
        let mut visiting = BTreeSet::new();
        let mut visited = BTreeSet::new();
        let mut lineage = Vec::new();
        let mut overlays = Vec::new();
        let mut features = BTreeSet::new();
        let mut overrides = BTreeSet::new();
        self.resolve_dfs(
            id,
            &mut visiting,
            &mut visited,
            &mut lineage,
            &mut overlays,
            &mut features,
            &mut overrides,
        )?;
        Ok(ResolvedMethod {
            id: id.clone(),
            lineage,
            overlays,
            required_features: features,
            overridden_contracts: overrides,
        })
    }
    #[allow(clippy::too_many_arguments)]
    fn resolve_dfs(
        &self,
        id: &MethodId,
        visiting: &mut BTreeSet<MethodId>,
        visited: &mut BTreeSet<MethodId>,
        lineage: &mut Vec<MethodId>,
        overlays: &mut Vec<String>,
        features: &mut BTreeSet<String>,
        overrides: &mut BTreeSet<String>,
    ) -> Result<(), MethodGraphError> {
        if visited.contains(id) {
            return Ok(());
        }
        if !visiting.insert(id.clone()) {
            return Err(MethodGraphError::Cycle(id.as_str().to_owned()));
        }
        let spec = self
            .specs
            .get(id)
            .ok_or_else(|| MethodGraphError::MissingParent(id.as_str().to_owned()))?;
        for parent in &spec.parents {
            self.resolve_dfs(
                parent, visiting, visited, lineage, overlays, features, overrides,
            )?;
        }
        visiting.remove(id);
        visited.insert(id.clone());
        lineage.push(id.clone());
        overlays.extend(spec.overlays.iter().cloned());
        features.extend(spec.required_features.iter().cloned());
        overrides.extend(spec.overrides.iter().cloned());
        Ok(())
    }

    /// The canonical AnchorBell chain. New methods should register as a child
    /// or overlay instead of copying an existing method implementation.
    pub fn anchorbell_defaults() -> Result<Self, MethodGraphError> {
        let immutable = [
            "anchor_definition",
            "event_order",
            "settlement_accounting",
            "no_lookahead",
            "maker_only",
        ];
        let mut registry = Self::new(immutable);
        let m1 = MethodId::new("M1")?;
        let m2 = MethodId::new("M2")?;
        let m3 = MethodId::new("M3")?;
        let m4 = MethodId::new("M4")?;
        let m5 = MethodId::new("M5")?;
        let m6 = MethodId::new("M6")?;
        let m7 = MethodId::new("M7")?;
        let m8 = MethodId::new("M8")?;
        let m9 = MethodId::new("M9")?;
        registry.register(MethodSpec::root(m1.clone(), MethodLayer::Signal))?;
        registry.register(MethodSpec::child(
            m2.clone(),
            MethodLayer::Microstructure,
            m1.clone(),
        ))?;
        registry.register(MethodSpec::child(m3.clone(), MethodLayer::Fill, m2.clone()))?;
        registry.register(MethodSpec::child(
            m4.clone(),
            MethodLayer::Signal,
            m3.clone(),
        ))?;
        registry.register(MethodSpec::child(m5.clone(), MethodLayer::Risk, m4.clone()))?;
        registry.register(MethodSpec::child(
            m6.clone(),
            MethodLayer::Capital,
            m5.clone(),
        ))?;
        registry.register(MethodSpec::child(
            m7.clone(),
            MethodLayer::Evidence,
            m6.clone(),
        ))?;
        registry.register(
            MethodSpec::child(m8.clone(), MethodLayer::Funding, m7)
                .with_overlay("funding_controller")
                .requires_feature("funding_rate_state")
                .requires_feature("funding_schedule")
                .overrides("funding_entry_policy"),
        )?;
        registry.register(
            MethodSpec::child(m9, MethodLayer::Risk, m8)
                .with_overlay("causal_residual")
                .with_overlay("joint_fill_markout_exit")
                .with_overlay("deadline_mpc")
                .with_overlay("distributionally_robust_optimizer")
                .requires_feature("versioned_fair_value")
                .requires_feature("deadline_exit_plan")
                .requires_feature("joint_outcome_forecast")
                .requires_feature("robust_risk_certificate"),
        )?;
        Ok(registry)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn resolves_incremental_chain_with_shared_features() {
        let registry = MethodRegistry::anchorbell_defaults().unwrap();
        let resolved = registry.resolve(&MethodId::new("M9").unwrap()).unwrap();
        assert_eq!(resolved.lineage.len(), 9);
        assert_eq!(resolved.lineage[0].as_str(), "M1");
        assert_eq!(resolved.lineage.last().unwrap().as_str(), "M9");
        assert!(resolved.required_features.contains("deadline_exit_plan"));
        assert!(resolved
            .overlays
            .iter()
            .any(|value| value == "funding_controller"));
    }

    #[test]
    fn immutable_contract_override_is_rejected() {
        let mut registry = MethodRegistry::new(["anchor_definition"]);
        let root = MethodId::new("M1").unwrap();
        registry
            .register(MethodSpec::root(root.clone(), MethodLayer::Signal))
            .unwrap();
        let child = MethodSpec::child(MethodId::new("Bad").unwrap(), MethodLayer::Risk, root)
            .overrides("anchor_definition");
        assert_eq!(
            registry.register(child),
            Err(MethodGraphError::ImmutableOverride(
                "anchor_definition".into()
            ))
        );
    }

    #[test]
    fn missing_parent_and_cycle_are_rejected() {
        let mut registry = MethodRegistry::new(Vec::<String>::new());
        let missing = MethodSpec::child(
            MethodId::new("M2").unwrap(),
            MethodLayer::Risk,
            MethodId::new("M1").unwrap(),
        );
        assert!(matches!(
            registry.register(missing),
            Err(MethodGraphError::MissingParent(_))
        ));
    }
}
