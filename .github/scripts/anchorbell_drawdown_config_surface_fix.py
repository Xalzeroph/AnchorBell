from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
RUNTIME = ROOT / "engine" / "src" / "simulation" / "runtime.rs"
BACKTEST = ROOT / "engine" / "src" / "bin" / "anchorbell_backtest.rs"
BATCH = ROOT / "engine" / "src" / "simulation_batch.rs"
PROFILE = ROOT / "engine" / "src" / "strategy" / "config.rs"
BATCH_BIN = ROOT / "engine" / "src" / "bin" / "anchorbell_simulation_batch.rs"


def replace_once(text: str, old: str, new: str, label: str) -> str:
    if old in text:
        return text.replace(old, new, 1)
    if new in text:
        return text
    raise SystemExit(f"missing anchor: {label}")


runtime = RUNTIME.read_text(encoding="utf-8")
backtest = BACKTEST.read_text(encoding="utf-8")
batch = BATCH.read_text(encoding="utf-8")
profile = PROFILE.read_text(encoding="utf-8")
batch_bin = BATCH_BIN.read_text(encoding="utf-8")

# ReplayConfig: one compact optional tuple keeps runtime.rs within its source budget.
runtime = replace_once(
    runtime,
    '''    pub funding_controller_enabled: bool,
    pub capital_usdt_ticks: Option<i64>,
    pub calibration_updates_enabled: bool,''',
    '''    pub funding_controller_enabled: bool,
    pub capital_usdt_ticks: Option<i64>,
    pub portfolio_drawdown_limits_bps: Option<(i64, i64)>,
    pub calibration_updates_enabled: bool,''',
    "ReplayConfig drawdown limits",
)
runtime = replace_once(
    runtime,
    '''            funding_controller_enabled: true,
            capital_usdt_ticks: None,
            calibration_updates_enabled: true,''',
    '''            funding_controller_enabled: true,
            capital_usdt_ticks: None,
            portfolio_drawdown_limits_bps: None,
            calibration_updates_enabled: true,''',
    "legacy ReplayConfig drawdown default",
)
runtime = replace_once(
    runtime,
    '''    if let Some(allocations) = allocations {
        engine = engine.with_position_allocations(allocations)?;
    }

    let reader = BufReader::new(File::open(input_path)?);''',
    '''    if let Some(allocations) = allocations {
        engine = engine.with_position_allocations(allocations)?;
    }
    if let Some((soft, hard)) = config.portfolio_drawdown_limits_bps {
        let capital = config
            .capital_usdt_ticks
            .ok_or(SimulationError::InvalidConfig("drawdown limits require capital"))?;
        engine = engine.with_portfolio_drawdown_limits_bps(capital, soft, hard)?;
    }

    let reader = BufReader::new(File::open(input_path)?);''',
    "replay drawdown builder hookup",
)

# Backtest CLI and report lineage.
backtest = replace_once(
    backtest,
    '''    funding_controller_enabled: bool,
    capital_usdt_ticks: Option<i64>,
    require_flat_at_end: bool,''',
    '''    funding_controller_enabled: bool,
    capital_usdt_ticks: Option<i64>,
    portfolio_drawdown_soft_limit_bps: i64,
    portfolio_drawdown_hard_limit_bps: i64,
    require_flat_at_end: bool,''',
    "backtest drawdown args",
)
backtest = replace_once(
    backtest,
    '''            funding_controller_enabled: args.funding_controller_enabled,
            capital_usdt_ticks: args.capital_usdt_ticks,
            calibration_updates_enabled: !args.freeze_calibration,''',
    '''            funding_controller_enabled: args.funding_controller_enabled,
            capital_usdt_ticks: args.capital_usdt_ticks,
            portfolio_drawdown_limits_bps: drawdown_limits(
                args.portfolio_drawdown_soft_limit_bps,
                args.portfolio_drawdown_hard_limit_bps,
            ),
            calibration_updates_enabled: !args.freeze_calibration,''',
    "backtest ReplayConfig drawdown",
)
backtest = replace_once(
    backtest,
    '''        "capital_usdt_ticks": args.capital_usdt_ticks,
        "max_mark_index_gap_bps": args.max_mark_index_gap_bps,''',
    '''        "capital_usdt_ticks": args.capital_usdt_ticks,
        "portfolio_drawdown_soft_limit_bps": args.portfolio_drawdown_soft_limit_bps,
        "portfolio_drawdown_hard_limit_bps": args.portfolio_drawdown_hard_limit_bps,
        "max_mark_index_gap_bps": args.max_mark_index_gap_bps,''',
    "backtest drawdown report",
)
backtest = replace_once(
    backtest,
    '''    let mut funding_controller_enabled = true;
    let mut capital_usdt_ticks = None;
    let mut require_flat_at_end = false;''',
    '''    let mut funding_controller_enabled = true;
    let mut capital_usdt_ticks = None;
    let mut portfolio_drawdown_soft_limit_bps = 0;
    let mut portfolio_drawdown_hard_limit_bps = 0;
    let mut require_flat_at_end = false;''',
    "backtest drawdown parser state",
)
backtest = replace_once(
    backtest,
    '''            "--capital-usdt" => {
                capital_usdt_ticks = Some(parse_usdt_ticks(&next(&mut args, &flag)?)?)
            }
            "--require-flat-at-end" => require_flat_at_end = true,''',
    '''            "--capital-usdt" => {
                capital_usdt_ticks = Some(parse_usdt_ticks(&next(&mut args, &flag)?)?)
            }
            "--portfolio-drawdown-soft-bps" => {
                portfolio_drawdown_soft_limit_bps = parse(&mut args, &flag)?
            }
            "--portfolio-drawdown-hard-bps" => {
                portfolio_drawdown_hard_limit_bps = parse(&mut args, &flag)?
            }
            "--require-flat-at-end" => require_flat_at_end = true,''',
    "backtest drawdown flags",
)
backtest = replace_once(
    backtest,
    '''        funding_controller_enabled,
        capital_usdt_ticks,
        require_flat_at_end,''',
    '''        funding_controller_enabled,
        capital_usdt_ticks,
        portfolio_drawdown_soft_limit_bps,
        portfolio_drawdown_hard_limit_bps,
        require_flat_at_end,''',
    "backtest drawdown Args construction",
)
backtest = replace_once(
    backtest,
    '''    Ok(Args {
''',
    '''    drawdown_limits(
        portfolio_drawdown_soft_limit_bps,
        portfolio_drawdown_hard_limit_bps,
    )
    .map(|_| ())
    .ok_or_else(|| {
        "portfolio drawdown requires 0/0 or 0 < soft < hard <= 10000 bps".to_owned()
    })?;
    if drawdown_limits(portfolio_drawdown_soft_limit_bps, portfolio_drawdown_hard_limit_bps)
        .is_some()
        && capital_usdt_ticks.is_none()
    {
        return Err("portfolio drawdown limits require --capital-usdt".to_owned());
    }
    Ok(Args {
''',
    "backtest drawdown validation",
)
if "fn drawdown_limits(" not in backtest:
    marker = '''fn parse_usdt_ticks(value: &str) -> Result<i64, String> {'''
    helper = '''fn drawdown_limits(soft: i64, hard: i64) -> Option<(i64, i64)> {
    if soft == 0 && hard == 0 {
        None
    } else if soft > 0 && hard > soft && hard <= 10_000 {
        Some((soft, hard))
    } else {
        None
    }
}

fn parse_usdt_ticks(value: &str) -> Result<i64, String> {'''
    if marker not in backtest:
        raise SystemExit("missing anchor: backtest drawdown helper")
    backtest = backtest.replace(marker, helper, 1)
backtest = backtest.replace(
    '--strategy-variant m0..m9 --capital-usdt N',
    '--strategy-variant m0..m9 --capital-usdt N --portfolio-drawdown-soft-bps N --portfolio-drawdown-hard-bps N',
    1,
)

# Batch config and manifest lineage.
batch = replace_once(
    batch,
    '''    pub position_allocations: Option<BTreeMap<String, PositionAllocation>>,
    pub output_root: PathBuf,''',
    '''    pub position_allocations: Option<BTreeMap<String, PositionAllocation>>,
    pub portfolio_drawdown_soft_limit_bps: i64,
    pub portfolio_drawdown_hard_limit_bps: i64,
    pub output_root: PathBuf,''',
    "batch drawdown config",
)
batch = replace_once(
    batch,
    '''    if let Some(allocations) = config.position_allocations.clone() {
        engine = engine.with_position_allocations(allocations)?;
    }
    Ok(engine)''',
    '''    if let Some(allocations) = config.position_allocations.clone() {
        engine = engine.with_position_allocations(allocations)?;
    }
    if config.portfolio_drawdown_soft_limit_bps != 0
        || config.portfolio_drawdown_hard_limit_bps != 0
    {
        let capital = config
            .position_allocations
            .as_ref()
            .map(|allocations| {
                allocations.values().fold(0_i64, |total, allocation| {
                    total.saturating_add(allocation.budget_usdt_ticks)
                })
            })
            .unwrap_or(0);
        engine = engine.with_portfolio_drawdown_limits_bps(
            capital,
            config.portfolio_drawdown_soft_limit_bps,
            config.portfolio_drawdown_hard_limit_bps,
        )?;
    }
    Ok(engine)''',
    "batch drawdown engine hookup",
)
batch = replace_once(
    batch,
    '''        "policy_id": config.policy_id,
        "m9_calibration_source_label": config.m9_calibration_source_label,''',
    '''        "policy_id": config.policy_id,
        "portfolio_drawdown_soft_limit_bps": config.portfolio_drawdown_soft_limit_bps,
        "portfolio_drawdown_hard_limit_bps": config.portfolio_drawdown_hard_limit_bps,
        "m9_calibration_source_label": config.m9_calibration_source_label,''',
    "batch drawdown parameter digest",
)
batch = replace_once(
    batch,
    '''        "duration_secs": config.duration_secs,
        "validation_fold_id": config.validation_fold_id,''',
    '''        "duration_secs": config.duration_secs,
        "portfolio_drawdown_soft_limit_bps": config.portfolio_drawdown_soft_limit_bps,
        "portfolio_drawdown_hard_limit_bps": config.portfolio_drawdown_hard_limit_bps,
        "validation_fold_id": config.validation_fold_id,''',
    "batch drawdown manifest",
)
# Existing test fixtures must preserve the disabled 0/0 default when the config gains fields.
batch = replace_once(
    batch,
    '''            position_allocations: Some(BTreeMap::new()),
            output_root: PathBuf::from("target/test-stress"),''',
    '''            position_allocations: Some(BTreeMap::new()),
            portfolio_drawdown_soft_limit_bps: 0,
            portfolio_drawdown_hard_limit_bps: 0,
            output_root: PathBuf::from("target/test-stress"),''',
    "stress fixture drawdown defaults",
)

# Backward-compatible profile surface: schema remains v2; missing fields default to disabled.
profile = replace_once(
    profile,
    '''    pub entry_threshold_bps: i64,
    pub threshold_scale_ppm: i64,
    pub max_position: i64,''',
    '''    pub entry_threshold_bps: i64,
    pub threshold_scale_ppm: i64,
    #[serde(default)]
    pub portfolio_drawdown_soft_limit_bps: i64,
    #[serde(default)]
    pub portfolio_drawdown_hard_limit_bps: i64,
    pub max_position: i64,''',
    "profile drawdown fields",
)
profile = replace_once(
    profile,
    '''        if self.index_anchor_refresh_ms == 0 {
            return Err("strategy profile index-anchor refresh must be enabled".to_owned());
        }
        if self.experiments.is_empty() {''',
    '''        if self.index_anchor_refresh_ms == 0 {
            return Err("strategy profile index-anchor refresh must be enabled".to_owned());
        }
        let drawdown_disabled = self.portfolio_drawdown_soft_limit_bps == 0
            && self.portfolio_drawdown_hard_limit_bps == 0;
        let drawdown_valid = self.portfolio_drawdown_soft_limit_bps > 0
            && self.portfolio_drawdown_hard_limit_bps > self.portfolio_drawdown_soft_limit_bps
            && self.portfolio_drawdown_hard_limit_bps <= 10_000;
        if !drawdown_disabled && !drawdown_valid {
            return Err(
                "portfolio drawdown limits require 0/0 or 0 < soft < hard <= 10000 bps"
                    .to_owned(),
            );
        }
        if self.experiments.is_empty() {''',
    "profile drawdown validation",
)
if "portfolio_drawdown_limits_are_backward_compatible_and_validated" not in profile:
    marker = '''    #[test]
    fn funding_ablation_cannot_be_mislabelled_as_m7() {'''
    test = '''    #[test]
    fn portfolio_drawdown_limits_are_backward_compatible_and_validated() {
        let mut profile = shipped_profile();
        assert_eq!(profile.portfolio_drawdown_soft_limit_bps, 0);
        assert_eq!(profile.portfolio_drawdown_hard_limit_bps, 0);
        profile.portfolio_drawdown_soft_limit_bps = 500;
        assert!(profile.validate().is_err());
        profile.portfolio_drawdown_hard_limit_bps = 1_000;
        assert!(profile.validate().is_ok());
    }

    #[test]
    fn funding_ablation_cannot_be_mislabelled_as_m7() {'''
    if marker not in profile:
        raise SystemExit("missing anchor: profile drawdown regression")
    profile = profile.replace(marker, test, 1)

# Profile-authoritative batch binary wiring.
batch_bin = replace_once(
    batch_bin,
    '''            position_allocations: Some(allocations),
            output_root,''',
    '''            position_allocations: Some(allocations),
            portfolio_drawdown_soft_limit_bps: profile.portfolio_drawdown_soft_limit_bps,
            portfolio_drawdown_hard_limit_bps: profile.portfolio_drawdown_hard_limit_bps,
            output_root,''',
    "batch binary drawdown profile wiring",
)

RUNTIME.write_text(runtime, encoding="utf-8")
BACKTEST.write_text(backtest, encoding="utf-8")
BATCH.write_text(batch, encoding="utf-8")
PROFILE.write_text(profile, encoding="utf-8")
BATCH_BIN.write_text(batch_bin, encoding="utf-8")
print(
    "portfolio drawdown config surface applied: "
    f"runtime={len(runtime.encode('utf-8'))}, "
    f"backtest={len(backtest.encode('utf-8'))}, "
    f"batch={len(batch.encode('utf-8'))} bytes"
)
