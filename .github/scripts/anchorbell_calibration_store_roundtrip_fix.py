from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
BACKTEST = ROOT / "engine" / "src" / "bin" / "anchorbell_backtest.rs"
text = BACKTEST.read_text(encoding="utf-8")

if "calibration_store_round_trip_preserves_replayable_state" in text:
    print("calibration store round-trip regression already present")
    raise SystemExit(0)

append = r'''

#[cfg(test)]
mod calibration_store_regression_tests {
    use super::*;

    fn temporary_store(name: &str) -> PathBuf {
        let nonce = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        std::env::temp_dir().join(format!(
            "anchorbell-{name}-{}-{nonce}.json",
            std::process::id()
        ))
    }

    #[test]
    fn calibration_store_round_trip_preserves_replayable_state() {
        let path = temporary_store("calibration-round-trip");
        let mut state = CalibrationState::new("TESTUSDT");
        state.observe_market(
            100,
            Some(2_000_000_000_000),
            Some(1_000_000_000_000),
            Some(3_000_000_000_000),
        );
        state.observe_order_placed(101);
        state.observe_fill(102, 101, 5, 20);
        state.observe_order_terminal(103, 101);
        state.observe_markout(104, 750_000_000_000);
        let snapshots = BTreeMap::from([(
            "TESTUSDT".to_owned(),
            state.snapshot(2_000_000_000_000),
        )]);

        write_calibration_store(&path, "TRAIN", &snapshots).unwrap();
        let loaded = load_calibration_seeds(&path, "TRAIN").unwrap();
        let restored = loaded.get("TESTUSDT").unwrap();
        assert_eq!(restored.instrument, state.instrument);
        assert_eq!(restored.first_event_time_ms, state.first_event_time_ms);
        assert_eq!(restored.last_event_time_ms, state.last_event_time_ms);
        assert_eq!(restored.orders_placed, state.orders_placed);
        assert_eq!(restored.fill_events, state.fill_events);
        assert_eq!(restored.completed_orders, state.completed_orders);

        let root: serde_json::Value =
            serde_json::from_slice(&std::fs::read(&path).unwrap()).unwrap();
        assert_eq!(
            root.get("schema_version").and_then(serde_json::Value::as_u64),
            Some(u64::from(CALIBRATION_SCHEMA_VERSION))
        );
        assert_eq!(
            root.get("model_version").and_then(serde_json::Value::as_str),
            Some(CALIBRATION_MODEL_VERSION)
        );
        assert_eq!(
            root.get("source_label").and_then(serde_json::Value::as_str),
            Some("TRAIN")
        );
        assert!(root
            .get("snapshots")
            .and_then(serde_json::Value::as_object)
            .is_some_and(|snapshots| snapshots.contains_key("TRAIN::TESTUSDT")));

        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn calibration_store_rejects_schema_and_model_drift() {
        let path = temporary_store("calibration-version-drift");
        let state = CalibrationState::new("TESTUSDT");
        let snapshots = BTreeMap::from([(
            "TESTUSDT".to_owned(),
            state.snapshot(2_000_000_000_000),
        )]);
        write_calibration_store(&path, "TRAIN", &snapshots).unwrap();

        let mut root: serde_json::Value =
            serde_json::from_slice(&std::fs::read(&path).unwrap()).unwrap();
        root["schema_version"] = serde_json::json!(u64::from(CALIBRATION_SCHEMA_VERSION) + 1);
        std::fs::write(&path, serde_json::to_vec_pretty(&root).unwrap()).unwrap();
        assert!(load_calibration_seeds(&path, "TRAIN")
            .unwrap_err()
            .contains("unsupported calibration schema"));

        root["schema_version"] = serde_json::json!(CALIBRATION_SCHEMA_VERSION);
        root["model_version"] = serde_json::json!("future-calibration-model");
        std::fs::write(&path, serde_json::to_vec_pretty(&root).unwrap()).unwrap();
        assert!(load_calibration_seeds(&path, "TRAIN")
            .unwrap_err()
            .contains("unsupported calibration model"));

        let _ = std::fs::remove_file(path);
    }
}
'''

BACKTEST.write_text(text + append, encoding="utf-8")
print("calibration store round-trip regression appended")
