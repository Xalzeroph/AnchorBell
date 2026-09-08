from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
BATCH = ROOT / "engine" / "src" / "simulation_batch.rs"

text = BATCH.read_text(encoding="utf-8")

old_metrics = '''                        let point = ledger.engine.performance_point(observed_at);
                        ledger.history.push_back(point.clone());
                        append_risk_history_sample(&mut ledger.risk_history, point, false);
                        while ledger.history.len() > DISPLAY_HISTORY_CAPACITY {
'''
new_metrics = '''                        let point = ledger.engine.performance_point(observed_at);
                        ledger.history.push_back(point);
                        while ledger.history.len() > DISPLAY_HISTORY_CAPACITY {
'''
if old_metrics in text:
    text = text.replace(old_metrics, new_metrics, 1)
elif new_metrics not in text:
    raise SystemExit("missing anchor: metrics-only display history")

old_dispatch = '''                    for ledger in &mut ledgers {
                        for record in ledger.engine.on_enveloped_event(&envelope)? {
                            let line = serde_json::to_string(&record)?;
                            ledger
                                .record_tx
                                .send(line)
                                .await
                                .map_err(|_| SimulationError::Io("ledger writer stopped".to_owned()))?;
                        }
                    }
'''
new_dispatch = '''                    for ledger in &mut ledgers {
                        // Risk statistics are sampled on exchange event time, not
                        // scheduler/metrics-tick time. Seed the fold before the
                        // first mutation so early PnL is never absorbed as baseline.
                        if ledger.risk_history.is_empty() {
                            let baseline = ledger.engine.performance_point(envelope.observed_at_ms);
                            append_risk_history_sample(&mut ledger.risk_history, baseline, false);
                        }
                        for record in ledger.engine.on_enveloped_event(&envelope)? {
                            let line = serde_json::to_string(&record)?;
                            ledger
                                .record_tx
                                .send(line)
                                .await
                                .map_err(|_| SimulationError::Io("ledger writer stopped".to_owned()))?;
                        }
                        let point = ledger.engine.performance_point(envelope.observed_at_ms);
                        append_risk_history_sample(&mut ledger.risk_history, point, false);
                    }
'''
if new_dispatch not in text:
    if old_dispatch not in text:
        raise SystemExit("missing anchor: event-driven risk sampling")
    text = text.replace(old_dispatch, new_dispatch, 1)

old_final = '''        let observed_at = now_ms();
        let point = ledger.engine.performance_point(observed_at);
        ledger.history.push_back(point.clone());
        append_risk_history_sample(&mut ledger.risk_history, point, true);
        while ledger.history.len() > DISPLAY_HISTORY_CAPACITY {
'''
new_final = '''        let observed_at = now_ms();
        ledger.history.push_back(ledger.engine.performance_point(observed_at));
        let event_at = ledger.engine.checkpoint_view(&ledger.spec.label).0;
        let risk_at = ledger
            .risk_history
            .back()
            .map(|point| point.observed_at_ms)
            .unwrap_or(0)
            .max(event_at);
        let risk_point = ledger.engine.performance_point(risk_at);
        append_risk_history_sample(&mut ledger.risk_history, risk_point, true);
        while ledger.history.len() > DISPLAY_HISTORY_CAPACITY {
'''
if new_final not in text:
    if old_final not in text:
        raise SystemExit("missing anchor: deterministic final risk sample")
    text = text.replace(old_final, new_final, 1)

BATCH.write_text(text, encoding="utf-8")
print("deterministic exchange-time risk sampling repair applied")
