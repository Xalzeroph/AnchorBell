from pathlib import Path

path = Path(__file__).with_name("anchorbell_walkforward_contract_fix.py")
text = path.read_text(encoding="utf-8")
start_marker = '''backtest = replace_once(
    backtest,
    ''' + "'''         --calibration-store PATH --calibration-source-label LABEL --ablate-funding"
end_marker = '''    "backtest walk-forward usage",
)'''

if 'usage_old = "--calibration-store PATH --calibration-source-label LABEL --ablate-funding"' in text:
    print("walk-forward usage anchor already hardened")
    raise SystemExit(0)

start = text.find(start_marker)
if start < 0:
    raise SystemExit("cannot find fragile walk-forward usage anchor")
end = text.find(end_marker, start)
if end < 0:
    raise SystemExit("cannot find end of fragile walk-forward usage anchor")
end += len(end_marker)
replacement = '''usage_old = "--calibration-store PATH --calibration-source-label LABEL --ablate-funding"
usage_new = "--calibration-store PATH --calibration-output PATH --calibration-source-label LABEL --freeze-calibration --ablate-funding"
backtest = replace_once(backtest, usage_old, usage_new, "backtest walk-forward usage")'''
text = text[:start] + replacement + text[end:]
path.write_text(text, encoding="utf-8")
print("walk-forward usage anchor hardened")
