from pathlib import Path

path = Path(__file__).with_name("anchorbell_walkforward_contract_fix.py")
text = path.read_text(encoding="utf-8")

start_marker = '''backtest = replace_once(
    backtest,
    ''' + "'''         --calibration-store PATH --calibration-source-label LABEL --ablate-funding"
end_marker = '''    "backtest walk-forward usage",
)'''
usage_hardened = 'usage_old = "--calibration-store PATH --calibration-source-label LABEL --ablate-funding"'

if usage_hardened not in text:
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
    print("walk-forward usage anchor hardened")
else:
    print("walk-forward usage anchor already hardened")

old_never = "        .unwrap_or_else(fail);"
new_never = "        .unwrap_or_else(|error| fail(error));"
if old_never in text:
    text = text.replace(old_never, new_never, 1)
    print("walk-forward Result<()> failure closure hardened")
elif new_never not in text:
    raise SystemExit("cannot find walk-forward Result<()> failure closure")
else:
    print("walk-forward Result<()> failure closure already hardened")

path.write_text(text, encoding="utf-8")
