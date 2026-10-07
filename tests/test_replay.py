import csv
import json

import pytest

from bot.config import load_settings
from bot.replay import iter_bars, parse_time, run_replay
from bot.storage import iter_jsonl, read_json
from tools.synthetic import write_csv, write_xlsx

from .conftest import T0

FAST = {
    "OVERLAY_EMA_SPAN_MIN": "120",
    "OVERLAY_WARMUP_BARS": "360",
    "LADDER_EMA_SPAN_DAYS": "2",
    "LADDER_WARMUP_DAYS": "1",
    "OVERLAY_MAX_HOLD_MIN": "720",
}


@pytest.fixture
def fixture_files(tmp_path):
    csv_path = tmp_path / "theta_tfuel_1m_fixture.csv"
    xlsx_path = tmp_path / "theta_tfuel_1m_fixture.xlsx"
    minutes = 3 * 1440
    write_csv(csv_path, T0, minutes, seed=3)
    write_xlsx(xlsx_path, T0, minutes, seed=3, rows_per_sheet=1500)  # several sheets
    return csv_path, xlsx_path


def settings(tmp_path):
    return load_settings(env_file=None, environ={"DATA_DIR": str(tmp_path / "unused"), **FAST})


def test_parse_time_variants():
    assert parse_time("2026-01-01 00:00:00") == T0
    assert parse_time("2026-01-01T00:00:00Z") == T0
    assert parse_time(T0) == T0
    assert parse_time(T0 // 1000) == T0
    assert parse_time(46023.0) == T0  # Excel serial date


def test_xlsx_multi_sheet_and_csv_read_identically(fixture_files):
    csv_path, xlsx_path = fixture_files
    a = list(iter_bars([csv_path]))
    b = list(iter_bars([xlsx_path]))
    assert len(a) == len(b) == 3 * 1440
    assert [x.ts_ms for x in a[:3]] == [T0, T0 + 60_000, T0 + 120_000]
    assert a[100].theta.c == pytest.approx(b[100].theta.c) and a[100].tfuel.v == pytest.approx(b[100].tfuel.v)


def test_ratio_is_recomputed_not_taken_from_column(tmp_path):
    p = tmp_path / "x.csv"
    with open(p, "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["time", "THETA_open", "THETA_high", "THETA_low", "THETA_close", "THETA_volume", "TFUEL_open",
                    "TFUEL_high", "TFUEL_low", "TFUEL_close", "TFUEL_volume", "THETA_per_TFUEL"])
        w.writerow(["2026-01-01 00:00:00", 1, 1, 1, 2.0, 10, 0.1, 0.1, 0.1, 0.1, 10, 0.05])  # wrong column value
    (bar,) = list(iter_bars([p]))
    assert bar.ratio == pytest.approx(20.0)


def test_replay_smoke(tmp_path, fixture_files):
    csv_path, _ = fixture_files
    out = tmp_path / "data_replay"
    report = run_replay(settings(tmp_path), [csv_path], ["s1k", "s4k"], out_dir=out, progress=False)
    # same JSON layout as the live bot
    assert sorted(p.name for p in (out / "market").glob("bars_*.jsonl")) == [
        "bars_2026-01-01.jsonl", "bars_2026-01-02.jsonl", "bars_2026-01-03.jsonl"]
    for inst in ("s1k", "s4k"):
        d = out / "instances" / inst
        for name in ("decisions.jsonl", "orders.jsonl", "ladder.jsonl", "state.json", "summary.json",
                     "signals_2026-01-02.jsonl", "equity_2026-01-02.jsonl"):
            assert (d / name).exists(), (inst, name)
        decs = list(iter_jsonl(d / "decisions.jsonl"))
        assert decs, "fixture should produce rebalances"
        orders = list(iter_jsonl(d / "orders.jsonl"))
        assert {o["variant"] for o in orders} == {"mid", "touch", "worst"}
        summ = read_json(d / "summary.json")
        assert summ["mode"] == "paper" and set(summ["excess_by_variant"]) == {"mid", "touch", "worst"}
    bar = json.loads((out / "market" / "bars_2026-01-02.jsonl").read_text().splitlines()[0])
    for k in ("ts", "theta", "tfuel", "ratio", "lr", "ema3d", "dev", "stale"):
        assert k in bar
    assert report["bars"] == 3 * 1440
    years = [r for r in report["years"] if r["instance"] == "s1k"]
    assert {r["variant"] for r in years} == {"mid", "touch", "worst"} and all(r["year"] == 2026 for r in years)
    ex = report["instances"]["s1k"]["excess_full_period"]
    assert ex["mid"] >= ex["worst"]  # close fills are never worse than worst-of-bar fills
    assert (out / "replay_report.json").exists()


def test_replay_start_window_and_nonempty_output_refused(tmp_path, fixture_files):
    csv_path, _ = fixture_files
    out = tmp_path / "r2"
    run_replay(settings(tmp_path), [csv_path], ["s1k"], out_dir=out, start="2026-01-02T00:00:00Z", progress=False)
    summ = read_json(out / "instances" / "s1k" / "summary.json")
    assert summ["trading_started"] >= "2026-01-02"
    assert not (out / "instances" / "s1k" / "signals_2026-01-01.jsonl").exists()  # warm-up only
    with pytest.raises(SystemExit):
        run_replay(settings(tmp_path), [csv_path], ["s1k"], out_dir=out, progress=False)


def test_per_year_report_splits_at_new_year(tmp_path):
    src = tmp_path / "ny.csv"
    write_csv(src, T0 - 2 * 86_400_000, 4 * 1440, seed=9)  # 2025-12-30 .. 2026-01-02
    report = run_replay(settings(tmp_path), [src], ["s1k"], out_dir=tmp_path / "out", progress=False)
    rows = sorted((r["year"], r["variant"], r["days"]) for r in report["years"])
    assert [y for y, v, _ in rows if v == "touch"] == [2025, 2026]
    days = {y: d for y, v, d in rows if v == "touch"}
    assert 1.5 < days[2025] < 2.0 and 1.9 < days[2026] <= 2.0
    first_2026 = next(r for r in report["years"] if r["year"] == 2026 and r["variant"] == "touch")
    assert first_2026["start"] == "2026-01-01T00:00:00Z"
