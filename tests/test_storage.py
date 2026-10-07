import json
import os

import pytest

from bot.storage import Storage, atomic_write_json, dumps, read_json, tail_jsonl
from bot.util import SimClock

from .conftest import T0


def test_atomic_write_replaces_whole_file(tmp_path):
    p = tmp_path / "state.json"
    atomic_write_json(p, {"a": 1})
    atomic_write_json(p, {"a": 2, "b": [1, 2]})
    assert read_json(p) == {"a": 2, "b": [1, 2]}
    assert [x.name for x in tmp_path.iterdir()] == ["state.json"]


def test_atomic_write_failure_keeps_old_file(tmp_path, monkeypatch):
    p = tmp_path / "state.json"
    atomic_write_json(p, {"version": 1})

    def boom(src, dst):
        raise OSError("disk full")

    monkeypatch.setattr(os, "replace", boom)
    with pytest.raises(OSError):
        atomic_write_json(p, {"version": 2})
    monkeypatch.undo()
    assert read_json(p) == {"version": 1}
    assert [x.name for x in tmp_path.iterdir()] == ["state.json"]  # temp file cleaned up


def test_dumps_handles_nan():
    assert json.loads(dumps({"x": float("nan"), "y": [float("inf"), 1.0]})) == {"x": None, "y": [None, 1.0]}


def test_records_daily_rotation_and_schema(tmp_path):
    clock = SimClock(T0)
    st = Storage(tmp_path, clock)
    st.write_bar(T0 + 60_000, ratio=20.0)
    st.write_bar(T0 + 86_400_000, ratio=21.0)
    st.write_signal("s1k", T0, dev=0.01)
    st.event("ERROR", "boom", "something failed", instance="s1k")
    st.flush()
    files = sorted(p.name for p in (tmp_path / "market").iterdir())
    assert files == ["bars_2026-01-01.jsonl", "bars_2026-01-02.jsonl"]
    line = (tmp_path / "market" / "bars_2026-01-01.jsonl").read_text().splitlines()[0]
    assert line.startswith('{"ts":"2026-01-01T00:01:00Z"')  # ts is always the first key
    rec = json.loads(line)
    assert rec["schema_version"] == 1
    sig = json.loads((tmp_path / "instances" / "s1k" / "signals_2026-01-01.jsonl").read_text())
    assert sig["instance"] == "s1k"
    ev = tail_jsonl(tmp_path / "events.jsonl", 5)
    assert ev[-1]["level"] == "ERROR" and ev[-1]["kind"] == "boom"
    assert st.errors_24h() == 1


def test_tail_jsonl_skips_torn_last_line(tmp_path):
    p = tmp_path / "x.jsonl"
    p.write_text("\n".join(json.dumps({"i": i}) for i in range(100)) + '\n{"i": 10')
    out = tail_jsonl(p, 3)
    assert [r["i"] for r in out] == [97, 98, 99]
