import pytest
from fastapi.testclient import TestClient

from bot.config import load_settings
from bot.replay import run_replay
from bot.storage import Storage, atomic_write_json
from dashboard.app import create_app
from dashboard.reader import pick_stride
from tools.synthetic import write_csv

from .conftest import T0


@pytest.fixture(scope="module")
def replay_dir(tmp_path_factory):
    tmp = tmp_path_factory.mktemp("dash")
    src = tmp / "fixture.csv"
    write_csv(src, T0, 3 * 1440, seed=5)
    s = load_settings(env_file=None, environ={"DATA_DIR": str(tmp / "x"), "OVERLAY_EMA_SPAN_MIN": "120",
                                               "OVERLAY_WARMUP_BARS": "360", "LADDER_WARMUP_DAYS": "1"})
    out = tmp / "data_replay"
    run_replay(s, [src], ["s1k", "s4k"], out_dir=out, progress=False)
    return out


def test_pick_stride():
    assert pick_stride(1440) == 1
    assert pick_stride(7 * 1440) == 10
    assert pick_stride(30 * 1440) == 30
    assert pick_stride(3 * 365 * 1440) == 1440
    assert pick_stride(1440, base=5) == 5


def test_index_and_apis(replay_dir):
    c = TestClient(create_app(replay_dir))
    r = c.get("/")
    assert r.status_code == 200 and "Chart" in r.text and "cdn.jsdelivr.net" in r.text
    ov = c.get("/api/overview").json()
    assert ov["replay"] is True and set(ov["instances"]) == {"s1k", "s4k"}
    assert ov["instances"]["s1k"]["reference"]["year"] == 2026
    for rng in ("24h", "7d", "30d", "all"):
        ratio = c.get(f"/api/ratio?range={rng}").json()
        assert ratio["points"], rng
        p = ratio["points"][-1]
        assert p["up"] > p["ema"] > p["lo"]
        assert set(ratio["markers"]) == {"s1k", "s4k"}
    ratio = c.get("/api/ratio?range=all").json()
    assert ratio["ladder"] and len(ratio["ladder"][0]["bands"]) == 2
    ex = c.get("/api/excess?range=all").json()
    assert set(ex["series"]["s1k"]) == {"mid", "touch", "worst"} and ex["primary"]["s1k"] == "touch"
    w = c.get("/api/weights?range=7d").json()
    assert w["series"]["s4k"]["w"] and w["series"]["s4k"]["w_target"]
    t = c.get("/api/tables?n=50").json()
    assert len(t["instances"]["s1k"]["decisions"]) <= 50 and t["events"]
    assert c.get("/api/compare").json() == {"instances": {}}
    assert c.get("/api/ratio?range=bogus").status_code == 422


def test_overview_kill_switch_and_live_compare(tmp_path):
    st = Storage(tmp_path)
    (tmp_path / "KILL").write_text("")
    st.write_bar(T0 + 300_000, ratio=20.0, ema3d_ratio=20.0, stale=False)
    st.write_equity("s1k", T0 + 300_000, variant="live", excess=0.01, w=0.5, w_target=0.5)
    st.write_equity("s1k", T0 + 300_000, variant="touch", excess=0.012, w=0.5, w_target=0.5)
    st.write_order("s1k", T0 + 60_000, mode="live", event="final", variant="live", slippage_bps_vs_mid=7.5)
    st.flush()
    atomic_write_json(st.summary_path("s1k"), {
        "ts": "2026-01-01T00:05:00Z", "mode": "live", "primary_variant": "live",
        "benchmark_start": "2026-01-01T00:00:00Z", "excess_by_variant": {"live": 0.01, "touch": 0.012},
        "excess_ytd": {"live": 0.01}, "live": {"active": True, "activated": "2026-01-01T00:00:00Z"},
    })
    c = TestClient(create_app(tmp_path))
    ov = c.get("/api/overview").json()
    assert ov["kill_switch"] is True and ov["instances"]["s1k"]["mode"] == "live"
    cmp = c.get("/api/compare").json()["instances"]["s1k"]
    assert cmp["live"] and cmp["touch"] and cmp["slippage"]["n"] == 1 and cmp["slippage"]["hist"] == [[5, 1]]


def test_empty_data_dir(tmp_path):
    c = TestClient(create_app(tmp_path))
    assert c.get("/api/overview").json()["instances"] == {}
    assert c.get("/api/ratio?range=7d").json()["points"] == []
    assert c.get("/api/excess?range=7d").json()["series"] == {}
