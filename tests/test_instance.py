
import pytest

from bot.instance import Instance, make_decision_id
from bot.portfolio import prices
from bot.storage import Storage, iter_jsonl, read_json
from bot.util import MINUTE_MS, SimClock

from .conftest import T0, bar_for_lr


def small_instance(tmp_path, settings_factory, **env):
    s = settings_factory(OVERLAY_EMA_SPAN_MIN=10, OVERLAY_WARMUP_BARS=20, LADDER_WARMUP_DAYS=1, **env)
    clock = SimClock(T0)
    st = Storage(tmp_path / "data", clock)
    inst = Instance(s.instance("s1k"), st, clock)
    return inst, st, clock


def feed(inst, clock, lrs, start_index=0, stale_at=(), phase="live"):
    sigs = []
    for i, lr in enumerate(lrs, start=start_index):
        ts = T0 + i * MINUTE_MS
        bar = bar_for_lr(ts, lr, spread=0.001, v=0.0 if i in stale_at else 1000.0)
        clock.set(bar.close_ms)
        sigs.append(inst.on_bar(bar, phase, prices(bar.mid("THETAUSDT"), bar.mid("TFUELUSDT"))))
    return sigs


def test_decision_id_format():
    assert make_decision_id(T0 + 12 * 60 * MINUTE_MS + 34 * MINUTE_MS) == "2601011234"
    assert make_decision_id(T0, "ib") == "ib2601010000"


def test_trading_starts_after_warmup_without_trading(tmp_path, settings_factory):
    inst, st, clock = small_instance(tmp_path, settings_factory)
    feed(inst, clock, [0.0] * 25)
    assert inst.started
    assert inst.last_target == pytest.approx(0.5)
    assert all(p.w(prices(1.0, 1.0)) == pytest.approx(0.5) for p in inst.paper.values())
    ev = [e for e in iter_jsonl(st.events_path()) if e["kind"] == "trading_started"]
    assert len(ev) == 1
    assert not (st.instance_dir("s1k") / "decisions.jsonl").exists()


def test_overlay_entry_creates_decision_and_paper_fills(tmp_path, settings_factory):
    inst, st, clock = small_instance(tmp_path, settings_factory)
    feed(inst, clock, [0.0] * 25)
    sigs = feed(inst, clock, [0.2, 0.2], start_index=25)  # THETA rich
    assert sigs[0].pos == -1 and sigs[0].action == "enter" and sigs[0].w_target == pytest.approx(0.25)
    decs = list(iter_jsonl(st.instance_dir("s1k") / "decisions.jsonl"))
    assert len(decs) == 1
    d = decs[0]
    for k in ("decision_id", "ts", "reason", "w_from", "w_target", "dv_usd", "mode"):
        assert k in d
    assert d["reason"] == "overlay_enter" and d["mode"] == "paper" and d["dv_usd"] < 0
    orders = list(iter_jsonl(st.instance_dir("s1k") / "orders.jsonl"))
    assert {(o["variant"], o["leg"]) for o in orders} == {(v, l) for v in ("mid", "touch", "worst") for l in (1, 2)}
    px = inst.last_px
    for v, p in inst.paper.items():
        assert p.w(px) == pytest.approx(0.25, abs=0.01), v
    sig_lines = list(iter_jsonl(st.instance_dir("s1k") / "signals_2026-01-01.jsonl"))
    for k in ("ts", "dev", "entry", "exit", "pos", "pos_prev", "ladder_w", "w_target", "w_current", "action"):
        assert k in sig_lines[-1]
    assert sig_lines[-2]["action"] == "enter"


def test_no_rebalance_on_drift_or_unchanged_target(tmp_path, settings_factory):
    inst, st, clock = small_instance(tmp_path, settings_factory)
    feed(inst, clock, [0.0] * 25)
    feed(inst, clock, [0.04, 0.05, 0.03, 0.02], start_index=25)  # inside the entry band
    assert not (st.instance_dir("s1k") / "decisions.jsonl").exists()


def test_stale_bar_does_not_trade(tmp_path, settings_factory):
    inst, st, clock = small_instance(tmp_path, settings_factory)
    feed(inst, clock, [0.0] * 25)
    sigs = feed(inst, clock, [0.2], start_index=25, stale_at={25})
    assert sigs[0].pos == 0  # stale: no open
    assert not (st.instance_dir("s1k") / "decisions.jsonl").exists()
    sigs = feed(inst, clock, [0.2], start_index=26)
    assert sigs[0].pos == -1
    assert len(list(iter_jsonl(st.instance_dir("s1k") / "decisions.jsonl"))) == 1


def test_equity_summary_and_state_round_trip(tmp_path, settings_factory):
    inst, st, clock = small_instance(tmp_path, settings_factory)
    feed(inst, clock, [0.0] * 25 + [0.2] * 10)
    eq = list(iter_jsonl(st.instance_dir("s1k") / "equity_2026-01-01.jsonl"))
    assert {e["variant"] for e in eq} == {"mid", "touch", "worst"}
    for k in ("ts", "variant", "bal", "mid", "value_usd", "hodl_value_usd", "excess", "theta_equiv_tokens",
              "tfuel_equiv_tokens", "w"):
        assert k in eq[-1]
    assert all(e["ts"].endswith(("0:00Z", "5:00Z")) for e in eq)  # every 5 minutes
    summ = read_json(st.summary_path("s1k"))
    for k in ("mode", "live_enabled", "benchmark_start", "excess_by_variant", "excess_ytd", "trades_total",
              "trades_30d", "fees_usd", "avg_slippage_bps", "current", "last_bar_ts", "health"):
        assert k in summ, k
    for k in ("ratio", "dev", "pos", "ladder_w", "w_target", "w_current"):
        assert k in summ["current"]
    assert summ["mode"] == "paper" and summ["trades_total"] == 1

    # restart: a fresh instance from state.json continues identically
    inst.save()
    clone = Instance(inst.cfg, st, clock)
    clone.load_state(read_json(st.state_path("s1k")))
    a = feed(inst, clock, [0.001], start_index=35)[0]
    b_bar = bar_for_lr(T0 + 35 * MINUTE_MS, 0.001, spread=0.001)
    b = clone.on_bar(b_bar, "live", prices(b_bar.mid("THETAUSDT"), b_bar.mid("TFUELUSDT")))
    assert (a.pos, a.w_target) == (b.pos, b.w_target) and a.dev == pytest.approx(b.dev)
    assert clone.paper["touch"].bal == pytest.approx(inst.paper["touch"].bal)


def test_target_change_during_backfill_executes_on_next_live_bar(tmp_path, settings_factory):
    inst, st, clock = small_instance(tmp_path, settings_factory)
    feed(inst, clock, [0.0] * 25)
    feed(inst, clock, [0.2, 0.2], start_index=25, phase="backfill")
    assert not (st.instance_dir("s1k") / "decisions.jsonl").exists()
    feed(inst, clock, [0.2], start_index=27)
    decs = list(iter_jsonl(st.instance_dir("s1k") / "decisions.jsonl"))
    assert len(decs) == 1 and decs[0]["reason"] == "resync"


def test_ladder_change_logged_and_moves_target(tmp_path, settings_factory):
    inst, st, clock = small_instance(tmp_path, settings_factory, LADDER_EMA_SPAN_DAYS=2)
    inst.warm_daily([("2025-12-30", 0.0), ("2025-12-31", 0.0)])
    feed(inst, clock, [0.0] * 25)
    # jump the clock to the 23:59 bar of the day with a very high ratio (THETA rich)
    i = 1439
    bar = bar_for_lr(T0 + i * MINUTE_MS, 0.5)
    clock.set(bar.close_ms)
    sig = inst.on_bar(bar, "live", prices(bar.theta.c, bar.tfuel.c))
    assert sig.ladder_updates and sig.ladder_updates[0].w_new < 0.5
    lad = list(iter_jsonl(st.instance_dir("s1k") / "ladder.jsonl"))
    for k in ("date", "lr_day", "ema60", "D", "ladder_w_prev", "ladder_w"):
        assert k in lad[-1]
    assert lad[-1]["date"] == "2026-01-01"
    decs = list(iter_jsonl(st.instance_dir("s1k") / "decisions.jsonl"))
    assert decs and "ladder" in decs[-1]["reason"]
