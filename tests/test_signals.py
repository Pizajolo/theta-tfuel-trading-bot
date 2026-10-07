import math

import pytest

from bot.config import StrategyParams
from bot.signals import (
    LadderParams,
    LadderState,
    OverlayParams,
    OverlayState,
    SignalEngine,
    combine_target,
    ema_alpha,
    ladder_decide,
    ladder_step,
    overlay_step,
    overlay_transition,
)
from bot.util import MINUTE_MS

from .conftest import T0

P = OverlayParams(span_min=10, entry=0.06, exit=0.002, ticket=0.25, max_hold_min=60, warmup_bars=0)
TIERS = ((0.15, 0.15), (0.30, 0.30))


# ---- overlay transition table -------------------------------------------------------------------
@pytest.mark.parametrize(
    "pos,d,expected",
    [
        (0, 0.07, (-1, "enter")),
        (0, -0.07, (1, "enter")),
        (0, 0.05, (0, "none")),
        (0, -0.0599, (0, "none")),
        (-1, 0.001, (0, "exit")),
        (1, -0.0019, (0, "exit")),
        (1, 0.003, (1, "none")),  # |d| not below exit -> hold
        (-1, -0.07, (1, "flip")),
        (1, 0.07, (-1, "flip")),
        (-1, 0.08, (-1, "none")),  # already short THETA, stays
        (1, -0.08, (1, "none")),
    ],
)
def test_overlay_transition_table(pos, d, expected):
    assert overlay_transition(pos, d, 0.06, 0.002) == expected


def _state_with_ema(ema: float, pos: int = 0, since: int | None = None) -> OverlayState:
    return OverlayState(ema=ema, n=10_000, pos=pos, pos_since_ms=since)


def test_ema_updated_before_deviation():
    st = _state_with_ema(0.0)
    new, d, _ = overlay_step(st, 0.11, T0, False, P)
    alpha = ema_alpha(10)
    assert new.ema == pytest.approx(alpha * 0.11)
    assert d == pytest.approx(0.11 - alpha * 0.11)


def test_overlay_entry_short_and_long():
    st, d, a = overlay_step(_state_with_ema(0.0), 0.2, T0, False, P)
    assert (st.pos, a) == (-1, "enter") and d > 0.06 and st.pos_since_ms == T0
    st, d, a = overlay_step(_state_with_ema(0.0), -0.2, T0, False, P)
    assert (st.pos, a) == (1, "enter")


def test_overlay_exit_and_flip():
    st = _state_with_ema(0.0, pos=-1, since=T0)
    st2, d, a = overlay_step(st, 0.0005, T0 + MINUTE_MS, False, P)
    assert (st2.pos, a) == (0, "exit") and st2.pos_since_ms is None
    st3, d, a = overlay_step(st, -0.2, T0 + MINUTE_MS, False, P)
    assert (st3.pos, a) == (1, "flip") and st3.pos_since_ms == T0 + MINUTE_MS


def test_stale_bar_blocks_open_flip_close_but_updates_ema():
    st = _state_with_ema(0.0)
    new, d, a = overlay_step(st, 0.2, T0, True, P)
    assert new.pos == 0 and a == "none"
    assert new.ema != st.ema and new.n == st.n + 1
    held = _state_with_ema(0.0, pos=-1, since=T0)
    new, _, a = overlay_step(held, 0.0, T0 + MINUTE_MS, True, P)  # would exit
    assert new.pos == -1 and a == "none"
    new, _, a = overlay_step(held, -0.2, T0 + MINUTE_MS, True, P)  # would flip
    assert new.pos == -1 and a == "none"


def test_failsafe_forces_flat_after_max_hold():
    held = _state_with_ema(0.0, pos=1, since=T0)
    # still within max hold (60 min)
    new, _, a = overlay_step(held, -0.03, T0 + 60 * MINUTE_MS, False, P)
    assert new.pos == 1 and a == "none"
    new, _, a = overlay_step(held, -0.03, T0 + 61 * MINUTE_MS, False, P)
    assert new.pos == 0 and a == "failsafe"
    # stale bar defers the failsafe
    new, _, a = overlay_step(held, -0.03, T0 + 61 * MINUTE_MS, True, P)
    assert new.pos == 1


def test_no_transitions_during_warmup():
    p = OverlayParams(span_min=10, entry=0.06, exit=0.002, ticket=0.25, max_hold_min=60, warmup_bars=30)
    st = OverlayState(ema=0.0, n=5)
    new, d, a = overlay_step(st, 0.5, T0, False, p)
    assert new.pos == 0 and a == "none" and d > 0.06


# ---- ladder -------------------------------------------------------------------------------------
def test_ladder_escalate_hold_and_neutral():
    cur = 0.5
    cur = ladder_decide(cur, 0.16, TIERS, 0.05)
    assert cur == pytest.approx(0.35)  # tier 1: THETA rich -> less THETA
    cur = ladder_decide(cur, 0.31, TIERS, 0.05)
    assert cur == pytest.approx(0.20)  # escalate to tier 2
    cur = ladder_decide(cur, 0.20, TIERS, 0.05)
    assert cur == pytest.approx(0.20)  # never step back halfway
    cur = ladder_decide(cur, 0.10, TIERS, 0.05)
    assert cur == pytest.approx(0.20)  # between exit and tier: hold
    cur = ladder_decide(cur, 0.04, TIERS, 0.05)
    assert cur == 0.5  # back to neutral


def test_ladder_flip_side():
    cur = ladder_decide(0.20, -0.16, TIERS, 0.05)
    assert cur == pytest.approx(0.65)  # flip from rich to cheap side
    cur = ladder_decide(cur, -0.35, TIERS, 0.05)
    assert cur == pytest.approx(0.80)
    cur = ladder_decide(cur, -0.18, TIERS, 0.05)
    assert cur == pytest.approx(0.80)
    cur = ladder_decide(cur, 0.17, TIERS, 0.05)
    assert cur == pytest.approx(0.35)


def test_ladder_weights_set():
    seen = set()
    cur = 0.5
    for D in (0.2, 0.4, 0.0, -0.2, -0.4, 0.0):
        cur = ladder_decide(cur, D, TIERS, 0.05)
        seen.add(round(cur, 2))
    assert seen == {0.5, 0.35, 0.2, 0.65, 0.8}


def test_ladder_step_ema_and_warmup():
    p = LadderParams(enabled=True, span_days=60, tiers=TIERS, exit=0.05, warmup_days=3)
    st = LadderState()
    st, D, wp, wn = ladder_step(st, "2026-01-01", 0.0, p)
    assert D == 0 and wn == 0.5
    st, D, wp, wn = ladder_step(st, "2026-01-02", 1.0, p)  # huge D but still warming up
    assert wn == 0.5 and st.n_days == 2
    st, D, wp, wn = ladder_step(st, "2026-01-03", 1.0, p)
    assert D > 0.3 and wn == pytest.approx(0.20)
    assert st.ema == pytest.approx(ema_alpha(60) * 1.0 + (1 - ema_alpha(60)) * ema_alpha(60) * 1.0)


def test_ladder_disabled_stays_neutral():
    p = LadderParams(enabled=False, span_days=60, tiers=TIERS, exit=0.05, warmup_days=0)
    st, D, wp, wn = ladder_step(LadderState(ema=0.0, n_days=100), "2026-01-01", 1.0, p)
    assert wn == 0.5


# ---- combination ---------------------------------------------------------------------------------
@pytest.mark.parametrize(
    "ladder,pos,lo,hi,expected",
    [
        (0.5, 0, 0.05, 0.95, 0.5),
        (0.5, 1, 0.05, 0.95, 0.75),
        (0.2, -1, 0.05, 0.95, 0.05),  # -0.05 clipped up
        (0.8, 1, 0.05, 0.95, 0.95),  # 1.05 clipped down
        (0.8, 1, 0.20, 0.80, 0.80),
        (0.2, -1, 0.20, 0.80, 0.20),
        (0.35, -1, 0.05, 0.95, 0.10),
    ],
)
def test_weight_clipping(ladder, pos, lo, hi, expected):
    assert combine_target(ladder, pos, 0.25, lo, hi) == pytest.approx(expected)


# ---- engine --------------------------------------------------------------------------------------
def test_engine_ladder_runs_on_2359_bar_and_date_change_fallback():
    sp = StrategyParams(overlay_ema_span_min=10, overlay_warmup_bars=1, ladder_warmup_days=1)
    eng = SignalEngine(sp)
    day = 86_400_000
    sig = eng.feed_bar(T0 + 1439 * MINUTE_MS, 0.0, False)  # 23:59 on 2026-01-01
    assert [u.date for u in sig.ladder_updates] == ["2026-01-01"]
    # 2026-01-02's 23:59 bar is missing: the first bar of 2026-01-03 closes 01-02
    eng.feed_bar(T0 + day + 600 * MINUTE_MS, 0.5, False)
    sig = eng.feed_bar(T0 + 2 * day, 0.6, False)
    assert [u.date for u in sig.ladder_updates] == ["2026-01-02"]
    assert sig.ladder_updates[0].lr_day == 0.5
    # days already seen are ignored
    assert eng.feed_daily_close("2026-01-02", 9.0) is None


def test_engine_round_trip_and_order():
    sp = StrategyParams(overlay_ema_span_min=10, overlay_warmup_bars=5)
    eng = SignalEngine(sp)
    for i in range(20):
        eng.feed_bar(T0 + i * MINUTE_MS, 0.01 * math.sin(i), False)
    clone = SignalEngine(sp, eng.to_dict())
    a = eng.feed_bar(T0 + 20 * MINUTE_MS, 0.3, False)
    b = clone.feed_bar(T0 + 20 * MINUTE_MS, 0.3, False)
    assert (a.pos, a.dev, a.w_target) == (b.pos, b.dev, b.w_target)
    with pytest.raises(ValueError):
        eng.feed_bar(T0, 0.0, False)


def test_engine_w_target_combines_layers():
    sp = StrategyParams(overlay_ema_span_min=10, overlay_warmup_bars=1, ladder_warmup_days=1)
    eng = SignalEngine(sp)
    eng.ladder.cur = 0.35
    eng.feed_bar(T0, 0.0, False)
    sig = eng.feed_bar(T0 + MINUTE_MS, -0.5, False)  # THETA cheap -> pos +1
    assert sig.pos == 1 and sig.w_target == pytest.approx(0.60)
