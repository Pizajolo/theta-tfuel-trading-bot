"""Pure signal logic: EMAs and the overlay / ladder state machines. No I/O in this module.

Conventions
-----------
* ``lr = ln(close_THETA / close_TFUEL)`` on closed 1m bars sharing the same open time.
* EMAs are updated *before* the deviation is computed: ``ema += alpha*(lr-ema); d = lr-ema``.
* ``pos`` is the overlay position: -1 (THETA rich, hold less), 0, +1 (THETA cheap, hold more).
* Weights are THETA weights within the THETA+TFUEL pair.
"""

from __future__ import annotations

import math
from dataclasses import asdict, dataclass, field
from typing import Any

from bot.config import StrategyParams
from bot.util import MINUTE_MS, minute_of_day, ms_to_date

W0 = 0.5


def ema_alpha(span: float) -> float:
    return 2.0 / (span + 1.0)


def ema_update(ema: float | None, x: float, alpha: float) -> float:
    return x if ema is None else ema + alpha * (x - ema)


def log_ratio(close_theta: float, close_tfuel: float) -> float:
    return math.log(close_theta / close_tfuel)


# ---------------------------------------------------------------------------------------------
# Minute overlay
# ---------------------------------------------------------------------------------------------
@dataclass(frozen=True)
class OverlayParams:
    span_min: int
    entry: float
    exit: float
    ticket: float
    max_hold_min: int
    warmup_bars: int

    @property
    def alpha(self) -> float:
        return ema_alpha(self.span_min)

    @classmethod
    def from_strategy(cls, s: StrategyParams) -> "OverlayParams":
        return cls(
            span_min=s.overlay_ema_span_min,
            entry=s.overlay_entry,
            exit=s.overlay_exit,
            ticket=s.overlay_ticket,
            max_hold_min=s.overlay_max_hold_min,
            warmup_bars=s.effective_overlay_warmup_bars,
        )


@dataclass
class OverlayState:
    ema: float | None = None
    n: int = 0  # bars folded into the EMA
    pos: int = 0
    pos_since_ms: int | None = None  # open time of the bar where the current non-zero pos started

    @property
    def warm(self) -> bool:  # convenience for callers that know the params
        return self.ema is not None


def overlay_transition(pos: int, d: float, entry: float, exit_: float) -> tuple[int, str]:
    """The transition table from the spec. Returns (new_pos, action)."""
    if pos == 0:
        if d > entry:
            return -1, "enter"
        if d < -entry:
            return 1, "enter"
        return 0, "none"
    if pos == -1 and d < -entry:
        return 1, "flip"
    if pos == 1 and d > entry:
        return -1, "flip"
    if abs(d) < exit_:
        return 0, "exit"
    return pos, "none"


def overlay_step(
    state: OverlayState, lr: float, ts_ms: int, stale: bool, p: OverlayParams
) -> tuple[OverlayState, float, str]:
    """Advance the overlay by one closed bar. Returns (new_state, deviation, action).

    * The EMA is always updated (also on stale bars and during warm-up).
    * No transitions during warm-up (fewer than ``warmup_bars`` bars seen).
    * No transitions on stale bars: no open, flip or close - the failsafe waits as well.
    * Failsafe: a non-zero position held longer than ``max_hold_min`` is forced flat.
    """
    ema = ema_update(state.ema, lr, p.alpha)
    n = state.n + 1
    d = lr - ema
    new = OverlayState(ema=ema, n=n, pos=state.pos, pos_since_ms=state.pos_since_ms)
    if n < p.warmup_bars or stale:
        return new, d, "none"

    pos, action = overlay_transition(state.pos, d, p.entry, p.exit)
    if action in ("enter", "flip"):
        new.pos, new.pos_since_ms = pos, ts_ms
    elif action == "exit":
        new.pos, new.pos_since_ms = 0, None
    elif state.pos != 0:
        since = state.pos_since_ms if state.pos_since_ms is not None else ts_ms
        if ts_ms - since > p.max_hold_min * MINUTE_MS:
            new.pos, new.pos_since_ms = 0, None
            action = "failsafe"
    return new, d, action


# ---------------------------------------------------------------------------------------------
# Daily ladder
# ---------------------------------------------------------------------------------------------
@dataclass(frozen=True)
class LadderParams:
    enabled: bool
    span_days: int
    tiers: tuple[tuple[float, float], ...]
    exit: float
    warmup_days: int
    w0: float = W0

    @property
    def alpha(self) -> float:
        return ema_alpha(self.span_days)

    @classmethod
    def from_strategy(cls, s: StrategyParams) -> "LadderParams":
        return cls(
            enabled=s.ladder_enabled,
            span_days=s.ladder_ema_span_days,
            tiers=tuple(sorted(s.ladder_tiers)),
            exit=s.ladder_exit,
            warmup_days=s.ladder_warmup_days,
        )


@dataclass
class LadderState:
    ema: float | None = None
    n_days: int = 0
    cur: float = W0
    last_date: str | None = None  # last UTC date whose close was folded in


def ladder_decide(cur: float, D: float, tiers: tuple[tuple[float, float], ...], exit_: float, w0: float = W0) -> float:
    """Hysteresis rule from the spec; returns the new ladder weight."""
    tgt = None
    for th, shift in tiers:  # ascending
        if D > th:
            tgt = w0 - shift  # THETA rich -> less THETA
        if D < -th:
            tgt = w0 + shift  # THETA cheap -> more THETA
    if tgt is not None:
        # only escalate further from neutral, or flip side; never step back halfway
        if (tgt < w0 and (cur >= w0 or tgt < cur)) or (tgt > w0 and (cur <= w0 or tgt > cur)):
            return tgt
        return cur
    if abs(D) < exit_:
        return w0
    return cur


def ladder_step(state: LadderState, date: str, lr_day: float, p: LadderParams) -> tuple[LadderState, float, float, float]:
    """Fold one daily close in. Returns (new_state, D, w_prev, w_new)."""
    ema = ema_update(state.ema, lr_day, p.alpha)
    n = state.n_days + 1
    D = lr_day - ema
    w_prev = state.cur
    if not p.enabled:
        w_new = p.w0
    elif n < p.warmup_days:
        w_new = state.cur  # stays neutral until the EMA has seen enough closes
    else:
        w_new = ladder_decide(state.cur, D, p.tiers, p.exit, p.w0)
    return LadderState(ema=ema, n_days=n, cur=w_new, last_date=date), D, w_prev, w_new


# ---------------------------------------------------------------------------------------------
# Combination
# ---------------------------------------------------------------------------------------------
def clip(x: float, lo: float, hi: float) -> float:
    return lo if x < lo else hi if x > hi else x


def combine_target(w_ladder: float, pos: int, ticket: float, w_min: float, w_max: float) -> float:
    return round(clip(w_ladder + pos * ticket, w_min, w_max), 10)


@dataclass
class LadderUpdate:
    date: str
    lr_day: float
    ema: float
    D: float
    w_prev: float
    w_new: float
    warm: bool


@dataclass
class BarSignal:
    ts_ms: int
    lr: float
    ema: float
    dev: float
    stale: bool
    pos_prev: int
    pos: int
    action: str
    ladder_w: float
    w_target: float
    overlay_ready: bool
    ladder_updates: list[LadderUpdate] = field(default_factory=list)


class SignalEngine:
    """Per-instance signal state: overlay + ladder, fed bar by bar. Serializable to dict."""

    def __init__(self, params: StrategyParams, state: dict[str, Any] | None = None) -> None:
        self.params = params
        self.op = OverlayParams.from_strategy(params)
        self.lp = LadderParams.from_strategy(params)
        self.overlay = OverlayState()
        self.ladder = LadderState()
        self.last_ts: int | None = None
        self.last_lr: float | None = None
        if state:
            self.load(state)

    # ---- persistence -----------------------------------------------------------------
    def to_dict(self) -> dict[str, Any]:
        return {
            "overlay": asdict(self.overlay),
            "ladder": asdict(self.ladder),
            "last_ts": self.last_ts,
            "last_lr": self.last_lr,
        }

    def load(self, d: dict[str, Any]) -> None:
        self.overlay = OverlayState(**d.get("overlay", {}))
        self.ladder = LadderState(**d.get("ladder", {}))
        self.last_ts = d.get("last_ts")
        self.last_lr = d.get("last_lr")

    def reset_overlay(self) -> None:
        """Forget the minute EMA/position (used when the data gap is too long to backfill)."""
        self.overlay = OverlayState()
        self.last_ts = None
        self.last_lr = None

    # ---- properties ------------------------------------------------------------------
    @property
    def overlay_ready(self) -> bool:
        return self.overlay.n >= self.op.warmup_bars

    @property
    def ladder_w(self) -> float:
        return self.ladder.cur if self.lp.enabled else self.lp.w0

    @property
    def w_target(self) -> float:
        return combine_target(self.ladder_w, self.overlay.pos, self.op.ticket, self.params.w_min, self.params.w_max)

    # ---- feeding -----------------------------------------------------------------------
    def feed_daily_close(self, date: str, lr_day: float) -> LadderUpdate | None:
        """Fold a UTC day's closing log ratio into the ladder. Days already seen are ignored."""
        if self.ladder.last_date is not None and date <= self.ladder.last_date:
            return None
        self.ladder, D, w_prev, w_new = ladder_step(self.ladder, date, lr_day, self.lp)
        return LadderUpdate(
            date=date,
            lr_day=lr_day,
            ema=self.ladder.ema,  # type: ignore[arg-type]
            D=D,
            w_prev=w_prev,
            w_new=w_new,
            warm=self.ladder.n_days >= self.lp.warmup_days,
        )

    def feed_bar(self, ts_ms: int, lr: float, stale: bool) -> BarSignal:
        """Process one closed 1m bar (bars must arrive in order, each exactly once)."""
        if self.last_ts is not None and ts_ms <= self.last_ts:
            raise ValueError(f"bar {ts_ms} is not after last bar {self.last_ts}")
        updates: list[LadderUpdate] = []
        # Fallback day close: the 23:59 bar of the previous day never arrived.
        if self.last_ts is not None and self.last_lr is not None:
            prev_date = ms_to_date(self.last_ts)
            if ms_to_date(ts_ms) > prev_date:
                u = self.feed_daily_close(prev_date, self.last_lr)
                if u:
                    updates.append(u)

        pos_prev = self.overlay.pos
        self.overlay, dev, action = overlay_step(self.overlay, lr, ts_ms, stale, self.op)

        # The 23:59 bar closes the UTC day: run the ladder right after 00:00.
        if minute_of_day(ts_ms) == 1439:
            u = self.feed_daily_close(ms_to_date(ts_ms), lr)
            if u:
                updates.append(u)

        self.last_ts, self.last_lr = ts_ms, lr
        return BarSignal(
            ts_ms=ts_ms,
            lr=lr,
            ema=self.overlay.ema,  # type: ignore[arg-type]
            dev=dev,
            stale=stale,
            pos_prev=pos_prev,
            pos=self.overlay.pos,
            action=action,
            ladder_w=self.ladder_w,
            w_target=self.w_target,
            overlay_ready=self.overlay_ready,
            ladder_updates=updates,
        )


class MarketEma:
    """Market-level 3-day EMA logged into ``bars_*.jsonl`` (global parameters)."""

    def __init__(self, span_min: int, state: dict[str, Any] | None = None) -> None:
        self.alpha = ema_alpha(span_min)
        self.ema: float | None = None
        self.n = 0
        self.last_ts: int | None = None
        if state:
            self.ema = state.get("ema")
            self.n = state.get("n", 0)
            self.last_ts = state.get("last_ts")

    def update(self, ts_ms: int, lr: float) -> tuple[float, float]:
        self.ema = ema_update(self.ema, lr, self.alpha)
        self.n += 1
        self.last_ts = ts_ms
        return self.ema, lr - self.ema

    def to_dict(self) -> dict[str, Any]:
        return {"ema": self.ema, "n": self.n, "last_ts": self.last_ts}
