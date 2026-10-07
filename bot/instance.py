"""One strategy instance (``s1k`` / ``s4k``): signals, paper shadow portfolios, optional live
execution, and all of its JSON output. Shared by the live bot and the replay so both use the
identical code path.
"""

from __future__ import annotations

import math
from typing import Any, Callable

from bot.config import InstanceConfig
from bot.execution_live import LiveExecutor
from bot.execution_paper import VARIANTS, PaperExecutor
from bot.market_data import JointBar
from bot.portfolio import Portfolio, trade_size
from bot.risk import RiskManager
from bot.signals import BarSignal, LadderUpdate, SignalEngine
from bot.storage import SCHEMA_VERSION, Storage
from bot.util import MINUTE_MS, Clock, ms_to_date, ms_to_dt, ms_to_iso

STATE_VERSION = 1
PRIMARY_PAPER = "touch"


def make_decision_id(ts_ms: int, prefix: str = "") -> str:
    """Short, deterministic decision id from the decision bar's open time (UTC): YYMMDDHHMM."""
    return prefix + ms_to_dt(ts_ms).strftime("%y%m%d%H%M")


class Instance:
    def __init__(
        self,
        cfg: InstanceConfig,
        storage: Storage,
        clock: Clock | None = None,
        replay: bool = False,
        summary_every_min: int = 1,
        state_every_min: int = 1,
    ) -> None:
        self.cfg = cfg
        self.name = cfg.name
        self.storage = storage
        self.clock = clock or Clock()
        self.replay = replay
        self.summary_every_min = summary_every_min
        self.state_every_min = state_every_min
        self.engine = SignalEngine(cfg.strategy)
        self.paper: dict[str, Portfolio] = {}
        self.paper_exec = PaperExecutor(
            self.name, storage, cfg.execution.fee_rate, cfg.execution.min_trade_usd, cfg.execution.paper_impact_bps_per_1k
        )
        self.risk = RiskManager(cfg.risk)
        self.executor: LiveExecutor | None = None
        self.live_port: Portfolio | None = None
        self.live_active = False  # a live period is running (benchmark snapshot exists)
        self.live_suspended = False  # kill switch: no orders, benchmark kept
        self.live_activated_ms: int | None = None
        self.live_history: list[dict[str, Any]] = []
        self.last_target: float | None = None
        self.trading_started_ms: int | None = None
        self.ladder_logged_until: str | None = None
        self.last_signal: BarSignal | None = None
        self.last_px: dict[str, float] | None = None
        self.last_bar: JointBar | None = None
        self.last_decision: dict[str, Any] | None = None
        self.kill_switch = False
        self.health: dict[str, Any] = {}
        self._executor_state: dict[str, Any] | None = None
        self.on_change: Callable[[], None] | None = None

    # ---- state ------------------------------------------------------------------------------
    def to_state(self) -> dict[str, Any]:
        return {
            "ts": ms_to_iso(self.clock.now_ms()),
            "schema_version": SCHEMA_VERSION,
            "state_version": STATE_VERSION,
            "instance": self.name,
            "engine": self.engine.to_dict(),
            "last_target": self.last_target,
            "trading_started_ms": self.trading_started_ms,
            "ladder_logged_until": self.ladder_logged_until,
            "last_decision": self.last_decision,
            "paper": {
                "portfolios": {v: p.to_dict() for v, p in self.paper.items()},
                "executor": self.paper_exec.to_dict(),
            },
            "live": {
                "active": self.live_active,
                "suspended": self.live_suspended,
                "activated_ms": self.live_activated_ms,
                "portfolio": self.live_port.to_dict() if self.live_port else None,
                "executor": self.executor.to_dict() if self.executor else self._executor_state,
                "risk": self.risk.to_dict(),
                "history": self.live_history[-20:],
            },
        }

    def load_state(self, st: dict[str, Any] | None) -> None:
        if not st:
            return
        self.engine.load(st.get("engine", {}))
        self.last_target = st.get("last_target")
        self.trading_started_ms = st.get("trading_started_ms")
        self.ladder_logged_until = st.get("ladder_logged_until")
        self.last_decision = st.get("last_decision")
        paper = st.get("paper", {})
        self.paper = {v: Portfolio.from_dict(p) for v, p in paper.get("portfolios", {}).items()}
        self.paper_exec.pending_worst = paper.get("executor", {}).get("pending_worst")
        live = st.get("live", {})
        self.live_active = bool(live.get("active"))
        self.live_suspended = bool(live.get("suspended"))
        self.live_activated_ms = live.get("activated_ms")
        self.live_port = Portfolio.from_dict(live["portfolio"]) if live.get("portfolio") else None
        self._executor_state = live.get("executor")
        if live.get("risk"):
            self.risk = RiskManager(self.cfg.risk, live["risk"])
        self.live_history = list(live.get("history", []))

    def save(self) -> None:
        self.storage.save_state(self.name, self.to_state())

    # ---- helpers ----------------------------------------------------------------------------
    @property
    def started(self) -> bool:
        return self.trading_started_ms is not None and bool(self.paper)

    @property
    def mode(self) -> str:
        return "live" if self.live_active else "paper"

    def primary_variant(self) -> str:
        return "live" if self.live_active and self.live_port is not None else PRIMARY_PAPER

    def primary(self) -> Portfolio | None:
        if self.live_active and self.live_port is not None:
            return self.live_port
        return self.paper.get(PRIMARY_PAPER)

    def all_portfolios(self) -> dict[str, Portfolio]:
        ports = dict(self.paper)
        if self.live_active and self.live_port is not None:
            ports["live"] = self.live_port
        return ports

    def _create_paper(self, px: dict[str, float], ts_ms: int, from_balances: dict[str, float] | None = None) -> None:
        for v in VARIANTS:
            if from_balances is not None:
                self.paper[v] = Portfolio.from_balances(v, from_balances, ts_ms)
            else:
                self.paper[v] = Portfolio.fifty_fifty(v, self.cfg.capital_usd, px, ts_ms)
        self.paper_exec.pending_worst = None

    # ---- ladder log ---------------------------------------------------------------------------
    def log_ladder(self, u: LadderUpdate, ts_ms: int | None = None, emit_event: bool = True) -> None:
        if self.ladder_logged_until is not None and u.date <= self.ladder_logged_until:
            return
        self.ladder_logged_until = u.date
        ts = ts_ms if ts_ms is not None else _day_close_ms(u.date)
        self.storage.write_ladder(
            self.name, ts, date=u.date, lr_day=u.lr_day, ema60=u.ema, D=u.D,
            ladder_w_prev=u.w_prev, ladder_w=u.w_new, warm=u.warm,
        )
        if u.w_new != u.w_prev and emit_event:  # warm-up history goes to ladder.jsonl only
            self.storage.event(
                "INFO", "ladder_change", f"ladder weight {u.w_prev:.2f} -> {u.w_new:.2f} (D={u.D:+.4f})",
                instance=self.name, ts_ms=ts, date=u.date,
            )

    def warm_daily(self, closes: list[tuple[str, float]]) -> int:
        n = 0
        for date, lr in closes:
            u = self.engine.feed_daily_close(date, lr)
            if u:
                self.log_ladder(u, emit_event=False)
                n += 1
        return n

    # ---- per bar -------------------------------------------------------------------------------
    def on_bar(self, bar: JointBar, phase: str, px: dict[str, float]) -> BarSignal:
        """Process one closed bar. ``phase``: warmup | backfill | live."""
        ts = bar.ts_ms
        decided_at = bar.close_ms
        sig = self.engine.feed_bar(ts, bar.lr, bar.stale)
        for u in sig.ladder_updates:
            self.log_ladder(u, emit_event=phase != "warmup")
        self.last_signal = sig
        self.last_px = px
        self.last_bar = bar
        if phase == "warmup":
            return sig
        if sig.action == "failsafe":
            self.storage.event(
                "WARNING", "overlay_failsafe",
                f"overlay position held > {self.cfg.strategy.overlay_max_hold_min} min; forced flat",
                instance=self.name, ts_ms=decided_at,
            )

        if self.started:
            self.paper_exec.on_bar(bar, self.paper, decided_at)  # settle last decision's "worst" fill

        action = sig.action
        if phase == "live":
            if not self.started:
                if sig.overlay_ready:
                    self._start_trading(sig, px, decided_at)
            else:
                if not bar.stale and self.last_target is not None and sig.w_target != self.last_target:
                    self._decide(sig, bar, px, decided_at)
                if self.executor is not None and self.live_active and not bar.stale:
                    kind = self.executor.pump(allowed=not self.live_suspended)
                    if kind == "retry":
                        action = "retry"

        prim = self.primary()
        self.storage.write_signal(
            self.name, ts,
            dev=sig.dev, entry=self.cfg.strategy.overlay_entry, exit=self.cfg.strategy.overlay_exit,
            pos=sig.pos, pos_prev=sig.pos_prev, ladder_w=sig.ladder_w, w_target=sig.w_target,
            w_current=prim.w(px) if prim else None, action=action,
            ema3d=sig.ema, lr=sig.lr, stale=bar.stale, phase=phase,
        )
        minute = decided_at // MINUTE_MS
        if self.started and minute % 5 == 0:
            self.write_equity(decided_at, px, sig.w_target)
        if self.on_change is not None:
            self.on_change()
        if phase == "live" or self.replay:
            if minute % self.summary_every_min == 0:
                self.write_summary(decided_at, px)
            if minute % self.state_every_min == 0:
                self.save()
        return sig

    def _start_trading(self, sig: BarSignal, px: dict[str, float], ts_ms: int) -> None:
        if not self.paper:
            if self.live_active and self.live_port is not None:
                self._create_paper(px, ts_ms, from_balances=self.live_port.bal)
            else:
                self._create_paper(px, ts_ms)
        self.trading_started_ms = ts_ms
        self.last_target = sig.w_target
        prim = self.primary()
        self.storage.event(
            "INFO", "trading_started",
            f"trading started ({self.mode}); w={prim.w(px):.3f}, w_target={sig.w_target:.3f}. "
            "The first w_target change rebalances.",
            instance=self.name, ts_ms=ts_ms, w=prim.w(px), w_target=sig.w_target,
        )

    def _reason(self, sig: BarSignal) -> str:
        parts = []
        if sig.action in ("enter", "exit", "flip", "failsafe"):
            parts.append(f"overlay_{sig.action}")
        if any(u.w_new != u.w_prev for u in sig.ladder_updates):
            parts.append("ladder")
        return "+".join(parts) if parts else "resync"

    def _decide(self, sig: BarSignal, bar: JointBar, px: dict[str, float], ts_ms: int) -> None:
        decision_id = make_decision_id(bar.ts_ms)
        prim = self.primary()
        assert prim is not None
        w_from = prim.w(px)
        dv = trade_size(sig.w_target, prim.bal, px)
        rec = self.storage.write_decision(
            self.name, ts_ms,
            decision_id=decision_id, reason=self._reason(sig), w_from=w_from, w_target=sig.w_target,
            dv_usd=dv, mode=self.mode, w_target_prev=self.last_target, ratio=bar.ratio, lr=bar.lr,
            dev=sig.dev, pos=sig.pos, ladder_w=sig.ladder_w, bar_ts=ms_to_iso(bar.ts_ms),
        )
        dvs = self.paper_exec.execute_decision(decision_id, sig.w_target, self.paper, bar, ts_ms, self.mode)
        if self.live_active and self.executor is not None:
            self.executor.new_decision(decision_id, sig.w_target, px, ts_ms)
        elif self.live_active:
            self.storage.event(
                "WARNING", "live_unavailable",
                f"decision {decision_id}: live execution unavailable; it is queued until live trading resumes",
                instance=self.name, ts_ms=ts_ms,
            )
        self.last_target = sig.w_target
        self.last_decision = {k: rec[k] for k in ("decision_id", "ts", "reason", "w_from", "w_target", "dv_usd", "mode")}
        self.last_decision["dv_by_variant"] = dvs

    # ---- outputs -------------------------------------------------------------------------------
    def write_equity(self, ts_ms: int, px: dict[str, float], w_target: float | None) -> None:
        for v, port in self.all_portfolios().items():
            port.ytd_excess(px, ts_ms)
            self.storage.write_equity(self.name, ts_ms, **port.equity_fields(px, w_target))

    def summary(self, now_ms: int, px: dict[str, float]) -> dict[str, Any]:
        ports = {v: p.summary(px, now_ms) for v, p in self.all_portfolios().items()}
        primary = self.primary_variant()
        prim = ports.get(primary, {})
        sig = self.last_signal
        s = self.cfg.strategy
        live_info: dict[str, Any] = {
            "active": self.live_active,
            "suspended": self.live_suspended,
            "activated": ms_to_iso(self.live_activated_ms),
            "risk": dict(self.risk.to_dict(), blocked=self.risk.blocked_today(now_ms)),
        }
        ex = self.executor
        if ex is not None:
            live_info.update(
                decision=ex.decision, pending_usdt=ex.pending_usdt, busy=ex.busy,
                balances=ex.balances, balances_ts=ms_to_iso(ex.balances_ts),
            )
        return self.storage.make_record(
            now_ms, self.name,
            mode=self.mode,
            live_enabled=self.live_active and not self.live_suspended,
            live_suspended=self.live_suspended,
            kill_switch=self.kill_switch,
            capital_usd=self.cfg.capital_usd,
            primary_variant=primary,
            benchmark_start=prim.get("benchmark_start"),
            excess_by_variant={v: p["excess"] for v, p in ports.items()},
            excess_ytd={v: p["excess_ytd"] for v, p in ports.items()},
            trades_total=prim.get("trades_total", 0),
            trades_30d=prim.get("trades_30d", 0),
            fills_total=prim.get("fills", 0),
            fees_usd=prim.get("fees_usd", 0.0),
            avg_slippage_bps=prim.get("avg_slippage_bps"),
            current={
                "ratio": math.exp(sig.lr) if sig else None,
                "lr": sig.lr if sig else None,
                "ema3d": sig.ema if sig else None,
                "dev": sig.dev if sig else None,
                "pos": sig.pos if sig else None,
                "ladder_w": sig.ladder_w if sig else None,
                "w_target": sig.w_target if sig else None,
                "w_current": prim.get("w"),
                "last_target": self.last_target,
                "ladder_ema60": self.engine.ladder.ema,
                "ladder_days": self.engine.ladder.n_days,
                "overlay_ready": self.engine.overlay_ready,
            },
            last_bar_ts=ms_to_iso(self.last_bar.ts_ms) if self.last_bar else None,
            trading_started=ms_to_iso(self.trading_started_ms),
            last_decision=self.last_decision,
            health=self.health,
            portfolios=ports,
            params={
                "overlay_entry": s.overlay_entry,
                "overlay_exit": s.overlay_exit,
                "overlay_ticket": s.overlay_ticket,
                "overlay_ema_span_min": s.overlay_ema_span_min,
                "ladder_tiers": [list(t) for t in s.ladder_tiers],
                "ladder_exit": s.ladder_exit,
                "ladder_ema_span_days": s.ladder_ema_span_days,
                "w_min": s.w_min,
                "w_max": s.w_max,
            },
            live=live_info,
        )

    def write_summary(self, now_ms: int, px: dict[str, float]) -> None:
        if not self.paper and self.live_port is None:
            return
        self.storage.save_summary(self.name, self.summary(now_ms, px))

    # ---- live lifecycle ---------------------------------------------------------------------
    def attach_executor(self, ex: LiveExecutor) -> None:
        self.executor = ex

    def begin_live_period(self, balances: dict[str, float], px: dict[str, float], now_ms: int) -> None:
        """Snapshot real balances as the new HODL benchmark; paper shadows are re-seeded from the
        same balances so live and paper are compared from the same starting point."""
        self.live_port = Portfolio.from_balances("live", balances, now_ms)
        self.live_active = True
        self.live_suspended = False
        self.live_activated_ms = now_ms
        self._create_paper(px, now_ms, from_balances=balances)
        if self.trading_started_ms is None and self.engine.overlay_ready:
            self.trading_started_ms = now_ms
            self.last_target = self.engine.w_target
        w = self.live_port.w(px)
        self.live_history.append({"activated": ms_to_iso(now_ms), "balances": balances, "w": w})
        self.storage.event(
            "WARNING", "live_activated",
            f"LIVE trading activated. Benchmark snapshot taken; current w={w:.3f}, "
            f"w_target={self.engine.w_target:.3f}. Balances are not converted; the next w_target change rebalances.",
            instance=self.name, ts_ms=now_ms, balances=balances, w=w,
        )
        self.save()

    def end_live_period(self, reason: str, now_ms: int) -> None:
        if not self.live_active:
            return
        self.live_active = False
        self.live_suspended = False
        if self.live_history:
            self.live_history[-1]["ended"] = ms_to_iso(now_ms)
            self.live_history[-1]["reason"] = reason
        self.storage.event("WARNING", "live_deactivated", f"live trading ended: {reason}; paper continues",
                           instance=self.name, ts_ms=now_ms)
        self.save()


def _day_close_ms(date: str) -> int:
    from bot.util import date_to_ms

    return date_to_ms(date) + 86_400_000


def date_of(ts_ms: int) -> str:
    return ms_to_date(ts_ms)
