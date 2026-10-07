"""Orchestrator and CLI.

    python -m bot run
    python -m bot status
    python -m bot init-balance --instance s1k
    python -m bot replay --file theta_tfuel_1m_2023_2025.xlsx --file theta_tfuel_1m_2026.xlsx --instance s1k
"""

from __future__ import annotations

import argparse
import asyncio
import contextlib
import fcntl
import json
import logging
import math
import os
import signal
import sys
from dataclasses import replace
from pathlib import Path
from typing import Any

from bot import __version__
from bot.binance_client import BinanceAPIError, BinanceClient
from bot.config import (
    LIVE_CONFIRM_PHRASE,
    ConfigError,
    Settings,
    load_settings,
    live_config_problems,
    strategy_changed,
)
from bot.execution_live import LiveExecutor
from bot.instance import Instance, make_decision_id
from bot.market_data import SYMBOLS, THETA, TFUEL, JointBar, MarketData
from bot.portfolio import ASSET_OF, prices, theta_weight, trade_size, value_of
from bot.signals import MarketEma
from bot.storage import Storage, read_json
from bot.symbol_filters import SymbolFilters, parse_exchange_info
from bot.util import DAY_MS, MINUTE_MS, Clock, floor_minute, ms_to_date, ms_to_iso

log = logging.getLogger("bot")

DAILY_HISTORY_DAYS = 400
RUNTIME_EVERY_SEC = 15


class BotLock:
    """Exclusive lock on ``DATA_DIR/bot.lock`` so two processes never trade the same state."""

    def __init__(self, data_dir: Path) -> None:
        data_dir.mkdir(parents=True, exist_ok=True)
        self.path = data_dir / "bot.lock"
        self.fh: Any = None

    def acquire(self) -> bool:
        self.fh = open(self.path, "a+")
        try:
            fcntl.flock(self.fh.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError:
            self.fh.close()
            self.fh = None
            return False
        self.fh.seek(0)
        self.fh.truncate()
        self.fh.write(str(os.getpid()))
        self.fh.flush()
        return True

    def release(self) -> None:
        if self.fh is not None:
            with contextlib.suppress(OSError):
                fcntl.flock(self.fh.fileno(), fcntl.LOCK_UN)
            self.fh.close()
            self.fh = None


def setup_logging(level: str) -> None:
    logging.basicConfig(
        level=getattr(logging, level.upper(), logging.INFO),
        format="%(asctime)s %(levelname)-7s %(name)s: %(message)s",
        datefmt="%Y-%m-%dT%H:%M:%S",
    )
    logging.getLogger("httpx").setLevel(logging.WARNING)
    logging.getLogger("websockets").setLevel(logging.WARNING)


class Bot:
    def __init__(self, settings: Settings) -> None:
        self.settings = settings
        self.clock = Clock()
        self.storage = Storage(settings.data_dir, self.clock)
        self.client = BinanceClient(settings.rest_url)
        self.market = MarketData(self.client, settings.ws_url, self.storage, self.on_bar, self.clock)
        self.instances: dict[str, Instance] = {}
        self.filters: dict[str, SymbolFilters] = {}
        mstate = self.storage.load_market_state() or {}
        self.market_ema = MarketEma(settings.strategy.overlay_ema_span_min, mstate.get("ema"))
        self.bars_logged_until: int | None = mstate.get("bars_logged_until")
        self.bnb_price: float | None = None
        self.kill_active = False
        self.live_clients: dict[str, BinanceClient] = {}
        self.live_problem_log: dict[str, str] = {}
        self.stop_event = asyncio.Event()
        self.started_ms = self.clock.now_ms()
        self._mode_lock = asyncio.Lock()
        self._bg: set[asyncio.Task] = set()

    # ---- prices ----------------------------------------------------------------------------
    def price(self, asset: str) -> float | None:
        if asset == "USDT":
            return 1.0
        if asset == "BNB":
            return self.bnb_price
        sym = {"THETA": THETA, "TFUEL": TFUEL}.get(asset)
        return self.market.mid(sym) if sym else None

    def current_px(self) -> dict[str, float] | None:
        th, tf = self.market.mid(THETA), self.market.mid(TFUEL)
        if not th or not tf:
            return None
        return prices(th, tf, self.bnb_price)

    async def fetch_px(self) -> dict[str, float] | None:
        """Current prices: stream mids, else REST book tickers (e.g. right after a restart)."""
        px = self.current_px()
        if px is not None:
            return px
        try:
            mids = {}
            for sym in SYMBOLS:
                t = await self.client.book_ticker(sym)
                mids[sym] = (float(t["bidPrice"]) + float(t["askPrice"])) / 2.0
            return prices(mids[THETA], mids[TFUEL], self.bnb_price)
        except Exception as exc:
            log.warning("price fetch failed: %r", exc)
            return None

    def kill_switch_on(self) -> bool:
        return self.settings.kill_switch or self.settings.kill_file().exists()

    # ---- lifecycle -------------------------------------------------------------------------
    async def run(self) -> int:
        s = self.settings
        self.storage.preload_error_count()
        self.storage.event("INFO", "startup", f"bot {__version__} starting", config=s.public_summary())
        problems = live_config_problems(s)
        if problems:
            for p in problems:
                self.storage.event("CRITICAL", "config_refused", p)
            return 2
        for inst in s.enabled_instances:
            if inst.live_requested and not s.live_confirmed:
                self.storage.event(
                    "WARNING", "live_not_confirmed",
                    f"{inst.prefix}_LIVE=true but LIVE_CONFIRM is not {LIVE_CONFIRM_PHRASE}; staying in PAPER mode",
                    instance=inst.name,
                )
        if not s.enabled_instances:
            self.storage.event("ERROR", "no_instances", "no instance is enabled")
            return 2

        with contextlib.suppress(Exception):
            await self.client.sync_time()
        info = await self.client.exchange_info(list(SYMBOLS))
        self.filters = parse_exchange_info(info, SYMBOLS)
        for rl in info.get("rateLimits", []):
            if rl.get("rateLimitType") == "REQUEST_WEIGHT" and rl.get("interval") == "MINUTE" and rl.get("intervalNum") == 1:
                self.client.weights.limit_1m = int(rl.get("limit", 6000))
        await self.refresh_bnb_price()

        for cfg in s.enabled_instances:
            inst = Instance(cfg, self.storage, self.clock)
            inst.load_state(self.storage.load_state(cfg.name))
            inst.on_change = None
            self.instances[cfg.name] = inst

        await self.warmup()
        self.kill_active = self.kill_switch_on()
        if self.kill_active:
            self.storage.event("CRITICAL", "kill_switch", "kill switch is ACTIVE at startup: no live orders")
        for inst in self.instances.values():
            inst.kill_switch = self.kill_active
        await self.reconcile_live_modes()

        loop = asyncio.get_running_loop()
        for sig in (signal.SIGINT, signal.SIGTERM):
            with contextlib.suppress(NotImplementedError):
                loop.add_signal_handler(sig, self.stop_event.set)

        tasks = [
            asyncio.ensure_future(self.market.run()),
            asyncio.ensure_future(self.config_loop()),
            asyncio.ensure_future(self.kill_loop()),
            asyncio.ensure_future(self.aux_loop()),
        ]
        await self.stop_event.wait()
        self.storage.event("INFO", "shutdown", "shutting down")
        self.market.stop()
        for inst in self.instances.values():
            if inst.executor is not None:
                await inst.executor.stop(wait=True)
            inst.save()
        for t in tasks:
            t.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        await self.client.close()
        for c in self.live_clients.values():
            await c.close()
        self.write_runtime()
        self.storage.close()
        return 0

    # ---- warm-up ---------------------------------------------------------------------------
    async def warmup(self) -> None:
        """Bring the market EMA and every instance up to date from REST history.

        * Instances whose state is recent enough continue from ``state.json`` and the gap is
          replayed bar by bar (phase "backfill": signals are logged, no orders).
        * Otherwise the overlay is re-initialised from 4 x span minute bars (3 x span warm-up
          plus one span so the state machine has history) and the ladder from daily closes.
        """
        now = self.clock.now_ms()
        last_closed = floor_minute(now) - MINUTE_MS
        starts: dict[str, int] = {}
        fresh: set[str] = set()
        need_daily = False
        for name, inst in self.instances.items():
            st = inst.cfg.strategy
            hist = st.effective_overlay_warmup_bars + st.overlay_ema_span_min
            last = inst.engine.last_ts
            if last is None or last < last_closed - hist * MINUTE_MS:
                if last is not None:
                    self.storage.event("WARNING", "rewarm", "state too old to backfill; re-initialising the overlay", instance=name)
                inst.engine.reset_overlay()
                starts[name] = last_closed - (hist - 1) * MINUTE_MS
                fresh.add(name)
                ll = inst.engine.ladder.last_date
                if ll is None or ll < ms_to_date(starts[name] - DAY_MS):
                    need_daily = True
            else:
                starts[name] = last + MINUTE_MS

        span = self.settings.strategy.overlay_ema_span_min
        m_last = self.market_ema.last_ts
        if m_last is None or m_last < last_closed - 3 * span * MINUTE_MS:
            self.market_ema = MarketEma(span)
            m_start = last_closed - (3 * span - 1) * MINUTE_MS
        else:
            m_start = m_last + MINUTE_MS
        start = min([m_start, *starts.values()])

        if need_daily:
            closes = await self.market.fetch_daily_closes(DAILY_HISTORY_DAYS)
            for name in fresh:
                first_day = ms_to_date(starts[name])
                n = self.instances[name].warm_daily([c for c in closes if c[0] < first_day])
                self.storage.event("INFO", "ladder_warmup", f"ladder warmed with {n} daily closes", instance=name)

        bars = await self.market.fetch_bars(start, last_closed) if start <= last_closed else []
        self.storage.event(
            "INFO", "warmup", f"processing {len(bars)} historical bars from {ms_to_iso(start)}",
            fresh=sorted(fresh), continuing=sorted(set(self.instances) - fresh),
        )
        for bar in bars:
            px = prices(bar.theta.c, bar.tfuel.c, self.bnb_price)
            self._market_bar(bar)
            for name, inst in self.instances.items():
                if inst.engine.last_ts is not None and bar.ts_ms <= inst.engine.last_ts:
                    continue
                inst.on_bar(bar, "warmup" if name in fresh else "backfill", px)
        if bars:
            self.market.start_from(bars[-1].ts_ms, {THETA: bars[-1].theta.c, TFUEL: bars[-1].tfuel.c})
        else:
            self.market.start_from(min(i.engine.last_ts or last_closed for i in self.instances.values()), {})
        for name, inst in self.instances.items():
            if not inst.engine.overlay_ready:
                self.storage.event(
                    "WARNING", "warmup_incomplete",
                    f"only {inst.engine.overlay.n} of {inst.engine.op.warmup_bars} warm-up bars available; "
                    "trading starts when warm-up completes", instance=name,
                )
            inst.save()
        self.save_market_state()

    def _market_bar(self, bar: JointBar) -> None:
        if self.market_ema.last_ts is not None and bar.ts_ms <= self.market_ema.last_ts:
            return
        ema, dev = self.market_ema.update(bar.ts_ms, bar.lr)
        if self.bars_logged_until is not None and bar.ts_ms <= self.bars_logged_until:
            return
        th_b, th_a = bar.book[THETA] if bar.book else (None, None)
        tf_b, tf_a = bar.book[TFUEL] if bar.book else (None, None)
        k1, k2 = bar.theta, bar.tfuel
        self.storage.write_bar(
            bar.ts_ms,
            theta={"o": k1.o, "h": k1.h, "l": k1.l, "c": k1.c, "v": k1.v, "bid": th_b, "ask": th_a},
            tfuel={"o": k2.o, "h": k2.h, "l": k2.l, "c": k2.c, "v": k2.v, "bid": tf_b, "ask": tf_a},
            ratio=bar.ratio, lr=bar.lr, ema3d=ema, ema3d_ratio=math.exp(ema), dev=dev, stale=bar.stale,
            synthetic=bar.synthetic,
        )
        self.bars_logged_until = bar.ts_ms

    def save_market_state(self) -> None:
        self.storage.save_market_state(
            {"ts": ms_to_iso(self.clock.now_ms()), "schema_version": 1, "ema": self.market_ema.to_dict(),
             "bars_logged_until": self.bars_logged_until}
        )

    # ---- per bar -------------------------------------------------------------------------------
    async def on_bar(self, bar: JointBar, phase: str) -> None:
        px = prices(bar.mid(THETA), bar.mid(TFUEL), self.bnb_price)
        self._market_bar(bar)
        health = self.health()
        for inst in self.instances.values():
            if inst.engine.last_ts is not None and bar.ts_ms <= inst.engine.last_ts:
                continue
            inst.health = health
            inst.kill_switch = self.kill_active
            try:
                inst.on_bar(bar, phase, px)
            except Exception as exc:  # one instance must never take the others down
                log.exception("[%s] bar processing failed", inst.name)
                self.storage.event("ERROR", "bar_failed", f"processing bar {ms_to_iso(bar.ts_ms)} failed: {exc!r}",
                                   instance=inst.name)
                continue
            if phase == "live" and inst.live_active and inst.executor is not None and not inst.executor.busy:
                self._spawn(self._refresh_live(inst))
        self.save_market_state()
        self.write_runtime()

    def _spawn(self, coro: Any) -> None:
        t = asyncio.ensure_future(coro)
        self._bg.add(t)
        t.add_done_callback(self._bg.discard)

    async def _refresh_live(self, inst: Instance) -> None:
        try:
            await inst.executor.refresh_balances()  # type: ignore[union-attr]
        except Exception as exc:
            log.warning("[%s] balance refresh failed: %r", inst.name, exc)

    def health(self) -> dict[str, Any]:
        h = self.market.health()
        return {"ws_connected": h["ws_connected"], "lag_sec": h["lag_sec"], "errors_24h": self.storage.errors_24h(),
                "reconnects": h["reconnects"]}

    def write_runtime(self) -> None:
        self.storage.save_runtime(
            {
                "ts": ms_to_iso(self.clock.now_ms()),
                "schema_version": 1,
                "version": __version__,
                "pid": os.getpid(),
                "started": ms_to_iso(self.started_ms),
                "testnet": self.settings.binance_testnet,
                "ws": self.market.health(),
                "errors_24h": self.storage.errors_24h(),
                "kill_switch": {
                    "active": self.kill_active,
                    "env": self.settings.kill_switch,
                    "file": self.settings.kill_file().exists(),
                },
                "bnb_price": self.bnb_price,
                "instances": {
                    n: {"mode": i.mode, "live_suspended": i.live_suspended,
                        "live_configured": self.settings.live_allowed(n) if n in [c.name for c in self.settings.instances] else False}
                    for n, i in self.instances.items()
                },
            }
        )

    # ---- live mode management -----------------------------------------------------------------
    def _client_for(self, name: str) -> BinanceClient:
        cfg = self.settings.instance(name)
        c = self.live_clients.get(name)
        if c is None:
            c = BinanceClient(self.settings.rest_url, cfg.api_key, cfg.api_secret)
            c.time_offset_ms = self.client.time_offset_ms
            self.live_clients[name] = c
        return c

    def _make_executor(self, inst: Instance) -> LiveExecutor:
        cfg = self.settings.instance(inst.name)
        return LiveExecutor(
            instance=inst.name,
            client=self._client_for(inst.name),
            filters=self.filters,
            params=cfg.execution,
            risk=inst.risk,
            storage=self.storage,
            portfolio_fn=lambda: inst.live_port,
            book_fn=self.market.book_top,
            price_fn=self.price,
            save_fn=inst.save,
            clock=self.clock,
            state=inst._executor_state,
        )

    async def reconcile_live_modes(self) -> None:
        """Bring every instance's live/paper mode in line with config + kill switch."""
        async with self._mode_lock:
            for name, inst in self.instances.items():
                try:
                    await self._reconcile_one(name, inst)
                except Exception as exc:
                    self.storage.event("ERROR", "live_mode_error", f"live mode change failed: {exc!r}", instance=name)

    async def _reconcile_one(self, name: str, inst: Instance) -> None:
        want = self.settings.live_allowed(name)
        now = self.clock.now_ms()
        if inst.live_active and self.kill_active:
            if not inst.live_suspended:
                inst.live_suspended = True
                if inst.executor is not None:
                    await inst.executor.kill()
                self.storage.event("CRITICAL", "kill_switch", "kill switch: live trading stopped, open orders cancelled; paper continues", instance=name)
                inst.save()
            return
        if not want:
            if inst.live_active:
                if inst.executor is not None:
                    await inst.executor.stop(wait=True)
                    inst._executor_state = inst.executor.to_dict()
                inst.end_live_period("live disabled in configuration", now)
            inst.executor = None
            return
        if self.kill_active:
            return  # configured for live but killed: stay in paper until the switch is cleared
        if inst.executor is None:
            ex = self._make_executor(inst)
            problems = await ex.startup_checks(self.settings.binance_testnet)
            if problems:
                key = "|".join(problems)
                if self.live_problem_log.get(name) != key:
                    self.live_problem_log[name] = key
                    for p in problems:
                        self.storage.event("ERROR", "live_refused", p, instance=name)
                if inst.live_active and not inst.live_suspended:
                    inst.live_suspended = True
                    inst.save()
                return
            self.live_problem_log.pop(name, None)
            await ex.reconcile()
            inst.attach_executor(ex)
            total, _ = await ex.refresh_balances()
            px = await self.fetch_px()
            if px is None:
                self.storage.event("ERROR", "live_refused", "no market prices available; retrying in 60 s", instance=name)
                inst.executor = None
                return
            if not inst.live_active:
                inst.begin_live_period(total, px, now)
            else:
                inst.live_port.bal = dict(total)  # type: ignore[union-attr]
                inst.live_suspended = False
                self.storage.event("WARNING", "live_resumed", f"LIVE trading resumed; w={theta_weight(total, px):.3f}", instance=name)
                self._requeue_live_decision(inst, px, now)
                inst.save()
        elif inst.live_suspended:
            inst.live_suspended = False
            self.storage.event("WARNING", "live_resumed", "kill switch cleared: LIVE trading resumed", instance=name)
            inst.save()

    def _requeue_live_decision(self, inst: Instance, px: dict[str, float], now: int) -> None:
        """A live decision taken while no executor was attached must not be lost."""
        ld = inst.last_decision
        ex = inst.executor
        if not ld or ld.get("mode") != "live" or ex is None:
            return
        if ex.decision is not None and ex.decision.get("id") == ld.get("decision_id"):
            return
        ex.new_decision(ld["decision_id"], float(ld["w_target"]), px, now)
        self.storage.event("WARNING", "live_resync", f"re-queued live decision {ld['decision_id']} "
                           f"(w_target={ld['w_target']:.3f}) taken while live execution was unavailable", instance=inst.name)

    # ---- loops --------------------------------------------------------------------------------
    async def kill_loop(self) -> None:
        while True:
            await asyncio.sleep(2)
            active = self.kill_switch_on()
            if active != self.kill_active:
                self.kill_active = active
                if active:
                    self.storage.event("CRITICAL", "kill_switch", "kill switch ACTIVATED (KILL_SWITCH or data/KILL)")
                else:
                    self.storage.event("WARNING", "kill_switch", "kill switch cleared")
                for inst in self.instances.values():
                    inst.kill_switch = active
                await self.reconcile_live_modes()
                self.write_runtime()

    async def config_loop(self) -> None:
        while True:
            await asyncio.sleep(60)
            try:
                new = load_settings(self.settings.env_file)
            except ConfigError as exc:
                self.storage.event("ERROR", "config_reload_failed", f".env reload failed, keeping previous config: {exc}")
                continue
            problems = live_config_problems(new)
            if problems:
                for p in problems:
                    self.storage.event("ERROR", "config_reload_refused", p)
                continue
            old = self.settings
            for name in strategy_changed(old, new):
                self.storage.event("WARNING", "restart_required",
                                   "strategy parameters, capital or API keys changed in .env; restart the bot to apply", instance=name)
            changed = []
            for cfg in new.instances:
                try:
                    o = old.instance(cfg.name)
                except KeyError:
                    continue
                if (o.live_requested, o.enabled) != (cfg.live_requested, cfg.enabled):
                    changed.append(f"{cfg.name}: live={cfg.live_requested} enabled={cfg.enabled}")
            if old.live_confirmed != new.live_confirmed:
                changed.append(f"LIVE_CONFIRM {'set' if new.live_confirmed else 'cleared'}")
            if old.kill_switch != new.kill_switch:
                changed.append(f"KILL_SWITCH={new.kill_switch}")
            if old.binance_testnet != new.binance_testnet:
                self.storage.event("WARNING", "restart_required", "BINANCE_TESTNET changed; restart the bot to apply")
                new = replace(new, binance_testnet=old.binance_testnet, rest_url=old.rest_url, ws_url=old.ws_url)
            # keep strategy params/keys of the running process; apply execution + risk hot
            insts = []
            for cfg in new.instances:
                try:
                    o = old.instance(cfg.name)
                    cfg = replace(cfg, strategy=o.strategy, capital_usd=o.capital_usd, api_key=o.api_key, api_secret=o.api_secret)
                except KeyError:
                    pass
                insts.append(cfg)
                inst = self.instances.get(cfg.name)
                if inst is not None:
                    inst.cfg = cfg
                    inst.risk.params = cfg.risk
                    inst.paper_exec.fee_rate = cfg.execution.fee_rate
                    inst.paper_exec.min_trade_usd = cfg.execution.min_trade_usd
                    if inst.executor is not None:
                        inst.executor.params = cfg.execution
            self.settings = replace(new, instances=tuple(insts), strategy=old.strategy, data_dir=old.data_dir)
            if changed:
                self.storage.event("WARNING", "mode_change", "configuration changed: " + "; ".join(changed))
            self.kill_active = self.kill_switch_on()
            await self.reconcile_live_modes()

    async def refresh_bnb_price(self) -> None:
        try:
            self.bnb_price = await self.client.ticker_price("BNBUSDT")
        except Exception as exc:
            log.debug("BNB price unavailable: %r", exc)

    async def aux_loop(self) -> None:
        n = 0
        while True:
            await asyncio.sleep(RUNTIME_EVERY_SEC)
            n += 1
            if n % 4 == 0:
                await self.refresh_bnb_price()
            self.write_runtime()
            if self.market.last_emitted is not None and (self.market.lag_sec() or 0) > 180:
                px = self.current_px()
                if px:
                    for inst in self.instances.values():
                        inst.health = self.health()
                        inst.write_summary(self.clock.now_ms(), px)


# ---------------------------------------------------------------------------------------------
# CLI commands
# ---------------------------------------------------------------------------------------------
def cmd_run(args: argparse.Namespace) -> int:
    settings = load_settings(args.env)
    setup_logging(settings.log_level)
    lock = BotLock(settings.data_dir)
    if not lock.acquire():
        print(f"another bot process holds {lock.path}; refusing to start", file=sys.stderr)
        return 3
    try:
        return asyncio.run(Bot(settings).run())
    finally:
        lock.release()


def _pct(x: Any) -> str:
    return "-" if x is None else f"{x*100:+.2f}%"


def cmd_status(args: argparse.Namespace) -> int:
    settings = load_settings(args.env)
    data = settings.data_dir
    rt = read_json(data / "runtime.json", {})
    print(f"data dir     : {data.resolve()}")
    print(f"testnet      : {settings.binance_testnet}   live confirmed: {settings.live_confirmed}")
    ks = settings.kill_switch or settings.kill_file().exists()
    print(f"kill switch  : {'ACTIVE' if ks else 'off'}")
    if rt:
        ws = rt.get("ws", {})
        print(f"runtime      : updated {rt.get('ts')}  ws_connected={ws.get('ws_connected')}  last bar {ws.get('last_bar_ts')}  lag {ws.get('lag_sec')}s")
    problems = live_config_problems(settings)
    for p in problems:
        print(f"CONFIG ERROR : {p}")
    for cfg in settings.instances:
        s = read_json(data / "instances" / cfg.name / "summary.json")
        live_cfg = settings.live_allowed(cfg.name)
        print()
        print(f"[{cfg.name}] enabled={cfg.enabled} capital={cfg.capital_usd:g} USD live configured={live_cfg}")
        if not s:
            print("  no summary yet")
            continue
        cur = s.get("current", {})
        ex = " ".join(f"{v}={_pct(x)}" for v, x in s.get("excess_by_variant", {}).items())
        print(f"  mode={s.get('mode')} primary={s.get('primary_variant')} benchmark since {s.get('benchmark_start')}")
        print(f"  excess vs HODL: {ex}")
        print(f"  ratio={cur.get('ratio')} dev={cur.get('dev')} pos={cur.get('pos')} ladder_w={cur.get('ladder_w')} "
              f"w_target={cur.get('w_target')} w_current={cur.get('w_current')}")
        print(f"  trades total/30d={s.get('trades_total')}/{s.get('trades_30d')} fees={s.get('fees_usd'):.2f} USD "
              f"avg slippage={s.get('avg_slippage_bps')} bps  last bar {s.get('last_bar_ts')}")
    return 0


def cmd_init_balance(args: argparse.Namespace) -> int:
    settings = load_settings(args.env)
    setup_logging(settings.log_level)
    name = args.instance
    try:
        cfg = settings.instance(name)
    except KeyError:
        print(f"unknown instance {name}", file=sys.stderr)
        return 2
    if not settings.live_allowed(name):
        print(f"{name} is not configured for live trading: need {cfg.prefix}_LIVE=true, "
              f"LIVE_CONFIRM={LIVE_CONFIRM_PHRASE} and {cfg.prefix}_API_KEY/SECRET", file=sys.stderr)
        return 2
    problems = live_config_problems(settings)
    if problems:
        print("\n".join(problems), file=sys.stderr)
        return 2
    if settings.kill_switch or settings.kill_file().exists():
        print("kill switch is active; refusing", file=sys.stderr)
        return 2
    lock = BotLock(settings.data_dir)
    if not lock.acquire():
        print("the bot is running (data/bot.lock is held). Stop it first, then run init-balance.", file=sys.stderr)
        return 3
    try:
        return asyncio.run(_init_balance(settings, name, args.yes))
    finally:
        lock.release()


async def _init_balance(settings: Settings, name: str, assume_yes: bool) -> int:
    cfg = settings.instance(name)
    clock = Clock()
    storage = Storage(settings.data_dir, clock)
    client = BinanceClient(settings.rest_url, cfg.api_key, cfg.api_secret)
    px_cache: dict[str, float] = {"USDT": 1.0}
    try:
        with contextlib.suppress(Exception):
            await client.sync_time()
        filters = parse_exchange_info(await client.exchange_info(list(SYMBOLS)), SYMBOLS)
        with contextlib.suppress(Exception):
            px_cache["BNB"] = await client.ticker_price("BNBUSDT")
        inst = Instance(cfg, storage, clock)
        inst.load_state(storage.load_state(name))
        ex = LiveExecutor(
            instance=name, client=client, filters=filters, params=cfg.execution, risk=inst.risk, storage=storage,
            portfolio_fn=lambda: inst.live_port, book_fn=lambda s: None, price_fn=lambda a: px_cache.get(a),
            save_fn=inst.save, clock=clock, state=inst._executor_state,
        )
        problems = await ex.startup_checks(settings.binance_testnet)
        if problems:
            print("refusing:\n  " + "\n  ".join(problems), file=sys.stderr)
            return 2
        await ex.reconcile()
        total, _ = await ex.refresh_balances()
        px = await ex.mids()
        px_cache.update(px)
        w = theta_weight(total, px)
        dv = trade_size(0.5, total, px)
        print(f"{name} balances: " + ", ".join(f"{a}={q:.6g}" for a, q in total.items()))
        print(f"value {value_of(total, px):.2f} USD, THETA weight w={w:.4f}")
        if abs(dv) < cfg.execution.min_trade_usd:
            print("already at 50/50 (|dv| < MIN_TRADE_USD); nothing to do")
            return 0
        side = "sell TFUEL, buy THETA" if dv > 0 else "sell THETA, buy TFUEL"
        print(f"plan: {side} for about {abs(dv):.2f} USD (LIMIT IOC, slices <= {cfg.execution.max_slice_usd:g} USD)")
        if not assume_yes:
            ans = input(f"Type '{name}' to place REAL orders on Binance{' TESTNET' if settings.binance_testnet else ''}: ")
            if ans.strip() != name:
                print("aborted")
                return 1
        decision_id = make_decision_id(clock.now_ms(), "ib")
        storage.write_decision(name, clock.now_ms(), decision_id=decision_id, reason="init_balance", w_from=w,
                               w_target=0.5, dv_usd=dv, mode="live")
        ex.new_decision(decision_id, 0.5, px, clock.now_ms())
        d = ex.decision
        while True:
            outcome = await ex.run_attempt(d)  # type: ignore[arg-type]
            if outcome == "done" or d["status"] in ("done", "failed"):  # type: ignore[index]
                break
            print(f"attempt {d['attempts']} incomplete; retrying in 60s")  # type: ignore[index]
            await asyncio.sleep(60)
        total, _ = await ex.refresh_balances()
        px = await ex.mids()
        print(f"result: {d['status']}; w={theta_weight(total, px):.4f}; pending USDT {ex.pending_usdt:.2f}")  # type: ignore[index]
        if d["status"] == "done":  # type: ignore[index]
            inst._executor_state = ex.to_dict()
            inst.executor = ex
            if inst.engine.last_ts is not None:
                inst.begin_live_period(total, px, clock.now_ms())
                storage.event("WARNING", "init_balance", "account rebalanced to 50/50; live benchmark re-snapshotted",
                              instance=name)
            else:
                storage.event("WARNING", "init_balance", "account rebalanced to 50/50", instance=name)
            inst.executor = None
            inst._executor_state = ex.to_dict()
            inst.save()
            return 0
        return 1
    finally:
        await client.close()
        storage.close()


def cmd_replay(args: argparse.Namespace) -> int:
    from bot.replay import format_report, run_replay

    settings = load_settings(args.env)
    setup_logging(settings.log_level)
    names: list[str] = []
    for item in args.instance or ["all"]:
        for n in item.split(","):
            n = n.strip()
            if n == "all":
                names.extend(c.name for c in settings.instances)
            elif n:
                names.append(n)
    names = list(dict.fromkeys(names))
    for n in names:
        settings.instance(n)  # validates
    report = run_replay(
        settings, args.file, names, out_dir=args.out, start=args.start, end=args.end,
        overlay_warmup_bars=args.overlay_warmup_bars, ladder_warmup_days=args.ladder_warmup_days,
    )
    print(format_report(report))
    print(f"\nreport: {Path(args.out) / 'replay_report.json'}; view with: DATA_DIR={args.out} python -m dashboard")
    return 0


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(prog="python -m bot", description="THETA/TFUEL ratio trading bot (Binance Spot)")
    p.add_argument("--env", default=os.environ.get("BOT_ENV_FILE", ".env"), help="path to the .env file")
    sub = p.add_subparsers(dest="cmd", required=True)
    sub.add_parser("run", help="run the bot (paper by default)").set_defaults(func=cmd_run)
    sub.add_parser("status", help="print instance summaries from DATA_DIR").set_defaults(func=cmd_status)
    ib = sub.add_parser("init-balance", help="rebalance a LIVE account to 50/50 THETA/TFUEL (interactive)")
    ib.add_argument("--instance", required=True)
    ib.add_argument("--yes", action="store_true", help="skip the interactive confirmation")
    ib.set_defaults(func=cmd_init_balance)
    rp = sub.add_parser("replay", help="replay historical 1m data (xlsx/csv) through the strategy")
    rp.add_argument("--file", action="append", required=True, help="input file; repeat for several files")
    rp.add_argument("--instance", action="append", help="s1k, s4k, s1k,s4k or all (default all)")
    rp.add_argument("--out", default="data_replay", help="output directory (default data_replay)")
    rp.add_argument("--start", help="ISO date/time: bars before it only warm up the indicators")
    rp.add_argument("--end", help="ISO date/time: stop after this bar")
    rp.add_argument("--overlay-warmup-bars", type=int, help="override the overlay warm-up (default 3 x span)")
    rp.add_argument("--ladder-warmup-days", type=int, help="override the ladder warm-up (default 180)")
    rp.set_defaults(func=cmd_replay)
    return p


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        return int(args.func(args) or 0)
    except ConfigError as exc:
        print(f"configuration error: {exc}", file=sys.stderr)
        return 2
    except BinanceAPIError as exc:
        print(f"Binance API error: {exc}", file=sys.stderr)
        return 4
