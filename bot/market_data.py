"""Market data: one combined WebSocket stream shared by all instances, plus REST backfill.

Streams: ``thetausdt@kline_1m``, ``tfuelusdt@kline_1m``, ``thetausdt@bookTicker``,
``tfuelusdt@bookTicker``.

Bars are emitted strictly in order, each minute exactly once, only once *both* symbols have a
closed bar with the same open time. Any hole (disconnect, missed close event, Binance's forced
24h disconnect, a symbol with no trades) is filled from REST klines, so bars and EMAs stay
continuous. A minute that is still missing for a symbol two minutes after it closed is
synthesised from the previous close with ``volume = 0`` (and is therefore ``stale``).
"""

from __future__ import annotations

import asyncio
import json
import logging
import math
import random
import time
from dataclasses import dataclass
from typing import Any, Awaitable, Callable

from bot.binance_client import BinanceClient
from bot.storage import Storage
from bot.util import DAY_MS, MINUTE_MS, Clock, floor_minute, ms_to_date, ms_to_iso

log = logging.getLogger("bot.market")

THETA, TFUEL = "THETAUSDT", "TFUELUSDT"
SYMBOLS = (THETA, TFUEL)
LIVE_WINDOW_MS = 150_000  # a bar that closed less than 2.5 minutes ago is "live"
GRACE_MS = 10_000  # wait this long after a bar's close before asking REST for it
SYNTH_AFTER_MS = 120_000  # missing minutes older than this are synthesised (volume 0)
BOOK_MAX_AGE_MS = 60_000
STALL_SEC = 45.0  # no message at all for this long -> reconnect
ROTATE_AFTER_SEC = 23.5 * 3600  # reconnect before Binance's forced 24h disconnect
MAX_RUNTIME_BACKFILL_MS = 10 * DAY_MS


@dataclass(slots=True)
class Kline:
    open_ms: int
    o: float
    h: float
    l: float
    c: float
    v: float
    synthetic: bool = False

    @classmethod
    def from_rest(cls, row: list) -> "Kline":
        return cls(int(row[0]), float(row[1]), float(row[2]), float(row[3]), float(row[4]), float(row[5]))

    @classmethod
    def from_ws(cls, k: dict) -> "Kline":
        return cls(int(k["t"]), float(k["o"]), float(k["h"]), float(k["l"]), float(k["c"]), float(k["v"]))

    @classmethod
    def flat(cls, open_ms: int, price: float) -> "Kline":
        return cls(open_ms, price, price, price, price, 0.0, synthetic=True)


@dataclass(slots=True)
class BookTop:
    bid: float
    ask: float
    ts_ms: int

    @property
    def mid(self) -> float:
        return (self.bid + self.ask) / 2.0


@dataclass(slots=True)
class JointBar:
    ts_ms: int  # open time of the minute
    theta: Kline
    tfuel: Kline
    book: dict[str, tuple[float, float]] | None = None  # best bid/ask at emission (live bars only)

    @property
    def close_ms(self) -> int:
        return self.ts_ms + MINUTE_MS

    @property
    def ratio(self) -> float:
        return self.theta.c / self.tfuel.c

    @property
    def lr(self) -> float:
        return math.log(self.theta.c / self.tfuel.c)

    @property
    def stale(self) -> bool:
        return self.theta.v == 0 or self.tfuel.v == 0

    @property
    def synthetic(self) -> bool:
        return self.theta.synthetic or self.tfuel.synthetic

    def kline(self, symbol: str) -> Kline:
        return self.theta if symbol == THETA else self.tfuel

    def bid_ask(self, symbol: str) -> tuple[float, float]:
        """Touch prices at the bar close; falls back to the close when no book is available."""
        if self.book and symbol in self.book:
            return self.book[symbol]
        c = self.kline(symbol).c
        return c, c

    def mid(self, symbol: str) -> float:
        b, a = self.bid_ask(symbol)
        return (b + a) / 2.0


def join_klines(
    rows: dict[str, dict[int, Kline]],
    start_ms: int,
    end_ms: int,
    prev_close: dict[str, float] | None,
    synth_until_ms: int,
) -> list[JointBar]:
    """Join per-symbol klines minute by minute.

    A minute missing for a symbol is synthesised from its previous close when it is older than
    ``synth_until_ms``; otherwise joining stops there (the data may still arrive).
    """
    prev = dict(prev_close or {})
    out: list[JointBar] = []
    m = start_ms
    while m <= end_ms:
        ks: dict[str, Kline] = {}
        for sym in SYMBOLS:
            k = rows.get(sym, {}).get(m)
            if k is None and m <= synth_until_ms and sym in prev:
                k = Kline.flat(m, prev[sym])
            if k is not None:
                ks[sym] = k
        if len(ks) < len(SYMBOLS):
            if m > synth_until_ms:
                break  # live edge: the data may still arrive, stop here
            m += MINUTE_MS  # series start (no previous close yet): skip the minute
            continue
        for sym in SYMBOLS:
            prev[sym] = ks[sym].c
        out.append(JointBar(m, ks[THETA], ks[TFUEL]))
        m += MINUTE_MS
    return out


OnBar = Callable[[JointBar, str], Awaitable[None]]


def _short(exc: BaseException) -> str:
    status = getattr(getattr(exc, "response", None), "status_code", None)
    text = f"{type(exc).__name__}" + (f" (HTTP {status})" if status else f": {exc}")
    return text[:300]


class MarketData:
    def __init__(
        self,
        client: BinanceClient,
        ws_url: str,
        storage: Storage,
        on_bar: OnBar,
        clock: Clock | None = None,
    ) -> None:
        self.client = client
        self.ws_url = ws_url.rstrip("/")
        self.storage = storage
        self.on_bar = on_bar
        self.clock = clock or Clock()
        self.book: dict[str, BookTop] = {}
        self.last_price: dict[str, float] = {}
        self.last_close: dict[str, float] = {}
        self.last_emitted: int | None = None
        self._pending: dict[int, dict[str, Kline]] = {}
        self._wake = asyncio.Event()
        self._stop = asyncio.Event()
        self._ws: Any = None
        self.ws_connected = False
        self.connected_since_ms: int | None = None
        self.last_msg_ms = 0
        self.reconnects = 0
        self.last_bar_processed_ms: int | None = None
        self._backfill_failures = 0
        self._backfill_fail_logged_ms = 0
        self._ws_fail_logged_ms = 0

    # ---- helpers -------------------------------------------------------------------------
    def stream_url(self) -> str:
        streams = [f"{s.lower()}@kline_1m" for s in SYMBOLS] + [f"{s.lower()}@bookTicker" for s in SYMBOLS]
        return f"{self.ws_url}/stream?streams={'/'.join(streams)}"

    def book_top(self, symbol: str, max_age_ms: int = BOOK_MAX_AGE_MS) -> BookTop | None:
        b = self.book.get(symbol)
        if b is None or self.clock.now_ms() - b.ts_ms > max_age_ms or b.bid <= 0 or b.ask <= 0:
            return None
        return b

    def mid(self, symbol: str) -> float | None:
        """Mid from the book; falls back to the last 1m close when the book is unavailable."""
        b = self.book_top(symbol)
        if b is not None:
            return b.mid
        return self.last_close.get(symbol) or self.last_price.get(symbol)

    def book_snapshot(self) -> dict[str, tuple[float, float]] | None:
        snap = {}
        for sym in SYMBOLS:
            b = self.book_top(sym)
            if b is None:
                return None
            snap[sym] = (b.bid, b.ask)
        return snap

    def lag_sec(self) -> float | None:
        if self.last_emitted is None:
            return None
        return max(0.0, (self.clock.now_ms() - (self.last_emitted + MINUTE_MS)) / 1000.0)

    def health(self) -> dict[str, Any]:
        return {
            "ws_connected": self.ws_connected,
            "connected_since": ms_to_iso(self.connected_since_ms),
            "last_msg": ms_to_iso(self.last_msg_ms or None),
            "reconnects": self.reconnects,
            "lag_sec": self.lag_sec(),
            "last_bar_ts": ms_to_iso(self.last_emitted),
        }

    # ---- REST history ---------------------------------------------------------------------
    async def fetch_bars(self, start_ms: int, end_ms: int, prev_close: dict[str, float] | None = None) -> list[JointBar]:
        """Closed joint bars with open time in [start_ms, end_ms] from REST klines."""
        now = self.clock.now_ms()
        end_ms = min(end_ms, floor_minute(now) - MINUTE_MS)
        if end_ms < start_ms:
            return []
        rows: dict[str, dict[int, Kline]] = {}
        for sym in SYMBOLS:
            raw = await self.client.klines_range(sym, "1m", start_ms, end_ms)
            rows[sym] = {int(r[0]): Kline.from_rest(r) for r in raw if int(r[0]) + MINUTE_MS <= now}
        return join_klines(rows, start_ms, end_ms, prev_close, now - SYNTH_AFTER_MS)

    async def fetch_daily_closes(self, days: int) -> list[tuple[str, float]]:
        """(date, ln(close_THETA/close_TFUEL)) for completed UTC days, oldest first.

        The close of a daily kline is the last trade of the day, i.e. the close of its 23:59 bar.
        """
        now = self.clock.now_ms()
        today = now - now % DAY_MS
        start = today - days * DAY_MS
        closes: dict[str, dict[int, float]] = {}
        for sym in SYMBOLS:
            raw = await self.client.klines_range(sym, "1d", start, today - DAY_MS)
            closes[sym] = {int(r[0]): float(r[4]) for r in raw if int(r[0]) + DAY_MS <= now}
        out = []
        for t in sorted(set(closes[THETA]) & set(closes[TFUEL])):
            out.append((ms_to_date(t), math.log(closes[THETA][t] / closes[TFUEL][t])))
        return out

    # ---- runtime ---------------------------------------------------------------------------
    def start_from(self, last_emitted_ms: int, last_close: dict[str, float]) -> None:
        """Called after warm-up: live emission continues right after this bar."""
        self.last_emitted = last_emitted_ms
        self.last_close.update(last_close)

    async def run(self) -> None:
        await asyncio.gather(self._ws_loop(), self._emitter(), self._watchdog())

    def stop(self) -> None:
        self._stop.set()
        self._wake.set()
        if self._ws is not None:
            asyncio.ensure_future(self._ws.close())

    def force_reconnect(self) -> None:
        if self._ws is not None:
            asyncio.ensure_future(self._ws.close())

    async def _ws_loop(self) -> None:
        from websockets.asyncio.client import connect

        backoff = 1.0
        first = True
        while not self._stop.is_set():
            started = time.monotonic()
            try:
                async with connect(
                    self.stream_url(),
                    ping_interval=20,
                    ping_timeout=20,
                    open_timeout=15,
                    close_timeout=5,
                    max_queue=4096,
                    max_size=2**22,
                ) as ws:
                    self._ws = ws
                    self.ws_connected = True
                    self.connected_since_ms = self.clock.now_ms()
                    if first:
                        self.storage.event("INFO", "ws_connected", "market data stream connected")
                    else:
                        self.reconnects += 1
                        self.storage.event("INFO", "ws_reconnected", f"market data stream reconnected (#{self.reconnects})")
                    first = False
                    self._wake.set()  # check for a gap right away
                    while not self._stop.is_set():
                        if time.monotonic() - started > ROTATE_AFTER_SEC:
                            self.storage.event("INFO", "ws_rotate", "proactive reconnect before the 24h limit")
                            break
                        try:
                            raw = await asyncio.wait_for(ws.recv(), timeout=STALL_SEC)
                        except asyncio.TimeoutError:
                            self.storage.event("WARNING", "ws_stalled", f"no market data for {STALL_SEC:.0f}s, reconnecting")
                            break
                        self.last_msg_ms = self.clock.now_ms()
                        if self._handle(raw) == "shutdown":
                            self.storage.event("INFO", "ws_server_shutdown", "server announced shutdown, reconnecting")
                            break
            except asyncio.CancelledError:
                raise
            except Exception as exc:  # network errors, handshake failures, closed connections
                now = self.clock.now_ms()
                if not self._stop.is_set() and (self.ws_connected or now - self._ws_fail_logged_ms >= 300_000):
                    # one WARNING per disconnect, then at most one per 5 minutes while reconnects fail
                    self._ws_fail_logged_ms = now
                    self.storage.event("WARNING", "ws_disconnect", f"market data stream error: {_short(exc)}")
            finally:
                self.ws_connected = False
                self._ws = None
            if self._stop.is_set():
                break
            if time.monotonic() - started > 60:
                backoff = 1.0
            await asyncio.sleep(backoff + random.random() * 0.5)
            backoff = min(backoff * 2, 60.0)

    def _handle(self, raw: str | bytes) -> str | None:
        try:
            msg = json.loads(raw)
        except (ValueError, TypeError):
            return None
        data = msg.get("data", msg) if isinstance(msg, dict) else None
        if not isinstance(data, dict):
            return None
        e = data.get("e")
        if e == "kline":
            k = data["k"]
            sym = data.get("s") or k.get("s")
            if sym not in SYMBOLS:
                return None
            self.last_price[sym] = float(k["c"])
            if k.get("x"):
                self._pending.setdefault(int(k["t"]), {})[sym] = Kline.from_ws(k)
                self._wake.set()
        elif e == "serverShutdown":
            return "shutdown"
        elif "b" in data and "a" in data and "s" in data:
            sym = data["s"]
            if sym in SYMBOLS:
                try:
                    self.book[sym] = BookTop(float(data["b"]), float(data["a"]), self.clock.now_ms())
                except (TypeError, ValueError):
                    pass
        return None

    async def _watchdog(self) -> None:
        while not self._stop.is_set():
            await asyncio.sleep(5)
            self._wake.set()

    async def _emitter(self) -> None:
        while not self._stop.is_set():
            try:
                await asyncio.wait_for(self._wake.wait(), timeout=5)
            except asyncio.TimeoutError:
                pass
            self._wake.clear()
            if self._stop.is_set():
                break
            try:
                await self.process_pending()
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                log.exception("bar emission failed")
                self.storage.event("ERROR", "bar_emit_failed", f"bar emission failed: {exc!r}")
                await asyncio.sleep(5)

    async def process_pending(self) -> None:
        if self.last_emitted is None:
            return
        await self._emit_contiguous()
        now = self.clock.now_ms()
        nxt = self.last_emitted + MINUTE_MS
        latest_closed = floor_minute(now) - MINUTE_MS
        if nxt <= latest_closed:
            later = any(t > nxt for t in self._pending)
            overdue = now >= nxt + MINUTE_MS + GRACE_MS
            if later or overdue:
                await self.backfill(latest_closed)
        for t in [t for t in self._pending if t <= (self.last_emitted or 0)]:
            del self._pending[t]

    async def _emit_contiguous(self) -> None:
        while self.last_emitted is not None:
            nxt = self.last_emitted + MINUTE_MS
            entry = self._pending.get(nxt)
            if not entry or any(s not in entry for s in SYMBOLS):
                return
            del self._pending[nxt]
            await self._deliver(JointBar(nxt, entry[THETA], entry[TFUEL]))

    async def backfill(self, to_ms: int) -> int:
        """Fill the hole after the last emitted bar from REST klines. Returns bars emitted."""
        assert self.last_emitted is not None
        start = self.last_emitted + MINUTE_MS
        if to_ms < start:
            return 0
        if to_ms - start > MAX_RUNTIME_BACKFILL_MS:
            start = to_ms - MAX_RUNTIME_BACKFILL_MS
        try:
            now = self.clock.now_ms()
            rows: dict[str, dict[int, Kline]] = {}
            for sym in SYMBOLS:
                raw = await self.client.klines_range(sym, "1m", start, to_ms)
                rows[sym] = {int(r[0]): Kline.from_rest(r) for r in raw if int(r[0]) + MINUTE_MS <= now}
                for t, entry in self._pending.items():  # WS bars fill anything REST does not have yet
                    if start <= t <= to_ms and sym in entry:
                        rows[sym].setdefault(t, entry[sym])
        except Exception as exc:
            now = self.clock.now_ms()
            self._backfill_failures += 1
            if now - self._backfill_fail_logged_ms >= 300_000:  # at most one WARNING per 5 minutes
                self._backfill_fail_logged_ms = now
                self.storage.event(
                    "WARNING", "backfill_failed",
                    f"REST backfill failed ({self._backfill_failures} attempt(s) so far), retrying: {exc!r}",
                )
            return 0
        if self._backfill_failures:
            self.storage.event("INFO", "backfill_recovered", f"REST backfill working again after {self._backfill_failures} failure(s)")
            self._backfill_failures = 0
            self._backfill_fail_logged_ms = 0
        bars = join_klines(rows, start, to_ms, self.last_close, now - SYNTH_AFTER_MS)
        n = 0
        for bar in bars:
            if self.last_emitted is not None and bar.ts_ms <= self.last_emitted:
                continue
            self._pending.pop(bar.ts_ms, None)
            await self._deliver(bar)
            n += 1
        if n > 1:
            self.storage.event(
                "INFO",
                "backfill",
                f"backfilled {n} bars {ms_to_iso(bars[0].ts_ms)} .. {ms_to_iso(bars[-1].ts_ms)}",
                bars=n,
            )
        return n

    async def _deliver(self, bar: JointBar) -> None:
        now = self.clock.now_ms()
        phase = "live" if now - bar.close_ms <= LIVE_WINDOW_MS else "backfill"
        if phase == "live":
            bar.book = self.book_snapshot()
        self.last_emitted = bar.ts_ms
        self.last_close[THETA] = bar.theta.c
        self.last_close[TFUEL] = bar.tfuel.c
        await self.on_bar(bar, phase)
        self.last_bar_processed_ms = now
