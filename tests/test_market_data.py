import asyncio
import json

from bot.market_data import SYNTH_AFTER_MS, Kline, MarketData, join_klines
from bot.storage import Storage, iter_jsonl
from bot.util import MINUTE_MS, SimClock
from tools.fake_exchange import FakeExchange

from .conftest import T0

M = MINUTE_MS


def rest_row(t, c, v=10.0):
    return [t, str(c), str(c * 1.001), str(c * 0.999), str(c), str(v), t + M - 1, "0", 1, "0", "0", "0"]


def ws_kline(sym, t, c, closed=True, v=10.0):
    return json.dumps({"stream": f"{sym.lower()}@kline_1m", "data": {"e": "kline", "s": sym, "k": {
        "t": t, "T": t + M - 1, "s": sym, "i": "1m", "o": str(c), "c": str(c), "h": str(c), "l": str(c),
        "v": str(v), "x": closed}}})


def ws_book(sym, bid, ask):
    return json.dumps({"stream": f"{sym.lower()}@bookTicker", "data": {"u": 1, "s": sym, "b": str(bid), "B": "1",
                                                                        "a": str(ask), "A": "1"}})


def test_join_klines_synthesises_old_holes_and_stops_at_live_edge():
    rows = {
        "THETAUSDT": {T0: Kline(T0, 1, 1, 1, 1.0, 5), T0 + 2 * M: Kline(T0 + 2 * M, 1, 1, 1, 1.1, 5)},
        "TFUELUSDT": {T0 + i * M: Kline(T0 + i * M, 1, 1, 1, 0.05, 5) for i in range(4)},
    }
    bars = join_klines(rows, T0, T0 + 3 * M, None, synth_until_ms=T0 + 2 * M)
    assert [b.ts_ms for b in bars] == [T0, T0 + M, T0 + 2 * M]  # T0+3M THETA missing and too recent
    assert bars[1].theta.synthetic and bars[1].theta.v == 0 and bars[1].theta.c == 1.0 and bars[1].stale
    assert not bars[2].stale


class Collector:
    def __init__(self):
        self.bars = []

    async def __call__(self, bar, phase):
        self.bars.append((bar.ts_ms, phase, bar.stale))


def make_market(tmp_path, clock, fx):
    col = Collector()
    md = MarketData(fx, "ws://unused", Storage(tmp_path, clock), col, clock)
    return md, col


def test_ws_bars_emitted_in_order_once_both_symbols_closed(tmp_path):
    clock = SimClock(T0 + 2 * M + 1000)  # bar T0+M closed one second ago
    fx = FakeExchange()
    md, col = make_market(tmp_path, clock, fx)
    md.start_from(T0, {"THETAUSDT": 1.0, "TFUELUSDT": 0.05})
    md._handle(ws_book("THETAUSDT", 0.999, 1.001))
    md._handle(ws_book("TFUELUSDT", 0.04999, 0.05001))
    md._handle(ws_kline("THETAUSDT", T0 + M, 1.0, closed=False))  # not closed: ignored
    md._handle(ws_kline("THETAUSDT", T0 + M, 1.0))
    asyncio.run(md.process_pending())
    assert col.bars == []  # TFUEL not closed yet
    md._handle(ws_kline("TFUELUSDT", T0 + M, 0.05))
    clock.set(T0 + 3 * M + 1000)
    md._handle(ws_kline("THETAUSDT", T0 + 2 * M, 1.0))
    md._handle(ws_kline("TFUELUSDT", T0 + 2 * M, 0.05))
    asyncio.run(md.process_pending())
    assert [b[0] for b in col.bars] == [T0 + M, T0 + 2 * M]
    assert all(p == "live" for _, p, _ in col.bars)
    md._handle(ws_kline("THETAUSDT", T0 + 2 * M, 1.0))  # duplicate close event
    md._handle(ws_kline("TFUELUSDT", T0 + 2 * M, 0.05))
    asyncio.run(md.process_pending())
    assert len(col.bars) == 2
    assert md.mid("THETAUSDT") == 1.0


def test_gap_after_disconnect_is_backfilled_from_rest(tmp_path):
    """Network dead for 5 minutes: the missing bars come from REST, in order, exactly once."""
    clock = SimClock(T0)
    fx = FakeExchange()
    for sym, c in (("THETAUSDT", 1.0), ("TFUELUSDT", 0.05)):
        fx.klines_data[f"{sym}:1m"] = {T0 + i * M: rest_row(T0 + i * M, c) for i in range(0, 12)}
    md, col = make_market(tmp_path, clock, fx)
    md.start_from(T0, {"THETAUSDT": 1.0, "TFUELUSDT": 0.05})
    # reconnect at T0+7min: WS delivers the bar of minute 6 first
    clock.set(T0 + 7 * M + 2000)
    md._handle(ws_kline("THETAUSDT", T0 + 6 * M, 1.0))
    md._handle(ws_kline("TFUELUSDT", T0 + 6 * M, 0.05))
    asyncio.run(md.process_pending())
    assert [b[0] for b in col.bars] == [T0 + i * M for i in range(1, 7)]
    assert len({b[0] for b in col.bars}) == len(col.bars)
    events = [e["kind"] for e in iter_jsonl(tmp_path / "events.jsonl")]
    assert "backfill" in events


def test_overdue_bar_triggers_rest_and_synthesises_missing_symbol(tmp_path):
    clock = SimClock(T0)
    fx = FakeExchange()
    fx.klines_data["THETAUSDT:1m"] = {T0 + i * M: rest_row(T0 + i * M, 1.0) for i in range(0, 5)}
    fx.klines_data["TFUELUSDT:1m"] = {T0 + i * M: rest_row(T0 + i * M, 0.05) for i in (0, 1, 3, 4)}  # minute 2: no trades
    md, col = make_market(tmp_path, clock, fx)
    md.start_from(T0, {"THETAUSDT": 1.0, "TFUELUSDT": 0.05})
    clock.set(T0 + 5 * M + SYNTH_AFTER_MS)
    asyncio.run(md.process_pending())
    # minute 5 has no data for either symbol and is older than SYNTH_AFTER_MS -> flat, stale bar;
    # minute 6 is too recent to synthesise, so emission stops there.
    assert [b[0] for b in col.bars] == [T0 + i * M for i in range(1, 6)]
    stale = {t: s for t, _, s in col.bars}
    assert stale[T0 + 2 * M] is True and stale[T0 + 3 * M] is False and stale[T0 + 5 * M] is True
    assert col.bars[0][1] == "backfill" and col.bars[-1][1] == "live"  # only recent bars count as live


def test_rest_failure_logs_warning_and_retries_later(tmp_path):
    clock = SimClock(T0 + 10 * M)
    fx = FakeExchange()

    async def broken(*a, **k):
        raise OSError("network down")

    fx.klines_range = broken
    md, col = make_market(tmp_path, clock, fx)
    md.start_from(T0, {"THETAUSDT": 1.0, "TFUELUSDT": 0.05})
    asyncio.run(md.process_pending())
    assert col.bars == []
    assert any(e["kind"] == "backfill_failed" for e in iter_jsonl(tmp_path / "events.jsonl"))


def test_stream_url_contains_all_streams(tmp_path):
    md, _ = make_market(tmp_path, SimClock(T0), FakeExchange())
    md.ws_url = "wss://stream.binance.com:9443"
    url = md.stream_url()
    assert url.startswith("wss://stream.binance.com:9443/stream?streams=")
    for s in ("thetausdt@kline_1m", "tfuelusdt@kline_1m", "thetausdt@bookTicker", "tfuelusdt@bookTicker"):
        assert s in url
