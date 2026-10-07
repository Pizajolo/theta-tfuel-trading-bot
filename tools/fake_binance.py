"""Local fake Binance Spot server (REST + combined WebSocket stream) for end-to-end runs.

NOT a production component. It lets you run the whole bot - paper and live mode - without
network access to Binance:

    FAKE_ACCOUNTS="k1:s1,k4:s4" python -m tools.fake_binance --port 8090

and in .env:

    BINANCE_REST_URL=http://127.0.0.1:8090
    BINANCE_WS_URL=ws://127.0.0.1:8090
    S1K_API_KEY=k1  S1K_API_SECRET=s1   (each key is its own sub-account, 500 USD THETA + 500 USD TFUEL)

Admin endpoints (POST) to exercise failure handling:

    /admin/outage?seconds=300       drop WebSockets and fail REST with 503 for N seconds
    /admin/disconnect               drop all WebSocket connections (like the 24h disconnect)
    /admin/shock?pct=8              THETA +8% with TFUEL lagging (ratio dislocation) -> overlay entry
    /admin/partial_fill?ratio=0.5   fill only 50% of each IOC order
    /admin/fail_next?symbol=THETAUSDT&n=1    reject the next n orders on a symbol
    /admin/lose_response?symbol=TFUELUSDT&n=1  place the next order but answer 503
    /admin/withdrawals?enabled=true API key permission flag
    GET /admin/state                balances and orders per account
"""

from __future__ import annotations

import argparse
import asyncio
import contextlib
import hashlib
import hmac
import json
import math
import os
import random
import time
from typing import Any
from urllib.parse import parse_qsl

import uvicorn
from fastapi import FastAPI, Request, WebSocket, WebSocketDisconnect
from fastapi.responses import JSONResponse

from bot.binance_client import BinanceAPIError, UnknownOrderStatus
from tools.fake_exchange import FakeExchange
from tools.synthetic import RatioModel

M = 60_000
DAY = 86_400_000
SYMS = ("THETAUSDT", "TFUELUSDT")


class Market:
    """Shared synthetic prices with minute klines (history + live)."""

    def __init__(self, history_days: float = 14, daily_days: int = 400, seed: int = 11) -> None:
        self.model = RatioModel(seed=seed, theta0=1.2, ratio0=22.0)
        self.prices: dict[str, float] = {"THETAUSDT": 1.2, "TFUELUSDT": 1.2 / 22.0, "BNBUSDT": 600.0}
        self.minutes: dict[str, dict[int, list]] = {s: {} for s in SYMS}
        self.daily: dict[str, dict[int, list]] = {s: {} for s in SYMS}
        now = int(time.time() * 1000)
        cur = now - now % M
        start = cur - int(history_days * DAY)
        start -= start % M
        rng = random.Random(seed)
        prev = {}
        for t in range(start, cur, M):
            th, tf = self.model.step()
            for s, p in (("THETAUSDT", th), ("TFUELUSDT", tf)):
                o = prev.get(s, p)
                self.minutes[s][t] = self._row(t, o, max(o, p) * 1.0004, min(o, p) * 0.9996, p, rng.uniform(1e3, 5e4))
                prev[s] = p
        self.prices["THETAUSDT"], self.prices["TFUELUSDT"] = prev["THETAUSDT"], prev["TFUELUSDT"]
        # coarse daily history before the minute history, ending at its first close
        first = {s: float(self.minutes[s][start][1]) for s in SYMS}
        d0 = start - start % DAY
        lr = math.log(first["THETAUSDT"] / first["TFUELUSDT"])
        th = first["THETAUSDT"]
        for i in range(1, daily_days):
            t = d0 - i * DAY
            th *= math.exp(rng.gauss(0, 0.03))
            lr += rng.gauss(0, 0.02)
            for s, p in (("THETAUSDT", th), ("TFUELUSDT", th / math.exp(lr))):
                self.daily[s][t] = self._row(t, p, p, p, p, 1e6, DAY)
        self.cur_open = cur
        self.cur: dict[str, list] = {s: self._row(cur, self.prices[s], self.prices[s], self.prices[s], self.prices[s], 0.0)
                                     for s in SYMS}
        self.rng = rng

    @staticmethod
    def _row(t: int, o: float, h: float, l: float, c: float, v: float, span: int = M) -> list:
        return [t, f"{o:.8f}", f"{h:.8f}", f"{l:.8f}", f"{c:.8f}", f"{v:.2f}", t + span - 1, "0", 10, "0", "0", "0"]

    def tick(self) -> list[tuple[str, list]]:
        """Advance ~1 second. Returns closed klines (symbol, row) when a minute rolls over."""
        closed = []
        now = int(time.time() * 1000)
        if now - now % M > self.cur_open:
            for s in SYMS:
                row = self.cur[s]
                self.minutes[s][self.cur_open] = row
                closed.append((s, row))
            self.cur_open = now - now % M
            self.cur = {s: self._row(self.cur_open, self.prices[s], self.prices[s], self.prices[s], self.prices[s], 0.0)
                        for s in SYMS}
        # small per-second moves; TFUEL follows THETA through the ratio model each ~minute
        if self.rng.random() < 1 / 60:
            th, tf = self.model.step()
            scale = self.prices["THETAUSDT"] / th
            self.prices["THETAUSDT"], self.prices["TFUELUSDT"] = th * scale, tf * scale
        for s in SYMS:
            self.prices[s] *= math.exp(self.rng.gauss(0, 0.00015))
            p = self.prices[s]
            r = self.cur[s]
            r[2] = f"{max(float(r[2]), p):.8f}"
            r[3] = f"{min(float(r[3]), p):.8f}"
            r[4] = f"{p:.8f}"
            r[5] = f"{float(r[5]) + self.rng.uniform(5, 200):.2f}"
        return closed

    def klines(self, symbol: str, interval: str, start: int | None, end: int | None, limit: int) -> list[list]:
        if interval == "1d":
            data = dict(self.daily[symbol])
            # aggregate minute history into days
            by_day: dict[int, list] = {}
            for t in sorted(self.minutes[symbol]):
                row = self.minutes[symbol][t]
                d = t - t % DAY
                b = by_day.get(d)
                if b is None:
                    by_day[d] = self._row(d, float(row[1]), float(row[2]), float(row[3]), float(row[4]), float(row[5]), DAY)
                else:
                    b[2] = f"{max(float(b[2]), float(row[2])):.8f}"
                    b[3] = f"{min(float(b[3]), float(row[3])):.8f}"
                    b[4] = row[4]
            data.update(by_day)
        else:
            data = dict(self.minutes[symbol])
            data[self.cur_open] = self.cur[symbol]
        ts = sorted(t for t in data if (start is None or t >= start) and (end is None or t <= end))
        if start is None:
            ts = ts[-limit:]
        return [data[t] for t in ts[:limit]]


def create_app(accounts: dict[str, str]) -> FastAPI:
    market = Market(float(os.environ.get("FAKE_HISTORY_DAYS", "14")))
    exchanges: dict[str, FakeExchange] = {}
    for key in accounts:
        fx = FakeExchange(balances={"THETA": 500.0 / market.prices["THETAUSDT"],
                                    "TFUEL": 500.0 / market.prices["TFUELUSDT"], "USDT": 10.0, "BNB": 0.05})
        fx.prices = market.prices  # shared price source
        fx.use_bnb = True
        exchanges[key] = fx
    clients: set[WebSocket] = set()
    ctl: dict[str, Any] = {"outage_until": 0.0}

    @contextlib.asynccontextmanager
    async def lifespan(_app: FastAPI):  # type: ignore[no-untyped-def]
        task = asyncio.get_running_loop().create_task(pump())
        yield
        task.cancel()

    app = FastAPI(lifespan=lifespan)

    def outage() -> bool:
        return time.time() < ctl["outage_until"]

    def err(status: int, code: int | None, msg: str) -> JSONResponse:
        return JSONResponse({"code": code, "msg": msg}, status_code=status, headers={"X-MBX-USED-WEIGHT-1M": "10"})

    def ok(data: Any) -> JSONResponse:
        return JSONResponse(data, headers={"X-MBX-USED-WEIGHT-1M": "10"})

    def signed(request: Request) -> tuple[FakeExchange | None, dict[str, str], JSONResponse | None]:
        key = request.headers.get("x-mbx-apikey", "")
        if key not in accounts:
            return None, {}, err(401, -2015, "Invalid API-key, IP, or permissions for action.")
        q = request.url.query
        body, _, sig = q.rpartition("&signature=")
        want = hmac.new(accounts[key].encode(), body.encode(), hashlib.sha256).hexdigest()
        if not hmac.compare_digest(want, sig.lower()):
            return None, {}, err(400, -1022, "Signature for this request is not valid.")
        params = dict(parse_qsl(body))
        ts = int(params.get("timestamp", "0"))
        if abs(time.time() * 1000 - ts) > int(params.get("recvWindow", "5000")) + 1000:
            return None, {}, err(400, -1021, "Timestamp for this request is outside of the recvWindow.")
        return exchanges[key], params, None

    @app.middleware("http")
    async def outage_mw(request: Request, call_next):  # type: ignore[no-untyped-def]
        if outage() and request.url.path.startswith(("/api", "/sapi")):
            return err(503, None, "simulated outage")
        return await call_next(request)

    @app.get("/api/v3/ping")
    async def ping() -> JSONResponse:
        return ok({})

    @app.get("/api/v3/time")
    async def server_time() -> JSONResponse:
        return ok({"serverTime": int(time.time() * 1000)})

    @app.get("/api/v3/exchangeInfo")
    async def exchange_info(symbols: str = '["THETAUSDT","TFUELUSDT"]') -> JSONResponse:
        return ok(FakeExchange().exchange_info_dict(json.loads(symbols)))

    @app.get("/api/v3/klines")
    async def klines(symbol: str, interval: str, startTime: int | None = None, endTime: int | None = None,
                     limit: int = 500) -> JSONResponse:
        return ok(market.klines(symbol, interval, startTime, endTime, min(limit, 1000)))

    @app.get("/api/v3/ticker/bookTicker")
    async def book_ticker(symbol: str) -> JSONResponse:
        return ok(await FakeExchange(prices=market.prices).book_ticker(symbol))

    @app.get("/api/v3/ticker/price")
    async def ticker_price(symbol: str) -> JSONResponse:
        return ok({"symbol": symbol, "price": f"{market.prices[symbol]:.8f}"})

    async def run_signed(request: Request, fn: str, *args_from: str) -> JSONResponse:
        fx, params, bad = signed(request)
        if bad:
            return bad
        try:
            if fn == "new_order":
                return ok(await fx.new_order(**params))  # type: ignore[union-attr]
            if fn == "my_trades":
                return ok(await fx.my_trades(params["symbol"], int(params["orderId"])))  # type: ignore[union-attr]
            res = await getattr(fx, fn)(*[params.get(a) for a in args_from])
            if fn == "get_order" and res is None:
                return err(400, -2013, "Order does not exist.")
            return ok(res)
        except UnknownOrderStatus:
            return err(503, None, "simulated lost response")
        except BinanceAPIError as exc:
            return err(exc.status or 400, exc.code, exc.msg)

    @app.get("/api/v3/account")
    async def account(request: Request) -> JSONResponse:
        return await run_signed(request, "account")

    @app.get("/sapi/v1/account/apiRestrictions")
    async def restrictions(request: Request) -> JSONResponse:
        return await run_signed(request, "api_restrictions")

    @app.post("/api/v3/order")
    async def new_order(request: Request) -> JSONResponse:
        return await run_signed(request, "new_order")

    @app.get("/api/v3/order")
    async def get_order(request: Request) -> JSONResponse:
        return await run_signed(request, "get_order", "symbol", "origClientOrderId")

    @app.delete("/api/v3/order")
    async def cancel_order(request: Request) -> JSONResponse:
        return await run_signed(request, "cancel_order", "symbol", "origClientOrderId")

    @app.get("/api/v3/openOrders")
    async def open_orders(request: Request) -> JSONResponse:
        return await run_signed(request, "open_orders", "symbol")

    @app.delete("/api/v3/openOrders")
    async def cancel_open(request: Request) -> JSONResponse:
        fx, params, bad = signed(request)
        if bad:
            return bad
        res = await fx.cancel_open_orders(params["symbol"])  # type: ignore[union-attr]
        return ok(res) if res else err(400, -2011, "Unknown order sent.")

    @app.get("/api/v3/myTrades")
    async def my_trades(request: Request) -> JSONResponse:
        return await run_signed(request, "my_trades")

    # ---- websocket ----------------------------------------------------------------------------
    @app.websocket("/stream")
    async def stream(ws: WebSocket) -> None:
        if outage():
            await ws.close(code=1013)
            return
        await ws.accept()
        clients.add(ws)
        try:
            while True:
                await ws.receive_text()
        except WebSocketDisconnect:
            pass
        finally:
            clients.discard(ws)

    async def broadcast(msgs: list[dict]) -> None:
        for ws in list(clients):
            try:
                for m in msgs:
                    await ws.send_text(json.dumps(m))
            except Exception:
                clients.discard(ws)

    def kline_msg(s: str, row: list, closed: bool) -> dict:
        return {"stream": f"{s.lower()}@kline_1m", "data": {"e": "kline", "E": int(time.time() * 1000), "s": s, "k": {
            "t": row[0], "T": row[6], "s": s, "i": "1m", "o": row[1], "c": row[4], "h": row[2], "l": row[3],
            "v": row[5], "n": 10, "x": closed, "q": "0"}}}

    async def pump() -> None:
        n = 0
        while True:
            await asyncio.sleep(0.5)
            n += 1
            closed = market.tick() if n % 2 == 0 else []  # the market keeps moving during an outage
            if outage():
                for ws in list(clients):
                    await ws.close(code=1001)
                    clients.discard(ws)
                continue
            msgs = [kline_msg(s, row, True) for s, row in closed]
            if n % 2 == 0:
                msgs += [kline_msg(s, market.cur[s], False) for s in SYMS]
            fxp = FakeExchange(prices=market.prices)
            for s in SYMS:
                bid, ask = fxp.book(s)
                msgs.append({"stream": f"{s.lower()}@bookTicker",
                             "data": {"u": n, "s": s, "b": f"{bid:.8f}", "B": "1000", "a": f"{ask:.8f}", "A": "1000"}})
            await broadcast(msgs)

    # ---- admin ---------------------------------------------------------------------------------
    @app.post("/admin/outage")
    async def admin_outage(seconds: float = 300) -> dict:
        ctl["outage_until"] = time.time() + seconds
        return {"outage_until": ctl["outage_until"]}

    @app.post("/admin/disconnect")
    async def admin_disconnect() -> dict:
        n = len(clients)
        for ws in list(clients):
            await ws.close(code=1001)
            clients.discard(ws)
        return {"closed": n}

    @app.post("/admin/shock")
    async def admin_shock(pct: float = 8.0) -> dict:
        # THETA jumps, TFUEL lags: a ratio dislocation that then decays over hours
        market.model.fast += math.log(1 + pct / 100)
        market.prices["THETAUSDT"] *= 1 + pct / 100
        return {"prices": market.prices}

    @app.post("/admin/partial_fill")
    async def admin_partial(ratio: float = 0.5) -> dict:
        for fx in exchanges.values():
            fx.partial_fill = ratio
        return {"partial_fill": ratio}

    @app.post("/admin/fail_next")
    async def admin_fail(symbol: str, n: int = 1) -> dict:
        for fx in exchanges.values():
            fx.fail_next[symbol] = [BinanceAPIError(400, -2010, "simulated rejection") for _ in range(n)]
        return {"ok": True}

    @app.post("/admin/lose_response")
    async def admin_lose(symbol: str, n: int = 1) -> dict:
        for fx in exchanges.values():
            fx.lose_response_next[symbol] = n
        return {"ok": True}

    @app.post("/admin/withdrawals")
    async def admin_withdrawals(enabled: bool = True) -> dict:
        for fx in exchanges.values():
            fx.withdrawals_enabled = enabled
        return {"enabled": enabled}

    @app.get("/admin/state")
    async def admin_state() -> dict:
        return {"prices": market.prices, "clients": len(clients), "outage": outage(),
                "accounts": {k: {"free": fx.free, "locked": fx.locked, "orders": len(fx.order_log),
                                 "last_orders": [{k2: o[k2] for k2 in ("clientOrderId", "symbol", "side", "status",
                                                                       "executedQty", "cummulativeQuoteQty")}
                                                 for o in fx.order_log[-10:]]}
                             for k, fx in exchanges.items()}}

    return app


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--host", default="127.0.0.1")
    ap.add_argument("--port", type=int, default=8090)
    a = ap.parse_args()
    accounts = dict(x.split(":", 1) for x in os.environ.get("FAKE_ACCOUNTS", "k1:s1,k4:s4").split(",") if ":" in x)
    uvicorn.run(create_app(accounts), host=a.host, port=a.port, log_level="warning")


if __name__ == "__main__":
    main()
