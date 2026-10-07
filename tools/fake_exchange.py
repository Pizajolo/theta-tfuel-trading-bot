"""In-process fake of the Binance Spot endpoints the bot uses (tests and local end-to-end runs).

It mirrors :class:`bot.binance_client.BinanceClient`'s async methods, keeps balances, matches
LIMIT IOC orders against a synthetic top of book and can inject failures:

* ``partial_fill``: fraction of each order that fills (IOC remainder expires)
* ``fail_next``: {symbol: [error, ...]} rejects the next order(s) on that symbol
* ``lose_response_next``: {symbol: n} places the order but raises ``UnknownOrderStatus``
* ``withdrawals_enabled`` / ``can_trade``: API key permission flags
"""

from __future__ import annotations

import itertools
import math
import time
from decimal import Decimal
from typing import Any

from bot.binance_client import BinanceAPIError, UnknownOrderStatus

DEFAULT_FILTERS = {
    "THETAUSDT": {"tick": "0.0001", "step": "0.1", "min_qty": "0.1", "min_notional": "5"},
    "TFUELUSDT": {"tick": "0.00001", "step": "1", "min_qty": "1", "min_notional": "5"},
    "BNBUSDT": {"tick": "0.01", "step": "0.001", "min_qty": "0.001", "min_notional": "5"},
}


def _dec_str(x: float) -> str:
    return format(Decimal(repr(x)).normalize(), "f")


class FakeExchange:
    def __init__(
        self,
        prices: dict[str, float] | None = None,
        balances: dict[str, float] | None = None,
        spread_bps: float = 10.0,
        fee_rate: float = 0.001,
        bnb_fee_rate: float = 0.00075,
    ) -> None:
        self.prices = dict(prices or {"THETAUSDT": 1.0, "TFUELUSDT": 0.05, "BNBUSDT": 600.0})
        self.free = {"THETA": 0.0, "TFUEL": 0.0, "USDT": 0.0, "BNB": 0.0}
        self.free.update(balances or {})
        self.locked = {a: 0.0 for a in self.free}
        self.spread_bps = spread_bps
        self.fee_rate = fee_rate
        self.bnb_fee_rate = bnb_fee_rate
        self.use_bnb = False
        self.partial_fill = 1.0
        self.fail_next: dict[str, list[BinanceAPIError]] = {}
        self.lose_response_next: dict[str, int] = {}
        self.withdrawals_enabled = False
        self.can_trade = True
        self.ip_restrict = True
        self.orders: dict[str, dict[str, Any]] = {}  # by clientOrderId (latest)
        self.order_log: list[dict[str, Any]] = []
        self.trades: list[dict[str, Any]] = []
        self._ids = itertools.count(1000)
        self._trade_ids = itertools.count(1)
        self.calls: list[str] = []
        self.klines_data: dict[str, dict[int, list]] = {}
        self.time_offset_ms = 0
        self.base_url = "fake://exchange"

    # ---- helpers ----------------------------------------------------------------------------
    def book(self, symbol: str) -> tuple[float, float]:
        p = self.prices[symbol]
        half = p * self.spread_bps / 2e4
        f = DEFAULT_FILTERS[symbol]
        tick = float(f["tick"])
        bid = math.floor((p - half) / tick) * tick
        ask = math.ceil((p + half) / tick) * tick
        if ask <= bid:
            ask = bid + tick
        return round(bid, 10), round(ask, 10)

    @staticmethod
    def split(symbol: str) -> tuple[str, str]:
        return symbol[:-4], "USDT"

    def total(self, asset: str) -> float:
        return self.free.get(asset, 0.0) + self.locked.get(asset, 0.0)

    # ---- public ---------------------------------------------------------------------------------
    async def close(self) -> None:
        pass

    async def sync_time(self) -> int:
        return 0

    async def server_time(self) -> int:
        return int(time.time() * 1000)

    async def ping(self) -> dict:
        return {}

    def exchange_info_dict(self, symbols: list[str] | tuple[str, ...]) -> dict:
        out = []
        for s in symbols:
            f = DEFAULT_FILTERS[s]
            base, quote = self.split(s)
            out.append({
                "symbol": s, "status": "TRADING", "baseAsset": base, "quoteAsset": quote,
                "filters": [
                    {"filterType": "PRICE_FILTER", "minPrice": f["tick"], "maxPrice": "100000", "tickSize": f["tick"]},
                    {"filterType": "LOT_SIZE", "minQty": f["min_qty"], "maxQty": "90000000", "stepSize": f["step"]},
                    {"filterType": "NOTIONAL", "minNotional": f["min_notional"], "applyMinToMarket": True,
                     "maxNotional": "9000000", "applyMaxToMarket": False, "avgPriceMins": 5},
                ],
            })
        return {"timezone": "UTC", "serverTime": int(time.time() * 1000),
                "rateLimits": [{"rateLimitType": "REQUEST_WEIGHT", "interval": "MINUTE", "intervalNum": 1, "limit": 6000}],
                "symbols": out}

    async def exchange_info(self, symbols: list[str] | tuple[str, ...]) -> dict:
        return self.exchange_info_dict(symbols)

    async def book_ticker(self, symbol: str) -> dict:
        self.calls.append(f"book_ticker {symbol}")
        bid, ask = self.book(symbol)
        return {"symbol": symbol, "bidPrice": _dec_str(bid), "bidQty": "100000", "askPrice": _dec_str(ask), "askQty": "100000"}

    async def ticker_price(self, symbol: str) -> float:
        return self.prices[symbol]

    async def klines(self, symbol: str, interval: str, start_ms: int | None = None, end_ms: int | None = None,
                     limit: int = 1000) -> list[list]:
        data = self.klines_data.get(f"{symbol}:{interval}", {})
        ts = sorted(t for t in data if (start_ms is None or t >= start_ms) and (end_ms is None or t <= end_ms))
        return [data[t] for t in ts[:limit]]

    async def klines_range(self, symbol: str, interval: str, start_ms: int, end_ms: int) -> list[list]:
        self.calls.append(f"klines_range {symbol} {interval} {start_ms} {end_ms}")
        return await self.klines(symbol, interval, start_ms, end_ms, 10**9)

    # ---- signed -------------------------------------------------------------------------------
    async def account(self) -> dict:
        self.calls.append("account")
        return {
            "canTrade": self.can_trade, "canWithdraw": True, "canDeposit": True, "accountType": "SPOT",
            "balances": [{"asset": a, "free": _dec_str(self.free[a]), "locked": _dec_str(self.locked.get(a, 0.0))}
                         for a in self.free],
            "permissions": ["SPOT"],
        }

    async def api_restrictions(self) -> dict:
        return {"ipRestrict": self.ip_restrict, "enableReading": True, "enableWithdrawals": self.withdrawals_enabled,
                "enableSpotAndMarginTrading": True, "enableInternalTransfer": False}

    def _check_filters(self, symbol: str, qty: Decimal, price: Decimal) -> None:
        f = DEFAULT_FILTERS[symbol]
        if qty % Decimal(f["step"]) != 0 or qty < Decimal(f["min_qty"]):
            raise BinanceAPIError(400, -1013, "Filter failure: LOT_SIZE")
        if price % Decimal(f["tick"]) != 0:
            raise BinanceAPIError(400, -1013, "Filter failure: PRICE_FILTER")
        if qty * price < Decimal(f["min_notional"]):
            raise BinanceAPIError(400, -1013, "Filter failure: NOTIONAL")

    async def new_order(self, **p: Any) -> dict:
        self.calls.append(f"new_order {p.get('symbol')} {p.get('side')} {p.get('newClientOrderId')}")
        symbol, side = p["symbol"], p["side"]
        cid = p.get("newClientOrderId") or f"auto{next(self._ids)}"
        if self.fail_next.get(symbol):
            raise self.fail_next[symbol].pop(0)
        if not self.can_trade:
            raise BinanceAPIError(400, -2010, "This account may not place or cancel orders.")
        existing = self.orders.get(cid)
        if existing and existing["status"] in ("NEW", "PARTIALLY_FILLED"):
            raise BinanceAPIError(400, -2010, "Duplicate order sent.")
        qty, price = Decimal(p["quantity"]), Decimal(p["price"])
        self._check_filters(symbol, qty, price)
        base, quote = self.split(symbol)
        q, pr = float(qty), float(price)
        if side == "SELL" and self.free[base] + 1e-12 < q:
            raise BinanceAPIError(400, -2010, "Account has insufficient balance for requested action.")
        if side == "BUY" and self.free[quote] + 1e-9 < q * pr:
            raise BinanceAPIError(400, -2010, "Account has insufficient balance for requested action.")
        bid, ask = self.book(symbol)
        fills = []
        executed = 0.0
        if p.get("timeInForce") == "GTC":  # resting order (used to test cancellation)
            status = "NEW"
            if side == "SELL":
                self.free[base] -= q
                self.locked[base] += q
            else:
                self.free[quote] -= q * pr
                self.locked[quote] += q * pr
        else:
            marketable = (side == "SELL" and pr <= bid) or (side == "BUY" and pr >= ask)
            if marketable and self.partial_fill > 0:
                step = float(DEFAULT_FILTERS[symbol]["step"])
                executed = math.floor(q * self.partial_fill / step + 1e-9) * step
                executed = round(executed, 8)
            fill_px = bid if side == "SELL" else ask
            if executed > 0:
                notional = executed * fill_px
                if self.use_bnb and self.free.get("BNB", 0.0) > 0:
                    comm_asset, comm = "BNB", notional * self.bnb_fee_rate / self.prices["BNBUSDT"]
                elif side == "SELL":
                    comm_asset, comm = quote, notional * self.fee_rate
                else:
                    comm_asset, comm = base, executed * self.fee_rate
                if side == "SELL":
                    self.free[base] -= executed
                    self.free[quote] += notional
                else:
                    self.free[quote] -= notional
                    self.free[base] += executed
                self.free[comm_asset] -= comm
                tid = next(self._trade_ids)
                fills.append({"price": _dec_str(fill_px), "qty": _dec_str(executed), "commission": _dec_str(comm),
                              "commissionAsset": comm_asset, "tradeId": tid})
            status = "FILLED" if executed >= q - 1e-12 else "EXPIRED"
        order = {
            "symbol": symbol, "orderId": next(self._ids), "orderListId": -1, "clientOrderId": cid,
            "transactTime": int(time.time() * 1000), "price": _dec_str(pr), "origQty": _dec_str(q),
            "executedQty": _dec_str(executed),
            "cummulativeQuoteQty": _dec_str(sum(float(f["price"]) * float(f["qty"]) for f in fills)),
            "status": status, "timeInForce": p.get("timeInForce"), "type": p.get("type"), "side": side,
            "fills": fills,
        }
        self.orders[cid] = order
        self.order_log.append(order)
        for f in fills:
            self.trades.append({"symbol": symbol, "id": f["tradeId"], "orderId": order["orderId"], "price": f["price"],
                                "qty": f["qty"], "commission": f["commission"], "commissionAsset": f["commissionAsset"]})
        if self.lose_response_next.get(symbol, 0) > 0:
            self.lose_response_next[symbol] -= 1
            raise UnknownOrderStatus("simulated lost response")
        return dict(order)

    async def get_order(self, symbol: str, orig_client_order_id: str) -> dict | None:
        self.calls.append(f"get_order {symbol} {orig_client_order_id}")
        o = self.orders.get(orig_client_order_id)
        if o is None or o["symbol"] != symbol:
            return None
        return {k: v for k, v in o.items() if k != "fills"}

    async def open_orders(self, symbol: str) -> list[dict]:
        return [dict(o) for o in self.orders.values() if o["symbol"] == symbol and o["status"] in ("NEW", "PARTIALLY_FILLED")]

    def _cancel(self, o: dict) -> None:
        base, quote = self.split(o["symbol"])
        q, pr = float(o["origQty"]) - float(o["executedQty"]), float(o["price"])
        if o["side"] == "SELL":
            self.locked[base] -= q
            self.free[base] += q
        else:
            self.locked[quote] -= q * pr
            self.free[quote] += q * pr
        o["status"] = "CANCELED"

    async def cancel_order(self, symbol: str, orig_client_order_id: str) -> dict:
        self.calls.append(f"cancel_order {symbol} {orig_client_order_id}")
        o = self.orders.get(orig_client_order_id)
        if o is None or o["status"] not in ("NEW", "PARTIALLY_FILLED"):
            raise BinanceAPIError(400, -2011, "Unknown order sent.")
        self._cancel(o)
        return dict(o)

    async def cancel_open_orders(self, symbol: str) -> list[dict]:
        self.calls.append(f"cancel_open_orders {symbol}")
        out = []
        for o in self.orders.values():
            if o["symbol"] == symbol and o["status"] in ("NEW", "PARTIALLY_FILLED"):
                self._cancel(o)
                out.append(dict(o))
        return out

    async def my_trades(self, symbol: str, order_id: int) -> list[dict]:
        return [t for t in self.trades if t["symbol"] == symbol and t["orderId"] == order_id]
