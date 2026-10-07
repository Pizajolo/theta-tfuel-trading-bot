"""Binance symbol filters (``GET /api/v3/exchangeInfo``) and quantity/price rounding.

Quantities are always rounded *down* to ``LOT_SIZE.stepSize``. Prices are rounded to
``PRICE_FILTER.tickSize`` in the direction that keeps the limit within the allowed slippage:
up for sells, down for buys.
"""

from __future__ import annotations

from dataclasses import dataclass
from decimal import ROUND_CEILING, ROUND_FLOOR, Decimal
from typing import Any


def D(x: Any) -> Decimal:
    return x if isinstance(x, Decimal) else Decimal(str(x))


def round_step_down(x: Any, step: Any) -> Decimal:
    x, step = D(x), D(step)
    if step <= 0:
        return x
    return (x / step).to_integral_value(rounding=ROUND_FLOOR) * step


def round_step_up(x: Any, step: Any) -> Decimal:
    x, step = D(x), D(step)
    if step <= 0:
        return x
    return (x / step).to_integral_value(rounding=ROUND_CEILING) * step


def fmt(x: Decimal) -> str:
    """Plain decimal string without exponent or trailing zeros (as Binance expects)."""
    s = format(x.normalize(), "f")
    return s if s not in ("-0", "") else "0"


@dataclass(frozen=True)
class SymbolFilters:
    symbol: str
    base_asset: str
    quote_asset: str
    status: str = "TRADING"
    tick_size: Decimal = Decimal("0")
    min_price: Decimal = Decimal("0")
    max_price: Decimal = Decimal("0")
    step_size: Decimal = Decimal("0")
    min_qty: Decimal = Decimal("0")
    max_qty: Decimal = Decimal("0")
    min_notional: Decimal = Decimal("0")
    max_notional: Decimal = Decimal("0")  # 0 = unlimited

    @classmethod
    def from_exchange_info(cls, info: dict[str, Any]) -> "SymbolFilters":
        kw: dict[str, Any] = {
            "symbol": info["symbol"],
            "base_asset": info.get("baseAsset", ""),
            "quote_asset": info.get("quoteAsset", ""),
            "status": info.get("status", "TRADING"),
        }
        for f in info.get("filters", []):
            t = f.get("filterType")
            if t == "PRICE_FILTER":
                kw.update(tick_size=D(f["tickSize"]), min_price=D(f["minPrice"]), max_price=D(f["maxPrice"]))
            elif t == "LOT_SIZE":
                kw.update(step_size=D(f["stepSize"]), min_qty=D(f["minQty"]), max_qty=D(f["maxQty"]))
            elif t == "NOTIONAL":
                kw["min_notional"] = max(kw.get("min_notional", Decimal(0)), D(f.get("minNotional", "0")))
                kw["max_notional"] = D(f.get("maxNotional", "0"))
            elif t == "MIN_NOTIONAL":
                kw["min_notional"] = max(kw.get("min_notional", Decimal(0)), D(f.get("minNotional", "0")))
        return cls(**kw)

    def qty_down(self, qty: Any) -> Decimal:
        q = round_step_down(qty, self.step_size)
        if self.max_qty > 0 and q > self.max_qty:
            q = round_step_down(self.max_qty, self.step_size)
        return max(q, Decimal(0))

    def price_for(self, price: Any, side: str) -> Decimal:
        """Round a limit price to the tick: sells round up, buys round down (never beyond the bound)."""
        p = round_step_up(price, self.tick_size) if side == "SELL" else round_step_down(price, self.tick_size)
        if self.min_price > 0 and p < self.min_price:
            p = self.min_price
        if self.max_price > 0 and p > self.max_price:
            p = self.max_price
        return p

    def check(self, qty: Decimal, price: Decimal) -> str | None:
        """Return a reason string if the order would violate a filter, else None."""
        if self.status != "TRADING":
            return f"{self.symbol} status is {self.status}"
        if qty <= 0:
            return "quantity rounds to zero"
        if self.min_qty > 0 and qty < self.min_qty:
            return f"qty {qty} < minQty {self.min_qty}"
        if self.step_size > 0 and (qty % self.step_size) != 0:
            return f"qty {qty} not a multiple of stepSize {self.step_size}"
        if self.tick_size > 0 and (price % self.tick_size) != 0:
            return f"price {price} not a multiple of tickSize {self.tick_size}"
        notional = qty * price
        if self.min_notional > 0 and notional < self.min_notional:
            return f"notional {notional} < minNotional {self.min_notional}"
        if self.max_notional > 0 and notional > self.max_notional:
            return f"notional {notional} > maxNotional {self.max_notional}"
        return None

    def min_notional_f(self) -> float:
        return float(self.min_notional)


def parse_exchange_info(info: dict[str, Any], symbols: tuple[str, ...] | list[str]) -> dict[str, SymbolFilters]:
    out = {}
    for s in info.get("symbols", []):
        if s.get("symbol") in symbols:
            out[s["symbol"]] = SymbolFilters.from_exchange_info(s)
    missing = set(symbols) - set(out)
    if missing:
        raise ValueError(f"exchangeInfo missing symbols: {sorted(missing)}")
    return out
