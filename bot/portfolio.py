"""Portfolios (paper variants and the live account view) and HODL benchmark maths.

* ``w = V_THETA / (V_THETA + V_TFUEL)`` at mid prices. USDT and BNB are excluded from ``w``
  but included in the total value.
* ``excess = V_strategy / V_hodl - 1`` where ``V_hodl`` values the token amounts held at the
  benchmark start at *current* prices.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from bot.util import DAY_MS, ms_to_iso, year_of

ASSETS = ("THETA", "TFUEL", "USDT", "BNB")
PAIR = ("THETA", "TFUEL")
SYMBOL_OF = {"THETA": "THETAUSDT", "TFUEL": "TFUELUSDT", "BNB": "BNBUSDT"}
ASSET_OF = {v: k for k, v in SYMBOL_OF.items()}


def prices(theta: float, tfuel: float, bnb: float | None = None) -> dict[str, float]:
    return {"THETA": theta, "TFUEL": tfuel, "USDT": 1.0, "BNB": bnb or 0.0}


def value_of(bal: dict[str, float], px: dict[str, float]) -> float:
    return sum(bal.get(a, 0.0) * px.get(a, 0.0) for a in ASSETS)


def pair_value(bal: dict[str, float], px: dict[str, float]) -> float:
    return bal.get("THETA", 0.0) * px["THETA"] + bal.get("TFUEL", 0.0) * px["TFUEL"]


def theta_weight(bal: dict[str, float], px: dict[str, float]) -> float:
    vp = pair_value(bal, px)
    return bal.get("THETA", 0.0) * px["THETA"] / vp if vp > 0 else 0.0


def trade_size(w_target: float, bal: dict[str, float], px: dict[str, float]) -> float:
    """``dv = (w_target - w) * V_pair`` in USD. Positive: buy THETA (sell TFUEL)."""
    return (w_target - theta_weight(bal, px)) * pair_value(bal, px)


@dataclass
class Portfolio:
    variant: str  # mid | touch | worst | live
    bal: dict[str, float]
    hodl: dict[str, float]
    benchmark_start_ms: int
    fees_usd: float = 0.0
    fills: int = 0
    slippage_bps_sum: float = 0.0
    turnover_usd: float = 0.0
    rebalances_total: int = 0
    rebalance_ts: list[int] = field(default_factory=list)  # last 30 days only
    ytd_year: int | None = None
    ytd_ratio: float | None = None  # (V/V_hodl) at the start of ``ytd_year``

    # ---- construction --------------------------------------------------------------------
    @classmethod
    def fifty_fifty(cls, variant: str, capital_usd: float, px: dict[str, float], ts_ms: int) -> "Portfolio":
        bal = {"THETA": capital_usd * 0.5 / px["THETA"], "TFUEL": capital_usd * 0.5 / px["TFUEL"], "USDT": 0.0, "BNB": 0.0}
        return cls(variant=variant, bal=dict(bal), hodl=dict(bal), benchmark_start_ms=ts_ms)

    @classmethod
    def from_balances(cls, variant: str, bal: dict[str, float], ts_ms: int) -> "Portfolio":
        b = {a: float(bal.get(a, 0.0)) for a in ASSETS}
        return cls(variant=variant, bal=dict(b), hodl=dict(b), benchmark_start_ms=ts_ms)

    def to_dict(self) -> dict[str, Any]:
        return {
            "variant": self.variant,
            "bal": self.bal,
            "hodl": self.hodl,
            "benchmark_start_ms": self.benchmark_start_ms,
            "fees_usd": self.fees_usd,
            "fills": self.fills,
            "slippage_bps_sum": self.slippage_bps_sum,
            "turnover_usd": self.turnover_usd,
            "rebalances_total": self.rebalances_total,
            "rebalance_ts": self.rebalance_ts,
            "ytd_year": self.ytd_year,
            "ytd_ratio": self.ytd_ratio,
        }

    @classmethod
    def from_dict(cls, d: dict[str, Any]) -> "Portfolio":
        return cls(**d)

    # ---- valuation ---------------------------------------------------------------------
    def value(self, px: dict[str, float]) -> float:
        return value_of(self.bal, px)

    def hodl_value(self, px: dict[str, float]) -> float:
        return value_of(self.hodl, px)

    def pair_value(self, px: dict[str, float]) -> float:
        return pair_value(self.bal, px)

    def w(self, px: dict[str, float]) -> float:
        return theta_weight(self.bal, px)

    def excess(self, px: dict[str, float]) -> float:
        hv = self.hodl_value(px)
        return self.value(px) / hv - 1.0 if hv > 0 else 0.0

    def ytd_excess(self, px: dict[str, float], now_ms: int) -> float:
        """Excess since the start of the current UTC year (or since the benchmark start)."""
        ratio = 1.0 + self.excess(px)
        y = year_of(now_ms)
        if self.ytd_year != y:
            # First valuation in a new year: anchor the YTD baseline. When the benchmark itself
            # started this year the baseline is 1.0 (excess is already "year to date").
            self.ytd_year = y
            self.ytd_ratio = 1.0 if year_of(self.benchmark_start_ms) == y else ratio
        base = self.ytd_ratio or 1.0
        return ratio / base - 1.0

    def avg_slippage_bps(self) -> float | None:
        return self.slippage_bps_sum / self.fills if self.fills else None

    def trades_30d(self, now_ms: int) -> int:
        cutoff = now_ms - 30 * DAY_MS
        self.rebalance_ts = [t for t in self.rebalance_ts if t >= cutoff]
        return len(self.rebalance_ts)

    def note_rebalance(self, ts_ms: int) -> None:
        self.rebalances_total += 1
        self.rebalance_ts.append(ts_ms)
        self.trades_30d(ts_ms)

    def note_fill(self, notional_usd: float, fee_usd: float, slippage_bps: float | None) -> None:
        self.fills += 1
        self.fees_usd += fee_usd
        self.turnover_usd += notional_usd
        if slippage_bps is not None:
            self.slippage_bps_sum += slippage_bps

    def equity_fields(self, px: dict[str, float], w_target: float | None = None) -> dict[str, Any]:
        v = self.value(px)
        hv = self.hodl_value(px)
        ex = v / hv - 1.0 if hv > 0 else 0.0
        out = {
            "variant": self.variant,
            "bal": {a: self.bal.get(a, 0.0) for a in ASSETS},
            "mid": {"THETA": px["THETA"], "TFUEL": px["TFUEL"]},
            "value_usd": v,
            "hodl_value_usd": hv,
            "excess": ex,
            "theta_equiv_tokens": self.hodl.get("THETA", 0.0) * (1.0 + ex),
            "tfuel_equiv_tokens": self.hodl.get("TFUEL", 0.0) * (1.0 + ex),
            "w": self.w(px),
        }
        if px.get("BNB"):
            out["mid"]["BNB"] = px["BNB"]
        if w_target is not None:
            out["w_target"] = w_target
        return out

    def summary(self, px: dict[str, float], now_ms: int) -> dict[str, Any]:
        f = self.equity_fields(px)
        f.pop("variant")
        f["hodl"] = dict(self.hodl)
        f["benchmark_start"] = ms_to_iso(self.benchmark_start_ms)
        f["excess_ytd"] = self.ytd_excess(px, now_ms)
        f["fees_usd"] = self.fees_usd
        f["fills"] = self.fills
        f["avg_slippage_bps"] = self.avg_slippage_bps()
        f["turnover_usd"] = self.turnover_usd
        f["trades_total"] = self.rebalances_total
        f["trades_30d"] = self.trades_30d(now_ms)
        return f
