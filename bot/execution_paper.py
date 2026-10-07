"""Paper execution: every decision is simulated in three fill variants, each with its own
shadow portfolio.

* ``mid``   - fills at the mid price, fee only (optimistic).
* ``touch`` - sells at the bid, buys at the ask at decision time, fee only. **Primary** paper
  portfolio.
* ``worst`` - fills at the worst price of the *next* closed 1m bar (low for sells, high for
  buys), plus fee - the pessimistic backtest assumption.

Trades go through USDT, sell leg first, exactly like live execution: the sell leg's proceeds
(after fee) fund the buy leg. The paper fee is charged in the received asset.
"""

from __future__ import annotations

from typing import Any

from bot.market_data import THETA, TFUEL, JointBar
from bot.portfolio import SYMBOL_OF, Portfolio, prices, trade_size
from bot.storage import Storage

VARIANTS = ("mid", "touch", "worst")


def leg_assets(dv: float) -> tuple[str, str]:
    """(sell_asset, buy_asset): dv > 0 needs more THETA, so sell TFUEL and buy THETA."""
    return ("TFUEL", "THETA") if dv > 0 else ("THETA", "TFUEL")


def slippage_bps(side: str, avg_price: float, mid: float) -> float | None:
    """Cost versus mid in basis points (positive = worse than mid)."""
    if not mid or not avg_price:
        return None
    return (avg_price - mid) / mid * 1e4 if side == "BUY" else (mid - avg_price) / mid * 1e4


def simulate_rebalance(
    port: Portfolio,
    dv: float,
    sell_price: float,
    buy_price: float,
    mid_px: dict[str, float],
    fee_rate: float,
    impact_bps_per_1k: float = 0.0,
) -> list[dict[str, Any]]:
    """Apply a two-leg rebalance to ``port`` in place and return the two fill records."""
    sell_asset, buy_asset = leg_assets(dv)
    impact = impact_bps_per_1k * abs(dv) / 1000.0 / 1e4
    sell_px = sell_price * (1.0 - impact)
    buy_px = buy_price * (1.0 + impact)

    # Leg 1: sell |dv| worth (sized at mid) of the rich asset; never more than held.
    sell_qty = min(abs(dv) / mid_px[sell_asset], port.bal.get(sell_asset, 0.0))
    if sell_qty <= 0 or sell_px <= 0 or buy_px <= 0:
        return []
    gross_usdt = sell_qty * sell_px
    fee1 = gross_usdt * fee_rate
    proceeds = gross_usdt - fee1
    port.bal[sell_asset] -= sell_qty
    port.bal["USDT"] = port.bal.get("USDT", 0.0) + proceeds

    # Leg 2: buy the cheap asset with the USDT actually received.
    gross_qty = proceeds / buy_px
    fee2 = gross_qty * fee_rate
    net_qty = gross_qty - fee2
    port.bal["USDT"] -= proceeds
    if abs(port.bal["USDT"]) < 1e-9:
        port.bal["USDT"] = 0.0
    port.bal[buy_asset] = port.bal.get(buy_asset, 0.0) + net_qty

    s1 = slippage_bps("SELL", sell_px, mid_px[sell_asset])
    s2 = slippage_bps("BUY", buy_px, mid_px[buy_asset])
    port.note_fill(gross_usdt, fee1, s1)
    port.note_fill(proceeds, fee2 * buy_px, s2)
    return [
        {
            "leg": 1,
            "symbol": SYMBOL_OF[sell_asset],
            "side": "SELL",
            "price": sell_px,
            "qty": sell_qty,
            "quote_qty": gross_usdt,
            "filled_qty": sell_qty,
            "avg_price": sell_px,
            "commission": fee1,
            "commission_asset": "USDT",
            "mid_at_decision": mid_px[sell_asset],
            "slippage_bps_vs_mid": s1,
        },
        {
            "leg": 2,
            "symbol": SYMBOL_OF[buy_asset],
            "side": "BUY",
            "price": buy_px,
            "qty": gross_qty,
            "quote_qty": proceeds,
            "filled_qty": gross_qty,
            "avg_price": buy_px,
            "commission": fee2,
            "commission_asset": buy_asset,
            "mid_at_decision": mid_px[buy_asset],
            "slippage_bps_vs_mid": s2,
        },
    ]


class PaperExecutor:
    def __init__(
        self,
        instance: str,
        storage: Storage,
        fee_rate: float,
        min_trade_usd: float,
        impact_bps_per_1k: float = 0.0,
        state: dict[str, Any] | None = None,
    ) -> None:
        self.instance = instance
        self.storage = storage
        self.fee_rate = fee_rate
        self.min_trade_usd = min_trade_usd
        self.impact = impact_bps_per_1k
        self.pending_worst: dict[str, Any] | None = (state or {}).get("pending_worst")

    def to_dict(self) -> dict[str, Any]:
        return {"pending_worst": self.pending_worst}

    def _log(self, ts_ms: int, decision_id: str, variant: str, rec: dict[str, Any], mode: str) -> None:
        leg = rec["leg"]
        self.storage.write_order(
            self.instance,
            ts_ms,
            decision_id=decision_id,
            client_order_id=f"{self.instance}-{decision_id}-{leg}-0",
            symbol=rec["symbol"],
            side=rec["side"],
            type="LIMIT",
            tif="IOC",
            price=rec["price"],
            qty=rec["qty"],
            quote_qty=rec["quote_qty"],
            status="FILLED",
            filled_qty=rec["filled_qty"],
            avg_price=rec["avg_price"],
            commission=rec["commission"],
            commission_asset=rec["commission_asset"],
            mid_at_decision=rec["mid_at_decision"],
            slippage_bps_vs_mid=rec["slippage_bps_vs_mid"],
            mode="paper",
            variant=variant,
            leg=leg,
            decision_mode=mode,
        )

    def execute_decision(
        self,
        decision_id: str,
        w_target: float,
        portfolios: dict[str, Portfolio],
        bar: JointBar,
        ts_ms: int,
        mode: str = "paper",
    ) -> dict[str, float]:
        """Simulate the decision for every variant. Returns the dv (USD) used per variant."""
        px = prices(bar.mid(THETA), bar.mid(TFUEL))
        book = {THETA: bar.bid_ask(THETA), TFUEL: bar.bid_ask(TFUEL)}
        dvs: dict[str, float] = {}
        for variant in ("mid", "touch"):
            port = portfolios.get(variant)
            if port is None:
                continue
            dv = trade_size(w_target, port.bal, px)
            dvs[variant] = dv
            if abs(dv) < self.min_trade_usd:
                continue
            sell_asset, buy_asset = leg_assets(dv)
            if variant == "mid":
                sell_price, buy_price = px[sell_asset], px[buy_asset]
            else:
                sell_price = book[SYMBOL_OF[sell_asset]][0]
                buy_price = book[SYMBOL_OF[buy_asset]][1]
            for rec in simulate_rebalance(port, dv, sell_price, buy_price, px, self.fee_rate, self.impact):
                self._log(ts_ms, decision_id, variant, rec, mode)
            port.note_rebalance(ts_ms)

        worst = portfolios.get("worst")
        if worst is not None:
            dv = trade_size(w_target, worst.bal, px)
            dvs["worst"] = dv
            self.pending_worst = None
            if abs(dv) >= self.min_trade_usd:
                # Filled on the next closed bar at its high (buys) / low (sells).
                self.pending_worst = {"decision_id": decision_id, "dv": dv, "mid": px, "decision_ts": ts_ms, "mode": mode}
        return dvs

    def on_bar(self, bar: JointBar, portfolios: dict[str, Portfolio], ts_ms: int) -> bool:
        """Settle a pending ``worst`` fill with this (the next) bar. Returns True if filled."""
        p = self.pending_worst
        if not p:
            return False
        self.pending_worst = None
        port = portfolios.get("worst")
        if port is None:
            return False
        dv = float(p["dv"])
        sell_asset, buy_asset = leg_assets(dv)
        sell_price = bar.kline(SYMBOL_OF[sell_asset]).l
        buy_price = bar.kline(SYMBOL_OF[buy_asset]).h
        recs = simulate_rebalance(port, dv, sell_price, buy_price, p["mid"], self.fee_rate, self.impact)
        for rec in recs:
            self._log(ts_ms, p["decision_id"], "worst", rec, p.get("mode", "paper"))
        if recs:
            port.note_rebalance(ts_ms)
        return bool(recs)
