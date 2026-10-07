"""Live execution on Binance Spot (one executor per live instance / sub-account).

A *decision* is a new THETA target weight. Executing it takes one or more *attempts*; each
attempt recomputes the trade from real balances, so a restarted or retried decision can never
overshoot (no double execution).

Per attempt:

1. Spend strategy-owned USDT left over from an earlier sell leg (``pending_usdt``) first,
   split between THETA and TFUEL so the pair moves towards the target.
2. ``dv = (w_target - w) * V_pair``; done if ``|dv| < MIN_TRADE_USD``.
3. Slices of at most ``MAX_SLICE_USD``, at least ``SLICE_INTERVAL_SEC`` apart: SELL the rich
   asset for USDT (leg 1), then BUY the cheap asset with the USDT actually received (leg 2).
   LIMIT IOC orders priced at the touch, at most ``MAX_SLIPPAGE`` through it.
4. After ``ORDER_TIMEOUT_SEC`` the remaining slices are abandoned; the next closed non-stale bar
   starts a new attempt (at most ``MAX_RETRIES`` retries, then an ERROR event).

If leg 1 fills but leg 2 fails the USDT is kept (``pending_usdt``), a WARNING is logged and
leg 2 is retried on the next bar.

Client order ids are deterministic: ``{instance}-{decision_id}-{leg}-{slice}``; ``slice`` is a
per-decision sequence that is persisted *before* each order is sent. On restart an order that
was in flight is looked up by its client order id instead of being re-sent.
"""

from __future__ import annotations

import asyncio
import re
import time
from dataclasses import dataclass, field
from decimal import Decimal
from typing import Any, Callable

from bot.binance_client import BinanceAPIError, BinanceClient, UnknownOrderStatus
from bot.config import ExecutionParams
from bot.execution_paper import leg_assets, slippage_bps
from bot.market_data import SYMBOLS, BookTop
from bot.portfolio import ASSET_OF, ASSETS, SYMBOL_OF, Portfolio, pair_value, trade_size
from bot.risk import RiskManager
from bot.storage import Storage
from bot.symbol_filters import SymbolFilters, fmt
from bot.util import Clock

CLIENT_ID_RE = re.compile(r"^[a-zA-Z0-9-_]{1,36}$")
DUST_MARGIN = 1.02  # stay a little above minNotional so price moves do not reject a slice


def client_order_id(instance: str, decision_id: str, leg: int, slice_no: int) -> str:
    cid = f"{instance}-{decision_id}-{leg}-{slice_no}"
    if not CLIENT_ID_RE.match(cid):
        raise ValueError(f"invalid client order id {cid!r}")
    return cid


@dataclass
class OrderOutcome:
    client_order_id: str
    symbol: str
    side: str
    leg: int
    status: str
    qty: float = 0.0
    price: float = 0.0
    executed_qty: float = 0.0
    quote_qty: float = 0.0
    commissions: dict[str, float] = field(default_factory=dict)
    error: str | None = None
    order_id: int | None = None

    @property
    def avg_price(self) -> float:
        return self.quote_qty / self.executed_qty if self.executed_qty > 0 else 0.0

    @property
    def filled(self) -> bool:
        return self.executed_qty > 0

    def usdt_commission(self) -> float:
        return self.commissions.get("USDT", 0.0)


def parse_order_response(resp: dict[str, Any], leg: int) -> OrderOutcome:
    comm: dict[str, float] = {}
    for f in resp.get("fills") or []:
        a = f.get("commissionAsset")
        if a:
            comm[a] = comm.get(a, 0.0) + float(f.get("commission", 0) or 0)
    quote = float(resp.get("cummulativeQuoteQty", 0) or 0)
    return OrderOutcome(
        client_order_id=resp.get("clientOrderId", ""),
        symbol=resp.get("symbol", ""),
        side=resp.get("side", ""),
        leg=leg,
        status=resp.get("status", "UNKNOWN"),
        qty=float(resp.get("origQty", 0) or 0),
        price=float(resp.get("price", 0) or 0),
        executed_qty=float(resp.get("executedQty", 0) or 0),
        quote_qty=max(quote, 0.0),
        commissions=comm,
        order_id=resp.get("orderId"),
    )


class LiveExecutor:
    def __init__(
        self,
        instance: str,
        client: BinanceClient,
        filters: dict[str, SymbolFilters],
        params: ExecutionParams,
        risk: RiskManager,
        storage: Storage,
        portfolio_fn: Callable[[], Portfolio | None],
        book_fn: Callable[[str], BookTop | None],
        price_fn: Callable[[str], float | None],
        save_fn: Callable[[], None],
        clock: Clock | None = None,
        state: dict[str, Any] | None = None,
        sleep: Callable[[float], Any] = asyncio.sleep,
    ) -> None:
        self.instance = instance
        self.client = client
        self.filters = filters
        self.params = params
        self.risk = risk
        self.storage = storage
        self.portfolio_fn = portfolio_fn
        self.book_fn = book_fn
        self.price_fn = price_fn
        self.save_fn = save_fn
        self.clock = clock or Clock()
        self._sleep = sleep
        st = state or {}
        self.pending_usdt: float = float(st.get("pending_usdt", 0.0))
        self.decision: dict[str, Any] | None = st.get("decision")
        self.inflight: dict[str, Any] | None = st.get("inflight")
        self.recent_orders: list[dict[str, Any]] = list(st.get("recent_orders", []))
        self.balances: dict[str, float] = dict(st.get("balances", {}))
        self.free: dict[str, float] = dict(st.get("free", {}))
        self.balances_ts: int | None = st.get("balances_ts")
        self.task: asyncio.Task | None = None
        self._stop = asyncio.Event()
        self.risk_event_logged_date: str | None = st.get("risk_event_logged_date")

    # ---- persistence ----------------------------------------------------------------------
    def to_dict(self) -> dict[str, Any]:
        return {
            "pending_usdt": self.pending_usdt,
            "decision": self.decision,
            "inflight": self.inflight,
            "recent_orders": self.recent_orders[-200:],
            "balances": self.balances,
            "free": self.free,
            "balances_ts": self.balances_ts,
            "risk_event_logged_date": self.risk_event_logged_date,
        }

    @property
    def busy(self) -> bool:
        return self.task is not None and not self.task.done()

    def _event(self, level: str, kind: str, msg: str, **data: Any) -> None:
        self.storage.event(level, kind, msg, instance=self.instance, **data)

    # ---- account ---------------------------------------------------------------------------
    async def refresh_balances(self) -> tuple[dict[str, float], dict[str, float]]:
        acct = await self.client.account()
        total = {a: 0.0 for a in ASSETS}
        free = {a: 0.0 for a in ASSETS}
        for b in acct.get("balances", []):
            a = b.get("asset")
            if a in total:
                f, l = float(b.get("free", 0) or 0), float(b.get("locked", 0) or 0)
                total[a] = f + l
                free[a] = f
        self.balances, self.free, self.balances_ts = total, free, self.clock.now_ms()
        port = self.portfolio_fn()
        if port is not None:
            port.bal = dict(total)
        return total, free

    async def startup_checks(self, testnet: bool) -> list[str]:
        """Fatal problems (empty list = OK). Withdrawal-enabled keys are refused."""
        problems: list[str] = []
        try:
            acct = await self.client.account()
        except BinanceAPIError as exc:
            return [f"account query failed: {exc}"]
        if not acct.get("canTrade", False):
            problems.append("account/API key cannot trade (canTrade=false)")
        if testnet:
            self._event("INFO", "permission_check", "Spot testnet has no /sapi endpoints: API key restriction check skipped")
        else:
            try:
                r = await self.client.api_restrictions()
            except BinanceAPIError as exc:
                return problems + [f"could not verify API key permissions (/sapi/v1/account/apiRestrictions): {exc}"]
            if r.get("enableWithdrawals"):
                problems.append("API key has WITHDRAWALS ENABLED - refusing to trade live. Create a trade-only key.")
            if not r.get("enableSpotAndMarginTrading"):
                problems.append("API key does not have spot trading enabled")
            if not r.get("ipRestrict"):
                self._event("WARNING", "ip_whitelist", "API key is not IP-restricted; restrict it to this server's IP")
        self._event(
            "INFO",
            "ip_whitelist",
            "Recommendation: restrict each API key to the bot's IP address (Binance API management)",
        )
        await self.check_bnb()
        return problems

    async def check_bnb(self) -> None:
        if not self.params.use_bnb_fees:
            return
        try:
            total, _ = await self.refresh_balances()
        except BinanceAPIError as exc:
            self._event("WARNING", "bnb_check", f"could not check BNB balance: {exc}")
            return
        bnb_px = self.price_fn("BNB") or 0.0
        bnb_usd = total.get("BNB", 0.0) * bnb_px
        if total.get("BNB", 0.0) <= 0:
            self._event(
                "WARNING",
                "bnb_low",
                "USE_BNB_FEES=true but the account holds no BNB; fees will be charged in the traded asset",
            )
        elif bnb_px and bnb_usd < self.params.bnb_min_usd:
            self._event("WARNING", "bnb_low", f"BNB balance {bnb_usd:.2f} USD is below BNB_MIN_USD {self.params.bnb_min_usd}")

    # ---- prices -------------------------------------------------------------------------------
    async def touch(self, symbol: str) -> tuple[float, float]:
        b = self.book_fn(symbol)
        if b is not None:
            return b.bid, b.ask
        r = await self.client.book_ticker(symbol)
        return float(r["bidPrice"]), float(r["askPrice"])

    async def mids(self) -> dict[str, float]:
        out = {"USDT": 1.0, "BNB": self.price_fn("BNB") or 0.0}
        for sym in SYMBOLS:
            bid, ask = await self.touch(sym)
            out[ASSET_OF[sym]] = (bid + ask) / 2.0
        return out

    def fee_usd(self, commissions: dict[str, float], px: dict[str, float]) -> float:
        return sum(q * (px.get(a) or self.price_fn(a) or 0.0) for a, q in commissions.items())

    # ---- decisions ---------------------------------------------------------------------------
    def new_decision(self, decision_id: str, w_target: float, mid: dict[str, float], ts_ms: int) -> None:
        old = self.decision
        if old and old.get("status") in ("new", "running", "pending_retry"):
            old["status"] = "superseded"
            self._event("INFO", "decision_superseded", f"decision {old['id']} superseded by {decision_id}")
        if self.busy:
            self._stop.set()
        self.decision = {
            "id": decision_id,
            "w_target": w_target,
            "status": "new",
            "attempts": 0,
            "seq": 0,
            "created_ms": ts_ms,
            "mid": {"THETA": mid["THETA"], "TFUEL": mid["TFUEL"]},
            "filled_any": False,
            "last_error": None,
        }
        self.save_fn()

    def pump(self, allowed: bool) -> str | None:
        """Called on every closed non-stale live bar. Starts an attempt when one is due.

        Returns "start" / "retry" when an attempt was launched, else None.
        """
        d = self.decision
        if not allowed or d is None or self.busy:
            return None
        status = d.get("status")
        if status not in ("new", "pending_retry"):
            return None
        if status == "pending_retry" and d["attempts"] > self.params.max_retries:
            d["status"] = "failed"
            self._event(
                "ERROR",
                "decision_failed",
                f"decision {d['id']} still incomplete after {self.params.max_retries} retries; giving up",
                decision_id=d["id"],
            )
            self.save_fn()
            return None
        kind = "start" if status == "new" else "retry"
        self._stop = asyncio.Event()
        self.task = asyncio.ensure_future(self.run_attempt(d))
        return kind

    async def stop(self, wait: bool = True) -> None:
        self._stop.set()
        if wait and self.task is not None:
            try:
                await asyncio.wait_for(asyncio.shield(self.task), timeout=30)
            except (asyncio.TimeoutError, Exception):
                self.task.cancel()

    async def kill(self) -> None:
        """Kill switch: stop the attempt and cancel every open order of this account."""
        await self.stop(wait=True)
        for sym in SYMBOLS:
            try:
                res = await self.client.cancel_open_orders(sym)
                if res:
                    self._event("WARNING", "orders_cancelled", f"kill switch cancelled {len(res)} open order(s) on {sym}")
            except BinanceAPIError as exc:
                self._event("ERROR", "cancel_failed", f"could not cancel open orders on {sym}: {exc}")
        if self.decision and self.decision.get("status") == "running":
            self.decision["status"] = "pending_retry"
        self.save_fn()

    async def run_attempt(self, d: dict[str, Any]) -> str:
        d["attempts"] += 1
        d["status"] = "running"
        d["attempt_started_ms"] = self.clock.now_ms()
        self.save_fn()
        outcome = "pending_retry"
        try:
            outcome = await self._attempt(d)
        except asyncio.CancelledError:
            outcome = "pending_retry"
            raise
        except Exception as exc:
            d["last_error"] = repr(exc)
            self._event("ERROR", "execution_error", f"decision {d['id']} attempt {d['attempts']} failed: {exc!r}")
            outcome = "pending_retry"
        finally:
            if d is self.decision and d.get("status") == "running":
                d["status"] = outcome
                if outcome == "done":
                    port = self.portfolio_fn()
                    if port is not None and d.get("filled_any"):
                        port.note_rebalance(self.clock.now_ms())
                    self._event("INFO", "decision_done", f"decision {d['id']} complete", decision_id=d["id"], attempts=d["attempts"])
                elif outcome == "pending_retry" and d["attempts"] > self.params.max_retries:
                    d["status"] = "failed"
                    self._event(
                        "ERROR",
                        "decision_failed",
                        f"decision {d['id']} incomplete after {self.params.max_retries} retries; giving up",
                        decision_id=d["id"],
                    )
            self.save_fn()
        return outcome

    def _stopped(self) -> bool:
        return self._stop.is_set()

    def _min_usd(self, symbol: str) -> float:
        return max(self.filters[symbol].min_notional_f() * DUST_MARGIN, 1e-9)

    async def _attempt(self, d: dict[str, Any]) -> str:
        p = self.params
        t0 = time.monotonic()
        w_target = float(d["w_target"])
        total, free = await self.refresh_balances()
        px = await self.mids()

        # 1) leftover USDT from an earlier sell leg goes first
        if self.pending_usdt > 0:
            ok = await self._spend_pending(d, w_target, total, free, px)
            if not ok:
                return "pending_retry"
            total, free = await self.refresh_balances()
            px = await self.mids()

        # 2) size the trade
        dv = trade_size(w_target, total, px)
        if abs(dv) < p.min_trade_usd:
            return "done" if self.pending_usdt < self._min_usd(SYMBOLS[0]) else "pending_retry"
        sell_asset, buy_asset = leg_assets(dv)
        sell_sym, buy_sym = SYMBOL_OF[sell_asset], SYMBOL_OF[buy_asset]
        remaining = abs(dv)
        zero_fills = 0

        # 3) slices
        while remaining >= self._min_usd(sell_sym):
            if self._stopped():
                return "pending_retry"
            slice_usd = min(p.max_slice_usd, remaining)
            if remaining - slice_usd < self._min_usd(sell_sym):
                slice_usd = remaining  # do not leave an unsellable remainder
            r1 = await self._sell(d, sell_asset, slice_usd, px)
            if r1 is None:
                return "pending_retry"
            if r1.filled:
                zero_fills = 0
                self.pending_usdt += r1.quote_qty - r1.usdt_commission()
                remaining -= r1.quote_qty
                self.save_fn()
            else:
                zero_fills += 1
                if zero_fills >= 3:
                    self._event("WARNING", "no_fill", f"decision {d['id']}: {sell_sym} sell IOC got no fill 3 times; will retry")
                    return "pending_retry"

            if self.pending_usdt >= self._min_usd(buy_sym):
                r2 = await self._buy(d, buy_asset, self.pending_usdt, px)
                if r2 is None or not r2.filled:
                    self._event(
                        "WARNING",
                        "leg2_failed",
                        f"decision {d['id']}: buy leg on {buy_sym} failed ({(r2.error or r2.status) if r2 else 'not sent'}); "
                        f"keeping {self.pending_usdt:.2f} USDT, retrying next bar",
                        decision_id=d["id"],
                    )
                    return "pending_retry"
                self._consume_pending(r2)

            if remaining < self._min_usd(sell_sym):
                break
            if await self._wait(p.slice_interval_sec):
                return "pending_retry"
            if time.monotonic() - t0 > p.order_timeout_sec:
                self._event(
                    "WARNING", "execution_timeout",
                    f"decision {d['id']}: ORDER_TIMEOUT_SEC reached with {remaining:.2f} USD left; retrying on the next bar",
                )
                return "pending_retry"

        if self.pending_usdt >= self._min_usd(buy_sym):
            return "pending_retry"
        return "done"

    def _consume_pending(self, r: OrderOutcome) -> None:
        self.pending_usdt -= r.quote_qty + r.usdt_commission()
        if self.pending_usdt < 1e-8:
            self.pending_usdt = 0.0
        self.save_fn()

    async def _wait(self, seconds: float) -> bool:
        """Sleep between slices; returns True if a stop was requested meanwhile."""
        try:
            await asyncio.wait_for(self._stop.wait(), timeout=seconds)
            return True
        except asyncio.TimeoutError:
            return False

    async def _spend_pending(
        self, d: dict[str, Any], w_target: float, total: dict[str, float], free: dict[str, float], px: dict[str, float]
    ) -> bool:
        u = min(self.pending_usdt, free.get("USDT", 0.0))
        if u < self.pending_usdt:
            self.pending_usdt = u
        v = pair_value(total, px) + u
        theta_part = min(max(w_target * v - total.get("THETA", 0.0) * px["THETA"], 0.0), u)
        parts = (("THETA", theta_part), ("TFUEL", u - theta_part))
        for asset, amount in parts:
            sym = SYMBOL_OF[asset]
            if amount < self._min_usd(sym):
                continue
            r = await self._buy(d, asset, amount, px)
            if r is None or not r.filled:
                self._event(
                    "WARNING",
                    "leg2_failed",
                    f"decision {d['id']}: retry of buy leg on {sym} failed ({(r.error or r.status) if r else 'not sent'}); "
                    f"keeping {self.pending_usdt:.2f} USDT",
                    decision_id=d["id"],
                )
                return False
            self._consume_pending(r)
        return True

    async def _sell(self, d: dict[str, Any], asset: str, usd: float, px: dict[str, float]) -> OrderOutcome | None:
        sym = SYMBOL_OF[asset]
        f = self.filters[sym]
        bid, ask = await self.touch(sym)
        _, free = await self.refresh_balances()
        limit = f.price_for(bid * (1.0 - self.params.max_slippage), "SELL")
        qty = f.qty_down(min(usd / bid, free.get(asset, 0.0)))
        return await self._submit(d, sym, "SELL", 1, qty, limit, bid, ask)

    async def _buy(self, d: dict[str, Any], asset: str, usdt: float, px: dict[str, float]) -> OrderOutcome | None:
        sym = SYMBOL_OF[asset]
        f = self.filters[sym]
        bid, ask = await self.touch(sym)
        _, free = await self.refresh_balances()
        limit = f.price_for(ask * (1.0 + self.params.max_slippage), "BUY")
        if limit <= 0:
            return None
        qty = f.qty_down(min(usdt / ask, free.get("USDT", 0.0) / float(limit)))
        return await self._submit(d, sym, "BUY", 2, qty, limit, bid, ask)

    async def _submit(
        self, d: dict[str, Any], sym: str, side: str, leg: int, qty: Decimal, limit: Decimal, bid: float, ask: float
    ) -> OrderOutcome | None:
        f = self.filters[sym]
        reason = f.check(qty, limit)
        if reason:
            self._event("WARNING", "order_skipped", f"{side} {sym} not sent: {reason}", decision_id=d["id"])
            return OrderOutcome("", sym, side, leg, "SKIPPED", error=reason)
        ref = bid if side == "SELL" else ask
        notional = float(qty) * ref
        port = self.portfolio_fn()
        pv = port.value(await self.mids()) if port else notional
        block = self.risk.check_order(notional, self.clock.now_ms(), pv)
        if block:
            if self.risk_event_logged_date != self.risk.state.date:
                self.risk_event_logged_date = self.risk.state.date
                self._event("ERROR", "risk_limit", f"live orders blocked for today: {block}", decision_id=d["id"])
            self.save_fn()
            return None
        if self._stopped():
            return None
        return await self.place_order(d, sym, side, leg, qty, limit, (bid + ask) / 2.0)

    async def place_order(
        self, d: dict[str, Any], sym: str, side: str, leg: int, qty: Decimal, limit: Decimal, mid_now: float
    ) -> OrderOutcome:
        cid = client_order_id(self.instance, d["id"], leg, d["seq"])
        d["seq"] += 1
        self.inflight = {"client_order_id": cid, "symbol": sym, "side": side, "leg": leg, "decision_id": d["id"],
                         "qty": float(qty), "price": float(limit), "sent_ms": self.clock.now_ms()}
        self.save_fn()  # persisted before sending: a crash can never re-use this id
        base = dict(
            decision_id=d["id"], client_order_id=cid, symbol=sym, side=side, type="LIMIT", tif="IOC",
            price=float(limit), qty=float(qty), leg=leg, mode="live", variant="live",
            mid_at_decision=d["mid"][ASSET_OF[sym]], mid_at_send=mid_now,
        )
        self.storage.write_order(self.instance, status="SUBMITTED", event="submitted", **base)
        try:
            resp = await self.client.new_order(
                symbol=sym, side=side, type="LIMIT", timeInForce="IOC", quantity=fmt(qty), price=fmt(limit),
                newClientOrderId=cid, newOrderRespType="FULL",
            )
            out = parse_order_response(resp, leg)
        except UnknownOrderStatus as exc:
            self._event("WARNING", "order_status_unknown", f"{cid}: {exc}; querying by client order id")
            out = await self.resolve(sym, cid, side, leg)
        except BinanceAPIError as exc:
            out = OrderOutcome(cid, sym, side, leg, "REJECTED", qty=float(qty), price=float(limit), error=f"{exc.code}: {exc.msg}")
        out.client_order_id = out.client_order_id or cid
        self._record(d, out, base)
        self.inflight = None
        self.recent_orders.append({"client_order_id": cid, "symbol": sym, "leg": leg, "ts": self.clock.now_ms()})
        self.save_fn()
        return out

    async def resolve(self, sym: str, cid: str, side: str, leg: int) -> OrderOutcome:
        """Find out what happened to an order whose response was lost."""
        last_exc: Exception | None = None
        for i in range(5):
            try:
                o = await self.client.get_order(sym, cid)
                if o is None:
                    return OrderOutcome(cid, sym, side, leg, "NOT_FOUND", error="order was not placed")
                out = parse_order_response(o, leg)
                if out.status in ("NEW", "PARTIALLY_FILLED"):  # IOC should never rest; cancel to be safe
                    try:
                        await self.client.cancel_order(sym, cid)
                    except BinanceAPIError:
                        pass
                    o = await self.client.get_order(sym, cid) or o
                    out = parse_order_response(o, leg)
                if out.filled and not out.commissions and out.order_id is not None:
                    try:
                        for t in await self.client.my_trades(sym, int(out.order_id)):
                            a = t.get("commissionAsset")
                            out.commissions[a] = out.commissions.get(a, 0.0) + float(t.get("commission", 0) or 0)
                    except BinanceAPIError:
                        pass
                return out
            except (BinanceAPIError, UnknownOrderStatus) as exc:
                last_exc = exc
                await self._sleep(min(2 ** i, 15))
        return OrderOutcome(cid, sym, side, leg, "UNKNOWN", error=f"could not resolve: {last_exc!r}")

    def _record(self, d: dict[str, Any], out: OrderOutcome, base: dict[str, Any]) -> None:
        mid_dec = base["mid_at_decision"]
        slip = slippage_bps(out.side, out.avg_price, mid_dec) if out.filled else None
        comm_asset = next(iter(out.commissions), None)
        px = {"USDT": 1.0, "THETA": self.price_fn("THETA") or 0.0, "TFUEL": self.price_fn("TFUEL") or 0.0,
              "BNB": self.price_fn("BNB") or 0.0}
        fee_usd = self.fee_usd(out.commissions, px)
        rec = dict(base)
        rec.update(
            status=out.status, event="final", quote_qty=out.quote_qty, filled_qty=out.executed_qty,
            avg_price=out.avg_price if out.filled else None,
            commission=out.commissions.get(comm_asset, 0.0) if comm_asset else 0.0,
            commission_asset=comm_asset, commissions=out.commissions, fee_usd=fee_usd,
            slippage_bps_vs_mid=slip, order_id=out.order_id,
        )
        if out.error:
            rec["error"] = out.error
        self.storage.write_order(self.instance, **rec)
        if out.filled:
            d["filled_any"] = True
            self.risk.record_fill(out.quote_qty)
            port = self.portfolio_fn()
            if port is not None:
                port.note_fill(out.quote_qty, fee_usd, slip)
        if out.status in ("REJECTED", "UNKNOWN"):
            self._event("WARNING", "order_" + out.status.lower(), f"{out.client_order_id} {out.side} {out.symbol}: {out.error}")

    # ---- restart -------------------------------------------------------------------------------
    async def reconcile(self) -> None:
        """Resolve an order that was in flight at shutdown and cancel stray open orders."""
        if self.inflight:
            inf = self.inflight
            out = await self.resolve(inf["symbol"], inf["client_order_id"], inf["side"], int(inf["leg"]))
            d = self.decision if self.decision and self.decision.get("id") == inf.get("decision_id") else {
                "id": inf.get("decision_id"), "mid": {"THETA": 0.0, "TFUEL": 0.0}}
            base = dict(
                decision_id=inf.get("decision_id"), client_order_id=inf["client_order_id"], symbol=inf["symbol"],
                side=inf["side"], type="LIMIT", tif="IOC", price=inf.get("price"), qty=inf.get("qty"), leg=inf["leg"],
                mode="live", variant="live", mid_at_decision=(d.get("mid") or {}).get(ASSET_OF[inf["symbol"]], 0.0),
                reconciled=True,
            )
            if out.status != "UNKNOWN":
                self._record(d, out, base)
                if out.filled:
                    if int(inf["leg"]) == 1:
                        self.pending_usdt += out.quote_qty - out.usdt_commission()
                    else:
                        self.pending_usdt = max(0.0, self.pending_usdt - out.quote_qty - out.usdt_commission())
                self._event("INFO", "order_reconciled", f"{inf['client_order_id']}: {out.status}, filled {out.executed_qty}")
                self.inflight = None
            else:
                self._event("ERROR", "order_unresolved", f"{inf['client_order_id']}: could not be resolved; will retry on next start")
        prefix = f"{self.instance}-"
        for sym in SYMBOLS:
            try:
                for o in await self.client.open_orders(sym):
                    if str(o.get("clientOrderId", "")).startswith(prefix):
                        await self.client.cancel_order(sym, o["clientOrderId"])
                        self._event("WARNING", "stray_order_cancelled", f"cancelled open order {o['clientOrderId']} on {sym}")
            except BinanceAPIError as exc:
                self._event("WARNING", "reconcile_failed", f"open order check on {sym} failed: {exc}")
        if self.decision and self.decision.get("status") == "running":
            self.decision["status"] = "pending_retry"
        self.save_fn()
