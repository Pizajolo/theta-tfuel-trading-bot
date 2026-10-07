import asyncio
import json

import pytest

from bot.binance_client import BinanceAPIError
from bot.config import ExecutionParams, RiskParams
from bot.execution_live import LiveExecutor, client_order_id
from bot.market_data import SYMBOLS
from bot.portfolio import Portfolio, theta_weight
from bot.risk import RiskManager
from bot.storage import Storage, iter_jsonl
from bot.symbol_filters import parse_exchange_info
from bot.util import SimClock
from tools.fake_exchange import FakeExchange

from .conftest import T0

DEC = "2601011200"


class Harness:
    def __init__(self, tmp_path, balances=None, exec_kw=None, risk_kw=None, state=None, fx=None):
        self.fx = fx or FakeExchange(balances=balances or {"THETA": 500.0, "TFUEL": 10_000.0, "USDT": 0.0, "BNB": 0.0})
        self.clock = SimClock(T0)
        self.storage = Storage(tmp_path, self.clock)
        self.filters = parse_exchange_info(self.fx.exchange_info_dict(SYMBOLS), SYMBOLS)
        params = dict(slice_interval_sec=0.0, order_timeout_sec=300.0, use_bnb_fees=False)
        params.update(exec_kw or {})
        self.params = ExecutionParams(**params)
        self.risk = RiskManager(RiskParams(**(risk_kw or {})))
        self.port = Portfolio.from_balances("live", self.fx.free, T0)
        self.saves = 0
        self.ex = LiveExecutor(
            "s1k", self.fx, self.filters, self.params, self.risk, self.storage,
            portfolio_fn=lambda: self.port, book_fn=lambda s: None, price_fn=self.price,
            save_fn=self.save, clock=self.clock, state=state,
        )

    def price(self, asset):
        return {"USDT": 1.0, "THETA": self.fx.prices["THETAUSDT"], "TFUEL": self.fx.prices["TFUELUSDT"],
                "BNB": self.fx.prices["BNBUSDT"]}.get(asset)

    def save(self):
        self.saves += 1
        self.state = json.loads(json.dumps(self.ex.to_dict()))

    def w(self):
        px = {"THETA": self.fx.prices["THETAUSDT"], "TFUEL": self.fx.prices["TFUELUSDT"]}
        bal = {a: self.fx.total(a) for a in ("THETA", "TFUEL")}
        return theta_weight(bal, px)

    def decide(self, w_target, dec=DEC):
        self.ex.new_decision(dec, w_target, {"THETA": 1.0, "TFUEL": 0.05}, T0)
        return self.ex.decision

    def orders(self):
        return list(iter_jsonl(self.storage.instance_dir("s1k") / "orders.jsonl"))

    def events(self, kind=None):
        ev = list(iter_jsonl(self.storage.events_path()))
        return [e for e in ev if kind is None or e["kind"] == kind]


def run(coro):
    return asyncio.run(coro)


def test_client_order_ids_are_deterministic_and_valid():
    assert client_order_id("s1k", DEC, 1, 0) == "s1k-2601011200-1-0"
    assert client_order_id("s4k", "ib2601011200", 2, 37) == "s4k-ib2601011200-2-37"
    assert client_order_id("s1k", DEC, 2, 5) == client_order_id("s1k", DEC, 2, 5)
    with pytest.raises(ValueError):
        client_order_id("s1k", "bad id!", 1, 0)
    with pytest.raises(ValueError):
        client_order_id("s1k", "x" * 40, 1, 0)


def test_full_rebalance_cycle(tmp_path):
    h = Harness(tmp_path)
    d = h.decide(0.75)
    outcome = run(h.ex.run_attempt(d))
    assert outcome == "done" and d["status"] == "done"
    assert h.w() == pytest.approx(0.75, abs=0.005)
    placed = [o["clientOrderId"] for o in h.fx.order_log]
    assert placed == ["s1k-2601011200-1-0", "s1k-2601011200-2-1"]
    sell, buy = h.fx.order_log
    assert sell["symbol"] == "TFUELUSDT" and sell["side"] == "SELL" and sell["timeInForce"] == "IOC"
    assert buy["symbol"] == "THETAUSDT" and buy["side"] == "BUY"
    # buy leg spends what the sell leg actually received
    received = float(sell["cummulativeQuoteQty"]) - sum(float(f["commission"]) for f in sell["fills"])
    assert float(buy["cummulativeQuoteQty"]) <= received + 1e-9
    assert h.ex.pending_usdt < 5
    final = [o for o in h.orders() if o.get("event") == "final"]
    assert {o["status"] for o in final} == {"FILLED"}
    for o in final:
        for k in ("decision_id", "client_order_id", "symbol", "side", "type", "tif", "price", "qty", "quote_qty",
                  "status", "filled_qty", "avg_price", "commission", "commission_asset", "mid_at_decision",
                  "slippage_bps_vs_mid", "mode", "variant"):
            assert k in o, k
        assert o["mode"] == "live" and o["variant"] == "live"
    assert h.port.rebalances_total == 1 and h.port.fills == 2


def test_limit_prices_within_max_slippage(tmp_path):
    h = Harness(tmp_path, exec_kw={"max_slippage": 0.003})
    run(h.ex.run_attempt(h.decide(0.75)))
    sell, buy = h.fx.order_log
    bid, _ = h.fx.book("TFUELUSDT")
    _, ask = h.fx.book("THETAUSDT")
    assert bid * (1 - 0.003) <= float(sell["price"]) <= bid
    assert ask <= float(buy["price"]) <= ask * (1 + 0.003)


def test_slicing_respects_max_slice(tmp_path):
    h = Harness(tmp_path, exec_kw={"max_slice_usd": 100.0})
    d = h.decide(0.95)  # dv = 450 USD
    assert run(h.ex.run_attempt(d)) == "done"
    sells = [o for o in h.fx.order_log if o["side"] == "SELL"]
    assert len(sells) >= 4
    for o in sells:
        assert float(o["cummulativeQuoteQty"]) <= 100.0 * 1.0001 + 6  # last slice may absorb dust
    seqs = [int(o["clientOrderId"].rsplit("-", 1)[1]) for o in h.fx.order_log]
    assert seqs == sorted(seqs) and len(set(seqs)) == len(seqs)
    assert h.w() == pytest.approx(0.95, abs=0.01)


def test_partial_fills_timeout_then_retries(tmp_path):
    h = Harness(tmp_path, exec_kw={"max_slice_usd": 100.0, "order_timeout_sec": 0.0})
    h.fx.partial_fill = 0.5
    d = h.decide(0.75)
    assert run(h.ex.run_attempt(d)) == "pending_retry"
    assert d["status"] == "pending_retry"
    assert any(e["kind"] == "execution_timeout" for e in h.events())

    async def retry_until_done():
        for _ in range(12):
            kind = h.ex.pump(allowed=True)
            assert kind in ("retry", None)
            if h.ex.task:
                await h.ex.task
            if d["status"] == "done":
                return

    run(retry_until_done())
    assert d["status"] == "done"
    assert h.w() == pytest.approx(0.75, abs=0.01)
    ids = [o["clientOrderId"] for o in h.fx.order_log]
    assert len(ids) == len(set(ids))
    assert any(o["status"] == "EXPIRED" and float(o["executedQty"]) > 0 for o in h.fx.order_log)


def test_leg_failure_keeps_usdt_and_retries_leg2(tmp_path):
    h = Harness(tmp_path)
    h.fx.fail_next["THETAUSDT"] = [BinanceAPIError(400, -2010, "Account has insufficient balance")]
    d = h.decide(0.75)
    assert run(h.ex.run_attempt(d)) == "pending_retry"
    assert h.ex.pending_usdt == pytest.approx(h.fx.free["USDT"])
    assert h.ex.pending_usdt > 200
    warn = h.events("leg2_failed")
    assert warn and warn[0]["level"] == "WARNING"
    tfuel_after_leg1 = h.fx.free["TFUEL"]

    async def next_bar():
        assert h.ex.pump(allowed=True) == "retry"
        await h.ex.task

    run(next_bar())
    assert d["status"] == "done"
    assert h.ex.pending_usdt < 5.0  # only dust below minNotional remains
    assert h.fx.free["TFUEL"] == pytest.approx(tfuel_after_leg1)  # no second sell: USDT went to THETA
    assert h.w() == pytest.approx(0.75, abs=0.005)


def test_never_sells_more_than_free_balance(tmp_path):
    fx = FakeExchange(balances={"THETA": 500.0, "TFUEL": 10_000.0})
    fx.free["TFUEL"], fx.locked["TFUEL"] = 2000.0, 8000.0  # most TFUEL locked elsewhere
    h = Harness(tmp_path, fx=fx)
    run(h.ex.run_attempt(h.decide(0.75)))
    sells = [o for o in fx.order_log if o["side"] == "SELL"]
    assert sum(float(o["executedQty"]) for o in sells) <= 2000.0


def test_lost_response_is_resolved_by_client_order_id(tmp_path):
    h = Harness(tmp_path)
    h.fx.lose_response_next["TFUELUSDT"] = 1
    d = h.decide(0.75)
    assert run(h.ex.run_attempt(d)) == "done"
    ids = [o["clientOrderId"] for o in h.fx.order_log]
    assert ids == ["s1k-2601011200-1-0", "s1k-2601011200-2-1"]  # never re-sent
    assert "get_order TFUELUSDT s1k-2601011200-1-0" in h.fx.calls
    assert h.w() == pytest.approx(0.75, abs=0.005)


def test_restart_with_inflight_order_reconciles_without_resending(tmp_path):
    fx = FakeExchange(balances={"THETA": 500.0, "TFUEL": 10_000.0})
    cid = "s1k-2601011200-1-0"
    # the order reached Binance, then the bot crashed before recording the response
    run(fx.new_order(symbol="TFUELUSDT", side="SELL", type="LIMIT", timeInForce="IOC", quantity="5000",
                     price="0.04", newClientOrderId=cid, newOrderRespType="FULL"))
    state = {
        "pending_usdt": 0.0,
        "decision": {"id": DEC, "w_target": 0.75, "status": "running", "attempts": 1, "seq": 1, "created_ms": T0,
                     "mid": {"THETA": 1.0, "TFUEL": 0.05}, "filled_any": False, "last_error": None},
        "inflight": {"client_order_id": cid, "symbol": "TFUELUSDT", "side": "SELL", "leg": 1, "decision_id": DEC,
                     "qty": 5000.0, "price": 0.04},
    }
    h = Harness(tmp_path, fx=fx, state=state)
    run(h.ex.reconcile())
    assert h.ex.inflight is None
    assert h.ex.decision["status"] == "pending_retry"
    assert h.ex.pending_usdt == pytest.approx(fx.free["USDT"])
    assert [c for c in fx.calls if c.startswith("new_order")] == [f"new_order TFUELUSDT SELL {cid}"]

    async def resume():
        assert h.ex.pump(allowed=True) == "retry"
        await h.ex.task

    run(resume())
    ids = [o["clientOrderId"] for o in fx.order_log]
    assert ids[0] == cid and cid not in ids[1:]
    assert ids[1] == "s1k-2601011200-2-1"  # sequence continues after the persisted one
    assert h.w() == pytest.approx(0.75, abs=0.005)


def test_restart_with_inflight_order_that_never_arrived(tmp_path):
    state = {"decision": {"id": DEC, "w_target": 0.75, "status": "running", "attempts": 1, "seq": 1,
                          "created_ms": T0, "mid": {"THETA": 1.0, "TFUEL": 0.05}},
             "inflight": {"client_order_id": "s1k-2601011200-1-0", "symbol": "TFUELUSDT", "side": "SELL", "leg": 1,
                          "decision_id": DEC}}
    h = Harness(tmp_path, state=state)
    run(h.ex.reconcile())
    assert h.ex.inflight is None and h.ex.pending_usdt == 0.0
    run(h.ex.run_attempt(h.ex.decision))
    assert h.fx.order_log[0]["clientOrderId"] == "s1k-2601011200-1-1"


def test_risk_limit_max_order_blocks_for_the_day(tmp_path):
    h = Harness(tmp_path, risk_kw={"max_order_usd": 50.0})
    d = h.decide(0.75)
    assert run(h.ex.run_attempt(d)) == "pending_retry"
    assert h.fx.order_log == []
    assert h.risk.blocked
    err = h.events("risk_limit")
    assert err and err[0]["level"] == "ERROR"
    # next UTC day the block is lifted
    h.risk.roll(T0 + 86_400_000, 1000.0)
    assert not h.risk.blocked


def test_risk_limit_trades_per_day(tmp_path):
    h = Harness(tmp_path, risk_kw={"max_trades_per_day": 1})
    d = h.decide(0.75)
    assert run(h.ex.run_attempt(d)) == "pending_retry"
    assert len(h.fx.order_log) == 1  # sell filled, buy blocked
    assert h.ex.pending_usdt > 200  # USDT kept for the buy leg
    assert h.risk.blocked


def test_risk_limit_turnover():
    r = RiskManager(RiskParams(max_order_usd=1500, max_trades_per_day=30, max_daily_turnover_pct=100))
    assert r.check_order(600, T0, 1000.0) is None
    r.record_fill(600)
    assert r.check_order(300, T0, 1000.0) is None
    r.record_fill(300)
    assert "turnover" in r.check_order(200, T0, 1000.0)
    assert r.check_order(1, T0, 1000.0) is not None  # blocked for the rest of the day


def test_kill_cancels_open_orders(tmp_path):
    h = Harness(tmp_path)
    run(h.fx.new_order(symbol="THETAUSDT", side="SELL", type="LIMIT", timeInForce="GTC", quantity="10",
                       price="2", newClientOrderId="s1k-stray-1-0"))
    assert run(h.fx.open_orders("THETAUSDT"))
    run(h.ex.kill())
    assert "cancel_open_orders THETAUSDT" in h.fx.calls and "cancel_open_orders TFUELUSDT" in h.fx.calls
    assert run(h.fx.open_orders("THETAUSDT")) == []


def test_new_decision_supersedes_pending_one(tmp_path):
    h = Harness(tmp_path)
    h.fx.fail_next["THETAUSDT"] = [BinanceAPIError(400, -2010, "boom")]
    d1 = h.decide(0.75)
    run(h.ex.run_attempt(d1))
    usdt = h.ex.pending_usdt
    assert usdt > 0
    d2 = h.decide(0.35, dec="2601011300")
    assert d1["status"] == "superseded"

    async def go():
        assert h.ex.pump(allowed=True) == "start"
        await h.ex.task

    run(go())
    assert d2["status"] == "done"
    assert h.ex.pending_usdt < 5.0
    assert h.w() == pytest.approx(0.35, abs=0.006)


def test_max_retries_then_error(tmp_path):
    h = Harness(tmp_path, exec_kw={"max_retries": 2})
    h.fx.partial_fill = 0.0  # nothing ever fills
    d = h.decide(0.75)

    async def loop():
        for _ in range(5):
            h.ex.pump(allowed=True)
            if h.ex.task:
                await h.ex.task

    run(loop())
    assert d["status"] == "failed"
    assert any(e["kind"] == "decision_failed" and e["level"] == "ERROR" for e in h.events())


def test_startup_checks(tmp_path):
    h = Harness(tmp_path)
    assert run(h.ex.startup_checks(testnet=False)) == []
    h.fx.withdrawals_enabled = True
    probs = run(h.ex.startup_checks(testnet=False))
    assert any("WITHDRAWALS" in p for p in probs)
    h.fx.withdrawals_enabled = False
    h.fx.can_trade = False
    assert any("canTrade" in p for p in run(h.ex.startup_checks(testnet=True)))


def test_bnb_fee_warning(tmp_path):
    h = Harness(tmp_path, exec_kw={"use_bnb_fees": True, "bnb_min_usd": 5.0},
                balances={"THETA": 500.0, "TFUEL": 10_000.0, "BNB": 0.001})
    run(h.ex.check_bnb())
    assert h.events("bnb_low")
