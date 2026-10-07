import pytest

from bot.execution_paper import PaperExecutor, simulate_rebalance
from bot.portfolio import Portfolio, prices
from bot.storage import Storage, iter_jsonl

from .conftest import T0, make_bar


def ports(px):
    return {v: Portfolio.fifty_fifty(v, 1000.0, px, T0) for v in ("mid", "touch", "worst")}


def test_simulate_rebalance_two_legs_with_fees():
    px = prices(1.0, 0.05)
    p = Portfolio.fifty_fifty("mid", 1000.0, px, T0)
    recs = simulate_rebalance(p, 250.0, 0.05, 1.0, px, fee_rate=0.001)
    assert [r["side"] for r in recs] == ["SELL", "BUY"]
    assert recs[0]["symbol"] == "TFUELUSDT" and recs[1]["symbol"] == "THETAUSDT"
    assert p.bal["TFUEL"] == pytest.approx(10_000 - 5_000)
    proceeds = 250.0 * 0.999
    assert p.bal["THETA"] == pytest.approx(500 + proceeds * 0.999)
    assert p.bal["USDT"] == 0.0
    assert p.fees_usd == pytest.approx(0.25 + proceeds * 0.001)
    assert p.w(px) == pytest.approx(0.75, abs=0.001)


def test_never_sells_more_than_held():
    px = prices(1.0, 0.05)
    p = Portfolio.from_balances("mid", {"THETA": 100.0, "TFUEL": 0.0}, T0)
    recs = simulate_rebalance(p, -500.0, 1.0, 0.05, px, fee_rate=0.001)
    assert recs[0]["qty"] == pytest.approx(100.0) and p.bal["THETA"] == 0.0


def test_variants_mid_touch_worst(tmp_path):
    st = Storage(tmp_path)
    px = prices(1.0, 0.05)
    pf = ports(px)
    ex = PaperExecutor("s1k", st, fee_rate=0.001, min_trade_usd=10.0)
    bar = make_bar(T0, 1.0, 0.05, spread=0.002)  # bid/ask +-0.1%
    ex.execute_decision("2601010000", 0.75, pf, bar, T0 + 60_000)
    assert pf["mid"].w(px) > pf["touch"].w(px)  # touch paid the spread
    assert pf["worst"].w(px) == pytest.approx(0.5)  # waits for the next bar
    assert ex.pending_worst is not None
    nxt = make_bar(T0 + 60_000, 1.0, 0.05)
    nxt.theta.h, nxt.tfuel.l = 1.003, 0.05 * 0.997  # next bar range wider than the spread
    assert ex.on_bar(nxt, pf, T0 + 120_000)
    assert ex.pending_worst is None
    v = {k: p.value(px) for k, p in pf.items()}
    assert v["mid"] > v["touch"] > v["worst"]
    orders = list(iter_jsonl(st.instance_dir("s1k") / "orders.jsonl"))
    assert len(orders) == 6
    by = {(o["variant"], o["leg"]): o for o in orders}
    assert by[("touch", 1)]["avg_price"] == pytest.approx(0.05 * 0.999)  # sold at the bid
    assert by[("touch", 2)]["avg_price"] == pytest.approx(1.0 * 1.001)  # bought at the ask
    assert by[("worst", 1)]["avg_price"] == pytest.approx(nxt.tfuel.l)  # next bar low
    assert by[("worst", 2)]["avg_price"] == pytest.approx(nxt.theta.h)  # next bar high
    assert by[("touch", 1)]["slippage_bps_vs_mid"] == pytest.approx(10.0)
    assert all(o["mode"] == "paper" for o in orders)


def test_small_trades_are_skipped(tmp_path):
    px = prices(1.0, 0.05)
    pf = ports(px)
    ex = PaperExecutor("s1k", Storage(tmp_path), fee_rate=0.001, min_trade_usd=10.0)
    dvs = ex.execute_decision("x", 0.505, pf, make_bar(T0, 1.0, 0.05), T0)
    assert abs(dvs["mid"]) < 10 and pf["mid"].fills == 0 and ex.pending_worst is None


def test_excess_and_token_equivalents():
    px = prices(1.0, 0.05)
    p = Portfolio.fifty_fifty("touch", 1000.0, px, T0)
    p.bal["THETA"] *= 1.1  # strategy holds 10% more THETA than HODL
    f = p.equity_fields(px)
    assert f["excess"] == pytest.approx(0.05)
    assert f["theta_equiv_tokens"] == pytest.approx(500 * 1.05)
    assert f["tfuel_equiv_tokens"] == pytest.approx(10_000 * 1.05)
