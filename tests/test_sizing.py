from decimal import Decimal

import pytest

from bot.portfolio import pair_value, prices, theta_weight, trade_size
from bot.symbol_filters import SymbolFilters, fmt, round_step_down, round_step_up

INFO = {
    "symbol": "TFUELUSDT",
    "status": "TRADING",
    "baseAsset": "TFUEL",
    "quoteAsset": "USDT",
    "filters": [
        {"filterType": "PRICE_FILTER", "minPrice": "0.00001", "maxPrice": "1000", "tickSize": "0.00001"},
        {"filterType": "LOT_SIZE", "minQty": "1", "maxQty": "9000000", "stepSize": "1"},
        {"filterType": "NOTIONAL", "minNotional": "5", "applyMinToMarket": True, "maxNotional": "9000000",
         "applyMaxToMarket": False, "avgPriceMins": 5},
    ],
}


def test_trade_size_and_weight():
    px = prices(1.0, 0.05)
    bal = {"THETA": 500.0, "TFUEL": 10_000.0, "USDT": 123.0, "BNB": 1.0}
    assert pair_value(bal, px) == pytest.approx(1000.0)
    assert theta_weight(bal, px) == pytest.approx(0.5)  # USDT/BNB excluded from w
    assert trade_size(0.75, bal, px) == pytest.approx(250.0)
    assert trade_size(0.35, bal, px) == pytest.approx(-150.0)
    assert trade_size(0.5, bal, px) == pytest.approx(0.0)


def test_round_step_down_and_up():
    assert round_step_down("123.456", "0.1") == Decimal("123.4")
    assert round_step_down(0.99999, "0.001") == Decimal("0.999")
    assert round_step_down(5, "1") == Decimal("5")
    assert round_step_up("0.050001", "0.00001") == Decimal("0.05001")
    assert fmt(Decimal("1E+1")) == "10"
    assert fmt(Decimal("0.05000")) == "0.05"


def test_symbol_filters_rounding_and_checks():
    f = SymbolFilters.from_exchange_info(INFO)
    assert f.tick_size == Decimal("0.00001") and f.step_size == Decimal("1") and f.min_notional == Decimal("5")
    assert f.qty_down(1234.99) == Decimal("1234")
    # sells round the limit up (less aggressive), buys round down
    assert f.price_for(0.0498765, "SELL") == Decimal("0.04988")
    assert f.price_for(0.0501235, "BUY") == Decimal("0.05012")
    assert f.check(Decimal("100"), Decimal("0.05")) is None  # exactly 5 USDT is allowed
    assert f.check(Decimal("99"), Decimal("0.05")) is not None  # 4.95 < 5
    assert f.check(Decimal("0"), Decimal("0.05")) == "quantity rounds to zero"
    assert f.check(Decimal("200"), Decimal("0.050005")) is not None  # off tick
    assert f.check(Decimal("200"), Decimal("0.05")) is None


def test_min_notional_legacy_filter():
    info = dict(INFO)
    info["filters"] = INFO["filters"][:2] + [{"filterType": "MIN_NOTIONAL", "minNotional": "10", "applyToMarket": True}]
    f = SymbolFilters.from_exchange_info(info)
    assert f.min_notional == Decimal("10")
