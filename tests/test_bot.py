import asyncio

import pytest

import bot.bot as botmod
from bot.config import LIVE_CONFIRM_PHRASE
from bot.instance import Instance
from bot.market_data import SYMBOLS
from bot.portfolio import Portfolio
from bot.storage import iter_jsonl
from bot.symbol_filters import parse_exchange_info
from tools.fake_exchange import FakeExchange

from .conftest import T0

LIVE = dict(LIVE_CONFIRM=LIVE_CONFIRM_PHRASE, S1K_LIVE="true", S1K_API_KEY="k1", S1K_API_SECRET="s1")


@pytest.fixture
def fake_clients(monkeypatch):
    exchanges: dict[str, FakeExchange] = {}

    def factory(base_url, api_key="", api_secret="", **kw):
        fx = exchanges.get(api_key)
        if fx is None:
            fx = FakeExchange(balances={"THETA": 500.0, "TFUEL": 10_000.0, "USDT": 10.0, "BNB": 0.05})
            exchanges[api_key] = fx
        return fx

    monkeypatch.setattr(botmod, "BinanceClient", factory)
    return exchanges


def make_bot(settings_factory, **env):
    s = settings_factory(**{**LIVE, **env})
    b = botmod.Bot(s)
    b.filters = parse_exchange_info(FakeExchange().exchange_info_dict(SYMBOLS), SYMBOLS)
    inst = Instance(s.instance("s1k"), b.storage, b.clock)
    b.instances["s1k"] = inst
    return b, inst


def events(b, kind):
    return [e for e in iter_jsonl(b.storage.events_path()) if e["kind"] == kind]


def test_refuses_to_start_with_shared_api_key(settings_factory, fake_clients):
    s = settings_factory(**LIVE, S4K_LIVE="true", S4K_API_KEY="k1", S4K_API_SECRET="other")
    assert asyncio.run(botmod.Bot(s).run()) == 2
    b = botmod.Bot(s)
    assert any("same API key" in e["message"] for e in events(b, "config_refused"))


def test_activation_uses_rest_prices_when_stream_has_none(settings_factory, fake_clients):
    b, inst = make_bot(settings_factory)
    assert b.current_px() is None  # no bars or book yet (e.g. right after a restart)
    asyncio.run(b._reconcile_one("s1k", inst))
    assert inst.live_active and inst.executor is not None
    assert inst.live_port.hodl["THETA"] == pytest.approx(500.0)
    assert not events(b, "live_refused")
    assert events(b, "live_activated")


def test_withdrawal_enabled_key_is_refused(settings_factory, fake_clients):
    b, inst = make_bot(settings_factory)
    fake_clients.setdefault("k1", FakeExchange()).withdrawals_enabled = True
    asyncio.run(b._reconcile_one("s1k", inst))
    assert not inst.live_active and inst.executor is None
    assert any("WITHDRAWALS" in e["message"] for e in events(b, "live_refused"))


def test_live_decision_taken_without_executor_is_requeued(settings_factory, fake_clients):
    b, inst = make_bot(settings_factory)
    # live period running, but the executor could not be attached when this decision was made
    inst.live_active = True
    inst.live_port = Portfolio.from_balances("live", {"THETA": 500.0, "TFUEL": 10_000.0}, T0)
    inst.last_decision = {"decision_id": "2601011200", "w_target": 0.75, "mode": "live"}

    async def go():
        await b._reconcile_one("s1k", inst)
        assert inst.executor.decision["id"] == "2601011200"
        assert inst.executor.decision["status"] == "new"
        assert inst.executor.pump(allowed=True) == "start"
        await inst.executor.task

    asyncio.run(go())
    assert events(b, "live_resync")
    fx = fake_clients["k1"]
    assert [o["clientOrderId"] for o in fx.order_log][:2] == ["s1k-2601011200-1-0", "s1k-2601011200-2-1"]


def test_kill_switch_halts_and_resumes_with_same_benchmark(settings_factory, fake_clients, tmp_path):
    b, inst = make_bot(settings_factory)
    asyncio.run(b._reconcile_one("s1k", inst))
    bench = dict(inst.live_port.hodl)
    b.kill_active = True
    asyncio.run(b._reconcile_one("s1k", inst))
    assert inst.live_suspended and events(b, "kill_switch")
    assert "cancel_open_orders THETAUSDT" in fake_clients["k1"].calls
    b.kill_active = False
    asyncio.run(b._reconcile_one("s1k", inst))
    assert not inst.live_suspended and inst.live_port.hodl == bench
