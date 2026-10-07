import pytest

from bot.config import LIVE_CONFIRM_PHRASE, ConfigError, live_config_problems, load_settings


def test_defaults(settings_factory):
    s = settings_factory()
    assert not s.binance_testnet and not s.kill_switch
    s1k, s4k = s.instance("s1k"), s.instance("s4k")
    assert s1k.capital_usd == 1000 and s4k.capital_usd == 4000
    st = s1k.strategy
    assert (st.overlay_ema_span_min, st.overlay_entry, st.overlay_exit, st.overlay_ticket) == (4320, 0.06, 0.002, 0.25)
    assert st.ladder_tiers == ((0.15, 0.15), (0.30, 0.30)) and st.ladder_exit == 0.05
    assert st.effective_overlay_warmup_bars == 3 * 4320
    assert (st.w_min, st.w_max) == (0.05, 0.95)
    assert s1k.execution.max_slice_usd == 500 and s1k.risk.max_order_usd == 1500
    assert s.rest_url == "https://api.binance.com"


def test_testnet_urls(settings_factory):
    s = settings_factory(BINANCE_TESTNET="true")
    assert s.rest_url == "https://testnet.binance.vision"
    assert s.ws_url.startswith("wss://stream.testnet.binance.vision")


def test_per_instance_override(settings_factory):
    s = settings_factory(OVERLAY_ENTRY="0.07", S1K_OVERLAY_ENTRY="0.05", S4K_MAX_SLICE_USD="250")
    assert s.instance("s1k").strategy.overlay_entry == 0.05
    assert s.instance("s4k").strategy.overlay_entry == 0.07
    assert s.instance("s4k").execution.max_slice_usd == 250
    assert s.instance("s1k").execution.max_slice_usd == 500


def test_live_requires_confirmation_and_keys(settings_factory):
    s = settings_factory(S1K_LIVE="true", S1K_API_KEY="k1", S1K_API_SECRET="x1")
    assert not s.live_allowed("s1k")  # LIVE_CONFIRM missing
    s = settings_factory(S1K_LIVE="true", S1K_API_KEY="k1", S1K_API_SECRET="x1", LIVE_CONFIRM=LIVE_CONFIRM_PHRASE)
    assert s.live_allowed("s1k") and not s.live_allowed("s4k")
    s = settings_factory(S1K_LIVE="true", LIVE_CONFIRM=LIVE_CONFIRM_PHRASE)
    assert not s.live_allowed("s1k")
    assert any("API_KEY" in p for p in live_config_problems(s))


def test_same_key_for_both_live_instances_is_refused(settings_factory):
    s = settings_factory(
        LIVE_CONFIRM=LIVE_CONFIRM_PHRASE,
        S1K_LIVE="true", S1K_API_KEY="same", S1K_API_SECRET="a",
        S4K_LIVE="true", S4K_API_KEY="same", S4K_API_SECRET="b",
    )
    assert any("same API key" in p for p in live_config_problems(s))


def test_secrets_never_in_repr_or_summary(settings_factory):
    s = settings_factory(S1K_API_KEY="AKIAVERYSECRETKEY123", S1K_API_SECRET="topsecretvalue")
    text = repr(s) + str(s.public_summary())
    assert "topsecretvalue" not in text and "AKIAVERYSECRETKEY123" not in text


def test_invalid_values_raise(settings_factory):
    with pytest.raises(ConfigError):
        settings_factory(OVERLAY_ENTRY="abc")
    with pytest.raises(ConfigError):
        settings_factory(W_MIN="0.9", W_MAX="0.1")
    with pytest.raises(ConfigError):
        settings_factory(LADDER_TIERS="0.15")


def test_env_file_and_process_env_precedence(tmp_path):
    env = tmp_path / ".env"
    env.write_text("OVERLAY_ENTRY=0.08\nKILL_SWITCH=true\n")
    s = load_settings(env, environ={"DATA_DIR": str(tmp_path)})
    assert s.instance("s1k").strategy.overlay_entry == 0.08 and s.kill_switch
    s = load_settings(env, environ={"DATA_DIR": str(tmp_path), "OVERLAY_ENTRY": "0.09"})
    assert s.instance("s1k").strategy.overlay_entry == 0.09
