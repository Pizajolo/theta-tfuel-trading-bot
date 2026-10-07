"""Configuration loaded from ``.env`` (plus process environment) with typed defaults.

Precedence: process environment > ``.env`` file > built-in defaults. The bot re-reads
the ``.env`` file every 60 seconds (see ``bot.bot``); values pinned in the process
environment therefore cannot be hot-reloaded.

Per-instance overrides use the instance prefix, e.g. ``S1K_OVERLAY_ENTRY=0.05`` overrides
``OVERLAY_ENTRY`` for ``s1k`` only. Any strategy, execution or risk key can be overridden.
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field, fields, replace
from pathlib import Path
from typing import Any, Mapping

from dotenv import dotenv_values

LIVE_CONFIRM_PHRASE = "I_UNDERSTAND_REAL_MONEY"
INSTANCE_NAMES = ("s1k", "s4k")
DEFAULT_CAPITAL = {"s1k": 1000.0, "s4k": 4000.0}

MAINNET_REST = "https://api.binance.com"
MAINNET_WS = "wss://stream.binance.com:9443"
TESTNET_REST = "https://testnet.binance.vision"
TESTNET_WS = "wss://stream.testnet.binance.vision:9443"


class ConfigError(ValueError):
    pass


def _parse_bool(v: str) -> bool:
    s = str(v).strip().lower()
    if s in ("1", "true", "yes", "on", "y"):
        return True
    if s in ("0", "false", "no", "off", "n", ""):
        return False
    raise ValueError(f"not a boolean: {v!r}")


def parse_tiers(v: str) -> tuple[tuple[float, float], ...]:
    """``"0.15:0.15,0.30:0.30"`` -> ((0.15, 0.15), (0.30, 0.30)), sorted by threshold."""
    tiers = []
    for part in str(v).split(","):
        part = part.strip()
        if not part:
            continue
        th, _, shift = part.partition(":")
        if not shift:
            raise ValueError(f"tier {part!r} must be threshold:shift")
        tiers.append((float(th), float(shift)))
    if not tiers:
        raise ValueError("LADDER_TIERS is empty")
    return tuple(sorted(tiers))


def format_tiers(tiers: tuple[tuple[float, float], ...]) -> str:
    return ",".join(f"{th:g}:{sh:g}" for th, sh in tiers)


@dataclass(frozen=True)
class StrategyParams:
    overlay_ema_span_min: int = 4320
    overlay_entry: float = 0.06
    overlay_exit: float = 0.002
    overlay_ticket: float = 0.25
    overlay_max_hold_min: int = 4320
    # Warm-up: the spec requires at least 3 x span bars before trading. 0 = use 3 x span.
    overlay_warmup_bars: int = 0
    ladder_enabled: bool = True
    ladder_ema_span_days: int = 60
    ladder_tiers: tuple[tuple[float, float], ...] = ((0.15, 0.15), (0.30, 0.30))
    ladder_exit: float = 0.05
    # Warm-up: the spec requires at least 180 daily closes before the ladder may move.
    ladder_warmup_days: int = 180
    w_min: float = 0.05
    w_max: float = 0.95

    @property
    def effective_overlay_warmup_bars(self) -> int:
        return self.overlay_warmup_bars if self.overlay_warmup_bars > 0 else 3 * self.overlay_ema_span_min


@dataclass(frozen=True)
class ExecutionParams:
    max_slippage: float = 0.003
    max_slice_usd: float = 500.0
    slice_interval_sec: float = 30.0
    order_timeout_sec: float = 300.0
    min_trade_usd: float = 10.0
    max_retries: int = 10
    use_bnb_fees: bool = True
    bnb_min_usd: float = 5.0
    fee_rate: float = 0.001
    # OPTIONAL (not part of the strategy spec, default off): extra simulated market impact for
    # paper fills, in basis points per 1,000 USD of trade notional.
    paper_impact_bps_per_1k: float = 0.0


@dataclass(frozen=True)
class RiskParams:
    max_order_usd: float = 1500.0
    max_trades_per_day: int = 30
    max_daily_turnover_pct: float = 300.0


@dataclass(frozen=True)
class InstanceConfig:
    name: str
    enabled: bool
    capital_usd: float
    live_requested: bool
    api_key: str = field(default="", repr=False)
    api_secret: str = field(default="", repr=False)
    strategy: StrategyParams = StrategyParams()
    execution: ExecutionParams = ExecutionParams()
    risk: RiskParams = RiskParams()

    @property
    def has_credentials(self) -> bool:
        return bool(self.api_key and self.api_secret)

    @property
    def prefix(self) -> str:
        return self.name.upper()


@dataclass(frozen=True)
class Settings:
    binance_testnet: bool
    live_confirm: str
    kill_switch: bool
    data_dir: Path
    dashboard_host: str
    dashboard_port: int
    log_level: str
    instances: tuple[InstanceConfig, ...]
    strategy: StrategyParams
    execution: ExecutionParams
    risk: RiskParams
    rest_url: str
    ws_url: str
    env_file: Path | None = None

    @property
    def live_confirmed(self) -> bool:
        return self.live_confirm == LIVE_CONFIRM_PHRASE

    def instance(self, name: str) -> InstanceConfig:
        for inst in self.instances:
            if inst.name == name:
                return inst
        raise KeyError(name)

    @property
    def enabled_instances(self) -> tuple[InstanceConfig, ...]:
        return tuple(i for i in self.instances if i.enabled)

    def live_allowed(self, name: str) -> bool:
        """Configured for live: instance flag + global confirmation + credentials.

        The kill switch is evaluated separately at runtime (it also covers ``data/KILL``).
        """
        inst = self.instance(name)
        return inst.enabled and inst.live_requested and self.live_confirmed and inst.has_credentials

    def kill_file(self) -> Path:
        return self.data_dir / "KILL"

    def public_summary(self) -> dict[str, Any]:
        """Config snapshot that is safe to log (no secrets)."""
        return {
            "binance_testnet": self.binance_testnet,
            "live_confirmed": self.live_confirmed,
            "kill_switch": self.kill_switch,
            "data_dir": str(self.data_dir),
            "rest_url": self.rest_url,
            "ws_url": self.ws_url,
            "instances": {
                i.name: {
                    "enabled": i.enabled,
                    "capital_usd": i.capital_usd,
                    "live_requested": i.live_requested,
                    "has_credentials": i.has_credentials,
                    "api_key_hint": mask_secret(i.api_key),
                    "strategy": _params_dict(i.strategy),
                    "execution": _params_dict(i.execution),
                    "risk": _params_dict(i.risk),
                }
                for i in self.instances
            },
        }


def mask_secret(s: str) -> str:
    if not s:
        return ""
    if len(s) <= 8:
        return "****"
    return f"{s[:4]}…{s[-4:]}"


def _params_dict(p: Any) -> dict[str, Any]:
    out = {}
    for f in fields(p):
        v = getattr(p, f.name)
        out[f.name] = format_tiers(v) if f.name == "ladder_tiers" else v
    return out


def _convert(name: str, raw: str, default: Any) -> Any:
    try:
        if name == "ladder_tiers":
            return parse_tiers(raw)
        if isinstance(default, bool):
            return _parse_bool(raw)
        if isinstance(default, int):
            return int(float(raw))
        if isinstance(default, float):
            return float(raw)
        return str(raw)
    except ValueError as exc:
        raise ConfigError(f"invalid value for {name.upper()}: {raw!r} ({exc})") from exc


def _build_params(cls: type, env: Mapping[str, str], prefix: str = "", base: Any = None) -> Any:
    base = base if base is not None else cls()
    changes = {}
    for f in fields(cls):
        key = (prefix + f.name).upper()
        raw = env.get(key)
        if raw is None or str(raw).strip() == "":
            continue
        changes[f.name] = _convert(f.name, str(raw).strip(), getattr(base, f.name))
    return replace(base, **changes) if changes else base


def _validate_params(where: str, s: StrategyParams, e: ExecutionParams, r: RiskParams) -> None:
    problems = []
    if s.overlay_ema_span_min < 1:
        problems.append("OVERLAY_EMA_SPAN_MIN must be >= 1")
    if not (0 <= s.overlay_exit < s.overlay_entry):
        problems.append("need 0 <= OVERLAY_EXIT < OVERLAY_ENTRY")
    if not (0 <= s.overlay_ticket <= 0.5):
        problems.append("OVERLAY_TICKET must be within [0, 0.5]")
    if not (0.0 <= s.w_min < s.w_max <= 1.0):
        problems.append("need 0 <= W_MIN < W_MAX <= 1")
    if s.ladder_ema_span_days < 1:
        problems.append("LADDER_EMA_SPAN_DAYS must be >= 1")
    for th, sh in s.ladder_tiers:
        if th <= 0 or not (0 <= sh <= 0.5):
            problems.append(f"bad ladder tier {th}:{sh}")
    if e.max_slippage < 0 or e.max_slippage > 0.05:
        problems.append("MAX_SLIPPAGE must be within [0, 0.05]")
    if e.max_slice_usd <= 0 or e.min_trade_usd < 0:
        problems.append("MAX_SLICE_USD must be > 0 and MIN_TRADE_USD >= 0")
    if e.max_retries < 0:
        problems.append("MAX_RETRIES must be >= 0")
    if r.max_order_usd <= 0 or r.max_trades_per_day <= 0 or r.max_daily_turnover_pct <= 0:
        problems.append("risk limits must be positive")
    if problems:
        raise ConfigError(f"{where}: " + "; ".join(problems))


def load_settings(env_file: str | Path | None = ".env", environ: Mapping[str, str] | None = None) -> Settings:
    file_values: dict[str, str] = {}
    env_path = Path(env_file) if env_file else None
    if env_path is not None and env_path.exists():
        # python-dotenv returns the comment text for "KEY=   # comment"; treat that as empty.
        file_values = {
            k: ("" if v.lstrip().startswith("#") else v) for k, v in dotenv_values(env_path).items() if v is not None
        }
    proc = dict(os.environ if environ is None else environ)
    env: dict[str, str] = {**file_values, **{k: v for k, v in proc.items() if v is not None}}

    def get(key: str, default: str = "") -> str:
        v = env.get(key)
        return default if v is None else str(v).strip()

    try:
        testnet = _parse_bool(get("BINANCE_TESTNET", "false"))
        kill = _parse_bool(get("KILL_SWITCH", "false"))
        port = int(get("DASHBOARD_PORT", "8050"))
    except ValueError as exc:
        raise ConfigError(str(exc)) from exc

    strategy = _build_params(StrategyParams, env)
    execution = _build_params(ExecutionParams, env)
    risk = _build_params(RiskParams, env)
    _validate_params("global", strategy, execution, risk)

    instances = []
    for name in INSTANCE_NAMES:
        p = name.upper() + "_"
        try:
            enabled = _parse_bool(get(p + "ENABLED", "true"))
            live = _parse_bool(get(p + "LIVE", "false"))
            capital = float(get(p + "CAPITAL_USD", str(DEFAULT_CAPITAL[name])))
        except ValueError as exc:
            raise ConfigError(f"{name}: {exc}") from exc
        if capital <= 0:
            raise ConfigError(f"{p}CAPITAL_USD must be > 0")
        s = _build_params(StrategyParams, env, p, strategy)
        e = _build_params(ExecutionParams, env, p, execution)
        r = _build_params(RiskParams, env, p, risk)
        _validate_params(name, s, e, r)
        instances.append(
            InstanceConfig(
                name=name,
                enabled=enabled,
                capital_usd=capital,
                live_requested=live,
                api_key=get(p + "API_KEY"),
                api_secret=get(p + "API_SECRET"),
                strategy=s,
                execution=e,
                risk=r,
            )
        )

    rest_default, ws_default = (TESTNET_REST, TESTNET_WS) if testnet else (MAINNET_REST, MAINNET_WS)
    return Settings(
        binance_testnet=testnet,
        live_confirm=get("LIVE_CONFIRM"),
        kill_switch=kill,
        data_dir=Path(get("DATA_DIR", "./data")),
        dashboard_host=get("DASHBOARD_HOST", "127.0.0.1"),
        dashboard_port=port,
        log_level=get("LOG_LEVEL", "INFO").upper(),
        instances=tuple(instances),
        strategy=strategy,
        execution=execution,
        risk=risk,
        # Development/testing only: point the bot at a local fake exchange (tools/fake_binance.py).
        rest_url=get("BINANCE_REST_URL", rest_default).rstrip("/"),
        ws_url=get("BINANCE_WS_URL", ws_default).rstrip("/"),
        env_file=env_path,
    )


def live_config_problems(settings: Settings) -> list[str]:
    """Hard errors that must stop the bot from starting at all."""
    problems = []
    live = [i for i in settings.enabled_instances if i.live_requested]
    if live and not settings.live_confirmed:
        # Not fatal: instances simply stay in paper mode. Reported as a warning by the bot.
        pass
    keys = {}
    for inst in live:
        if not inst.has_credentials:
            problems.append(f"{inst.name}: {inst.prefix}_LIVE=true but {inst.prefix}_API_KEY/SECRET is empty")
            continue
        if inst.api_key in keys:
            problems.append(
                f"{inst.name} and {keys[inst.api_key]} use the same API key; each live instance needs its own "
                "Binance sub-account key"
            )
        keys[inst.api_key] = inst.name
    return problems


def strategy_changed(a: Settings, b: Settings) -> list[str]:
    """Names of instances whose strategy parameters differ (these need a restart)."""
    out = []
    for ia in a.instances:
        try:
            ib = b.instance(ia.name)
        except KeyError:
            continue
        if ia.strategy != ib.strategy or ia.capital_usd != ib.capital_usd or ia.api_key != ib.api_key or ia.api_secret != ib.api_secret:
            out.append(ia.name)
    return out
