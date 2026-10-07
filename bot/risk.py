"""Hard risk limits for live trading.

``MAX_ORDER_USD`` (per order notional), ``MAX_TRADES_PER_DAY`` (orders with fills per UTC day)
and ``MAX_DAILY_TURNOVER_PCT`` (filled notional per UTC day, relative to the portfolio value at
the first check of the day). An order that would breach any limit is not sent and blocks all
further live orders until the next UTC day.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import Any

from bot.config import RiskParams
from bot.util import ms_to_date


@dataclass
class RiskState:
    date: str | None = None
    trades: int = 0
    turnover_usd: float = 0.0
    day_start_value_usd: float = 0.0
    blocked: bool = False
    block_reason: str | None = None


class RiskManager:
    def __init__(self, params: RiskParams, state: dict[str, Any] | None = None) -> None:
        self.params = params
        self.state = RiskState(**state) if state else RiskState()

    def to_dict(self) -> dict[str, Any]:
        return asdict(self.state)

    def roll(self, now_ms: int, portfolio_value_usd: float) -> None:
        d = ms_to_date(now_ms)
        if self.state.date != d:
            self.state = RiskState(date=d, day_start_value_usd=max(portfolio_value_usd, 0.0))
        elif self.state.day_start_value_usd <= 0 and portfolio_value_usd > 0:
            self.state.day_start_value_usd = portfolio_value_usd

    @property
    def blocked(self) -> bool:
        return self.state.blocked

    def blocked_today(self, now_ms: int) -> bool:
        """True while today's block is in force (the next UTC day lifts it)."""
        return self.state.blocked and self.state.date == ms_to_date(now_ms)

    def turnover_limit_usd(self) -> float:
        return self.params.max_daily_turnover_pct / 100.0 * self.state.day_start_value_usd

    def check_order(self, notional_usd: float, now_ms: int, portfolio_value_usd: float) -> str | None:
        """Pre-trade check. Returns ``None`` if allowed, else the reason (and blocks the day)."""
        self.roll(now_ms, portfolio_value_usd)
        if self.state.blocked:
            return self.state.block_reason or "blocked"
        reason = None
        if notional_usd > self.params.max_order_usd:
            reason = f"order notional {notional_usd:.2f} USD > MAX_ORDER_USD {self.params.max_order_usd:.2f}"
        elif self.state.trades + 1 > self.params.max_trades_per_day:
            reason = f"MAX_TRADES_PER_DAY {self.params.max_trades_per_day} reached"
        elif self.state.day_start_value_usd > 0 and self.state.turnover_usd + notional_usd > self.turnover_limit_usd():
            reason = (
                f"daily turnover {self.state.turnover_usd + notional_usd:.2f} USD would exceed "
                f"MAX_DAILY_TURNOVER_PCT {self.params.max_daily_turnover_pct:g}% of {self.state.day_start_value_usd:.2f}"
            )
        if reason:
            self.state.blocked = True
            self.state.block_reason = reason
        return reason

    def record_fill(self, notional_usd: float) -> None:
        if notional_usd > 0:
            self.state.trades += 1
            self.state.turnover_usd += notional_usd
