"""Time helpers and clocks. All timestamps are UTC, internally epoch milliseconds."""

from __future__ import annotations

import time
from datetime import date, datetime, timedelta, timezone

MINUTE_MS = 60_000
DAY_MS = 86_400_000


def now_ms() -> int:
    return int(time.time() * 1000)


def ms_to_dt(ms: int) -> datetime:
    return datetime.fromtimestamp(ms / 1000, tz=timezone.utc)


_DATE_CACHE: dict[int, str] = {}


def ms_to_iso(ms: int | None) -> str | None:
    """ISO-8601 UTC with a trailing Z. Milliseconds are only shown when non-zero."""
    if ms is None:
        return None
    day, rem = divmod(int(ms), DAY_MS)
    secs, frac = divmod(rem, 1000)
    h, r = divmod(secs, 3600)
    m, s = divmod(r, 60)
    base = f"{_day_str(day)}T{h:02d}:{m:02d}:{s:02d}"
    return f"{base}.{frac:03d}Z" if frac else f"{base}Z"


def _day_str(day: int) -> str:
    s = _DATE_CACHE.get(day)
    if s is None:
        s = datetime.fromtimestamp(day * 86_400, tz=timezone.utc).strftime("%Y-%m-%d")
        _DATE_CACHE[day] = s
    return s


def iso_to_ms(s: str) -> int:
    if s.endswith("Z"):
        s = s[:-1] + "+00:00"
    dt = datetime.fromisoformat(s)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return int(round(dt.timestamp() * 1000))


def ms_to_date(ms: int) -> str:
    return _day_str(int(ms) // DAY_MS)


def date_to_ms(d: str) -> int:
    return int(datetime.strptime(d, "%Y-%m-%d").replace(tzinfo=timezone.utc).timestamp() * 1000)


def next_date(d: str) -> str:
    return (datetime.strptime(d, "%Y-%m-%d").date() + timedelta(days=1)).isoformat()


def floor_minute(ms: int) -> int:
    return ms - (ms % MINUTE_MS)


def minute_of_day(ms: int) -> int:
    return (ms % DAY_MS) // MINUTE_MS


def year_of(ms: int) -> int:
    return datetime.fromtimestamp(ms // 1000, tz=timezone.utc).year


def year_start_ms(year: int) -> int:
    return int(datetime(year, 1, 1, tzinfo=timezone.utc).timestamp() * 1000)


def today_utc() -> date:
    return datetime.now(timezone.utc).date()


class Clock:
    """Wall clock. Replays swap in :class:`SimClock` so every component sees simulated time."""

    def now_ms(self) -> int:
        return now_ms()


class SimClock(Clock):
    def __init__(self, start_ms: int = 0) -> None:
        self._now = start_ms

    def set(self, ms: int) -> None:
        self._now = int(ms)

    def now_ms(self) -> int:
        return self._now
