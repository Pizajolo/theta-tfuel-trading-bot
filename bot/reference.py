"""Backtest reference ranges (combined strategy, 3-day EMA 6% + full ladder).

Pessimistic (worst-of-bar fills) .. optimistic (close fills), 0.1% fee per leg plus market
impact. 2026 is year-to-date (the backtest data ends in October 2026).
"""

from __future__ import annotations

BACKTEST_REFERENCE: dict[str, dict[int, tuple[float, float]]] = {
    "s1k": {2023: (0.10, 0.16), 2024: (0.26, 0.49), 2025: (0.16, 0.47), 2026: (0.14, 0.22)},
    "s4k": {2023: (0.09, 0.15), 2024: (0.22, 0.44), 2025: (0.14, 0.44), 2026: (0.09, 0.17)},
}

# Length of each reference period in days (2026 is YTD to roughly 7 Oct 2026).
REFERENCE_DAYS: dict[int, float] = {2023: 365.0, 2024: 366.0, 2025: 365.0, 2026: 280.0}

REFERENCE_NOTE = (
    "Backtest 2023..Oct 2026 on Binance 1m data: pessimistic (worst-of-bar) .. optimistic (close fills), "
    "0.1% fee per leg plus market impact. 2026 = YTD."
)


def reference_range(instance: str, year: int) -> tuple[float, float] | None:
    return BACKTEST_REFERENCE.get(instance, {}).get(year)


def prorated_range(instance: str, year: int, days_covered: float) -> tuple[float, float, str] | None:
    """Reference range scaled (compounded) to ``days_covered`` days of the given year.

    Years without a reference use the average of the full 2023-2025 years.
    """
    ref = reference_range(instance, year)
    ref_days = REFERENCE_DAYS.get(year, 365.0)
    label = f"{year}" + (" YTD" if year == 2026 else "")
    if ref is None:
        full = [BACKTEST_REFERENCE.get(instance, {}).get(y) for y in (2023, 2024, 2025)]
        full = [r for r in full if r]
        if not full:
            return None
        ref = (sum(r[0] for r in full) / len(full), sum(r[1] for r in full) / len(full))
        ref_days = 365.0
        label = "avg 2023-2025"
    f = max(0.0, min(days_covered / ref_days, 1.5))
    lo, hi = ((1 + ref[0]) ** f - 1, (1 + ref[1]) ** f - 1)
    return lo, hi, label
