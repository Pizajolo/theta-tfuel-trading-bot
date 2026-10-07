"""Synthetic THETA/TFUEL 1m data with a mean-reverting ratio (for tests and demos only).

    python -m tools.synthetic --days 30 --out theta_tfuel_1m_synthetic.csv
    python -m tools.synthetic --days 10 --out theta_tfuel_1m_synthetic.xlsx

The output has the same columns as the user's ``theta_tfuel_1m_*.xlsx`` files.
"""

from __future__ import annotations

import argparse
import csv
import math
import random
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterator

HEADER = [
    "time",
    "THETA_open", "THETA_high", "THETA_low", "THETA_close", "THETA_volume",
    "TFUEL_open", "TFUEL_high", "TFUEL_low", "TFUEL_close", "TFUEL_volume",
    "THETA_per_TFUEL",
]


class RatioModel:
    """THETA follows a random walk; ln(THETA/TFUEL) = slow OU swing + fast OU dislocations."""

    def __init__(self, seed: int = 7, theta0: float = 1.0, ratio0: float = 20.0) -> None:
        self.rng = random.Random(seed)
        self.theta = theta0
        self.mu0 = math.log(ratio0)
        self.slow = 0.0  # multi-week swing around mu0
        self.fast = 0.0  # intraday dislocation
        self.tfuel = theta0 / ratio0

    def step(self) -> tuple[float, float]:
        r = self.rng
        self.theta *= math.exp(r.gauss(0.0, 0.0012))
        self.slow += -0.00002 * self.slow + r.gauss(0.0, 0.0009)
        self.fast += -0.004 * self.fast + r.gauss(0.0, 0.0015)
        if r.random() < 1 / 1500:  # THETA moves fast, TFUEL lags
            self.fast += r.choice((-1, 1)) * r.uniform(0.05, 0.11)
        lr = self.mu0 + self.slow + self.fast
        self.tfuel = self.theta / math.exp(lr)
        return self.theta, self.tfuel


def generate(start_ms: int, minutes: int, seed: int = 7, stale_prob: float = 0.002) -> Iterator[list]:
    m = RatioModel(seed)
    r = random.Random(seed + 1)
    prev_th, prev_tf = m.theta, m.tfuel
    for i in range(minutes):
        th, tf = m.step()
        rows = []
        for o, c in ((prev_th, th), (prev_tf, tf)):
            hi = max(o, c) * (1 + abs(r.gauss(0, 0.0006)))
            lo = min(o, c) * (1 - abs(r.gauss(0, 0.0006)))
            v = 0.0 if r.random() < stale_prob else round(r.uniform(500, 50000), 2)
            if v == 0.0:
                o = hi = lo = c = o
            rows.append((o, hi, lo, c, v))
        (to, th_, tl, tc, tv), (fo, fh, fl, fc, fv) = rows
        prev_th, prev_tf = tc, fc
        ts = start_ms + i * 60_000
        yield [ts, to, th_, tl, tc, tv, fo, fh, fl, fc, fv, tc / fc]


def write_csv(path: Path, start_ms: int, minutes: int, seed: int = 7) -> None:
    with open(path, "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(HEADER)
        for row in generate(start_ms, minutes, seed):
            ts = datetime.fromtimestamp(row[0] / 1000, tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
            w.writerow([ts] + [f"{x:.8g}" for x in row[1:]])


def write_xlsx(path: Path, start_ms: int, minutes: int, seed: int = 7, rows_per_sheet: int = 1_000_000) -> None:
    from openpyxl import Workbook

    wb = Workbook(write_only=True)
    ws = None
    n = 0
    for row in generate(start_ms, minutes, seed):
        if ws is None or n >= rows_per_sheet:
            ws = wb.create_sheet(f"data{len(wb.worksheets) + 1}")
            ws.append(HEADER)
            n = 0
        ws.append([datetime.fromtimestamp(row[0] / 1000, tz=timezone.utc).replace(tzinfo=None)] + row[1:])
        n += 1
    wb.save(path)


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--out", required=True)
    ap.add_argument("--days", type=float, default=30)
    ap.add_argument("--start", default="2026-01-01")
    ap.add_argument("--seed", type=int, default=7)
    a = ap.parse_args()
    start = int(datetime.fromisoformat(a.start).replace(tzinfo=timezone.utc).timestamp() * 1000)
    minutes = int(a.days * 1440)
    out = Path(a.out)
    (write_xlsx if out.suffix.lower() == ".xlsx" else write_csv)(out, start, minutes, a.seed)
    print(f"wrote {minutes} rows to {out}")


if __name__ == "__main__":
    main()
