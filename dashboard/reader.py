"""Read-only access to the bot's JSON / JSONL files with per-file caching and downsampling.

Daily files are parsed once per (file, stride) and re-parsed only when the file changes
(size/mtime). Downsampling picks every ``stride``-th minute straight from the ISO ``ts`` string
(always the first key of every record), so skipped lines are never JSON-decoded.
"""

from __future__ import annotations

import json
import math
import os
import re
import threading
from pathlib import Path
from typing import Any, Callable

from bot.storage import iter_jsonl, read_json, tail_jsonl
from bot.util import DAY_MS, MINUTE_MS, iso_to_ms, ms_to_date

NICE_STRIDES = (1, 2, 5, 10, 15, 30, 60, 120, 240, 360, 720, 1440)
RANGES_MIN = {"24h": 1440, "7d": 7 * 1440, "30d": 30 * 1440}
DATE_RE = re.compile(r"_(\d{4}-\d{2}-\d{2})\.jsonl$")
TS_PREFIX = '{"ts":"'


def pick_stride(span_min: float, max_points: int = 1500, base: int = 1) -> int:
    need = max(base, math.ceil(span_min / max_points))
    for s in NICE_STRIDES:
        if s >= need and s % base == 0:
            return s
    return 1440


def _minute_of_day(line: str) -> int | None:
    # '{"ts":"2026-10-07T12:05:00Z", ...' -> hour at [18:20], minute at [21:23]
    if not line.startswith(TS_PREFIX):
        return None
    try:
        return int(line[18:20]) * 60 + int(line[21:23])
    except ValueError:
        return None


class FileCache:
    def __init__(self, max_entries: int = 4000) -> None:
        self._data: dict[tuple[str, int, str], tuple[tuple[float, int], list]] = {}
        self._lock = threading.Lock()
        self.max_entries = max_entries

    def get(self, path: Path, stride: int, tag: str, parse: Callable[[dict], Any]) -> list:
        try:
            st = os.stat(path)
        except FileNotFoundError:
            return []
        sig = (st.st_mtime, st.st_size)
        key = (str(path), stride, tag)
        with self._lock:
            hit = self._data.get(key)
            if hit and hit[0] == sig:
                return hit[1]
        out = []
        with open(path, encoding="utf-8") as f:
            for line in f:
                if stride > 1:
                    m = _minute_of_day(line)
                    if m is None or m % stride:
                        continue
                try:
                    rec = json.loads(line)
                except json.JSONDecodeError:
                    continue
                v = parse(rec)
                if v is not None:
                    out.append(v)
        with self._lock:
            if len(self._data) >= self.max_entries:
                self._data.clear()
            self._data[key] = (sig, out)
        return out


class DataReader:
    def __init__(self, root: Path) -> None:
        self.root = Path(root)
        self.cache = FileCache()

    # ---- basics -----------------------------------------------------------------------------
    def instances(self) -> list[str]:
        d = self.root / "instances"
        if not d.exists():
            return []
        return sorted(p.name for p in d.iterdir() if p.is_dir())

    def summary(self, inst: str) -> dict | None:
        return read_json(self.root / "instances" / inst / "summary.json")

    def runtime(self) -> dict:
        return read_json(self.root / "runtime.json", {}) or {}

    def daily_files(self, directory: Path, prefix: str) -> list[tuple[str, Path]]:
        if not directory.exists():
            return []
        out = []
        for p in directory.glob(f"{prefix}_*.jsonl"):
            m = DATE_RE.search(p.name)
            if m:
                out.append((m.group(1), p))
        return sorted(out)

    def last_bar_ms(self) -> int | None:
        rt = self.runtime()
        ts = (rt.get("ws") or {}).get("last_bar_ts")
        if ts:
            return iso_to_ms(ts)
        files = self.daily_files(self.root / "market", "bars")
        if files:
            tail = tail_jsonl(files[-1][1], 1)
            if tail:
                return iso_to_ms(tail[-1]["ts"])
        return None

    def _first_ts(self, directory: Path, prefix: str) -> int | None:
        files = self.daily_files(directory, prefix)
        if not files:
            return None
        with open(files[0][1], encoding="utf-8") as f:
            line = f.readline()
        try:
            return iso_to_ms(json.loads(line)["ts"])
        except (ValueError, KeyError):
            return None

    def first_bar_ms(self) -> int | None:
        return self._first_ts(self.root / "market", "bars")

    def first_equity_ms(self, inst: str) -> int | None:
        return self._first_ts(self.root / "instances" / inst, "equity")

    def window(self, rng: str) -> tuple[int, int] | None:
        """(start_ms, end_ms) ending at the latest bar (so replays display correctly)."""
        end = self.last_bar_ms()
        if end is None:
            return None
        end += MINUTE_MS
        if rng in RANGES_MIN:
            return end - RANGES_MIN[rng] * MINUTE_MS, end
        files = self.daily_files(self.root / "market", "bars")
        start = iso_to_ms(files[0][0] + "T00:00:00Z") if files else end - DAY_MS
        return start, end

    def _files_in(self, directory: Path, prefix: str, start: int, end: int) -> list[Path]:
        d0, d1 = ms_to_date(start), ms_to_date(end)
        return [p for d, p in self.daily_files(directory, prefix) if d0 <= d <= d1]

    # ---- series -------------------------------------------------------------------------------
    def bars(self, start: int, end: int, stride: int) -> list[tuple[int, float, float | None, bool]]:
        def parse(r: dict) -> tuple | None:
            ratio = r.get("ratio")
            if ratio is None:
                return None
            ema = r.get("ema3d_ratio")
            if ema is None and r.get("ema3d") is not None:
                ema = math.exp(r["ema3d"])
            return (iso_to_ms(r["ts"]), ratio, ema, bool(r.get("stale")))

        out = []
        for p in self._files_in(self.root / "market", "bars", start, end):
            out.extend(x for x in self.cache.get(p, stride, "bars", parse) if start <= x[0] <= end)
        return out

    def equity(self, inst: str, start: int, end: int, stride: int) -> list[dict]:
        def parse(r: dict) -> dict | None:
            return {"t": iso_to_ms(r["ts"]), "v": r.get("variant"), "excess": r.get("excess"), "w": r.get("w"),
                    "wt": r.get("w_target"), "value": r.get("value_usd")}

        out = []
        for p in self._files_in(self.root / "instances" / inst, "equity", start, end):
            out.extend(x for x in self.cache.get(p, max(stride, 5), "equity", parse) if start <= x["t"] <= end)
        return out

    def jsonl(self, inst: str | None, name: str) -> list[dict]:
        p = (self.root / "instances" / inst / f"{name}.jsonl") if inst else (self.root / f"{name}.jsonl")
        return self.cache.get(p, 1, "raw", lambda r: r)

    def tail(self, inst: str | None, name: str, n: int) -> list[dict]:
        p = (self.root / "instances" / inst / f"{name}.jsonl") if inst else (self.root / f"{name}.jsonl")
        return tail_jsonl(p, n)

    def ladder(self, inst: str) -> list[dict]:
        return self.jsonl(inst, "ladder")

    def iter_orders(self, inst: str) -> list[dict]:
        return self.jsonl(inst, "orders")
