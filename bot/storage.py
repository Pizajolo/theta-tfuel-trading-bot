"""JSON / JSON-Lines persistence. These files are the single source of truth for the dashboard.

* Append-only streams are JSON Lines, rotated daily by the UTC date of the record's ``ts``.
* State files are written atomically (temp file + ``os.replace``).
* Every record carries ``ts`` (ISO-8601 UTC, always the first key), ``schema_version`` and,
  where relevant, ``instance``.
* Nothing is ever deleted automatically.
"""

from __future__ import annotations

import json
import logging
import math
import os
import threading
from collections import OrderedDict, deque
from pathlib import Path
from typing import Any, Callable, Iterable

from bot.util import Clock, ms_to_date, ms_to_iso

SCHEMA_VERSION = 1
log = logging.getLogger("bot.storage")


def _sanitize(obj: Any) -> Any:
    if isinstance(obj, float):
        return obj if math.isfinite(obj) else None
    if isinstance(obj, dict):
        return {k: _sanitize(v) for k, v in obj.items()}
    if isinstance(obj, (list, tuple)):
        return [_sanitize(v) for v in obj]
    return obj


def _default(obj: Any) -> Any:
    if isinstance(obj, Path):
        return str(obj)
    if hasattr(obj, "__float__"):
        return float(obj)
    raise TypeError(f"not JSON serializable: {type(obj).__name__}")


def dumps(obj: Any) -> str:
    """Compact JSON; NaN/inf become null so browsers can parse every line."""
    try:
        return json.dumps(obj, separators=(",", ":"), ensure_ascii=False, allow_nan=False, default=_default)
    except ValueError:
        return json.dumps(_sanitize(obj), separators=(",", ":"), ensure_ascii=False, default=_default)


def atomic_write_json(path: Path | str, obj: Any, indent: int | None = None) -> None:
    """Write JSON atomically: readers see either the old or the new file, never a partial one."""
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(f".{path.name}.{os.getpid()}.{threading.get_ident()}.tmp")
    try:
        text = json.dumps(_sanitize(obj), indent=indent, ensure_ascii=False, default=_default) if indent else dumps(obj)
        with open(tmp, "w", encoding="utf-8") as f:
            f.write(text)
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp, path)
    except BaseException:
        try:
            tmp.unlink()
        except FileNotFoundError:
            pass
        raise


def read_json(path: Path | str, default: Any = None) -> Any:
    try:
        with open(path, encoding="utf-8") as f:
            return json.load(f)
    except FileNotFoundError:
        return default
    except (json.JSONDecodeError, OSError) as exc:
        log.warning("could not read %s: %s", path, exc)
        return default


def iter_jsonl(path: Path | str) -> Iterable[dict]:
    """Yield records, skipping a torn last line (e.g. after a crash mid-write)."""
    try:
        with open(path, encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    yield json.loads(line)
                except json.JSONDecodeError:
                    continue
    except FileNotFoundError:
        return


def tail_jsonl(path: Path | str, n: int, block: int = 65536) -> list[dict]:
    """Last ``n`` records of a JSONL file without reading the whole file."""
    path = Path(path)
    if n <= 0 or not path.exists():
        return []
    with open(path, "rb") as f:
        f.seek(0, os.SEEK_END)
        end = f.tell()
        data = b""
        pos = end
        while pos > 0 and data.count(b"\n") <= n:
            step = min(block, pos)
            pos -= step
            f.seek(pos)
            data = f.read(step) + data
    out = []
    for raw in data.splitlines()[-(n + 1):]:
        raw = raw.strip()
        if not raw:
            continue
        try:
            out.append(json.loads(raw))
        except json.JSONDecodeError:
            continue
    return out[-n:]


class JsonlWriter:
    """Keeps a small LRU of open append handles. ``flush_each`` is on for live, off for replays."""

    def __init__(self, flush_each: bool = True, max_open: int = 32) -> None:
        self.flush_each = flush_each
        self.max_open = max_open
        self._handles: OrderedDict[str, Any] = OrderedDict()
        self._lock = threading.Lock()

    def append(self, path: str, line: str) -> None:
        with self._lock:
            fh = self._handles.get(path)
            if fh is None:
                os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
                fh = open(path, "a", encoding="utf-8")
                self._handles[path] = fh
                while len(self._handles) > self.max_open:
                    _, old = self._handles.popitem(last=False)
                    old.close()
            else:
                self._handles.move_to_end(path)
            fh.write(line + "\n")
            if self.flush_each:
                fh.flush()

    def flush(self) -> None:
        with self._lock:
            for fh in self._handles.values():
                fh.flush()

    def close(self) -> None:
        with self._lock:
            for fh in self._handles.values():
                fh.close()
            self._handles.clear()


class Storage:
    """All file locations under ``DATA_DIR`` and typed writers for each stream."""

    def __init__(self, data_dir: Path | str, clock: Clock | None = None, flush_each: bool = True) -> None:
        self.root = Path(data_dir)
        self.root.mkdir(parents=True, exist_ok=True)
        self.clock = clock or Clock()
        self.writer = JsonlWriter(flush_each=flush_each)
        self._error_times: deque[int] = deque()
        self._paths: dict[tuple[str, str, str], str] = {}
        self.event_listeners: list[Callable[[dict], None]] = []
        self.log_events = True

    # ---- paths -------------------------------------------------------------------------
    def market_dir(self) -> Path:
        return self.root / "market"

    def instance_dir(self, instance: str) -> Path:
        return self.root / "instances" / instance

    def state_path(self, instance: str) -> Path:
        return self.instance_dir(instance) / "state.json"

    def summary_path(self, instance: str) -> Path:
        return self.instance_dir(instance) / "summary.json"

    def market_state_path(self) -> Path:
        return self.market_dir() / "state.json"

    def runtime_path(self) -> Path:
        return self.root / "runtime.json"

    def events_path(self) -> Path:
        return self.root / "events.jsonl"

    # ---- records -----------------------------------------------------------------------
    def make_record(self, ts_ms: int | None = None, instance: str | None = None, **fields: Any) -> dict:
        ts_ms = self.clock.now_ms() if ts_ms is None else ts_ms
        rec: dict[str, Any] = {"ts": ms_to_iso(ts_ms), "schema_version": SCHEMA_VERSION}
        if instance is not None:
            rec["instance"] = instance
        rec.update(fields)
        return rec

    def _path(self, instance: str | None, name: str, date: str = "") -> str:
        key = (instance or "", name, date)
        p = self._paths.get(key)
        if p is None:
            base = self.instance_dir(instance) if instance else self.market_dir()
            p = str(base / (f"{name}_{date}.jsonl" if date else f"{name}.jsonl"))
            if len(self._paths) > 4096:
                self._paths.clear()
            self._paths[key] = p
        return p

    def _append(self, path: str, rec: dict) -> dict:
        self.writer.append(path, dumps(rec))
        return rec

    def write_bar(self, ts_ms: int, **fields: Any) -> dict:
        rec = self.make_record(ts_ms, **fields)
        return self._append(self._path(None, "bars", ms_to_date(ts_ms)), rec)

    def write_signal(self, instance: str, ts_ms: int, **fields: Any) -> dict:
        rec = self.make_record(ts_ms, instance, **fields)
        return self._append(self._path(instance, "signals", ms_to_date(ts_ms)), rec)

    def write_decision(self, instance: str, ts_ms: int, **fields: Any) -> dict:
        rec = self.make_record(ts_ms, instance, **fields)
        return self._append(self._path(instance, "decisions"), rec)

    def write_order(self, instance: str, ts_ms: int | None = None, **fields: Any) -> dict:
        rec = self.make_record(ts_ms, instance, **fields)
        return self._append(self._path(instance, "orders"), rec)

    def write_equity(self, instance: str, ts_ms: int, **fields: Any) -> dict:
        rec = self.make_record(ts_ms, instance, **fields)
        return self._append(self._path(instance, "equity", ms_to_date(ts_ms)), rec)

    def write_ladder(self, instance: str, ts_ms: int, **fields: Any) -> dict:
        rec = self.make_record(ts_ms, instance, **fields)
        return self._append(self._path(instance, "ladder"), rec)

    def event(
        self,
        level: str,
        kind: str,
        message: str,
        instance: str | None = None,
        ts_ms: int | None = None,
        **data: Any,
    ) -> dict:
        """Startups, mode changes, warnings, errors, reconnects, kill switch."""
        level = level.upper()
        rec = self.make_record(ts_ms, instance, level=level, kind=kind, message=message)
        if data:
            rec["data"] = data
        self._append(str(self.events_path()), rec)
        ts = ts_ms if ts_ms is not None else self.clock.now_ms()
        if level in ("ERROR", "CRITICAL"):
            self._error_times.append(ts)
        if self.log_events:
            pylevel = getattr(logging, level, logging.INFO)
            prefix = f"[{instance}] " if instance else ""
            logging.getLogger("bot.events").log(pylevel, "%s%s: %s", prefix, kind, message)
        for cb in self.event_listeners:
            try:
                cb(rec)
            except Exception:  # pragma: no cover - listeners must never break logging
                log.exception("event listener failed")
        return rec

    def errors_24h(self) -> int:
        cutoff = self.clock.now_ms() - 86_400_000
        while self._error_times and self._error_times[0] < cutoff:
            self._error_times.popleft()
        return len(self._error_times)

    def preload_error_count(self, max_lines: int = 5000) -> None:
        """Seed the 24h error counter from the tail of events.jsonl after a restart."""
        from bot.util import iso_to_ms

        cutoff = self.clock.now_ms() - 86_400_000
        for rec in tail_jsonl(self.events_path(), max_lines):
            if rec.get("level") in ("ERROR", "CRITICAL"):
                try:
                    t = iso_to_ms(rec["ts"])
                except (KeyError, ValueError):
                    continue
                if t >= cutoff:
                    self._error_times.append(t)

    # ---- state files ---------------------------------------------------------------------
    def save_state(self, instance: str, state: dict) -> None:
        atomic_write_json(self.state_path(instance), state)

    def load_state(self, instance: str) -> dict | None:
        return read_json(self.state_path(instance))

    def save_summary(self, instance: str, summary: dict) -> None:
        atomic_write_json(self.summary_path(instance), summary)

    def save_market_state(self, state: dict) -> None:
        atomic_write_json(self.market_state_path(), state)

    def load_market_state(self) -> dict | None:
        return read_json(self.market_state_path())

    def save_runtime(self, runtime: dict) -> None:
        atomic_write_json(self.runtime_path(), runtime)

    def flush(self) -> None:
        self.writer.flush()

    def close(self) -> None:
        self.writer.close()
