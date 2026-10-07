"""FastAPI backend. Reads only DATA_DIR; never talks to Binance."""

from __future__ import annotations

import math
import time
from pathlib import Path
from typing import Any

from fastapi import FastAPI, Query
from fastapi.responses import FileResponse, JSONResponse

from bot.reference import BACKTEST_REFERENCE, REFERENCE_NOTE, prorated_range
from bot.util import DAY_MS, MINUTE_MS, iso_to_ms, ms_to_iso, year_of, year_start_ms
from dashboard.reader import DataReader, pick_stride

STATIC = Path(__file__).parent / "static"
RANGE = Query("7d", pattern="^(24h|7d|30d|all)$")


def _ms(ts: str | None) -> int | None:
    try:
        return iso_to_ms(ts) if ts else None
    except ValueError:
        return None


def reference_status(inst: str, summary: dict, now_ms: int) -> dict | None:
    """Where the current-year result sits relative to the (prorated) backtest range."""
    prim = summary.get("primary_variant") or "touch"
    ytd = (summary.get("excess_ytd") or {}).get(prim)
    if ytd is None:
        return None
    year = year_of(now_ms)
    bench = _ms(summary.get("benchmark_start")) or now_ms
    since = max(year_start_ms(year), bench)
    days = max((now_ms - since) / DAY_MS, 0.0)
    pr = prorated_range(inst, year, days)
    if pr is None:
        return None
    lo, hi, label = pr
    if ytd >= lo:
        status, text = "good", "within or above the reference range"
    elif ytd >= 0:
        status, text = "warning", "positive but below the reference range"
    else:
        status, text = "critical", "below HODL"
    return {"year": year, "days": round(days, 1), "lo": lo, "hi": hi, "label": label, "value": ytd,
            "variant": prim, "status": status, "text": text, "full_range": BACKTEST_REFERENCE.get(inst, {}).get(year)}


def create_app(data_dir: Path) -> FastAPI:
    reader = DataReader(data_dir)
    app = FastAPI(title="THETA/TFUEL ratio bot dashboard", docs_url=None, redoc_url=None)

    @app.get("/")
    def index() -> FileResponse:
        return FileResponse(STATIC / "index.html", headers={"Cache-Control": "no-cache"})

    @app.get("/api/overview")
    def overview() -> dict[str, Any]:
        now = int(time.time() * 1000)
        rt = reader.runtime()
        last_bar = reader.last_bar_ms()
        replay = bool(rt.get("replay"))
        data_now = (last_bar + MINUTE_MS) if (replay and last_bar) else now
        kill_file = (Path(data_dir) / "KILL").exists()
        ks = rt.get("kill_switch") or {}
        instances = {}
        for inst in reader.instances():
            s = reader.summary(inst)
            if not s:
                continue
            s["reference"] = reference_status(inst, s, data_now)
            instances[inst] = s
        age = (now - (last_bar + MINUTE_MS)) / 1000 if last_bar else None
        return {
            "now": ms_to_iso(now),
            "data_dir": str(Path(data_dir).resolve()),
            "replay": replay,
            "last_bar_ts": ms_to_iso(last_bar),
            "data_age_sec": age,
            "fresh": age is not None and age <= 180,
            "ws_connected": (rt.get("ws") or {}).get("ws_connected"),
            "runtime_ts": rt.get("ts"),
            "runtime_age_sec": (now - _ms(rt["ts"])) / 1000 if rt.get("ts") else None,
            "kill_switch": bool(ks.get("active")) or kill_file,
            "kill_file": kill_file,
            "testnet": rt.get("testnet"),
            "errors_24h": rt.get("errors_24h"),
            "instances": instances,
            "reference": {"table": BACKTEST_REFERENCE, "note": REFERENCE_NOTE},
        }

    @app.get("/api/ratio")
    def ratio(range: str = RANGE) -> dict[str, Any]:  # noqa: A002
        win = reader.window(range)
        if not win:
            return {"points": [], "markers": {}, "ladder": [], "entry": None}
        start, end = win
        stride = pick_stride((end - start) / MINUTE_MS)
        bars = reader.bars(start, end, stride)
        insts = reader.instances()
        entry = None
        tiers: list = []
        for inst in insts:
            s = reader.summary(inst) or {}
            p = s.get("params") or {}
            if entry is None and p.get("overlay_entry") is not None:
                entry = p["overlay_entry"]
                tiers = p.get("ladder_tiers") or []
        entry = entry if entry is not None else 0.06
        tiers = tiers or [[0.15, 0.15], [0.30, 0.30]]
        points = [
            {"t": t, "r": r, "ema": e, "up": e * math.exp(entry) if e else None,
             "lo": e * math.exp(-entry) if e else None, "stale": st}
            for t, r, e, st in bars
        ]
        ladder = []
        if insts:
            recs = reader.ladder(insts[0])
            prev = None
            for rec in recs:
                t = _ms(rec.get("ts"))  # day close (00:00 of the next day)
                if t is None or rec.get("ema60") is None:
                    continue
                if t < start:
                    prev = rec
                    continue
                if t > end:
                    break
                ladder.append(rec)
            if prev is not None:
                ladder.insert(0, dict(prev, ts=ms_to_iso(start)))
        lad = []
        for rec in ladder:
            e = rec["ema60"]
            lad.append({"t": _ms(rec["ts"]), "ema": math.exp(e), "D": rec.get("D"), "w": rec.get("ladder_w"),
                        "bands": [[math.exp(e - th), math.exp(e + th)] for th, _ in tiers]})
        if lad:
            lad.append(dict(lad[-1], t=end))
        markers: dict[str, list] = {}
        for inst in insts:
            ms = []
            for d in reader.jsonl(inst, "decisions"):
                t = _ms(d.get("ts"))
                if t is None or t < start or t > end or d.get("ratio") is None:
                    continue
                ms.append({"t": t, "r": d["ratio"], "w_from": d.get("w_from"), "w_target": d.get("w_target"),
                           "reason": d.get("reason"), "mode": d.get("mode"), "dv": d.get("dv_usd"),
                           "id": d.get("decision_id")})
            markers[inst] = ms
        return {"range": range, "start": start, "end": end, "stride_min": stride, "entry": entry,
                "tiers": tiers, "points": points, "ladder": lad, "markers": markers}

    def _equity(range_: str) -> dict[str, Any]:
        win = reader.window(range_)
        if not win:
            return {"series": {}}
        start, end = win
        stride = pick_stride((end - start) / MINUTE_MS, base=5)
        series: dict[str, dict[str, list]] = {}
        for inst in reader.instances():
            by: dict[str, list] = {}
            for r in reader.equity(inst, start, end, stride):
                by.setdefault(r["v"], []).append(r)
            series[inst] = by
        return {"range": range_, "start": start, "end": end, "stride_min": stride, "series": series}

    @app.get("/api/excess")
    def excess(range: str = RANGE) -> dict[str, Any]:  # noqa: A002
        data = _equity(range)
        out = {}
        for inst, by in data.get("series", {}).items():
            out[inst] = {v: [[r["t"], r["excess"]] for r in rows] for v, rows in by.items()}
        prim = {i: (reader.summary(i) or {}).get("primary_variant", "touch") for i in out}
        return {"range": range, "start": data.get("start"), "end": data.get("end"), "series": out, "primary": prim}

    @app.get("/api/weights")
    def weights(range: str = RANGE) -> dict[str, Any]:  # noqa: A002
        data = _equity(range)
        out = {}
        for inst, by in data.get("series", {}).items():
            prim = (reader.summary(inst) or {}).get("primary_variant", "touch")
            rows = by.get(prim) or by.get("touch") or []
            out[inst] = {"variant": prim, "w": [[r["t"], r["w"]] for r in rows],
                         "w_target": [[r["t"], r["wt"]] for r in rows if r.get("wt") is not None]}
        return {"range": range, "start": data.get("start"), "end": data.get("end"), "series": out}

    @app.get("/api/tables")
    def tables(n: int = Query(50, ge=1, le=500)) -> dict[str, Any]:
        insts = {}
        for inst in reader.instances():
            s = reader.summary(inst) or {}
            insts[inst] = {
                "primary_variant": s.get("primary_variant", "touch"),
                "decisions": list(reversed(reader.tail(inst, "decisions", n))),
                "orders": list(reversed(reader.tail(inst, "orders", n * 6))),
            }
        events = list(reversed(reader.tail(None, "events", n)))
        return {"instances": insts, "events": events}

    @app.get("/api/compare")
    def compare() -> dict[str, Any]:
        out = {}
        for inst in reader.instances():
            s = reader.summary(inst) or {}
            live = s.get("live") or {}
            if s.get("mode") != "live" and not live.get("active"):
                continue
            start = _ms(live.get("activated")) or _ms(s.get("benchmark_start"))
            end = (reader.last_bar_ms() or int(time.time() * 1000)) + MINUTE_MS
            if start is None:
                continue
            stride = pick_stride((end - start) / MINUTE_MS, base=5)
            by: dict[str, list] = {}
            for r in reader.equity(inst, start, end, stride):
                if r["v"] in ("live", "touch"):
                    by.setdefault(r["v"], []).append([r["t"], r["excess"]])
            slips = []
            for o in reader.iter_orders(inst):
                if o.get("mode") == "live" and o.get("event") == "final" and o.get("slippage_bps_vs_mid") is not None:
                    slips.append(o["slippage_bps_vs_mid"])
            hist: dict[str, int] = {}
            for x in slips:
                b = int(math.floor(x / 5.0) * 5)
                hist[str(b)] = hist.get(str(b), 0) + 1
            out[inst] = {
                "activated": live.get("activated"),
                "live": by.get("live", []),
                "touch": by.get("touch", []),
                "excess_live": (s.get("excess_by_variant") or {}).get("live"),
                "excess_touch": (s.get("excess_by_variant") or {}).get("touch"),
                "slippage": {"n": len(slips), "mean": sum(slips) / len(slips) if slips else None,
                             "hist": sorted(([int(k), v] for k, v in hist.items()))},
            }
        return {"instances": out}

    @app.exception_handler(Exception)
    async def errors(request: Any, exc: Exception) -> JSONResponse:  # pragma: no cover
        return JSONResponse({"error": repr(exc)}, status_code=500)

    return app
