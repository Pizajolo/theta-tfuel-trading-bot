from __future__ import annotations

import math
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from bot.config import load_settings  # noqa: E402
from bot.market_data import JointBar, Kline  # noqa: E402

T0 = 1_767_225_600_000  # 2026-01-01T00:00:00Z


def make_bar(ts_ms: int, theta: float, tfuel: float, v: float = 1000.0, book: dict | None = None,
             spread: float = 0.0) -> JointBar:
    th = Kline(ts_ms, theta, theta * 1.001, theta * 0.999, theta, v)
    tf = Kline(ts_ms, tfuel, tfuel * 1.001, tfuel * 0.999, tfuel, v)
    if book is None and spread:
        book = {
            "THETAUSDT": (theta * (1 - spread / 2), theta * (1 + spread / 2)),
            "TFUELUSDT": (tfuel * (1 - spread / 2), tfuel * (1 + spread / 2)),
        }
    return JointBar(ts_ms, th, tf, book)


def bar_for_lr(ts_ms: int, lr: float, theta: float = 1.0, **kw) -> JointBar:
    return make_bar(ts_ms, theta, theta / math.exp(lr), **kw)


@pytest.fixture
def settings_factory(tmp_path):
    def make(**env):
        base = {"DATA_DIR": str(tmp_path / "data")}
        base.update({k: str(v) for k, v in env.items()})
        return load_settings(env_file=None, environ=base)

    return make
