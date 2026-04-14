"""Tests para compute_time_to_peak (BTC time-to-peak v1 vs ancla)."""

from __future__ import annotations

import importlib.util
import sys
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]


def _load():
    path = ROOT / "scripts" / "compute_time_to_peak.py"
    spec = importlib.util.spec_from_file_location("compute_time_to_peak_ut", path)
    assert spec and spec.loader
    mod = importlib.util.module_from_spec(spec)
    sys.modules["compute_time_to_peak_ut"] = mod
    spec.loader.exec_module(mod)
    return mod


@pytest.fixture(scope="module")
def ttp():
    return _load()


def _row(run_id: str, ts: datetime, btc: float | None) -> dict:
    return {
        "run_id": run_id,
        "ts": ts,
        "narrative_saved_at": ts.replace(tzinfo=UTC).isoformat().replace("+00:00", "Z"),
        "btc_usd": btc,
        "path": Path(run_id),
        "raw": {},
    }


def test_clear_peak_later_in_window(ttp):
    t0 = datetime(2026, 1, 1, 12, 0, tzinfo=UTC)
    t_peak = t0 + timedelta(days=1, hours=2)
    rows = [
        _row("a", t0, 100.0),
        _row("b", t0 + timedelta(hours=6), 95.0),
        _row("c", t_peak, 120.0),
    ]
    doc = ttp.build_time_to_peak_document(rows[0], rows, 0, "t")
    h = doc["horizons"]["1d"]
    assert h["status"] == "ok"
    assert h["target_run_id"] == "c"
    assert h["btc_best_return"] == pytest.approx(0.2)
    assert h["btc_time_to_peak_seconds"] == pytest.approx((t_peak - t0).total_seconds())


def test_monotonic_down_best_is_first_least_bad(ttp):
    t0 = datetime(2026, 1, 1, tzinfo=UTC)
    rows = [
        _row("a", t0, 100.0),
        _row("b", t0 + timedelta(hours=6), 98.0),
        _row("c", t0 + timedelta(days=1), 90.0),
    ]
    doc = ttp.build_time_to_peak_document(rows[0], rows, 0, "t")
    h = doc["horizons"]["1d"]
    assert h["status"] == "ok"
    assert h["btc_best_return"] == pytest.approx(-0.02)
    assert h["target_run_id"] == "c"
    # primer máximo (-0.02) en b
    assert h["btc_time_to_peak_seconds"] == pytest.approx((rows[1]["ts"] - t0).total_seconds())


def test_tie_resolves_to_earliest_snapshot(ttp):
    t0 = datetime(2026, 1, 1, tzinfo=UTC)
    rows = [
        _row("a", t0, 100.0),
        _row("b", t0 + timedelta(hours=10), 110.0),
        _row("c", t0 + timedelta(hours=20), 110.0),
        _row("d", t0 + timedelta(days=1, hours=1), 105.0),
    ]
    doc = ttp.build_time_to_peak_document(rows[0], rows, 0, "t")
    h = doc["horizons"]["1d"]
    assert h["status"] == "ok"
    assert h["btc_best_return"] == pytest.approx(0.1)
    assert h["btc_time_to_peak_seconds"] == pytest.approx((rows[1]["ts"] - t0).total_seconds())


def test_missing_future(ttp):
    t0 = datetime(2026, 1, 1, tzinfo=UTC)
    rows = [_row("solo", t0, 50.0)]
    doc = ttp.build_time_to_peak_document(rows[0], rows, 0, "t")
    for key in ("1d", "3d", "7d"):
        assert doc["horizons"][key]["status"] == "missing_future"


def test_missing_anchor_price(ttp):
    t0 = datetime(2026, 1, 1, tzinfo=UTC)
    rows = [
        _row("a", t0, None),
        _row("b", t0 + timedelta(days=2), 100.0),
    ]
    doc = ttp.build_time_to_peak_document(rows[0], rows, 0, "t")
    for key in ("1d", "3d", "7d"):
        assert doc["horizons"][key]["status"] == "invalid_anchor_reference"


def test_invalid_anchor_zero(ttp):
    t0 = datetime(2026, 1, 1, tzinfo=UTC)
    rows = [
        _row("a", t0, 0.0),
        _row("b", t0 + timedelta(days=2), 100.0),
    ]
    doc = ttp.build_time_to_peak_document(rows[0], rows, 0, "t")
    assert doc["horizons"]["1d"]["status"] == "invalid_anchor_reference"


def test_window_only_invalid_prices(ttp):
    t0 = datetime(2026, 1, 1, tzinfo=UTC)
    rows = [
        _row("a", t0, 100.0),
        _row("b", t0 + timedelta(hours=12), None),
        _row("c", t0 + timedelta(days=1, hours=1), None),
    ]
    doc = ttp.build_time_to_peak_document(rows[0], rows, 0, "t")
    h = doc["horizons"]["1d"]
    assert h["status"] == "missing_price"
    assert h["btc_best_return"] is None
