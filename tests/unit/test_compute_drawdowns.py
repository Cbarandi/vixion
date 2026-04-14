"""Tests para compute_drawdowns (BTC drawdown v1 vs ancla)."""

from __future__ import annotations

import importlib.util
import sys
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]


def _load():
    path = ROOT / "scripts" / "compute_drawdowns.py"
    spec = importlib.util.spec_from_file_location("compute_drawdowns_ut", path)
    assert spec and spec.loader
    mod = importlib.util.module_from_spec(spec)
    sys.modules["compute_drawdowns_ut"] = mod
    spec.loader.exec_module(mod)
    return mod


@pytest.fixture(scope="module")
def dd():
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


def test_drawdown_with_intermediate_worse_than_target(dd):
    t0 = datetime(2026, 1, 1, 12, 0, tzinfo=UTC)
    rows = [
        _row("a", t0, 100.0),
        _row("b", t0 + timedelta(hours=12), 90.0),
        _row("c", t0 + timedelta(days=1, hours=1), 110.0),
    ]
    doc = dd.build_drawdown_document(rows[0], rows, 0, "t")
    h = doc["horizons"]["1d"]
    assert h["status"] == "ok"
    assert h["target_run_id"] == "c"
    assert h["btc_max_drawdown"] == pytest.approx(0.1)


def test_no_drawdown_price_only_up(dd):
    t0 = datetime(2026, 1, 1, tzinfo=UTC)
    rows = [
        _row("a", t0, 100.0),
        _row("b", t0 + timedelta(days=1), 105.0),
        _row("c", t0 + timedelta(days=2), 110.0),
    ]
    doc = dd.build_drawdown_document(rows[0], rows, 0, "t")
    h = doc["horizons"]["1d"]
    assert h["status"] == "ok"
    assert h["btc_max_drawdown"] == pytest.approx(0.0)


def test_missing_future_snapshots(dd):
    t0 = datetime(2026, 1, 1, tzinfo=UTC)
    rows = [_row("only", t0, 50.0)]
    doc = dd.build_drawdown_document(rows[0], rows, 0, "t")
    for key in ("1d", "3d", "7d"):
        assert doc["horizons"][key]["status"] == "missing_future"
        assert doc["horizons"][key]["target_run_id"] is None


def test_missing_anchor_price(dd):
    t0 = datetime(2026, 1, 1, tzinfo=UTC)
    rows = [
        _row("a", t0, None),
        _row("b", t0 + timedelta(days=2), 100.0),
    ]
    doc = dd.build_drawdown_document(rows[0], rows, 0, "t")
    for key in ("1d", "3d", "7d"):
        assert doc["horizons"][key]["status"] == "invalid_anchor_reference"


def test_invalid_anchor_zero(dd):
    t0 = datetime(2026, 1, 1, tzinfo=UTC)
    rows = [
        _row("a", t0, 0.0),
        _row("b", t0 + timedelta(days=2), 100.0),
    ]
    doc = dd.build_drawdown_document(rows[0], rows, 0, "t")
    assert doc["horizons"]["1d"]["status"] == "invalid_anchor_reference"


def test_missing_price_all_nulls_in_window(dd):
    t0 = datetime(2026, 1, 1, tzinfo=UTC)
    rows = [
        _row("a", t0, 100.0),
        _row("b", t0 + timedelta(hours=12), None),
        _row("c", t0 + timedelta(days=1, hours=1), None),
    ]
    doc = dd.build_drawdown_document(rows[0], rows, 0, "t")
    h = doc["horizons"]["1d"]
    assert h["target_run_id"] == "c"
    assert h["status"] == "missing_price"
    assert h["btc_max_drawdown"] is None


def test_skips_non_positive_prices_but_computes_from_valid(dd):
    t0 = datetime(2026, 1, 1, tzinfo=UTC)
    rows = [
        _row("a", t0, 100.0),
        _row("b", t0 + timedelta(hours=6), None),
        _row("c", t0 + timedelta(hours=12), 0.0),
        _row("d", t0 + timedelta(days=1), 95.0),
    ]
    doc = dd.build_drawdown_document(rows[0], rows, 0, "t")
    h = doc["horizons"]["1d"]
    assert h["status"] == "ok"
    assert h["btc_max_drawdown"] == pytest.approx(0.05)
