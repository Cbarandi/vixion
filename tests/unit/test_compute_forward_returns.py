"""Tests para compute_forward_returns (sin I/O de red)."""

from __future__ import annotations

import json
import importlib.util
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]


def _load():
    path = ROOT / "scripts" / "compute_forward_returns.py"
    spec = importlib.util.spec_from_file_location("compute_forward_returns", path)
    assert spec and spec.loader
    mod = importlib.util.module_from_spec(spec)
    sys.modules["compute_forward_returns_ut"] = mod
    spec.loader.exec_module(mod)
    return mod


@pytest.fixture(scope="module")
def fr():
    return _load()


def test_simple_return(fr):
    v, st = fr.simple_return(100.0, 110.0)
    assert st == "ok"
    assert v == pytest.approx(0.1)
    assert fr.simple_return(None, 100.0)[1] == "missing_price"
    assert fr.simple_return(0.0, 100.0)[1] == "invalid_anchor_reference"


def test_pick_future_respects_horizon(fr):
    rows = [
        {
            "run_id": "a",
            "ts": fr.parse_iso_to_utc("2020-01-01T00:00:00+00:00"),
            "narrative_saved_at": "2020-01-01T00:00:00+00:00",
            "btc_usd": 100.0,
            "eth_usd": 10.0,
        },
        {
            "run_id": "b",
            "ts": fr.parse_iso_to_utc("2020-01-02T12:00:00+00:00"),
            "narrative_saved_at": "2020-01-02T12:00:00+00:00",
            "btc_usd": 110.0,
            "eth_usd": 11.0,
        },
    ]
    fut = fr.pick_future_snapshot(rows, 0, rows[0]["ts"], 1)
    assert fut is not None
    assert fut["run_id"] == "b"


def test_forward_returns_when_future_data_exists(fr, tmp_path):
    mc = tmp_path / "mc"
    mc.mkdir()
    # r1 anchor
    (mc / "market_context_r1.json").write_text(
        json.dumps(
            {
                "run_id": "r1",
                "narrative_saved_at": "2020-01-01T00:00:00+00:00",
                "btc_usd": 100.0,
                "eth_usd": 50.0,
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )
    (mc / "market_context_r2.json").write_text(
        json.dumps(
            {
                "run_id": "r2",
                "narrative_saved_at": "2020-01-02T12:00:00+00:00",
                "btc_usd": 110.0,
                "eth_usd": 55.0,
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )
    (mc / "market_context_r3.json").write_text(
        json.dumps(
            {
                "run_id": "r3",
                "narrative_saved_at": "2020-01-05T00:00:00+00:00",
                "btc_usd": 120.0,
                "eth_usd": 60.0,
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )
    (mc / "market_context_r4.json").write_text(
        json.dumps(
            {
                "run_id": "r4",
                "narrative_saved_at": "2020-01-10T00:00:00+00:00",
                "btc_usd": 130.0,
                "eth_usd": 65.0,
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )

    out = tmp_path / "out"
    w, _ = fr.compute_all_forward_returns(mc, out, now_iso="2020-01-15T00:00:00+00:00")
    assert w == 4

    doc = json.loads((out / "forward_returns_r1.json").read_text(encoding="utf-8"))
    assert doc["btc_return_1d"] == pytest.approx(0.1)
    assert doc["eth_return_1d"] == pytest.approx(0.1)
    assert doc["btc_return_3d"] == pytest.approx(0.2)
    assert doc["btc_return_7d"] == pytest.approx(0.3)


def test_horizon_partial_when_target_missing_one_price(fr, tmp_path):
    """Futuro existe pero falta un precio → retorno parcial y status partial."""
    mc = tmp_path / "mc"
    mc.mkdir()
    (mc / "market_context_r1.json").write_text(
        json.dumps(
            {
                "run_id": "r1",
                "narrative_saved_at": "2020-01-01T00:00:00+00:00",
                "btc_usd": 100.0,
                "eth_usd": 50.0,
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )
    (mc / "market_context_r2.json").write_text(
        json.dumps(
            {
                "run_id": "r2",
                "narrative_saved_at": "2020-01-02T12:00:00+00:00",
                "btc_usd": 110.0,
                "eth_usd": None,
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )
    out = tmp_path / "out"
    fr.compute_all_forward_returns(mc, out, now_iso="2020-01-02T12:00:00+00:00")
    doc = json.loads((out / "forward_returns_r1.json").read_text(encoding="utf-8"))
    h1 = doc["horizons"]["1d"]
    assert h1["status"] == "partial"
    assert h1["btc_return"] == pytest.approx(0.1)
    assert h1["eth_return"] is None


def test_horizon_missing_price_when_anchor_prices_missing(fr, tmp_path):
    """Ancla sin precios pero hay futuro con precios → missing_price."""
    mc = tmp_path / "mc"
    mc.mkdir()
    (mc / "market_context_r1.json").write_text(
        json.dumps(
            {
                "run_id": "r1",
                "narrative_saved_at": "2020-01-01T00:00:00+00:00",
                "btc_usd": None,
                "eth_usd": None,
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )
    (mc / "market_context_r2.json").write_text(
        json.dumps(
            {
                "run_id": "r2",
                "narrative_saved_at": "2020-01-02T12:00:00+00:00",
                "btc_usd": 110.0,
                "eth_usd": 55.0,
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )
    out = tmp_path / "out"
    fr.compute_all_forward_returns(mc, out, now_iso="2020-01-02T12:00:00+00:00")
    doc = json.loads((out / "forward_returns_r1.json").read_text(encoding="utf-8"))
    assert doc["horizons"]["1d"]["status"] == "missing_price"
    assert doc["btc_return_1d"] is None
    assert doc["eth_return_1d"] is None


def test_missing_future_horizons(fr, tmp_path):
    mc = tmp_path / "mc"
    mc.mkdir()
    (mc / "market_context_only.json").write_text(
        json.dumps(
            {
                "run_id": "only",
                "narrative_saved_at": "2020-01-01T00:00:00+00:00",
                "btc_usd": 100.0,
                "eth_usd": 50.0,
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )
    out = tmp_path / "out"
    fr.compute_all_forward_returns(mc, out, now_iso="2020-01-01T01:00:00+00:00")
    doc = json.loads((out / "forward_returns_only.json").read_text(encoding="utf-8"))
    assert doc["horizons"]["1d"]["status"] == "missing_future"
    assert doc["btc_return_1d"] is None
