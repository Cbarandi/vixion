"""Tests unitarios para persist_market_context (sin red)."""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]


def _load():
    path = ROOT / "scripts" / "persist_market_context.py"
    spec = importlib.util.spec_from_file_location("persist_market_context", path)
    assert spec and spec.loader
    mod = importlib.util.module_from_spec(spec)
    name = "persist_market_context_ut"
    sys.modules[name] = mod
    spec.loader.exec_module(mod)
    return mod


@pytest.fixture(scope="module")
def mc():
    return _load()


def test_run_id_matches_saved_at_convention(mc):
    rid = mc.run_id_from_saved_at("2026-04-14T12:15:40.702755+00:00")
    assert rid == "20260414_121540_702755"


def test_build_payload_ok(mc):
    p = mc.build_market_context_payload(
        run_id="r1",
        narrative_saved_at="2026-01-01T00:00:00+00:00",
        narratives_source_rel="data/narratives/x.json",
        btc_usd=100_000.123456789,
        eth_usd=3000.5,
        fetch_error=None,
        price_provider="test",
    )
    assert p["fetch_status"] == "ok"
    assert p["fetch_error"] is None
    assert p["btc_usd"] == pytest.approx(100_000.12345679)
    assert p["eth_usd"] == 3000.5
    assert p["schema_version"] == 1


def test_build_payload_partial(mc):
    p = mc.build_market_context_payload(
        run_id="r1",
        narrative_saved_at="2026-01-01T00:00:00+00:00",
        narratives_source_rel="x",
        btc_usd=1.0,
        eth_usd=None,
        fetch_error="network",
        price_provider="test",
    )
    assert p["fetch_status"] == "partial"
    assert p["fetch_error"] is None


def test_build_payload_unavailable(mc):
    p = mc.build_market_context_payload(
        run_id="r1",
        narrative_saved_at="2026-01-01T00:00:00+00:00",
        narratives_source_rel="x",
        btc_usd=None,
        eth_usd=None,
        fetch_error="timeout",
        price_provider="test",
    )
    assert p["fetch_status"] == "unavailable"
    assert p["fetch_error"] == "timeout"


def test_fetch_coingecko_parses_response(mc):
    from unittest.mock import MagicMock, patch

    mock_resp = MagicMock()
    mock_resp.raise_for_status = MagicMock()
    mock_resp.json.return_value = {"bitcoin": {"usd": 100.0}, "ethereum": {"usd": 200.5}}

    with patch.object(mc.requests, "get", return_value=mock_resp):
        btc, eth, err = mc.fetch_btc_eth_usd_coingecko()

    assert btc == 100.0
    assert eth == 200.5
    assert err is None
