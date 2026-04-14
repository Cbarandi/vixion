"""Tests de enriquecimiento lifecycle en generate_alerts."""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]


def _load_gen():
    path = ROOT / "scripts" / "generate_alerts.py"
    spec = importlib.util.spec_from_file_location("generate_alerts", path)
    assert spec and spec.loader
    mod = importlib.util.module_from_spec(spec)
    name = "generate_alerts_lifecycle"
    sys.modules[name] = mod
    spec.loader.exec_module(mod)
    return mod


@pytest.fixture(scope="module")
def ga():
    return _load_gen()


def test_normalize_key_matches_lifecycle_rule(ga):
    assert ga.normalize_narrative_key("  foo   bar ") == "foo bar"


def test_lifecycle_key_sets_from_payload(ga):
    new_keys, rising_keys = ga.lifecycle_key_sets_from_payload(
        {
            "new": [
                {"narrative_key": "alpha", "narrative": "alpha"},
                {"narrative": "beta  value"},
            ],
            "rising": [
                {"narrative_key": "gamma", "narrative": "gamma"},
            ],
        },
    )
    assert "alpha" in new_keys
    assert ga.normalize_narrative_key("beta  value") in new_keys
    assert "gamma" in rising_keys


def test_enrich_tags_new_and_rising(ga):
    alerts = [
        {"type": "early_opportunity", "narrative": "alpha", "narrative_strength": 25.0},
        {
            "type": "confirmed_momentum",
            "narrative": "gamma",
            "narrative_strength": 50.0,
        },
        {"type": "early_opportunity", "narrative": "other", "narrative_strength": 30.0},
    ]
    n = ga.enrich_alerts_with_lifecycle(
        alerts,
        new_keys={"alpha"},
        rising_keys={"gamma"},
    )
    assert n == 2
    assert alerts[0].get("lifecycle") == {"phase": "new"}
    assert alerts[1].get("lifecycle") == {"phase": "rising"}
    assert "lifecycle" not in alerts[2]


def test_new_takes_precedence_if_key_in_both(ga):
    """Si una clave estuviera en ambos sets (anómalo), NEW gana."""
    alerts = [{"type": "surge", "narrative": "x", "growth": 1.0}]
    ga.enrich_alerts_with_lifecycle(
        alerts,
        new_keys={"x"},
        rising_keys={"x"},
    )
    assert alerts[0]["lifecycle"] == {"phase": "new"}


def test_format_telegram_includes_lifecycle(ga):
    base = {
        "type": "early_opportunity",
        "narrative": "Test",
        "narrative_strength": 22.0,
        "lifecycle": {"phase": "new"},
    }
    text = ga.format_alert_for_telegram(base)
    assert "lifecycle: NEW" in text
    assert "🚀 EARLY" in text

    base["lifecycle"] = {"phase": "rising"}
    text2 = ga.format_alert_for_telegram(base)
    assert "lifecycle: RISING" in text2
