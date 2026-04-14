"""
Tests unitarios para classify_narrative_lifecycle (import vía importlib).
"""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]


def _load():
    path = ROOT / "scripts" / "classify_narrative_lifecycle.py"
    spec = importlib.util.spec_from_file_location("classify_narrative_lifecycle", path)
    assert spec and spec.loader
    mod = importlib.util.module_from_spec(spec)
    name = "classify_narrative_lifecycle"
    if name not in sys.modules:
        sys.modules[name] = mod
        spec.loader.exec_module(mod)
    else:
        mod = sys.modules[name]
    return mod


@pytest.fixture(scope="module")
def clf():
    return _load()


def test_new_only_from_added(clf):
    diff = {
        "current_run_id": "r1",
        "previous_run_id": None,
        "added": [
            {
                "narrative_key": "topic a",
                "narrative": "topic a",
                "narrative_strength": 10.0,
                "rank": 1,
                "type": "confirmed",
            }
        ],
        "changed": [],
    }
    r = clf.classify_lifecycle_from_diff(diff, threshold=2.0)
    assert len(r["new"]) == 1
    assert r["new"][0]["narrative_key"] == "topic a"
    assert r["rising"] == []
    assert r["fading"] == []


def test_first_snapshot_style_all_new_empty_momentum(clf):
    """Diff tipo primer snapshot: solo added, sin changed → sin RISING/FADING."""
    diff = {
        "previous_run_id": None,
        "added": [
            {
                "narrative_key": "x",
                "narrative": "x",
                "narrative_strength": 5.0,
                "rank": 1,
                "type": "early",
            }
        ],
        "changed": [],
    }
    r = clf.classify_lifecycle_from_diff(diff, threshold=2.0)
    assert len(r["new"]) == 1
    assert r["rising"] == []
    assert r["fading"] == []


def test_rising_when_delta_above_threshold(clf):
    diff = {
        "added": [],
        "changed": [
            {
                "narrative_key": "up",
                "narrative": "up",
                "delta_strength": 5.0,
                "current_strength": 50.0,
                "previous_strength": 45.0,
                "current_rank": 1,
                "previous_rank": 2,
            }
        ],
    }
    r = clf.classify_lifecycle_from_diff(diff, threshold=2.0)
    assert r["new"] == []
    assert len(r["rising"]) == 1
    assert r["rising"][0]["narrative_key"] == "up"
    assert r["fading"] == []


def test_fading_when_delta_below_negative_threshold(clf):
    diff = {
        "added": [],
        "changed": [
            {
                "narrative_key": "down",
                "narrative": "down",
                "delta_strength": -4.0,
                "current_strength": 10.0,
                "previous_strength": 14.0,
                "current_rank": 5,
                "previous_rank": 3,
            }
        ],
    }
    r = clf.classify_lifecycle_from_diff(diff, threshold=2.0)
    assert r["new"] == []
    assert r["rising"] == []
    assert len(r["fading"]) == 1
    assert r["fading"][0]["narrative_key"] == "down"


def test_within_threshold_neither_rising_nor_fading(clf):
    diff = {
        "added": [],
        "changed": [
            {
                "narrative_key": "flat",
                "narrative": "flat",
                "delta_strength": 1.0,
                "current_strength": 20.0,
                "previous_strength": 19.0,
                "current_rank": 1,
                "previous_rank": 1,
            }
        ],
    }
    r = clf.classify_lifecycle_from_diff(diff, threshold=2.0)
    assert r["rising"] == []
    assert r["fading"] == []


def test_threshold_boundary_strict(clf):
    """delta_strength == threshold no cuenta como RISING (estricto >)."""
    diff = {
        "added": [],
        "changed": [
            {"narrative_key": "edge", "narrative": "edge", "delta_strength": 2.0}
        ],
    }
    r = clf.classify_lifecycle_from_diff(diff, threshold=2.0)
    assert r["rising"] == []
    assert r["fading"] == []
