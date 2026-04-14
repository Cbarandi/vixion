"""
Tests unitarios para la lógica de diff de historial de narrativas.

Carga el script bajo scripts/ (no es paquete) mediante importlib.
"""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]


def _load_script():
    path = ROOT / "scripts" / "persist_narrative_history.py"
    spec = importlib.util.spec_from_file_location("persist_narrative_history", path)
    assert spec and spec.loader
    mod = importlib.util.module_from_spec(spec)
    # Evitar re-exec en imports repetidos
    name = "persist_narrative_history"
    if name not in sys.modules:
        sys.modules[name] = mod
        spec.loader.exec_module(mod)
    else:
        mod = sys.modules[name]
    return mod


@pytest.fixture(scope="module")
def pnh():
    return _load_script()


def test_normalize_collapse_whitespace(pnh):
    assert pnh.normalize_narrative_key("  foo   bar  ") == "foo bar"


def test_build_diff_first_run_no_previous(pnh):
    current = [
        {
            "narrative": "equity market volatility",
            "type": "confirmed",
            "total_articles": 10,
            "narrative_strength": 50.0,
        },
    ]
    diff = pnh.build_diff(
        "run_b",
        None,
        current,
        None,
        diff_generated_at="2026-04-14T00:00:00+00:00",
    )
    assert diff["current_run_id"] == "run_b"
    assert diff["previous_run_id"] is None
    assert diff["changed"] == []
    assert diff["removed"] == []
    assert len(diff["added"]) == 1
    assert diff["added"][0]["narrative_key"] == "equity market volatility"
    assert diff["added"][0]["rank"] == 1
    assert diff["note"] == "first_snapshot_no_previous_run"


def test_build_diff_added_removed_changed(pnh):
    prev_rows = [
        {
            "narrative": "stays both runs",
            "type": "confirmed",
            "total_articles": 5,
            "narrative_strength": 40.0,
        },
        {
            "narrative": "removed narrative",
            "type": "early",
            "total_articles": 3,
            "narrative_strength": 20.0,
        },
    ]
    curr_rows = [
        {
            "narrative": "stays both runs",
            "type": "confirmed",
            "total_articles": 8,
            "narrative_strength": 55.0,
        },
        {
            "narrative": "brand new topic",
            "type": "confirmed",
            "total_articles": 12,
            "narrative_strength": 30.0,
        },
    ]
    diff = pnh.build_diff(
        "run_2",
        "run_1",
        curr_rows,
        prev_rows,
        diff_generated_at="2026-04-14T12:00:00+00:00",
    )
    assert diff["previous_run_id"] == "run_1"
    keys_added = {x["narrative_key"] for x in diff["added"]}
    keys_removed = {x["narrative_key"] for x in diff["removed"]}
    assert keys_added == {"brand new topic"}
    assert keys_removed == {"removed narrative"}

    changed = {c["narrative_key"]: c for c in diff["changed"]}
    assert set(changed) == {"stays both runs"}
    c = changed["stays both runs"]
    assert c["current_strength"] == 55.0
    assert c["previous_strength"] == 40.0
    assert c["delta_strength"] == pytest.approx(15.0)
    assert c["current_total_articles"] == 8
    assert c["previous_total_articles"] == 5
    assert c["current_rank"] == 1
    assert c["previous_rank"] == 1
    assert c["delta_rank"] == 0


def test_build_diff_empty_previous_list_is_not_first_run(pnh):
    """Lista vacía ≠ ausencia de baseline: todo el actual cuenta como añadido."""
    diff = pnh.build_diff(
        "run_2",
        "run_1",
        [
            {
                "narrative": "only now",
                "type": "confirmed",
                "total_articles": 1,
                "narrative_strength": 10.0,
            }
        ],
        [],
        diff_generated_at="2026-01-01T00:00:00+00:00",
    )
    assert diff["previous_run_id"] == "run_1"
    assert len(diff["added"]) == 1
    assert diff["changed"] == []
    assert "note" not in diff
