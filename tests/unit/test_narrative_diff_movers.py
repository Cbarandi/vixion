"""Tests para extracción de Top Movers desde diff narrativo."""

from __future__ import annotations

from vixion.ops.narrative_diff_movers import build_top_movers_from_diff


def test_rising_and_falling_sorted_by_delta_strength() -> None:
    diff = {
        "diff_generated_at": "t0",
        "current_run_id": "a",
        "previous_run_id": "b",
        "changed": [
            {
                "narrative": "mild up",
                "narrative_key": "mild up",
                "delta_strength": 0.5,
                "current_strength": 5.0,
                "previous_strength": 4.5,
                "current_rank": 10,
                "previous_rank": 12,
            },
            {
                "narrative": "big up",
                "narrative_key": "big up",
                "delta_strength": 3.0,
                "current_strength": 9.0,
                "previous_strength": 6.0,
                "current_rank": 2,
                "previous_rank": 5,
            },
            {
                "narrative": "big down",
                "narrative_key": "big down",
                "delta_strength": -2.0,
                "current_strength": 1.0,
                "previous_strength": 3.0,
                "current_rank": 20,
                "previous_rank": 8,
            },
            {
                "narrative": "tiny down",
                "narrative_key": "tiny down",
                "delta_strength": -0.25,
                "current_strength": 4.0,
                "previous_strength": 4.25,
                "current_rank": 11,
                "previous_rank": 11,
            },
        ],
        "added": [],
        "removed": [],
    }
    out = build_top_movers_from_diff(diff, limit=5)
    assert [r["narrative"] for r in out["rising"]] == ["big up", "mild up"]
    assert [r["narrative"] for r in out["falling"]] == ["big down", "tiny down"]
    assert out["meta"]["counts"]["changed"] == 4


def test_zero_delta_excluded_from_both_lists() -> None:
    diff = {
        "changed": [
            {
                "narrative": "flat",
                "narrative_key": "flat",
                "delta_strength": 0.0,
                "current_strength": 3.0,
                "previous_strength": 3.0,
                "current_rank": 5,
                "previous_rank": 5,
            },
            {
                "narrative": "up",
                "narrative_key": "up",
                "delta_strength": 1.0,
                "current_strength": 4.0,
                "previous_strength": 3.0,
                "current_rank": 4,
                "previous_rank": 6,
            },
        ],
    }
    out = build_top_movers_from_diff(diff, limit=5)
    assert len(out["rising"]) == 1
    assert out["rising"][0]["narrative"] == "up"
    assert out["falling"] == []


def test_stable_tie_breaker_by_narrative_name() -> None:
    diff = {
        "changed": [
            {
                "narrative": "b",
                "narrative_key": "b",
                "delta_strength": 1.0,
                "current_strength": 2.0,
                "previous_strength": 1.0,
                "current_rank": 1,
                "previous_rank": 2,
            },
            {
                "narrative": "a",
                "narrative_key": "a",
                "delta_strength": 1.0,
                "current_strength": 2.0,
                "previous_strength": 1.0,
                "current_rank": 1,
                "previous_rank": 2,
            },
        ],
    }
    out = build_top_movers_from_diff(diff, limit=5)
    assert [r["narrative"] for r in out["rising"]] == ["a", "b"]


def test_limit_caps_lists() -> None:
    diff = {
        "changed": [
            {
                "narrative": f"n{i}",
                "narrative_key": f"n{i}",
                "delta_strength": float(i),
                "current_strength": float(i),
                "previous_strength": 0.0,
                "current_rank": 1,
                "previous_rank": 10,
            }
            for i in range(1, 8)
        ],
    }
    out = build_top_movers_from_diff(diff, limit=3)
    assert len(out["rising"]) == 3
    assert out["rising"][0]["delta_strength"] == 7.0


def test_skips_invalid_changed_rows() -> None:
    diff = {
        "changed": [
            {"narrative": "", "delta_strength": 1.0},
            {"narrative": "ok", "narrative_key": "ok", "delta_strength": 2.0, "current_strength": 3.0},
            {"narrative": "bad", "delta_strength": "x"},
        ],
    }
    out = build_top_movers_from_diff(diff, limit=5)
    assert len(out["rising"]) == 1
    assert out["rising"][0]["narrative"] == "ok"
