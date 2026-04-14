"""Tests para rank_narrative_edge (sin I/O real del repo)."""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]


def _load():
    path = ROOT / "scripts" / "rank_narrative_edge.py"
    spec = importlib.util.spec_from_file_location("rank_narrative_edge_ut", path)
    assert spec and spec.loader
    mod = importlib.util.module_from_spec(spec)
    sys.modules["rank_narrative_edge_ut"] = mod
    spec.loader.exec_module(mod)
    return mod


@pytest.fixture(scope="module")
def edge():
    return _load()


def test_weighted_positive_rate_reweights_when_horizon_missing(edge):
    row = {
        "positive_rate_1d": 1.0,
        "count_with_returns_1d": 5,
        "positive_rate_3d": 0.0,
        "count_with_returns_3d": 5,
        "positive_rate_7d": None,
        "count_with_returns_7d": 0,
    }
    # solo 1d y 3d: pesos 0.4 y 0.35 -> renorm 0.4/0.75, 0.35/0.75
    w1 = 0.4 / 0.75
    w3 = 0.35 / 0.75
    expected = w1 * 1.0 + w3 * 0.0
    assert abs(edge.weighted_positive_rate(row) - expected) < 1e-9


def test_weighted_positive_rate_none_when_no_returns(edge):
    row = {
        "positive_rate_1d": 0.5,
        "count_with_returns_1d": 0,
    }
    assert edge.weighted_positive_rate(row) is None


def test_shrink_toward_neutral(edge):
    assert edge.shrink_toward_neutral(1.0, 3, 2.0) == pytest.approx(0.5 + 0.5 * (3 / 5))
    assert edge.shrink_toward_neutral(0.5, 10, 2.0) == pytest.approx(0.5)


def test_min_occurrences_excludes(edge):
    agg = {
        "generated_at": "t",
        "narratives": [
            {
                "narrative_key": "low_n",
                "occurrences": 2,
                "count_with_returns_1d": 2,
                "positive_rate_1d": 1.0,
            },
        ],
    }
    out = edge.build_ranking_payload(
        agg, min_occurrences=3, shrinkage_k=2.0, source_path="/tmp/in.json"
    )
    assert out["ranked"] == []
    assert any(x["narrative_key"] == "low_n" and x["eligible"] is False for x in out["all_narratives"])


def test_stable_sort_tie_breaker(edge):
    agg = {
        "generated_at": "t",
        "narratives": [
            {
                "narrative_key": "b",
                "occurrences": 5,
                "count_with_returns_1d": 5,
                "positive_rate_1d": 0.8,
                "count_with_returns_3d": 0,
                "count_with_returns_7d": 0,
            },
            {
                "narrative_key": "a",
                "occurrences": 5,
                "count_with_returns_1d": 5,
                "positive_rate_1d": 0.8,
                "count_with_returns_3d": 0,
                "count_with_returns_7d": 0,
            },
        ],
    }
    out = edge.build_ranking_payload(
        agg, min_occurrences=3, shrinkage_k=2.0, source_path="/tmp/in.json"
    )
    keys = [r["narrative_key"] for r in out["ranked"]]
    assert keys == ["a", "b"]


def test_ranked_order_by_edge_score(edge):
    agg = {
        "generated_at": "t",
        "narratives": [
            {
                "narrative_key": "weak",
                "occurrences": 10,
                "count_with_returns_1d": 10,
                "positive_rate_1d": 0.2,
                "count_with_returns_3d": 0,
                "count_with_returns_7d": 0,
            },
            {
                "narrative_key": "strong",
                "occurrences": 10,
                "count_with_returns_1d": 10,
                "positive_rate_1d": 0.9,
                "count_with_returns_3d": 0,
                "count_with_returns_7d": 0,
            },
        ],
    }
    out = edge.build_ranking_payload(
        agg, min_occurrences=3, shrinkage_k=2.0, source_path="/tmp/in.json"
    )
    assert [r["narrative_key"] for r in out["ranked"]] == ["strong", "weak"]
    assert out["ranked"][0]["rank"] == 1


def test_capped_linear_penalty(edge):
    assert edge.capped_linear_penalty(None, 0.15, 0.5) == 0.0
    assert edge.capped_linear_penalty(0.15, 0.15, 0.5) == pytest.approx(0.5)
    assert edge.capped_linear_penalty(0.30, 0.15, 0.5) == pytest.approx(0.5)
    assert edge.capped_linear_penalty(0.075, 0.15, 0.5) == pytest.approx(0.25)


def test_v2_without_dd_ttp_data_penalties_zero(edge):
    agg = {
        "generated_at": "t",
        "narratives": [
            {
                "narrative_key": "x",
                "occurrences": 10,
                "count_with_returns_1d": 10,
                "positive_rate_1d": 0.8,
                "count_with_returns_3d": 0,
                "count_with_returns_7d": 0,
            },
        ],
    }
    out = edge.build_ranking_payload(
        agg, min_occurrences=3, shrinkage_k=2.0, source_path="/tmp/in.json"
    )
    r = out["ranked"][0]
    assert r["drawdown_penalty"] == 0.0
    assert r["time_to_peak_penalty"] == 0.0
    assert r["edge_score_v2"] == pytest.approx(r["edge_score"])


def test_v2_rank_prefers_lower_drawdown_when_v1_tied(edge):
    agg = {
        "generated_at": "t",
        "narratives": [
            {
                "narrative_key": "painful",
                "occurrences": 10,
                "count_with_returns_1d": 10,
                "positive_rate_1d": 0.8,
                "count_with_returns_3d": 0,
                "count_with_returns_7d": 0,
                "avg_btc_max_drawdown_1d": 0.30,
                "count_with_drawdown_1d": 10,
            },
            {
                "narrative_key": "smooth",
                "occurrences": 10,
                "count_with_returns_1d": 10,
                "positive_rate_1d": 0.8,
                "count_with_returns_3d": 0,
                "count_with_returns_7d": 0,
                "avg_btc_max_drawdown_1d": 0.03,
                "count_with_drawdown_1d": 10,
            },
        ],
    }
    out = edge.build_ranking_payload(
        agg,
        min_occurrences=3,
        shrinkage_k=2.0,
        source_path="/tmp/in.json",
        v2_dd_ref=0.15,
        v2_ttp_ref_hours=48.0,
        v2_penalty_max=0.5,
    )
    assert [r["narrative_key"] for r in out["ranked"]] == ["smooth", "painful"]
    assert out["ranked"][0]["edge_score"] == pytest.approx(out["ranked"][1]["edge_score"])
    assert out["ranked"][0]["edge_score_v2"] > out["ranked"][1]["edge_score_v2"]


def test_v2_time_penalty_reorders(edge):
    agg = {
        "generated_at": "t",
        "narratives": [
            {
                "narrative_key": "slow",
                "occurrences": 10,
                "count_with_returns_1d": 10,
                "positive_rate_1d": 0.8,
                "count_with_returns_3d": 0,
                "count_with_returns_7d": 0,
                "avg_btc_time_to_peak_hours_1d": 96.0,
                "count_with_time_to_peak_1d": 10,
            },
            {
                "narrative_key": "fast",
                "occurrences": 10,
                "count_with_returns_1d": 10,
                "positive_rate_1d": 0.8,
                "count_with_returns_3d": 0,
                "count_with_returns_7d": 0,
                "avg_btc_time_to_peak_hours_1d": 6.0,
                "count_with_time_to_peak_1d": 10,
            },
        ],
    }
    out = edge.build_ranking_payload(
        agg,
        min_occurrences=3,
        shrinkage_k=2.0,
        source_path="/tmp/in.json",
        v2_dd_ref=0.15,
        v2_ttp_ref_hours=48.0,
        v2_penalty_max=0.5,
    )
    assert [r["narrative_key"] for r in out["ranked"]] == ["fast", "slow"]
    assert out["ranked"][0]["time_to_peak_penalty"] < out["ranked"][1]["time_to_peak_penalty"]


def test_payload_schema_v2(edge):
    out = edge.build_ranking_payload(
        {"generated_at": "t", "narratives": []},
        min_occurrences=3,
        shrinkage_k=2.0,
        source_path="/tmp/in.json",
    )
    assert out["schema_version"] == 2
    assert out["ranking_id"] == "narrative_edge_v2"
    assert "edge_score_v2" in out["formula"]
    assert out["formula"]["v2_penalty_max"] == 0.5
