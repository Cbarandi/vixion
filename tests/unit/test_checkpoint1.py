"""Lógica pura Checkpoint 1 (sin Postgres)."""

from __future__ import annotations

from vixion.ops.checkpoint1 import score_band, summarize_reviews


def test_score_band():
    assert score_band(0) == "0-24"
    assert score_band(24) == "0-24"
    assert score_band(25) == "25-49"
    assert score_band(74) == "50-74"
    assert score_band(100) == "75-100"


def test_summarize_reviews_empty():
    s = summarize_reviews([])
    assert s["total_reviews"] == 0
    assert s["verdict_pct"]["good"] == 0.0


def test_summarize_reviews_counts():
    rows = [
        {"verdict": "good", "reason_code": "other", "narrative_state": "confirmed", "narrative_score": 60},
        {"verdict": "good", "reason_code": "other", "narrative_state": "confirmed", "narrative_score": 60},
        {"verdict": "bad", "reason_code": "off_topic", "narrative_state": "early", "narrative_score": 10},
        {"verdict": "unsure", "reason_code": "too_broad", "narrative_state": "emerging", "narrative_score": 40},
    ]
    s = summarize_reviews(rows)
    assert s["total_reviews"] == 4
    assert s["verdict_counts"]["good"] == 2
    assert s["verdict_pct"]["good"] == 50.0
    assert s["verdict_pct"]["bad"] == 25.0
    assert s["top_reason_codes"][0]["reason_code"] == "other"
    assert s["narrative_state_distribution"]["confirmed"] == 2
    assert s["narrative_score_band_distribution"]["50-74"] == 2
    assert s["narrative_score_band_distribution"]["0-24"] == 1
