"""Tests para aggregate_narrative_outcomes (sin I/O real del repo)."""

from __future__ import annotations

import json
import importlib.util
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]


def _load():
    path = ROOT / "scripts" / "aggregate_narrative_outcomes.py"
    spec = importlib.util.spec_from_file_location("aggregate_narrative_outcomes", path)
    assert spec and spec.loader
    mod = importlib.util.module_from_spec(spec)
    sys.modules["aggregate_narrative_outcomes_ut"] = mod
    spec.loader.exec_module(mod)
    return mod


@pytest.fixture(scope="module")
def agg():
    return _load()


def test_normalize_groups_variants(agg):
    assert agg.normalize_narrative_key("  foo   bar  ") == agg.normalize_narrative_key("foo bar")


def test_aggregate_multiple_runs_and_averages(agg, tmp_path):
    snap = tmp_path / "snapshots"
    fwd = tmp_path / "forward"
    lc = tmp_path / "lifecycle"
    snap.mkdir()
    fwd.mkdir()
    lc.mkdir()

    (snap / "run_a.json").write_text(
        json.dumps(
            {
                "narratives": [
                    {"narrative": "alpha theme", "narrative_strength": 10.0},
                    {"narrative": "beta theme", "narrative_strength": 5.0},
                ],
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )
    (fwd / "forward_returns_run_a.json").write_text(
        json.dumps(
            {
                "btc_return_1d": 0.10,
                "btc_return_3d": 0.20,
                "btc_return_7d": 0.30,
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )
    (lc / "lifecycle_run_a.json").write_text(
        json.dumps(
            {
                "new": [{"narrative_key": "alpha theme", "narrative": "alpha theme"}],
                "rising": [],
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )

    (snap / "run_b.json").write_text(
        json.dumps(
            {
                "narratives": [
                    {"narrative": "alpha theme", "narrative_strength": 11.0},
                ],
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )
    (fwd / "forward_returns_run_b.json").write_text(
        json.dumps(
            {
                "btc_return_1d": -0.05,
                "btc_return_3d": None,
                "btc_return_7d": 0.15,
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )
    (lc / "lifecycle_run_b.json").write_text(
        json.dumps({"new": [], "rising": []}, ensure_ascii=False),
        encoding="utf-8",
    )

    out = agg.build_aggregate_payload(snap, fwd, lc, generated_at="2020-01-01T00:00:00+00:00")
    assert out["runs_with_snapshots"] == 2
    assert out["runs_with_forward_returns"] == 2

    by_key = {n["narrative_key"]: n for n in out["narratives"]}
    alpha = by_key["alpha theme"]
    assert alpha["occurrences"] == 2
    assert alpha["runs_tagged_new"] == 1
    assert alpha["avg_btc_return_1d"] == pytest.approx(0.025)
    assert alpha["positive_rate_1d"] == pytest.approx(0.5)
    assert alpha["count_with_returns_3d"] == 1
    assert alpha["avg_btc_return_3d"] == pytest.approx(0.20)


def test_missing_forward_returns_graceful(agg, tmp_path):
    snap = tmp_path / "snapshots"
    fwd = tmp_path / "forward"
    lc = tmp_path / "lifecycle"
    snap.mkdir()
    fwd.mkdir()
    lc.mkdir()

    (snap / "solo.json").write_text(
        json.dumps(
            {"narratives": [{"narrative": "x", "narrative_strength": 1.0}]},
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )

    out = agg.build_aggregate_payload(snap, fwd, lc, generated_at="2020-01-01T00:00:00+00:00")
    assert out["runs_with_snapshots"] == 1
    assert out["runs_with_forward_returns"] == 0
    row = out["narratives"][0]
    assert row["occurrences"] == 1
    assert row["avg_btc_return_1d"] is None
    assert row["positive_rate_1d"] is None
