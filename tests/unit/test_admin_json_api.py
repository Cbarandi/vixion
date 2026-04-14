"""Smoke del mini-API en app.main (panel admin / JSON scored)."""

from __future__ import annotations

from fastapi.testclient import TestClient

from app.main import app


def test_narrative_history_latest_returns_shape() -> None:
    with TestClient(app) as client:
        r = client.get("/narrative-history/latest")
    assert r.status_code == 200
    data = r.json()
    assert set(data.keys()) >= {
        "lifecycle",
        "diff_meta",
        "lifecycle_source",
        "diff_source",
    }


def test_outcomes_narrative_aggregates_latest_returns_shape() -> None:
    with TestClient(app) as client:
        r = client.get("/outcomes/narrative-aggregates/latest")
    assert r.status_code == 200
    data = r.json()
    assert set(data.keys()) >= {"aggregate", "source_file"}


def test_outcomes_narrative_edge_latest_returns_shape() -> None:
    with TestClient(app) as client:
        r = client.get("/outcomes/narrative-edge/latest")
    assert r.status_code == 200
    data = r.json()
    assert set(data.keys()) >= {"ranking", "source_file"}


def test_narrative_history_diff_movers_latest_returns_shape() -> None:
    with TestClient(app) as client:
        r = client.get("/narrative-history/diff-movers/latest")
    assert r.status_code == 200
    data = r.json()
    assert set(data.keys()) >= {"movers", "source_file"}


def test_narrative_history_snapshot_timelines_latest_returns_shape() -> None:
    with TestClient(app) as client:
        r = client.get("/narrative-history/snapshot-timelines/latest")
    assert r.status_code == 200
    data = r.json()
    assert set(data.keys()) >= {"runs", "timelines", "meta", "source_runs_index"}
