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
