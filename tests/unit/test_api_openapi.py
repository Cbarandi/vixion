"""OpenAPI sin levantar Postgres."""

from __future__ import annotations


def test_openapi_lists_narrative_paths():
    from vixion.api.main import app

    paths = app.openapi()["paths"]
    assert "/health" in paths
    assert "/narratives" in paths
    assert "/narratives/top" in paths
    assert "/narratives/{narrative_id}/items" in paths
    assert "/narratives/{narrative_id}/reviews" in paths
    assert "/narratives/{narrative_id}" in paths
