"""Comprueba que el admin incluye los controles de filtros narrativa."""

from __future__ import annotations

from pathlib import Path


def test_admin_index_has_narrative_filter_ids() -> None:
    root = Path(__file__).resolve().parents[2]
    html = (root / "admin" / "index.html").read_text(encoding="utf-8")
    for id_ in (
        "narFilterLifecycle",
        "narFilterMinOcc",
        "narFilterMinEdge",
        "narFilterMaxRows",
        "narFilterSort",
    ):
        assert f'id="{id_}"' in html, f"missing #{id_}"
