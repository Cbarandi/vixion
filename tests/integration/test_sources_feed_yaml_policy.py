"""Política: YAML actualiza ``sources`` existentes por slug."""

from __future__ import annotations

import uuid

import pytest

from vixion.repos import sources as sources_repo

pytestmark = pytest.mark.integration


def test_ensure_rss_feed_source_updates_url_on_resync(db_conn):
    slug = f"yaml-pol-{uuid.uuid4().hex[:10]}"
    sid1 = sources_repo.ensure_rss_feed_source(
        db_conn,
        slug=slug,
        display_name="Name A",
        rss_url="https://example.invalid/a.xml",
    )
    sid2 = sources_repo.ensure_rss_feed_source(
        db_conn,
        slug=slug,
        display_name="Name B",
        rss_url="https://example.invalid/b.xml",
    )
    assert sid1 == sid2
    cfg = sources_repo.get_source_config(db_conn, sid1)
    assert cfg["rss_url"] == "https://example.invalid/b.xml"
    with db_conn.cursor() as cur:
        cur.execute("SELECT name FROM sources WHERE id = %s", (sid1,))
        assert cur.fetchone()[0] == "Name B"
