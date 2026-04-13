"""E2E: INGEST_SOURCE_TICK → PROCESS_ITEM → items / links / narrative_current / events."""

from __future__ import annotations

import textwrap
import uuid
from pathlib import Path

import pytest

from vixion.ingestion.feeds_config import load_feed_specs
from vixion.ingestion.keys import ingest_tick_job_key
from vixion.ingestion.service import sync_feed_sources_from_config
from vixion.repos import jobs as jobs_repo
from vixion.workers.dispatch import dispatch_claimed_job

pytestmark = pytest.mark.integration

FIXTURE = Path(__file__).resolve().parents[1] / "fixtures" / "minimal_feed.xml"


def _drain_pending_jobs(db_conn, *, worker_id: str, max_jobs: int = 32) -> int:
    n = 0
    for _ in range(max_jobs):
        job = jobs_repo.claim_next_job(db_conn, worker_id=worker_id)
        if not job:
            break
        jid = int(job["id"])
        dispatch_claimed_job(db_conn, job)
        jobs_repo.mark_job_succeeded(db_conn, jid)
        n += 1
    return n


def test_rss_tick_to_narratives_e2e(db_conn, monkeypatch, tmp_path):
    monkeypatch.setattr(
        "vixion.ingestion.service.rss_client.fetch_feed_bytes",
        lambda _url: FIXTURE.read_bytes(),
    )
    slug = f"e2e-rss-{uuid.uuid4().hex[:12]}"
    yml = tmp_path / "feeds.yaml"
    yml.write_text(
        textwrap.dedent(
            f"""
            feeds:
              - slug: {slug}
                name: "E2E fixture feed"
                url: "https://example.invalid/e2e.xml"
            """
        ).strip(),
        encoding="utf-8",
    )
    feeds = load_feed_specs(yml)
    assert len(feeds) == 1
    slug_to_id = sync_feed_sources_from_config(db_conn, feeds)
    sid = slug_to_id[slug]

    # source_id (no run_all): la DB de integración puede tener otras fuentes RSS ya persistidas.
    window = f"e2e-win-{uuid.uuid4().hex[:16]}"
    ikey = ingest_tick_job_key(sid, window)
    _jid, created = jobs_repo.enqueue_ingest_source_tick(
        db_conn, idempotency_key=ikey, payload={"source_id": sid}
    )
    assert created is True

    processed = _drain_pending_jobs(db_conn, worker_id="pytest-e2e")
    assert processed >= 1

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT count(*) FROM items WHERE primary_source_id = %s",
            (sid,),
        )
        assert int(cur.fetchone()[0]) == 2

        cur.execute(
            "SELECT count(*) FROM item_occurrences WHERE source_id = %s",
            (sid,),
        )
        assert int(cur.fetchone()[0]) == 2

        cur.execute(
            """
            SELECT count(*) FROM narrative_item_links nil
            JOIN items i ON i.id = nil.item_id
            WHERE i.primary_source_id = %s
            """,
            (sid,),
        )
        assert int(cur.fetchone()[0]) == 2

        cur.execute(
            """
            SELECT count(*) FROM narrative_current nc
            JOIN narrative_item_links nil ON nil.narrative_id = nc.narrative_id
            JOIN items i ON i.id = nil.item_id
            WHERE i.primary_source_id = %s
            """,
            (sid,),
        )
        assert int(cur.fetchone()[0]) == 2

        cur.execute(
            """
            SELECT count(*) FROM narrative_events ne
            JOIN narrative_item_links nil ON nil.narrative_id = ne.narrative_id
            JOIN items i ON i.id = nil.item_id
            WHERE i.primary_source_id = %s
            """,
            (sid,),
        )
        assert int(cur.fetchone()[0]) >= 2

        cur.execute(
            """
            SELECT count(*) FROM narrative_snapshots ns
            JOIN narrative_item_links nil ON nil.narrative_id = ns.narrative_id
            JOIN items i ON i.id = nil.item_id
            WHERE i.primary_source_id = %s
            """,
            (sid,),
        )
        assert int(cur.fetchone()[0]) >= 1
