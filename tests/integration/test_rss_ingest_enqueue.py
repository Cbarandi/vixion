"""Ingesta RSS stub → raw_ingests + jobs PROCESS_ITEM (dedupe)."""

from __future__ import annotations

import uuid
from pathlib import Path

import pytest

from vixion.ingestion.service import run_ingest_tick_payload, run_rss_ingest
from vixion.repos import sources as sources_repo

pytestmark = pytest.mark.integration

FIXTURE = Path(__file__).resolve().parents[1] / "fixtures" / "minimal_feed.xml"


def _stub_fetch(_url: str) -> bytes:
    return FIXTURE.read_bytes()


def test_rss_ingest_raw_ingests_and_jobs_dedupe(db_conn):
    slug = f"rss-fix-{uuid.uuid4().hex[:12]}"
    sid = sources_repo.ensure_rss_feed_source(
        db_conn,
        slug=slug,
        display_name="Fixture RSS",
        rss_url="https://example.invalid/feed.xml",
    )
    st1 = run_rss_ingest(
        db_conn,
        source_id=sid,
        feed_url="https://example.invalid/feed.xml",
        feed_slug=slug,
        fetcher=_stub_fetch,
    )
    assert st1["entries_seen"] == 2
    assert st1["process_item_enqueued_new"] == 2
    assert st1["process_item_job_deduped"] == 0

    with db_conn.cursor() as cur:
        cur.execute(
            """
            SELECT status::text, (stats->>'entries_seen')::int, finished_at IS NOT NULL
            FROM raw_ingests WHERE source_id = %s ORDER BY id DESC LIMIT 1
            """,
            (sid,),
        )
        status, seen, finished = cur.fetchone()
        assert status == "success"
        assert seen == 2
        assert finished is True

        cur.execute(
            """
            SELECT count(*) FROM jobs
            WHERE job_type = 'PROCESS_ITEM'::job_type
              AND payload->>'source_id' = %s
            """,
            (str(sid),),
        )
        assert int(cur.fetchone()[0]) == 2

    st2 = run_rss_ingest(
        db_conn,
        source_id=sid,
        feed_url="https://example.invalid/feed.xml",
        feed_slug=slug,
        fetcher=_stub_fetch,
    )
    assert st2["process_item_enqueued_new"] == 0
    assert st2["process_item_job_deduped"] == 2

    with db_conn.cursor() as cur:
        cur.execute(
            """
            SELECT count(*) FROM jobs
            WHERE job_type = 'PROCESS_ITEM'::job_type
              AND payload->>'source_id' = %s
            """,
            (str(sid),),
        )
        assert int(cur.fetchone()[0]) == 2

        cur.execute("SELECT count(*) FROM raw_ingests WHERE source_id = %s", (sid,))
        assert int(cur.fetchone()[0]) == 2


def test_ingest_tick_by_source_id(db_conn, monkeypatch):
    monkeypatch.setattr(
        "vixion.ingestion.service.rss_client.fetch_feed_bytes",
        lambda _url: FIXTURE.read_bytes(),
    )
    slug = f"rss-tick-{uuid.uuid4().hex[:12]}"
    sid = sources_repo.ensure_rss_feed_source(
        db_conn,
        slug=slug,
        display_name="Tick fixture",
        rss_url="https://example.invalid/tick.xml",
    )
    out = run_ingest_tick_payload(db_conn, {"source_id": sid})
    assert len(out) == 1
    assert out[0]["process_item_enqueued_new"] == 2
