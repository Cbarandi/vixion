"""Pipeline PROCESS_ITEM end-to-end contra PostgreSQL real."""

from __future__ import annotations

import pytest

from vixion.contracts import RawIngestCandidate
from vixion.pipeline import process_item as pi

pytestmark = pytest.mark.integration


def _insert_test_source(conn) -> int:
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO sources (source_kind, name, config)
            VALUES ('rss'::source_kind, 'test-src-pipeline', '{}'::jsonb)
            RETURNING id
            """
        )
        return int(cur.fetchone()[0])


def test_process_item_english_creates_item_narrative_events(db_conn):
    sid = _insert_test_source(db_conn)
    cand = RawIngestCandidate(
        source_id=sid,
        title="AI infra demand surge continues",
        body="Hyperscalers expand capex as cloud growth accelerates across regions.",
        fetched_url=f"https://example.test/news/{sid}/ai-infra",
        native_id=f"t3_test_{sid}_1",
        published_at=None,
    )
    res = pi.process_item(db_conn, cand)
    assert res.status == "completed"
    assert res.item_id
    assert res.narrative_id

    with db_conn.cursor() as cur:
        cur.execute("SELECT processing_stage::text FROM items WHERE id = %s", (res.item_id,))
        assert cur.fetchone()[0] == "completed"

        cur.execute(
            "SELECT count(*) FROM narrative_events WHERE narrative_id = %s",
            (str(res.narrative_id),),
        )
        assert int(cur.fetchone()[0]) >= 3

        cur.execute(
            "SELECT rep_version, item_count, score FROM narrative_current WHERE narrative_id = %s",
            (str(res.narrative_id),),
        )
        rv, ic, sc = cur.fetchone()
        assert int(rv) >= 1
        assert int(ic) == 1
        assert int(sc) > 0

        cur.execute(
            """
            SELECT reason::text, score_breakdown->>'snapshot_kind'
            FROM narrative_snapshots
            WHERE narrative_id = %s
            ORDER BY id ASC
            """,
            (str(res.narrative_id),),
        )
        rows = cur.fetchall()
        assert rows[0][0] == "scheduled"
        assert rows[0][1] == "technical_birth"
        reasons = {r[0] for r in rows}
        assert "threshold" in reasons, "debe existir snapshot operativo post-scoring"


def test_process_item_spanish_skipped_non_english(db_conn):
    sid = _insert_test_source(db_conn)
    cand = RawIngestCandidate(
        source_id=sid,
        title="Gobierno anuncia medidas",
        body=(
            "El gobierno anunció hoy medidas importantes para la economía nacional. "
            "Los ministros explicaron los detalles en rueda de prensa y respondieron "
            "a las preguntas de los periodistas durante más de una hora."
        ),
        fetched_url=f"https://example.test/es/{sid}",
        native_id=f"t3_test_{sid}_es",
        published_at=None,
    )
    res = pi.process_item(db_conn, cand)
    assert res.status == "skipped_non_english"
    assert res.item_id
    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT processing_stage::text, content_locale_status::text FROM items WHERE id = %s",
            (res.item_id,),
        )
        st, loc = cur.fetchone()
        assert st == "skipped_non_en"
        assert loc == "rejected_non_en"


def test_hard_dedupe_completed_registers_new_occurrence_second_source(db_conn):
    """Mismo canónico (content_hash), otra fuente / fingerprint → nueva fila occurrence."""
    sid_a = _insert_test_source(db_conn)
    sid_b = _insert_test_source(db_conn)
    title = "Shared headline for syndication test"
    body = "Identical body text for hash collision across sources."
    r1 = pi.process_item(
        db_conn,
        RawIngestCandidate(
            source_id=sid_a,
            title=title,
            body=body,
            fetched_url=f"https://a.example/s/{sid_a}",
            native_id=f"na_{sid_a}",
            published_at=None,
        ),
    )
    assert r1.status == "completed"
    iid = r1.item_id
    r2 = pi.process_item(
        db_conn,
        RawIngestCandidate(
            source_id=sid_b,
            title=title,
            body=body,
            fetched_url=f"https://b.example/s/{sid_b}",
            native_id=f"nb_{sid_b}",
            published_at=None,
        ),
    )
    assert r2.status == "skipped_duplicate"
    assert r2.item_id == iid
    assert r2.extra.get("occurrence_registered") is True
    with db_conn.cursor() as cur:
        cur.execute("SELECT count(*) FROM item_occurrences WHERE item_id = %s", (iid,))
        assert int(cur.fetchone()[0]) == 2


def test_idempotent_same_fingerprint_skips_occurrence(db_conn):
    sid = _insert_test_source(db_conn)
    cand = RawIngestCandidate(
        source_id=sid,
        title="Idempotent dup",
        body="Same text twice.",
        fetched_url=f"https://idem.example/{sid}",
        native_id=f"id_{sid}",
        published_at=None,
    )
    assert pi.process_item(db_conn, cand).status == "completed"
    r2 = pi.process_item(db_conn, cand)
    assert r2.status == "skipped_duplicate"
    assert r2.extra.get("occurrence_registered") is False
    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT count(*) FROM item_occurrences WHERE item_id = %s",
            (r2.item_id,),
        )
        assert int(cur.fetchone()[0]) == 1
