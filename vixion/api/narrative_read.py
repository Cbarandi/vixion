"""Consultas de lectura sobre narrative_current y tablas relacionadas."""

from __future__ import annotations

from datetime import datetime
from typing import Any
from uuid import UUID

import psycopg
from psycopg.rows import dict_row

_SOURCES_SUBQUERY = """
    coalesce(
        (
            SELECT string_agg(x.n, ', ' ORDER BY x.n)
            FROM (
                SELECT DISTINCT s.name AS n
                FROM narrative_item_links nil
                JOIN items i ON i.id = nil.item_id
                LEFT JOIN sources s ON s.id = i.primary_source_id
                WHERE nil.narrative_id = nc.narrative_id
                  AND s.name IS NOT NULL
            ) x
        ),
        ''
    )
"""


def _list_base_select() -> str:
    return f"""
        SELECT
            nc.narrative_id::text AS id,
            nc.current_title AS title,
            nc.score::int AS score,
            nc.state::text AS state,
            nc.trend::text AS trend,
            nc.item_count::int AS item_count,
            nc.updated_at,
        {_SOURCES_SUBQUERY} AS sources
        FROM narrative_current nc
        WHERE 1 = 1
    """


def list_narratives(
    conn: psycopg.Connection,
    *,
    state: str | None,
    min_score: int | None,
    updated_since: datetime | None,
    include_dormant: bool,
    limit: int,
    offset: int,
    order_mode: str,
) -> list[dict[str, Any]]:
    sql = _list_base_select()
    params: list[Any] = []
    if not include_dormant:
        sql += " AND nc.state IS DISTINCT FROM 'dormant'::narrative_state"
    if state is not None:
        params.append(state)
        sql += " AND nc.state = %s::narrative_state"
    if min_score is not None:
        params.append(min_score)
        sql += " AND nc.score >= %s"
    if updated_since is not None:
        params.append(updated_since)
        sql += " AND nc.updated_at >= %s"
    if order_mode == "top":
        sql += " ORDER BY nc.score DESC, nc.updated_at DESC"
    else:
        sql += " ORDER BY nc.updated_at DESC"
    params.extend([limit, offset])
    sql += " LIMIT %s OFFSET %s"
    with conn.cursor(row_factory=dict_row) as cur:
        cur.execute(sql, params)
        return [dict(r) for r in cur.fetchall()]


def get_narrative_created_at(conn: psycopg.Connection, narrative_id: UUID) -> datetime | None:
    with conn.cursor() as cur:
        cur.execute("SELECT created_at FROM narratives WHERE id = %s", (str(narrative_id),))
        row = cur.fetchone()
        if not row:
            return None
        return row[0]


def fetch_current_block(conn: psycopg.Connection, narrative_id: UUID) -> dict[str, Any] | None:
    with conn.cursor(row_factory=dict_row) as cur:
        cur.execute(
            """
            SELECT
                nc.narrative_id::text AS narrative_id,
                nc.current_title AS title,
                nc.score::int AS score,
                nc.state::text AS state,
                nc.trend::text AS trend,
                nc.item_count::int AS item_count,
                nc.rep_version::int AS rep_version,
                nc.updated_at,
                nc.scored_at,
                coalesce(nc.source_dist, '{}'::jsonb) AS source_dist
            FROM narrative_current nc
            WHERE nc.narrative_id = %s
            """,
            (str(narrative_id),),
        )
        return cur.fetchone()


def list_events(
    conn: psycopg.Connection, narrative_id: UUID, *, limit: int
) -> list[dict[str, Any]]:
    with conn.cursor(row_factory=dict_row) as cur:
        cur.execute(
            """
            SELECT
                ne.id,
                ne.event_type::text AS event_type,
                ne.occurred_at,
                ne.related_item_id,
                coalesce(ne.payload, '{}'::jsonb) AS payload,
                ne.score_before::int AS score_before,
                ne.score_after::int AS score_after,
                ne.state_before::text AS state_before,
                ne.state_after::text AS state_after
            FROM narrative_events ne
            WHERE ne.narrative_id = %s
            ORDER BY ne.occurred_at DESC, ne.id DESC
            LIMIT %s
            """,
            (str(narrative_id), limit),
        )
        return [dict(r) for r in cur.fetchall()]


def list_snapshots(
    conn: psycopg.Connection, narrative_id: UUID, *, limit: int
) -> list[dict[str, Any]]:
    with conn.cursor(row_factory=dict_row) as cur:
        cur.execute(
            """
            SELECT
                ns.id,
                ns.snapshot_ts_utc,
                ns.reason::text AS reason,
                ns.score::int AS score,
                ns.state::text AS state,
                ns.trend::text AS trend,
                ns.item_count::int AS item_count,
                ns.fingerprint
            FROM narrative_snapshots ns
            WHERE ns.narrative_id = %s
            ORDER BY ns.snapshot_ts_utc DESC, ns.id DESC
            LIMIT %s
            """,
            (str(narrative_id), limit),
        )
        return [dict(r) for r in cur.fetchall()]


def list_narrative_items(
    conn: psycopg.Connection, narrative_id: UUID, *, limit: int = 200
) -> list[dict[str, Any]]:
    """Ítems enlazados a la narrativa (orden por linked_at descendente)."""
    with conn.cursor(row_factory=dict_row) as cur:
        cur.execute(
            """
            SELECT
                nil.item_id::bigint AS item_id,
                coalesce(i.title, '') AS title,
                coalesce(i.canonical_url, occ.fetched_url) AS url,
                occ.published_at,
                coalesce(s.name, '') AS source_name,
                nil.linked_at,
                nlp.sentiment AS sentiment,
                nlp.intensity AS intensity
            FROM narrative_item_links nil
            JOIN items i ON i.id = nil.item_id
            LEFT JOIN sources s ON s.id = i.primary_source_id
            LEFT JOIN LATERAL (
                SELECT io.fetched_url, io.published_at
                FROM item_occurrences io
                WHERE io.item_id = i.id
                ORDER BY io.ingested_at DESC
                LIMIT 1
            ) occ ON true
            LEFT JOIN LATERAL (
                SELECT inp.sentiment, inp.intensity
                FROM item_nlp_profiles inp
                WHERE inp.item_id = i.id
                ORDER BY inp.processed_at DESC NULLS LAST, inp.id DESC
                LIMIT 1
            ) nlp ON true
            WHERE nil.narrative_id = %s
            ORDER BY nil.linked_at DESC
            LIMIT %s
            """,
            (str(narrative_id), limit),
        )
        return [dict(r) for r in cur.fetchall()]


def fetch_latest_review(conn: psycopg.Connection, narrative_id: UUID) -> dict[str, Any] | None:
    with conn.cursor(row_factory=dict_row) as cur:
        cur.execute(
            """
            SELECT
                nr.id,
                nr.verdict::text AS verdict,
                nr.reason_code::text AS reason_code,
                nr.notes,
                nr.reviewer,
                nr.reviewed_at
            FROM narrative_reviews nr
            WHERE nr.narrative_id = %s
            ORDER BY nr.reviewed_at DESC, nr.id DESC
            LIMIT 1
            """,
            (str(narrative_id),),
        )
        return cur.fetchone()
