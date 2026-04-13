"""Traza `raw_ingests` por corrida de ingesta."""

from __future__ import annotations

from typing import Any

import psycopg
from psycopg.types.json import Json


def start_raw_ingest(conn: psycopg.Connection, *, source_id: int) -> int:
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO raw_ingests (source_id, status, stats)
            VALUES (%s, 'running'::ingest_status, '{}'::jsonb)
            RETURNING id
            """,
            (source_id,),
        )
        return int(cur.fetchone()[0])


def finish_raw_ingest(
    conn: psycopg.Connection,
    *,
    raw_ingest_id: int,
    status: str,
    stats: dict[str, Any],
    error_message: str | None = None,
) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE raw_ingests SET
                finished_at = now(),
                status = %s::ingest_status,
                stats = %s::jsonb,
                error_message = %s
            WHERE id = %s
            """,
            (status, Json(stats), error_message, raw_ingest_id),
        )
