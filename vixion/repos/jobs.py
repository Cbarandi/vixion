"""Cola Postgres `jobs` — reclamar y cerrar trabajos."""

from __future__ import annotations

import json
import logging
from typing import Any

import psycopg
from psycopg.rows import dict_row
from psycopg.types.json import Json

log = logging.getLogger(__name__)


def enqueue_process_item(
    conn: psycopg.Connection,
    *,
    idempotency_key: str,
    payload: dict[str, Any],
    run_after: str | None = None,
) -> tuple[int, bool]:
    """
    Devuelve (job_id, created).
    ``created=False`` si hubo conflicto por ``idempotency_key`` (job ya existía).
    """
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO jobs (idempotency_key, job_type, payload, status, run_after)
            VALUES (%s, 'PROCESS_ITEM'::job_type, %s::jsonb, 'pending'::job_status, COALESCE(%s, now()))
            ON CONFLICT (idempotency_key) DO NOTHING
            RETURNING id
            """,
            (idempotency_key, Json(payload), run_after),
        )
        r = cur.fetchone()
        if r:
            return int(r[0]), True
        cur.execute("SELECT id FROM jobs WHERE idempotency_key = %s", (idempotency_key,))
        r2 = cur.fetchone()
        if not r2:
            return 0, False
        return int(r2[0]), False


def enqueue_ingest_source_tick(
    conn: psycopg.Connection,
    *,
    idempotency_key: str,
    payload: dict[str, Any],
    run_after: str | None = None,
) -> tuple[int, bool]:
    """Encola un tick de ingesta RSS (``INGEST_SOURCE_TICK``)."""
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO jobs (idempotency_key, job_type, payload, status, run_after)
            VALUES (%s, 'INGEST_SOURCE_TICK'::job_type, %s::jsonb, 'pending'::job_status, COALESCE(%s, now()))
            ON CONFLICT (idempotency_key) DO NOTHING
            RETURNING id
            """,
            (idempotency_key, Json(payload), run_after),
        )
        r = cur.fetchone()
        if r:
            return int(r[0]), True
        cur.execute("SELECT id FROM jobs WHERE idempotency_key = %s", (idempotency_key,))
        r2 = cur.fetchone()
        if not r2:
            return 0, False
        return int(r2[0]), False


def claim_next_job(conn: psycopg.Connection, *, worker_id: str) -> dict[str, Any] | None:
    with conn.cursor(row_factory=dict_row) as cur:
        cur.execute(
            """
            WITH cte AS (
                SELECT id FROM jobs
                WHERE status = 'pending'::job_status AND run_after <= now()
                ORDER BY priority DESC, id
                FOR UPDATE SKIP LOCKED
                LIMIT 1
            )
            UPDATE jobs j
            SET status = 'running'::job_status,
                locked_at = now(),
                locked_by = %s,
                updated_at = now()
            FROM cte
            WHERE j.id = cte.id
            RETURNING j.*
            """,
            (worker_id,),
        )
        return cur.fetchone()


def mark_job_succeeded(conn: psycopg.Connection, job_id: int) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE jobs SET status = 'succeeded'::job_status, locked_at = NULL, locked_by = NULL,
                   last_error = NULL, updated_at = now()
            WHERE id = %s AND status = 'running'::job_status
            """,
            (job_id,),
        )


def mark_job_failed(conn: psycopg.Connection, job_id: int, err: str, *, dead: bool = False) -> None:
    status = "dead" if dead else "failed"
    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE jobs SET status = %s::job_status, last_error = %s, locked_at = NULL, locked_by = NULL,
                   updated_at = now()
            WHERE id = %s
            """,
            (status, err[:8000], job_id),
        )


def bump_job_retry(conn: psycopg.Connection, job_id: int, err: str) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE jobs SET
                attempts = attempts + 1,
                status = CASE WHEN attempts + 1 >= max_attempts THEN 'dead'::job_status ELSE 'pending'::job_status END,
                last_error = %s,
                run_after = now() + ((attempts + 1) * 30) * interval '1 second',
                locked_at = NULL,
                locked_by = NULL,
                updated_at = now()
            WHERE id = %s AND status = 'running'::job_status
            """,
            (err[:8000], job_id),
        )


def parse_process_item_payload(row: dict[str, Any]) -> dict[str, Any]:
    raw = row.get("payload")
    if isinstance(raw, str):
        return json.loads(raw)
    if isinstance(raw, dict):
        return raw
    return dict(raw or {})
