"""Alta de revisiones manuales (narrative_reviews + evento)."""

from __future__ import annotations

from typing import Any
from uuid import UUID

import psycopg

from vixion.repos import journal


def insert_narrative_review(
    conn: psycopg.Connection,
    *,
    narrative_id: UUID,
    verdict: str,
    reason_code: str,
    notes: str | None,
    reviewer: str,
) -> int:
    """
    Inserta fila en ``narrative_reviews`` y registra ``REVIEW_RECORDED`` en el journal.
    """
    with conn.transaction():
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO narrative_reviews (narrative_id, verdict, reason_code, notes, reviewer)
                VALUES (%s, %s::review_verdict, %s::review_reason_code, %s, %s)
                RETURNING id
                """,
                (str(narrative_id), verdict, reason_code, notes, reviewer),
            )
            rid = int(cur.fetchone()[0])

        payload: dict[str, Any] = {
            "review_id": rid,
            "verdict": verdict,
            "reason_code": reason_code,
        }
        journal.insert_narrative_event(
            conn,
            narrative_id=narrative_id,
            event_type="REVIEW_RECORDED",
            payload=payload,
        )
    return rid
