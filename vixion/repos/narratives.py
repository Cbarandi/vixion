"""Narrativas, links y proyección narrative_current."""

from __future__ import annotations

from datetime import datetime
from typing import Any
from uuid import UUID

import psycopg
from psycopg.rows import dict_row
from psycopg.types.json import Json

from vixion.constants import MAX_COSINE_DISTANCE_FOR_ASSIGN


def find_best_matching_narrative(
    conn: psycopg.Connection,
    *,
    vector_literal: str,
    embedding_model_version: str,
) -> UUID | None:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT nc.narrative_id,
                   (nc.rep_embedding <=> %s::vector) AS dist
            FROM narrative_current nc
            JOIN narratives n ON n.id = nc.narrative_id
            WHERE nc.rep_embedding IS NOT NULL
              AND nc.state IS DISTINCT FROM 'dormant'::narrative_state
              AND n.embedding_model_version = %s
            ORDER BY nc.rep_embedding <=> %s::vector ASC
            LIMIT 1
            """,
            (vector_literal, embedding_model_version, vector_literal),
        )
        row = cur.fetchone()
        if not row:
            return None
        nid, dist = row[0], float(row[1])
        if dist <= MAX_COSINE_DISTANCE_FOR_ASSIGN:
            return UUID(str(nid))
        return None


def insert_narrative(
    conn: psycopg.Connection,
    *,
    embedding_model_id: int,
    embedding_model_version: str,
    cluster_policy_version: str,
) -> UUID:
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO narratives (embedding_model_id, embedding_model_version, cluster_policy_version)
            VALUES (%s, %s, %s)
            RETURNING id
            """,
            (embedding_model_id, embedding_model_version, cluster_policy_version),
        )
        return UUID(str(cur.fetchone()[0]))


def link_item_to_narrative(
    conn: psycopg.Connection,
    *,
    narrative_id: UUID,
    item_id: int,
    similarity: float | None,
) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO narrative_item_links (narrative_id, item_id, similarity_to_rep_at_link)
            VALUES (%s, %s, %s)
            """,
            (str(narrative_id), item_id, similarity),
        )


def count_narrative_items(conn: psycopg.Connection, narrative_id: UUID) -> int:
    with conn.cursor() as cur:
        cur.execute(
            "SELECT count(*)::int FROM narrative_item_links WHERE narrative_id = %s",
            (str(narrative_id),),
        )
        return int(cur.fetchone()[0])


def get_narrative_current(conn: psycopg.Connection, narrative_id: UUID) -> dict[str, Any] | None:
    with conn.cursor(row_factory=dict_row) as cur:
        cur.execute(
            "SELECT * FROM narrative_current WHERE narrative_id = %s",
            (str(narrative_id),),
        )
        return cur.fetchone()


def update_narrative_current_first_item(
    conn: psycopg.Connection,
    *,
    narrative_id: UUID,
    title: str,
    published_at: datetime | None,
    vector_literal: str,
    source_dist: dict[str, int],
) -> None:
    """Un solo UPDATE: primer ítem + rep + rep_version=1 (invariante PRIME)."""
    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE narrative_current SET
                item_count = 1,
                rep_embedding = %s::vector,
                rep_version = 1,
                current_title = %s,
                first_item_published_at = COALESCE(%s, now()),
                last_item_published_at = COALESCE(%s, now()),
                last_item_ingested_at = now(),
                last_rep_computed_at = now(),
                source_dist = %s::jsonb,
                updated_at = now()
            WHERE narrative_id = %s
              AND item_count = 0
              AND rep_version = 0
            """,
            (
                vector_literal,
                title[:500],
                published_at,
                published_at,
                Json(source_dist),
                str(narrative_id),
            ),
        )
        if cur.rowcount != 1:
            raise RuntimeError(
                "update_narrative_current_first_item no aplicó fila (estado narrative_current inesperado)."
            )


def update_narrative_current_more_items(
    conn: psycopg.Connection,
    *,
    narrative_id: UUID,
    published_at: datetime | None,
    source_dist: dict[str, int],
) -> None:
    """
    Ítems siguientes: metadatos + `item_count` desde COUNT(links).

    PRIME Fase 1: la fuente de verdad de cardinalidad es `narrative_item_links`;
    recalcular con subconsulta evita deriva si un futuro bug dejara `item_count`
    desalineado. Coste O(n) por UPDATE — aceptable antes de ingesta masiva;
    alternativa incremental (item_count+1) es más barata pero menos robusta.
    """
    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE narrative_current nc SET
                item_count = sub.cnt,
                last_item_published_at = COALESCE(%s, nc.last_item_published_at),
                last_item_ingested_at = now(),
                source_dist = %s::jsonb,
                updated_at = now()
            FROM (
                SELECT count(*)::int AS cnt FROM narrative_item_links WHERE narrative_id = %s
            ) sub
            WHERE nc.narrative_id = %s
            """,
            (published_at, Json(source_dist), str(narrative_id), str(narrative_id)),
        )


def fetch_item_vectors_text(
    conn: psycopg.Connection, narrative_id: UUID, embedding_model_version: str, limit: int
) -> list[str]:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT ie.vector::text
            FROM narrative_item_links nil
            JOIN item_embeddings ie ON ie.item_id = nil.item_id
            WHERE nil.narrative_id = %s
              AND ie.embedding_model_version = %s
            ORDER BY nil.linked_at DESC
            LIMIT %s
            """,
            (str(narrative_id), embedding_model_version, limit),
        )
        return [r[0] for r in cur.fetchall()]


def update_rep_after_batch(
    conn: psycopg.Connection,
    *,
    narrative_id: UUID,
    vector_literal: str,
    based_on_sample: dict[str, Any],
) -> int:
    """Incrementa rep_version, actualiza rep, historial append-only (caller inserta history)."""
    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE narrative_current SET
                rep_embedding = %s::vector,
                rep_version = rep_version + 1,
                last_rep_computed_at = now(),
                updated_at = now()
            WHERE narrative_id = %s
            RETURNING rep_version
            """,
            (vector_literal, str(narrative_id)),
        )
        row = cur.fetchone()
        return int(row[0]) if row else -1


def apply_score_and_state(
    conn: psycopg.Connection,
    *,
    narrative_id: UUID,
    score: int,
    state: str,
    trend: str,
    breakdown: dict[str, Any],
    scoring_policy_version: str,
) -> tuple[int, str]:
    """Devuelve (score_antes, state_antes) tras actualizar."""
    cur_row = get_narrative_current(conn, narrative_id)
    if not cur_row:
        raise RuntimeError("narrative_current inexistente")
    prev_score = int(cur_row["score"])
    prev_state = str(cur_row["state"])
    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE narrative_current SET
                score = %s,
                state = %s::narrative_state,
                trend = %s::narrative_trend,
                score_breakdown = %s::jsonb,
                scoring_policy_version = %s,
                scored_at = now(),
                updated_at = now()
            WHERE narrative_id = %s
            """,
            (score, state, trend, Json(breakdown), scoring_policy_version, str(narrative_id)),
        )
    return prev_score, prev_state


def parse_pg_vector_text(s: str) -> list[float]:
    inner = s.strip()
    if inner.startswith("[") and inner.endswith("]"):
        inner = inner[1:-1]
    parts = [p.strip() for p in inner.split(",") if p.strip()]
    return [float(x) for x in parts]
