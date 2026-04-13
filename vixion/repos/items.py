"""Repositorio de ítems y ocurrencias."""

from __future__ import annotations

from datetime import datetime
from typing import Any

import psycopg
from psycopg.rows import dict_row
from psycopg.types.json import Json


def find_item_by_native_occurrence(
    conn: psycopg.Connection, source_id: int, native_id: str
) -> int | None:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT io.item_id FROM item_occurrences io
            WHERE io.source_id = %s AND io.native_id = %s
            LIMIT 1
            """,
            (source_id, native_id),
        )
        r = cur.fetchone()
        return int(r[0]) if r else None


def find_item_by_canonical_url(conn: psycopg.Connection, canonical_url: str) -> int | None:
    with conn.cursor() as cur:
        cur.execute(
            "SELECT id FROM items WHERE canonical_url = %s LIMIT 1",
            (canonical_url,),
        )
        r = cur.fetchone()
        return int(r[0]) if r else None


def find_item_by_content_hash(conn: psycopg.Connection, content_hash: str) -> int | None:
    with conn.cursor() as cur:
        cur.execute(
            "SELECT id FROM items WHERE content_hash = %s LIMIT 1",
            (content_hash,),
        )
        r = cur.fetchone()
        return int(r[0]) if r else None


def get_item_row(conn: psycopg.Connection, item_id: int) -> dict[str, Any] | None:
    with conn.cursor(row_factory=dict_row) as cur:
        cur.execute("SELECT * FROM items WHERE id = %s", (item_id,))
        return cur.fetchone()


def occurrence_exists(conn: psycopg.Connection, item_id: int, fingerprint: str) -> bool:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT 1 FROM item_occurrences
            WHERE item_id = %s AND occurrence_fingerprint = %s
            """,
            (item_id, fingerprint),
        )
        return cur.fetchone() is not None


def insert_item(
    conn: psycopg.Connection,
    *,
    canonical_url: str | None,
    content_hash: str,
    source_native_id: str | None,
    title: str,
    body_text: str,
    language: str,
    content_locale_status: str,
    primary_source_id: int,
    dedupe_kind: str,
    processing_stage: str,
) -> int:
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO items (
                canonical_url, content_hash, source_native_id, title, body_text,
                language, content_locale_status, primary_source_id, dedupe_kind, processing_stage
            )
            VALUES (
                %s, %s, %s, %s, %s,
                %s::language_code, %s::content_locale_status, %s, %s::dedupe_kind, %s::item_processing_stage
            )
            RETURNING id
            """,
            (
                canonical_url,
                content_hash,
                source_native_id,
                title,
                body_text,
                language,
                content_locale_status,
                primary_source_id,
                dedupe_kind,
                processing_stage,
            ),
        )
        return int(cur.fetchone()[0])


def insert_occurrence(
    conn: psycopg.Connection,
    *,
    item_id: int,
    source_id: int,
    raw_ingest_id: int | None,
    fetched_url: str | None,
    published_at: datetime | None,
    native_id: str | None,
    fingerprint: str,
) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO item_occurrences (
                item_id, source_id, raw_ingest_id, fetched_url, published_at, native_id, occurrence_fingerprint
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (item_id, occurrence_fingerprint) DO NOTHING
            """,
            (item_id, source_id, raw_ingest_id, fetched_url, published_at, native_id, fingerprint),
        )


def touch_item_last_seen(conn: psycopg.Connection, item_id: int) -> None:
    with conn.cursor() as cur:
        cur.execute(
            "UPDATE items SET last_seen_at = now(), updated_at = now() WHERE id = %s",
            (item_id,),
        )


def update_item_stage(conn: psycopg.Connection, item_id: int, stage: str) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE items SET processing_stage = %s::item_processing_stage, updated_at = now()
            WHERE id = %s
            """,
            (stage, item_id),
        )


def insert_nlp_profile(
    conn: psycopg.Connection,
    *,
    item_id: int,
    nlp_model_version: str,
    content_type: str,
    sentiment: float,
    intensity: float,
    topics: list[str],
) -> int:
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO item_nlp_profiles (
                item_id, nlp_model_version, content_type, sentiment, intensity, topics, extra
            )
            VALUES (
                %s, %s, %s::nlp_content_type, %s, %s, %s::jsonb, '{}'::jsonb
            )
            ON CONFLICT (item_id, nlp_model_version) DO UPDATE SET
                content_type = EXCLUDED.content_type,
                sentiment = EXCLUDED.sentiment,
                intensity = EXCLUDED.intensity,
                topics = EXCLUDED.topics,
                processed_at = now()
            RETURNING id
            """,
            (item_id, nlp_model_version, content_type, sentiment, intensity, Json(topics)),
        )
        return int(cur.fetchone()[0])


def insert_embedding(
    conn: psycopg.Connection,
    *,
    item_id: int,
    embedding_model_id: int,
    embedding_model_version: str,
    vector_literal: str,
) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO item_embeddings (item_id, embedding_model_id, embedding_model_version, vector)
            VALUES (%s, %s, %s, %s::vector)
            ON CONFLICT (item_id, embedding_model_id, embedding_model_version) DO UPDATE SET
                vector = EXCLUDED.vector,
                created_at = now()
            """,
            (item_id, embedding_model_id, embedding_model_version, vector_literal),
        )


def get_embedding_model_id(conn: psycopg.Connection) -> int:
    with conn.cursor() as cur:
        cur.execute(
            "SELECT id FROM embedding_models WHERE active ORDER BY id LIMIT 1"
        )
        r = cur.fetchone()
        if not r:
            raise RuntimeError("No hay embedding_models activos (migración v0).")
        return int(r[0])
