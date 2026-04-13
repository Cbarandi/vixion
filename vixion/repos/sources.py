"""Catálogo `sources` — alta/lookup RSS."""

from __future__ import annotations

from typing import Any

import psycopg
from psycopg.rows import dict_row
from psycopg.types.json import Json


def get_rss_source_by_slug(conn: psycopg.Connection, slug: str) -> dict[str, Any] | None:
    """Slug guardado en config->slug."""
    with conn.cursor(row_factory=dict_row) as cur:
        cur.execute(
            """
            SELECT id, name, config FROM sources
            WHERE source_kind = 'rss'::source_kind
              AND is_enabled = true
              AND config->>'slug' = %s
            LIMIT 1
            """,
            (slug,),
        )
        return cur.fetchone()


def ensure_rss_feed_source(
    conn: psycopg.Connection, *, slug: str, display_name: str, rss_url: str
) -> int:
    """
    Inserta o devuelve `sources.id` para un feed RSS (una fila por ``slug``).

    Política PRIME: el YAML es fuente de verdad. Si ya existe la fila para el
    ``slug``, se actualizan ``name`` y ``config`` (incl. ``rss_url``) para reflejar
    el archivo actual — sin fallar ni dejar URLs obsoletas en silencio.
    """
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT id FROM sources
            WHERE source_kind = 'rss'::source_kind AND config->>'slug' = %s
            LIMIT 1
            """,
            (slug,),
        )
        r = cur.fetchone()
        if r:
            sid = int(r[0])
            cur.execute(
                """
                UPDATE sources
                SET name = %s,
                    config = %s::jsonb,
                    updated_at = now()
                WHERE id = %s
                """,
                (display_name, Json({"slug": slug, "rss_url": rss_url}), sid),
            )
            return sid
        cur.execute(
            """
            INSERT INTO sources (source_kind, name, config, is_enabled)
            VALUES (
                'rss'::source_kind,
                %s,
                %s::jsonb,
                true
            )
            RETURNING id
            """,
            (
                display_name,
                Json({"slug": slug, "rss_url": rss_url}),
            ),
        )
        return int(cur.fetchone()[0])


def get_source_config(conn: psycopg.Connection, source_id: int) -> dict[str, Any]:
    with conn.cursor(row_factory=dict_row) as cur:
        cur.execute("SELECT id, name, config FROM sources WHERE id = %s", (source_id,))
        row = cur.fetchone()
        if not row:
            raise LookupError(f"source id={source_id} no existe")
        cfg = row["config"]
        if isinstance(cfg, str):
            import json

            cfg = json.loads(cfg)
        return dict(cfg or {})


def list_rss_sources(conn: psycopg.Connection) -> list[dict[str, Any]]:
    with conn.cursor(row_factory=dict_row) as cur:
        cur.execute(
            """
            SELECT id, name, config FROM sources
            WHERE source_kind = 'rss'::source_kind AND is_enabled = true
            ORDER BY id
            """
        )
        return [dict(r) for r in cur.fetchall()]
