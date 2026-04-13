"""Consultas y resúmenes operativos (sin servicios nuevos)."""

from __future__ import annotations

import os
from collections import defaultdict
from collections.abc import Iterator
from typing import Any

import psycopg
from psycopg.rows import dict_row


def _env_int(name: str, default: int) -> int:
    raw = os.environ.get(name)
    if raw is None or not raw.strip():
        return default
    try:
        return int(raw)
    except ValueError:
        return default


def thresholds() -> dict[str, int]:
    return {
        "pending_jobs": _env_int("VIXION_OPS_PENDING_WARN", 200),
        "failed_jobs": _env_int("VIXION_OPS_FAILED_WARN", 20),
        "dead_jobs": _env_int("VIXION_OPS_DEAD_WARN", 5),
        "ingest_non_success_streak": _env_int("VIXION_OPS_INGEST_BAD_STREAK", 3),
    }


def fetch_jobs_summary(conn: psycopg.Connection) -> list[dict[str, Any]]:
    with conn.cursor(row_factory=dict_row) as cur:
        cur.execute(
            """
            SELECT job_type::text AS job_type, status::text AS status, count(*)::bigint AS n
            FROM jobs
            GROUP BY job_type, status
            ORDER BY job_type, status
            """
        )
        return [dict(r) for r in cur.fetchall()]


def fetch_oldest_pending_by_type(conn: psycopg.Connection) -> list[dict[str, Any]]:
    with conn.cursor(row_factory=dict_row) as cur:
        cur.execute(
            """
            SELECT
                job_type::text AS job_type,
                min(created_at) AS oldest_created_at,
                extract(epoch FROM (now() - min(created_at))) / 60.0 AS oldest_pending_age_min
            FROM jobs
            WHERE status = 'pending'::job_status
            GROUP BY job_type
            ORDER BY job_type
            """
        )
        return [dict(r) for r in cur.fetchall()]


def fetch_recent_raw_ingests(conn: psycopg.Connection, *, limit: int = 40) -> list[dict[str, Any]]:
    with conn.cursor(row_factory=dict_row) as cur:
        cur.execute(
            """
            SELECT
                ri.id,
                ri.source_id,
                ri.status::text AS status,
                ri.started_at,
                ri.finished_at,
                ri.error_message,
                s.name AS source_name,
                coalesce(s.config->>'slug', '') AS feed_slug,
                ri.stats->>'entries_seen' AS entries_seen,
                ri.stats->>'process_item_enqueued_new' AS enqueued_new,
                ri.stats->>'process_item_job_deduped' AS deduped,
                ri.stats->>'entries_skipped_no_link' AS skipped_no_link,
                CASE
                    WHEN jsonb_typeof(coalesce(ri.stats->'errors', '[]'::jsonb)) = 'array'
                    THEN jsonb_array_length(coalesce(ri.stats->'errors', '[]'::jsonb))
                    ELSE 0
                END AS error_count
            FROM raw_ingests ri
            JOIN sources s ON s.id = ri.source_id
            ORDER BY ri.started_at DESC
            LIMIT %s
            """,
            (limit,),
        )
        return [dict(r) for r in cur.fetchall()]


def fetch_recent_narratives(conn: psycopg.Connection, *, limit: int = 20) -> list[dict[str, Any]]:
    with conn.cursor(row_factory=dict_row) as cur:
        cur.execute(
            """
            SELECT
                nc.narrative_id::text AS narrative_id,
                left(nc.current_title, 120) AS current_title,
                nc.score,
                nc.state::text AS state,
                nc.item_count,
                nc.updated_at,
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
                ) AS source_names
            FROM narrative_current nc
            ORDER BY nc.updated_at DESC NULLS LAST
            LIMIT %s
            """,
            (limit,),
        )
        return [dict(r) for r in cur.fetchall()]


def _runs_per_source_ordered(
    rows: list[dict[str, Any]], *, max_per_source: int = 24
) -> dict[int, list[dict[str, Any]]]:
    """``rows`` más reciente primero globalmente; reagrupa por source conservando orden."""
    out: dict[int, list[dict[str, Any]]] = defaultdict(list)
    for r in rows:
        sid = int(r["source_id"])
        if len(out[sid]) < max_per_source:
            out[sid].append(r)
    return out


def leading_non_success_streak(runs_newest_first: list[dict[str, Any]]) -> int:
    n = 0
    for r in runs_newest_first:
        if str(r.get("status") or "") == "success":
            break
        n += 1
    return n


def collect_signals(conn: psycopg.Connection) -> list[str]:
    """Señales operativas (texto); sin side effects."""
    t = thresholds()
    signals: list[str] = []

    with conn.cursor(row_factory=dict_row) as cur:
        cur.execute(
            "SELECT status::text AS status, count(*)::bigint AS n FROM jobs GROUP BY status"
        )
        counts = {str(r["status"]): int(r["n"]) for r in cur.fetchall()}
    pending = counts.get("pending", 0)
    failed = counts.get("failed", 0)
    dead = counts.get("dead", 0)
    if pending >= t["pending_jobs"]:
        signals.append(f"WARN cola pending alta: {pending} (umbral {t['pending_jobs']})")
    if failed >= t["failed_jobs"]:
        signals.append(f"WARN jobs failed: {failed} (umbral {t['failed_jobs']})")
    if dead >= t["dead_jobs"]:
        signals.append(f"WARN jobs dead: {dead} (umbral {t['dead_jobs']})")

    recent = fetch_recent_raw_ingests(conn, limit=400)
    by_src = _runs_per_source_ordered(recent)
    streak_need = t["ingest_non_success_streak"]
    for sid, runs in by_src.items():
        streak = leading_non_success_streak(runs)
        if streak >= streak_need:
            slug = str(runs[0].get("feed_slug") or "")
            name = str(runs[0].get("source_name") or "")
            signals.append(
                f"WARN ingest racha sin éxito source_id={sid} slug={slug!r} name={name!r}: "
                f"{streak} corridas consecutivas (umbral {streak_need})"
            )

    return signals


def format_lines(
    *,
    jobs_rows: list[dict[str, Any]],
    pending_age: list[dict[str, Any]],
    ingests: list[dict[str, Any]],
    narratives: list[dict[str, Any]],
    signals: list[str],
) -> Iterator[str]:
    yield "=== jobs (conteos por tipo y estado) ==="
    if not jobs_rows:
        yield "(sin filas)"
    else:
        for r in jobs_rows:
            yield f"  {r['job_type']:22} {r['status']:12} {int(r['n']):6}"
    yield ""
    yield "=== jobs pending — antigüedad mínima (min) ==="
    if not pending_age:
        yield "  (sin pending)"
    else:
        for r in pending_age:
            age = r.get("oldest_pending_age_min")
            age_s = f"{float(age):.1f}" if age is not None else "?"
            yield (
                f"  {r['job_type']:22} oldest_created={r['oldest_created_at']} "
                f"age_min≈{age_s}"
            )
    yield ""
    yield "=== raw_ingests recientes (corrida / feed) ==="
    yield (
        "  id     src slug            status   seen  new  dedup skip err "
        "started_at"
    )
    for r in ingests:
        yield (
            f"  {int(r['id']):5} {int(r['source_id']):4} "
            f"{str(r.get('feed_slug') or '')[:16]:16} "
            f"{str(r.get('status')):8} "
            f"{str(r.get('entries_seen') or '-'):>4} "
            f"{str(r.get('enqueued_new') or '-'):>4} "
            f"{str(r.get('deduped') or '-'):>4} "
            f"{str(r.get('skipped_no_link') or '-'):>4} "
            f"{int(r.get('error_count') or 0):>3} "
            f"{r.get('started_at')}"
        )
    yield ""
    yield "=== narrative_current (recientes) ==="
    yield "  narrative_id                         score state      items updated_at"
    for r in narratives:
        yield (
            f"  {str(r.get('narrative_id'))[:36]:36} "
            f"{int(r.get('score') or 0):3} "
            f"{str(r.get('state') or ''):10} "
            f"{int(r.get('item_count') or 0):4} "
            f"{r.get('updated_at')} "
            f"src={str(r.get('source_names') or '')[:60]}"
        )
    yield ""
    yield "=== señales operativas (umbrales vía env VIXION_OPS_*) ==="
    if not signals:
        yield "  (ninguna)"
    else:
        for s in signals:
            yield f"  {s}"
