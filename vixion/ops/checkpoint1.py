"""Checkpoint 1: export de reviews, resumen y muestra de narrativas a revisar."""

from __future__ import annotations

import json
from collections import Counter
from collections.abc import Iterator
from datetime import datetime
from typing import Any, TextIO

import psycopg
from psycopg.rows import dict_row


EXPORT_COLUMNS = (
    "review_id",
    "narrative_id",
    "verdict",
    "reason_code",
    "reviewer",
    "reviewed_at",
    "notes",
    "narrative_score",
    "narrative_state",
    "item_count",
    "title",
    "narrative_updated_at",
)


def fetch_reviews_for_export(conn: psycopg.Connection) -> list[dict[str, Any]]:
    """Reviews con proyección actual de ``narrative_current`` (útil para informe operativo)."""
    with conn.cursor(row_factory=dict_row) as cur:
        cur.execute(
            """
            SELECT
                nr.id AS review_id,
                nr.narrative_id::text AS narrative_id,
                nr.verdict::text AS verdict,
                nr.reason_code::text AS reason_code,
                nr.reviewer,
                nr.reviewed_at,
                nr.notes,
                nc.score::int AS narrative_score,
                nc.state::text AS narrative_state,
                nc.item_count::int AS item_count,
                coalesce(nc.current_title, '') AS title,
                nc.updated_at AS narrative_updated_at
            FROM narrative_reviews nr
            INNER JOIN narrative_current nc ON nc.narrative_id = nr.narrative_id
            ORDER BY nr.reviewed_at DESC, nr.id DESC
            """
        )
        return [dict(r) for r in cur.fetchall()]


def fetch_sample_narratives(
    conn: psycopg.Connection,
    *,
    limit: int,
    min_item_count: int,
    include_dormant: bool,
) -> list[dict[str, Any]]:
    """Muestra guiada: narrativas recientes con filtros mínimos."""
    sql = """
        SELECT
            nc.narrative_id::text AS narrative_id,
            coalesce(nc.current_title, '') AS title,
            nc.score::int AS score,
            nc.state::text AS state,
            nc.item_count::int AS item_count,
            nc.updated_at
        FROM narrative_current nc
        WHERE nc.item_count >= %s
    """
    params: list[Any] = [min_item_count]
    if not include_dormant:
        sql += " AND nc.state IS DISTINCT FROM 'dormant'::narrative_state"
    sql += " ORDER BY nc.updated_at DESC NULLS LAST LIMIT %s"
    params.append(limit)
    with conn.cursor(row_factory=dict_row) as cur:
        cur.execute(sql, params)
        return [dict(r) for r in cur.fetchall()]


def score_band(score: int) -> str:
    if score < 25:
        return "0-24"
    if score < 50:
        return "25-49"
    if score < 75:
        return "50-74"
    return "75-100"


def summarize_reviews(rows: list[dict[str, Any]]) -> dict[str, Any]:
    """Agregados para texto/JSON (sin DB)."""
    n = len(rows)
    verdicts = Counter(str(r.get("verdict") or "") for r in rows)
    reasons = Counter(str(r.get("reason_code") or "") for r in rows)
    states = Counter(str(r.get("narrative_state") or "") for r in rows)
    bands = Counter(score_band(int(r.get("narrative_score") or 0)) for r in rows)

    def pct(k: str, c: Counter[str]) -> float:
        if n == 0:
            return 0.0
        return round(100.0 * c.get(k, 0) / n, 1)

    top_reasons = reasons.most_common(8)

    return {
        "total_reviews": n,
        "verdict_counts": dict(verdicts),
        "verdict_pct": {
            "good": pct("good", verdicts),
            "bad": pct("bad", verdicts),
            "unsure": pct("unsure", verdicts),
        },
        "top_reason_codes": [{"reason_code": k, "count": v} for k, v in top_reasons if k],
        "narrative_state_distribution": dict(states),
        "narrative_score_band_distribution": dict(bands),
    }


def _csv_cell(v: Any) -> str:
    if v is None:
        return ""
    if isinstance(v, datetime):
        return v.isoformat()
    return str(v)


def write_export_csv(rows: list[dict[str, Any]], fp: TextIO) -> None:
    import csv

    w = csv.DictWriter(fp, fieldnames=list(EXPORT_COLUMNS), extrasaction="ignore")
    w.writeheader()
    for r in rows:
        flat = {k: _csv_cell(r.get(k)) for k in EXPORT_COLUMNS}
        w.writerow(flat)


def write_export_json(rows: list[dict[str, Any]], fp: TextIO) -> None:
    def _default(o: object) -> str:
        if isinstance(o, datetime):
            return o.isoformat()
        raise TypeError(type(o))

    json.dump(rows, fp, indent=2, default=_default)
    fp.write("\n")


def format_summary_text(summary: dict[str, Any], *, rows: list[dict[str, Any]] | None = None) -> Iterator[str]:
    yield "=== Checkpoint 1 — resumen de narrative_reviews ==="
    yield f"Total filas (reviews con narrative_current): {summary['total_reviews']}"
    vc = summary["verdict_counts"]
    vp = summary["verdict_pct"]
    yield "Veredictos:"
    for k in ("good", "bad", "unsure"):
        yield f"  {k:6}  n={vc.get(k, 0):4}  ({vp.get(k, 0.0):.1f}%)"
    yield ""
    yield "Top reason_code:"
    for row in summary.get("top_reason_codes") or []:
        yield f"  {row['reason_code']:20} {row['count']}"
    yield ""
    yield "Distribución narrative_state (proyección actual):"
    for st, c in sorted(summary.get("narrative_state_distribution", {}).items(), key=lambda x: -x[1]):
        yield f"  {st:12} {c}"
    yield ""
    yield "Distribución score band (narrative_score actual):"
    for b in ("0-24", "25-49", "50-74", "75-100"):
        c = summary.get("narrative_score_band_distribution", {}).get(b, 0)
        if c:
            yield f"  {b:8} {c}"
    yield ""
    if rows is not None and rows:
        yield "Últimas 5 reviews (reviewed_at):"
        for r in rows[:5]:
            yield (
                f"  {r.get('reviewed_at')}  {r.get('verdict'):6}  "
                f"{str(r.get('reason_code')):16}  {str(r.get('narrative_id'))[:8]}…  "
                f"{str(r.get('title') or '')[:50]}"
            )
