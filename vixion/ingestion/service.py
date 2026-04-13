"""Orquestación: raw_ingest → parse → enqueue PROCESS_ITEM."""

from __future__ import annotations

import logging
from collections.abc import Callable
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any

import psycopg

from vixion.ingestion import rss_client
from vixion.ingestion.keys import process_item_job_key

if TYPE_CHECKING:
    from vixion.ingestion.feeds_config import FeedSpec
from vixion.repos import jobs as jobs_repo
from vixion.repos import raw_ingests as raw_ingests_repo
from vixion.repos import sources as sources_repo

log = logging.getLogger(__name__)


def _iso_utc(dt: datetime | None) -> str | None:
    if dt is None:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc).isoformat()


def run_rss_ingest(
    conn: psycopg.Connection,
    *,
    source_id: int,
    feed_url: str,
    feed_slug: str,
    fetcher: Callable[[str], bytes] | None = None,
) -> dict[str, Any]:
    """
    Una corrida: `raw_ingests` + fetch + parse + enqueue PROCESS_ITEM por entrada válida.
    `fetcher(url)->bytes` inyectable en tests.
    """
    fetch = fetcher or rss_client.fetch_feed_bytes
    rid = raw_ingests_repo.start_raw_ingest(conn, source_id=source_id)
    log.info("RSS ingest_start feed=%s source_id=%s raw_ingest_id=%s", feed_slug, source_id, rid)
    stats: dict[str, Any] = {
        "feed_slug": feed_slug,
        "entries_seen": 0,
        "process_item_enqueued_new": 0,
        "process_item_job_deduped": 0,
        "entries_skipped_no_link": 0,
        "errors": [],
    }
    final_status = "success"
    err_top: str | None = None
    try:
        content = fetch(feed_url)
        entries = rss_client.parse_feed_entries(content)
        stats["entries_seen"] = len(entries)
        for ent in entries:
            link = ent.get("link") or ""
            title = ent.get("title") or ""
            if not link.strip():
                stats["entries_skipped_no_link"] += 1
                continue
            body = ent.get("summary") or ""
            stable = ent.get("stable_id") or link
            native_id = f"rss:{stable[:500]}"
            ikey = process_item_job_key(source_id, stable)
            payload = {
                "source_id": source_id,
                "title": title[:2000],
                "body": body[:50000],
                "fetched_url": link[:4000],
                "native_id": native_id[:2000],
                "published_at": _iso_utc(ent.get("published_at")),
                "raw_ingest_id": rid,
            }
            try:
                _jid, created = jobs_repo.enqueue_process_item(
                    conn, idempotency_key=ikey, payload=payload
                )
                if created:
                    stats["process_item_enqueued_new"] += 1
                else:
                    stats["process_item_job_deduped"] += 1
            except Exception as exc:  # noqa: BLE001
                stats["errors"].append({"link": link[:500], "error": repr(exc)})
                final_status = "partial"
    except Exception as exc:  # noqa: BLE001
        log.exception("RSS ingest falló feed=%s", feed_slug)
        err_top = repr(exc)
        stats["errors"].append({"fatal": True, "error": err_top})
        final_status = "failed"

    raw_ingests_repo.finish_raw_ingest(
        conn,
        raw_ingest_id=rid,
        status=final_status,
        stats=stats,
        error_message=err_top,
    )
    err_n = len(stats.get("errors") or [])
    log.info(
        "RSS ingest_done feed=%s raw_ingest_id=%s status=%s seen=%s enqueued_new=%s "
        "deduped=%s skipped_no_link=%s err_count=%s",
        feed_slug,
        rid,
        final_status,
        stats.get("entries_seen"),
        stats.get("process_item_enqueued_new"),
        stats.get("process_item_job_deduped"),
        stats.get("entries_skipped_no_link"),
        err_n,
    )
    return stats


def run_ingest_tick_payload(conn: psycopg.Connection, payload: dict[str, Any]) -> list[dict[str, Any]]:
    """
    Ejecuta `INGEST_SOURCE_TICK`: payload `{"run_all": true}` o `{"source_id": N}`.
    """
    out: list[dict[str, Any]] = []
    if payload.get("run_all"):
        for row in sources_repo.list_rss_sources(conn):
            cfg = row.get("config") or {}
            if isinstance(cfg, str):
                import json

                cfg = json.loads(cfg)
            url = cfg.get("rss_url")
            slug = cfg.get("slug", "unknown")
            if not url:
                continue
            stats = run_rss_ingest(
                conn, source_id=int(row["id"]), feed_url=str(url), feed_slug=str(slug)
            )
            out.append(stats)
        return out

    sid = payload.get("source_id")
    if sid is None:
        raise ValueError("INGEST_SOURCE_TICK requiere source_id o run_all")
    cfg = sources_repo.get_source_config(conn, int(sid))
    url = cfg.get("rss_url")
    if not url:
        raise ValueError(f"source {sid} sin rss_url en config")
    slug = str(cfg.get("slug", "unknown"))
    out.append(run_rss_ingest(conn, source_id=int(sid), feed_url=str(url), feed_slug=slug))
    return out


def sync_feed_sources_from_config(conn: psycopg.Connection, feeds: list[FeedSpec]) -> dict[str, int]:
    """Asegura filas `sources` para cada feed del YAML. Devuelve slug->source_id."""
    mapping: dict[str, int] = {}
    for f in feeds:
        sid = sources_repo.ensure_rss_feed_source(
            conn, slug=f.slug, display_name=f.name, rss_url=f.url
        )
        mapping[f.slug] = sid
    return mapping
