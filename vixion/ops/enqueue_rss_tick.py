"""Encola ``INGEST_SOURCE_TICK`` para ingesta periódica (cron-friendly)."""

from __future__ import annotations

import argparse
import logging
import os
import sys
from pathlib import Path

from vixion.db.conn import connect
from vixion.ingestion.feeds_config import load_feed_specs
from vixion.ingestion.keys import ingest_tick_job_key, ingest_tick_run_all_job_key
from vixion.ingestion.service import sync_feed_sources_from_config
from vixion.ops.tick_window import tick_window_utc
from vixion.repos import jobs as jobs_repo

log = logging.getLogger(__name__)


def main(argv: list[str] | None = None) -> int:
    logging.basicConfig(
        level=os.environ.get("LOG_LEVEL", "INFO"),
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    p = argparse.ArgumentParser(
        description="Encola INGEST_SOURCE_TICK (idempotente por ventana). Opcional: sync feeds YAML→sources."
    )
    p.add_argument(
        "--config",
        type=Path,
        default=None,
        help="YAML de feeds (VIXION_FEEDS_CONFIG / config/feeds.yaml)",
    )
    p.add_argument(
        "--sync-config",
        action="store_true",
        help="Sincroniza sources desde el YAML antes de encolar (slug = fila; actualiza rss_url/name).",
    )
    p.add_argument(
        "--source-id",
        type=int,
        default=None,
        help="Tick solo para ese source_id; si se omite, run_all=true (todas las fuentes RSS habilitadas).",
    )
    p.add_argument(
        "--slot-minutes",
        type=int,
        default=15,
        help="Tamaño de ventana UTC para idempotencia (debe coincidir con el intervalo del cron).",
    )
    p.add_argument(
        "--window",
        type=str,
        default=None,
        help="Ventana ISO UTC explícita (override; por defecto se calcula con --slot-minutes).",
    )
    args = p.parse_args(argv)

    if not os.environ.get("DATABASE_URL"):
        log.error("DATABASE_URL requerida")
        return 2

    window = args.window or tick_window_utc(slot_minutes=args.slot_minutes)

    with connect() as conn:
        conn.autocommit = False
        if args.sync_config:
            feeds = load_feed_specs(args.config)
            sync_feed_sources_from_config(conn, feeds)
            log.info("sync_config feeds=%s", len(feeds))

        if args.source_id is not None:
            ikey = ingest_tick_job_key(args.source_id, window)
            payload = {"source_id": args.source_id}
        else:
            ikey = ingest_tick_run_all_job_key(window)
            payload = {"run_all": True}

        jid, created = jobs_repo.enqueue_ingest_source_tick(
            conn, idempotency_key=ikey, payload=payload
        )
        conn.commit()
        log.info(
            "INGEST_SOURCE_TICK window=%s idempotency_key=%s... job_id=%s created=%s",
            window,
            ikey[:16],
            jid,
            created,
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
