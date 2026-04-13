"""CLI: resumen operativo (ingestas, jobs, narrativas, señales)."""

from __future__ import annotations

import argparse
import logging
import os
import sys

from vixion.db.conn import connect
from vixion.ops.observability import (
    collect_signals,
    fetch_jobs_summary,
    fetch_oldest_pending_by_type,
    fetch_recent_narratives,
    fetch_recent_raw_ingests,
    format_lines,
)

log = logging.getLogger(__name__)


def main(argv: list[str] | None = None) -> int:
    logging.basicConfig(
        level=os.environ.get("LOG_LEVEL", "WARNING"),
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    p = argparse.ArgumentParser(
        description="VIXION — observabilidad mínima (Postgres): ingestas, jobs, narrativas, señales.",
        epilog=(
            "Umbrales de señales (enteros, vía env): "
            "VIXION_OPS_PENDING_WARN (default 200), "
            "VIXION_OPS_FAILED_WARN (20), "
            "VIXION_OPS_DEAD_WARN (5), "
            "VIXION_OPS_INGEST_BAD_STREAK (3 corridas sin success seguidas por feed)."
        ),
    )
    p.add_argument(
        "--ingest-limit",
        type=int,
        default=40,
        help="Filas recientes de raw_ingests a mostrar.",
    )
    p.add_argument(
        "--narrative-limit",
        type=int,
        default=20,
        help="Filas recientes de narrative_current.",
    )
    p.add_argument(
        "--signals-only",
        action="store_true",
        help="Solo imprime señales (WARN); útil para cron.",
    )
    p.add_argument(
        "--strict",
        action="store_true",
        help="Exit 1 si hay al menos una señal WARN.",
    )
    args = p.parse_args(argv)

    if not os.environ.get("DATABASE_URL"):
        log.error("DATABASE_URL requerida")
        return 2

    with connect() as conn:
        conn.autocommit = True
        signals = collect_signals(conn)
        if args.signals_only:
            for s in signals:
                print(s)
            if args.strict and signals:
                return 1
            return 0

        jobs_rows = fetch_jobs_summary(conn)
        pending_age = fetch_oldest_pending_by_type(conn)
        ingests = fetch_recent_raw_ingests(conn, limit=args.ingest_limit)
        narratives = fetch_recent_narratives(conn, limit=args.narrative_limit)
        for line in format_lines(
            jobs_rows=jobs_rows,
            pending_age=pending_age,
            ingests=ingests,
            narratives=narratives,
            signals=signals,
        ):
            print(line)

    if args.strict and signals:
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
