"""CLI: sincroniza `sources` desde YAML y ejecuta ingesta RSS por feed."""

from __future__ import annotations

import argparse
import logging
import os
import sys
from pathlib import Path

from vixion.db.conn import connect
from vixion.ingestion.feeds_config import load_feed_specs
from vixion.ingestion.service import run_rss_ingest, sync_feed_sources_from_config

log = logging.getLogger(__name__)


def main(argv: list[str] | None = None) -> int:
    logging.basicConfig(
        level=os.environ.get("LOG_LEVEL", "INFO"),
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    p = argparse.ArgumentParser(description="VIXION — ingesta RSS y enqueue PROCESS_ITEM")
    p.add_argument(
        "--config",
        type=Path,
        default=None,
        help="YAML de feeds (por defecto VIXION_FEEDS_CONFIG o config/feeds.yaml)",
    )
    args = p.parse_args(argv)
    if not os.environ.get("DATABASE_URL"):
        log.error("DATABASE_URL requerida")
        return 2
    feeds = load_feed_specs(args.config)
    with connect() as conn:
        conn.autocommit = False
        slug_to_id = sync_feed_sources_from_config(conn, feeds)
        for spec in feeds:
            sid = slug_to_id[spec.slug]
            run_rss_ingest(conn, source_id=sid, feed_url=spec.url, feed_slug=spec.slug)
        conn.commit()
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
