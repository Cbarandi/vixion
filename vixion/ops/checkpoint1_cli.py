"""CLI Checkpoint 1: export, resumen y muestra guiada."""

from __future__ import annotations

import argparse
import logging
import os
import sys

from vixion.db.conn import connect
from vixion.ops.checkpoint1 import (
    fetch_reviews_for_export,
    fetch_sample_narratives,
    format_summary_text,
    summarize_reviews,
    write_export_csv,
    write_export_json,
)

log = logging.getLogger(__name__)


def _cmd_export(args: argparse.Namespace) -> int:
    with connect() as conn:
        conn.autocommit = True
        rows = fetch_reviews_for_export(conn)
    out = open(args.output, "w", encoding="utf-8") if args.output else sys.stdout
    try:
        if args.format == "csv":
            write_export_csv(rows, out)
        else:
            write_export_json(rows, out)
    finally:
        if args.output:
            out.close()
    log.info("export rows=%s format=%s", len(rows), args.format)
    return 0


def _cmd_summary(args: argparse.Namespace) -> int:
    with connect() as conn:
        conn.autocommit = True
        rows = fetch_reviews_for_export(conn)
    summary = summarize_reviews(rows)
    if args.json:
        import json

        print(json.dumps(summary, indent=2))
    else:
        for line in format_summary_text(summary, rows=rows if args.with_tail else None):
            print(line)
    return 0


def _cmd_sample(args: argparse.Namespace) -> int:
    with connect() as conn:
        conn.autocommit = True
        rows = fetch_sample_narratives(
            conn,
            limit=args.limit,
            min_item_count=args.min_item_count,
            include_dormant=args.include_dormant,
        )
    if args.sample_fmt == "json":
        import json
        from datetime import datetime

        def _default(o: object) -> str:
            if isinstance(o, datetime):
                return o.isoformat()
            raise TypeError(type(o))

        print(json.dumps(rows, indent=2, default=_default))
    else:
        print("narrative_id                         score state        items  updated_at")
        for r in rows:
            print(
                f"{str(r.get('narrative_id')):36} "
                f"{int(r.get('score') or 0):3} "
                f"{str(r.get('state') or ''):12} "
                f"{int(r.get('item_count') or 0):4}  "
                f"{r.get('updated_at')}"
            )
            t = str(r.get("title") or "")[:72]
            if t:
                print(f"  title: {t}")
    log.info("sample rows=%s", len(rows))
    return 0


def main(argv: list[str] | None = None) -> int:
    logging.basicConfig(
        level=os.environ.get("LOG_LEVEL", "INFO"),
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    p = argparse.ArgumentParser(
        description="VIXION Checkpoint 1 — export/summary de reviews y muestra de narrativas."
    )
    sub = p.add_subparsers(dest="command", required=True)

    pe = sub.add_parser("export", help="Exporta reviews + contexto narrative_current (CSV o JSON).")
    pe.add_argument("--format", choices=("csv", "json"), default="csv")
    pe.add_argument("-o", "--output", default=None, help="Archivo (default: stdout).")
    pe.set_defaults(func=_cmd_export)

    ps = sub.add_parser("summary", help="Resumen agregado de reviews.")
    ps.add_argument("--json", action="store_true", help="Solo JSON en stdout.")
    ps.add_argument(
        "--with-tail",
        action="store_true",
        help="Incluye últimas 5 reviews en el bloque de texto.",
    )
    ps.set_defaults(func=_cmd_summary)

    pm = sub.add_parser("sample", help="Lista narrativas recientes para revisar (muestra guiada).")
    pm.add_argument(
        "--limit",
        type=int,
        default=20,
        help="Máximo de filas (default 20).",
    )
    pm.add_argument("--min-item-count", type=int, default=2, dest="min_item_count")
    pm.add_argument(
        "--include-dormant",
        action="store_true",
        help="Incluye state=dormant (default: excluidas).",
    )
    pm.add_argument(
        "--output-format",
        choices=("text", "json"),
        default="text",
        dest="sample_fmt",
        help="text = tabla; json = lista.",
    )
    pm.set_defaults(func=_cmd_sample)

    args = p.parse_args(argv)
    if not os.environ.get("DATABASE_URL"):
        log.error("DATABASE_URL requerida")
        return 2
    return int(args.func(args))


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
