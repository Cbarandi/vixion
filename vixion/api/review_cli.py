"""CLI: registrar una narrative_review (sin auth; solo entornos de confianza)."""

from __future__ import annotations

import argparse
import logging
import os
import sys
from uuid import UUID

from vixion.api import narrative_read
from vixion.db.conn import connect
from vixion.repos import narrative_reviews as narrative_reviews_repo

log = logging.getLogger(__name__)


def main(argv: list[str] | None = None) -> int:
    logging.basicConfig(level=os.environ.get("LOG_LEVEL", "WARNING"))
    p = argparse.ArgumentParser(
        description="Inserta narrative_reviews + evento REVIEW_RECORDED (PRIME)."
    )
    p.add_argument("narrative_id", type=UUID, help="UUID de la narrativa")
    p.add_argument("--verdict", required=True, choices=["good", "bad", "unsure"])
    p.add_argument(
        "--reason-code",
        required=True,
        dest="reason_code",
        choices=[
            "off_topic",
            "too_broad",
            "duplicate_theme",
            "language_noise",
            "spam",
            "other",
        ],
    )
    p.add_argument("--notes", default=None, help="Texto libre opcional")
    p.add_argument("--reviewer", default="manual_cli", help="Identificador del revisor")
    args = p.parse_args(argv)

    if not os.environ.get("DATABASE_URL"):
        log.error("DATABASE_URL requerida")
        return 2

    with connect() as conn:
        conn.autocommit = True
        if not narrative_read.fetch_current_block(conn, args.narrative_id):
            log.error("narrative_id no encontrada en narrative_current")
            return 2
        rid = narrative_reviews_repo.insert_narrative_review(
            conn,
            narrative_id=args.narrative_id,
            verdict=args.verdict,
            reason_code=args.reason_code,
            notes=args.notes,
            reviewer=args.reviewer,
        )
    print(rid)
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
