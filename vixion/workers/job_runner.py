"""Runner mínimo: reclamar jobs de la cola Postgres (`PROCESS_ITEM`, `INGEST_SOURCE_TICK`)."""

from __future__ import annotations

import argparse
import logging
import os
import sys

from vixion.db.conn import connect
from vixion.repos import jobs as jobs_repo
from vixion.workers.dispatch import dispatch_claimed_job

log = logging.getLogger(__name__)


def run_once(worker_id: str) -> bool:
    """
    Reclama un job (commit inmediato) y ejecuta PROCESS_ITEM en otra transacción.
    Devuelve True si había un job reclamado (éxito o fallo registrado), False si cola vacía.
    """
    with connect() as cclaim:
        cclaim.autocommit = True
        job = jobs_repo.claim_next_job(cclaim, worker_id=worker_id)
        if not job:
            return False
    jid = int(job["id"])
    log.info("claimed job id=%s type=%s worker=%s", jid, job.get("job_type"), worker_id)
    with connect() as conn:
        conn.autocommit = False
        try:
            dispatch_claimed_job(conn, job)
            jobs_repo.mark_job_succeeded(conn, jid)
            conn.commit()
        except Exception as exc:  # noqa: BLE001 — runner PRIME captura amplio y registra
            conn.rollback()
            log.exception("job %s falló: %s", jid, exc)
            try:
                with connect() as c2:
                    c2.autocommit = False
                    jobs_repo.bump_job_retry(c2, jid, repr(exc))
                    c2.commit()
            except Exception:  # noqa: BLE001
                log.exception("no se pudo registrar reintento para job %s", jid)
    return True


def main(argv: list[str] | None = None) -> int:
    logging.basicConfig(
        level=os.environ.get("LOG_LEVEL", "INFO"),
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    p = argparse.ArgumentParser(description="VIXION job runner (Postgres queue)")
    p.add_argument("--worker-id", default=os.environ.get("WORKER_ID", "worker-1"))
    p.add_argument("--loop", action="store_true", help="Bucle hasta SIGINT (sleep 1s si vacío)")
    p.add_argument("--max-jobs", type=int, default=None)
    args = p.parse_args(argv)

    if not os.environ.get("DATABASE_URL"):
        log.error("DATABASE_URL requerida")
        return 2

    n = 0
    if args.loop:
        import time

        while True:
            if run_once(args.worker_id):
                n += 1
                if args.max_jobs is not None and n >= args.max_jobs:
                    break
            else:
                time.sleep(1.0)
    else:
        run_once(args.worker_id)
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
