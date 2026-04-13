"""Ejecución del cuerpo de un job ya reclamado (runner + tests E2E)."""

from __future__ import annotations

import logging
from typing import Any

import psycopg

from vixion.ingestion.service import run_ingest_tick_payload
from vixion.pipeline.process_item import process_item, raw_candidate_from_job_payload
from vixion.repos import jobs as jobs_repo

log = logging.getLogger(__name__)


def dispatch_claimed_job(conn: psycopg.Connection, job: dict[str, Any]) -> None:
    """
    Ejecuta la lógica del job. No marca éxito ni hace commit.
    Debe llamarse dentro de una transacción abierta.
    """
    jid = int(job["id"])
    jt = str(job["job_type"])
    if jt == "PROCESS_ITEM":
        payload = jobs_repo.parse_process_item_payload(job)
        cand = raw_candidate_from_job_payload(payload)
        res = process_item(conn, cand)
        log.info(
            "job %s PROCESS_ITEM -> %s item=%s narrative=%s detail=%s",
            jid,
            res.status,
            res.item_id,
            res.narrative_id,
            res.detail or res.error,
        )
    elif jt == "INGEST_SOURCE_TICK":
        payload = jobs_repo.parse_process_item_payload(job)
        tick_stats = run_ingest_tick_payload(conn, payload)
        log.info("job %s INGEST_SOURCE_TICK -> %s feed run(s)", jid, len(tick_stats))
        for st in tick_stats:
            log.info(
                "ingest_tick_feed feed=%s seen=%s enqueued_new=%s deduped=%s skipped_no_link=%s err_count=%s",
                st.get("feed_slug"),
                st.get("entries_seen"),
                st.get("process_item_enqueued_new"),
                st.get("process_item_job_deduped"),
                st.get("entries_skipped_no_link"),
                len(st.get("errors") or []),
            )
    else:
        raise ValueError(f"job_type no soportado: {jt}")
