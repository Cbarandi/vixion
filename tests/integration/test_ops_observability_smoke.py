"""Smoke: consultas de observabilidad contra Postgres."""

from __future__ import annotations

import pytest

from vixion.ops.observability import (
    collect_signals,
    fetch_jobs_summary,
    fetch_oldest_pending_by_type,
    fetch_recent_narratives,
    fetch_recent_raw_ingests,
)

pytestmark = pytest.mark.integration


def test_observability_queries_run(db_conn):
    fetch_jobs_summary(db_conn)
    fetch_oldest_pending_by_type(db_conn)
    fetch_recent_raw_ingests(db_conn, limit=5)
    fetch_recent_narratives(db_conn, limit=5)
    collect_signals(db_conn)
