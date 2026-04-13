"""Smoke: consultas Checkpoint 1 contra Postgres."""

from __future__ import annotations

import pytest

from vixion.ops.checkpoint1 import fetch_reviews_for_export, fetch_sample_narratives, summarize_reviews

pytestmark = pytest.mark.integration


def test_checkpoint1_queries_run(db_conn):
    rev = fetch_reviews_for_export(db_conn)
    assert isinstance(rev, list)
    summarize_reviews(rev)

    sample = fetch_sample_narratives(
        db_conn, limit=5, min_item_count=1, include_dormant=False
    )
    assert isinstance(sample, list)
