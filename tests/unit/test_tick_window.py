"""Ventanas UTC para idempotencia de ticks."""

from __future__ import annotations

from datetime import datetime, timezone

import pytest

from vixion.ingestion.keys import ingest_tick_run_all_job_key
from vixion.ops.tick_window import tick_window_utc


def test_tick_window_utc_floors_to_slot():
    t = datetime(2026, 4, 11, 14, 37, 59, tzinfo=timezone.utc)
    w = tick_window_utc(slot_minutes=15, now=t)
    assert w == "2026-04-11T14:30:00+00:00".replace("+00:00", "Z")


def test_tick_window_rejects_bad_slot():
    with pytest.raises(ValueError):
        tick_window_utc(slot_minutes=7)


def test_ingest_tick_run_all_key_stable():
    a = ingest_tick_run_all_job_key("2026-04-11T14:30:00Z")
    b = ingest_tick_run_all_job_key("2026-04-11T14:30:00Z")
    assert a == b
    assert len(a) == 64
