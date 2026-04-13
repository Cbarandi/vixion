"""Parsing RSS (bytes) → entradas normalizadas."""

from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path

from vixion.ingestion import rss_client

FIXTURE = Path(__file__).resolve().parents[1] / "fixtures" / "minimal_feed.xml"


def test_parse_feed_entries_minimal_fixture():
    entries = rss_client.parse_feed_entries(FIXTURE.read_bytes())
    assert len(entries) == 2
    assert entries[0]["title"] == "Alpha headline"
    assert entries[0]["link"] == "https://fixture.test/item/alpha"
    assert "Alpha summary" in entries[0]["summary"]
    assert entries[0]["stable_id"] == "urn:fixture:alpha"
    assert entries[0]["published_at"] == datetime(2024, 1, 1, 12, 0, tzinfo=timezone.utc)
    assert entries[1]["stable_id"] == "urn:fixture:beta"


def test_process_item_job_key_stable():
    from vixion.ingestion.keys import process_item_job_key

    a = process_item_job_key(3, "urn:fixture:alpha")
    b = process_item_job_key(3, "urn:fixture:alpha")
    c = process_item_job_key(4, "urn:fixture:alpha")
    assert a == b
    assert a != c
    assert len(a) == 64
