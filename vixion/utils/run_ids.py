"""Canonical helpers for saved_at parsing and run_id derivation."""

from __future__ import annotations

from datetime import UTC, datetime


def parse_saved_at_utc(iso_str: str) -> datetime:
    """Parse ISO-like timestamp and normalize to aware UTC datetime."""
    raw = iso_str.strip()
    if raw.endswith("Z"):
        raw = raw[:-1] + "+00:00"
    dt = datetime.fromisoformat(raw)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=UTC)
    else:
        dt = dt.astimezone(UTC)
    return dt


def run_id_from_saved_at(iso_str: str) -> str:
    """Canonical run_id: YYYYMMDD_HHMMSS_microseconds in UTC."""
    return parse_saved_at_utc(iso_str).strftime("%Y%m%d_%H%M%S_%f")
