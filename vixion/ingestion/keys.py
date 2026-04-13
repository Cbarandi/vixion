"""Claves idempotentes estables para jobs."""

from __future__ import annotations

import hashlib


def process_item_job_key(source_id: int, stable_entry_id: str) -> str:
    base = f"vixion:rss:process_item:{source_id}:{stable_entry_id}"
    return hashlib.sha256(base.encode("utf-8")).hexdigest()


def ingest_tick_job_key(source_id: int, window_utc: str) -> str:
    """Ventana explícita p.ej. ISO hora `2026-04-12T14` para no duplicar ticks programados."""
    base = f"vixion:rss:ingest_tick:{source_id}:{window_utc}"
    return hashlib.sha256(base.encode("utf-8")).hexdigest()


def ingest_tick_run_all_job_key(window_utc: str) -> str:
    """Idempotencia para payload ``{\"run_all\": true}`` (un tick por ventana)."""
    base = f"vixion:rss:ingest_tick:run_all:{window_utc}"
    return hashlib.sha256(base.encode("utf-8")).hexdigest()
