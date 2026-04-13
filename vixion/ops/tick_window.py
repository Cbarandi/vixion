"""Ventana UTC alineada a slots (cron + idempotencia de ticks)."""

from __future__ import annotations

from datetime import datetime, timezone


def tick_window_utc(*, slot_minutes: int, now: datetime | None = None) -> str:
    """
    Devuelve etiqueta estable para la ventana actual, p. ej. slot de 15 min.
    Formato ISO 8601 con sufijo Z (UTC).
    """
    if slot_minutes < 1 or slot_minutes > 60 or 60 % slot_minutes != 0:
        raise ValueError("slot_minutes debe dividir 60 (p. ej. 5, 10, 15, 20, 30).")
    t = now or datetime.now(timezone.utc)
    if t.tzinfo is None:
        t = t.replace(tzinfo=timezone.utc)
    t = t.astimezone(timezone.utc)
    m = (t.minute // slot_minutes) * slot_minutes
    floored = t.replace(minute=m, second=0, microsecond=0)
    return floored.isoformat().replace("+00:00", "Z")
