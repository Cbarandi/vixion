"""Contratos de datos mínimos (sin ORM)."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Literal
from uuid import UUID


@dataclass(frozen=True, slots=True)
class RawIngestCandidate:
    """Candidato normalizado mínimo antes de persistir ítem canónico."""

    source_id: int
    title: str
    body: str
    fetched_url: str | None = None
    native_id: str | None = None
    published_at: datetime | None = None
    raw_ingest_id: int | None = None


@dataclass(slots=True)
class ProcessItemResult:
    """Resultado de PROCESS_ITEM (éxito o salida controlada)."""

    status: Literal["completed", "skipped_duplicate", "skipped_non_english", "failed"]
    item_id: int | None = None
    narrative_id: UUID | None = None
    detail: str = ""
    error: str | None = None
    extra: dict[str, Any] = field(default_factory=dict)
