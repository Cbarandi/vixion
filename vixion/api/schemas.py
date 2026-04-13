"""Schemas de respuesta API (solo lectura)."""

from __future__ import annotations

from datetime import datetime
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field


class HealthResponse(BaseModel):
    status: str = "ok"
    database: str = Field(description="ok | error")


class NarrativeListItem(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: str
    title: str
    score: int
    state: str
    trend: str
    item_count: int
    updated_at: datetime
    sources: str = Field(default="", description="Nombres de fuentes agregados (ítems enlazados).")


class NarrativeListResponse(BaseModel):
    items: list[NarrativeListItem]
    limit: int
    offset: int


class NarrativeTopResponse(BaseModel):
    items: list[NarrativeListItem]
    limit: int


class NarrativeCurrentBlock(BaseModel):
    narrative_id: str
    title: str
    score: int
    state: str
    trend: str
    item_count: int
    rep_version: int
    updated_at: datetime
    scored_at: datetime | None = None
    source_dist: dict[str, Any] = Field(default_factory=dict)


class NarrativeEventOut(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: int
    event_type: str
    occurred_at: datetime
    related_item_id: int | None = None
    payload: dict[str, Any] = Field(default_factory=dict)
    score_before: int | None = None
    score_after: int | None = None
    state_before: str | None = None
    state_after: str | None = None


class NarrativeSnapshotOut(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: int
    snapshot_ts_utc: datetime
    reason: str
    score: int
    state: str
    trend: str
    item_count: int
    fingerprint: str | None = None


class NarrativeReviewOut(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: int
    verdict: str
    reason_code: str
    notes: str | None = None
    reviewer: str
    reviewed_at: datetime


class NarrativeDetailResponse(BaseModel):
    narrative_id: str
    narrative_created_at: datetime | None = None
    current: NarrativeCurrentBlock
    events: list[NarrativeEventOut]
    snapshots: list[NarrativeSnapshotOut]
    review: NarrativeReviewOut | None = None


class NarrativeItemRow(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    item_id: int
    title: str
    url: str | None = None
    published_at: datetime | None = None
    source_name: str = ""
    linked_at: datetime
    sentiment: float | None = None
    intensity: float | None = None


class NarrativeItemsResponse(BaseModel):
    narrative_id: str
    items: list[NarrativeItemRow]
    limit: int


ReviewVerdictLiteral = Literal["good", "bad", "unsure"]
ReviewReasonLiteral = Literal[
    "off_topic",
    "too_broad",
    "duplicate_theme",
    "language_noise",
    "spam",
    "other",
]


class NarrativeReviewCreate(BaseModel):
    verdict: ReviewVerdictLiteral
    reason_code: ReviewReasonLiteral
    notes: str | None = Field(None, max_length=8000)
    reviewer: str = Field(default="manual_api", max_length=200)


class NarrativeReviewCreatedResponse(BaseModel):
    review_id: int
    narrative_id: str
