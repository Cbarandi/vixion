"""Rutas HTTP de lectura."""

from __future__ import annotations

from datetime import datetime
from enum import Enum
from typing import Annotated
from uuid import UUID

from fastapi import APIRouter, HTTPException, Query

from vixion.api import narrative_read
from vixion.api.deps import DbConn
from vixion.api.schemas import (
    HealthResponse,
    NarrativeCurrentBlock,
    NarrativeDetailResponse,
    NarrativeEventOut,
    NarrativeItemRow,
    NarrativeItemsResponse,
    NarrativeListItem,
    NarrativeListResponse,
    NarrativeReviewCreate,
    NarrativeReviewCreatedResponse,
    NarrativeReviewOut,
    NarrativeSnapshotOut,
    NarrativeTopResponse,
)
from vixion.db.conn import connect
from vixion.repos import narrative_reviews as narrative_reviews_repo

router = APIRouter()


class NarrativeStateFilter(str, Enum):
    early = "early"
    emerging = "emerging"
    confirmed = "confirmed"
    fading = "fading"
    dormant = "dormant"


@router.get("/health", response_model=HealthResponse, tags=["meta"])
def health() -> HealthResponse:
    try:
        with connect() as conn:
            conn.autocommit = True
            conn.execute("SELECT 1")
    except Exception:  # noqa: BLE001 — health: cualquier fallo de DB se reporta como error
        return HealthResponse(status="ok", database="error")
    return HealthResponse(status="ok", database="ok")


@router.get("/narratives", response_model=NarrativeListResponse, tags=["narratives"])
def list_narratives_endpoint(
    conn: DbConn,
    state: Annotated[NarrativeStateFilter | None, Query(description="Filtrar por estado PRIME.")] = None,
    min_score: Annotated[int | None, Query(ge=0, le=100)] = None,
    updated_since: Annotated[datetime | None, Query(description="ISO 8601; solo filas con updated_at >= este instante.")] = None,
    include_dormant: Annotated[bool, Query(description="Si false (default), excluye state=dormant.")] = False,
    limit: Annotated[int, Query(ge=1, le=100)] = 50,
    offset: Annotated[int, Query(ge=0)] = 0,
) -> NarrativeListResponse:
    rows = narrative_read.list_narratives(
        conn,
        state=state.value if state else None,
        min_score=min_score,
        updated_since=updated_since,
        include_dormant=include_dormant,
        limit=limit,
        offset=offset,
        order_mode="updated_at",
    )
    items = [NarrativeListItem.model_validate(r) for r in rows]
    return NarrativeListResponse(items=items, limit=limit, offset=offset)


@router.get("/narratives/top", response_model=NarrativeTopResponse, tags=["narratives"])
def list_narratives_top(
    conn: DbConn,
    state: Annotated[NarrativeStateFilter | None, Query()] = None,
    min_score: Annotated[int | None, Query(ge=0, le=100)] = None,
    updated_since: Annotated[datetime | None, Query()] = None,
    include_dormant: Annotated[bool, Query()] = False,
    limit: Annotated[int, Query(ge=1, le=50)] = 20,
) -> NarrativeTopResponse:
    rows = narrative_read.list_narratives(
        conn,
        state=state.value if state else None,
        min_score=min_score,
        updated_since=updated_since,
        include_dormant=include_dormant,
        limit=limit,
        offset=0,
        order_mode="top",
    )
    items = [NarrativeListItem.model_validate(r) for r in rows]
    return NarrativeTopResponse(items=items, limit=limit)


@router.get(
    "/narratives/{narrative_id}/items",
    response_model=NarrativeItemsResponse,
    tags=["narratives"],
)
def list_narrative_items_endpoint(
    conn: DbConn,
    narrative_id: UUID,
    limit: Annotated[int, Query(ge=1, le=500)] = 200,
) -> NarrativeItemsResponse:
    if not narrative_read.fetch_current_block(conn, narrative_id):
        raise HTTPException(status_code=404, detail="narrative not found")
    rows = narrative_read.list_narrative_items(conn, narrative_id, limit=limit)
    items = [NarrativeItemRow.model_validate(r) for r in rows]
    return NarrativeItemsResponse(narrative_id=str(narrative_id), items=items, limit=limit)


@router.post(
    "/narratives/{narrative_id}/reviews",
    response_model=NarrativeReviewCreatedResponse,
    tags=["reviews"],
)
def create_narrative_review(
    conn: DbConn,
    narrative_id: UUID,
    body: NarrativeReviewCreate,
) -> NarrativeReviewCreatedResponse:
    if not narrative_read.fetch_current_block(conn, narrative_id):
        raise HTTPException(status_code=404, detail="narrative not found")
    rid = narrative_reviews_repo.insert_narrative_review(
        conn,
        narrative_id=narrative_id,
        verdict=body.verdict,
        reason_code=body.reason_code,
        notes=body.notes,
        reviewer=body.reviewer,
    )
    return NarrativeReviewCreatedResponse(review_id=rid, narrative_id=str(narrative_id))


@router.get("/narratives/{narrative_id}", response_model=NarrativeDetailResponse, tags=["narratives"])
def get_narrative_detail(
    conn: DbConn,
    narrative_id: UUID,
    events_limit: Annotated[int, Query(ge=1, le=200)] = 50,
    snapshots_limit: Annotated[int, Query(ge=1, le=100)] = 30,
) -> NarrativeDetailResponse:
    cur_block = narrative_read.fetch_current_block(conn, narrative_id)
    if not cur_block:
        raise HTTPException(status_code=404, detail="narrative not found")
    created = narrative_read.get_narrative_created_at(conn, narrative_id)
    events_raw = narrative_read.list_events(conn, narrative_id, limit=events_limit)
    snaps_raw = narrative_read.list_snapshots(conn, narrative_id, limit=snapshots_limit)
    rev_raw = narrative_read.fetch_latest_review(conn, narrative_id)

    current = NarrativeCurrentBlock(
        narrative_id=cur_block["narrative_id"],
        title=cur_block["title"] or "",
        score=int(cur_block["score"]),
        state=str(cur_block["state"]),
        trend=str(cur_block["trend"]),
        item_count=int(cur_block["item_count"]),
        rep_version=int(cur_block["rep_version"]),
        updated_at=cur_block["updated_at"],
        scored_at=cur_block.get("scored_at"),
        source_dist=dict(cur_block.get("source_dist") or {}),
    )
    events = [NarrativeEventOut.model_validate(e) for e in events_raw]
    snapshots = [NarrativeSnapshotOut.model_validate(s) for s in snaps_raw]
    review = NarrativeReviewOut.model_validate(rev_raw) if rev_raw else None

    return NarrativeDetailResponse(
        narrative_id=str(narrative_id),
        narrative_created_at=created,
        current=current,
        events=events,
        snapshots=snapshots,
        review=review,
    )
