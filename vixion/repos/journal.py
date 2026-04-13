"""Eventos y snapshots append-only."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any
from uuid import UUID

import psycopg
from psycopg.types.json import Json, Jsonb


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


def insert_narrative_event(
    conn: psycopg.Connection,
    *,
    narrative_id: UUID,
    event_type: str,
    occurred_at: datetime | None = None,
    related_item_id: int | None = None,
    payload: dict[str, Any] | None = None,
    score_before: int | None = None,
    score_after: int | None = None,
    state_before: str | None = None,
    state_after: str | None = None,
) -> None:
    when = occurred_at or utcnow()
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO narrative_events (
                narrative_id, event_type, occurred_at, related_item_id, payload,
                score_before, score_after, state_before, state_after
            )
            VALUES (
                %s, %s::narrative_event_type, %s, %s, COALESCE(%s::jsonb, '{}'::jsonb),
                %s, %s, %s, %s
            )
            """,
            (
                str(narrative_id),
                event_type,
                when,
                related_item_id,
                Jsonb(payload or {}),
                score_before,
                score_after,
                state_before,
                state_after,
            ),
        )


def insert_narrative_snapshot(
    conn: psycopg.Connection,
    *,
    narrative_id: UUID,
    snapshot_ts_utc: datetime,
    reason: str,
    score: int,
    state: str,
    trend: str,
    item_count: int,
    source_dist: dict[str, Any],
    score_breakdown: dict[str, Any] | None,
    cluster_policy_version: str,
    scoring_policy_version: str | None,
    embedding_model_version: str,
    fingerprint: str | None = None,
) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO narrative_snapshots (
                narrative_id, snapshot_ts_utc, reason, score, state, trend, item_count,
                source_dist, score_breakdown, cluster_policy_version, scoring_policy_version,
                embedding_model_version, fingerprint
            )
            VALUES (
                %s, %s, %s::snapshot_reason, %s, %s::narrative_state, %s::narrative_trend, %s,
                %s::jsonb, %s::jsonb, %s, %s, %s, %s
            )
            """,
            (
                str(narrative_id),
                snapshot_ts_utc,
                reason,
                score,
                state,
                trend,
                item_count,
                Json(source_dist),
                Json(score_breakdown if score_breakdown is not None else {}),
                cluster_policy_version,
                scoring_policy_version,
                embedding_model_version,
                fingerprint,
            ),
        )


def insert_representation_history(
    conn: psycopg.Connection,
    *,
    narrative_id: UUID,
    rep_version: int,
    vector_literal: str,
    based_on_item_sample: dict[str, Any],
) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO narrative_representation_history (
                narrative_id, rep_version, rep_embedding, method, based_on_item_sample, effective_at
            )
            VALUES (%s, %s, %s::vector, %s, %s::jsonb, now())
            """,
            (
                str(narrative_id),
                rep_version,
                vector_literal,
                "incremental_centroid_frozen_v1",
                Json(based_on_item_sample),
            ),
        )
