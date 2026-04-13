"""Pipeline PROCESS_ITEM — primer flujo end-to-end PRIME."""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Any
from uuid import UUID

import psycopg
from psycopg.errors import UniqueViolation

from vixion.constants import (
    CLUSTER_POLICY_VERSION,
    EMBEDDING_MODEL_VERSION,
    REP_RECOMPUTE_EVERY_N_ITEMS,
)
from vixion.contracts import ProcessItemResult, RawIngestCandidate
from vixion.repos import items as items_repo
from vixion.repos import journal as journal_repo
from vixion.repos import narratives as narratives_repo
from vixion.services import canonicalization, locale_gate
from vixion.services import embeddings_stub, nlp_stub, representation, scoring_v0

log = logging.getLogger(__name__)


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


def _resolve_existing_item_id(conn: psycopg.Connection, cand: RawIngestCandidate, chash: str, curl: str | None) -> int | None:
    if cand.native_id:
        iid = items_repo.find_item_by_native_occurrence(conn, cand.source_id, cand.native_id)
        if iid:
            return iid
    if curl:
        iid = items_repo.find_item_by_canonical_url(conn, curl)
        if iid:
            return iid
    return items_repo.find_item_by_content_hash(conn, chash)


def process_item(conn: psycopg.Connection, cand: RawIngestCandidate) -> ProcessItemResult:
    """
    Ejecuta PROCESS_ITEM dentro de la transacción abierta en `conn`.
    El caller hace COMMIT/ROLLBACK.
    """
    curl = canonicalization.normalize_url(cand.fetched_url)
    chash = canonicalization.content_hash(cand.title, cand.body)
    fp = canonicalization.occurrence_fingerprint(
        cand.source_id, cand.native_id, curl, chash
    )

    existing = _resolve_existing_item_id(conn, cand, chash, curl)
    if existing is not None:
        if items_repo.occurrence_exists(conn, existing, fp):
            return ProcessItemResult(
                status="skipped_duplicate",
                item_id=existing,
                detail="occurrence_fingerprint_idempotente",
                extra={"occurrence_registered": False, "dedupe": "same_fingerprint"},
            )
        items_repo.insert_occurrence(
            conn,
            item_id=existing,
            source_id=cand.source_id,
            raw_ingest_id=cand.raw_ingest_id,
            fetched_url=cand.fetched_url,
            published_at=cand.published_at,
            native_id=cand.native_id,
            fingerprint=fp,
        )
        items_repo.touch_item_last_seen(conn, existing)
        row = items_repo.get_item_row(conn, existing)
        assert row
        stage = str(row["processing_stage"])
        if stage == "completed":
            return ProcessItemResult(
                status="skipped_duplicate",
                item_id=existing,
                detail="item_ya_procesado_nueva_aparición_registrada",
                extra={
                    "occurrence_registered": True,
                    "dedupe": "hard_canonical_completed",
                    "traceability": "new_row_item_occurrences_when_fingerprint_new",
                },
            )
        if stage in ("skipped_non_en",):
            return ProcessItemResult(
                status="skipped_duplicate",
                item_id=existing,
                detail="item_rechazado_por_idioma",
                extra={"occurrence_registered": True, "dedupe": "hard_locale_reject"},
            )
        return ProcessItemResult(
            status="failed",
            item_id=existing,
            error="item_existente_no_completado_reproceso_v0_no_soportado",
            extra={"occurrence_registered": True, "dedupe": "hard_incomplete_unsupported"},
        )

    locale = locale_gate.assess_locale(f"{cand.title}\n{cand.body}")
    if not locale.accepted:
        item_id = items_repo.insert_item(
            conn,
            canonical_url=curl,
            content_hash=chash,
            source_native_id=cand.native_id,
            title=canonicalization.title_for_display(cand.title),
            body_text=cand.body,
            language="und",
            content_locale_status="rejected_non_en",
            primary_source_id=cand.source_id,
            dedupe_kind="new_unique",
            processing_stage="skipped_non_en",
        )
        items_repo.insert_occurrence(
            conn,
            item_id=item_id,
            source_id=cand.source_id,
            raw_ingest_id=cand.raw_ingest_id,
            fetched_url=cand.fetched_url,
            published_at=cand.published_at,
            native_id=cand.native_id,
            fingerprint=fp,
        )
        return ProcessItemResult(
            status="skipped_non_english",
            item_id=item_id,
            detail=locale.reason,
            extra={"detected": locale.language_code},
        )

    item_id = items_repo.insert_item(
        conn,
        canonical_url=curl,
        content_hash=chash,
        source_native_id=cand.native_id,
        title=canonicalization.title_for_display(cand.title),
        body_text=cand.body,
        language="en",
        content_locale_status="accepted_en",
        primary_source_id=cand.source_id,
        dedupe_kind="new_unique",
        processing_stage="normalized",
    )
    items_repo.insert_occurrence(
        conn,
        item_id=item_id,
        source_id=cand.source_id,
        raw_ingest_id=cand.raw_ingest_id,
        fetched_url=cand.fetched_url,
        published_at=cand.published_at,
        native_id=cand.native_id,
        fingerprint=fp,
    )

    nlp = nlp_stub.build_nlp_profile(cand.title, cand.body)
    items_repo.insert_nlp_profile(
        conn,
        item_id=item_id,
        nlp_model_version=nlp_stub.nlp_model_version(),
        content_type=nlp.content_type,
        sentiment=nlp.sentiment,
        intensity=nlp.intensity,
        topics=nlp.topics,
    )
    items_repo.update_item_stage(conn, item_id, "nlp_done")

    text_for_emb = f"{cand.title}\n{cand.body}".strip()
    vec = embeddings_stub.stub_embedding_vector(text_for_emb)
    vec_lit = embeddings_stub.vector_to_pg_literal(vec)
    emb_model_id = items_repo.get_embedding_model_id(conn)
    items_repo.insert_embedding(
        conn,
        item_id=item_id,
        embedding_model_id=emb_model_id,
        embedding_model_version=EMBEDDING_MODEL_VERSION,
        vector_literal=vec_lit,
    )
    items_repo.update_item_stage(conn, item_id, "embedded")

    nid = narratives_repo.find_best_matching_narrative(
        conn,
        vector_literal=vec_lit,
        embedding_model_version=EMBEDDING_MODEL_VERSION,
    )
    created_new = False
    if nid is None:
        nid = narratives_repo.insert_narrative(
            conn,
            embedding_model_id=emb_model_id,
            embedding_model_version=EMBEDDING_MODEL_VERSION,
            cluster_policy_version=CLUSTER_POLICY_VERSION,
        )
        created_new = True
        journal_repo.insert_narrative_event(
            conn,
            narrative_id=nid,
            event_type="NARRATIVE_CREATED",
            related_item_id=item_id,
            payload={"embedding_model_version": EMBEDDING_MODEL_VERSION},
        )
        # Snapshot técnico T0 (identidad narrativa sin señal operativa).
        # Outcome Engine: usar como ancla temporal / existencia; el primer snapshot
        # operativo útil para mercado es el de `threshold` tras primer SCORE_CHANGED.
        journal_repo.insert_narrative_snapshot(
            conn,
            narrative_id=nid,
            snapshot_ts_utc=_utcnow(),
            reason="scheduled",
            score=0,
            state="early",
            trend="flat",
            item_count=0,
            source_dist={},
            score_breakdown={
                "snapshot_kind": "technical_birth",
                "purpose": "narrative_identity_t0_not_operational_signal",
                "outcome_engine_note": "join_anchor_only_pre_first_item",
            },
            cluster_policy_version=CLUSTER_POLICY_VERSION,
            scoring_policy_version=None,
            embedding_model_version=EMBEDDING_MODEL_VERSION,
            fingerprint=f"technical_birth_{nid}",
        )

    sim: float | None = None
    if not created_new:
        nc_row = narratives_repo.get_narrative_current(conn, nid)
        if nc_row and nc_row.get("rep_embedding") is not None:
            with conn.cursor() as curx:
                curx.execute(
                    "SELECT (rep_embedding <=> %s::vector)::float FROM narrative_current WHERE narrative_id = %s",
                    (vec_lit, str(nid)),
                )
                r0 = curx.fetchone()
                if r0:
                    sim = max(0.0, 1.0 - float(r0[0]))

    try:
        narratives_repo.link_item_to_narrative(
            conn, narrative_id=nid, item_id=item_id, similarity=sim
        )
    except UniqueViolation:
        return ProcessItemResult(
            status="skipped_duplicate",
            item_id=item_id,
            narrative_id=nid,
            detail="item_ya_enlazado_a_narrativa",
        )

    items_repo.update_item_stage(conn, item_id, "narrative_linked")

    journal_repo.insert_narrative_event(
        conn,
        narrative_id=nid,
        event_type="ITEM_LINKED",
        related_item_id=item_id,
        payload={"stage": "post_link"},
    )

    cnt = narratives_repo.count_narrative_items(conn, nid)
    dist = {str(cand.source_id): 1}
    if cnt == 1:
        narratives_repo.update_narrative_current_first_item(
            conn,
            narrative_id=nid,
            title=canonicalization.title_for_display(cand.title),
            published_at=cand.published_at,
            vector_literal=vec_lit,
            source_dist=dist,
        )
        journal_repo.insert_representation_history(
            conn,
            narrative_id=nid,
            rep_version=1,
            vector_literal=vec_lit,
            based_on_item_sample={"item_ids": [item_id], "k": 1, "reason": "first_item"},
        )
        journal_repo.insert_narrative_event(
            conn,
            narrative_id=nid,
            event_type="REP_UPDATED",
            related_item_id=item_id,
            payload={"rep_version": 1, "reason": "first_item"},
        )
    else:
        cur_row = narratives_repo.get_narrative_current(conn, nid)
        prev_dist = dict(cur_row["source_dist"]) if cur_row and cur_row.get("source_dist") else {}
        merged = {**prev_dist}
        k = str(cand.source_id)
        merged[k] = int(merged.get(k, 0)) + 1

        if cnt > 1 and cnt % REP_RECOMPUTE_EVERY_N_ITEMS == 0:
            texts = narratives_repo.fetch_item_vectors_text(
                conn, nid, EMBEDDING_MODEL_VERSION, limit=200
            )
            vecs = [narratives_repo.parse_pg_vector_text(t) for t in texts]
            new_rep = representation.centroid_l2_normalized(vecs)
            new_lit = embeddings_stub.vector_to_pg_literal(new_rep)
            new_ver = narratives_repo.update_rep_after_batch(
                conn,
                narrative_id=nid,
                vector_literal=new_lit,
                based_on_sample={"item_count": cnt, "k_used": len(vecs)},
            )
            journal_repo.insert_representation_history(
                conn,
                narrative_id=nid,
                rep_version=new_ver,
                vector_literal=new_lit,
                based_on_item_sample={"item_ids_hint": "last_k_linked", "n": len(vecs)},
            )
            journal_repo.insert_narrative_event(
                conn,
                narrative_id=nid,
                event_type="REP_UPDATED",
                related_item_id=item_id,
                payload={"rep_version": new_ver, "reason": f"batch_every_{REP_RECOMPUTE_EVERY_N_ITEMS}"},
            )
            narratives_repo.update_narrative_current_more_items(
                conn,
                narrative_id=nid,
                published_at=cand.published_at,
                source_dist=merged,
            )
        else:
            narratives_repo.update_narrative_current_more_items(
                conn,
                narrative_id=nid,
                published_at=cand.published_at,
                source_dist=merged,
            )

    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT avg(sub.sentiment)::float
            FROM (
                SELECT DISTINCT nil.item_id, p.sentiment
                FROM narrative_item_links nil
                JOIN item_nlp_profiles p ON p.item_id = nil.item_id
                WHERE nil.narrative_id = %s
            ) sub
            """,
            (str(nid),),
        )
        avg_sent = cur.fetchone()[0]
        cur.execute(
            """
            SELECT count(DISTINCT io.source_id)::int
            FROM narrative_item_links nil
            JOIN item_occurrences io ON io.item_id = nil.item_id
            WHERE nil.narrative_id = %s
            """,
            (str(nid),),
        )
        n_src = cur.fetchone()[0]
    avg_sent = float(avg_sent or 0.0)
    n_src = int(n_src or 1)

    cur_row = narratives_repo.get_narrative_current(conn, nid)
    ic = int(cur_row["item_count"]) if cur_row else cnt
    score, breakdown, spol = scoring_v0.score_narrative_v0(
        item_count=ic,
        sentiment=avg_sent,
        intensity=float(nlp.intensity),
        distinct_sources=n_src,
    )
    new_state = scoring_v0.state_from_score_v0(score)
    trend = "flat"
    prev_score, prev_state = narratives_repo.apply_score_and_state(
        conn,
        narrative_id=nid,
        score=score,
        state=new_state,
        trend=trend,
        breakdown=breakdown,
        scoring_policy_version=spol,
    )

    fresh = narratives_repo.get_narrative_current(conn, nid)

    if prev_score != score:
        journal_repo.insert_narrative_event(
            conn,
            narrative_id=nid,
            event_type="SCORE_CHANGED",
            related_item_id=item_id,
            payload={"breakdown": breakdown},
            score_before=prev_score,
            score_after=score,
        )
        journal_repo.insert_narrative_snapshot(
            conn,
            narrative_id=nid,
            snapshot_ts_utc=_utcnow(),
            reason="threshold",
            score=score,
            state=new_state,
            trend=trend,
            item_count=ic,
            source_dist=dict(fresh["source_dist"]) if fresh and fresh.get("source_dist") else {},
            score_breakdown=breakdown,
            cluster_policy_version=CLUSTER_POLICY_VERSION,
            scoring_policy_version=spol,
            embedding_model_version=EMBEDDING_MODEL_VERSION,
            fingerprint=f"score_{nid}_{score}_{item_id}",
        )
    if prev_state != new_state:
        journal_repo.insert_narrative_event(
            conn,
            narrative_id=nid,
            event_type="STATE_CHANGED",
            related_item_id=item_id,
            payload={"reason": "score_v0"},
            state_before=prev_state,
            state_after=new_state,
        )

    items_repo.update_item_stage(conn, item_id, "completed")
    log.info("PROCESS_ITEM ok item_id=%s narrative_id=%s", item_id, nid)
    return ProcessItemResult(
        status="completed",
        item_id=item_id,
        narrative_id=nid,
        detail="pipeline_v0",
    )


def raw_candidate_from_job_payload(p: dict[str, Any]) -> RawIngestCandidate:
    pub = p.get("published_at")
    published_at = None
    if isinstance(pub, str):
        published_at = datetime.fromisoformat(pub.replace("Z", "+00:00"))
    elif isinstance(pub, datetime):
        published_at = pub
    return RawIngestCandidate(
        source_id=int(p["source_id"]),
        title=str(p.get("title") or ""),
        body=str(p.get("body") or ""),
        fetched_url=p.get("fetched_url"),
        native_id=p.get("native_id"),
        published_at=published_at,
        raw_ingest_id=int(p["raw_ingest_id"]) if p.get("raw_ingest_id") is not None else None,
    )
