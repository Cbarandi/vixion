"""VIXION PRIME — DB core v0 (PostgreSQL + pgvector + queue).

Revision ID: v0_prime_core
Revises:
Create Date: 2026-04-11

"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa
from sqlalchemy import text
from sqlalchemy.dialects.postgresql import ENUM, JSONB, UUID as PGUUID

from pgvector.sqlalchemy import Vector

# PRIME Phase 1: single embedding dimension (sentence-transformers / MiniLM class).
VECTOR_DIM = 384

revision = "v0_prime_core"
down_revision = None
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("CREATE EXTENSION IF NOT EXISTS vector")

    # --- PostgreSQL ENUMs (explicit; columns use create_type=False) ---
    enums = [
        "CREATE TYPE source_kind AS ENUM ('rss', 'reddit')",
        "CREATE TYPE ingest_status AS ENUM ('running', 'success', 'partial', 'failed')",
        "CREATE TYPE dedupe_kind AS ENUM ('canonical_url', 'source_native_id', 'content_hash', 'new_unique')",
        """CREATE TYPE item_processing_stage AS ENUM (
            'received', 'normalized', 'nlp_done', 'embedded', 'narrative_linked',
            'completed', 'failed', 'skipped_non_en'
        )""",
        "CREATE TYPE nlp_content_type AS ENUM ('news', 'opinion', 'discussion', 'headline', 'unknown')",
        "CREATE TYPE narrative_state AS ENUM ('early', 'emerging', 'confirmed', 'fading', 'dormant')",
        "CREATE TYPE narrative_trend AS ENUM ('up', 'flat', 'down')",
        """CREATE TYPE narrative_event_type AS ENUM (
            'NARRATIVE_CREATED', 'ITEM_LINKED', 'REP_UPDATED', 'SCORE_CHANGED',
            'STATE_CHANGED', 'PEAK_DETECTED', 'DORMANT_MARKED', 'REVIEW_RECORDED'
        )""",
        "CREATE TYPE snapshot_reason AS ENUM ('scheduled', 'threshold', 'state_change', 'peak', 'manual')",
        "CREATE TYPE review_verdict AS ENUM ('good', 'bad', 'unsure')",
        """CREATE TYPE review_reason_code AS ENUM (
            'off_topic', 'too_broad', 'duplicate_theme', 'language_noise', 'spam', 'other'
        )""",
        "CREATE TYPE job_status AS ENUM ('pending', 'running', 'succeeded', 'failed', 'dead')",
        """CREATE TYPE job_type AS ENUM (
            'PROCESS_ITEM', 'RECOMPUTE_NARRATIVE_SCORE', 'RECOMPUTE_REPRESENTATION',
            'MARK_DORMANT', 'INGEST_SOURCE_TICK'
        )""",
        "CREATE TYPE market_timeframe AS ENUM ('1h', '1d')",
        "CREATE TYPE content_locale_status AS ENUM ('accepted_en', 'rejected_non_en')",
        "CREATE TYPE language_code AS ENUM ('en', 'und')",
    ]
    for stmt in enums:
        op.execute(text(stmt))

    sk = ENUM("rss", "reddit", name="source_kind", create_type=False)
    is_ = ENUM("running", "success", "partial", "failed", name="ingest_status", create_type=False)
    dk = ENUM(
        "canonical_url",
        "source_native_id",
        "content_hash",
        "new_unique",
        name="dedupe_kind",
        create_type=False,
    )
    ips = ENUM(
        "received",
        "normalized",
        "nlp_done",
        "embedded",
        "narrative_linked",
        "completed",
        "failed",
        "skipped_non_en",
        name="item_processing_stage",
        create_type=False,
    )
    nct = ENUM(
        "news",
        "opinion",
        "discussion",
        "headline",
        "unknown",
        name="nlp_content_type",
        create_type=False,
    )
    ns = ENUM(
        "early",
        "emerging",
        "confirmed",
        "fading",
        "dormant",
        name="narrative_state",
        create_type=False,
    )
    nt = ENUM("up", "flat", "down", name="narrative_trend", create_type=False)
    net = ENUM(
        "NARRATIVE_CREATED",
        "ITEM_LINKED",
        "REP_UPDATED",
        "SCORE_CHANGED",
        "STATE_CHANGED",
        "PEAK_DETECTED",
        "DORMANT_MARKED",
        "REVIEW_RECORDED",
        name="narrative_event_type",
        create_type=False,
    )
    sr = ENUM(
        "scheduled",
        "threshold",
        "state_change",
        "peak",
        "manual",
        name="snapshot_reason",
        create_type=False,
    )
    rv = ENUM("good", "bad", "unsure", name="review_verdict", create_type=False)
    rrc = ENUM(
        "off_topic",
        "too_broad",
        "duplicate_theme",
        "language_noise",
        "spam",
        "other",
        name="review_reason_code",
        create_type=False,
    )
    jst = ENUM("pending", "running", "succeeded", "failed", "dead", name="job_status", create_type=False)
    jt = ENUM(
        "PROCESS_ITEM",
        "RECOMPUTE_NARRATIVE_SCORE",
        "RECOMPUTE_REPRESENTATION",
        "MARK_DORMANT",
        "INGEST_SOURCE_TICK",
        name="job_type",
        create_type=False,
    )
    mtf = ENUM("1h", "1d", name="market_timeframe", create_type=False)
    cls = ENUM("accepted_en", "rejected_non_en", name="content_locale_status", create_type=False)
    lc = ENUM("en", "und", name="language_code", create_type=False)

    op.create_table(
        "embedding_models",
        sa.Column("id", sa.SmallInteger(), primary_key=True, autoincrement=True),
        sa.Column("name", sa.Text(), nullable=False),
        sa.Column("dimension", sa.SmallInteger(), nullable=False),
        sa.Column("active", sa.Boolean(), nullable=False, server_default=sa.text("true")),
        sa.Column("notes", sa.Text(), nullable=True),
    )

    op.create_table(
        "sources",
        sa.Column("id", sa.BigInteger(), primary_key=True, autoincrement=True),
        sa.Column("source_kind", sk, nullable=False),
        sa.Column("name", sa.Text(), nullable=False),
        sa.Column("config", JSONB(), nullable=False, server_default=sa.text("'{}'::jsonb")),
        sa.Column("is_enabled", sa.Boolean(), nullable=False, server_default=sa.text("true")),
        sa.Column("source_quality_tier", sa.SmallInteger(), nullable=True),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("now()"),
        ),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("now()"),
        ),
    )

    op.create_table(
        "raw_ingests",
        sa.Column("id", sa.BigInteger(), primary_key=True, autoincrement=True),
        sa.Column("source_id", sa.BigInteger(), sa.ForeignKey("sources.id", ondelete="CASCADE"), nullable=False),
        sa.Column("started_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("now()")),
        sa.Column("finished_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column(
            "status",
            is_,
            nullable=False,
            server_default=sa.text("'running'::ingest_status"),
        ),
        sa.Column("stats", JSONB(), nullable=False, server_default=sa.text("'{}'::jsonb")),
        sa.Column("error_message", sa.Text(), nullable=True),
    )

    op.create_table(
        "items",
        sa.Column("id", sa.BigInteger(), primary_key=True, autoincrement=True),
        sa.Column("canonical_url", sa.Text(), nullable=True),
        sa.Column("content_hash", sa.Text(), nullable=False),
        sa.Column("source_native_id", sa.Text(), nullable=True),
        sa.Column("title", sa.Text(), nullable=False, server_default=sa.text("''")),
        sa.Column("body_text", sa.Text(), nullable=False, server_default=sa.text("''")),
        sa.Column(
            "language",
            lc,
            nullable=False,
            server_default=sa.text("'en'::language_code"),
        ),
        sa.Column(
            "content_locale_status",
            cls,
            nullable=False,
            server_default=sa.text("'accepted_en'::content_locale_status"),
        ),
        sa.Column("first_seen_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("now()")),
        sa.Column("last_seen_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("now()")),
        sa.Column("primary_source_id", sa.BigInteger(), sa.ForeignKey("sources.id", ondelete="RESTRICT"), nullable=True),
        sa.Column(
            "dedupe_kind",
            dk,
            nullable=False,
            server_default=sa.text("'new_unique'::dedupe_kind"),
        ),
        sa.Column(
            "processing_stage",
            ips,
            nullable=False,
            server_default=sa.text("'received'::item_processing_stage"),
        ),
        sa.Column("canonical_text_locked_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("now()"),
        ),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("now()"),
        ),
        sa.CheckConstraint("length(content_hash) >= 32", name="ck_items_content_hash_min_len"),
    )
    op.create_index("ix_items_last_seen_at", "items", ["last_seen_at"], unique=False)

    op.create_table(
        "item_occurrences",
        sa.Column("id", sa.BigInteger(), primary_key=True, autoincrement=True),
        sa.Column("item_id", sa.BigInteger(), sa.ForeignKey("items.id", ondelete="CASCADE"), nullable=False),
        sa.Column("source_id", sa.BigInteger(), sa.ForeignKey("sources.id", ondelete="RESTRICT"), nullable=False),
        sa.Column("raw_ingest_id", sa.BigInteger(), sa.ForeignKey("raw_ingests.id", ondelete="SET NULL"), nullable=True),
        sa.Column("fetched_url", sa.Text(), nullable=True),
        sa.Column("published_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("ingested_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("now()")),
        sa.Column("native_id", sa.Text(), nullable=True),
        sa.Column("occurrence_fingerprint", sa.Text(), nullable=False),
        sa.UniqueConstraint("item_id", "occurrence_fingerprint", name="uq_item_occurrences_item_fingerprint"),
    )

    op.create_table(
        "item_nlp_profiles",
        sa.Column("id", sa.BigInteger(), primary_key=True, autoincrement=True),
        sa.Column("item_id", sa.BigInteger(), sa.ForeignKey("items.id", ondelete="CASCADE"), nullable=False),
        sa.Column("nlp_model_version", sa.Text(), nullable=False),
        sa.Column(
            "content_type",
            nct,
            nullable=False,
            server_default=sa.text("'unknown'::nlp_content_type"),
        ),
        sa.Column("sentiment", sa.Numeric(5, 4), nullable=True),
        sa.Column("intensity", sa.Numeric(5, 4), nullable=True),
        sa.Column("topics", JSONB(), nullable=False, server_default=sa.text("'[]'::jsonb")),
        sa.Column("extra", JSONB(), nullable=False, server_default=sa.text("'{}'::jsonb")),
        sa.Column("processed_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("now()")),
        sa.UniqueConstraint("item_id", "nlp_model_version", name="uq_item_nlp_profiles_item_model"),
        sa.CheckConstraint("sentiment IS NULL OR (sentiment >= -1 AND sentiment <= 1)", name="ck_item_nlp_sentiment_range"),
        sa.CheckConstraint("intensity IS NULL OR (intensity >= 0 AND intensity <= 1)", name="ck_item_nlp_intensity_range"),
    )

    op.create_table(
        "item_nlp_entities",
        sa.Column("id", sa.BigInteger(), primary_key=True, autoincrement=True),
        sa.Column(
            "nlp_profile_id",
            sa.BigInteger(),
            sa.ForeignKey("item_nlp_profiles.id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column("entity_text", sa.Text(), nullable=False),
        sa.Column("entity_type", sa.Text(), nullable=False),
        sa.Column("salience", sa.Numeric(6, 5), nullable=True),
    )

    op.create_table(
        "item_embeddings",
        sa.Column("id", sa.BigInteger(), primary_key=True, autoincrement=True),
        sa.Column("item_id", sa.BigInteger(), sa.ForeignKey("items.id", ondelete="CASCADE"), nullable=False),
        sa.Column(
            "embedding_model_id",
            sa.SmallInteger(),
            sa.ForeignKey("embedding_models.id", ondelete="RESTRICT"),
            nullable=False,
        ),
        sa.Column("embedding_model_version", sa.Text(), nullable=False),
        sa.Column("vector", Vector(VECTOR_DIM), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("now()")),
        sa.UniqueConstraint(
            "item_id",
            "embedding_model_id",
            "embedding_model_version",
            name="uq_item_embeddings_item_model_version",
        ),
    )

    op.create_table(
        "narratives",
        sa.Column("id", PGUUID(as_uuid=True), primary_key=True, server_default=text("gen_random_uuid()")),
        sa.Column(
            "embedding_model_id",
            sa.SmallInteger(),
            sa.ForeignKey("embedding_models.id", ondelete="RESTRICT"),
            nullable=False,
        ),
        sa.Column("embedding_model_version", sa.Text(), nullable=False),
        sa.Column("cluster_policy_version", sa.Text(), nullable=False),
        sa.Column("lineage_parent_narrative_id", PGUUID(as_uuid=True), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("now()")),
        sa.ForeignKeyConstraint(
            ["lineage_parent_narrative_id"],
            ["narratives.id"],
            name="fk_narratives_lineage_parent",
            ondelete="SET NULL",
        ),
    )

    op.create_table(
        "narrative_current",
        sa.Column(
            "narrative_id",
            PGUUID(as_uuid=True),
            sa.ForeignKey("narratives.id", ondelete="CASCADE"),
            primary_key=True,
        ),
        sa.Column("current_title", sa.Text(), nullable=False, server_default=sa.text("''")),
        sa.Column(
            "state",
            ns,
            nullable=False,
            server_default=sa.text("'early'::narrative_state"),
        ),
        sa.Column("score", sa.SmallInteger(), nullable=False, server_default=sa.text("0")),
        sa.Column(
            "trend",
            nt,
            nullable=False,
            server_default=sa.text("'flat'::narrative_trend"),
        ),
        sa.Column("rep_embedding", Vector(VECTOR_DIM), nullable=True),
        sa.Column("rep_version", sa.Integer(), nullable=False, server_default=sa.text("0")),
        sa.Column("item_count", sa.Integer(), nullable=False, server_default=sa.text("0")),
        sa.Column("source_dist", JSONB(), nullable=False, server_default=sa.text("'{}'::jsonb")),
        sa.Column("first_item_published_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("last_item_published_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("last_item_ingested_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("dormant_since", sa.DateTime(timezone=True), nullable=True),
        sa.Column("last_rep_computed_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("score_breakdown", JSONB(), nullable=True),
        sa.Column("scoring_policy_version", sa.Text(), nullable=True),
        sa.Column("scored_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("now()"),
        ),
        sa.CheckConstraint("score >= 0 AND score <= 100", name="ck_narrative_current_score_range"),
        sa.CheckConstraint("item_count >= 0", name="ck_narrative_current_item_count_nonneg"),
        # PRIME: rep_embedding NULL iff rep_version = 0; any computed rep bumps version to >= 1.
        sa.CheckConstraint(
            "((rep_embedding IS NULL) AND (rep_version = 0)) "
            "OR ((rep_embedding IS NOT NULL) AND (rep_version >= 1))",
            name="ck_narrative_current_rep_embedding_rep_version",
        ),
    )

    # --- Narrative birth: exactly one narrative_current row per narratives row (DB-enforced) ---
    op.execute(text("CREATE SCHEMA IF NOT EXISTS vixion"))
    op.execute(
        text(
            """
            CREATE OR REPLACE FUNCTION vixion.tg_narratives_create_current_row()
            RETURNS trigger
            LANGUAGE plpgsql
            AS $fn$
            BEGIN
              INSERT INTO narrative_current (
                narrative_id,
                current_title,
                state,
                score,
                trend,
                rep_embedding,
                rep_version,
                item_count,
                source_dist,
                first_item_published_at,
                last_item_published_at,
                last_item_ingested_at,
                dormant_since,
                last_rep_computed_at,
                score_breakdown,
                scoring_policy_version,
                scored_at,
                updated_at
              )
              VALUES (
                NEW.id,
                '',
                'early'::narrative_state,
                0,
                'flat'::narrative_trend,
                NULL,
                0,
                0,
                '{}'::jsonb,
                NULL,
                NULL,
                NULL,
                NULL,
                NULL,
                NULL,
                NULL,
                NULL,
                now()
              );
              RETURN NEW;
            END;
            $fn$
            """
        )
    )
    op.execute(
        text(
            """
            CREATE TRIGGER trg_narratives_birth_narrative_current
            AFTER INSERT ON narratives
            FOR EACH ROW
            EXECUTE PROCEDURE vixion.tg_narratives_create_current_row()
            """
        )
    )

    op.execute(
        text(
            """
            COMMENT ON TABLE narrative_current IS
            'PRIME mutable projection. One row per narratives.id: auto-created on narrative INSERT. '
            'Birth: rep_embedding NULL, rep_version=0, item_count=0, score=0, trend=flat, state=early; '
            'first_item_published_at / last_item_published_at / last_item_ingested_at NULL. '
            'Post-first-accepted-item: single UPDATE sets item_count>=1, rep_embedding NOT NULL, rep_version>=1 '
            '(CHECK is evaluated per statement; never commit a state with item_count>=1 and rep_embedding NULL). '
            'Applications must not INSERT into narrative_current.';
            """
        )
    )
    op.execute(
        text(
            """
            COMMENT ON COLUMN narrative_current.rep_embedding IS
            'incremental_centroid_frozen_v1 vector; NULL only in birth (rep_version=0, item_count=0).';
            """
        )
    )
    op.execute(
        text(
            """
            COMMENT ON COLUMN narrative_current.rep_version IS
            '0 before first rep bump; >=1 once rep_embedding is set (see narrative_representation_history).';
            """
        )
    )
    op.execute(
        text(
            """
            COMMENT ON COLUMN narrative_current.item_count IS
            'Accepted linked items; 0 at birth; must match narrative_item_links cardinality in app layer.';
            """
        )
    )

    op.create_table(
        "narrative_item_links",
        sa.Column(
            "narrative_id",
            PGUUID(as_uuid=True),
            sa.ForeignKey("narratives.id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column("item_id", sa.BigInteger(), sa.ForeignKey("items.id", ondelete="CASCADE"), nullable=False),
        sa.Column("linked_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("now()")),
        sa.Column("similarity_to_rep_at_link", sa.Numeric(8, 6), nullable=True),
        sa.PrimaryKeyConstraint("narrative_id", "item_id", name="pk_narrative_item_links"),
        sa.UniqueConstraint("item_id", name="uq_narrative_item_links_item_id"),
    )

    op.create_table(
        "narrative_representation_history",
        sa.Column("id", sa.BigInteger(), primary_key=True, autoincrement=True),
        sa.Column(
            "narrative_id",
            PGUUID(as_uuid=True),
            sa.ForeignKey("narratives.id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column("rep_version", sa.Integer(), nullable=False),
        sa.Column("rep_embedding", Vector(VECTOR_DIM), nullable=False),
        sa.Column("method", sa.Text(), nullable=False),
        sa.Column("based_on_item_sample", JSONB(), nullable=True),
        sa.Column("effective_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("now()")),
        sa.CheckConstraint(
            "method = 'incremental_centroid_frozen_v1'",
            name="ck_narrative_rep_history_method",
        ),
    )

    ns_nullable = ENUM(
        "early",
        "emerging",
        "confirmed",
        "fading",
        "dormant",
        name="narrative_state",
        create_type=False,
    )

    op.create_table(
        "narrative_events",
        sa.Column("id", sa.BigInteger(), primary_key=True, autoincrement=True),
        sa.Column(
            "narrative_id",
            PGUUID(as_uuid=True),
            sa.ForeignKey("narratives.id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column("event_type", net, nullable=False),
        sa.Column("occurred_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("related_item_id", sa.BigInteger(), sa.ForeignKey("items.id", ondelete="SET NULL"), nullable=True),
        sa.Column("payload", JSONB(), nullable=False, server_default=sa.text("'{}'::jsonb")),
        sa.Column("score_before", sa.SmallInteger(), nullable=True),
        sa.Column("score_after", sa.SmallInteger(), nullable=True),
        sa.Column("state_before", ns_nullable, nullable=True),
        sa.Column("state_after", ns_nullable, nullable=True),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("now()"),
        ),
    )

    op.create_table(
        "narrative_snapshots",
        sa.Column("id", sa.BigInteger(), primary_key=True, autoincrement=True),
        sa.Column(
            "narrative_id",
            PGUUID(as_uuid=True),
            sa.ForeignKey("narratives.id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column("snapshot_ts_utc", sa.DateTime(timezone=True), nullable=False),
        sa.Column("reason", sr, nullable=False),
        sa.Column("score", sa.SmallInteger(), nullable=False),
        sa.Column("state", ns, nullable=False),
        sa.Column("trend", nt, nullable=False),
        sa.Column("item_count", sa.Integer(), nullable=False),
        sa.Column("source_dist", JSONB(), nullable=False, server_default=sa.text("'{}'::jsonb")),
        sa.Column("score_breakdown", JSONB(), nullable=True),
        sa.Column("cluster_policy_version", sa.Text(), nullable=False),
        sa.Column("scoring_policy_version", sa.Text(), nullable=True),
        sa.Column("embedding_model_version", sa.Text(), nullable=False),
        sa.Column("fingerprint", sa.Text(), nullable=True),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("now()"),
        ),
        sa.CheckConstraint("score >= 0 AND score <= 100", name="ck_narrative_snapshots_score_range"),
    )

    op.create_table(
        "narrative_reviews",
        sa.Column("id", sa.BigInteger(), primary_key=True, autoincrement=True),
        sa.Column(
            "narrative_id",
            PGUUID(as_uuid=True),
            sa.ForeignKey("narratives.id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column("verdict", rv, nullable=False),
        sa.Column("reason_code", rrc, nullable=False),
        sa.Column("notes", sa.Text(), nullable=True),
        sa.Column("reviewer", sa.Text(), nullable=False),
        sa.Column("reviewed_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("now()")),
    )

    op.create_table(
        "jobs",
        sa.Column("id", sa.BigInteger(), primary_key=True, autoincrement=True),
        sa.Column("idempotency_key", sa.Text(), nullable=False),
        sa.Column("job_type", jt, nullable=False),
        sa.Column("stage", sa.Text(), nullable=True),
        sa.Column(
            "status",
            jst,
            nullable=False,
            server_default=sa.text("'pending'::job_status"),
        ),
        sa.Column("priority", sa.SmallInteger(), nullable=False, server_default=sa.text("0")),
        sa.Column("run_after", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("now()")),
        sa.Column("attempts", sa.Integer(), nullable=False, server_default=sa.text("0")),
        sa.Column("max_attempts", sa.Integer(), nullable=False, server_default=sa.text("10")),
        sa.Column("locked_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("locked_by", sa.Text(), nullable=True),
        sa.Column("last_error", sa.Text(), nullable=True),
        sa.Column("payload", JSONB(), nullable=False, server_default=sa.text("'{}'::jsonb")),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("now()"),
        ),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("now()"),
        ),
        sa.UniqueConstraint("idempotency_key", name="uq_jobs_idempotency_key"),
    )

    op.create_table(
        "market_bars_btc_spot",
        sa.Column("timeframe", mtf, nullable=False),
        sa.Column("bar_ts_utc", sa.DateTime(timezone=True), nullable=False),
        sa.Column("open", sa.Numeric(24, 8), nullable=False),
        sa.Column("high", sa.Numeric(24, 8), nullable=False),
        sa.Column("low", sa.Numeric(24, 8), nullable=False),
        sa.Column("close", sa.Numeric(24, 8), nullable=False),
        sa.Column("volume", sa.Numeric(24, 8), nullable=True),
        sa.Column("source", sa.Text(), nullable=False),
        sa.Column("ingested_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("now()")),
        sa.PrimaryKeyConstraint("timeframe", "bar_ts_utc", name="pk_market_bars_btc_spot"),
    )

    # --- Dedupe & API indexes ---
    op.execute(
        text(
            "CREATE UNIQUE INDEX uq_items_canonical_url ON items (canonical_url) "
            "WHERE canonical_url IS NOT NULL"
        )
    )
    op.create_index("uq_items_content_hash", "items", ["content_hash"], unique=True)

    op.execute(
        text(
            "CREATE UNIQUE INDEX uq_item_occurrences_source_native_id "
            "ON item_occurrences (source_id, native_id) WHERE native_id IS NOT NULL"
        )
    )
    op.create_index(
        "ix_item_occurrences_item_ingested",
        "item_occurrences",
        ["item_id", "ingested_at"],
        unique=False,
    )

    op.execute(
        text(
            "CREATE INDEX ix_jobs_queue_pending ON jobs (status, run_after, id) "
            "WHERE status = 'pending'::job_status"
        )
    )
    op.create_index("ix_jobs_created_at", "jobs", ["created_at"], unique=False)

    op.execute(
        text(
            "CREATE INDEX ix_narrative_current_api_score ON narrative_current "
            "(score DESC, updated_at DESC) WHERE state <> 'dormant'::narrative_state"
        )
    )
    op.execute(
        text(
            "CREATE INDEX ix_narrative_current_last_item ON narrative_current (last_item_ingested_at) "
            "WHERE last_item_ingested_at IS NOT NULL"
        )
    )

    op.execute(
        text(
            "CREATE INDEX ix_narrative_events_narrative_time ON narrative_events "
            "(narrative_id, occurred_at DESC)"
        )
    )
    op.execute(
        text(
            "CREATE INDEX ix_narrative_snapshots_narrative_time ON narrative_snapshots "
            "(narrative_id, snapshot_ts_utc DESC)"
        )
    )
    op.create_index("ix_narrative_snapshots_ts", "narrative_snapshots", ["snapshot_ts_utc"], unique=False)

    op.execute(
        text(
            "CREATE INDEX ix_narrative_reviews_narrative_time ON narrative_reviews "
            "(narrative_id, reviewed_at DESC)"
        )
    )

    # pgvector: cosine similarity (items normalized at application layer).
    op.execute(
        text(
            f"CREATE INDEX ix_item_embeddings_vector_hnsw ON item_embeddings "
            f"USING hnsw (vector vector_cosine_ops)"
        )
    )
    op.execute(
        text(
            "CREATE INDEX ix_narrative_current_rep_hnsw ON narrative_current "
            "USING hnsw (rep_embedding vector_cosine_ops) "
            "WHERE rep_embedding IS NOT NULL"
        )
    )

    # --- Seed: default embedding model (384-d MiniLM-class) ---
    op.execute(
        text(
            """
            INSERT INTO embedding_models (name, dimension, active, notes)
            VALUES (
                'sentence-transformers/all-MiniLM-L6-v2',
                :dim,
                true,
                'PRIME seed — Phase 1 English; VECTOR_DIM in migration must match dimension.'
            )
            """
        ).bindparams(dim=VECTOR_DIM)
    )


def downgrade() -> None:
    op.execute(text("DROP INDEX IF EXISTS ix_narrative_current_rep_hnsw"))
    op.execute(text("DROP INDEX IF EXISTS ix_item_embeddings_vector_hnsw"))
    op.execute(text("DROP INDEX IF EXISTS ix_narrative_reviews_narrative_time"))
    op.drop_index("ix_narrative_snapshots_ts", table_name="narrative_snapshots")
    op.execute(text("DROP INDEX IF EXISTS ix_narrative_snapshots_narrative_time"))
    op.execute(text("DROP INDEX IF EXISTS ix_narrative_events_narrative_time"))
    op.execute(text("DROP INDEX IF EXISTS ix_narrative_current_last_item"))
    op.execute(text("DROP INDEX IF EXISTS ix_narrative_current_api_score"))
    op.drop_index("ix_jobs_created_at", table_name="jobs")
    op.execute(text("DROP INDEX IF EXISTS ix_jobs_queue_pending"))
    op.drop_index("ix_item_occurrences_item_ingested", table_name="item_occurrences")
    op.execute(text("DROP INDEX IF EXISTS uq_item_occurrences_source_native_id"))
    op.drop_index("uq_items_content_hash", table_name="items")
    op.execute(text("DROP INDEX IF EXISTS uq_items_canonical_url"))

    op.drop_table("market_bars_btc_spot")
    op.drop_table("jobs")
    op.drop_table("narrative_reviews")
    op.drop_table("narrative_snapshots")
    op.drop_table("narrative_events")
    op.drop_table("narrative_representation_history")
    op.drop_table("narrative_item_links")

    op.execute(text("DROP TRIGGER IF EXISTS trg_narratives_birth_narrative_current ON narratives"))
    op.execute(text("DROP FUNCTION IF EXISTS vixion.tg_narratives_create_current_row()"))

    op.drop_table("narrative_current")
    op.drop_table("narratives")
    op.drop_table("item_embeddings")
    op.drop_table("item_nlp_entities")
    op.drop_table("item_nlp_profiles")
    op.drop_table("item_occurrences")
    op.drop_table("items")
    op.drop_table("raw_ingests")
    op.drop_table("sources")
    op.drop_table("embedding_models")

    op.execute(text("DROP SCHEMA IF EXISTS vixion"))

    types = [
        "language_code",
        "content_locale_status",
        "market_timeframe",
        "job_type",
        "job_status",
        "review_reason_code",
        "review_verdict",
        "snapshot_reason",
        "narrative_event_type",
        "narrative_trend",
        "narrative_state",
        "nlp_content_type",
        "item_processing_stage",
        "dedupe_kind",
        "ingest_status",
        "source_kind",
    ]
    for t in types:
        op.execute(text(f"DROP TYPE IF EXISTS {t} CASCADE"))

    op.execute("DROP EXTENSION IF EXISTS vector")
