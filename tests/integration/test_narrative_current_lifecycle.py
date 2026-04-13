"""
Lifecycle PRIME: narratives → narrative_current → primer rep.

Invariantes en BASE DE DATOS (v0):
- Por cada INSERT en `narratives`, el trigger `trg_narratives_birth_narrative_current`
  crea exactamente una fila en `narrative_current` con el estado inicial acordado.
- CHECK `ck_narrative_current_rep_embedding_rep_version`:
  (rep_embedding IS NULL AND rep_version = 0)
  OR (rep_embedding IS NOT NULL AND rep_version >= 1)
- CHECK score en [0,100], item_count >= 0.

Invariantes en APLICACIÓN (no validadas por CHECK en esta migración):
- Coherencia de `item_count` con el número real de filas en `narrative_item_links`
  para ítems “aceptados”.
- Que la transición “primer ítem aceptado” ocurra en un único UPDATE (o equivalente
  atómico) para no commitear estados imposibles respecto al contrato de producto
  (p. ej. item_count >= 1 con rep NULL): PostgreSQL evalúa CHECK por sentencia,
  no deferible entre sentencias.
"""

from __future__ import annotations

import uuid

import pytest
from psycopg.errors import CheckViolation

pytestmark = pytest.mark.integration

# Debe coincidir con VECTOR_DIM en la migración v0.
VECTOR_DIM = 384


def _embedding_model_id(conn) -> int:
    with conn.cursor() as cur:
        cur.execute(
            "SELECT id FROM embedding_models WHERE active ORDER BY id LIMIT 1"
        )
        row = cur.fetchone()
        assert row is not None, "Seed embedding_models ausente: ejecuta migraciones v0."
        return int(row[0])


def _insert_narrative(conn) -> uuid.UUID:
    nid = uuid.uuid4()
    emb_id = _embedding_model_id(conn)
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO narratives (
                id, embedding_model_id, embedding_model_version, cluster_policy_version
            )
            VALUES (%s, %s, %s, %s)
            """,
            (
                nid,
                emb_id,
                "test-emb-v1",
                "clusterPolicy=v0_rep=incremental_centroid_frozen_v1",
            ),
        )
    return nid


def _rep_vector_sql_param() -> str:
    """Vector constante 384-d (literal para cast ::vector)."""
    return "[" + ",".join(["0.01"] * VECTOR_DIM) + "]"


class TestNarrativeBirthTrigger:
    def test_insert_narratives_creates_narrative_current_with_prime_initial_state(
        self, db_conn
    ) -> None:
        """Caso 1 — Birth trigger: fila current automática y valores iniciales exactos."""
        nid = _insert_narrative(db_conn)

        with db_conn.cursor() as cur:
            cur.execute(
                """
                SELECT
                    current_title,
                    state::text,
                    score,
                    trend::text,
                    rep_embedding,
                    rep_version,
                    item_count,
                    first_item_published_at,
                    last_item_published_at,
                    last_item_ingested_at,
                    dormant_since,
                    last_rep_computed_at,
                    score_breakdown,
                    scoring_policy_version,
                    scored_at
                FROM narrative_current
                WHERE narrative_id = %s
                """,
                (nid,),
            )
            row = cur.fetchone()

        assert row is not None, "Debe existir narrative_current tras INSERT en narratives."
        (
            current_title,
            state,
            score,
            trend,
            rep_embedding,
            rep_version,
            item_count,
            first_pub,
            last_pub,
            last_ing,
            dormant_since,
            last_rep,
            score_breakdown,
            scoring_policy_version,
            scored_at,
        ) = row

        assert current_title == ""
        assert state == "early"
        assert score == 0
        assert trend == "flat"
        assert rep_embedding is None
        assert rep_version == 0
        assert item_count == 0
        assert first_pub is None
        assert last_pub is None
        assert last_ing is None
        assert dormant_since is None
        assert last_rep is None
        assert score_breakdown is None
        assert scoring_policy_version is None
        assert scored_at is None


class TestFirstAcceptedItemTransition:
    def test_atomic_first_rep_update_passes_check_constraints(self, db_conn) -> None:
        """Caso 2 — Transición válida: item_count, rep_embedding y rep_version en un UPDATE."""
        nid = _insert_narrative(db_conn)
        vec = _rep_vector_sql_param()

        with db_conn.cursor() as cur:
            cur.execute(
                """
                UPDATE narrative_current
                SET
                    item_count = 1,
                    rep_embedding = %s::vector,
                    rep_version = 1,
                    first_item_published_at = now(),
                    last_item_published_at = now(),
                    last_item_ingested_at = now(),
                    last_rep_computed_at = now(),
                    updated_at = now()
                WHERE narrative_id = %s
                """,
                (vec, nid),
            )
            cur.execute(
                """
                SELECT item_count, rep_version, rep_embedding IS NOT NULL
                FROM narrative_current WHERE narrative_id = %s
                """,
                (nid,),
            )
            ic, rv, has_rep = cur.fetchone()

        assert ic == 1
        assert rv == 1
        assert has_rep is True


class TestInvalidRepLifecycle:
    def test_rep_null_with_rep_version_positive_fails_check(self, db_conn) -> None:
        """Caso 3a — inválido: rep_embedding NULL con rep_version >= 1."""
        nid = _insert_narrative(db_conn)
        with db_conn.cursor() as cur, pytest.raises(CheckViolation):
            cur.execute(
                """
                UPDATE narrative_current
                SET rep_version = 1, rep_embedding = NULL, updated_at = now()
                WHERE narrative_id = %s
                """,
                (nid,),
            )

    def test_rep_non_null_with_rep_version_zero_fails_check(self, db_conn) -> None:
        """Caso 3b — inválido: rep_embedding no nulo con rep_version = 0."""
        nid = _insert_narrative(db_conn)
        vec = _rep_vector_sql_param()
        with db_conn.cursor() as cur, pytest.raises(CheckViolation):
            cur.execute(
                """
                UPDATE narrative_current
                SET rep_embedding = %s::vector, rep_version = 0, updated_at = now()
                WHERE narrative_id = %s
                """,
                (vec, nid),
            )
