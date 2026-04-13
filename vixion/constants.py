"""Constantes operativas PRIME compartidas (alineadas con migración v0)."""

from __future__ import annotations

VECTOR_DIM = 384

# Versionado explícito (strings en DB / trazabilidad).
# v2: stub mezcla hash global + sketch léxico para que noticias solapadas converjan (v1 era casi ortogonal).
EMBEDDING_MODEL_VERSION = "stub-deterministic-v2"
NLP_MODEL_VERSION = "stub-v1"
CLUSTER_POLICY_VERSION = "incremental_centroid_frozen_v1;M=10;H=6h;Kmax=200"
SCORING_POLICY_VERSION = "v0_volume_sentiment_v1"
# Etiqueta explícita para no mezclar con scoring “de producto” / Fase 2.
SCORING_V0_LAYER = "v0_placeholder_operational_only"

# Asignación narrativa: distancia coseno pgvector (<=>) sobre vectores unitarios.
# Con stub v2 (mezcla shake + bag léxico), pares temáticamente cercanos quedan ~0.55–0.80;
# 0.22 solo era viable con embeddings semánticos reales, no con stub PRIME.
MAX_COSINE_DISTANCE_FOR_ASSIGN = 0.78

# Peso del canal bag-of-words (hash a dim) en stub v2. Mayor → más convergencia por vocabulario compartido.
STUB_EMBED_BAG_WEIGHT = 0.62

# Cada M ítems enlazados se recalcula rep (centroide) en v0.
REP_RECOMPUTE_EVERY_N_ITEMS = 10
