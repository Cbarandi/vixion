"""Embedding determinista v0 (sin API externa). Vector L2-normalizado dim=VECTOR_DIM."""

from __future__ import annotations

import hashlib
import math
import re
import struct

from vixion.constants import STUB_EMBED_BAG_WEIGHT, VECTOR_DIM, EMBEDDING_MODEL_VERSION

_TOKEN_RE = re.compile(r"[a-zA-Z]{4,}")


def _bag_token_unit_vector(text: str, dim: int) -> list[float]:
    """Histograma de tokens → dim vía hash (misma raíz léxica ≈ mismos bins activados)."""
    vec = [0.0] * dim
    for tok in _TOKEN_RE.findall((text or "").lower()):
        h = int.from_bytes(
            hashlib.blake2b(tok.encode("utf-8"), digest_size=4).digest(),
            "little",
        ) % dim
        vec[h] += 1.0
    norm = math.sqrt(sum(x * x for x in vec)) or 1.0
    return [x / norm for x in vec]


def _shake_unit_vector(seed: str, dim: int) -> list[float]:
    raw = hashlib.shake_256(seed.encode("utf-8")).digest(dim * 4)
    floats = list(struct.unpack("<" + "f" * dim, raw))
    floats = [0.0 if not math.isfinite(x) else x for x in floats]
    norm = math.sqrt(sum(x * x for x in floats)) or 1.0
    return [x / norm for x in floats]


def stub_embedding_vector(text: str, dim: int = VECTOR_DIM) -> list[float]:
    """
    v2: mezcla vector global (shake del texto) + vector bag-of-tokens (bins hash).
    v1 solo shake: casi ortogonal entre artículos → ningún match bajo el umbral coseno.
    """
    text = (text or "").strip()
    w = float(STUB_EMBED_BAG_WEIGHT)
    w = min(1.0, max(0.0, w))
    g = _shake_unit_vector(text, dim) if text else _shake_unit_vector("", dim)
    b = _bag_token_unit_vector(text, dim)
    merged = [w * b[i] + (1.0 - w) * g[i] for i in range(dim)]
    norm = math.sqrt(sum(x * x for x in merged)) or 1.0
    return [x / norm for x in merged]


def vector_to_pg_literal(vec: list[float]) -> str:
    return "[" + ",".join(str(v) for v in vec) + "]"


def embedding_model_version() -> str:
    return EMBEDDING_MODEL_VERSION
