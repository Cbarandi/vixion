"""rep(N) incremental_centroid_frozen_v1 — solo matemática pura."""

from __future__ import annotations

import math


def centroid_l2_normalized(vectors: list[list[float]]) -> list[float]:
    if not vectors:
        raise ValueError("vectors vacío")
    dim = len(vectors[0])
    acc = [0.0] * dim
    for v in vectors:
        for i, x in enumerate(v):
            acc[i] += x
    n = len(vectors)
    acc = [x / n for x in acc]
    norm = math.sqrt(sum(x * x for x in acc)) or 1.0
    return [x / norm for x in acc]
