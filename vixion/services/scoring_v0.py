"""Scoring v0 — heurística operativa mínima, NO señal de trading."""

from __future__ import annotations

from vixion.constants import SCORING_POLICY_VERSION, SCORING_V0_LAYER


def score_narrative_v0(
    *,
    item_count: int,
    sentiment: float,
    intensity: float,
    distinct_sources: int,
) -> tuple[int, dict, str]:
    """
    Fórmula cerrada v0 (inputs reales del pipeline):

    - ``base = min(55, 12 + item_count * 6)``  — tope 55 (~8+ ítems); masa narrativa.
    - ``int_bonus = min(25, int(intensity * 28))`` — tope 25; ``intensity`` ∈ [0,1].
    - ``sent_tilt = clamp(round(sentiment * 12), -8, 8)`` — ``sentiment`` ∈ [-1,1].
    - ``src_bonus = min(15, distinct_sources * 5)`` — tope 15 (≥3 fuentes).

    ``score = clamp(base + int_bonus + sent_tilt + src_bonus, 0, 100)``.

    Estabilidad: determinista en los inputs. Riesgo: umbrales de ``state_from_score_v0``
    son arbitrarios v0; no usar para alpha hasta reemplazo por motor de scoring
    versionado y calibrado.
    """
    base = min(55, 12 + item_count * 6)
    int_bonus = min(25, int(intensity * 28))
    sent_tilt = int(max(-8, min(8, sentiment * 12)))
    src_bonus = min(15, distinct_sources * 5)
    raw = base + int_bonus + sent_tilt + src_bonus
    score = int(max(0, min(100, raw)))
    breakdown = {
        "scoring_layer": SCORING_V0_LAYER,
        "not_for_downstream_signal": True,
        "base_volume": base,
        "intensity_bonus": int_bonus,
        "sentiment_tilt": sent_tilt,
        "source_diversity_bonus": src_bonus,
        "raw_sum_pre_clamp": raw,
        "formula": "v0_volume_sentiment_v1",
        "inputs": {
            "item_count": item_count,
            "sentiment": sentiment,
            "intensity": intensity,
            "distinct_sources": distinct_sources,
        },
    }
    return score, breakdown, SCORING_POLICY_VERSION


def state_from_score_v0(score: int) -> str:
    """Umbrales fijos v0 (28 / 52 / 78) — arbitrarios; solo para UI ordenada."""
    if score < 28:
        return "early"
    if score < 52:
        return "emerging"
    if score < 78:
        return "confirmed"
    return "fading"
