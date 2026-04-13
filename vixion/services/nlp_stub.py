"""NLP mínimo v0 — stub determinista y explícito (sustituible por modelo real)."""

from __future__ import annotations

from dataclasses import dataclass

from vixion.constants import NLP_MODEL_VERSION


@dataclass(frozen=True, slots=True)
class NlpProfileV0:
    sentiment: float
    intensity: float
    content_type: str  # news | headline | discussion
    topics: list[str]


def build_nlp_profile(title: str, body: str) -> NlpProfileV0:
    text = f"{title}\n{body}".lower()
    # Sentimiento grosero por palabras clave (determinista).
    pos = sum(1 for w in ("growth", "surge", "gain", "bull", "win", "success") if w in text)
    neg = sum(1 for w in ("crash", "fear", "loss", "bear", "fail", "crisis") if w in text)
    sentiment = max(-1.0, min(1.0, (pos - neg) * 0.15))
    intensity = 0.35 + 0.05 * min(len(text) // 500, 5)
    intensity = min(1.0, intensity)
    combined = len(title) + len(body)
    if combined > 800:
        ctype = "news"
    elif combined > 120:
        ctype = "discussion"
    else:
        ctype = "headline"
    topics = [w.strip("#").lower() for w in title.split() if len(w) > 3][:8]
    return NlpProfileV0(sentiment=sentiment, intensity=intensity, content_type=ctype, topics=topics)


def nlp_model_version() -> str:
    return NLP_MODEL_VERSION
