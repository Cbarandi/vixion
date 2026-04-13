#!/usr/bin/env python3
"""Scoring v1: prioridad, señal y riesgo sobre artículos ya clasificados."""

from __future__ import annotations

import json
import re
import sys
from datetime import UTC, datetime
from pathlib import Path
from typing import Any


PROJECT_ROOT = Path(__file__).resolve().parent.parent
DATA_CLASSIFIED_DIR = PROJECT_ROOT / "data" / "classified"
DATA_SCORED_DIR = PROJECT_ROOT / "data" / "scored"

# --- Ajuste fino v1 (todos los puntos viven aquí) ---

PRIORITY_TAG_COUNT_PTS_PER_TAG = 7
PRIORITY_TAG_COUNT_CAP = 35

PRIORITY_HIGH_VALUE_TAGS: dict[str, int] = {
    "geopolitics": 14,
    "macro": 12,
    "energy": 12,
    "ai": 10,
    "crypto": 10,
}

PRIORITY_NARRATIVE_PTS_EACH = 8
PRIORITY_NARRATIVE_CAP = 24

SIGNAL_COMBOS: dict[frozenset[str], tuple[str, int]] = {
    frozenset({"energy", "geopolitics"}): ("energy+geopolitics", 32),
    frozenset({"macro", "markets"}): ("macro+markets", 28),
    frozenset({"ai", "equities"}): ("ai+equities", 26),
    frozenset({"crypto", "regulation"}): ("crypto+regulation", 26),
    frozenset({"banking", "equities"}): ("banking+equities", 24),
}

RISK_TAG_POINTS: dict[str, int] = {
    "geopolitics": 18,
    "energy": 14,
    "macro": 14,
}

RISK_KEYWORD_POINTS: dict[str, int] = {
    "war": 16,
    "sanctions": 14,
    "blockade": 14,
    "volatility": 12,
    "selloff": 14,
    "sell-off": 12,
    "recession": 16,
}


def clamp_score(value: float | int) -> int:
    """Limita un score al rango 0–100."""
    return max(0, min(100, int(round(float(value)))))


def score_bucket(priority_score: int) -> str:
    """Bucket por prioridad (solo mira priority_score)."""
    if priority_score >= 80:
        return "critical"
    if priority_score >= 55:
        return "high"
    if priority_score >= 30:
        return "medium"
    return "low"


def _article_text(article: dict[str, Any]) -> str:
    parts = [
        article.get("title") or "",
        article.get("summary") or "",
        article.get("content_text") or "",
    ]
    return " ".join(parts).lower()


def _risk_keyword_hits(text: str) -> dict[str, int]:
    """Detecta palabras de riesgo; tokens cortos con límites tipo palabra."""
    found: dict[str, int] = {}
    for kw, pts in RISK_KEYWORD_POINTS.items():
        if " " in kw or "-" in kw:
            if kw in text:
                found[kw] = pts
        elif re.search(rf"(?<![a-z0-9]){re.escape(kw)}(?![a-z0-9])", text):
            found[kw] = pts
    return found


def score_article(article: dict[str, Any]) -> dict[str, Any]:
    """
    Calcula priority_score, signal_score, risk_score y scoring_breakdown.
    Preserva el resto de campos del artículo.
    """
    raw_tags = article.get("topic_tags") or []
    tags = set(raw_tags) if isinstance(raw_tags, (list, tuple, set)) else set()
    narratives = article.get("narrative_candidates") or []
    if not isinstance(narratives, list):
        narratives = []
    text = _article_text(article)

    # ----- priority -----
    priority_detail: dict[str, Any] = {}
    p_raw = 0.0

    tag_count_pts = min(len(tags) * PRIORITY_TAG_COUNT_PTS_PER_TAG, PRIORITY_TAG_COUNT_CAP)
    p_raw += tag_count_pts
    priority_detail["tag_count"] = {"tags_seen": len(tags), "points": tag_count_pts}

    high_value_hits: dict[str, int] = {}
    for tag_name, pts in PRIORITY_HIGH_VALUE_TAGS.items():
        if tag_name in tags:
            high_value_hits[tag_name] = pts
            p_raw += pts
    priority_detail["high_value_tags"] = high_value_hits

    narr_pts = min(len(narratives) * PRIORITY_NARRATIVE_PTS_EACH, PRIORITY_NARRATIVE_CAP)
    p_raw += narr_pts
    priority_detail["narratives"] = {
        "count": len(narratives),
        "points": narr_pts,
        "labels_used": list(narratives),
    }

    priority_detail["raw_before_clamp"] = round(p_raw, 2)
    priority_score = clamp_score(p_raw)
    priority_detail["final"] = priority_score

    # ----- signal -----
    signal_detail: dict[str, Any] = {}
    s_raw = 0.0
    combo_hits: dict[str, int] = {}

    for needed, (label, pts) in SIGNAL_COMBOS.items():
        if needed <= tags:
            combo_hits[label] = pts
            s_raw += pts

    signal_detail["combinations"] = combo_hits
    signal_detail["raw_before_clamp"] = round(s_raw, 2)
    signal_score = clamp_score(s_raw)
    signal_detail["final"] = signal_score

    # ----- risk -----
    risk_detail: dict[str, Any] = {}
    r_raw = 0.0

    tag_risk: dict[str, int] = {}
    for tag_name, pts in RISK_TAG_POINTS.items():
        if tag_name in tags:
            tag_risk[tag_name] = pts
            r_raw += pts
    risk_detail["tags"] = tag_risk

    kw_hits = _risk_keyword_hits(text)
    r_raw += sum(kw_hits.values())
    risk_detail["risk_keywords"] = kw_hits

    risk_detail["raw_before_clamp"] = round(r_raw, 2)
    risk_score = clamp_score(r_raw)
    risk_detail["final"] = risk_score

    scoring_breakdown: dict[str, Any] = {
        "priority": priority_detail,
        "signal": signal_detail,
        "risk": risk_detail,
    }

    out = dict(article)
    out["priority_score"] = priority_score
    out["signal_score"] = signal_score
    out["risk_score"] = risk_score
    out["scoring_breakdown"] = scoring_breakdown
    out["score_bucket"] = score_bucket(priority_score)
    return out


def find_latest_classified_json(classified_dir: Path) -> Path:
    """Último rss_classified_*.json por mtime."""
    candidates = sorted(
        classified_dir.glob("rss_classified_*.json"),
        key=lambda p: p.stat().st_mtime,
        reverse=True,
    )
    if not candidates:
        raise FileNotFoundError(f"No hay rss_classified_*.json en {classified_dir}")
    return candidates[0]


def save_scored_json(articles: list[dict[str, Any]], scored_dir: Path) -> Path:
    scored_dir.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now(UTC).strftime("%Y%m%d_%H%M%S")
    out_path = scored_dir / f"rss_scored_{stamp}.json"
    payload = {
        "saved_at": datetime.now(UTC).isoformat(),
        "article_count": len(articles),
        "articles": articles,
    }
    out_path.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )
    return out_path


def main() -> None:
    try:
        classified_path = find_latest_classified_json(DATA_CLASSIFIED_DIR)
    except FileNotFoundError as exc:
        print(f"[ERROR] {exc}", file=sys.stderr)
        raise SystemExit(1) from exc

    try:
        payload = json.loads(classified_path.read_text(encoding="utf-8"))
    except (OSError, UnicodeError, json.JSONDecodeError) as exc:
        print(f"[ERROR] No se pudo leer {classified_path}: {exc}", file=sys.stderr)
        raise SystemExit(1) from exc

    articles_in = payload.get("articles")
    if not isinstance(articles_in, list):
        print("[ERROR] El JSON no contiene lista 'articles'.", file=sys.stderr)
        raise SystemExit(1)

    scored_articles: list[dict[str, Any]] = []
    for item in articles_in:
        if isinstance(item, dict):
            scored_articles.append(score_article(item))

    print(f"Archivo leído: {classified_path}")
    print(f"Artículos puntuados: {len(scored_articles)}")

    try:
        out_path = save_scored_json(scored_articles, DATA_SCORED_DIR)
    except OSError as exc:
        print(f"[ERROR] No se pudo guardar: {exc}", file=sys.stderr)
        raise SystemExit(1) from exc

    print(f"Archivo generado: {out_path}")


if __name__ == "__main__":
    main()
