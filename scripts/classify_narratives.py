#!/usr/bin/env python3
"""Clasificación narrativa v0 por reglas (keywords) sobre JSON normalizado."""

from __future__ import annotations

import argparse
import json
import re
import sys
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

PROJECT_ROOT = Path(__file__).resolve().parent.parent
DATA_PROCESSED_DIR = PROJECT_ROOT / "data" / "processed"
DATA_CLASSIFIED_DIR = PROJECT_ROOT / "data" / "classified"

# source_type -> (glob de entrada en processed/, prefijo del fichero en classified/)
SOURCE_LAYOUT: dict[str, tuple[str, str]] = {
    "rss": ("rss_normalized_*.json", "rss_classified"),
    "reddit": ("reddit_normalized_*.json", "reddit_classified"),
}

# Categoría -> palabras o frases (minúsculas para comparar con texto ya en lower)
KEYWORD_RULES: dict[str, tuple[str, ...]] = {
    "macro": (
        "inflation",
        "cpi",
        "fed",
        "ecb",
        "rates",
        "recession",
        "growth",
        "monetary",
        "gdp",
        "treasury",
    ),
    "markets": (
        "market",
        "rally",
        "selloff",
        "sell-off",
        "volatility",
        "bonds",
        "yields",
        "trading",
        "futures",
    ),
    "ai": (
        "ai",
        "artificial intelligence",
        "openai",
        "machine learning",
        "llm",
        "chatgpt",
        "nvidia",
        "chip",
        "semiconductor",
        "model",
        "superintelligence",
    ),
    "energy": (
        "oil",
        "gas",
        "opec",
        "crude",
        "energy",
        "petroleum",
        "refinery",
        "strait of hormuz",
    ),
    "crypto": (
        "bitcoin",
        "btc",
        "ethereum",
        "crypto",
        "token",
        "blockchain",
        "defi",
    ),
    "geopolitics": (
        "war",
        "sanctions",
        "iran",
        "china",
        "russia",
        "military",
        "blockade",
        "nato",
        "middle east",
        "ukraine",
    ),
    "banking": (
        "goldman",
        "jpmorgan",
        "j.p. morgan",
        "morgan stanley",
        "citigroup",
        "lender",
        "lenders",
        "bank",
        "banks",
        "banking",
        "credit",
    ),
    "regulation": (
        "regulator",
        "regulation",
        "regulatory",
        "antitrust",
        "sec",
        "fine",
        "penalty",
        "lawsuit",
        "compliance",
        "law",
    ),
    "equities": (
        "stocks",
        "stock",
        "shares",
        "share price",
        "equity",
        "equities",
        "s&p",
        "nasdaq",
        "earnings",
    ),
}


def _hay_ai_como_palabra(text: str) -> bool:
    """Detecta 'ai' como palabra suelta (evita ruido en otras palabras)."""
    return bool(re.search(r"(?<![a-z0-9])ai(?![a-z0-9])", text))


def _matches_keyword(text: str, kw: str) -> bool:
    kw_lower = kw.lower().strip()
    if not kw_lower:
        return False
    if kw_lower == "ai":
        return _hay_ai_como_palabra(text)
    if " " in kw_lower:
        return kw_lower in text
    return bool(re.search(rf"(?<![a-z0-9]){re.escape(kw_lower)}(?![a-z0-9])", text))


def _article_search_text(article: dict[str, Any]) -> str:
    parts = [
        article.get("title") or "",
        article.get("summary") or "",
        article.get("content_text") or "",
    ]
    return " ".join(parts).lower()


def find_latest_processed_json(source_type: str) -> Path:
    """Último JSON normalizado para la fuente indicada (por mtime) en data/processed/."""
    if source_type not in SOURCE_LAYOUT:
        raise ValueError(f"source_type no soportado: {source_type!r}. Use: rss, reddit")
    glob_pat, _ = SOURCE_LAYOUT[source_type]
    candidates = sorted(
        DATA_PROCESSED_DIR.glob(glob_pat),
        key=lambda p: p.stat().st_mtime,
        reverse=True,
    )
    if not candidates:
        raise FileNotFoundError(
            f"No hay {glob_pat} en {DATA_PROCESSED_DIR} (source_type={source_type})",
        )
    return candidates[0]


def classify_article(article: dict[str, Any]) -> list[str]:
    """Devuelve topic_tags únicos según keywords (orden alfabético estable)."""
    text = _article_search_text(article)
    found: set[str] = set()

    for category, keywords in KEYWORD_RULES.items():
        for kw in keywords:
            if _matches_keyword(text, kw):
                found.add(category)
                break

    return sorted(found)


def infer_narrative_candidates(topic_tags: list[str]) -> list[str]:
    """Propone frases candidatas según las etiquetas detectadas (sin duplicados)."""
    tagset = set(topic_tags)
    out: list[str] = []
    seen: set[str] = set()

    def add(phrase: str) -> None:
        if phrase not in seen:
            seen.add(phrase)
            out.append(phrase)

    if "macro" in tagset:
        add("central bank policy pressure")
    if "energy" in tagset:
        add("energy supply shock risk")
    if "ai" in tagset:
        add("AI platform competition")
    if "banking" in tagset:
        add("bank earnings strength")
    if "geopolitics" in tagset:
        add("geopolitical escalation risk")
    if "equities" in tagset or "markets" in tagset:
        add("equity market volatility")
    if "crypto" in tagset:
        add("digital asset flow narrative")
    if "regulation" in tagset:
        add("policy and regulatory overhang")

    return out


def save_classified_json(
    articles: list[dict[str, Any]],
    classified_dir: Path,
    source_type: str,
) -> Path:
    """Escribe {rss|reddit}_classified_YYYYMMDD_HHMMSS.json en UTF-8."""
    _, out_prefix = SOURCE_LAYOUT[source_type]
    classified_dir.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now(UTC).strftime("%Y%m%d_%H%M%S")
    out_path = classified_dir / f"{out_prefix}_{stamp}.json"
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


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Clasificación narrativa v0 (RSS o Reddit normalizado).",
    )
    p.add_argument(
        "--source",
        choices=("rss", "reddit"),
        default="rss",
        help="Fuente del JSON en data/processed/ (default: rss)",
    )
    return p.parse_args(argv)


def main(argv: list[str] | None = None) -> None:
    args = parse_args(argv)
    source_type: str = args.source

    print(f"Fuente: {source_type}")

    try:
        processed_path = find_latest_processed_json(source_type)
    except (FileNotFoundError, ValueError) as exc:
        print(f"[ERROR] {exc}", file=sys.stderr)
        raise SystemExit(1) from exc

    try:
        payload = json.loads(processed_path.read_text(encoding="utf-8"))
    except (OSError, UnicodeError, json.JSONDecodeError) as exc:
        print(f"[ERROR] No se pudo leer {processed_path}: {exc}", file=sys.stderr)
        raise SystemExit(1) from exc

    articles_in = payload.get("articles")
    if not isinstance(articles_in, list):
        print("[ERROR] El JSON no contiene lista 'articles'.", file=sys.stderr)
        raise SystemExit(1)

    classified_articles: list[dict[str, Any]] = []
    for item in articles_in:
        if not isinstance(item, dict):
            continue
        topic_tags = classify_article(item)
        narrative_candidates = infer_narrative_candidates(topic_tags)
        row = dict(item)
        row["topic_tags"] = topic_tags
        row["narrative_candidates"] = narrative_candidates
        classified_articles.append(row)

    print(f"Archivo leído: {processed_path}")
    print(f"Artículos clasificados: {len(classified_articles)}")

    try:
        out_path = save_classified_json(classified_articles, DATA_CLASSIFIED_DIR, source_type)
    except OSError as exc:
        print(f"[ERROR] No se pudo guardar: {exc}", file=sys.stderr)
        raise SystemExit(1) from exc

    print(f"Archivo generado: {out_path}")


if __name__ == "__main__":
    main()
