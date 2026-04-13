#!/usr/bin/env python3
"""Unifica artículos clasificados RSS + Reddit en un solo dataset."""

from __future__ import annotations

import json
import sys
from datetime import UTC, datetime
from pathlib import Path
from typing import Any


PROJECT_ROOT = Path(__file__).resolve().parent.parent
DATA_CLASSIFIED_DIR = PROJECT_ROOT / "data" / "classified"
DATA_MERGED_DIR = PROJECT_ROOT / "data" / "merged"


def find_latest_classified(glob_pat: str, label: str) -> Path:
    candidates = sorted(
        DATA_CLASSIFIED_DIR.glob(glob_pat),
        key=lambda p: p.stat().st_mtime,
        reverse=True,
    )
    if not candidates:
        raise FileNotFoundError(f"No hay {glob_pat} en {DATA_CLASSIFIED_DIR} ({label})")
    return candidates[0]


def _detect_source_type(article: dict[str, Any]) -> str:
    for key in ("source_type", "ingest_type"):
        v = article.get(key)
        if isinstance(v, str) and v.strip():
            return v.strip().lower()
    src = (article.get("source") or "").strip().lower()
    if src == "reddit":
        return "reddit"
    return "rss"


def enrich_merged_article(article: dict[str, Any]) -> dict[str, Any]:
    """Copia el artículo y añade origin_weight e is_reddit."""
    row = dict(article)
    st = _detect_source_type(row)

    if st == "reddit":
        row["source_type"] = "reddit"
        row["origin_weight"] = 0.7
        row["is_reddit"] = True
    else:
        row["source_type"] = "rss"
        row["origin_weight"] = 1.0
        row["is_reddit"] = False

    return row


def load_articles(path: Path) -> list[dict[str, Any]]:
    text = path.read_text(encoding="utf-8")
    payload = json.loads(text)
    items = payload.get("articles")
    if not isinstance(items, list):
        raise ValueError(f"{path} no contiene lista 'articles'.")
    out: list[dict[str, Any]] = []
    for item in items:
        if isinstance(item, dict):
            out.append(item)
    return out


def save_merged_json(articles: list[dict[str, Any]], merged_dir: Path) -> Path:
    merged_dir.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now(UTC).strftime("%Y%m%d_%H%M%S")
    out_path = merged_dir / f"merged_{stamp}.json"
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
        rss_path = find_latest_classified("rss_classified_*.json", "RSS")
        reddit_path = find_latest_classified("reddit_classified_*.json", "Reddit")
    except FileNotFoundError as exc:
        print(f"[ERROR] {exc}", file=sys.stderr)
        raise SystemExit(1) from exc

    try:
        rss_articles = load_articles(rss_path)
        reddit_articles = load_articles(reddit_path)
    except (OSError, UnicodeError, json.JSONDecodeError, ValueError) as exc:
        print(f"[ERROR] No se pudo leer o validar JSON: {exc}", file=sys.stderr)
        raise SystemExit(1) from exc

    merged: list[dict[str, Any]] = []
    for a in rss_articles:
        merged.append(enrich_merged_article(a))
    for a in reddit_articles:
        merged.append(enrich_merged_article(a))

    print(f"Archivo RSS leído:    {rss_path} ({len(rss_articles)} artículos)")
    print(f"Archivo Reddit leído: {reddit_path} ({len(reddit_articles)} artículos)")
    print()
    print(f"Artículos RSS:    {len(rss_articles)}")
    print(f"Artículos Reddit: {len(reddit_articles)}")
    print(f"Total combinado:  {len(merged)}")

    try:
        out_path = save_merged_json(merged, DATA_MERGED_DIR)
    except OSError as exc:
        print(f"[ERROR] No se pudo guardar: {exc}", file=sys.stderr)
        raise SystemExit(1) from exc

    print()
    print(f"Archivo generado: {out_path}")


if __name__ == "__main__":
    main()
