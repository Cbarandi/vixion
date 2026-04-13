#!/usr/bin/env python3
"""Normaliza artículos RSS desde el JSON raw más reciente en data/raw/."""

from __future__ import annotations

import json
import sys
from datetime import UTC, datetime
from pathlib import Path
from typing import Any


PROJECT_ROOT = Path(__file__).resolve().parent.parent
DATA_RAW_DIR = PROJECT_ROOT / "data" / "raw"
DATA_PROCESSED_DIR = PROJECT_ROOT / "data" / "processed"


def find_latest_raw_json(raw_dir: Path) -> Path:
    """Devuelve el rss_*.json más reciente en raw_dir (por fecha de modificación)."""
    candidates = sorted(raw_dir.glob("rss_*.json"), key=lambda p: p.stat().st_mtime, reverse=True)
    if not candidates:
        raise FileNotFoundError(f"No hay archivos rss_*.json en {raw_dir}")
    return candidates[0]


def normalize_article(raw: dict[str, Any]) -> dict[str, Any]:
    """Mapea un artículo del ingest RSS al esquema normalizado v1."""
    title = (raw.get("title") or "").strip()
    summary = (raw.get("summary") or "").strip()
    author = (raw.get("author") or "").strip()
    link = (raw.get("link") or "").strip()
    content_text = f"{title} {summary}".strip()

    return {
        "article_id": (raw.get("article_id") or "").strip(),
        "source": (raw.get("source") or "").strip(),
        "source_type": "rss",
        "title": title,
        "summary": summary,
        "author": author,
        "url": link,
        "published_at": (raw.get("published") or "").strip(),
        "fetched_at": (raw.get("fetched_at") or "").strip(),
        "content_text": content_text,
        "language": "en",
        "asset_tags": [],
        "topic_tags": [],
        "narrative_candidates": [],
        "entities": [],
        "ingest_type": "rss",
    }


def save_normalized_json(articles: list[dict[str, Any]], processed_dir: Path) -> Path:
    """Crea processed_dir si hace falta y escribe rss_normalized_YYYYMMDD_HHMMSS.json en UTF-8."""
    processed_dir.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now(UTC).strftime("%Y%m%d_%H%M%S")
    out_path = processed_dir / f"rss_normalized_{stamp}.json"
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
        raw_path = find_latest_raw_json(DATA_RAW_DIR)
    except FileNotFoundError as exc:
        print(f"[ERROR] {exc}", file=sys.stderr)
        raise SystemExit(1) from exc

    try:
        raw_text = raw_path.read_text(encoding="utf-8")
        payload = json.loads(raw_text)
    except (OSError, UnicodeError, json.JSONDecodeError) as exc:
        print(f"[ERROR] No se pudo leer o parsear {raw_path}: {exc}", file=sys.stderr)
        raise SystemExit(1) from exc

    articles_in = payload.get("articles")
    if not isinstance(articles_in, list):
        print("[ERROR] El JSON raw no contiene una lista en 'articles'.", file=sys.stderr)
        raise SystemExit(1)

    normalized: list[dict[str, Any]] = []
    for item in articles_in:
        if isinstance(item, dict):
            normalized.append(normalize_article(item))

    print(f"Archivo raw leído: {raw_path}")
    print(f"Artículos procesados: {len(normalized)}")

    try:
        out_path = save_normalized_json(normalized, DATA_PROCESSED_DIR)
    except OSError as exc:
        print(f"[ERROR] No se pudo guardar el JSON: {exc}", file=sys.stderr)
        raise SystemExit(1) from exc

    print(f"Archivo generado: {out_path}")


if __name__ == "__main__":
    main()
