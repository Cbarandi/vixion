#!/usr/bin/env python3
"""Normaliza posts Reddit desde el JSON raw más reciente en data/raw/."""

from __future__ import annotations

import json
import sys
from datetime import UTC, datetime
from pathlib import Path
from typing import Any


PROJECT_ROOT = Path(__file__).resolve().parent.parent
DATA_RAW_DIR = PROJECT_ROOT / "data" / "raw"
DATA_PROCESSED_DIR = PROJECT_ROOT / "data" / "processed"


def find_latest_reddit_raw_json(raw_dir: Path) -> Path:
    """Devuelve el reddit_*.json más reciente en raw_dir (por fecha de modificación)."""
    candidates = sorted(raw_dir.glob("reddit_*.json"), key=lambda p: p.stat().st_mtime, reverse=True)
    if not candidates:
        raise FileNotFoundError(f"No hay archivos reddit_*.json en {raw_dir}")
    return candidates[0]


def normalize_post(raw: dict[str, Any]) -> dict[str, Any]:
    """Mapea un post del ingest Reddit al esquema normalizado v1 (mismo que RSS)."""
    title = (raw.get("title") or "").strip()
    summary = (raw.get("selftext") or "").strip()
    content_text = f"{title} {summary}".strip()

    return {
        "article_id": (raw.get("post_id") or "").strip(),
        "source": "reddit",
        "source_type": "reddit",
        "title": title,
        "summary": summary,
        "author": "",
        "url": (raw.get("url") or "").strip(),
        "published_at": (raw.get("created_utc") or "").strip(),
        "fetched_at": (raw.get("fetched_at") or "").strip(),
        "content_text": content_text,
        "language": "en",
        "asset_tags": [],
        "topic_tags": [],
        "narrative_candidates": [],
        "entities": [],
        "ingest_type": "reddit",
    }


def save_reddit_normalized_json(articles: list[dict[str, Any]], processed_dir: Path) -> Path:
    """Crea processed_dir si hace falta y escribe reddit_normalized_YYYYMMDD_HHMMSS.json en UTF-8."""
    processed_dir.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now(UTC).strftime("%Y%m%d_%H%M%S")
    out_path = processed_dir / f"reddit_normalized_{stamp}.json"
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
        raw_path = find_latest_reddit_raw_json(DATA_RAW_DIR)
    except FileNotFoundError as exc:
        print(f"[ERROR] {exc}", file=sys.stderr)
        raise SystemExit(1) from exc

    try:
        raw_text = raw_path.read_text(encoding="utf-8")
        payload = json.loads(raw_text)
    except (OSError, UnicodeError, json.JSONDecodeError) as exc:
        print(f"[ERROR] No se pudo leer o parsear {raw_path}: {exc}", file=sys.stderr)
        raise SystemExit(1) from exc

    posts_in = payload.get("posts")
    if not isinstance(posts_in, list):
        print("[ERROR] El JSON raw no contiene una lista en 'posts'.", file=sys.stderr)
        raise SystemExit(1)

    normalized: list[dict[str, Any]] = []
    for item in posts_in:
        if isinstance(item, dict):
            normalized.append(normalize_post(item))

    print(f"Archivo raw leído: {raw_path}")
    print(f"Posts procesados: {len(normalized)}")

    try:
        out_path = save_reddit_normalized_json(normalized, DATA_PROCESSED_DIR)
    except OSError as exc:
        print(f"[ERROR] No se pudo guardar el JSON: {exc}", file=sys.stderr)
        raise SystemExit(1) from exc

    print(f"Archivo generado: {out_path}")


if __name__ == "__main__":
    main()
