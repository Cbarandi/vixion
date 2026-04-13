#!/usr/bin/env python3
"""Scoring v1 sobre dataset merged: misma lógica que score_articles + peso origin_weight en señal."""

from __future__ import annotations

import importlib.util
import json
import sys
from datetime import UTC, datetime
from pathlib import Path
from typing import Any


PROJECT_ROOT = Path(__file__).resolve().parent.parent
DATA_MERGED_DIR = PROJECT_ROOT / "data" / "merged"
DATA_SCORED_DIR = PROJECT_ROOT / "data" / "scored"


def _load_score_articles_module() -> Any:
    path = PROJECT_ROOT / "scripts" / "score_articles.py"
    spec = importlib.util.spec_from_file_location("score_articles", path)
    if spec is None or spec.loader is None:
        raise ImportError(f"No se pudo cargar {path}")
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


_sa = _load_score_articles_module()
score_article = _sa.score_article
clamp_score = _sa.clamp_score


def find_latest_merged_json(merged_dir: Path) -> Path:
    candidates = sorted(
        merged_dir.glob("merged_*.json"),
        key=lambda p: p.stat().st_mtime,
        reverse=True,
    )
    if not candidates:
        raise FileNotFoundError(f"No hay merged_*.json en {merged_dir}")
    return candidates[0]


def apply_origin_weight_to_signal(row: dict[str, Any]) -> dict[str, Any]:
    """
    Tras score_article: signal_score *= origin_weight (clamp 0–100).
    Actualiza scoring_breakdown['signal'] con trazabilidad.
    """
    w = row.get("origin_weight", 1.0)
    try:
        weight = float(w)
    except (TypeError, ValueError):
        weight = 1.0

    before = int(row.get("signal_score", 0))
    after = clamp_score(before * weight)
    row["signal_score"] = after

    bd = row.get("scoring_breakdown")
    if isinstance(bd, dict):
        sig = bd.get("signal")
        if isinstance(sig, dict):
            sig = dict(sig)
            sig["final_before_origin_weight"] = before
            sig["origin_weight"] = weight
            sig["final"] = after
            bd = dict(bd)
            bd["signal"] = sig
            row["scoring_breakdown"] = bd

    return row


def save_merged_scored_json(articles: list[dict[str, Any]], scored_dir: Path) -> Path:
    scored_dir.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now(UTC).strftime("%Y%m%d_%H%M%S")
    out_path = scored_dir / f"merged_scored_{stamp}.json"
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
        merged_path = find_latest_merged_json(DATA_MERGED_DIR)
    except FileNotFoundError as exc:
        print(f"[ERROR] {exc}", file=sys.stderr)
        raise SystemExit(1) from exc

    try:
        payload = json.loads(merged_path.read_text(encoding="utf-8"))
    except (OSError, UnicodeError, json.JSONDecodeError) as exc:
        print(f"[ERROR] No se pudo leer {merged_path}: {exc}", file=sys.stderr)
        raise SystemExit(1) from exc

    articles_in = payload.get("articles")
    if not isinstance(articles_in, list):
        print("[ERROR] El merged no contiene lista 'articles'.", file=sys.stderr)
        raise SystemExit(1)

    scored: list[dict[str, Any]] = []
    for item in articles_in:
        if not isinstance(item, dict):
            continue
        row = score_article(item)
        scored.append(apply_origin_weight_to_signal(row))

    print(f"Archivo leído: {merged_path}")
    print(f"Artículos puntuados: {len(scored)}")

    try:
        out_path = save_merged_scored_json(scored, DATA_SCORED_DIR)
    except OSError as exc:
        print(f"[ERROR] No se pudo guardar: {exc}", file=sys.stderr)
        raise SystemExit(1) from exc

    print(f"Archivo generado: {out_path}")


if __name__ == "__main__":
    main()
