#!/usr/bin/env python3
"""Detecta narrativas dominantes y tipos (confirmed / early / institutional) desde merged o merged_scored."""

from __future__ import annotations

import json
import statistics
import sys
from datetime import UTC, datetime
from pathlib import Path
from typing import Any


PROJECT_ROOT = Path(__file__).resolve().parent.parent
DATA_MERGED_DIR = PROJECT_ROOT / "data" / "merged"
DATA_SCORED_DIR = PROJECT_ROOT / "data" / "scored"
DATA_NARRATIVES_DIR = PROJECT_ROOT / "data" / "narratives"


def find_latest_input_for_narratives() -> tuple[Path, str]:
    """
    Prefiere el último merged_scored_*.json (tras score_merged) para usar señales reales;
    si no hay, usa merged_*.json (solo peso/volumen en narrative_strength).
    """
    scored = sorted(
        DATA_SCORED_DIR.glob("merged_scored_*.json"),
        key=lambda p: p.stat().st_mtime,
        reverse=True,
    )
    if scored:
        return scored[0], "merged_scored"
    merged = sorted(
        DATA_MERGED_DIR.glob("merged_*.json"),
        key=lambda p: p.stat().st_mtime,
        reverse=True,
    )
    if merged:
        return merged[0], "merged"
    raise FileNotFoundError(
        f"No hay merged_scored_*.json en {DATA_SCORED_DIR} "
        f"ni merged_*.json en {DATA_MERGED_DIR}. "
        "Ejecuta merge_sources.py (y opcionalmente score_merged.py).",
    )


def _article_key(article: dict[str, Any]) -> str:
    aid = article.get("article_id")
    if isinstance(aid, str) and aid.strip():
        return aid.strip()
    return str(id(article))


def _mean_scores(articles: list[dict[str, Any]], key: str) -> float:
    vals: list[float] = []
    for a in articles:
        v = a.get(key)
        if isinstance(v, (int, float)) and not isinstance(v, bool):
            vals.append(float(v))
        else:
            try:
                if v is not None:
                    vals.append(float(v))
            except (TypeError, ValueError):
                pass
    return statistics.mean(vals) if vals else 0.0


def _narrative_type(rss_count: int, reddit_count: int) -> str:
    if rss_count > 0 and reddit_count > 0:
        return "confirmed"
    if reddit_count > 0 and rss_count == 0:
        return "early"
    if rss_count > 0 and reddit_count == 0:
        return "institutional"
    return "unknown"


def build_narrative_buckets(articles: list[dict[str, Any]]) -> dict[str, dict[str, dict[str, Any]]]:
    """narrative_label -> { article_key -> article }"""
    buckets: dict[str, dict[str, dict[str, Any]]] = {}
    for article in articles:
        if not isinstance(article, dict):
            continue
        narrs = article.get("narrative_candidates")
        if not isinstance(narrs, list):
            continue
        ak = _article_key(article)
        seen_labels: set[str] = set()
        for raw in narrs:
            if not isinstance(raw, str):
                continue
            label = raw.strip()
            if not label or label in seen_labels:
                continue
            seen_labels.add(label)
            buckets.setdefault(label, {})[ak] = article
    return buckets


def compute_narrative_row(label: str, articles_map: dict[str, dict[str, Any]]) -> dict[str, Any]:
    articles = list(articles_map.values())
    total_articles = len(articles)

    rss_count = sum(1 for a in articles if not a.get("is_reddit", False))
    reddit_count = sum(1 for a in articles if a.get("is_reddit", False))

    total_weight = sum(float(a.get("origin_weight", 1.0)) for a in articles)

    avg_priority = _mean_scores(articles, "priority_score")
    avg_signal = _mean_scores(articles, "signal_score")
    avg_risk = _mean_scores(articles, "risk_score")

    ntype = _narrative_type(rss_count, reddit_count)
    narrative_strength = total_weight + avg_signal * 0.5

    return {
        "narrative": label,
        "type": ntype,
        "total_articles": total_articles,
        "rss_count": rss_count,
        "reddit_count": reddit_count,
        "total_weight": round(total_weight, 4),
        "avg_priority_score": round(avg_priority, 4),
        "avg_signal_score": round(avg_signal, 4),
        "avg_risk_score": round(avg_risk, 4),
        "narrative_strength": round(narrative_strength, 4),
    }


def save_narratives_json(rows: list[dict[str, Any]], out_dir: Path) -> Path:
    out_dir.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now(UTC).strftime("%Y%m%d_%H%M%S")
    out_path = out_dir / f"narratives_{stamp}.json"
    payload = {
        "saved_at": datetime.now(UTC).isoformat(),
        "narrative_count": len(rows),
        "narratives": rows,
    }
    out_path.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )
    return out_path


def main() -> None:
    try:
        input_path, input_kind = find_latest_input_for_narratives()
    except FileNotFoundError as exc:
        print(f"[ERROR] {exc}", file=sys.stderr)
        raise SystemExit(1) from exc

    try:
        payload = json.loads(input_path.read_text(encoding="utf-8"))
    except (OSError, UnicodeError, json.JSONDecodeError) as exc:
        print(f"[ERROR] No se pudo leer {input_path}: {exc}", file=sys.stderr)
        raise SystemExit(1) from exc

    articles_in = payload.get("articles")
    if not isinstance(articles_in, list):
        print("[ERROR] El merged no contiene lista 'articles'.", file=sys.stderr)
        raise SystemExit(1)

    buckets = build_narrative_buckets(articles_in)
    rows: list[dict[str, Any]] = []
    for label, amap in buckets.items():
        rows.append(compute_narrative_row(label, amap))

    rows.sort(key=lambda r: r["narrative_strength"], reverse=True)

    print(f"Dataset leído ({input_kind}): {input_path}")
    print(f"Narrativas detectadas: {len(rows)}")
    print()
    print("Top 5 por narrative_strength:")
    for i, r in enumerate(rows[:5], start=1):
        print(
            f"  {i}. [{r['type']}] {r['narrative'][:70]}"
            f"{'…' if len(r['narrative']) > 70 else ''} "
            f"(strength={r['narrative_strength']}, n={r['total_articles']}, "
            f"rss={r['rss_count']}, reddit={r['reddit_count']})",
        )

    try:
        out_path = save_narratives_json(rows, DATA_NARRATIVES_DIR)
    except OSError as exc:
        print(f"[ERROR] No se pudo guardar: {exc}", file=sys.stderr)
        raise SystemExit(1) from exc

    print()
    print(f"Archivo generado: {out_path}")


if __name__ == "__main__":
    main()
