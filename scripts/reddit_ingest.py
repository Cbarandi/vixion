#!/usr/bin/env python3
"""Ingesta de posts recientes desde Reddit (JSON público, sin OAuth)."""

from __future__ import annotations

import json
import sys
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import requests

SUBREDDITS: tuple[str, ...] = (
    "cryptocurrency",
    "wallstreetbets",
    "finance",
    "investing",
)

LIMIT = 50
REDDIT_TEMPLATE = "https://www.reddit.com/r/{subreddit}/new.json?limit={limit}"

HEADERS = {
    "User-Agent": (
        "VIXION/0.1 (educational narrative research; "
        "+https://example.invalid/contact)"
    ),
    "Accept": "application/json",
}

PROJECT_ROOT = Path(__file__).resolve().parent.parent
DATA_RAW_DIR = PROJECT_ROOT / "data" / "raw"
REQUEST_TIMEOUT = 30


def _post_from_child(child: dict[str, Any], fetched_at: str) -> dict[str, Any] | None:
    if child.get("kind") != "t3":
        return None
    d = child.get("data")
    if not isinstance(d, dict):
        return None
    post_id = d.get("id")
    if not post_id:
        return None
    created = d.get("created_utc")
    if isinstance(created, (int, float)):
        created_iso = datetime.fromtimestamp(float(created), tz=UTC).isoformat()
    else:
        created_iso = ""
    return {
        "post_id": str(post_id),
        "subreddit": str(d.get("subreddit") or "").strip(),
        "title": str(d.get("title") or "").strip(),
        "selftext": str(d.get("selftext") or "").strip(),
        "url": str(d.get("url") or "").strip(),
        "score": int(d.get("score") or 0),
        "num_comments": int(d.get("num_comments") or 0),
        "created_utc": created_iso,
        "fetched_at": fetched_at,
    }


def fetch_subreddit_posts(subreddit: str) -> list[dict[str, Any]]:
    """Descarga /new.json para un subreddit; lista vacía si falla."""
    url = REDDIT_TEMPLATE.format(subreddit=subreddit, limit=LIMIT)
    fetched_at = datetime.now(UTC).isoformat()
    posts: list[dict[str, Any]] = []

    try:
        resp = requests.get(url, headers=HEADERS, timeout=REQUEST_TIMEOUT)
        resp.raise_for_status()
        payload = resp.json()
    except requests.RequestException as exc:
        print(f"[ERROR] {subreddit}: petición HTTP fallida — {exc}", file=sys.stderr)
        return posts
    except ValueError as exc:
        print(f"[ERROR] {subreddit}: JSON inválido — {exc}", file=sys.stderr)
        return posts

    data = payload.get("data") if isinstance(payload, dict) else None
    children = data.get("children") if isinstance(data, dict) else None
    if not isinstance(children, list):
        print(f"[WARN] {subreddit}: estructura inesperada (sin children).", file=sys.stderr)
        return posts

    for child in children:
        if not isinstance(child, dict):
            continue
        row = _post_from_child(child, fetched_at)
        if row:
            posts.append(row)
    return posts


def save_reddit_json(posts: list[dict[str, Any]]) -> Path:
    DATA_RAW_DIR.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now(UTC).strftime("%Y%m%d_%H%M%S")
    out_path = DATA_RAW_DIR / f"reddit_{stamp}.json"
    payload = {
        "saved_at": datetime.now(UTC).isoformat(),
        "post_count": len(posts),
        "posts": posts,
    }
    out_path.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )
    return out_path


def main() -> None:
    all_posts: list[dict[str, Any]] = []

    for name in SUBREDDITS:
        print(f"Obteniendo r/{name}…")
        batch = fetch_subreddit_posts(name)
        print(f"  → {len(batch)} posts")
        all_posts.extend(batch)

    print()
    print("=" * 40)
    print(f"Total posts: {len(all_posts)}")
    print("=" * 40)

    try:
        out_path = save_reddit_json(all_posts)
    except OSError as exc:
        print(f"[ERROR] No se pudo guardar JSON: {exc}", file=sys.stderr)
        raise SystemExit(1) from exc

    print()
    print(f"JSON guardado (UTF-8): {out_path}")


if __name__ == "__main__":
    main()
