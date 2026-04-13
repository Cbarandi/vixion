#!/usr/bin/env python3
"""Descarga artículos desde feeds RSS de Financial Times y Bloomberg Markets."""

from __future__ import annotations

import hashlib
import json
import sys
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import feedparser
from bs4 import BeautifulSoup

RSS_FEEDS: tuple[tuple[str, str], ...] = (
    ("Financial Times", "https://www.ft.com/rss/home"),
    ("Bloomberg Markets", "https://feeds.bloomberg.com/markets/news.rss"),
)

DEFAULT_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (compatible; VIXION-rss-ingest/1.0; "
        "+https://example.invalid)"
    ),
}

DATA_RAW_DIR = Path(__file__).resolve().parent.parent / "data" / "raw"


def _entry_published_iso(entry: Any) -> str:
    """Fecha de publicación en ISO 8601 con UTC, o cadena vacía si no hay dato fiable."""
    for key in ("published_parsed", "updated_parsed"):
        st = entry.get(key) if isinstance(entry, dict) else getattr(entry, key, None)
        if st:
            try:
                return datetime(*st[:6], tzinfo=UTC).isoformat()
            except (TypeError, ValueError):
                continue
    raw = entry.get("published", "") if isinstance(entry, dict) else getattr(entry, "published", "")
    return (raw or "").strip()


def _clean_html(raw: str) -> str:
    """Extrae texto plano del HTML del resumen."""
    if not raw or not raw.strip():
        return ""
    text = BeautifulSoup(raw, "html.parser").get_text(separator=" ", strip=True)
    return " ".join(text.split())


def _entry_summary_raw(entry: Any) -> str:
    for key in ("summary", "subtitle", "description"):
        val = entry.get(key) if isinstance(entry, dict) else getattr(entry, key, None)
        if not val:
            continue
        if isinstance(val, dict) and val.get("value"):
            return str(val["value"])
        return str(val)
    return ""


def _entry_author(entry: Any) -> str:
    author = entry.get("author") if isinstance(entry, dict) else getattr(entry, "author", None)
    if author:
        return str(author).strip()
    authors = entry.get("authors") if isinstance(entry, dict) else getattr(entry, "authors", None)
    if not authors:
        return ""
    names: list[str] = []
    for a in authors:
        if isinstance(a, dict):
            n = a.get("name")
            if n:
                names.append(str(n).strip())
        elif getattr(a, "name", None):
            names.append(str(a.name).strip())
    return ", ".join(names)


def _article_id(source: str, link: str, title: str) -> str:
    payload = f"{source}\n{link}\n{title}".encode("utf-8")
    return hashlib.sha256(payload).hexdigest()


def _dedupe_by_link(articles: list[dict[str, str]]) -> tuple[list[dict[str, str]], int]:
    """Elimina duplicados en la misma ejecución (mismo link; si no hay link, usa article_id)."""
    seen: set[str] = set()
    out: list[dict[str, str]] = []
    for a in articles:
        link = (a.get("link") or "").strip()
        key = link if link else (a.get("article_id") or "")
        if key in seen:
            continue
        seen.add(key)
        out.append(a)
    return out, len(articles) - len(out)


def fetch_feed(url: str, source: str) -> list[dict[str, str]]:
    """Descarga un feed RSS y devuelve artículos normalizados."""
    articles: list[dict[str, str]] = []
    fetched_at = datetime.now(UTC).isoformat()

    try:
        parsed = feedparser.parse(url, request_headers=DEFAULT_HEADERS)
    except Exception as exc:
        print(f"[ERROR] No se pudo leer el feed ({source}): {exc}", file=sys.stderr)
        return articles

    if getattr(parsed, "bozo", False) and getattr(parsed, "bozo_exception", None):
        print(f"[WARN] Feed con advertencias ({source}): {parsed.bozo_exception}")

    status = getattr(parsed, "status", None)
    if status is not None and status >= 400:
        print(f"[ERROR] HTTP {status} al obtener {source} ({url})", file=sys.stderr)
        return articles

    for entry in parsed.get("entries", []):
        title = (entry.get("title") or "").strip()
        link = (entry.get("link") or "").strip()
        summary_raw = _entry_summary_raw(entry)
        summary = _clean_html(summary_raw) if summary_raw else ""
        author = _entry_author(entry)

        row: dict[str, str] = {
            "article_id": _article_id(source, link, title),
            "source": source,
            "title": title,
            "link": link,
            "published": _entry_published_iso(entry),
            "fetched_at": fetched_at,
        }
        if summary:
            row["summary"] = summary
        if author:
            row["author"] = author
        articles.append(row)

    return articles


def save_articles_json(articles: list[dict[str, str]]) -> Path:
    """Crea data/raw/ si hace falta y guarda todos los artículos en UTF-8."""
    DATA_RAW_DIR.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now(UTC).strftime("%Y%m%d_%H%M%S")
    out_path = DATA_RAW_DIR / f"rss_{stamp}.json"
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
    all_articles: list[dict[str, str]] = []

    for source, url in RSS_FEEDS:
        print(f"Obteniendo {source}...")
        batch = fetch_feed(url, source)
        print(f"  → {len(batch)} artículos")
        all_articles.extend(batch)

    before = len(all_articles)
    all_articles, dup_removed = _dedupe_by_link(all_articles)
    if dup_removed:
        print(f"\n[INFO] Duplicados omitidos (mismo enlace): {dup_removed} (de {before} a {len(all_articles)})")

    print()
    print("=" * 40)
    print(f"Total de artículos: {len(all_articles)}")
    print("=" * 40)
    print()
    print("Ejemplos de títulos:")
    for article in all_articles[:12]:
        print(f"  - [{article['source']}] {article['title']}")

    try:
        out_path = save_articles_json(all_articles)
    except OSError as exc:
        print(f"[ERROR] No se pudo guardar el JSON: {exc}", file=sys.stderr)
        raise SystemExit(1) from exc

    print()
    print(f"JSON guardado (UTF-8): {out_path}")


if __name__ == "__main__":
    main()
