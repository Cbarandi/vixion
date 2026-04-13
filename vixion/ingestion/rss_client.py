"""Fetch HTTP + parse RSS (feedparser)."""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from typing import Any

import feedparser
import httpx

log = logging.getLogger(__name__)

USER_AGENT = "VIXION/0.1 (+https://xolid.ai; RSS ingest)"
DEFAULT_TIMEOUT = httpx.Timeout(25.0, connect=10.0)


def fetch_feed_bytes(url: str, *, timeout: httpx.Timeout | None = None) -> bytes:
    to = timeout or DEFAULT_TIMEOUT
    with httpx.Client(timeout=to, follow_redirects=True, headers={"User-Agent": USER_AGENT}) as client:
        try:
            r = client.get(url)
            r.raise_for_status()
        except httpx.HTTPStatusError as exc:
            log.warning(
                "RSS fetch HTTP status=%s url=%s",
                exc.response.status_code,
                url[:220],
            )
            raise
        except httpx.RequestError as exc:
            log.warning("RSS fetch request_failed url=%s err=%s", url[:220], exc)
            raise
        return r.content


def _parse_pub_date(entry: Any) -> datetime | None:
    raw = getattr(entry, "published", None) or getattr(entry, "updated", None)
    if not raw:
        return None
    try:
        dt = parsedate_to_datetime(str(raw))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)
    except (TypeError, ValueError):
        return None


def parse_feed_entries(content: bytes) -> list[dict[str, Any]]:
    """
    Devuelve lista de dicts: title, link, summary, published_at (datetime|None), stable_id.
    """
    parsed = feedparser.parse(content)
    if getattr(parsed, "bozo", False) and not getattr(parsed, "entries", None):
        exc = getattr(parsed, "bozo_exception", None)
        raise ValueError(f"RSS parse error: {exc}")
    out: list[dict[str, Any]] = []
    for entry in parsed.entries or []:
        title = (getattr(entry, "title", None) or "").strip()
        link = (getattr(entry, "link", None) or "").strip()
        summary = (
            getattr(entry, "summary", None)
            or getattr(entry, "description", None)
            or ""
        )
        summary = str(summary).strip()
        guid = (getattr(entry, "id", None) or getattr(entry, "guid", None) or "").strip()
        stable_id = guid or link or f"title:{title}"
        out.append(
            {
                "title": title,
                "link": link,
                "summary": summary,
                "published_at": _parse_pub_date(entry),
                "stable_id": stable_id[:800],
            }
        )
    return out
