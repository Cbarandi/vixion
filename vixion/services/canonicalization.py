"""Canonicalización de URL y huella de contenido."""

from __future__ import annotations

import hashlib
from urllib.parse import parse_qsl, urlencode, urlparse, urlunparse

_TRACKING_PARAMS = frozenset(
    {
        "utm_source",
        "utm_medium",
        "utm_campaign",
        "utm_term",
        "utm_content",
        "gclid",
        "fbclid",
    }
)


def normalize_url(url: str | None) -> str | None:
    if not url or not url.strip():
        return None
    u = url.strip()
    parsed = urlparse(u)
    scheme = (parsed.scheme or "https").lower()
    netloc = (parsed.netloc or "").lower()
    path = parsed.path or "/"
    q = [(k, v) for k, v in parse_qsl(parsed.query, keep_blank_values=True) if k.lower() not in _TRACKING_PARAMS]
    query = urlencode(q)
    return urlunparse((scheme, netloc, path.rstrip("/") or "/", query, "", ""))


def body_for_hash(title: str, body: str, max_chars: int = 8000) -> str:
    raw = f"{title.strip()}\n{body.strip()}"
    return raw[:max_chars]


def content_hash(title: str, body: str) -> str:
    """SHA-256 hex (64 chars) — cumple ck_items_content_hash_min_len."""
    payload = body_for_hash(title, body).encode("utf-8")
    return hashlib.sha256(payload).hexdigest()


def occurrence_fingerprint(source_id: int, native_id: str | None, canonical_url: str | None, content_hash: str) -> str:
    """Huella estable por aparición (dedupe de filas occurrence)."""
    parts = f"{source_id}|{native_id or ''}|{canonical_url or ''}|{content_hash}"
    return hashlib.sha256(parts.encode("utf-8")).hexdigest()


def title_for_display(title: str) -> str:
    t = title.strip()
    return t if t else "(untitled)"
