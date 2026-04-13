"""Carga YAML de feeds RSS (config mantenible)."""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

import yaml


@dataclass(frozen=True, slots=True)
class FeedSpec:
    slug: str
    name: str
    url: str


def default_feeds_path() -> Path:
    env = os.environ.get("VIXION_FEEDS_CONFIG")
    if env:
        return Path(env).expanduser()
    return Path("config/feeds.yaml")


def load_feed_specs(path: Path | None = None) -> list[FeedSpec]:
    p = path or default_feeds_path()
    if not p.is_file():
        ex = Path(__file__).resolve().parents[2] / "config" / "feeds.example.yaml"
        if ex.is_file():
            p = ex
        else:
            raise FileNotFoundError(
                f"No feeds config at {p}. Set VIXION_FEEDS_CONFIG or copy config/feeds.example.yaml to config/feeds.yaml."
            )
    raw = yaml.safe_load(p.read_text(encoding="utf-8")) or {}
    feeds = raw.get("feeds") or []
    out: list[FeedSpec] = []
    for row in feeds:
        if not isinstance(row, dict):
            continue
        out.append(
            FeedSpec(
                slug=str(row["slug"]).strip(),
                name=str(row["name"]).strip(),
                url=str(row["url"]).strip(),
            )
        )
    if not out:
        raise ValueError(f"feeds vacío en {p}")
    return out
