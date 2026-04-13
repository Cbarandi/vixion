"""Database package — URL helpers for Alembic / app."""

from __future__ import annotations

import os


def database_url() -> str:
    """SQLAlchemy URL for PostgreSQL (psycopg3)."""
    url = os.environ.get("DATABASE_URL")
    if not url:
        raise RuntimeError(
            "DATABASE_URL is not set. Example: "
            "postgresql+psycopg://user:pass@localhost:5432/vixion"
        )
    return url
