"""Conexión PostgreSQL (psycopg3)."""

from __future__ import annotations

import os

import psycopg


def dsn_from_env() -> str:
    raw = os.environ.get("DATABASE_URL")
    if not raw:
        raise RuntimeError("DATABASE_URL no está definida.")
    if raw.startswith("postgresql+psycopg://"):
        return "postgresql://" + raw.removeprefix("postgresql+psycopg://")
    if raw.startswith("postgres://"):
        return "postgresql://" + raw.removeprefix("postgres://")
    return raw


def connect() -> psycopg.Connection:
    return psycopg.connect(dsn_from_env())
