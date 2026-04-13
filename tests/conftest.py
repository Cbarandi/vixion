"""Pytest configuration — integration DB and Alembic."""

from __future__ import annotations

import os
import subprocess
from pathlib import Path

import psycopg
import pytest

ROOT = Path(__file__).resolve().parent.parent


def _to_psycopg_dsn(database_url: str) -> str:
    """Alembic uses postgresql+psycopg://; libpq/psycopg want postgresql://."""
    if database_url.startswith("postgresql+psycopg://"):
        return "postgresql://" + database_url.removeprefix("postgresql+psycopg://")
    if database_url.startswith("postgres://"):
        return "postgresql://" + database_url.removeprefix("postgres://")
    return database_url


@pytest.fixture(scope="session")
def integration_dsn() -> str:
    """DSN for psycopg (postgresql://…). Skips entire session if DATABASE_URL unset."""
    raw = os.environ.get("DATABASE_URL")
    if not raw:
        pytest.skip(
            "DATABASE_URL no definida: los tests de integración requieren PostgreSQL con pgvector."
        )
    return _to_psycopg_dsn(raw)


@pytest.fixture(scope="session")
def _alembic_upgrade_head(integration_dsn: str) -> None:
    """Aplica migraciones una vez por sesión de tests."""
    subprocess.run(
        ["alembic", "upgrade", "head"],
        cwd=ROOT,
        env=os.environ.copy(),
        check=True,
    )


@pytest.fixture
def db_conn(integration_dsn: str, _alembic_upgrade_head: None):
    """
    Conexión con transacción por test; ROLLBACK al final para no ensuciar la base.

    Invariantes de aplicación (p. ej. item_count vs links) deben probarse aquí
    o en tests de servicio; la DB no valida coherencia cross-tabla.
    """
    with psycopg.connect(integration_dsn) as conn:
        conn.autocommit = False
        conn.execute("BEGIN")
        try:
            yield conn
        finally:
            conn.rollback()
