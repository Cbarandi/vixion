"""Dependencias FastAPI (conexión Postgres)."""

from __future__ import annotations

from collections.abc import Generator
from typing import Annotated

import psycopg
from fastapi import Depends

from vixion.db.conn import connect


def get_db_connection() -> Generator[psycopg.Connection, None, None]:
    with connect() as conn:
        conn.autocommit = True
        yield conn


DbConn = Annotated[psycopg.Connection, Depends(get_db_connection)]
