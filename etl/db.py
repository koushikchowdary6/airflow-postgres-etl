"""PostgreSQL connection and transaction helpers.

Every write path runs inside an explicit transaction. The DDL, the upsert and
the analytics rebuild are each atomic, so a failure part-way through leaves the
warehouse in its previous state rather than half-updated. That is what makes
"restart the failed task" a safe recovery procedure.
"""

from __future__ import annotations

from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path
from typing import Any

import psycopg2
import psycopg2.extensions

from .config import SQL_DIR, Config
from .logging_setup import get_logger

logger = get_logger(__name__)


class DatabaseUnavailable(RuntimeError):
    """Raised when PostgreSQL cannot be reached, with how to fix it."""


def connect(config: Config) -> psycopg2.extensions.connection:
    """Open a connection, translating a refused connection into advice."""
    try:
        connection = psycopg2.connect(**config.connection_kwargs())
    except psycopg2.OperationalError as exc:
        raise DatabaseUnavailable(
            f"Could not connect to {config.safe_target()}: {exc.args[0].strip() if exc.args else exc}\n"
            "Check that PostgreSQL is running and reachable:\n"
            "  docker compose up -d postgres   (then: docker compose ps)\n"
            "  pg_isready -h <host> -p <port>\n"
            "and that POSTGRES_* variables are set (see .env.example)."
        ) from exc
    connection.autocommit = False
    return connection


@contextmanager
def transaction(config: Config) -> Iterator[psycopg2.extensions.cursor]:
    """Yield a cursor inside a transaction; commit on success, roll back on error."""
    connection = connect(config)
    try:
        with connection, connection.cursor() as cursor:
            yield cursor
    finally:
        connection.close()


def read_sql(filename: str) -> str:
    path = Path(SQL_DIR) / filename
    try:
        return path.read_text(encoding="utf-8")
    except FileNotFoundError:
        raise FileNotFoundError(
            f"SQL file not found: {path}. Run commands from the repository root so sql/ resolves."
        ) from None


def ensure_schema(config: Config) -> None:
    """Apply sql/001_schema.sql. Safe to run on every pipeline run."""
    with transaction(config) as cursor:
        cursor.execute(read_sql("001_schema.sql"))
    logger.info("schema ensured", extra={"target": config.safe_target()})


def fetch_all(config: Config, sql: str, params: tuple[Any, ...] | None = None) -> list[tuple[Any, ...]]:
    with transaction(config) as cursor:
        cursor.execute(sql, params)
        return cursor.fetchall()


def scalar(config: Config, sql: str, params: tuple[Any, ...] | None = None) -> Any:
    rows = fetch_all(config, sql, params)
    return rows[0][0] if rows else None
