"""Shared test fixtures.

Two kinds of test live here:

* **Unit tests** - validation, extraction, pure watermark arithmetic. No
  database, no network, no clock dependency (``today`` is pinned).
* **Integration tests** - the real SQL running against a real PostgreSQL.
  These are the ones that prove the upsert and the quality checks behave, so
  they are not mocked. If no database is reachable they skip with a message
  saying how to start one, rather than failing or silently passing.
"""

from __future__ import annotations

import os
from datetime import date
from pathlib import Path

import pytest

from etl.config import Config
from etl.db import DatabaseUnavailable, connect, ensure_schema

FIXTURE_DIR = Path(__file__).parent / "fixtures"

# Pinned so future-date validation never depends on the real clock.
TODAY = date(2026, 10, 1)

SKIP_REASON = (
    "No PostgreSQL reachable. Start one with `docker compose up -d postgres`, "
    "or set POSTGRES_HOST/PORT/DB/USER/PASSWORD to an existing instance. "
    "Integration tests are skipped, not failed, so unit tests still run."
)


@pytest.fixture(scope="session")
def pg_config() -> Config:
    """Config pointing at a test database, or skip the test."""
    if not os.environ.get("POSTGRES_PASSWORD"):
        pytest.skip(SKIP_REASON)
    # Default to a dedicated test database so a stray run cannot touch real data.
    os.environ.setdefault("POSTGRES_DB", "congress_etl_test")
    config = Config.from_env()
    try:
        connect(config).close()
    except DatabaseUnavailable:
        pytest.skip(SKIP_REASON)
    if "test" not in config.database:
        pytest.skip(
            f"Refusing to run integration tests against {config.database!r}: the database name must contain 'test'."
        )
    ensure_schema(config)
    return config


@pytest.fixture
def clean_db(pg_config: Config) -> Config:
    """An empty warehouse before each integration test."""
    connection = connect(pg_config)
    try:
        with connection, connection.cursor() as cursor:
            cursor.execute(
                """
                    TRUNCATE TABLE
                          staging.member_term_stage
                        , analytics.member_term
                        , analytics.congress_age_summary
                        , etl.watermark
                        , etl.rejected_row
                        , etl.batch_run
                    RESTART IDENTITY
                    """
            )
    finally:
        connection.close()
    return pg_config


@pytest.fixture
def etl_env(clean_db: Config, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Config:
    """Clean database plus a temporary data directory seeded with the fixture CSV."""
    import shutil

    data_dir = tmp_path / "data"
    raw = data_dir / "raw"
    raw.mkdir(parents=True)
    shutil.copy(FIXTURE_DIR / "congress_clean.csv", raw / "aging_congress.csv")
    monkeypatch.setenv("ETL_DATA_DIR", str(data_dir))
    return Config.from_env()
