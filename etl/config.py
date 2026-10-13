"""Runtime configuration.

Read from environment variables so the same code runs under Airflow, under
Docker Compose, in CI and from a shell with no edits.

The password is read from the environment and never logged. ``Config.__repr__``
is overridden so a stray log line or traceback cannot print it.
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
SQL_DIR = REPO_ROOT / "sql"

STAGING_SCHEMA = "staging"
ANALYTICS_SCHEMA = "analytics"
CONTROL_SCHEMA = "etl"

PIPELINE_NAME = "congress_demographics"


class ConfigError(RuntimeError):
    """Raised when configuration is missing or invalid."""


def _env(name: str, default: str | None = None) -> str | None:
    value = os.environ.get(name, default)
    return value.strip() if value else None


def _env_int(name: str, default: int) -> int:
    raw = _env(name)
    if raw is None:
        return default
    try:
        return int(raw)
    except ValueError as exc:
        raise ConfigError(f"{name} must be an integer, got {raw!r}") from exc


def _env_bool(name: str, default: bool) -> bool:
    raw = _env(name)
    if raw is None:
        return default
    return raw.lower() in {"1", "true", "yes", "on"}


@dataclass(frozen=True)
class Config:
    """Resolved configuration for one pipeline run."""

    host: str = "localhost"
    port: int = 5432
    database: str = "congress_etl"
    user: str = "etl"
    password: str = field(default="", repr=False)

    data_dir: Path = field(default_factory=lambda: REPO_ROOT / "data")

    # Re-extract this many congresses below the watermark on each run, so a
    # late correction to an already-loaded congress is picked up. Safe because
    # the upsert is keyed on the natural key.
    lookback_congresses: int = 1

    fail_on_quality_error: bool = True
    statement_timeout_ms: int = 120_000

    @classmethod
    def from_env(cls) -> Config:
        password = _env("POSTGRES_PASSWORD")
        if not password:
            raise ConfigError(
                "POSTGRES_PASSWORD is not set. Copy .env.example to .env and fill it in, "
                "then: set -a && . ./.env && set +a"
            )
        return cls(
            host=_env("POSTGRES_HOST") or "localhost",
            port=_env_int("POSTGRES_PORT", 5432),
            database=_env("POSTGRES_DB") or "congress_etl",
            user=_env("POSTGRES_USER") or "etl",
            password=password,
            data_dir=Path(_env("ETL_DATA_DIR") or (REPO_ROOT / "data")),
            lookback_congresses=_env_int("ETL_LOOKBACK_CONGRESSES", 1),
            fail_on_quality_error=_env_bool("ETL_FAIL_ON_QUALITY_ERROR", True),
            statement_timeout_ms=_env_int("ETL_STATEMENT_TIMEOUT_MS", 120_000),
        )

    @property
    def raw_dir(self) -> Path:
        return self.data_dir / "raw"

    @property
    def staged_dir(self) -> Path:
        return self.data_dir / "staged"

    def ensure_dirs(self) -> None:
        for path in (self.raw_dir, self.staged_dir):
            path.mkdir(parents=True, exist_ok=True)

    def connection_kwargs(self) -> dict[str, object]:
        return {
            "host": self.host,
            "port": self.port,
            "dbname": self.database,
            "user": self.user,
            "password": self.password,
            # Fail a wedged statement rather than holding a lock forever.
            "options": f"-c statement_timeout={self.statement_timeout_ms}",
        }

    def safe_target(self) -> str:
        """Connection description with no credentials, for logs."""
        return f"postgresql://{self.user}@{self.host}:{self.port}/{self.database}"

    def __repr__(self) -> str:  # noqa: D105 - keep the password out of tracebacks
        return (
            f"Config(host={self.host!r}, port={self.port}, database={self.database!r}, "
            f"user={self.user!r}, password=***, lookback_congresses={self.lookback_congresses})"
        )
