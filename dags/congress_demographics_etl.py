"""Congressional demographics ETL.

Daily batch load of FiveThirtyEight's "aging Congress" dataset into PostgreSQL:
extract and validate, COPY into staging, idempotent upsert into the fact table,
rebuild the aggregate model, verify data quality, then advance the watermark.

Design notes for anyone reading this DAG:

* **The DAG holds no business logic.** Every task calls a function in ``etl/``,
  which keeps the pipeline testable without Airflow and runnable from the CLI.
* **Nothing touches the database at parse time.** ``Config.from_env()`` is
  called inside each task, never at module level. A DAG file that connects to
  a database while being parsed will break the scheduler, which re-parses every
  file on a loop.
* **Each task is independently retryable.** Staging is truncated before every
  load, the upsert is keyed on the natural key, and the aggregate is rebuilt in
  full, so re-running any task after a failure is safe.
* **The watermark advances last**, only after the quality checks pass, so a bad
  run is retried from the same position rather than silently skipping data.
"""

from __future__ import annotations

import sys
from datetime import UTC, datetime, timedelta
from pathlib import Path

from airflow.decorators import dag, task

# Make the project importable when the repository root is not already on
# PYTHONPATH. Docker Compose sets PYTHONPATH=/opt/airflow/project, but a bare
# `airflow dags list` against this folder would otherwise fail to import `etl`.
PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

DEFAULT_ARGS = {
    "owner": "data-engineering",
    # Two retries with backoff covers the common transient causes: a brief
    # network failure fetching the source, or PostgreSQL still starting up.
    # It does not paper over a data error, which fails identically every time.
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=20),
    # A task that has not finished in 20 minutes is wedged, not slow: this
    # dataset is ~29k rows. Without a timeout a stuck task holds its slot
    # indefinitely.
    "execution_timeout": timedelta(minutes=20),
    # Each run loads the full current file, so a missed or failed previous run
    # does not invalidate today's.
    "depends_on_past": False,
}


@dag(
    dag_id="congress_demographics_etl",
    description="Load congressional demographics into PostgreSQL (incremental, idempotent)",
    schedule="0 6 * * *",
    start_date=datetime(2026, 1, 1, tzinfo=UTC),
    # The source is a single cumulative file with no per-date slices, so
    # backfilling historical dates would re-load identical data.
    catchup=False,
    # One run at a time: concurrent runs would contend on the same staging
    # table and watermark row.
    max_active_runs=1,
    dagrun_timeout=timedelta(hours=1),
    default_args=DEFAULT_ARGS,
    tags=["etl", "postgres", "congress", "incremental"],
    doc_md=__doc__,
)
def congress_demographics_etl() -> None:
    @task
    def ensure_schema() -> dict:
        """Create schemas, tables, constraints and indexes if absent."""
        from etl.config import Config
        from etl.pipeline import step_ensure_schema

        return step_ensure_schema(Config.from_env())

    @task
    def extract(_schema: dict) -> dict:
        """Download, validate and stage to a CSV. Returns the batch summary."""
        from airflow.operators.python import get_current_context

        from etl.config import Config
        from etl.pipeline import step_extract

        run_id = get_current_context()["run_id"]
        return step_extract(Config.from_env(), dag_run_id=run_id)

    @task
    def load_staging(extracted: dict) -> dict:
        """Truncate staging and COPY the staged CSV into PostgreSQL."""
        from etl.config import Config
        from etl.pipeline import step_load_staging

        return step_load_staging(
            Config.from_env(),
            batch_id=extracted["batch_id"],
            staged_path=extracted["staged_path"],
        )

    @task
    def upsert(staged: dict) -> dict:
        """Idempotent upsert from staging into analytics.member_term."""
        from etl.config import Config
        from etl.pipeline import step_upsert

        return step_upsert(Config.from_env(), batch_id=staged["batch_id"])

    @task
    def build_analytics(_upserted: dict) -> dict:
        """Rebuild analytics.congress_age_summary from the fact table."""
        from etl.config import Config
        from etl.pipeline import step_build_analytics

        return step_build_analytics(Config.from_env())

    @task
    def data_quality(extracted: dict, _analytics: dict) -> dict:
        """Run the SQL quality suite. Fails the task on any failed check."""
        from etl.config import Config
        from etl.pipeline import step_quality

        return step_quality(Config.from_env(), batch_id=extracted["batch_id"])

    @task
    def advance_watermark(extracted: dict, _quality: dict) -> dict:
        """Advance the watermark and mark the batch succeeded."""
        from etl.config import Config
        from etl.pipeline import step_advance_watermark

        return step_advance_watermark(
            Config.from_env(),
            batch_id=extracted["batch_id"],
            max_congress=extracted["max_congress"],
        )

    schema = ensure_schema()
    extracted = extract(schema)
    staged = load_staging(extracted)
    upserted = upsert(staged)
    analytics = build_analytics(upserted)
    quality = data_quality(extracted, analytics)
    advance_watermark(extracted, quality)


congress_demographics_etl()
