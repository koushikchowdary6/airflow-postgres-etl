"""Pipeline steps.

Each step is a plain function taking a ``Config`` and returning a JSON-safe
dict. The Airflow DAG is a thin wrapper around these, which means:

  * the pipeline logic is testable without Airflow installed;
  * the same code runs from the CLI for local development and debugging;
  * each step opens its own connection and transaction, which is required
    anyway because Airflow runs tasks in separate processes.

Hand-off between steps is a file plus a batch id, not in-memory state, for the
same reason.
"""

from __future__ import annotations

from datetime import date
from pathlib import Path
from typing import Any

from .config import PIPELINE_NAME, Config
from .db import ensure_schema, transaction
from .extract import extract_file, new_batch_id
from .load import (
    build_analytics,
    copy_staged_csv,
    fact_fingerprint,
    fact_row_count,
    finish_batch,
    insert_rejects,
    start_batch,
    truncate_staging,
    upsert_fact,
    write_staged_csv,
)
from .logging_setup import get_logger
from .quality import QualityCheck, QualityCheckFailed, enforce, format_table, run_checks
from .source import AGING_CONGRESS, fetch
from .watermark import advance_watermark, effective_floor, get_watermark

logger = get_logger(__name__)


def step_ensure_schema(config: Config) -> dict[str, Any]:
    ensure_schema(config)
    return {"schema": "ready"}


def step_extract(
    config: Config,
    *,
    batch_id: str | None = None,
    offline: bool = False,
    full_refresh: bool = False,
    dag_run_id: str | None = None,
    today: date | None = None,
) -> dict[str, Any]:
    """Fetch, validate and write a staged CSV. Returns an XCom-safe summary."""
    config.ensure_dirs()
    batch_id = batch_id or new_batch_id()
    spec = AGING_CONGRESS
    raw_path = config.raw_dir / spec.filename

    if not raw_path.exists():
        if offline:
            raise FileNotFoundError(
                f"{raw_path} is missing and --offline was given. Download it first "
                f"(python -m etl.cli fetch), or place the CSV at that path. See docs/DATASETS.md."
            )
        fetch(spec, raw_path)

    with transaction(config) as cursor:
        start_batch(cursor, batch_id, PIPELINE_NAME, dag_run_id)
        watermark = None if full_refresh else get_watermark(cursor, PIPELINE_NAME)
    floor = None if full_refresh else effective_floor(watermark, config.lookback_congresses)

    logger.info(
        "extracting",
        extra={
            "batch_id": batch_id,
            "watermark": watermark,
            "min_congress": floor,
            "full_refresh": full_refresh,
        },
    )
    result = extract_file(spec, raw_path, batch_id=batch_id, min_congress=floor, today=today)

    staged_path = config.staged_dir / f"{batch_id}.csv"
    write_staged_csv(result.records, staged_path)

    with transaction(config) as cursor:
        insert_rejects(cursor, batch_id, result.rejects)
        finish_batch(
            cursor,
            batch_id,
            "running",
            counts={
                "rows_read": result.rows_read,
                "rows_rejected": len(result.rejects),
                "max_congress": result.max_congress,
            },
        )

    return {
        "batch_id": batch_id,
        "staged_path": str(staged_path),
        "rows_read": result.rows_read,
        "records": len(result.records),
        "rejects": len(result.rejects),
        "duplicates_dropped": result.duplicates_dropped,
        "rows_skipped_by_watermark": result.rows_skipped_by_watermark,
        "max_congress": result.max_congress,
    }


def step_load_staging(config: Config, *, batch_id: str, staged_path: str) -> dict[str, Any]:
    """Truncate staging and COPY the staged CSV into it."""
    with transaction(config) as cursor:
        truncate_staging(cursor)
        loaded = copy_staged_csv(cursor, Path(staged_path))
    return {"batch_id": batch_id, "rows_staged": loaded}


def step_upsert(config: Config, *, batch_id: str) -> dict[str, Any]:
    """Apply the idempotent upsert from staging to the fact table."""
    with transaction(config) as cursor:
        before = fact_row_count(cursor)
        stats = upsert_fact(cursor)
        after = fact_row_count(cursor)
        finish_batch(
            cursor,
            batch_id,
            "running",
            counts={
                "rows_staged": stats["rows_staged"],
                "rows_inserted": stats["rows_inserted"],
                "rows_updated": stats["rows_updated"],
            },
        )
    return {"batch_id": batch_id, "fact_rows_before": before, "fact_rows_after": after, **stats}


def step_build_analytics(config: Config) -> dict[str, Any]:
    with transaction(config) as cursor:
        rows = build_analytics(cursor)
    return {"summary_rows": rows}


def step_quality(config: Config, *, batch_id: str) -> dict[str, Any]:
    """Run the quality suite. Marks the batch failed and raises on failure."""
    with transaction(config) as cursor:
        checks = run_checks(cursor)
    print("\n=== Data-quality checks ===")
    print(format_table(checks))
    try:
        enforce(checks, fail_on_error=config.fail_on_quality_error)
    except QualityCheckFailed as exc:
        with transaction(config) as cursor:
            finish_batch(cursor, batch_id, "failed", error_message=str(exc))
        raise
    return {
        "batch_id": batch_id,
        "checks": [c.as_dict() for c in checks],
        "all_passed": all(c.passed for c in checks),
    }


def step_advance_watermark(config: Config, *, batch_id: str, max_congress: int | None) -> dict[str, Any]:
    """Advance the watermark and close out the batch.

    Runs only after the quality checks pass, so a failed run is retried from
    the same position instead of silently skipping data.
    """
    moved = False
    with transaction(config) as cursor:
        if max_congress is not None:
            moved = advance_watermark(cursor, PIPELINE_NAME, int(max_congress))
        watermark = get_watermark(cursor, PIPELINE_NAME)
        finish_batch(cursor, batch_id, "succeeded")
    return {"batch_id": batch_id, "watermark_advanced": moved, "watermark": watermark}


def run_all(
    config: Config,
    *,
    offline: bool = False,
    full_refresh: bool = False,
    dag_run_id: str | None = None,
) -> dict[str, Any]:
    """Run every step in order. Used by the CLI; the DAG runs them as tasks."""
    step_ensure_schema(config)
    extracted = step_extract(config, offline=offline, full_refresh=full_refresh, dag_run_id=dag_run_id)
    batch_id = extracted["batch_id"]
    try:
        staged = step_load_staging(config, batch_id=batch_id, staged_path=extracted["staged_path"])
        upserted = step_upsert(config, batch_id=batch_id)
        analytics = step_build_analytics(config)
        quality = step_quality(config, batch_id=batch_id)
        advanced = step_advance_watermark(config, batch_id=batch_id, max_congress=extracted["max_congress"])
    except Exception as exc:
        # Record the failure against the batch before propagating, so the audit
        # table reflects reality even when the run dies.
        with transaction(config) as cursor:
            finish_batch(cursor, batch_id, "failed", error_message=str(exc))
        raise

    with transaction(config) as cursor:
        fingerprint = fact_fingerprint(cursor)

    summary = {
        **extracted,
        **staged,
        **upserted,
        **analytics,
        "quality_passed": quality["all_passed"],
        **advanced,
        "fact_fingerprint": fingerprint,
    }
    logger.info("run complete", extra=summary)
    return summary


def latest_checks(config: Config) -> list[QualityCheck]:
    with transaction(config) as cursor:
        return run_checks(cursor)
