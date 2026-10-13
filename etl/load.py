"""Loading: staged CSV -> staging table -> fact table -> aggregate model."""

from __future__ import annotations

import csv
from collections.abc import Sequence
from pathlib import Path
from typing import Any

import psycopg2.extensions
import psycopg2.extras

from .db import read_sql
from .logging_setup import get_logger
from .schema import STAGE_COLUMNS, MemberTerm, RejectedRow

logger = get_logger(__name__)


def write_staged_csv(records: Sequence[MemberTerm], destination: Path) -> Path:
    """Write validated records to a CSV that COPY can read.

    The extract and load steps run as separate Airflow tasks in separate
    processes, so validated rows are handed over as a file rather than in
    memory. The file doubles as an artefact for debugging a failed load.
    """
    destination.parent.mkdir(parents=True, exist_ok=True)
    with destination.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.writer(handle)
        writer.writerow(STAGE_COLUMNS)
        for record in records:
            writer.writerow(record.as_row())
    logger.info("wrote staged csv", extra={"path": str(destination), "rows": len(records)})
    return destination


def truncate_staging(cursor: psycopg2.extensions.cursor) -> None:
    """Empty the staging table.

    Makes the load itself idempotent: re-running the same batch cannot
    accumulate rows in staging.
    """
    cursor.execute("TRUNCATE TABLE staging.member_term_stage")


def copy_staged_csv(cursor: psycopg2.extensions.cursor, source: Path) -> int:
    """Bulk-load the staged CSV with COPY. Returns rows loaded.

    COPY rather than many INSERTs: one round trip and one WAL-light bulk write
    instead of tens of thousands of statements.
    """
    if not source.exists():
        raise FileNotFoundError(
            f"Staged file {source} is missing. Re-run the extract step; the file is written to "
            f"ETL_DATA_DIR/staged/<batch_id>.csv."
        )
    columns = ", ".join(STAGE_COLUMNS)
    with source.open("r", encoding="utf-8") as handle:
        cursor.copy_expert(
            f"COPY staging.member_term_stage ({columns}) FROM STDIN WITH (FORMAT csv, HEADER true)",
            handle,
        )
    cursor.execute("SELECT COUNT(*) FROM staging.member_term_stage")
    loaded = int(cursor.fetchone()[0])
    logger.info("copied into staging", extra={"rows": loaded, "path": str(source)})
    return loaded


def upsert_fact(cursor: psycopg2.extensions.cursor) -> dict[str, int]:
    """Run the idempotent upsert. Returns inserted/updated/unchanged counts."""
    cursor.execute("SELECT COUNT(*) FROM staging.member_term_stage")
    staged = int(cursor.fetchone()[0])

    cursor.execute(read_sql("002_upsert.sql"))
    # RETURNING (xmax = 0): true for an inserted row, false for an updated one.
    outcomes = cursor.fetchall()
    inserted = sum(1 for row in outcomes if row[0])
    updated = len(outcomes) - inserted

    stats = {
        "rows_staged": staged,
        "rows_inserted": inserted,
        "rows_updated": updated,
        # Rows the DO UPDATE ... WHERE filtered out: present and identical, so
        # not written at all.
        "rows_unchanged": staged - len(outcomes),
    }
    logger.info("upsert complete", extra=stats)
    return stats


def build_analytics(cursor: psycopg2.extensions.cursor) -> int:
    """Rebuild the aggregate model. Returns row count."""
    cursor.execute(read_sql("003_analytics.sql"))
    cursor.execute("SELECT COUNT(*) FROM analytics.congress_age_summary")
    rows = int(cursor.fetchone()[0])
    logger.info("analytics model rebuilt", extra={"rows": rows})
    return rows


def insert_rejects(cursor: psycopg2.extensions.cursor, batch_id: str, rejects: Sequence[RejectedRow]) -> int:
    """Persist rejected input rows so they can be investigated later."""
    if not rejects:
        return 0
    psycopg2.extras.execute_values(
        cursor,
        """
        INSERT INTO etl.rejected_row (batch_id, source_row_number, column_name, raw_value, reason)
        VALUES %s
        """,
        [r.as_row(batch_id) for r in rejects],
        page_size=500,
    )
    logger.warning("recorded rejected rows", extra={"batch_id": batch_id, "rows": len(rejects)})
    return len(rejects)


# ---------------------------------------------------------------- batch audit


def start_batch(cursor: psycopg2.extensions.cursor, batch_id: str, pipeline_name: str, dag_run_id: str | None) -> None:
    cursor.execute(
        """
        INSERT INTO etl.batch_run (batch_id, pipeline_name, dag_run_id, status)
        VALUES (%s, %s, %s, 'running')
        ON CONFLICT (batch_id) DO UPDATE SET
              status = 'running'
            , dag_run_id = EXCLUDED.dag_run_id
            , started_at = now()
            , finished_at = NULL
            , error_message = NULL
        """,
        (batch_id, pipeline_name, dag_run_id),
    )


def finish_batch(
    cursor: psycopg2.extensions.cursor,
    batch_id: str,
    status: str,
    *,
    counts: dict[str, Any] | None = None,
    error_message: str | None = None,
) -> None:
    counts = counts or {}
    cursor.execute(
        """
        UPDATE etl.batch_run SET
              status        = %s
            , rows_read     = COALESCE(%s, rows_read)
            , rows_staged   = COALESCE(%s, rows_staged)
            , rows_rejected = COALESCE(%s, rows_rejected)
            , rows_inserted = COALESCE(%s, rows_inserted)
            , rows_updated  = COALESCE(%s, rows_updated)
            , max_congress  = COALESCE(%s, max_congress)
            , finished_at   = now()
            , error_message = %s
        WHERE batch_id = %s
        """,
        (
            status,
            counts.get("rows_read"),
            counts.get("rows_staged"),
            counts.get("rows_rejected"),
            counts.get("rows_inserted"),
            counts.get("rows_updated"),
            counts.get("max_congress"),
            error_message[:2000] if error_message else None,
            batch_id,
        ),
    )


# ------------------------------------------------------------------ reporting


def fact_row_count(cursor: psycopg2.extensions.cursor) -> int:
    cursor.execute("SELECT COUNT(*) FROM analytics.member_term")
    return int(cursor.fetchone()[0])


def fact_fingerprint(cursor: psycopg2.extensions.cursor) -> str:
    """Stable digest over business columns plus updated_at.

    Used to prove a rerun changed nothing at all, which is stricter than
    comparing row counts.
    """
    cursor.execute(
        """
        SELECT md5(string_agg(row_signature, '|' ORDER BY row_signature))
        FROM (
            SELECT congress || ':' || bioguide_id || ':' || age_years || ':' || updated_at AS row_signature
            FROM analytics.member_term
        ) AS signatures
        """
    )
    result = cursor.fetchone()[0]
    return result or "(empty)"
