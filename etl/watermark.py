"""Incremental watermark, persisted in PostgreSQL.

The watermark is the highest ``congress`` that has been loaded *and* passed the
quality checks. It lives in the database rather than a file so concurrent or
distributed workers see one value, and so it commits in the same transaction
as the data it describes.

``congress`` is used instead of a timestamp because it is the dataset's own
monotonic sequence: readable, integer, and free of timezone and clock-skew
problems.
"""

from __future__ import annotations

import psycopg2.extensions

from .logging_setup import get_logger

logger = get_logger(__name__)


def get_watermark(cursor: psycopg2.extensions.cursor, pipeline_name: str) -> int | None:
    cursor.execute(
        "SELECT last_congress FROM etl.watermark WHERE pipeline_name = %s",
        (pipeline_name,),
    )
    row = cursor.fetchone()
    return int(row[0]) if row else None


def set_watermark(cursor: psycopg2.extensions.cursor, pipeline_name: str, last_congress: int) -> None:
    cursor.execute(
        """
        INSERT INTO etl.watermark (pipeline_name, last_congress)
        VALUES (%s, %s)
        ON CONFLICT (pipeline_name) DO UPDATE SET
              last_congress = EXCLUDED.last_congress
            , updated_at = now()
        """,
        (pipeline_name, last_congress),
    )
    logger.info("watermark set", extra={"pipeline_name": pipeline_name, "last_congress": last_congress})


def advance_watermark(cursor: psycopg2.extensions.cursor, pipeline_name: str, candidate: int) -> bool:
    """Move the watermark forward only. Returns True if it moved.

    Monotonic by design: a re-run over older data must not rewind the position
    and cause the next run to re-process everything.
    """
    current = get_watermark(cursor, pipeline_name)
    if current is not None and candidate <= current:
        logger.info(
            "watermark not advanced",
            extra={"pipeline_name": pipeline_name, "current": current, "candidate": candidate},
        )
        return False
    set_watermark(cursor, pipeline_name, candidate)
    return True


def effective_floor(watermark: int | None, lookback_congresses: int) -> int | None:
    """Lowest ``congress`` to extract, given the watermark and lookback.

    Returns None for a first run, meaning "process everything". The lookback
    re-reads recently loaded Congresses so a late upstream correction is picked
    up; that is only safe because the upsert is keyed on the natural key.
    """
    if watermark is None:
        return None
    if lookback_congresses < 0:
        raise ValueError("lookback_congresses must be >= 0")
    return max(1, watermark - lookback_congresses)
