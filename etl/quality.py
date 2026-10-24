"""Data-quality checks.

The assertions live in sql/004_quality.sql so the same predicates can be run
by hand in psql against a suspect warehouse. This module executes them,
reports and decides whether the run fails.
"""

from __future__ import annotations

from dataclasses import dataclass

import psycopg2.extensions

from .db import read_sql
from .logging_setup import get_logger

logger = get_logger(__name__)

# This is the required contract of sql/004_quality.sql. A partially deployed
# or accidentally edited suite must never look green simply because the
# remaining checks happen to pass (in particular, all([]) is True).
REQUIRED_QUALITY_CHECKS = frozenset({
    "age_years_within_range",
    "all_staged_rows_loaded",
    "birthday_precedes_start_date",
    "chamber_values_valid",
    "fact_table_not_empty",
    "natural_key_is_unique",
    "no_future_start_dates",
    "staged_values_match_fact",
    "summary_reconciles_with_fact",
})


class QualityCheckFailed(RuntimeError):
    """Raised when one or more data-quality checks fail."""


@dataclass(frozen=True)
class QualityCheck:
    check_name: str
    status: str
    observed: int
    detail: str

    @property
    def passed(self) -> bool:
        return self.status.upper() == "PASS"

    def as_dict(self) -> dict[str, object]:
        return {
            "check_name": self.check_name,
            "status": self.status,
            "observed": self.observed,
            "detail": self.detail,
        }


def run_checks(cursor: psycopg2.extensions.cursor) -> list[QualityCheck]:
    cursor.execute(read_sql("004_quality.sql"))
    checks = [
        QualityCheck(check_name=str(r[0]), status=str(r[1]), observed=int(r[2]), detail=str(r[3]))
        for r in cursor.fetchall()
    ]
    for check in checks:
        log = logger.info if check.passed else logger.error
        log("quality check", extra=check.as_dict())
    return checks


def failures(checks: list[QualityCheck]) -> list[QualityCheck]:
    return [c for c in checks if not c.passed]


def enforce(checks: list[QualityCheck], *, fail_on_error: bool = True) -> None:
    """Require a complete, unique suite before interpreting individual results.

    Missing or duplicate checks are infrastructure integrity failures, never
    optional data-quality warnings. This prevents an empty or partial SQL
    result from silently advancing the incremental watermark.
    """
    names = [check.check_name for check in checks]
    seen: set[str] = set()
    duplicates: set[str] = set()
    for name in names:
        if name in seen:
            duplicates.add(name)
        seen.add(name)
    missing = REQUIRED_QUALITY_CHECKS - seen
    if missing or duplicates:
        problems = []
        if missing:
            problems.append(f"missing checks: {', '.join(sorted(missing))}")
        if duplicates:
            problems.append(f"duplicate checks: {', '.join(sorted(duplicates))}")
        raise QualityCheckFailed(
            "Data-quality suite integrity failure (" + "; ".join(problems) + "); "
            "watermark must not advance."
        )

    bad = failures(checks)
    if not bad:
        return
    detail = "; ".join(f"{c.check_name} (observed {c.observed}: {c.detail})" for c in bad)
    message = (
        f"{len(bad)} data-quality check(s) failed: {detail}. "
        "Re-run the matching predicate from sql/004_quality.sql as a SELECT to list the offending rows."
    )
    if fail_on_error:
        raise QualityCheckFailed(message)
    # NOTE: the key here must not collide with a LogRecord attribute.
    # logging.Logger.makeRecord raises KeyError if `extra` contains a reserved
    # name such as "message", "module" or "filename", which turns a log line
    # into a crash. Using "message" here was a real bug caught by the tests.
    logger.warning(
        "quality checks failed but are configured as warnings",
        extra={"failure_count": len(bad), "failed_checks": detail},
    )


def format_table(checks: list[QualityCheck]) -> str:
    if not checks:
        return "(no checks run)"
    width = max(len(c.check_name) for c in checks)
    lines = [f"{'CHECK'.ljust(width)}  STATUS  OBSERVED  DETAIL", "-" * (width + 40)]
    for c in checks:
        lines.append(f"{c.check_name.ljust(width)}  {c.status:<6}  {c.observed:>8}  {c.detail}")
    return "\n".join(lines)
