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
REQUIRED_QUALITY_CHECKS = frozenset(
    {
        "age_years_within_range",
        "all_staged_rows_loaded",
        "birthday_precedes_start_date",
        "chamber_values_valid",
        "fact_table_not_empty",
        "natural_key_is_unique",
        "no_future_start_dates",
        "staged_values_match_fact",
        "summary_reconciles_with_fact",
    }
)


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


def _validated_quality_check(row: tuple[object, ...]) -> QualityCheck:
    """Do not coerce malformed SQL evidence into a plausible PASS result.

    COUNT(*) yields a Python int with psycopg2. In particular, int(True),
    int(0.5), and int("0") would silently change the meaning of the result.
    """
    if len(row) != 4:
        raise QualityCheckFailed("Invalid SQL quality result: expected four columns")
    name, status, observed, detail = row
    if not isinstance(name, str) or not name.strip() or name != name.strip():
        raise QualityCheckFailed("Invalid SQL quality result: malformed check name")
    if not isinstance(status, str) or status not in {"PASS", "FAIL"}:
        raise QualityCheckFailed(f"Invalid SQL quality result for {name}: status must be PASS or FAIL")
    if isinstance(observed, bool) or not isinstance(observed, int) or observed < 0:
        raise QualityCheckFailed(f"Invalid SQL quality result for {name}: observed must be a nonnegative integer")
    if not isinstance(detail, str) or not detail.strip():
        raise QualityCheckFailed(f"Invalid SQL quality result for {name}: missing detail")
    return QualityCheck(name, status, observed, detail)


def run_checks(cursor: psycopg2.extensions.cursor) -> list[QualityCheck]:
    cursor.execute(read_sql("004_quality.sql"))
    checks = [_validated_quality_check(tuple(row)) for row in cursor.fetchall()]
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
    # Callers can supply QualityCheck objects directly, without run_checks().
    # Validate those too: malformed evidence must never advance the watermark,
    # even when fail_on_error=False.
    for check in checks:
        if not isinstance(check, QualityCheck):
            raise QualityCheckFailed("Invalid SQL quality result: expected QualityCheck")
        _validated_quality_check((check.check_name, check.status, check.observed, check.detail))
    names = [check.check_name for check in checks]
    seen: set[str] = set()
    duplicates: set[str] = set()
    for name in names:
        if name in seen:
            duplicates.add(name)
        seen.add(name)
    missing = REQUIRED_QUALITY_CHECKS - seen
    unexpected = seen - REQUIRED_QUALITY_CHECKS
    if missing or duplicates or unexpected:
        problems = []
        if missing:
            problems.append(f"missing checks: {', '.join(sorted(missing))}")
        if duplicates:
            problems.append(f"duplicate checks: {', '.join(sorted(duplicates))}")
        if unexpected:
            problems.append(f"unexpected checks: {', '.join(sorted(unexpected))}")
        raise QualityCheckFailed(
            "Data-quality suite integrity failure (" + "; ".join(problems) + "); watermark must not advance."
        )

    # SQL returns an observed count as well as a PASS/FAIL label. A changed
    # query that returns PASS alongside offending rows must not silently
    # advance the watermark, even in warning mode. The non-empty fact-table
    # check is the sole inverse predicate: it passes when count > 0.
    contradictory = []
    for check in checks:
        expected_pass = check.observed > 0 if check.check_name == "fact_table_not_empty" else check.observed == 0
        if check.observed < 0 or check.passed != expected_pass:
            contradictory.append(check.check_name)
    if contradictory:
        raise QualityCheckFailed(
            "Data-quality suite integrity failure (status/count mismatch: "
            + ", ".join(sorted(contradictory))
            + "); watermark must not advance."
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
