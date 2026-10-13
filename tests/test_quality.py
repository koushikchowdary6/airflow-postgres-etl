"""Integration tests for the data-quality suite.

Each check is verified twice over: that it passes on good data, and that it
actually fires when the defect it describes is present. A check that can never
fail is worse than no check, because it buys false confidence.
"""

from __future__ import annotations

import logging
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path

import psycopg2.extensions
import pytest

from etl.config import Config
from etl.db import connect, transaction
from etl.load import build_analytics, upsert_fact
from etl.quality import QualityCheckFailed, enforce, failures, format_table, run_checks

from .test_load import stage

EXPECTED_CHECKS = {
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


@contextmanager
def rollback_cursor(config: Config) -> Iterator[psycopg2.extensions.cursor]:
    """A cursor whose work is always rolled back.

    Needed for the tests that temporarily drop a CHECK constraint to prove the
    corresponding SQL check can detect the defect. Nothing is committed, so the
    schema is intact afterwards.
    """
    connection = connect(config)
    try:
        cursor = connection.cursor()
        yield cursor
    finally:
        connection.rollback()
        connection.close()


@pytest.fixture
def loaded(clean_db: Config, tmp_path: Path) -> Config:
    """A warehouse loaded from the clean fixture, with the model built."""
    with transaction(clean_db) as cursor:
        stage(cursor, tmp_path, "congress_clean.csv")
        upsert_fact(cursor)
        build_analytics(cursor)
    return clean_db


class TestCleanData:
    def test_every_check_runs(self, loaded: Config):
        with transaction(loaded) as cursor:
            checks = run_checks(cursor)
        assert {c.check_name for c in checks} == EXPECTED_CHECKS

    def test_every_check_passes(self, loaded: Config):
        with transaction(loaded) as cursor:
            checks = run_checks(cursor)
        assert failures(checks) == []

    def test_enforce_is_silent_on_success(self, loaded: Config):
        with transaction(loaded) as cursor:
            enforce(run_checks(cursor), fail_on_error=True)


class TestDefectDetection:
    def test_empty_fact_table_is_caught(self, loaded: Config):
        with rollback_cursor(loaded) as cursor:
            cursor.execute("DELETE FROM analytics.member_term")
            checks = {c.check_name: c for c in run_checks(cursor)}
        assert checks["fact_table_not_empty"].status == "FAIL"

    def test_future_start_date_is_caught(self, loaded: Config):
        with rollback_cursor(loaded) as cursor:
            cursor.execute(
                """
                INSERT INTO analytics.member_term (
                    congress, bioguide_id, start_date, chamber, state_abbrev, party_code,
                    party_label, bioname, birthday, cumulative_congresses, cumulative_chamber,
                    age_days, age_years, generation)
                VALUES (70, 'F000001', CURRENT_DATE + 365, 'House', 'NY', 200, 'Republican',
                        'FUTURE, Row', DATE '1970-01-01', 1, 1, 18035, 49.3, 'Boomers')
                """
            )
            checks = {c.check_name: c for c in run_checks(cursor)}
        assert checks["no_future_start_dates"].status == "FAIL"
        assert checks["no_future_start_dates"].observed == 1

    def test_missing_staged_row_is_caught(self, loaded: Config):
        with rollback_cursor(loaded) as cursor:
            cursor.execute("DELETE FROM analytics.member_term WHERE congress = 66")
            checks = {c.check_name: c for c in run_checks(cursor)}
        assert checks["all_staged_rows_loaded"].status == "FAIL"
        assert checks["all_staged_rows_loaded"].observed == 2

    def test_value_drift_between_staging_and_fact_is_caught(self, loaded: Config):
        with rollback_cursor(loaded) as cursor:
            cursor.execute("UPDATE analytics.member_term SET age_years = 50.0 WHERE congress = 66")
            checks = {c.check_name: c for c in run_checks(cursor)}
        assert checks["staged_values_match_fact"].status == "FAIL"

    def test_stale_summary_model_is_caught(self, loaded: Config):
        with rollback_cursor(loaded) as cursor:
            # Change the fact table without rebuilding the aggregate.
            cursor.execute("DELETE FROM analytics.member_term WHERE congress = 67")
            checks = {c.check_name: c for c in run_checks(cursor)}
        assert checks["summary_reconciles_with_fact"].status == "FAIL"

    def test_duplicate_natural_key_is_caught_if_the_constraint_is_gone(self, loaded: Config):
        # The unique constraint normally makes this impossible. Dropping it
        # proves the check is a real second line of defence rather than dead
        # SQL that can never fire.
        with rollback_cursor(loaded) as cursor:
            cursor.execute("ALTER TABLE analytics.member_term DROP CONSTRAINT member_term_natural_key")
            cursor.execute(
                """
                INSERT INTO analytics.member_term (
                    congress, bioguide_id, start_date, chamber, state_abbrev, party_code,
                    party_label, bioname, birthday, cumulative_congresses, cumulative_chamber,
                    age_days, age_years, generation)
                SELECT congress, bioguide_id, start_date, chamber, state_abbrev, party_code,
                       party_label, bioname, birthday, cumulative_congresses, cumulative_chamber,
                       age_days, age_years, generation
                FROM analytics.member_term LIMIT 1
                """
            )
            checks = {c.check_name: c for c in run_checks(cursor)}
        assert checks["natural_key_is_unique"].status == "FAIL"
        assert checks["natural_key_is_unique"].observed == 1

    def test_out_of_range_age_is_caught_if_the_constraint_is_gone(self, loaded: Config):
        with rollback_cursor(loaded) as cursor:
            cursor.execute("ALTER TABLE analytics.member_term DROP CONSTRAINT member_term_age_years_range")
            cursor.execute("UPDATE analytics.member_term SET age_years = 200 WHERE congress = 66")
            checks = {c.check_name: c for c in run_checks(cursor)}
        assert checks["age_years_within_range"].status == "FAIL"

    def test_invalid_chamber_is_caught_if_the_constraint_is_gone(self, loaded: Config):
        with rollback_cursor(loaded) as cursor:
            cursor.execute("ALTER TABLE analytics.member_term DROP CONSTRAINT member_term_chamber_valid")
            cursor.execute("UPDATE analytics.member_term SET chamber = 'Assembly' WHERE congress = 66")
            checks = {c.check_name: c for c in run_checks(cursor)}
        assert checks["chamber_values_valid"].status == "FAIL"

    def test_birthday_after_start_is_caught_if_the_constraint_is_gone(self, loaded: Config):
        with rollback_cursor(loaded) as cursor:
            cursor.execute("ALTER TABLE analytics.member_term DROP CONSTRAINT member_term_birthday_before_start")
            cursor.execute("UPDATE analytics.member_term SET birthday = start_date WHERE congress = 66")
            checks = {c.check_name: c for c in run_checks(cursor)}
        assert checks["birthday_precedes_start_date"].status == "FAIL"


class TestEnforcement:
    def test_failure_raises_with_a_pointer_to_the_sql(self, loaded: Config):
        with rollback_cursor(loaded) as cursor:
            cursor.execute("DELETE FROM analytics.member_term")
            checks = run_checks(cursor)
        with pytest.raises(QualityCheckFailed) as excinfo:
            enforce(checks, fail_on_error=True)
        assert "004_quality.sql" in str(excinfo.value)

    def test_failures_can_be_downgraded_to_warnings(self, loaded: Config, caplog):
        with rollback_cursor(loaded) as cursor:
            cursor.execute("DELETE FROM analytics.member_term")
            checks = run_checks(cursor)
        # caplog.at_level forces the warning branch to actually execute. Without
        # it, any test that raised the root log level earlier in the session
        # would make this call a no-op and hide a crash inside it - which is
        # exactly how a real bug in this branch went unnoticed once.
        with caplog.at_level(logging.WARNING):
            enforce(checks, fail_on_error=False)  # must not raise
        assert "configured as warnings" in caplog.text


class TestReporting:
    def test_rendered_table_lists_every_check(self, loaded: Config):
        with transaction(loaded) as cursor:
            rendered = format_table(run_checks(cursor))
        for name in EXPECTED_CHECKS:
            assert name in rendered

    def test_empty_list_renders_safely(self):
        assert format_table([]) == "(no checks run)"
