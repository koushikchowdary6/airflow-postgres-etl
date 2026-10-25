"""Integration tests for loading: COPY, upsert, constraints, analytics model.

These run the real SQL against a real PostgreSQL. They are the tests that
actually establish idempotency, so nothing here is mocked.
"""

from __future__ import annotations

from decimal import Decimal
from pathlib import Path
from unittest.mock import Mock

import psycopg2
import pytest

from etl.config import Config
from etl.db import transaction
from etl.extract import extract_file
from etl.load import (
    build_analytics,
    copy_staged_csv,
    fact_fingerprint,
    fact_row_count,
    insert_rejects,
    truncate_staging,
    upsert_fact,
    write_staged_csv,
)
from etl.schema import STAGE_COLUMNS, SchemaError
from etl.source import AGING_CONGRESS

from .conftest import FIXTURE_DIR, TODAY


def stage(cursor, tmp_path: Path, filename: str, **kwargs) -> int:
    """Extract a fixture and COPY it into the staging table."""
    kwargs.setdefault("today", TODAY)
    kwargs.setdefault("batch_id", "test-batch")
    result = extract_file(AGING_CONGRESS, FIXTURE_DIR / filename, **kwargs)
    path = write_staged_csv(result.records, tmp_path / f"{filename}.staged.csv")
    truncate_staging(cursor)
    return copy_staged_csv(cursor, path)


class TestCopyIntoStaging:
    def test_copies_every_record(self, clean_db: Config, tmp_path: Path):
        with transaction(clean_db) as cursor:
            assert stage(cursor, tmp_path, "congress_clean.csv") == 4

    def test_staging_is_replaced_between_batches(self, clean_db: Config, tmp_path: Path):
        with transaction(clean_db) as cursor:
            stage(cursor, tmp_path, "congress_clean.csv")
            assert stage(cursor, tmp_path, "congress_incremental.csv") == 3

    def test_empty_batch_copies_zero_rows(self, clean_db: Config, tmp_path: Path):
        with transaction(clean_db) as cursor:
            assert stage(cursor, tmp_path, "congress_header_only.csv") == 0

    def test_missing_staged_file_raises_an_actionable_error(self, clean_db: Config, tmp_path: Path):
        with transaction(clean_db) as cursor, pytest.raises(FileNotFoundError, match="Re-run the extract step"):
            copy_staged_csv(cursor, tmp_path / "does_not_exist.csv")


@pytest.mark.parametrize(
    "header",
    [
        list(STAGE_COLUMNS[1:2] + STAGE_COLUMNS[:1] + STAGE_COLUMNS[2:]),
        list(STAGE_COLUMNS[:-1]),
        [*STAGE_COLUMNS, "unexpected"],
        [*STAGE_COLUMNS[:-1], STAGE_COLUMNS[0]],
        [],
    ],
)
def test_copy_rejects_corrupt_staged_headers_before_sql(tmp_path: Path, header: list[str]):
    path = tmp_path / "tampered.csv"
    path.write_text(",".join(header) + "\n", encoding="utf-8")
    cursor = Mock()
    with pytest.raises(SchemaError, match="staged CSV header"):
        copy_staged_csv(cursor, path)
    cursor.copy_expert.assert_not_called()
    cursor.execute.assert_not_called()


def test_copy_rejects_headerless_staged_file(tmp_path: Path):
    path = tmp_path / "no-header.csv"
    path.write_text("", encoding="utf-8")
    cursor = Mock()
    with pytest.raises(SchemaError, match="staged CSV header"):
        copy_staged_csv(cursor, path)
    cursor.copy_expert.assert_not_called()


def test_copy_allows_header_only_staged_file(tmp_path: Path):
    path = tmp_path / "empty-batch.csv"
    path.write_text(",".join(STAGE_COLUMNS) + "\n", encoding="utf-8")
    cursor = Mock()
    cursor.fetchone.return_value = (0,)
    assert copy_staged_csv(cursor, path) == 0
    cursor.copy_expert.assert_called_once()


class TestUpsert:
    def test_first_load_inserts_everything(self, clean_db: Config, tmp_path: Path):
        with transaction(clean_db) as cursor:
            stage(cursor, tmp_path, "congress_clean.csv")
            stats = upsert_fact(cursor)
            assert stats["rows_inserted"] == 4
            assert stats["rows_updated"] == 0
            assert fact_row_count(cursor) == 4

    def test_rerunning_the_same_batch_changes_nothing(self, clean_db: Config, tmp_path: Path):
        with transaction(clean_db) as cursor:
            stage(cursor, tmp_path, "congress_clean.csv")
            upsert_fact(cursor)
            stats = upsert_fact(cursor)
            assert stats["rows_inserted"] == 0
            assert stats["rows_updated"] == 0
            assert stats["rows_unchanged"] == 4
            assert fact_row_count(cursor) == 4

    def test_rerun_does_not_touch_updated_at(self, clean_db: Config, tmp_path: Path):
        with transaction(clean_db) as cursor:
            stage(cursor, tmp_path, "congress_clean.csv")
            upsert_fact(cursor)
            before = fact_fingerprint(cursor)
            upsert_fact(cursor)
            # Unchanged rows are filtered by the DO UPDATE ... WHERE clause, so
            # even the audit column is untouched. This is the strict form of
            # idempotency, not merely "no duplicates appeared".
            assert fact_fingerprint(cursor) == before

    def test_new_rows_are_inserted_and_corrections_applied(self, clean_db: Config, tmp_path: Path):
        with transaction(clean_db) as cursor:
            stage(cursor, tmp_path, "congress_clean.csv")
            upsert_fact(cursor)
            stage(cursor, tmp_path, "congress_incremental.csv")
            stats = upsert_fact(cursor)
            # 2 new congress-68 rows inserted; 1 congress-67 row corrected.
            assert stats["rows_inserted"] == 2
            assert stats["rows_updated"] == 1
            assert fact_row_count(cursor) == 6

    def test_correction_is_applied_in_place(self, clean_db: Config, tmp_path: Path):
        with transaction(clean_db) as cursor:
            stage(cursor, tmp_path, "congress_clean.csv")
            upsert_fact(cursor)
            cursor.execute(
                "SELECT age_years FROM analytics.member_term WHERE congress = 67 AND bioguide_id = 'P000001'"
            )
            assert cursor.fetchone()[0] == Decimal("46.000000")

            stage(cursor, tmp_path, "congress_incremental.csv")
            upsert_fact(cursor)
            cursor.execute(
                "SELECT COUNT(*), MAX(age_years) FROM analytics.member_term "
                "WHERE congress = 67 AND bioguide_id = 'P000001'"
            )
            count, age = cursor.fetchone()
            assert count == 1  # updated, not duplicated
            assert age == Decimal("46.500000")

    def test_within_batch_duplicates_do_not_break_the_upsert(self, clean_db: Config, tmp_path: Path):
        # Without DISTINCT ON, PostgreSQL raises
        # "ON CONFLICT DO UPDATE command cannot affect row a second time".
        with transaction(clean_db) as cursor:
            stage(cursor, tmp_path, "congress_duplicates.csv")
            stats = upsert_fact(cursor)
            assert stats["rows_inserted"] == 2
            cursor.execute(
                "SELECT age_years FROM analytics.member_term WHERE congress = 66 AND bioguide_id = 'D000001'"
            )
            assert cursor.fetchone()[0] == Decimal("99.900000")

    def test_empty_batch_is_a_no_op(self, clean_db: Config, tmp_path: Path):
        with transaction(clean_db) as cursor:
            stage(cursor, tmp_path, "congress_clean.csv")
            upsert_fact(cursor)
            stage(cursor, tmp_path, "congress_header_only.csv")
            stats = upsert_fact(cursor)
            assert stats["rows_inserted"] == 0
            assert fact_row_count(cursor) == 4


class TestDatabaseConstraints:
    def test_natural_key_is_enforced_by_the_database(self, clean_db: Config, tmp_path: Path):
        """Even a hand-written INSERT cannot create a duplicate business row."""
        with transaction(clean_db) as cursor:
            stage(cursor, tmp_path, "congress_clean.csv")
            upsert_fact(cursor)
        with pytest.raises(psycopg2.errors.UniqueViolation), transaction(clean_db) as cursor:
            cursor.execute(
                """
                    INSERT INTO analytics.member_term (
                        congress, bioguide_id, start_date, chamber, state_abbrev, party_code,
                        party_label, bioname, birthday, cumulative_congresses, cumulative_chamber,
                        age_days, age_years, generation)
                    VALUES (66, 'D000001', DATE '1919-05-19', 'House', 'NY', 200, 'Republican',
                            'DOE, John', DATE '1870-01-01', 1, 1, 18035, 49.3, 'Lost')
                    """
            )

    @pytest.mark.parametrize(
        ("column", "value", "constraint"),
        [
            ("chamber", "'Assembly'", "chamber_valid"),
            ("age_years", "200", "age_years_range"),
            ("age_days", "-1", "age_days_non_negative"),
            ("congress", "999", "congress_range"),
        ],
    )
    def test_check_constraints_reject_bad_values(self, clean_db: Config, column: str, value: str, constraint: str):
        columns = {
            "congress": "66",
            "bioguide_id": "'X000001'",
            "start_date": "DATE '1919-05-19'",
            "chamber": "'House'",
            "state_abbrev": "'NY'",
            "party_code": "200",
            "party_label": "'Republican'",
            "bioname": "'TEST, Row'",
            "birthday": "DATE '1870-01-01'",
            "cumulative_congresses": "1",
            "cumulative_chamber": "1",
            "age_days": "18035",
            "age_years": "49.3",
            "generation": "'Lost'",
        }
        columns[column] = value
        with pytest.raises(psycopg2.errors.CheckViolation, match=constraint), transaction(clean_db) as cursor:
            cursor.execute(
                f"INSERT INTO analytics.member_term ({', '.join(columns)}) VALUES ({', '.join(columns.values())})"
            )

    def test_birthday_must_precede_start_date(self, clean_db: Config):
        with (
            pytest.raises(psycopg2.errors.CheckViolation, match="birthday_before_start"),
            transaction(clean_db) as cursor,
        ):
            cursor.execute(
                """
                    INSERT INTO analytics.member_term (
                        congress, bioguide_id, start_date, chamber, state_abbrev, party_code,
                        party_label, bioname, birthday, cumulative_congresses, cumulative_chamber,
                        age_days, age_years, generation)
                    VALUES (66, 'Y000001', DATE '1919-05-19', 'House', 'NY', 200, 'Republican',
                            'TEST, Row', DATE '1950-01-01', 1, 1, 18035, 49.3, 'Lost')
                    """
            )


class TestAnalyticsModel:
    def test_builds_one_row_per_congress_and_chamber(self, clean_db: Config, tmp_path: Path):
        with transaction(clean_db) as cursor:
            stage(cursor, tmp_path, "congress_clean.csv")
            upsert_fact(cursor)
            assert build_analytics(cursor) == 4  # 2 congresses x 2 chambers

    def test_computes_median_and_counts(self, clean_db: Config, tmp_path: Path):
        with transaction(clean_db) as cursor:
            stage(cursor, tmp_path, "congress_clean.csv")
            upsert_fact(cursor)
            build_analytics(cursor)
            cursor.execute(
                """
                SELECT member_count, median_age_years, oldest_member
                FROM analytics.congress_age_summary
                WHERE congress = 66 AND chamber = 'Senate'
                """
            )
            count, median, oldest = cursor.fetchone()
            assert count == 1
            assert median == Decimal("58.9000")
            assert oldest == "ROE, Jane"

    def test_change_vs_previous_congress_uses_the_window_function(self, clean_db: Config, tmp_path: Path):
        with transaction(clean_db) as cursor:
            stage(cursor, tmp_path, "congress_clean.csv")
            upsert_fact(cursor)
            build_analytics(cursor)
            cursor.execute(
                """
                SELECT congress, median_change_vs_prev
                FROM analytics.congress_age_summary
                WHERE chamber = 'House' ORDER BY congress
                """
            )
            rows = dict(cursor.fetchall())
            assert rows[66] is None  # no previous Congress
            assert rows[67] == Decimal("1.9000")  # 51.2 - 49.3

    def test_model_is_rebuilt_not_appended(self, clean_db: Config, tmp_path: Path):
        with transaction(clean_db) as cursor:
            stage(cursor, tmp_path, "congress_clean.csv")
            upsert_fact(cursor)
            build_analytics(cursor)
            assert build_analytics(cursor) == 4


class TestRejectPersistence:
    def test_rejects_are_stored_with_their_reasons(self, clean_db: Config, tmp_path: Path):
        result = extract_file(
            AGING_CONGRESS,
            FIXTURE_DIR / "congress_malformed.csv",
            batch_id="reject-batch",
            today=TODAY,
        )
        with transaction(clean_db) as cursor:
            assert insert_rejects(cursor, "reject-batch", result.rejects) == 8
            cursor.execute(
                "SELECT COUNT(DISTINCT column_name) FROM etl.rejected_row WHERE batch_id = %s",
                ("reject-batch",),
            )
            assert cursor.fetchone()[0] == 7

    def test_no_rejects_writes_nothing(self, clean_db: Config):
        with transaction(clean_db) as cursor:
            assert insert_rejects(cursor, "empty-batch", []) == 0
