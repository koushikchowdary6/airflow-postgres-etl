"""Extraction tests: clean input, rejects, duplicates, watermark, empty files."""

from __future__ import annotations

from decimal import Decimal

import pytest

from etl.extract import extract_file
from etl.schema import SchemaError
from etl.source import AGING_CONGRESS

from .conftest import FIXTURE_DIR, TODAY


def extract(filename: str, **kwargs):
    kwargs.setdefault("today", TODAY)
    kwargs.setdefault("batch_id", "test-batch")
    return extract_file(AGING_CONGRESS, FIXTURE_DIR / filename, **kwargs)


class TestCleanExtract:
    def test_reads_every_row(self):
        result = extract("congress_clean.csv")
        assert result.rows_read == 4
        assert len(result.records) == 4
        assert result.rejects == []

    def test_reports_the_highest_congress(self):
        assert extract("congress_clean.csv").max_congress == 67

    def test_records_are_sorted_by_natural_key(self):
        records = extract("congress_clean.csv").records
        assert [(r.congress, r.bioguide_id) for r in records] == sorted((r.congress, r.bioguide_id) for r in records)

    def test_repeated_extraction_is_deterministic(self):
        first = [r.as_row() for r in extract("congress_clean.csv").records]
        second = [r.as_row() for r in extract("congress_clean.csv").records]
        assert first == second


class TestMalformedInput:
    def test_bad_rows_are_rejected_without_failing_the_batch(self):
        result = extract("congress_malformed.csv")
        assert result.rows_read == 9
        assert len(result.rejects) == 8
        assert len(result.records) == 1  # the single good row still loads

    def test_the_good_row_survives(self):
        result = extract("congress_malformed.csv")
        assert result.records[0].bioguide_id == "G000001"

    def test_every_reject_names_its_column(self):
        columns = {r.column_name for r in extract("congress_malformed.csv").rejects}
        assert columns == {
            "congress",
            "start_date",
            "chamber",
            "state_abbrev",
            "age_years",
            "bioguide_id",
            "birthday",
        }

    def test_rejects_carry_the_file_row_number(self):
        rejects = extract("congress_malformed.csv").rejects
        # First data row is file row 2.
        assert min(r.source_row_number for r in rejects) == 2
        assert max(r.source_row_number for r in rejects) == 9

    def test_missing_column_fails_fast(self):
        with pytest.raises(SchemaError, match="age_years"):
            extract("congress_missing_column.csv")


class TestDuplicates:
    def test_same_natural_key_collapses_to_one_record(self):
        result = extract("congress_duplicates.csv")
        keys = [(r.congress, r.bioguide_id) for r in result.records]
        assert len(keys) == len(set(keys)) == 2

    def test_duplicates_are_counted(self):
        assert extract("congress_duplicates.csv").duplicates_dropped == 1

    def test_last_occurrence_wins(self):
        result = extract("congress_duplicates.csv")
        doe = next(r for r in result.records if r.bioguide_id == "D000001")
        assert doe.age_years == Decimal("99.900000")


class TestEmptyInput:
    def test_header_only_file_yields_nothing_and_does_not_raise(self):
        result = extract("congress_header_only.csv")
        assert result.rows_read == 0
        assert result.records == []
        assert result.max_congress is None


class TestWatermarkFloor:
    def test_rows_below_the_floor_are_skipped(self):
        result = extract("congress_clean.csv", min_congress=67)
        assert result.rows_skipped_by_watermark == 2
        assert {r.congress for r in result.records} == {67}

    def test_floor_is_inclusive(self):
        result = extract("congress_clean.csv", min_congress=66)
        assert result.rows_skipped_by_watermark == 0
        assert len(result.records) == 4

    def test_floor_above_all_data_yields_an_empty_batch(self):
        result = extract("congress_clean.csv", min_congress=99)
        assert result.records == []
        assert result.rows_skipped_by_watermark == 4

    def test_summary_is_json_safe_for_xcom(self):
        import json

        # The DAG passes this dict between tasks via XCom, which must serialize.
        json.dumps(extract("congress_clean.csv").summary())
