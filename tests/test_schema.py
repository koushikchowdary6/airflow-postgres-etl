"""Unit tests for row-level validation and normalization."""

from __future__ import annotations

from datetime import date
from decimal import Decimal

import pytest

from etl.schema import (
    RowRejected,
    SchemaError,
    parse_row,
    party_label,
    validate_header,
)
from etl.source import AGING_CONGRESS

TODAY = date(2026, 10, 1)

VALID = {
    "congress": "66",
    "start_date": "1919-05-19",
    "chamber": "House",
    "state_abbrev": "NY",
    "party_code": "200",
    "bioname": "DOE, John",
    "bioguide_id": "D000001",
    "birthday": "1870-01-01",
    "cmltv_cong": "1",
    "cmltv_chamber": "1",
    "age_days": "18035",
    "age_years": "49.3",
    "generation": "Lost",
}


def row(**overrides: str) -> dict[str, str]:
    return {**VALID, **overrides}


class TestValidRow:
    def test_parses_a_good_row(self):
        record = parse_row(row(), 2, today=TODAY)
        assert record.congress == 66
        assert record.bioguide_id == "D000001"
        assert record.chamber == "House"

    def test_normalizes_types(self):
        record = parse_row(row(), 2, today=TODAY)
        assert isinstance(record.congress, int)
        assert record.start_date == date(1919, 5, 19)
        assert record.birthday == date(1870, 1, 1)
        assert record.age_years == Decimal("49.300000")

    def test_uppercases_state_code(self):
        assert parse_row(row(state_abbrev="ny"), 2, today=TODAY).state_abbrev == "NY"

    def test_keeps_source_row_number(self):
        assert parse_row(row(), 47, today=TODAY).source_row_number == 47

    def test_adds_a_readable_party_label(self):
        assert parse_row(row(party_code="100"), 2, today=TODAY).party_label == "Democrat"
        assert parse_row(row(party_code="200"), 2, today=TODAY).party_label == "Republican"


class TestPartyLabel:
    def test_maps_known_codes(self):
        assert party_label(100) == "Democrat"
        assert party_label(328) == "Independent"

    def test_unknown_code_degrades_without_losing_the_code(self):
        # An unmapped code must not drop the row; the raw code is preserved.
        assert party_label(999) == "Other (code 999)"


class TestRejectedRows:
    @pytest.mark.parametrize(
        ("overrides", "column", "fragment"),
        [
            ({"congress": "notanumber"}, "congress", "expected an integer"),
            ({"congress": "999"}, "congress", "above the maximum"),
            ({"congress": "0"}, "congress", "below the minimum"),
            ({"start_date": "not-a-date"}, "start_date", "expected an ISO date"),
            ({"start_date": "2099-01-01"}, "start_date", "in the future"),
            ({"start_date": "1700-01-01"}, "start_date", "is before"),
            ({"chamber": "Assembly"}, "chamber", "expected one of"),
            ({"state_abbrev": "XYZ"}, "state_abbrev", "two-letter"),
            ({"age_years": "200"}, "age_years", "outside the plausible range"),
            ({"age_years": "2"}, "age_years", "outside the plausible range"),
            ({"age_years": "abc"}, "age_years", "expected a number"),
            ({"age_days": "-5"}, "age_days", "below the minimum"),
            ({"bioguide_id": ""}, "bioguide_id", "required field is empty"),
            ({"bioname": "  "}, "bioname", "required field is empty"),
            ({"generation": ""}, "generation", "required field is empty"),
        ],
    )
    def test_each_defect_is_rejected_with_its_column_and_reason(self, overrides, column, fragment):
        with pytest.raises(RowRejected) as excinfo:
            parse_row(row(**overrides), 2, today=TODAY)
        assert excinfo.value.column_name == column
        assert fragment in excinfo.value.reason

    def test_birthday_must_precede_term_start(self):
        with pytest.raises(RowRejected) as excinfo:
            parse_row(row(birthday="1950-01-01"), 2, today=TODAY)
        assert excinfo.value.column_name == "birthday"
        assert "not before the term start date" in excinfo.value.reason

    def test_reject_carries_the_raw_value_for_investigation(self):
        with pytest.raises(RowRejected) as excinfo:
            parse_row(row(age_years="200"), 2, today=TODAY)
        assert excinfo.value.raw_value == "200"


class TestHeaderValidation:
    def test_accepts_the_real_header(self):
        validate_header(list(AGING_CONGRESS.required_columns), AGING_CONGRESS.required_columns)

    def test_tolerates_extra_columns(self):
        validate_header([*AGING_CONGRESS.required_columns, "extra_column"], AGING_CONGRESS.required_columns)

    def test_names_the_missing_column(self):
        header = [c for c in AGING_CONGRESS.required_columns if c != "age_years"]
        with pytest.raises(SchemaError, match="age_years"):
            validate_header(header, AGING_CONGRESS.required_columns)

    def test_rejects_absent_header(self):
        with pytest.raises(SchemaError, match="no header row"):
            validate_header(None, AGING_CONGRESS.required_columns)
