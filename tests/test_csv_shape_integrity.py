"""CSV shape drift must not silently change ETL records or watermark decisions."""

from __future__ import annotations

from etl.extract import extract_file
from etl.schema import SchemaError
from etl.source import AGING_CONGRESS

from .conftest import FIXTURE_DIR, TODAY


def _rows():
    return (FIXTURE_DIR / "congress_clean.csv").read_text(encoding="utf-8").splitlines()


def test_duplicate_required_header_fails_before_any_rows(tmp_path):
    lines = _rows()
    lines[0] += ",congress"
    path = tmp_path / "duplicate.csv"
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")

    try:
        extract_file(AGING_CONGRESS, path, today=TODAY)
    except SchemaError as exc:
        assert "duplicate CSV column" in str(exc)
        assert "congress" in str(exc)
    else:
        raise AssertionError("ambiguous duplicate header was accepted")


def test_duplicate_optional_header_is_not_silently_accepted(tmp_path):
    lines = _rows()
    lines[0] += ",extra,extra"
    path = tmp_path / "duplicate-extra.csv"
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")

    try:
        extract_file(AGING_CONGRESS, path, today=TODAY)
    except SchemaError as exc:
        assert "extra" in str(exc)
    else:
        raise AssertionError("duplicate optional header was accepted")


def test_blank_header_column_is_rejected(tmp_path):
    lines = _rows()
    lines[0] += ","
    path = tmp_path / "blank-header.csv"
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")

    try:
        extract_file(AGING_CONGRESS, path, today=TODAY)
    except SchemaError as exc:
        assert "empty CSV column name" in str(exc)
    else:
        raise AssertionError("empty header was accepted")


def test_surplus_fields_are_rejected_with_row_number_before_watermark(tmp_path):
    lines = _rows()
    # Row 2 is below the watermark, but its malformed shape must not vanish
    # into the skipped-row count. Row 3 is valid and remains loadable.
    lines[1] += ",UNEXPECTED"
    path = tmp_path / "surplus.csv"
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")

    result = extract_file(AGING_CONGRESS, path, today=TODAY, min_congress=67)
    assert result.rows_read == 4
    assert result.rows_skipped_by_watermark == 1
    assert len(result.rejects) == 1
    reject = result.rejects[0]
    assert reject.source_row_number == 2
    assert reject.column_name == "__row__"
    assert reject.raw_value == "UNEXPECTED"
    assert "more fields" in reject.reason
    assert len(result.records) == 2


def test_surplus_fields_do_not_discard_other_valid_rows(tmp_path):
    lines = _rows()
    lines[2] += ",EXTRA"
    path = tmp_path / "surplus-current.csv"
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")

    result = extract_file(AGING_CONGRESS, path, today=TODAY)
    assert result.rows_read == 4
    assert len(result.records) == 3
    assert len(result.rejects) == 1
    assert result.rejects[0].source_row_number == 3
