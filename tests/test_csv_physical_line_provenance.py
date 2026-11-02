"""CSV rejects should cite physical file lines, not logical record offsets."""

from etl.extract import extract_file
from etl.source import AGING_CONGRESS

from .conftest import FIXTURE_DIR, TODAY


def _extract_modified(tmp_path, content):
    source = tmp_path / "source.csv"
    source.write_text(content, encoding="utf-8")
    return extract_file(AGING_CONGRESS, source, today=TODAY)


def test_reject_after_quoted_multiline_field_uses_physical_line(tmp_path):
    source = (FIXTURE_DIR / "congress_clean.csv").read_text(encoding="utf-8")
    source = source.replace('"DOE, John"', '"DOE,\\nJohn"', 1)
    source = source.replace('"DOE,\\nJohn"', '"DOE,\nJohn"', 1)
    source = source.replace("58.900000", "invalid-age", 1)

    result = _extract_modified(tmp_path, source)

    assert result.rows_read == 4
    assert len(result.records) == 3
    assert len(result.rejects) == 1
    assert result.rejects[0].column_name == "age_years"
    # Header line 1; first data record spans lines 2-3; bad record is line 4.
    assert result.rejects[0].source_row_number == 4


def test_reject_after_blank_csv_line_uses_physical_line(tmp_path):
    source = (FIXTURE_DIR / "congress_clean.csv").read_text(encoding="utf-8")
    source = source.replace("66,1919-05-19,Senate", "\n66,1919-05-19,Senate", 1)
    source = source.replace("58.900000", "invalid-age", 1)

    result = _extract_modified(tmp_path, source)

    assert result.rows_read == 4
    assert len(result.records) == 3
    assert len(result.rejects) == 1
    assert result.rejects[0].source_row_number == 4
