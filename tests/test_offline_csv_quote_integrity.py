"""Offline CSV extraction must reject malformed quoting, not stage partial data."""

from __future__ import annotations

import csv

import pytest

from etl.extract import extract_file
from etl.source import AGING_CONGRESS

from .conftest import FIXTURE_DIR, TODAY


@pytest.mark.parametrize("corruption", ["unterminated", "trailing_text"])
def test_malformed_quoted_record_fails_entire_offline_extract(tmp_path, corruption):
    source = (FIXTURE_DIR / "congress_clean.csv").read_text(encoding="utf-8")
    if corruption == "unterminated":
        # The first four valid rows must not be returned as a partial batch.
        source += '67,"unterminated'
    else:
        # A permissive CSV reader accepts characters after a closing quote.
        source = source.replace('"DOE, John"', '"DOE, John"junk', 1)
    path = tmp_path / "invalid.csv"
    path.write_text(source, encoding="utf-8")

    with pytest.raises(csv.Error):
        extract_file(AGING_CONGRESS, path, today=TODAY)


def test_valid_quoted_multiline_field_remains_loadable(tmp_path):
    source = (FIXTURE_DIR / "congress_clean.csv").read_text(encoding="utf-8")
    source = source.replace('"DOE, John"', '"DOE,\\nJohn"', 1)
    # Replace the literal escape with a real newline inside the quoted field.
    source = source.replace('"DOE,\\nJohn"', '"DOE,\nJohn"', 1)
    path = tmp_path / "multiline.csv"
    path.write_text(source, encoding="utf-8")

    result = extract_file(AGING_CONGRESS, path, today=TODAY)
    assert result.rows_read == 4
    assert len(result.records) == 4
    assert result.rejects == []
    assert any(record.bioname == "DOE,\nJohn" for record in result.records)
