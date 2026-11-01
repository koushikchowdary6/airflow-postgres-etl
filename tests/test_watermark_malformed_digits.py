"""Malformed numeric Congress fields must be rejected, not crash the watermark filter."""

from __future__ import annotations

import pytest

from etl.extract import extract_file
from etl.source import AGING_CONGRESS

from .conftest import FIXTURE_DIR, TODAY


@pytest.mark.parametrize(
    "bad_congress",
    [
        "²",  # isdigit() is true, but int() rejects superscript digits
        "9" * 5000,  # isdigit() is true, but Python limits integer conversion
    ],
    ids=["superscript-digit", "oversized-decimal"],
)
def test_bad_congress_digits_become_row_rejects_with_incremental_floor(tmp_path, bad_congress):
    source = (FIXTURE_DIR / "congress_clean.csv").read_text(encoding="utf-8")
    source = source.replace("66,1919-05-19", f"{bad_congress},1919-05-19", 1)
    path = tmp_path / "malformed_congress.csv"
    path.write_text(source, encoding="utf-8")

    result = extract_file(AGING_CONGRESS, path, min_congress=67, today=TODAY)

    assert result.rows_read == 4
    assert result.rows_skipped_by_watermark == 1
    assert len(result.records) == 2
    assert len(result.rejects) == 1
    assert result.rejects[0].column_name == "congress"
    assert result.rejects[0].raw_value == bad_congress
