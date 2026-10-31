"""COPY must reject stale or reordered stage schemas before touching PostgreSQL."""

import csv
from pathlib import Path

import pytest

from etl.load import copy_staged_csv
from etl.schema import STAGE_COLUMNS


class RecordingCursor:
    def __init__(self):
        self.copy_calls = []
        self.statements = []

    def copy_expert(self, sql, handle):
        self.copy_calls.append((sql, handle.read()))

    def execute(self, sql):
        self.statements.append(sql)

    def fetchone(self):
        return (1,)


@pytest.mark.parametrize(
    "header",
    [
        [],
        list(reversed(STAGE_COLUMNS)),
        list(STAGE_COLUMNS[:-1]),
        [*STAGE_COLUMNS, "unexpected"],
        [*STAGE_COLUMNS[:-1], STAGE_COLUMNS[0]],
        [""] * len(STAGE_COLUMNS),
    ],
    ids=["missing", "reordered", "short", "extra", "duplicate", "empty"],
)
def test_copy_rejects_incompatible_headers_before_database_write(
    tmp_path: Path, header: list[str]
):
    path = tmp_path / "staged.csv"
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.writer(handle)
        if header:
            writer.writerow(header)
        writer.writerow(["sample"] * len(STAGE_COLUMNS))

    cursor = RecordingCursor()
    with pytest.raises(ValueError, match="header must exactly match"):
        copy_staged_csv(cursor, path)

    assert cursor.copy_calls == []
    assert cursor.statements == []


def test_copy_rewinds_validated_file_before_bulk_load(tmp_path: Path):
    path = tmp_path / "staged.csv"
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.writer(handle)
        writer.writerow(STAGE_COLUMNS)
        writer.writerow(["example, with comma"] * len(STAGE_COLUMNS))

    cursor = RecordingCursor()
    assert copy_staged_csv(cursor, path) == 1

    assert len(cursor.copy_calls) == 1
    statement, copied = cursor.copy_calls[0]
    assert "HEADER true" in statement
    assert copied == path.read_text(encoding="utf-8")
    assert cursor.statements == ["SELECT COUNT(*) FROM staging.member_term_stage"]
