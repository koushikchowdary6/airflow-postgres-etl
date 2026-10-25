"""Staged ETL handoff must not expose incomplete CSVs after failures."""

import csv
from pathlib import Path

import pytest

import etl.load as load_module
from etl.schema import STAGE_COLUMNS


class FakeRecord:
    def __init__(self, value):
        self.value = value

    def as_row(self):
        return [self.value] * len(STAGE_COLUMNS)


class BrokenRecord:
    def as_row(self):
        raise RuntimeError("simulated serialization failure")


def test_staged_csv_is_complete_and_replaced_on_success(tmp_path: Path):
    destination = tmp_path / "batch.csv"
    assert load_module.write_staged_csv([FakeRecord("old")], destination) == destination
    assert load_module.write_staged_csv([FakeRecord("new")], destination) == destination

    with destination.open(newline="", encoding="utf-8") as handle:
        rows = list(csv.reader(handle))
    assert rows[0] == list(STAGE_COLUMNS)
    assert rows[1] == ["new"] * len(STAGE_COLUMNS)
    assert len(rows) == 2
    assert not list(tmp_path.glob(".*.tmp"))


def test_serialization_failure_preserves_existing_stage(tmp_path: Path):
    destination = tmp_path / "batch.csv"
    load_module.write_staged_csv([FakeRecord("good")], destination)
    original = destination.read_bytes()

    with pytest.raises(RuntimeError, match="simulated serialization failure"):
        load_module.write_staged_csv([FakeRecord("partial"), BrokenRecord()], destination)

    assert destination.read_bytes() == original
    assert not list(tmp_path.glob(".*.tmp"))


def test_serialization_failure_does_not_publish_new_stage(tmp_path: Path):
    destination = tmp_path / "new.csv"
    with pytest.raises(RuntimeError, match="simulated serialization failure"):
        load_module.write_staged_csv([BrokenRecord()], destination)

    assert not destination.exists()
    assert not list(tmp_path.glob(".*.tmp"))


def test_failed_replace_preserves_previous_stage(tmp_path: Path, monkeypatch):
    destination = tmp_path / "batch.csv"
    load_module.write_staged_csv([FakeRecord("good")], destination)
    original = destination.read_bytes()

    def fail_replace(source, target):
        raise OSError("simulated disk failure")

    monkeypatch.setattr(load_module.os, "replace", fail_replace)
    with pytest.raises(OSError, match="simulated disk failure"):
        load_module.write_staged_csv([FakeRecord("new")], destination)

    assert destination.read_bytes() == original
    assert not list(tmp_path.glob(".*.tmp"))
