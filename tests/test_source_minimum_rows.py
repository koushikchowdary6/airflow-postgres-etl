"""A parseable but severely truncated source is not a complete snapshot."""

from pathlib import Path
from types import SimpleNamespace

import pytest
import requests

from etl.source import AGING_CONGRESS, SourceSpec, fetch


@pytest.fixture
def spec():
    return SourceSpec(
        name="fixture",
        description="offline test",
        url="https://example.test/data.csv",
        filename="data.csv",
        required_columns=("id",),
        attribution="test",
        minimum_rows=3,
    )


def _serve(monkeypatch, payload: bytes):
    monkeypatch.setattr(
        requests,
        "get",
        lambda url, timeout, stream: SimpleNamespace(iter_content=lambda chunk_size: (payload,), close=lambda: None, raise_for_status=lambda: None),
    )


def test_short_valid_csv_preserves_previous_snapshot(tmp_path: Path, monkeypatch, spec):
    destination = tmp_path / "source.csv"
    previous = b"id\n1\n2\n3\n4\n"
    destination.write_bytes(previous)
    _serve(monkeypatch, b"id\n1\n2\n")

    with pytest.raises(RuntimeError, match="invalid CSV body"):
        fetch(spec, destination)

    assert destination.read_bytes() == previous
    assert not list(tmp_path.glob("*.tmp"))


def test_short_valid_csv_cannot_initialize_snapshot(tmp_path: Path, monkeypatch, spec):
    destination = tmp_path / "source.csv"
    _serve(monkeypatch, b"id\n1\n2\n")

    with pytest.raises(RuntimeError, match="invalid CSV body"):
        fetch(spec, destination)

    assert not destination.exists()


def test_blank_lines_do_not_satisfy_minimum_row_count(tmp_path: Path, monkeypatch, spec):
    destination = tmp_path / "source.csv"
    _serve(monkeypatch, b"id\n1\n\n\n2\n")

    with pytest.raises(RuntimeError, match="invalid CSV body"):
        fetch(spec, destination)

    assert not destination.exists()


def test_complete_snapshot_passes_with_minimum_rows(tmp_path: Path, monkeypatch, spec):
    destination = tmp_path / "source.csv"
    payload = b"id\n1\n\n2\n3\n"
    _serve(monkeypatch, payload)

    assert fetch(spec, destination) == destination
    assert destination.read_bytes() == payload


@pytest.mark.parametrize("minimum_rows", [0, -1, True, 1.5, "10"])
def test_invalid_source_minimum_is_rejected_before_download(tmp_path: Path, monkeypatch, spec, minimum_rows):
    invalid = SourceSpec(
        name=spec.name,
        description=spec.description,
        url=spec.url,
        filename=spec.filename,
        required_columns=spec.required_columns,
        attribution=spec.attribution,
        minimum_rows=minimum_rows,
    )
    monkeypatch.setattr(
        requests,
        "get",
        lambda *args, **kwargs: pytest.fail("invalid source config must not download"),
    )
    with pytest.raises(ValueError, match="minimum_rows"):
        fetch(invalid, tmp_path / "source.csv")


def test_aging_congress_uses_a_conservative_historical_row_floor():
    assert AGING_CONGRESS.minimum_rows == 25_000
