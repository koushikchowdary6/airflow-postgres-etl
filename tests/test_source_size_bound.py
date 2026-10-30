"""Oversized upstream CSVs must not replace known-good ETL snapshots."""

from pathlib import Path
from types import SimpleNamespace

import pytest
import requests

from etl import source


@pytest.fixture
def spec():
    return source.SourceSpec(
        name="fixture",
        description="offline test",
        url="https://example.test/data.csv",
        filename="data.csv",
        required_columns=("id",),
        attribution="test",
    )


@pytest.mark.parametrize("has_existing", [True, False])
def test_oversized_csv_preserves_snapshot(tmp_path: Path, monkeypatch, spec, has_existing):
    destination = tmp_path / "source.csv"
    previous = b"id\nprevious\n"
    if has_existing:
        destination.write_bytes(previous)

    payload = b"id\n" + b"x" * 100 + b"\n"
    monkeypatch.setattr(source, "MAX_SOURCE_BYTES", 32)
    monkeypatch.setattr(
        requests,
        "get",
        lambda url, timeout, stream: SimpleNamespace(iter_content=lambda chunk_size: (payload,), close=lambda: None, raise_for_status=lambda: None),
    )

    with pytest.raises(RuntimeError, match="exceeds maximum size"):
        source.fetch(spec, destination)

    if has_existing:
        assert destination.read_bytes() == previous
    else:
        assert not destination.exists()
    assert not list(tmp_path.glob("*.tmp"))


def test_csv_at_exact_size_limit_is_accepted(tmp_path: Path, monkeypatch, spec):
    destination = tmp_path / "source.csv"
    payload = b"id\n123\n"
    monkeypatch.setattr(source, "MAX_SOURCE_BYTES", len(payload))
    monkeypatch.setattr(
        requests,
        "get",
        lambda url, timeout, stream: SimpleNamespace(iter_content=lambda chunk_size: (payload,), close=lambda: None, raise_for_status=lambda: None),
    )

    assert source.fetch(spec, destination) == destination
    assert destination.read_bytes() == payload
