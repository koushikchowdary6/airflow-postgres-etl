"""Reject truncated or syntactically broken upstream CSV before replacing a good snapshot."""

from pathlib import Path
from types import SimpleNamespace

import pytest
import requests

from etl.source import SourceSpec, fetch


@pytest.fixture
def spec():
    return SourceSpec(
        name="fixture",
        description="test",
        url="https://example.test/data.csv",
        filename="data.csv",
        required_columns=("id",),
        attribution="test",
    )


@pytest.mark.parametrize(
    "payload",
    [
        b"id\n",
        b"id\n\n",
        b'id\n"unterminated',
        b'id\n1\n"unterminated',
        b"id\n1\n\xff",
    ],
    ids=["header-only", "blank-data", "unclosed-first-row", "unclosed-later-row", "invalid-later-utf8"],
)
def test_incomplete_or_corrupt_download_preserves_last_good_snapshot(tmp_path: Path, monkeypatch, spec, payload):
    destination = tmp_path / "source.csv"
    previous = b"id\nprevious\n"
    destination.write_bytes(previous)
    monkeypatch.setattr(
        requests,
        "get",
        lambda url, timeout, stream: SimpleNamespace(iter_content=lambda chunk_size: (payload,), close=lambda: None, raise_for_status=lambda: None),
    )

    with pytest.raises(RuntimeError, match=r"invalid CSV (?:header|body)"):
        fetch(spec, destination)

    assert destination.read_bytes() == previous
    assert not list(tmp_path.glob("*.tmp"))


def test_invalid_body_never_creates_new_snapshot(tmp_path: Path, monkeypatch, spec):
    destination = tmp_path / "source.csv"
    monkeypatch.setattr(
        requests,
        "get",
        lambda url, timeout, stream: SimpleNamespace(iter_content=lambda chunk_size: (b'id\n"unfinished',), close=lambda: None, raise_for_status=lambda: None),
    )

    with pytest.raises(RuntimeError, match="invalid CSV body"):
        fetch(spec, destination)

    assert not destination.exists()


def test_valid_quoted_multiline_csv_is_published_without_modification(tmp_path: Path, monkeypatch, spec):
    destination = tmp_path / "source.csv"
    payload = b'id\n"line one\nline two"\n"another, value"\n'
    monkeypatch.setattr(
        requests,
        "get",
        lambda url, timeout, stream: SimpleNamespace(iter_content=lambda chunk_size: (payload,), close=lambda: None, raise_for_status=lambda: None),
    )

    assert fetch(spec, destination) == destination
    assert destination.read_bytes() == payload


def test_row_level_shape_errors_remain_extractors_responsibility(tmp_path: Path, monkeypatch, spec):
    destination = tmp_path / "source.csv"
    payload = b"id\nvalid\nextra,field\n"
    monkeypatch.setattr(
        requests,
        "get",
        lambda url, timeout, stream: SimpleNamespace(iter_content=lambda chunk_size: (payload,), close=lambda: None, raise_for_status=lambda: None),
    )

    fetch(spec, destination)
    assert destination.read_bytes() == payload
