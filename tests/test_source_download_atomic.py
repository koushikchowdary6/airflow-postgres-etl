"""Downloaded source snapshots are replaced only after complete writes."""

from pathlib import Path
from types import SimpleNamespace

import pytest
import requests

import etl.source as source


@pytest.fixture
def source_spec():
    return source.SourceSpec(
        name="fixture",
        description="test",
        url="https://example.test/data.csv",
        filename="data.csv",
        required_columns=("id",),
        attribution="test",
    )


def _fake_response(payload: bytes):
    return SimpleNamespace(content=payload, raise_for_status=lambda: None)


def test_successful_download_atomically_replaces_existing_source(
    tmp_path: Path, monkeypatch, source_spec
):
    target = tmp_path / "nested" / "data.csv"
    target.parent.mkdir()
    target.write_bytes(b"previous,valid,snapshot\n")
    monkeypatch.setattr(requests, "get", lambda url, timeout: _fake_response(b"id\n1\n"))

    assert source.fetch(source_spec, target) == target
    assert target.read_bytes() == b"id\n1\n"
    assert not list(target.parent.glob("*.tmp"))


def test_failed_atomic_replace_preserves_last_good_source(
    tmp_path: Path, monkeypatch, source_spec
):
    target = tmp_path / "data.csv"
    target.write_bytes(b"old\n")
    monkeypatch.setattr(requests, "get", lambda url, timeout: _fake_response(b"new\n"))

    def fail_replace(src, dst):
        assert Path(src).read_bytes() == b"new\n"
        assert dst == target
        raise OSError("simulated rename failure")

    monkeypatch.setattr(source.os, "replace", fail_replace)
    with pytest.raises(OSError, match="simulated rename failure"):
        source.fetch(source_spec, target)

    assert target.read_bytes() == b"old\n"
    assert not list(tmp_path.glob("*.tmp"))


def test_failed_download_does_not_touch_existing_snapshot(
    tmp_path: Path, monkeypatch, source_spec
):
    target = tmp_path / "data.csv"
    target.write_bytes(b"old\n")

    def fail_get(url, timeout):
        raise requests.ConnectionError("upstream unavailable")

    monkeypatch.setattr(requests, "get", fail_get)
    with pytest.raises(RuntimeError, match="Could not download fixture"):
        source.fetch(source_spec, target)

    assert target.read_bytes() == b"old\n"
    assert not list(tmp_path.glob("*.tmp"))


def test_failed_download_does_not_publish_new_snapshot(
    tmp_path: Path, monkeypatch, source_spec
):
    target = tmp_path / "data.csv"
    monkeypatch.setattr(requests, "get", lambda url, timeout: _fake_response(b"new\n"))
    monkeypatch.setattr(source.os, "replace", lambda src, dst: (_ for _ in ()).throw(OSError("disk failure")))

    with pytest.raises(OSError, match="disk failure"):
        source.fetch(source_spec, target)

    assert not target.exists()
    assert not list(tmp_path.glob("*.tmp"))
