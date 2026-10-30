"""Streaming source downloads enforce the byte cap before buffering the body."""

from pathlib import Path

import pytest
import requests

from etl import source


class StreamResponse:
    def __init__(self, chunks):
        self.chunks = chunks
        self.closed = False
        self.consumed = 0

    def raise_for_status(self):
        return None

    def iter_content(self, chunk_size):
        assert chunk_size > 0
        for chunk in self.chunks:
            self.consumed += 1
            if isinstance(chunk, Exception):
                raise chunk
            yield chunk

    def close(self):
        self.closed = True


@pytest.fixture
def spec():
    return source.SourceSpec(
        name="stream-test",
        description="fixture",
        url="https://example.test/data.csv",
        filename="data.csv",
        required_columns=("id",),
        attribution="test",
    )


def _serve(monkeypatch, response):
    def fake_get(url, timeout, stream):
        assert url == "https://example.test/data.csv"
        assert stream is True
        assert timeout == source.HTTP_TIMEOUT_SECONDS
        return response

    monkeypatch.setattr(requests, "get", fake_get)


def test_streamed_multichunk_csv_is_published_byte_for_byte(tmp_path: Path, monkeypatch, spec):
    target = tmp_path / "data.csv"
    payload = b'id\\r\\n"multi\\r\\nline"\\r\\n'
    response = StreamResponse([payload[:4], b"", payload[4:9], payload[9:]])
    _serve(monkeypatch, response)

    assert source.fetch(spec, target) == target
    assert target.read_bytes() == payload
    assert response.closed is True
    assert response.consumed == 4
    assert not list(tmp_path.glob("*.tmp"))


def test_stream_limit_rejects_before_consuming_remaining_chunks(
    tmp_path: Path, monkeypatch, spec
):
    target = tmp_path / "data.csv"
    target.write_bytes(b"id\\nprevious\\n")
    monkeypatch.setattr(source, "MAX_SOURCE_BYTES", 8)
    response = StreamResponse([b"id\\n1\\n", b"more", b"never-read"])
    _serve(monkeypatch, response)

    with pytest.raises(RuntimeError, match="exceeds maximum size"):
        source.fetch(spec, target)

    assert response.consumed == 2
    assert response.closed is True
    assert target.read_bytes() == b"id\\nprevious\\n"
    assert not list(tmp_path.glob("*.tmp"))


def test_interrupted_stream_preserves_existing_snapshot(tmp_path: Path, monkeypatch, spec):
    target = tmp_path / "data.csv"
    target.write_bytes(b"id\\nprevious\\n")
    response = StreamResponse([b"id\\n1\\n", requests.ChunkedEncodingError("connection lost")])
    _serve(monkeypatch, response)

    with pytest.raises(RuntimeError, match="interrupted stream"):
        source.fetch(spec, target)

    assert target.read_bytes() == b"id\\nprevious\\n"
    assert response.closed is True
    assert not list(tmp_path.glob("*.tmp"))


def test_invalid_streamed_csv_closes_response_and_preserves_snapshot(
    tmp_path: Path, monkeypatch, spec
):
    target = tmp_path / "data.csv"
    target.write_bytes(b"id\\nprevious\\n")
    response = StreamResponse([b'id\\n"unterminated'])
    _serve(monkeypatch, response)

    with pytest.raises(RuntimeError, match="invalid CSV body"):
        source.fetch(spec, target)

    assert target.read_bytes() == b"id\\nprevious\\n"
    assert response.closed is True
    assert not list(tmp_path.glob("*.tmp"))
