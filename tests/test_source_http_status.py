"""Reject non-200 upstream responses before they can replace a complete snapshot."""

from pathlib import Path

import pytest
import requests

from etl.source import SourceSpec, fetch


class Response:
    def __init__(self, status_code: int, payload: bytes):
        self.status_code = status_code
        self.payload = payload
        self.closed = False
        self.streamed = False

    def raise_for_status(self):
        # requests.Response.raise_for_status() does not reject 202/206/304.
        return None

    def iter_content(self, chunk_size):
        self.streamed = True
        yield self.payload

    def close(self):
        self.closed = True


@pytest.mark.parametrize("status_code", [202, 206, 304])
@pytest.mark.parametrize("previous", [True, False])
def test_non_complete_http_response_never_publishes_snapshot(tmp_path: Path, monkeypatch, status_code, previous):
    destination = tmp_path / "source.csv"
    if previous:
        destination.write_bytes(b"id\nlast-good\n")
    spec = SourceSpec(
        name="fixture",
        description="test",
        url="https://example.test/data.csv",
        filename="source.csv",
        required_columns=("id",),
        attribution="test",
    )
    # A complete-looking CSV is still not evidence that a 206 fragment is
    # the full upstream object. Status must be checked before streaming.
    response = Response(status_code, b"id\nnew\n")
    monkeypatch.setattr(requests, "get", lambda *args, **kwargs: response)

    with pytest.raises(RuntimeError, match=f"HTTP {status_code}"):
        fetch(spec, destination)

    assert response.closed
    assert not response.streamed
    if previous:
        assert destination.read_bytes() == b"id\nlast-good\n"
    else:
        assert not destination.exists()
    assert not list(tmp_path.glob("*.tmp"))
