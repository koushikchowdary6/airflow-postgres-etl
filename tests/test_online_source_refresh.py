"""Online runs must refresh upstream data; offline runs must remain reproducible."""

from __future__ import annotations

import shutil
from pathlib import Path

import pytest

from etl.config import Config
from etl.db import transaction
from etl.pipeline import step_extract

from .conftest import FIXTURE_DIR


@pytest.mark.parametrize("cached_snapshot", [True, False])
def test_online_run_fetches_even_with_a_cached_snapshot(
    etl_env: Config, monkeypatch: pytest.MonkeyPatch, cached_snapshot: bool
) -> None:
    raw_path = etl_env.raw_dir / "aging_congress.csv"
    if not cached_snapshot:
        raw_path.unlink()
    refreshed = FIXTURE_DIR / "congress_incremental.csv"
    calls: list[Path] = []

    def fake_fetch(spec, destination: Path) -> Path:
        calls.append(destination)
        shutil.copy(refreshed, destination)
        return destination

    monkeypatch.setattr("etl.pipeline.fetch", fake_fetch)
    result = step_extract(etl_env, offline=False)

    assert calls == [raw_path]
    assert raw_path.read_bytes() == refreshed.read_bytes()
    assert result["rows_read"] == 6


def test_offline_run_never_fetches_and_uses_existing_snapshot(
    etl_env: Config, monkeypatch: pytest.MonkeyPatch
) -> None:
    raw_path = etl_env.raw_dir / "aging_congress.csv"
    before = raw_path.read_bytes()

    def unexpected_fetch(*args, **kwargs):
        raise AssertionError("offline mode attempted a network fetch")

    monkeypatch.setattr("etl.pipeline.fetch", unexpected_fetch)
    result = step_extract(etl_env, offline=True)

    assert result["rows_read"] == 4
    assert raw_path.read_bytes() == before


def test_failed_online_refresh_does_not_load_stale_cache_or_start_batch(
    etl_env: Config, monkeypatch: pytest.MonkeyPatch
) -> None:
    raw_path = etl_env.raw_dir / "aging_congress.csv"
    before = raw_path.read_bytes()

    def failed_fetch(*args, **kwargs):
        raise RuntimeError("upstream refresh failed")

    monkeypatch.setattr("etl.pipeline.fetch", failed_fetch)
    with pytest.raises(RuntimeError, match="upstream refresh failed"):
        step_extract(etl_env, offline=False)

    assert raw_path.read_bytes() == before
    with transaction(etl_env) as cursor:
        cursor.execute("SELECT COUNT(*) FROM etl.batch_run")
        assert cursor.fetchone()[0] == 0
