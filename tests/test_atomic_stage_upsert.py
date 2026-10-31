"""The shared staging table must not mix rows from overlapping batch runs."""

from __future__ import annotations

import threading
from pathlib import Path

import pytest

import etl.pipeline as pipeline
from etl.config import Config
from etl.db import transaction
from etl.extract import extract_file
from etl.load import fact_fingerprint, write_staged_csv
from etl.source import AGING_CONGRESS

from .conftest import FIXTURE_DIR, TODAY


def staged_fixture(tmp_path: Path, filename: str) -> str:
    result = extract_file(
        AGING_CONGRESS,
        FIXTURE_DIR / filename,
        batch_id=filename,
        today=TODAY,
    )
    return str(write_staged_csv(result.records, tmp_path / f"{filename}.staged.csv"))


def test_failed_copy_rolls_back_staging_truncate_and_fact(clean_db: Config, tmp_path: Path):
    good = staged_fixture(tmp_path, "congress_clean.csv")
    pipeline.step_load_and_upsert(clean_db, batch_id="good", staged_path=good)
    with transaction(clean_db) as cursor:
        cursor.execute("SELECT COUNT(*) FROM staging.member_term_stage")
        previous_staged = cursor.fetchone()[0]
        previous_fact = fact_fingerprint(cursor)

    malformed = tmp_path / "malformed.csv"
    malformed.write_text("not,the,required,header\n", encoding="utf-8")
    with pytest.raises(ValueError, match="header must exactly match"):
        pipeline.step_load_and_upsert(clean_db, batch_id="bad", staged_path=str(malformed))

    with transaction(clean_db) as cursor:
        cursor.execute("SELECT COUNT(*) FROM staging.member_term_stage")
        assert cursor.fetchone()[0] == previous_staged == 4
        assert fact_fingerprint(cursor) == previous_fact


def test_overlapping_batches_cannot_replace_staging_before_upsert(
    clean_db: Config, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
):
    first_path = staged_fixture(tmp_path, "congress_clean.csv")
    second_path = staged_fixture(tmp_path, "congress_incremental.csv")
    first_copied = threading.Event()
    second_attempted_truncate = threading.Event()
    allow_first_upsert = threading.Event()
    second_finished = threading.Event()
    original_copy = pipeline.copy_staged_csv
    original_truncate = pipeline.truncate_staging
    results: dict[str, dict] = {}
    errors: list[BaseException] = []

    def controlled_copy(cursor, path):
        count = original_copy(cursor, path)
        if threading.current_thread().name == "first-batch":
            first_copied.set()
            if not allow_first_upsert.wait(timeout=10):
                raise TimeoutError("test timed out waiting to release first batch")
        return count

    def observed_truncate(cursor):
        if threading.current_thread().name == "second-batch":
            second_attempted_truncate.set()
        return original_truncate(cursor)

    def worker(name: str, path: str):
        try:
            results[name] = pipeline.step_load_and_upsert(clean_db, batch_id=name, staged_path=path)
        except BaseException as exc:
            errors.append(exc)
        finally:
            if name == "second":
                second_finished.set()

    monkeypatch.setattr(pipeline, "copy_staged_csv", controlled_copy)
    monkeypatch.setattr(pipeline, "truncate_staging", observed_truncate)
    first = threading.Thread(target=worker, args=("first", first_path), name="first-batch")
    second = threading.Thread(target=worker, args=("second", second_path), name="second-batch")
    first.start()
    try:
        assert first_copied.wait(timeout=5), "first COPY did not complete"
        second.start()
        assert second_attempted_truncate.wait(timeout=5), "second TRUNCATE did not start"
        # The second run cannot commit a replacement staging batch while the
        # first transaction holds the staging-table lock through its upsert.
        assert not second_finished.wait(timeout=0.2)
    finally:
        allow_first_upsert.set()
        first.join(timeout=10)
        if second.ident is not None:
            second.join(timeout=10)

    assert not first.is_alive() and not second.is_alive()
    assert not errors, errors
    assert results["first"]["rows_staged"] == 4
    assert results["first"]["rows_inserted"] == 4
    assert results["second"]["rows_staged"] == 3
    assert results["second"]["rows_inserted"] == 2
    assert results["second"]["rows_updated"] == 1
    assert results["second"]["fact_rows_after"] == 6
