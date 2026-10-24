"""End-to-end pipeline tests, including the engineering scenarios that matter.

Covers: first run, repeated run with unchanged source, new incremental records,
duplicate input, malformed input, partial failure and restart.
"""

from __future__ import annotations

import shutil
from dataclasses import replace

import pytest

from etl.config import PIPELINE_NAME, Config
from etl.db import transaction
from etl.load import fact_fingerprint, fact_row_count
from etl.pipeline import (
    run_all,
    step_advance_watermark,
    step_extract,
    step_load_staging,
    step_quality,
    step_upsert,
)
from etl.quality import QualityCheckFailed
from etl.watermark import get_watermark

from .conftest import FIXTURE_DIR


def seed(config: Config, filename: str) -> None:
    """Replace the source file the pipeline will read."""
    config.ensure_dirs()
    shutil.copy(FIXTURE_DIR / filename, config.raw_dir / "aging_congress.csv")


class TestFirstRun:
    def test_loads_everything_and_passes_quality(self, etl_env: Config):
        summary = run_all(etl_env, offline=True)
        assert summary["rows_read"] == 4
        assert summary["rows_inserted"] == 4
        assert summary["fact_rows_after"] == 4
        assert summary["quality_passed"] is True

    def test_advances_the_watermark(self, etl_env: Config):
        run_all(etl_env, offline=True)
        with transaction(etl_env) as cursor:
            assert get_watermark(cursor, PIPELINE_NAME) == 67

    def test_records_a_succeeded_batch(self, etl_env: Config):
        summary = run_all(etl_env, offline=True)
        with transaction(etl_env) as cursor:
            cursor.execute(
                "SELECT status, rows_read, rows_inserted FROM etl.batch_run WHERE batch_id = %s",
                (summary["batch_id"],),
            )
            assert cursor.fetchone() == ("succeeded", 4, 4)

    def test_builds_the_analytics_model(self, etl_env: Config):
        assert run_all(etl_env, offline=True)["summary_rows"] == 4


class TestRepeatedRunUnchangedSource:
    def test_second_run_inserts_nothing(self, etl_env: Config):
        run_all(etl_env, offline=True)
        summary = run_all(etl_env, offline=True)
        assert summary["rows_inserted"] == 0
        assert summary["rows_updated"] == 0
        assert summary["fact_rows_after"] == 4

    def test_second_run_leaves_the_warehouse_byte_identical(self, etl_env: Config):
        run_all(etl_env, offline=True)
        with transaction(etl_env) as cursor:
            before = fact_fingerprint(cursor)
        run_all(etl_env, offline=True)
        with transaction(etl_env) as cursor:
            assert fact_fingerprint(cursor) == before

    def test_full_refresh_is_also_idempotent(self, etl_env: Config):
        run_all(etl_env, offline=True)
        with transaction(etl_env) as cursor:
            before = fact_fingerprint(cursor)
        summary = run_all(etl_env, offline=True, full_refresh=True)
        assert summary["rows_staged"] == 4  # everything re-staged
        assert summary["rows_unchanged"] == 4  # and nothing written
        with transaction(etl_env) as cursor:
            assert fact_fingerprint(cursor) == before

    def test_watermark_does_not_rewind(self, etl_env: Config):
        run_all(etl_env, offline=True)
        run_all(etl_env, offline=True, full_refresh=True)
        with transaction(etl_env) as cursor:
            assert get_watermark(cursor, PIPELINE_NAME) == 67


class TestIncrementalRecords:
    def test_new_records_are_added_and_corrections_applied(self, etl_env: Config):
        run_all(etl_env, offline=True)
        seed(etl_env, "congress_incremental.csv")
        summary = run_all(etl_env, offline=True)
        assert summary["rows_inserted"] == 2
        assert summary["rows_updated"] == 1
        assert summary["fact_rows_after"] == 6

    def test_watermark_advances_to_the_new_congress(self, etl_env: Config):
        run_all(etl_env, offline=True)
        seed(etl_env, "congress_incremental.csv")
        run_all(etl_env, offline=True)
        with transaction(etl_env) as cursor:
            assert get_watermark(cursor, PIPELINE_NAME) == 68

    def test_watermark_limits_what_is_re_read(self, etl_env: Config):
        run_all(etl_env, offline=True)  # watermark 67, lookback 1 -> floor 66
        summary = run_all(etl_env, offline=True)
        assert summary["rows_skipped_by_watermark"] == 0  # floor 66 keeps all 4 rows
        assert summary["rows_staged"] == 4


class TestMalformedAndDuplicateInput:
    def test_malformed_rows_are_rejected_and_the_run_still_succeeds(self, etl_env: Config):
        seed(etl_env, "congress_malformed.csv")
        summary = run_all(etl_env, offline=True)
        assert summary["rejects"] == 8
        assert summary["rows_inserted"] == 1
        assert summary["quality_passed"] is True

    def test_rejects_are_queryable_afterwards(self, etl_env: Config):
        seed(etl_env, "congress_malformed.csv")
        summary = run_all(etl_env, offline=True)
        with transaction(etl_env) as cursor:
            cursor.execute("SELECT COUNT(*) FROM etl.rejected_row WHERE batch_id = %s", (summary["batch_id"],))
            assert cursor.fetchone()[0] == 8

    def test_duplicate_input_does_not_duplicate_business_rows(self, etl_env: Config):
        seed(etl_env, "congress_duplicates.csv")
        summary = run_all(etl_env, offline=True)
        assert summary["duplicates_dropped"] == 1
        assert summary["fact_rows_after"] == 2

    def test_empty_source_leaves_the_warehouse_untouched(self, etl_env: Config):
        run_all(etl_env, offline=True)
        with transaction(etl_env) as cursor:
            before = fact_fingerprint(cursor)
        seed(etl_env, "congress_header_only.csv")
        summary = run_all(etl_env, offline=True)
        assert summary["rows_staged"] == 0
        assert summary["fact_rows_after"] == 4
        with transaction(etl_env) as cursor:
            assert fact_fingerprint(cursor) == before


class TestPartialFailureAndRecovery:
    def test_quality_failure_leaves_the_watermark_behind(self, etl_env: Config):
        """A failed run must not advance the watermark, or data would be skipped."""
        run_all(etl_env, offline=True)
        with transaction(etl_env) as cursor:
            watermark_before = get_watermark(cursor, PIPELINE_NAME)

        seed(etl_env, "congress_incremental.csv")
        extracted = step_extract(etl_env, offline=True)
        step_load_staging(etl_env, batch_id=extracted["batch_id"], staged_path=extracted["staged_path"])
        step_upsert(etl_env, batch_id=extracted["batch_id"])
        # Skip build_analytics: the summary model is now stale, which the
        # cross-model reconciliation check is designed to catch.
        with pytest.raises(QualityCheckFailed):
            step_quality(etl_env, batch_id=extracted["batch_id"])

        with transaction(etl_env) as cursor:
            assert get_watermark(cursor, PIPELINE_NAME) == watermark_before

    def test_failed_batch_is_recorded_as_failed(self, etl_env: Config):
        run_all(etl_env, offline=True)
        seed(etl_env, "congress_incremental.csv")
        extracted = step_extract(etl_env, offline=True)
        step_load_staging(etl_env, batch_id=extracted["batch_id"], staged_path=extracted["staged_path"])
        step_upsert(etl_env, batch_id=extracted["batch_id"])
        with pytest.raises(QualityCheckFailed):
            step_quality(etl_env, batch_id=extracted["batch_id"])

        with transaction(etl_env) as cursor:
            cursor.execute(
                "SELECT status, error_message FROM etl.batch_run WHERE batch_id = %s",
                (extracted["batch_id"],),
            )
            status, message = cursor.fetchone()
        assert status == "failed"
        assert "summary_reconciles_with_fact" in message

    def test_rerunning_after_a_failure_recovers(self, etl_env: Config):
        """Restart is the whole recovery procedure: no manual cleanup needed."""
        run_all(etl_env, offline=True)
        seed(etl_env, "congress_incremental.csv")
        extracted = step_extract(etl_env, offline=True)
        step_load_staging(etl_env, batch_id=extracted["batch_id"], staged_path=extracted["staged_path"])
        step_upsert(etl_env, batch_id=extracted["batch_id"])
        with pytest.raises(QualityCheckFailed):
            step_quality(etl_env, batch_id=extracted["batch_id"])

        summary = run_all(etl_env, offline=True)
        assert summary["quality_passed"] is True
        assert summary["fact_rows_after"] == 6
        with transaction(etl_env) as cursor:
            assert get_watermark(cursor, PIPELINE_NAME) == 68

    def test_missing_source_file_in_offline_mode_fails_clearly(self, etl_env: Config):
        (etl_env.raw_dir / "aging_congress.csv").unlink()
        with pytest.raises(FileNotFoundError, match="offline"):
            run_all(etl_env, offline=True)

    def test_staging_is_truncated_so_a_rerun_cannot_double_stage(self, etl_env: Config):
        run_all(etl_env, offline=True)
        summary = run_all(etl_env, offline=True)
        with transaction(etl_env) as cursor:
            cursor.execute("SELECT COUNT(*) FROM staging.member_term_stage")
            assert cursor.fetchone()[0] == summary["rows_staged"] == 4

    def test_warning_only_quality_failure_does_not_commit_watermark(self, etl_env: Config):
        """Logging quality failures must never certify an unverified batch."""
        run_all(etl_env, offline=True)
        with transaction(etl_env) as cursor:
            previous = get_watermark(cursor, PIPELINE_NAME)

        seed(etl_env, "congress_incremental.csv")
        warning_config = replace(etl_env, fail_on_quality_error=False)
        extracted = step_extract(warning_config, offline=True)
        step_load_staging(
            warning_config,
            batch_id=extracted["batch_id"],
            staged_path=extracted["staged_path"],
        )
        step_upsert(warning_config, batch_id=extracted["batch_id"])
        # The aggregate was deliberately not rebuilt: reconciliation fails.
        quality = step_quality(warning_config, batch_id=extracted["batch_id"])
        assert quality["all_passed"] is False

        with pytest.raises(QualityCheckFailed, match="watermark held back"):
            step_advance_watermark(
                warning_config,
                batch_id=extracted["batch_id"],
                max_congress=extracted["max_congress"],
                quality_passed=quality["all_passed"],
            )

        with transaction(etl_env) as cursor:
            assert get_watermark(cursor, PIPELINE_NAME) == previous
            cursor.execute(
                "SELECT status FROM etl.batch_run WHERE batch_id = %s",
                (extracted["batch_id"],),
            )
            assert cursor.fetchone()[0] == "failed"


class TestConcurrencyGuard:
    def test_fact_count_matches_the_staged_natural_keys(self, etl_env: Config):
        run_all(etl_env, offline=True)
        with transaction(etl_env) as cursor:
            cursor.execute("SELECT COUNT(DISTINCT (congress, bioguide_id)) FROM staging.member_term_stage")
            staged_keys = cursor.fetchone()[0]
            assert fact_row_count(cursor) == staged_keys
