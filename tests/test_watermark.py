"""Watermark tests: pure arithmetic plus the PostgreSQL-backed store."""

from __future__ import annotations

import pytest

from etl.config import PIPELINE_NAME, Config
from etl.db import transaction
from etl.watermark import advance_watermark, effective_floor, get_watermark, set_watermark


class TestEffectiveFloor:
    def test_first_run_processes_everything(self):
        assert effective_floor(None, 1) is None

    def test_floor_is_watermark_minus_lookback(self):
        assert effective_floor(118, 1) == 117

    def test_zero_lookback_is_a_strict_watermark(self):
        assert effective_floor(118, 0) == 118

    def test_floor_never_drops_below_the_first_congress(self):
        assert effective_floor(2, 10) == 1

    def test_negative_lookback_is_rejected(self):
        with pytest.raises(ValueError, match="lookback_congresses"):
            effective_floor(118, -1)


class TestWatermarkStore:
    def test_absent_watermark_reads_as_none(self, clean_db: Config):
        with transaction(clean_db) as cursor:
            assert get_watermark(cursor, PIPELINE_NAME) is None

    def test_roundtrips(self, clean_db: Config):
        with transaction(clean_db) as cursor:
            set_watermark(cursor, PIPELINE_NAME, 100)
        with transaction(clean_db) as cursor:
            assert get_watermark(cursor, PIPELINE_NAME) == 100

    def test_set_overwrites(self, clean_db: Config):
        with transaction(clean_db) as cursor:
            set_watermark(cursor, PIPELINE_NAME, 100)
            set_watermark(cursor, PIPELINE_NAME, 105)
            assert get_watermark(cursor, PIPELINE_NAME) == 105

    def test_advance_moves_forward(self, clean_db: Config):
        with transaction(clean_db) as cursor:
            assert advance_watermark(cursor, PIPELINE_NAME, 100) is True
            assert advance_watermark(cursor, PIPELINE_NAME, 110) is True
            assert get_watermark(cursor, PIPELINE_NAME) == 110

    def test_advance_refuses_to_rewind(self, clean_db: Config):
        with transaction(clean_db) as cursor:
            set_watermark(cursor, PIPELINE_NAME, 110)
            assert advance_watermark(cursor, PIPELINE_NAME, 100) is False
            assert get_watermark(cursor, PIPELINE_NAME) == 110

    def test_advance_to_the_same_value_is_a_no_op(self, clean_db: Config):
        with transaction(clean_db) as cursor:
            set_watermark(cursor, PIPELINE_NAME, 110)
            assert advance_watermark(cursor, PIPELINE_NAME, 110) is False

    def test_pipelines_are_tracked_independently(self, clean_db: Config):
        with transaction(clean_db) as cursor:
            set_watermark(cursor, PIPELINE_NAME, 110)
            set_watermark(cursor, "other_pipeline", 42)
            assert get_watermark(cursor, PIPELINE_NAME) == 110
            assert get_watermark(cursor, "other_pipeline") == 42
