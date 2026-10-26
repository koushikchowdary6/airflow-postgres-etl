"""Pure unit tests for fail-closed quality-suite result integrity."""

import pytest

from etl.quality import (
    REQUIRED_QUALITY_CHECKS,
    QualityCheck,
    QualityCheckFailed,
    enforce,
)


def clean_suite():
    # fact_table_not_empty is the one inverse count predicate.
    return [
        QualityCheck(name, "PASS", 1 if name == "fact_table_not_empty" else 0, "test")
        for name in sorted(REQUIRED_QUALITY_CHECKS)
    ]


@pytest.mark.parametrize("fail_on_error", [True, False])
def test_unknown_quality_check_fails_closed_even_in_warning_mode(fail_on_error):
    checks = clean_suite() + [QualityCheck("unreviewed_probe", "PASS", 0, "test")]
    with pytest.raises(QualityCheckFailed, match="unexpected checks: unreviewed_probe"):
        enforce(checks, fail_on_error=fail_on_error)


@pytest.mark.parametrize(
    "name,status,count",
    [
        ("natural_key_is_unique", "PASS", 2),
        ("natural_key_is_unique", "FAIL", 0),
        ("fact_table_not_empty", "PASS", 0),
        ("fact_table_not_empty", "FAIL", 1),
        ("staged_values_match_fact", "PASS", -1),
    ],
)
@pytest.mark.parametrize("fail_on_error", [True, False])
def test_contradictory_status_and_observed_count_never_pass(name, status, count, fail_on_error):
    checks = [QualityCheck(c.check_name, status, count, c.detail) if c.check_name == name else c for c in clean_suite()]
    with pytest.raises(QualityCheckFailed, match="status/count mismatch"):
        enforce(checks, fail_on_error=fail_on_error)


def test_complete_consistent_suite_passes():
    enforce(clean_suite())


def test_genuine_failure_can_still_be_warning_only(caplog):
    checks = [
        QualityCheck(c.check_name, "FAIL", 2, c.detail) if c.check_name == "natural_key_is_unique" else c
        for c in clean_suite()
    ]
    enforce(checks, fail_on_error=False)
    assert "configured as warnings" in caplog.text
