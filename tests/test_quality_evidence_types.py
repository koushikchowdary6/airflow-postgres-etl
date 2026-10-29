"""Regression tests: malformed SQL check rows cannot masquerade as quality evidence."""

from unittest.mock import Mock

import pytest

from etl.quality import (
    REQUIRED_QUALITY_CHECKS,
    QualityCheck,
    QualityCheckFailed,
    enforce,
    run_checks,
)


def cursor_for(row):
    cursor = Mock()
    cursor.fetchall.return_value = [row]
    return cursor


@pytest.mark.parametrize("observed", [True, False, 0.5, -1, "0", None, 0.0])
def test_run_checks_rejects_coerced_or_negative_counts(observed):
    cursor = cursor_for(("natural_key_is_unique", "PASS", observed, "duplicate keys"))
    with pytest.raises(QualityCheckFailed, match="observed must be a nonnegative integer"):
        run_checks(cursor)


@pytest.mark.parametrize(
    "row, message",
    [
        (("natural_key_is_unique", "pass", 0, "duplicate keys"), "status must be PASS or FAIL"),
        (("natural_key_is_unique", "UNKNOWN", 0, "duplicate keys"), "status must be PASS or FAIL"),
        (("natural_key_is_unique", True, 0, "duplicate keys"), "status must be PASS or FAIL"),
        (("natural_key_is_unique", "PASS", 0, None), "missing detail"),
        (("natural_key_is_unique", "PASS", 0, "  "), "missing detail"),
        (("  ", "PASS", 0, "duplicate keys"), "malformed check name"),
        (("natural_key_is_unique", "PASS", 0), "expected four columns"),
    ],
)
def test_run_checks_rejects_invalid_result_contract(row, message):
    with pytest.raises(QualityCheckFailed, match=message):
        run_checks(cursor_for(row))


def test_run_checks_preserves_valid_integer_evidence():
    check = run_checks(cursor_for(("natural_key_is_unique", "PASS", 0, "duplicate keys")))[0]
    assert check == QualityCheck("natural_key_is_unique", "PASS", 0, "duplicate keys")


@pytest.mark.parametrize("fail_on_error", [True, False])
@pytest.mark.parametrize(
    "bad",
    [
        QualityCheck("natural_key_is_unique", "PASS", False, "duplicate keys"),
        QualityCheck("natural_key_is_unique", "pass", 0, "duplicate keys"),
        QualityCheck("natural_key_is_unique", "PASS", "0", "duplicate keys"),
        QualityCheck("natural_key_is_unique", "PASS", 0, ""),
    ],
)
def test_direct_enforcement_fails_closed_on_malformed_evidence(bad, fail_on_error):
    suite = [
        QualityCheck(name, "PASS", 1 if name == "fact_table_not_empty" else 0, "test")
        for name in sorted(REQUIRED_QUALITY_CHECKS)
    ]
    suite = [bad if check.check_name == bad.check_name else check for check in suite]
    with pytest.raises(QualityCheckFailed, match="Invalid SQL quality result"):
        enforce(suite, fail_on_error=fail_on_error)
