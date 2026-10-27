"""Validation, normalization and the canonical record.

A row is accepted only if every required field parses and passes its range
check. Anything else becomes a ``RejectedRow`` carrying the file row number,
the offending column, the raw value and the reason - enough to find and fix
the input without re-running anything.

Validation is explicit rather than delegated to a schema library so each rule
is visible and individually testable, and so the error messages say what to do.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal, InvalidOperation

# Range bounds. These are facts about the dataset's domain, not arbitrary
# guards: the 1st Congress convened in 1789 and no member has been verified
# older than 100 at the start of a term.
MIN_CONGRESS = 1
MAX_CONGRESS = 150
MIN_START_DATE = date(1789, 3, 4)
MIN_BIRTH_DATE = date(1700, 1, 1)
MIN_AGE_YEARS = Decimal("18")
MAX_AGE_YEARS = Decimal("105")

VALID_CHAMBERS = frozenset({"House", "Senate"})

AGE_YEARS_SCALE = Decimal("0.000001")

# ICPSR party codes present in this dataset. The raw code is always kept; this
# mapping only adds a readable label, so an unmapped code degrades to
# "Other (code N)" rather than losing the row.
PARTY_LABELS = {
    100: "Democrat",
    112: "Conservative",
    200: "Republican",
    328: "Independent",
    329: "Independent Democrat",
    331: "Independent Republican",
    347: "Prohibitionist",
    354: "Silver Republican",
    355: "Union",
    356: "Union Labor",
    370: "Progressive",
    380: "Socialist",
    402: "Liberal",
    522: "American Labor",
    537: "Farmer-Labor",
}


class SchemaError(ValueError):
    """Raised when the input file's header does not match expectations."""


@dataclass(frozen=True)
class MemberTerm:
    """One member's service in one Congress. Natural key: (congress, bioguide_id)."""

    congress: int
    bioguide_id: str
    start_date: date
    chamber: str
    state_abbrev: str
    party_code: int
    party_label: str
    bioname: str
    birthday: date
    cumulative_congresses: int
    cumulative_chamber: int
    age_days: int
    age_years: Decimal
    generation: str
    source_row_number: int

    def as_row(self) -> tuple[object, ...]:
        """Column order must match staging.member_term_stage in sql/001_schema.sql."""
        return (
            self.congress,
            self.bioguide_id,
            self.start_date,
            self.chamber,
            self.state_abbrev,
            self.party_code,
            self.party_label,
            self.bioname,
            self.birthday,
            self.cumulative_congresses,
            self.cumulative_chamber,
            self.age_days,
            self.age_years,
            self.generation,
            self.source_row_number,
        )


STAGE_COLUMNS: tuple[str, ...] = (
    "congress",
    "bioguide_id",
    "start_date",
    "chamber",
    "state_abbrev",
    "party_code",
    "party_label",
    "bioname",
    "birthday",
    "cumulative_congresses",
    "cumulative_chamber",
    "age_days",
    "age_years",
    "generation",
    "source_row_number",
)


@dataclass(frozen=True)
class RejectedRow:
    source_row_number: int
    column_name: str
    raw_value: str
    reason: str

    def as_row(self, batch_id: str) -> tuple[object, ...]:
        return (batch_id, self.source_row_number, self.column_name, self.raw_value[:500], self.reason)


class RowRejected(Exception):
    """Internal signal: this row cannot be loaded. Carries the reject detail."""

    def __init__(self, column_name: str, raw_value: str, reason: str) -> None:
        super().__init__(f"{column_name}: {reason}")
        self.column_name = column_name
        self.raw_value = raw_value
        self.reason = reason


def validate_header(header: list[str] | None, required: tuple[str, ...]) -> None:
    if header is None:
        raise SchemaError("input file has no header row")
    # DictReader silently overwrites earlier values when column names repeat.
    # Reject ambiguous headers before any records or watermark decisions.
    seen: set[str] = set()
    duplicates: set[str] = set()
    for name in header:
        if name in seen:
            duplicates.add(name)
        seen.add(name)
    if duplicates:
        raise SchemaError(f"input has duplicate CSV column(s): {sorted(duplicates)}")
    if any(not name.strip() for name in header):
        raise SchemaError("input has an empty CSV column name")
    missing = sorted(set(required) - set(header))
    if missing:
        raise SchemaError(
            f"input is missing required column(s) {missing}. Found: {sorted(header)}. "
            "The upstream schema may have changed; compare against etl/source.py."
        )


def _require(row: dict[str, str], column: str) -> str:
    value = (row.get(column) or "").strip()
    if not value:
        raise RowRejected(column, value, "required field is empty")
    return value


def _parse_int(row: dict[str, str], column: str, *, low: int | None = None, high: int | None = None) -> int:
    raw = _require(row, column)
    try:
        value = int(raw)
    except ValueError:
        raise RowRejected(column, raw, f"expected an integer, got {raw!r}") from None
    if low is not None and value < low:
        raise RowRejected(column, raw, f"value {value} is below the minimum {low}")
    if high is not None and value > high:
        raise RowRejected(column, raw, f"value {value} is above the maximum {high}")
    return value


def _parse_date(row: dict[str, str], column: str, *, earliest: date, today: date) -> date:
    raw = _require(row, column)
    try:
        value = datetime.strptime(raw, "%Y-%m-%d").date()
    except ValueError:
        raise RowRejected(column, raw, f"expected an ISO date (YYYY-MM-DD), got {raw!r}") from None
    if value < earliest:
        raise RowRejected(column, raw, f"date {value.isoformat()} is before {earliest.isoformat()}")
    if value > today:
        raise RowRejected(column, raw, f"date {value.isoformat()} is in the future (today is {today.isoformat()})")
    return value


def _parse_decimal(row: dict[str, str], column: str, *, low: Decimal, high: Decimal) -> Decimal:
    raw = _require(row, column)
    try:
        value = Decimal(raw)
    except InvalidOperation:
        raise RowRejected(column, raw, f"expected a number, got {raw!r}") from None
    if not value.is_finite():
        raise RowRejected(column, raw, "value is not finite")
    if not (low <= value <= high):
        raise RowRejected(column, raw, f"value {value} is outside the plausible range [{low}, {high}]")
    return value.quantize(AGE_YEARS_SCALE)


def party_label(code: int) -> str:
    return PARTY_LABELS.get(code, f"Other (code {code})")


def parse_row(row: dict[str, str], source_row_number: int, *, today: date) -> MemberTerm:
    """Validate and normalize one CSV row. Raises RowRejected on bad input."""
    congress = _parse_int(row, "congress", low=MIN_CONGRESS, high=MAX_CONGRESS)
    start_date = _parse_date(row, "start_date", earliest=MIN_START_DATE, today=today)
    birthday = _parse_date(row, "birthday", earliest=MIN_BIRTH_DATE, today=today)

    if birthday >= start_date:
        raise RowRejected(
            "birthday",
            birthday.isoformat(),
            f"birthday is not before the term start date {start_date.isoformat()}",
        )

    chamber = _require(row, "chamber")
    if chamber not in VALID_CHAMBERS:
        raise RowRejected("chamber", chamber, f"expected one of {sorted(VALID_CHAMBERS)}, got {chamber!r}")

    state_abbrev = _require(row, "state_abbrev").upper()
    if len(state_abbrev) != 2:
        raise RowRejected("state_abbrev", state_abbrev, "expected a two-letter state or territory code")

    party_code = _parse_int(row, "party_code", low=0)

    return MemberTerm(
        congress=congress,
        bioguide_id=_require(row, "bioguide_id"),
        start_date=start_date,
        chamber=chamber,
        state_abbrev=state_abbrev,
        party_code=party_code,
        party_label=party_label(party_code),
        bioname=_require(row, "bioname"),
        birthday=birthday,
        cumulative_congresses=_parse_int(row, "cmltv_cong", low=1),
        cumulative_chamber=_parse_int(row, "cmltv_chamber", low=1),
        age_days=_parse_int(row, "age_days", low=0),
        age_years=_parse_decimal(row, "age_years", low=MIN_AGE_YEARS, high=MAX_AGE_YEARS),
        generation=_require(row, "generation"),
        source_row_number=source_row_number,
    )
