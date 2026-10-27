"""Extraction: CSV -> validated records + rejects, honouring the watermark.

Returns an ``ExtractResult`` rather than writing to the database, so the
extract step is a pure function of (file, watermark) and can be unit-tested
without PostgreSQL.
"""

from __future__ import annotations

import csv
import uuid
from dataclasses import dataclass, field
from datetime import UTC, date, datetime
from pathlib import Path

from .logging_setup import get_logger
from .schema import MemberTerm, RejectedRow, RowRejected, parse_row, validate_header
from .source import SourceSpec

logger = get_logger(__name__)


def new_batch_id() -> str:
    stamp = datetime.now(tz=UTC).strftime("%Y%m%dT%H%M%SZ")
    return f"{stamp}-{uuid.uuid4().hex[:8]}"


@dataclass
class ExtractResult:
    batch_id: str
    source_name: str
    records: list[MemberTerm] = field(default_factory=list)
    rejects: list[RejectedRow] = field(default_factory=list)
    rows_read: int = 0
    rows_skipped_by_watermark: int = 0
    duplicates_dropped: int = 0

    @property
    def max_congress(self) -> int | None:
        return max((r.congress for r in self.records), default=None)

    def summary(self) -> dict[str, object]:
        return {
            "batch_id": self.batch_id,
            "source_name": self.source_name,
            "rows_read": self.rows_read,
            "records": len(self.records),
            "rejects": len(self.rejects),
            "duplicates_dropped": self.duplicates_dropped,
            "rows_skipped_by_watermark": self.rows_skipped_by_watermark,
            "max_congress": self.max_congress,
        }


def extract_file(
    spec: SourceSpec,
    path: Path,
    *,
    batch_id: str | None = None,
    min_congress: int | None = None,
    today: date | None = None,
) -> ExtractResult:
    """Read and validate a source CSV.

    ``min_congress`` is the inclusive floor from the watermark: rows for older
    Congresses are skipped without being parsed.
    """
    batch_id = batch_id or new_batch_id()
    horizon = today or datetime.now(tz=UTC).date()
    result = ExtractResult(batch_id=batch_id, source_name=spec.name)
    # Natural key -> record. A later row for the same key supersedes an earlier
    # one, matching the upsert's "last write wins" rule.
    deduped: dict[tuple[int, str], MemberTerm] = {}

    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        reader = csv.DictReader(handle)
        validate_header(reader.fieldnames, spec.required_columns)
        for offset, row in enumerate(reader):
            # Row 1 is the header, so data rows start at 2 - the number a
            # person sees when they open the file to investigate a reject.
            row_number = offset + 2
            result.rows_read += 1

            # DictReader stores surplus fields under the None key. Ignoring
            # them would silently accept shifted/truncated source records.
            # Check before the watermark so malformed old rows remain visible.
            if None in row:
                result.rejects.append(
                    RejectedRow(
                        source_row_number=row_number,
                        column_name="__row__",
                        raw_value=",".join(row[None])[:500],
                        reason="row has more fields than the CSV header",
                    )
                )
                continue

            # Cheap pre-filter: skip old Congresses before full validation.
            raw_congress = (row.get("congress") or "").strip()
            if min_congress is not None and raw_congress.isdigit() and int(raw_congress) < min_congress:
                result.rows_skipped_by_watermark += 1
                continue

            try:
                record = parse_row(row, row_number, today=horizon)
            except RowRejected as rejected:
                result.rejects.append(
                    RejectedRow(
                        source_row_number=row_number,
                        column_name=rejected.column_name,
                        raw_value=rejected.raw_value,
                        reason=rejected.reason,
                    )
                )
                continue

            key = (record.congress, record.bioguide_id)
            if key in deduped:
                result.duplicates_dropped += 1
            deduped[key] = record

    result.records = sorted(deduped.values(), key=lambda r: (r.congress, r.bioguide_id))
    logger.info("extract complete", extra=result.summary())
    return result
