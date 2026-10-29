"""Source definition.

One real, openly licensed public dataset: FiveThirtyEight's "aging Congress"
file, one row per member per Congress from the 66th (1919) to the 118th (2023).

Licence: Creative Commons Attribution 4.0 (CC BY 4.0). Attribution is required
and is given in README.md and docs/DATASETS.md.

Why this dataset suits an incremental ETL:
  * ``congress`` is a monotonically increasing integer - a natural, readable
    watermark, unlike a timestamp that needs timezone care.
  * ``(congress, bioguide_id)`` is a genuine natural key (verified: 0 duplicate
    combinations across all 29,120 rows), so the unique constraint and the
    upsert rest on a real property of the data rather than an assumption.
  * A member appears in many Congresses, so the table exercises real joins and
    window functions rather than being a flat extract.
"""

from __future__ import annotations

import csv
import io
import os
import tempfile
from dataclasses import dataclass
from pathlib import Path

from .logging_setup import get_logger
from .schema import SchemaError, validate_header

logger = get_logger(__name__)

HTTP_TIMEOUT_SECONDS = 60
# Bound parsing and persistence of unexpectedly large upstream responses.
# requests.get() has already buffered the response; this is a CSV processing
# and disk-publishing limit, not a network streaming/memory limit.
MAX_SOURCE_BYTES = 20 * 1024 * 1024


@dataclass(frozen=True)
class SourceSpec:
    name: str
    description: str
    url: str
    filename: str
    required_columns: tuple[str, ...]
    attribution: str
    minimum_rows: int = 1


AGING_CONGRESS = SourceSpec(
    name="aging_congress",
    description="One row per member of Congress per Congress, with age at term start",
    url="https://raw.githubusercontent.com/fivethirtyeight/data/master/congress-demographics/data_aging_congress.csv",
    filename="aging_congress.csv",
    required_columns=(
        "congress",
        "start_date",
        "chamber",
        "state_abbrev",
        "party_code",
        "bioname",
        "bioguide_id",
        "birthday",
        "cmltv_cong",
        "cmltv_chamber",
        "age_days",
        "age_years",
        "generation",
    ),
    attribution="FiveThirtyEight, 'Congress Today Is Older Than It's Ever Been' (CC BY 4.0)",
    # Known historical snapshot has 29,120 rows. Reject implausibly short
    # but syntactically valid 200 responses before publishing them.
    minimum_rows=25_000,
)


def fetch(spec: SourceSpec, destination: Path, *, timeout: int = HTTP_TIMEOUT_SECONDS) -> Path:
    """Download the source CSV.

    ``requests`` is imported lazily so the package stays importable in an
    environment without it (CI unit tests run entirely from fixtures).
    """
    import requests  # noqa: PLC0415

    if isinstance(spec.minimum_rows, bool) or not isinstance(spec.minimum_rows, int) or spec.minimum_rows < 1:
        raise ValueError("source minimum_rows must be a positive integer")
    destination.parent.mkdir(parents=True, exist_ok=True)
    logger.info("fetching source", extra={"source": spec.name, "url": spec.url})
    try:
        response = requests.get(spec.url, timeout=timeout)
        response.raise_for_status()
    except requests.RequestException as exc:
        raise RuntimeError(
            f"Could not download {spec.name} from {spec.url}: {exc}. "
            f"If this host has no outbound network access, download the file manually to "
            f"{destination} and re-run with --offline. See docs/DATASETS.md."
        ) from exc
    # A successful HTTP status does not guarantee a usable snapshot. An empty
    # 200 response must not overwrite the last known-good offline source.
    if not response.content:
        raise RuntimeError(f"Downloaded source {spec.name} is empty; previous snapshot preserved")
    if len(response.content) > MAX_SOURCE_BYTES:
        raise RuntimeError(f"Downloaded source {spec.name} exceeds maximum size; previous snapshot preserved")
    # A 200 response may be an HTML error page, a header-only/truncated
    # snapshot, or a CSV with malformed quoting late in the file. Validate
    # the *entire* downloaded stream before atomically publishing it. Row-level
    # schema rejects remain the extractor's responsibility; this check only
    # establishes that the CSV can be decoded and parsed completely.
    with io.TextIOWrapper(io.BytesIO(response.content), encoding="utf-8-sig", newline="") as stream:
        reader = csv.reader(stream, strict=True)
        try:
            header = next(reader, None)
            validate_header(header, spec.required_columns)
        except (UnicodeDecodeError, csv.Error, SchemaError) as exc:
            raise RuntimeError(
                f"Downloaded source {spec.name} has an invalid CSV header; previous snapshot preserved"
            ) from exc
        try:
            # csv.reader yields [] for blank physical lines. Count all
            # nonblank records, not just the first valid row: a truncated
            # upstream response may still be perfectly parseable CSV.
            row_count = sum(bool(row) for row in reader)
            if row_count < spec.minimum_rows:
                raise ValueError(f"CSV has {row_count} data rows; expected at least {spec.minimum_rows}")
        except (UnicodeDecodeError, csv.Error, ValueError) as exc:
            raise RuntimeError(
                f"Downloaded source {spec.name} has an invalid CSV body; previous snapshot preserved"
            ) from exc
    # Publish only a fully written download. A disk error or interrupted
    # replacement must not truncate the last known-good raw source: an
    # incremental rerun may need that snapshot for recovery.
    temporary: Path | None = None
    try:
        with tempfile.NamedTemporaryFile(
            mode="wb",
            dir=destination.parent,
            prefix=f".{destination.name}.",
            suffix=".tmp",
            delete=False,
        ) as handle:
            temporary = Path(handle.name)
            handle.write(response.content)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, destination)
    finally:
        if temporary is not None:
            temporary.unlink(missing_ok=True)
    logger.info(
        "fetched source",
        extra={"source": spec.name, "bytes": len(response.content), "path": str(destination)},
    )
    return destination
