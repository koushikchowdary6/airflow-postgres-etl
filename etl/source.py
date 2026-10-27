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

import os
import tempfile
from dataclasses import dataclass
from pathlib import Path

from .logging_setup import get_logger

logger = get_logger(__name__)

HTTP_TIMEOUT_SECONDS = 60


@dataclass(frozen=True)
class SourceSpec:
    name: str
    description: str
    url: str
    filename: str
    required_columns: tuple[str, ...]
    attribution: str


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
)


def fetch(spec: SourceSpec, destination: Path, *, timeout: int = HTTP_TIMEOUT_SECONDS) -> Path:
    """Download the source CSV.

    ``requests`` is imported lazily so the package stays importable in an
    environment without it (CI unit tests run entirely from fixtures).
    """
    import requests  # noqa: PLC0415

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
