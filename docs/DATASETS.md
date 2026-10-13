# Dataset

One real, public, openly licensed dataset. Nothing here is personal,
confidential or restricted: it describes elected federal officials in their
public capacity, and every field is already published.

## Aging Congress

| | |
|---|---|
| Source | <https://github.com/fivethirtyeight/data/tree/master/congress-demographics> |
| File | `https://raw.githubusercontent.com/fivethirtyeight/data/master/congress-demographics/data_aging_congress.csv` |
| Article | FiveThirtyEight, "Congress Today Is Older Than It's Ever Been" |
| Licence | Creative Commons Attribution 4.0 (CC BY 4.0) — **attribution required** |
| Grain | one row per member of Congress per Congress |
| Coverage | 66th Congress (1919) to 118th (2023), 29,120 rows |

Attribution, as the licence requires: data from **FiveThirtyEight**,
["Congress Today Is Older Than It's Ever Been"](https://github.com/fivethirtyeight/data/tree/master/congress-demographics),
used under CC BY 4.0.

### Columns consumed

| Column | Becomes | Notes |
|---|---|---|
| `congress` | `congress` | Watermark column. Integer, monotonically increasing. |
| `bioguide_id` | `bioguide_id` | Stable per-person identifier from the Biographical Directory. |
| `start_date` | `start_date` | Date the Congress convened. |
| `chamber` | `chamber` | `House` or `Senate`; validated against that set. |
| `state_abbrev` | `state_abbrev` | Two-letter state or territory code. |
| `party_code` | `party_code`, `party_label` | ICPSR numeric code; the label is added, the code is kept. |
| `bioname` | `bioname` | `SURNAME, Forename`. |
| `birthday` | `birthday` | Validated to fall before `start_date`. |
| `cmltv_cong` | `cumulative_congresses` | Congresses served to date. |
| `cmltv_chamber` | `cumulative_chamber` | Congresses served in this chamber to date. |
| `age_days` | `age_days` | Age in days at `start_date`. |
| `age_years` | `age_years` | Age in years; range-checked to [18, 105]. |
| `generation` | `generation` | Named generational cohort. |

## Why this dataset suits an incremental, idempotent ETL

**`congress` is a natural watermark.** It is the dataset's own monotonic
integer sequence. Using it avoids every timestamp problem — no timezones, no
clock skew, no "is this boundary inclusive" ambiguity — and a watermark of
`118` is immediately readable, unlike `2026-10-13T06:00:00Z`.

**`(congress, bioguide_id)` is a genuine natural key.** Verified rather than
assumed: across all 29,120 rows there are **zero** duplicate combinations. The
unique constraint and the upsert therefore rest on a real property of the data.
A member serving several terms appears once per Congress, which is correct, and
a member who moved from the House to the Senate still appears once per Congress.

**The data exercises real modelling.** Members recur across Congresses, so the
table supports career-history queries, window functions over time, and
group-by-chamber aggregation — not just a flat extract.

**It is append-mostly with occasional revision**, which is exactly the shape
that makes a deterministic key plus an upsert necessary rather than decorative.
A new Congress adds rows; a corrected birth date changes an existing one.

## Acquiring the data

```bash
make fetch              # or: python -m etl.cli fetch
```

The file lands in `data/raw/aging_congress.csv`, which is gitignored: it is
reproducible from the URL above and does not belong in version control.

With no outbound network access, download it by hand to that path and run:

```bash
python -m etl.cli run --offline
```

Small committed fixtures in `tests/fixtures/` let the whole test suite run with
no network at all.

## A note on what is *not* validated

The pipeline checks structure, types, ranges and internal consistency
(`birthday < start_date`, age within plausible bounds, chamber from a fixed
set). It does not verify the facts against an independent source — if
FiveThirtyEight records a wrong birth date, this pipeline will faithfully load
a wrong birth date and every check will pass. That is a deliberate boundary:
these are data-quality checks, not fact-checking.
