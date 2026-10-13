# Verification record

Captured on 2026-10-13. Environment: Linux x86_64, Python 3.13.16,
PostgreSQL 16.15 (real server, not a mock), Airflow 2.11.2.

Everything below is output from commands that actually ran. Where something
could not be verified, it says so rather than being omitted.

## Summary

| Check | Tool | Result |
|---|---|---|
| Unit + integration tests | pytest 9.1.1 | **133 passed**, 7 skipped (Airflow-only) |
| DAG tests with Airflow installed | pytest + Airflow 2.11.2 | **23 passed** |
| Lint | ruff 0.17.0 | **clean** |
| Formatting | ruff format | **clean** (23 files) |
| `docker-compose.yml` | `docker compose config` | **valid** |
| `docker/init-databases.sh` | `bash -n` | **valid syntax** |
| Pipeline on real data | PostgreSQL 16 | **29,120 rows, 0 rejected** |
| Data-quality checks | SQL | **9/9 pass** |
| Idempotency, incremental rerun | PostgreSQL 16 | **0 inserted, 0 updated, fingerprint unchanged** |
| Idempotency, full re-process | PostgreSQL 16 | **0 inserted, 0 updated, fingerprint unchanged** |
| DAG executed end to end | `airflow dags test` | **all 7 tasks SUCCESS, DagRun success** |
| Idempotency through Airflow | `airflow dags test` twice | **fingerprint unchanged** |
| Full Docker Compose stack | `docker compose up` | **not run** — see below |
| CI (GitHub Actions) | — | **status unknown** — workflows committed but not yet run |

### What "not run" means for Docker

The Docker daemon was not available in this environment (`docker info` failed;
the client is installed, the daemon is not reachable). So:

* `docker compose config` **did** run and parses the file successfully, which
  validates the YAML, the variable interpolation, the service definitions and
  the dependency graph.
* `docker compose up` **did not** run. The images were never pulled, the
  containers never started, and the health checks never executed.

Because Docker was unavailable, PostgreSQL was installed directly instead
(`apt-get install postgresql`, 16.15) and every integration test, the pipeline
and the DAG were run against that real server. **The SQL, the upsert, the
constraints and the orchestration are genuinely verified; the container
orchestration is not.**

## Test suite

```
$ pytest
................sssssss................................................. [ 51%]
....................................................................     [100%]
133 passed, 7 skipped in 6.54s
```

The 7 skips are the real-Airflow `DagBag` tests, which skip where Airflow is
not installed. Run with Airflow available:

```
$ python -m pytest tests/test_dag.py -W "ignore::DeprecationWarning"
.......................                                                  [100%]
23 passed
```

CI fails the build if integration tests skip there, so a misconfigured service
container cannot masquerade as a green run.

Coverage by area:

| File | Needs a DB | Focus |
|---|---|---|
| `test_schema.py` | no | 15 parametrized reject cases, type normalization, party labels, header validation |
| `test_extract.py` | no | clean extract, 8 malformed rows, duplicates, header-only file, watermark floor, XCom serializability |
| `test_dag.py` | no | DAG structure, retry/timeout settings, no DB work at import time; plus real `DagBag` graph checks |
| `test_watermark.py` | yes | persistence, monotonic advance, refusal to rewind, per-pipeline isolation |
| `test_load.py` | yes | COPY, insert/update/no-op upsert paths, within-batch duplicates, unique and CHECK constraints, analytics model and window functions |
| `test_quality.py` | yes | all 9 checks pass on clean data, and each one fires when its defect is injected |
| `test_pipeline.py` | yes | first run, repeated run, incremental records, malformed input, partial failure, restart recovery |

## A real bug these tests caught

`etl/quality.py` logged with `extra={"message": ...}`. `message` is a reserved
`LogRecord` attribute, so `logging.Logger.makeRecord` raises
`KeyError: "Attempt to overwrite 'message' in LogRecord"`. The line that was
meant to warn would instead have crashed the run, in exactly the situation
where someone had chosen to downgrade quality failures to warnings.

The same bug existed in the companion `terraform-gcp-data-pipeline` project,
where it was **masked by test ordering**: an earlier test raised the root log
level, so the warning branch never executed and the suite stayed green. It
only surfaced when the test ran in isolation.

Both are fixed, and the test now wraps the call in `caplog.at_level(WARNING)`
so the branch is forced to execute regardless of what other tests leave behind.

## Pipeline run on real data

29,120 rows, 66th through 118th Congress.

```
=== Run summary ===
rows read             : 29120
rows staged           : 29120
rejected rows         : 0
duplicates dropped    : 0
fact rows before/after: 0 / 29120
  inserted            : 29120
  updated             : 0
  unchanged (skipped) : 0
summary model rows    : 106
max congress in batch : 118
watermark             : 118 (advanced: True)
fact fingerprint      : bba6289bdd44e929d738adf78666206f
```

All nine checks passed:

```
CHECK                         STATUS  OBSERVED  DETAIL
age_years_within_range        PASS           0  rows with age_years outside [18, 105]
all_staged_rows_loaded        PASS           0  staged natural keys missing from the fact table
birthday_precedes_start_date  PASS           0  rows where birthday is not before start_date
chamber_values_valid          PASS           0  rows whose chamber is neither House nor Senate
fact_table_not_empty          PASS       29120  rows in analytics.member_term
natural_key_is_unique         PASS           0  (congress, bioguide_id) combinations appearing more than once
no_future_start_dates         PASS           0  member terms starting after today
staged_values_match_fact      PASS           0  staged keys whose fact values differ from the staged values
summary_reconciles_with_fact  PASS           0  (congress, chamber) groups where the summary count does not match the fact table
```

The resulting model, which is checkable against published reporting on the
ageing of Congress:

```
 congress | chamber | member_count | median_age_years | median_change_vs_prev | pct_aged_65_plus
----------+---------+--------------+------------------+-----------------------+------------------
      118 | House   |          435 |          57.8398 |               -0.4353 |            28.74
      118 | Senate  |          101 |          65.2649 |                0.9829 |            50.50
      117 | House   |          449 |          58.2752 |                0.4435 |            30.96
      117 | Senate  |          102 |          64.2820 |                0.9350 |            49.02
      116 | House   |          442 |          57.8316 |               -0.2957 |            29.86
      116 | Senate  |          102 |          63.3470 |                0.7461 |            43.14
```

## Idempotency

### Incremental rerun

Watermark at 118 with a lookback of 1, so only the trailing window is re-read:

```
rows read             : 29120
rows skipped (watermark): 28033
rows staged           : 1087
  inserted            : 0
  updated             : 0
  unchanged (skipped) : 1087
fact fingerprint      : bba6289bdd44e929d738adf78666206f   (unchanged)
```

### Full re-process

The stricter test. Every row is re-staged and put through the upsert:

```
fingerprint BEFORE: 29120 rows, md5 bba6289bdd44e929d738adf78666206f
rows staged           : 29120
  inserted            : 0
  updated             : 0
  unchanged (skipped) : 29120
fingerprint AFTER : 29120 rows, md5 bba6289bdd44e929d738adf78666206f
```

`unchanged (skipped) = 29120` is the point: the `DO UPDATE ... WHERE` clause
filtered out every row, so nothing was written and even `updated_at` is
untouched. That is stricter than "no duplicates appeared".

### Corrections are still applied

Idempotency must not mean "ignores changes". `test_load.py` covers this: with a
corrected value in the source, the upsert reports `rows_updated == 1`, the row
count does not grow, and the new value is in place
(`test_correction_is_applied_in_place`).

## Airflow

### DAG execution

Run with `airflow dags test congress_demographics_etl` against Airflow 2.11.2
and the real PostgreSQL:

```
Marking task as SUCCESS ... task_id=ensure_schema
Marking task as SUCCESS ... task_id=extract
Marking task as SUCCESS ... task_id=load_staging
Marking task as SUCCESS ... task_id=upsert
Marking task as SUCCESS ... task_id=build_analytics
Marking task as SUCCESS ... task_id=data_quality
Marking task as SUCCESS ... task_id=advance_watermark
DagRun Finished: dag_id=congress_demographics_etl, ..., state=success
```

### Task graph, as Airflow built it

```
  ensure_schema        <- []
  extract              <- ['ensure_schema']
  load_staging         <- ['extract']
  upsert               <- ['load_staging']
  build_analytics      <- ['upsert']
  data_quality         <- ['build_analytics', 'extract']
  advance_watermark    <- ['data_quality', 'extract']
```

Every task: `retries=2`, `execution_timeout=0:20:00`. DAG: `catchup=False`,
`max_active_runs=1`, `schedule='0 6 * * *'`.

### Idempotency through the orchestrator

```
BEFORE 2nd airflow run: 29120 rows | fingerprint 6ec23996c9209085415186e365a66ee6
  DagRun 2: state=success
AFTER  2nd airflow run: 29120 rows | fingerprint 6ec23996c9209085415186e365a66ee6
```

(The fingerprint differs from the section above because that run used a
separate load of the same data; what matters is that it did not change across
the rerun.)

## Not verified

1. **The Docker Compose stack was never started.** `docker compose config`
   validates the file; it does not prove the images pull, the containers
   become healthy, or the Airflow webserver serves. The daemon was unavailable.
2. **The Airflow scheduler was never run.** `airflow dags test` executes tasks
   in-process. Scheduling, the executor, parallelism, the UI and real retry
   behaviour on failure are therefore unproven.
3. **Retries were never observed firing.** They are configured and asserted in
   tests; no task was made to fail transiently to watch a retry happen.
4. **GitHub Actions status is unknown**: the workflows are committed but had
   not run at the time of writing.
5. **Scale is untested.** 29,120 rows is small. Nothing here establishes
   behaviour at millions of rows; see the README's limitations.
6. **Concurrency is untested.** `max_active_runs=1` prevents overlap by
   configuration, not by anything proven here.
