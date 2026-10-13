"""Command-line entry point.

    python -m etl.cli init-db        # create schemas, tables, constraints, indexes
    python -m etl.cli fetch          # download the source CSV
    python -m etl.cli run            # full pipeline
    python -m etl.cli run --offline --full-refresh
    python -m etl.cli quality        # re-run the data-quality checks
    python -m etl.cli state          # watermark and recent batch history
    python -m etl.cli examples       # run the example analytical queries

Exit codes: 0 success, 1 pipeline or data-quality failure, 2 configuration or
database-connection error.
"""

from __future__ import annotations

import argparse
import sys

from .config import PIPELINE_NAME, Config, ConfigError
from .db import DatabaseUnavailable, read_sql, transaction
from .logging_setup import configure_logging, get_logger
from .pipeline import latest_checks, run_all, step_ensure_schema
from .quality import QualityCheckFailed, enforce, format_table
from .source import AGING_CONGRESS, fetch
from .watermark import get_watermark

logger = get_logger(__name__)


def cmd_init_db(_: argparse.Namespace) -> int:
    config = Config.from_env()
    step_ensure_schema(config)
    print(f"Schema ready on {config.safe_target()}")
    return 0


def cmd_fetch(_: argparse.Namespace) -> int:
    config = Config.from_env()
    config.ensure_dirs()
    path = fetch(AGING_CONGRESS, config.raw_dir / AGING_CONGRESS.filename)
    print(f"Downloaded {path} ({path.stat().st_size:,} bytes)")
    return 0


def cmd_run(args: argparse.Namespace) -> int:
    config = Config.from_env()
    summary = run_all(config, offline=args.offline, full_refresh=args.full_refresh)
    print("\n=== Run summary ===")
    print(f"batch_id              : {summary['batch_id']}")
    print(f"rows read             : {summary['rows_read']}")
    print(f"rows skipped (watermark): {summary['rows_skipped_by_watermark']}")
    print(f"rows staged           : {summary['rows_staged']}")
    print(f"rejected rows         : {summary['rejects']}")
    print(f"duplicates dropped    : {summary['duplicates_dropped']}")
    print(f"fact rows before/after: {summary['fact_rows_before']} / {summary['fact_rows_after']}")
    print(f"  inserted            : {summary['rows_inserted']}")
    print(f"  updated             : {summary['rows_updated']}")
    print(f"  unchanged (skipped) : {summary['rows_unchanged']}")
    print(f"summary model rows    : {summary['summary_rows']}")
    print(f"max congress in batch : {summary['max_congress']}")
    print(f"watermark             : {summary['watermark']} (advanced: {summary['watermark_advanced']})")
    print(f"fact fingerprint      : {summary['fact_fingerprint']}")
    return 0


def cmd_quality(_: argparse.Namespace) -> int:
    config = Config.from_env()
    checks = latest_checks(config)
    print(format_table(checks))
    enforce(checks, fail_on_error=config.fail_on_quality_error)
    return 0


def cmd_state(_: argparse.Namespace) -> int:
    config = Config.from_env()
    with transaction(config) as cursor:
        watermark = get_watermark(cursor, PIPELINE_NAME)
        cursor.execute(
            """
            SELECT batch_id, status, rows_read, rows_staged, rows_rejected,
                   rows_inserted, rows_updated, max_congress, started_at, finished_at
            FROM etl.batch_run
            ORDER BY started_at DESC
            LIMIT 10
            """
        )
        batches = cursor.fetchall()
        cursor.execute("SELECT COUNT(*) FROM analytics.member_term")
        fact_rows = cursor.fetchone()[0]

    print(f"target          : {config.safe_target()}")
    print(f"pipeline        : {PIPELINE_NAME}")
    print(f"watermark       : {watermark if watermark is not None else '(none yet)'}")
    print(f"fact rows       : {fact_rows}")
    if not batches:
        print("\nNo batch runs recorded yet.")
        return 0
    print("\nRecent batches:")
    print(f"  {'BATCH_ID':<28} {'STATUS':<10} {'READ':>6} {'STAGED':>7} {'REJ':>5} {'INS':>6} {'UPD':>6} {'CONG':>5}")
    for b in batches:
        print(
            f"  {b[0]:<28} {b[1]:<10} {_n(b[2]):>6} {_n(b[3]):>7} {_n(b[4]):>5} "
            f"{_n(b[5]):>6} {_n(b[6]):>6} {_n(b[7]):>5}"
        )
    return 0


def _n(value: object) -> str:
    return "-" if value is None else str(value)


def cmd_examples(_: argparse.Namespace) -> int:
    config = Config.from_env()
    statements = [s.strip() for s in read_sql("005_example_queries.sql").split(";") if s.strip()]
    with transaction(config) as cursor:
        for index, statement in enumerate(statements, start=1):
            if not any(line.strip() and not line.strip().startswith("--") for line in statement.splitlines()):
                continue
            print(f"\n--- example query {index} ---")
            cursor.execute(statement)
            columns = [d[0] for d in cursor.description or []]
            print(" | ".join(columns))
            for row in cursor.fetchall():
                print(" | ".join(str(v) for v in row))
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="etl", description="Congressional demographics ETL")
    parser.add_argument("--log-level", default=None, help="DEBUG, INFO (default), WARNING, ERROR")
    subparsers = parser.add_subparsers(dest="command", required=True)

    init_db = subparsers.add_parser("init-db", help="create schemas, tables, constraints and indexes")
    init_db.set_defaults(func=cmd_init_db)

    fetch_cmd = subparsers.add_parser("fetch", help="download the source CSV")
    fetch_cmd.set_defaults(func=cmd_fetch)

    run = subparsers.add_parser("run", help="run the full pipeline")
    run.add_argument("--offline", action="store_true", help="never download; require a local source file")
    run.add_argument("--full-refresh", action="store_true", help="ignore the watermark and re-process every row")
    run.set_defaults(func=cmd_run)

    quality = subparsers.add_parser("quality", help="re-run the data-quality checks")
    quality.set_defaults(func=cmd_quality)

    state = subparsers.add_parser("state", help="show the watermark and recent batch runs")
    state.set_defaults(func=cmd_state)

    examples = subparsers.add_parser("examples", help="run the example analytical queries")
    examples.set_defaults(func=cmd_examples)

    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    configure_logging(args.log_level)
    try:
        return int(args.func(args))
    except (ConfigError, DatabaseUnavailable) as exc:
        logger.error("configuration or connection error", extra={"error": str(exc)})
        print(f"\n{type(exc).__name__}: {exc}", file=sys.stderr)
        return 2
    except QualityCheckFailed as exc:
        logger.error("data quality failure", extra={"error": str(exc)})
        print(f"\nData-quality failure: {exc}", file=sys.stderr)
        return 1
    except (FileNotFoundError, KeyError, RuntimeError, ValueError) as exc:
        logger.error("pipeline failure", extra={"error": str(exc), "error_type": type(exc).__name__})
        print(f"\n{type(exc).__name__}: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
