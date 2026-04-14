from __future__ import annotations

import argparse
import csv
import time
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from query_dataset.clients import adx_client, duckdb_client, motherduck_client
from query_dataset.config import (
    DEFAULT_QUERY_LIMIT,
    PROJECT_ROOT,
    get_env,
    get_int_env,
    load_dotenv,
    resolve_query_path,
)
from query_dataset.results import QueryResult, render_result

DEFAULT_METRICS_PATH = PROJECT_ROOT / "output" / "query_metrics.csv"
BASIC_METRICS_FIELDNAMES = [
    "timestamp_utc",
    "iteration",
    "engine",
    "query_source",
    "count",
    "total_elapsed_ms",
]
ADX_METRICS_FIELDNAMES = [
    *BASIC_METRICS_FIELDNAMES,
    "result_cache_hit",
    "adx_hot_cache_hit_bytes",
    "adx_cold_cache_hit_bytes",
    "adx_bypass_bytes",
]


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Query DuckDB, MotherDuck, or ADX from one CLI.")
    parser.add_argument(
        "--engine",
        choices=("duckdb", "motherduck", "adx"),
        default="duckdb",
        help="Engine to query.",
    )
    parser.add_argument("--sql", help="Inline SQL or KQL to run.")
    parser.add_argument("--file", help="Path to a .sql or .kql file. Relative paths resolve from the project root.")
    parser.add_argument("--count-sql", help="Optional inline SQL or KQL that returns a total row count in the first column.")
    parser.add_argument(
        "--count-file",
        help="Optional path to a .sql or .kql file that returns a total row count in the first column.",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=get_int_env("QUERY_LIMIT", DEFAULT_QUERY_LIMIT),
        help="Default row limit used by engine-specific fallback queries.",
    )
    parser.add_argument(
        "--database",
        help="Optional engine-specific database override: DuckDB file path, MotherDuck database, or ADX database.",
    )
    parser.add_argument(
        "--output",
        choices=("table", "json"),
        default=get_env("QUERY_OUTPUT", "table") or "table",
        help="Render output as a table or JSON.",
    )
    parser.add_argument(
        "--interval-seconds",
        type=float,
        default=0.0,
        help="Seconds to wait between runs. Values greater than 0 enable loop mode.",
    )
    parser.add_argument(
        "--iterations",
        type=int,
        help="Number of runs to execute. When omitted in loop mode, QUERY_LOOP_ITERATIONS from .env is used when set.",
    )
    parser.add_argument(
        "--metrics-file",
        help="CSV file to append per-run metrics to. Defaults to output/query_metrics.csv.",
    )
    parser.add_argument(
        "--no-print-results",
        action="store_true",
        help="Skip printing query results to stdout. Useful for looped monitoring runs.",
    )
    parser.add_argument(
        "--cold-benchmark",
        action="store_true",
        help="Disable DuckDB cache reuse and bypass opt-in ADX query-results cache for each run.",
    )
    return parser


def load_query_text(sql: str | None, file_path: str | None) -> str | None:
    if sql and file_path:
        raise SystemExit("Use either inline SQL or a file path for each query, not both.")

    if file_path:
        path = resolve_query_path(file_path)
        if not path.exists():
            raise SystemExit(f"Query file not found: {path}")
        return path.read_text(encoding="utf-8")

    return sql


def run_engine(
    engine: str,
    query_text: str | None,
    limit: int,
    database: str | None,
    *,
    cold_benchmark: bool,
) -> QueryResult:
    if engine == "duckdb":
        return duckdb_client.run_query(
            query_text,
            limit,
            database,
            cold_benchmark=cold_benchmark,
        )
    if engine == "motherduck":
        if cold_benchmark:
            raise SystemExit("--cold-benchmark is currently supported only with --engine duckdb or --engine adx")
        return motherduck_client.run_query(query_text, limit, database)
    return adx_client.run_query(
        query_text,
        limit,
        database,
        cold_benchmark=cold_benchmark,
    )


def resolve_metrics_path(raw_path: str | None) -> Path:
    if not raw_path:
        return DEFAULT_METRICS_PATH

    path = Path(raw_path).expanduser()
    if path.is_absolute():
        return path
    return PROJECT_ROOT / path


def extract_total_count(result: QueryResult) -> int:
    if not result.rows or not result.rows[0]:
        raise SystemExit("Count query did not return a value.")

    value = result.rows[0][0]
    try:
        return int(value)
    except (TypeError, ValueError) as exc:
        raise SystemExit(f"Count query must return an integer in the first column, got: {value!r}") from exc


def infer_main_count(engine: str, query_text: str | None, result: QueryResult) -> int | None:
    if not query_text:
        return None

    normalized = " ".join(query_text.lower().split())
    looks_like_count = (
        "count(" in normalized
        or "| count" in normalized
        or "summarize count()" in normalized
    )
    if not looks_like_count:
        return None

    if engine == "adx" and "count" not in normalized:
        return None

    if len(result.rows) != 1 or not result.rows[0]:
        return None

    value = result.rows[0][0]
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def get_default_loop_iterations() -> int | None:
    raw_value = get_env("QUERY_LOOP_ITERATIONS")
    if raw_value in (None, ""):
        return None

    try:
        parsed_value = int(raw_value)
    except ValueError as exc:
        raise SystemExit("QUERY_LOOP_ITERATIONS must be an integer when set") from exc

    if parsed_value <= 0:
        raise SystemExit("QUERY_LOOP_ITERATIONS must be >= 1 when set")

    return parsed_value


def seconds_to_milliseconds(seconds: float) -> float:
    return seconds * 1000.0


def format_interval_seconds(interval_seconds: float) -> str:
    return f"{interval_seconds:g}"


def stringify_metric_value(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, float):
        return f"{value:.3f}"
    return str(value)


def build_metrics_metadata(result: QueryResult) -> dict[str, Any]:
    return {
        "result_cache_hit": result.metadata.get("result_cache_hit", False),
        "adx_hot_cache_hit_bytes": result.metadata.get("adx_hot_cache_hit_bytes"),
        "adx_cold_cache_hit_bytes": result.metadata.get("adx_cold_cache_hit_bytes"),
        "adx_bypass_bytes": result.metadata.get("adx_bypass_bytes"),
    }


def metrics_fieldnames_for_engine(engine: str) -> list[str]:
    if engine == "adx":
        return ADX_METRICS_FIELDNAMES
    return BASIC_METRICS_FIELDNAMES


def validate_existing_metrics_header(metrics_path: Path, fieldnames: list[str]) -> None:
    if not metrics_path.exists():
        return

    with metrics_path.open("r", encoding="utf-8", newline="") as handle:
        reader = csv.reader(handle)
        existing_header = next(reader, None)

    if existing_header is None:
        return

    if existing_header != fieldnames:
        raise SystemExit(
            f"Metrics file {metrics_path} has an older header. Use a new metrics file path or remove the old file first."
        )


def append_metrics(
    metrics_path: Path,
    *,
    iteration: int,
    engine: str,
    query_source: str,
    count: int,
    total_elapsed_ms: float,
    result: QueryResult,
) -> None:
    metrics_path.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = metrics_fieldnames_for_engine(engine)
    validate_existing_metrics_header(metrics_path, fieldnames)
    write_header = not metrics_path.exists()
    utc_timestamp = datetime.now(UTC).strftime("%Y-%m-%d %H:%M:%S.%f")[:-3] + " UTC"

    row = {
        "timestamp_utc": utc_timestamp,
        "iteration": iteration,
        "engine": engine,
        "query_source": query_source,
        "count": count,
        "total_elapsed_ms": f"{total_elapsed_ms:.3f}",
    }
    if engine == "adx":
        metadata = build_metrics_metadata(result)
        row.update(
            {
                "result_cache_hit": stringify_metric_value(metadata["result_cache_hit"]),
                "adx_hot_cache_hit_bytes": stringify_metric_value(metadata["adx_hot_cache_hit_bytes"]),
                "adx_cold_cache_hit_bytes": stringify_metric_value(metadata["adx_cold_cache_hit_bytes"]),
                "adx_bypass_bytes": stringify_metric_value(metadata["adx_bypass_bytes"]),
            }
        )

    with metrics_path.open("a", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        if write_header:
            writer.writeheader()
        writer.writerow(row)


def validate_args(args: argparse.Namespace) -> None:
    if args.interval_seconds < 0:
        raise SystemExit("--interval-seconds must be >= 0")
    if args.iterations is not None and args.iterations <= 0:
        raise SystemExit("--iterations must be >= 1")


def main(argv: list[str] | None = None) -> int:
    load_dotenv()
    parser = build_parser()
    args = parser.parse_args(argv)
    validate_args(args)
    query_text = load_query_text(args.sql, args.file)
    count_query_text = load_query_text(args.count_sql, args.count_file)

    default_loop_iterations = get_default_loop_iterations()
    effective_iterations = args.iterations
    if effective_iterations is None and args.interval_seconds > 0:
        effective_iterations = default_loop_iterations

    loop_mode = args.interval_seconds > 0 or effective_iterations not in (None, 1)
    total_runs = effective_iterations if effective_iterations is not None else (1 if not loop_mode else None)
    metrics_path = resolve_metrics_path(args.metrics_file)
    query_source = args.file or "inline/default"
    iteration = 0

    try:
        while True:
            iteration += 1
            total_started = time.perf_counter()

            result = run_engine(
                args.engine,
                query_text,
                args.limit,
                args.database,
                cold_benchmark=args.cold_benchmark,
            )

            count = len(result.rows)
            if count_query_text:
                count_result = run_engine(
                    args.engine,
                    count_query_text,
                    args.limit,
                    args.database,
                    cold_benchmark=args.cold_benchmark,
                )
                count = extract_total_count(count_result)
            else:
                inferred_count = infer_main_count(args.engine, query_text, result)
                if inferred_count is not None:
                    count = inferred_count

            total_elapsed_ms = seconds_to_milliseconds(time.perf_counter() - total_started)

            if not args.no_print_results:
                if loop_mode:
                    print(f"Run {iteration} completed in {total_elapsed_ms:.3f}ms")
                render_result(result, args.output)
                print(f"Count: {count}")
                print(
                    f"Saved metrics for run {iteration} to {metrics_path} "
                    f"(count={count}, total={total_elapsed_ms:.3f}ms)"
                )

            append_metrics(
                metrics_path,
                iteration=iteration,
                engine=args.engine,
                query_source=query_source,
                count=count,
                total_elapsed_ms=total_elapsed_ms,
                result=result,
            )

            if total_runs is not None and iteration >= total_runs:
                break
            if args.interval_seconds <= 0:
                break
            time.sleep(args.interval_seconds)
    except KeyboardInterrupt:
        if args.no_print_results:
            if args.interval_seconds > 0:
                print(
                    f"{query_source} executed {iteration - 1} iterations at "
                    f"{format_interval_seconds(args.interval_seconds)}-second intervals."
                )
            else:
                print(f"{query_source} executed {iteration - 1} iterations.")
        print("Stopped by user.")
        return 130

    if args.no_print_results:
        if args.interval_seconds > 0:
            print(
                f"{query_source} executed {iteration} iterations at "
                f"{format_interval_seconds(args.interval_seconds)}-second intervals."
            )
        else:
            print(f"{query_source} executed {iteration} iterations.")

    return 0


def main_entry() -> int:
    return main()
