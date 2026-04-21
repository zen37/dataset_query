from __future__ import annotations

from typing import Any

import duckdb

from query_dataset.config import get_env, require_env
from query_dataset.results import QueryResult

AZURE_URI_MARKERS = (
    "abfss://",
    "adl://",
    "az://",
    "azure://",
    "wasb://",
    "wasbs://",
)
DATA_FORMAT_READERS = {
    "parquet": "read_parquet",
    "csv": "read_csv_auto",
    "json": "read_json_auto",
}
DEFAULT_VIEW_PATH_ENVS = {
    "telemetry": "DUCKDB_TELEMETRY_PATH",
    "errors": "DUCKDB_ERRORS_PATH",
    "maint": "DUCKDB_MAINT_PATH",
    "failures": "DUCKDB_FAILURES_PATH",
    "machines": "DUCKDB_MACHINES_PATH",
}


def quote_sql(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def get_non_empty_env(name: str) -> str | None:
    value = get_env(name)
    if value is None:
        return None

    stripped = value.strip()
    return stripped or None


def requires_azure(query_text: str) -> bool:
    lowered = query_text.lower()
    return any(marker in lowered for marker in AZURE_URI_MARKERS)


def normalize_data_source(raw_source: str | None) -> str | None:
    if raw_source is None:
        return None

    source = raw_source.strip().lower().replace("-", "_")
    aliases = {
        "azure": "adls",
        "azure_storage": "adls",
    }
    source = aliases.get(source, source)
    if source in {"local", "adls", "auto"}:
        return source

    raise SystemExit("Unsupported DUCKDB_DATA_SOURCE. Use one of: local, adls, auto")


def infer_data_format(path: str) -> str:
    lowered = path.lower().split("?", 1)[0]
    if lowered.endswith((".csv", ".csv.gz", ".tsv", ".tsv.gz")):
        return "csv"
    if lowered.endswith((".json", ".json.gz", ".jsonl", ".jsonl.gz", ".ndjson")):
        return "json"
    return "parquet"


def normalize_data_format(raw_format: str | None, path: str) -> str:
    if raw_format is None or raw_format.strip().lower() == "auto":
        return infer_data_format(path)

    data_format = raw_format.strip().lower().replace("-", "_")
    aliases = {
        "csv_auto": "csv",
        "json_auto": "json",
        "jsonl": "json",
        "ndjson": "json",
    }
    data_format = aliases.get(data_format, data_format)
    if data_format in DATA_FORMAT_READERS:
        return data_format

    raise SystemExit("Unsupported DUCKDB_DATA_FORMAT. Use one of: parquet, csv, json, auto")


def resolve_data_format(path: str, *, env_name: str = "DUCKDB_DATA_FORMAT") -> str:
    configured_format = get_non_empty_env(env_name)
    if configured_format is None and env_name != "DUCKDB_DATA_FORMAT":
        configured_format = get_non_empty_env("DUCKDB_DATA_FORMAT")

    return normalize_data_format(configured_format, path)


def reader_expression(path: str, data_format: str) -> str:
    reader = DATA_FORMAT_READERS[data_format]
    return f"{reader}({quote_sql(path)})"


def build_secret_sql() -> str:
    auth_mode = (get_env("ADLS_AUTH_MODE", "service_principal") or "").lower()
    account_name = get_env("AZURE_STORAGE_ACCOUNT")
    scope = get_env("ADLS_SCOPE")
    parts: list[str] = ["TYPE azure"]

    if auth_mode == "service_principal":
        parts.extend(
            [
                "PROVIDER service_principal",
                f"TENANT_ID {quote_sql(require_env('AZURE_TENANT_ID'))}",
                f"CLIENT_ID {quote_sql(require_env('AZURE_CLIENT_ID'))}",
                f"CLIENT_SECRET {quote_sql(require_env('AZURE_CLIENT_SECRET'))}",
            ]
        )
        if account_name:
            parts.append(f"ACCOUNT_NAME {quote_sql(account_name)}")
    elif auth_mode == "credential_chain":
        parts.append("PROVIDER credential_chain")
        if account_name:
            parts.append(f"ACCOUNT_NAME {quote_sql(account_name)}")

        chain = get_env("AZURE_CREDENTIAL_CHAIN")
        if chain:
            parts.append(f"CHAIN {quote_sql(chain)}")
    elif auth_mode == "connection_string":
        parts.append(f"CONNECTION_STRING {quote_sql(require_env('AZURE_CONNECTION_STRING'))}")
        if account_name:
            parts.append(f"ACCOUNT_NAME {quote_sql(account_name)}")
    elif auth_mode == "anonymous":
        parts.append("PROVIDER config")
        if account_name:
            parts.append(f"ACCOUNT_NAME {quote_sql(account_name)}")
    else:
        raise SystemExit(
            "Unsupported ADLS_AUTH_MODE. Use one of: service_principal, credential_chain, "
            "connection_string, anonymous"
        )

    if scope:
        parts.append(f"SCOPE {quote_sql(scope)}")

    return "CREATE OR REPLACE SECRET adls_secret (\n    " + ",\n    ".join(parts) + "\n);"


def resolve_default_data_path() -> str:
    data_source = normalize_data_source(get_non_empty_env("DUCKDB_DATA_SOURCE"))
    data_path = get_non_empty_env("DUCKDB_DATA_PATH")

    if data_source == "local":
        if data_path is None:
            raise SystemExit("DUCKDB_DATA_PATH is required when DUCKDB_DATA_SOURCE=local")
        return data_path

    if data_source == "adls":
        return data_path or require_env("ADLS_URI")

    return data_path or require_env("ADLS_URI")


def default_query(limit: int) -> str:
    configured = get_env("DUCKDB_SQL")
    if configured:
        return configured

    data_path = resolve_default_data_path()
    data_format = resolve_data_format(data_path)
    return f"SELECT * FROM {reader_expression(data_path, data_format)} LIMIT {limit};"


def configured_view_paths() -> dict[str, str]:
    paths: dict[str, str] = {}
    for view_name, env_name in DEFAULT_VIEW_PATH_ENVS.items():
        path = get_non_empty_env(env_name)
        if path is not None:
            paths[view_name] = path
    return paths


def create_configured_views(
    connection: duckdb.DuckDBPyConnection,
    view_paths: dict[str, str],
) -> None:
    for view_name, path in view_paths.items():
        data_format = resolve_data_format(path, env_name=f"DUCKDB_{view_name.upper()}_FORMAT")
        connection.execute(
            f"CREATE OR REPLACE TEMP VIEW {view_name} AS "
            f"SELECT * FROM {reader_expression(path, data_format)};"
        )


def result_from_cursor(cursor: Any) -> QueryResult:
    rows = [list(row) for row in cursor.fetchall()]
    columns = [column[0] for column in cursor.description] if cursor.description else []
    return QueryResult(columns=columns, rows=rows)


def apply_benchmark_settings(
    connection: duckdb.DuckDBPyConnection,
    *,
    cold_benchmark: bool,
    uses_azure: bool,
) -> None:
    if not cold_benchmark:
        return

    connection.execute("SET enable_external_file_cache = false;")
    if uses_azure:
        connection.execute("SET azure_context_caching = false;")


def run_query(
    query_text: str | None,
    limit: int,
    database: str | None = None,
    *,
    cold_benchmark: bool = False,
) -> QueryResult:
    final_query = query_text or default_query(limit)
    database_path = database or get_env("DUCKDB_DATABASE", ":memory:") or ":memory:"
    connection = duckdb.connect(database=database_path)
    view_paths = configured_view_paths()
    uses_azure = requires_azure(final_query) or any(
        requires_azure(path) for path in view_paths.values()
    )

    try:
        if uses_azure:
            connection.execute("INSTALL azure;")
            connection.execute("LOAD azure;")
            apply_benchmark_settings(connection, cold_benchmark=cold_benchmark, uses_azure=True)
            connection.execute(build_secret_sql())
        else:
            apply_benchmark_settings(connection, cold_benchmark=cold_benchmark, uses_azure=False)

        create_configured_views(connection, view_paths)
        cursor = connection.execute(final_query)
        return result_from_cursor(cursor)
    finally:
        connection.close()
