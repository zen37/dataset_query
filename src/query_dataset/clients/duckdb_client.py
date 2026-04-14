from __future__ import annotations

from typing import Any

import duckdb

from query_dataset.config import get_env, require_env
from query_dataset.results import QueryResult


def quote_sql(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


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


def default_query(limit: int) -> str:
    configured = get_env("DUCKDB_SQL")
    if configured:
        return configured

    adls_uri = require_env("ADLS_URI")
    return f"SELECT * FROM {quote_sql(adls_uri)} LIMIT {limit};"


def requires_azure(query_text: str) -> bool:
    lowered = query_text.lower()
    return any(marker in lowered for marker in ("abfss://", "azure://", "az://", "adl://"))


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
    uses_azure = requires_azure(final_query)

    try:
        if uses_azure:
            connection.execute("INSTALL azure;")
            connection.execute("LOAD azure;")
            apply_benchmark_settings(connection, cold_benchmark=cold_benchmark, uses_azure=True)
            connection.execute(build_secret_sql())
        else:
            apply_benchmark_settings(connection, cold_benchmark=cold_benchmark, uses_azure=False)

        cursor = connection.execute(final_query)
        return result_from_cursor(cursor)
    finally:
        connection.close()
