from __future__ import annotations

from urllib.parse import quote_plus

import duckdb

from query_dataset.config import get_env
from query_dataset.results import QueryResult


def build_connection_string(database: str | None = None) -> str:
    database_name = database or get_env("MOTHERDUCK_DATABASE", "") or ""
    connection_string = f"md:{database_name}"

    params: list[str] = []
    token = get_env("MOTHERDUCK_TOKEN")
    if token:
        params.append(f"motherduck_token={quote_plus(token)}")

    if (get_env("MOTHERDUCK_SAAS_MODE", "false") or "false").lower() == "true":
        params.append("saas_mode=true")

    attach_mode = get_env("MOTHERDUCK_ATTACH_MODE")
    if attach_mode:
        params.append(f"attach_mode={quote_plus(attach_mode)}")

    dbinstance_inactivity_ttl = get_env("MOTHERDUCK_DBINSTANCE_INACTIVITY_TTL")
    if dbinstance_inactivity_ttl:
        params.append(f"dbinstance_inactivity_ttl={quote_plus(dbinstance_inactivity_ttl)}")

    if params:
        return connection_string + "?" + "&".join(params)
    return connection_string


def default_query() -> str:
    return get_env("MOTHERDUCK_SQL", "SHOW DATABASES;") or "SHOW DATABASES;"


def run_query(query_text: str | None, limit: int, database: str | None = None) -> QueryResult:
    del limit
    final_query = query_text or default_query()
    connection = duckdb.connect(build_connection_string(database))

    try:
        cursor = connection.execute(final_query)
        rows = [list(row) for row in cursor.fetchall()]
        columns = [column[0] for column in cursor.description] if cursor.description else []
        return QueryResult(columns=columns, rows=rows)
    finally:
        connection.close()
