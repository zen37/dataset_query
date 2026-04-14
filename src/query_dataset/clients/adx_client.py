from __future__ import annotations

import json
from datetime import timedelta
from typing import Any

from azure.kusto.data import ClientRequestProperties, KustoClient, KustoConnectionStringBuilder
from azure.kusto.data._models import WellKnownDataSet

from query_dataset.config import require_env
from query_dataset.results import QueryResult


def build_connection() -> KustoClient:
    cluster_url = require_env("ADX_CLUSTER_URL")
    tenant_id = require_env("AZURE_TENANT_ID")
    client_id = require_env("AZURE_CLIENT_ID")
    client_secret = require_env("AZURE_CLIENT_SECRET")

    kcsb = KustoConnectionStringBuilder.with_aad_application_key_authentication(
        cluster_url,
        client_id,
        client_secret,
        tenant_id,
    )
    return KustoClient(kcsb)


def default_query(limit: int) -> str:
    return f".show tables | take {limit}"


def build_request_properties(*, cold_benchmark: bool) -> ClientRequestProperties | None:
    if not cold_benchmark:
        return None

    properties = ClientRequestProperties()
    properties.set_option("query_results_cache_max_age", timedelta(0))
    return properties


def parse_json_value(raw_value: Any) -> Any:
    if not isinstance(raw_value, str):
        return raw_value

    try:
        return json.loads(raw_value)
    except json.JSONDecodeError:
        return raw_value


def row_value(row: Any, column_name: str) -> Any:
    try:
        return row[column_name]
    except Exception:
        return None


def extract_extended_properties(response: Any) -> dict[str, Any]:
    extended_properties: dict[str, Any] = {}
    properties_table = next((table for table in response.tables if table.table_kind == WellKnownDataSet.QueryProperties), None)
    if properties_table is None:
        return extended_properties

    for row in properties_table:
        key = row_value(row, "Key")
        if key is None:
            continue
        extended_properties[str(key)] = parse_json_value(row_value(row, "Value"))

    return extended_properties


def extract_query_resource_consumption(response: Any) -> dict[str, Any]:
    status_table = next(
        (table for table in response.tables if table.table_kind == WellKnownDataSet.QueryCompletionInformation),
        None,
    )
    if status_table is None:
        return {}

    for row in status_table:
        if row_value(row, "EventTypeName") != "QueryResourceConsumption":
            continue
        payload = parse_json_value(row_value(row, "Payload"))
        return payload if isinstance(payload, dict) else {}

    return {}


def build_metadata(response: Any) -> dict[str, Any]:
    metadata: dict[str, Any] = {
        "result_cache_hit": False,
        "adx_hot_cache_hit_bytes": None,
        "adx_cold_cache_hit_bytes": None,
        "adx_bypass_bytes": None,
    }

    extended_properties = extract_extended_properties(response)
    metadata["result_cache_hit"] = "ServerCache" in extended_properties

    resource_consumption = extract_query_resource_consumption(response)
    cache_usage = resource_consumption.get("resource_usage", {}).get("cache", {})
    shard_cache = cache_usage.get("shards", {})

    if cache_usage.get("results_cache_origin"):
        metadata["result_cache_hit"] = True

    hot_cache = shard_cache.get("hot", {})
    cold_cache = shard_cache.get("cold", {})
    metadata["adx_hot_cache_hit_bytes"] = hot_cache.get("hitbytes")
    metadata["adx_cold_cache_hit_bytes"] = cold_cache.get("hitbytes")
    metadata["adx_bypass_bytes"] = shard_cache.get("bypassbytes")
    return metadata


def run_query(
    query_text: str | None,
    limit: int,
    database: str | None = None,
    *,
    cold_benchmark: bool = False,
) -> QueryResult:
    final_query = query_text or default_query(limit)
    database_name = database or require_env("ADX_DATABASE")
    client = build_connection()
    properties = build_request_properties(cold_benchmark=cold_benchmark)

    try:
        if final_query.lstrip().startswith("."):
            response = client.execute_mgmt(database_name, final_query, properties=properties)
        else:
            response = client.execute(database_name, final_query, properties=properties)

        table = response.primary_results[0]
        columns = [column.column_name for column in table.columns]
        rows = [[row[column] for column in columns] for row in table]
        metadata = build_metadata(response)
        return QueryResult(columns=columns, rows=rows, metadata=metadata)
    finally:
        close = getattr(client, "close", None)
        if callable(close):
            close()
