# Query Dataset

A small `uv`-managed toolkit for querying three engines from one project:

- DuckDB, including ADLS Gen2 over `abfss://`
- MotherDuck
- Azure Data Explorer (ADX)

## Layout

```text
query_dataset/
  .env
  .env.example
  pyproject.toml
  src/query_dataset/
    cli.py
    config.py
    results.py
    clients/
      duckdb_client.py
      motherduck_client.py
      adx_client.py
  queries/
    duckdb/
    motherduck/
    adx/
  output/
```

## Setup

```bash
uv sync
cp .env.example .env
```

Fill in the values you need in `.env`.

## Run queries

Shared CLI:

```bash
uv run query-dataset --engine duckdb --file queries/duckdb/simple.sql
uv run query-dataset --engine duckdb --file queries/duckdb/heavy.sql
uv run query-dataset --engine motherduck --file queries/motherduck/simple.sql
uv run query-dataset --engine motherduck --file queries/motherduck/heavy.sql
uv run query-dataset --engine adx --file queries/adx/simple.kql
```

DuckDB wrapper:

```bash
uv run python duckdb.py --file queries/duckdb/heavy.sql
uv run python duckdb.py --file queries/duckdb/local_simple.sql
uv run python duckdb.py --file queries/duckdb/local_heavy.sql
```

MotherDuck wrapper:

```bash
uv run python motherduck.py --file queries/motherduck/simple.sql
uv run python motherduck.py --file queries/motherduck/heavy.sql
```

ADX wrapper:

```bash
uv run python adx.py --file queries/adx/simple.kql
```

You can also pass inline queries:

```bash
uv run query-dataset --engine duckdb --sql "select 1 as ok"
uv run query-dataset --engine motherduck --sql "show databases"
uv run query-dataset --engine adx --sql ".show tables"
```

## DuckDB data sources

DuckDB can read local files or ADLS files with the same client. Direct SQL works as-is, so this local CSV query does not use Azure auth at all:

```bash
uv run python duckdb.py --sql "select count(*) from read_csv_auto('/Users/mihai/data/iot/PdM_telemetry.csv')"
```

For the default DuckDB query, configure one path and format in `.env`:

```env
DUCKDB_DATA_SOURCE=local
DUCKDB_DATA_FORMAT=csv
DUCKDB_DATA_PATH=/Users/mihai/data/iot/PdM_telemetry.csv
```

Supported `DUCKDB_DATA_FORMAT` values are `parquet`, `csv`, `json`, and `auto`. `auto` or an omitted format infers from the path extension and falls back to Parquet.

To keep using ADLS, keep the data source as `adls` and provide `ADLS_URI` plus the usual ADLS auth settings:

```env
DUCKDB_DATA_SOURCE=adls
DUCKDB_DATA_FORMAT=parquet
ADLS_URI=abfss://myaccount.dfs.core.windows.net/myfilesystem/path/*.parquet
ADLS_AUTH_MODE=credential_chain
AZURE_CREDENTIAL_CHAIN=cli;env
```

You can also register the Predictive Maintenance files as temp views before each query. These paths can be local files or ADLS URLs, and they use `DUCKDB_DATA_FORMAT` unless you set a per-view format such as `DUCKDB_TELEMETRY_FORMAT=csv`:

```env
DUCKDB_DATA_SOURCE=local
DUCKDB_DATA_FORMAT=csv
DUCKDB_TELEMETRY_PATH=/Users/mihai/data/iot/PdM_telemetry.csv
DUCKDB_ERRORS_PATH=/Users/mihai/data/iot/PdM_errors.csv
DUCKDB_MAINT_PATH=/Users/mihai/data/iot/PdM_maint.csv
DUCKDB_FAILURES_PATH=/Users/mihai/data/iot/PdM_failures.csv
DUCKDB_MACHINES_PATH=/Users/mihai/data/iot/PdM_machines.csv
```

With those view paths set, the local examples can run without hard-coded file paths in SQL:

```bash
uv run python duckdb.py --file queries/duckdb/local_simple.sql
uv run python duckdb.py --file queries/duckdb/local_heavy.sql
```

## MotherDuck prerequisites

MotherDuck access requires valid values in `.env`:

```env
MOTHERDUCK_DATABASE=your_database
MOTHERDUCK_TOKEN=...
```

Optional MotherDuck settings:

```env
MOTHERDUCK_SAAS_MODE=false
MOTHERDUCK_DBINSTANCE_INACTIVITY_TTL=0s
```

`MOTHERDUCK_DBINSTANCE_INACTIVITY_TTL=0s` reduces warm instance reuse between runs, but it is not a guaranteed full cache bypass signal like the ADX metrics path.

MotherDuck queries are expected to live in `.sql` files under `queries/motherduck/`. The current examples are `queries/motherduck/simple.sql` and `queries/motherduck/heavy.sql`.

To run the current MotherDuck query files directly:

```bash
cd /Users/mihai/dev/python/dataset_query
uv run python motherduck.py --file queries/motherduck/simple.sql
uv run python motherduck.py --file queries/motherduck/heavy.sql
```

## ADX prerequisites

ADX defaults to Azure CLI authentication, so first sign in with the Azure CLI:

```bash
az login
```

Then configure ADX in `.env`:

```env
ADX_AUTH_MODE=azure_cli
ADX_CLUSTER_URL=https://yourcluster.region.kusto.windows.net
ADX_DATABASE=your_database
```

If your account has access to multiple tenants, sign in to the right tenant before running queries:

```bash
az login --tenant your-tenant-id
```

The previous service-principal-secret flow is still available by setting `ADX_AUTH_MODE=service_principal_secret` and providing `AZURE_TENANT_ID`, `AZURE_CLIENT_ID`, and `AZURE_CLIENT_SECRET`.

ADX queries are expected to live in `.kql` files under `queries/adx/`. The default example is `queries/adx/simple.kql`.

To run the current ADX query file directly:

```bash
cd /Users/mihai/dev/python/dataset_query
uv run python adx.py --file queries/adx/simple.kql
```

## Loop a query and capture metrics

Loop mode is enabled with `--interval-seconds`. Each run appends a row to a CSV metrics file with a UTC timestamp in `YYYY-MM-DD HH:MM:SS.mmm UTC` format, the iteration number, engine, query source, a single `count` value, the total run time in milliseconds, and any engine-specific cache metadata.

The default loop count is configured in `.env` with `QUERY_LOOP_ITERATIONS`. If you omit `--iterations`, the CLI uses that value when `--interval-seconds` is set.

DuckDB wrapper example:

```bash
uv run python duckdb.py \
  --file queries/duckdb/heavy.sql \
  --interval-seconds 10 \
  --iterations 3 \
  --metrics-file output/duckdb_heavy_metrics.csv \
  --no-print-results
```

MotherDuck wrapper example:

```bash
uv run python motherduck.py \
  --file queries/motherduck/heavy.sql \
  --interval-seconds 10 \
  --iterations 3 \
  --metrics-file output/motherduck_heavy_metrics.csv \
  --no-print-results
```

ADX wrapper example:

```bash
uv run python adx.py \
  --file queries/adx/simple.kql \
  --interval-seconds 10 \
  --iterations 3 \
  --metrics-file output/adx_simple_metrics.csv \
  --no-print-results
```

With `--no-print-results`, the CLI stays quiet during the loop and prints one summary line at the end, for example:

```text
queries/motherduck/heavy.sql executed 3 iterations at 10-second intervals.
```

## Cold Benchmark Mode

Use `--cold-benchmark` when you want to reduce warm-run effects without changing the query text.

For DuckDB runs, this flag disables:

- `enable_external_file_cache`
- `azure_context_caching`

For ADX runs, this flag sends the `query_results_cache_max_age=0` request property so the query does not opt into Kusto query-results cache reuse.

ADX note: this does not disable ADX hot/data cache on the cluster, so repeated runs can still benefit from service-managed caching.

DuckDB example:

```bash
uv run python duckdb.py \
  --file queries/duckdb/heavy.sql \
  --interval-seconds 10 \
  --iterations 3 \
  --metrics-file output/duckdb_heavy_cold_metrics.csv \
  --no-print-results \
  --cold-benchmark
```

ADX example:

```bash
uv run python adx.py \
  --file queries/adx/simple.kql \
  --interval-seconds 10 \
  --iterations 3 \
  --metrics-file output/adx_simple_cold_metrics.csv \
  --no-print-results \
  --cold-benchmark
```

`--cold-benchmark` is currently supported with DuckDB and ADX. It is not currently supported with MotherDuck.

`--count-file` is optional and only needed if you add a separate count query file.

Useful options:

- `--iterations 5` to override the default loop count for one run
- set `QUERY_LOOP_ITERATIONS=10` in `.env` to control the default loop count
- set `QUERY_LOOP_ITERATIONS=` blank if you want loop mode to keep running until `Ctrl+C`
- omit `--metrics-file` to use `output/query_metrics.csv`
- use `--count-sql` or `--count-file` when you want the full matching row count logged too

The metrics CSV always contains these base columns:

- `timestamp_utc`
- `iteration`
- `engine`
- `query_source`
- `count`: for `COUNT(*)` queries, the actual count value; for ADX count operators like `| count`, the returned count value; otherwise the number of rows returned by the query
- `total_elapsed_ms`: full runtime for the run in milliseconds

ADX metrics files also include these extra cache columns:

- `result_cache_hit`: `true` when ADX served the result from query-results cache, `false` when it did not
- `adx_hot_cache_hit_bytes`: ADX shard-cache bytes served from hot cache
- `adx_cold_cache_hit_bytes`: ADX shard-cache bytes served from cold cache
- `adx_bypass_bytes`: ADX shard-cache bytes explicitly bypassed

If you reuse an older metrics file after a schema change, the CLI now stops with a clear message instead of appending mismatched columns. Use a new metrics filename or remove the old CSV first.

## Notes

- `duckdb.py`, `motherduck.py`, and `adx.py` are thin wrappers around the shared CLI.
- DuckDB is pinned to `1.4.x` because MotherDuck's official docs currently describe `1.4.1` support.
- MotherDuck uses the `md:` connection string with `MOTHERDUCK_TOKEN` authentication.
- ADX uses `azure-kusto-data` and defaults to Azure CLI authentication from your active `az login` session.
