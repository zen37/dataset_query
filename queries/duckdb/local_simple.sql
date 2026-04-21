-- Requires DUCKDB_TELEMETRY_PATH to point at a local or ADLS telemetry file.
SELECT COUNT(*) AS total_rows
FROM telemetry;
