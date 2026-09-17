# Connectors

| Database | Extra | Connection string |
|---|---|---|
| PostgreSQL | built in | `postgres://user:pass@host:5432/db` |
| BigQuery | `scherlok[bigquery]` | `bigquery://project-id/dataset-name` |
| Snowflake | `scherlok[snowflake]` | `snowflake://account/database/schema` (credentials via `SNOWFLAKE_USER`, `SNOWFLAKE_PASSWORD`, `SNOWFLAKE_WAREHOUSE`) |
| MySQL | `scherlok[mysql]` | `mysql://user:pass@host:3306/dbname` |
| DuckDB | `scherlok[duckdb]` | `duckdb:///path/to/file.db` or `duckdb:///:memory:` |

Every connector profiles through metadata and aggregate queries only (`information_schema`, `COUNT`, `AVG`, `STDDEV`, top values). Scherlok never copies rows out of your warehouse.

Want another warehouse? Databricks is tracked in [#37](https://github.com/rbmuller/scherlok/issues/37); the connector interface is five methods in `src/scherlok/connectors/base.py`.

## BigQuery notes

--8<-- "src/scherlok/connectors/bigquery.md"
