# Connectors

| Database | Extra | Connection string |
|---|---|---|
| PostgreSQL | built in | `postgres://user:pass@host:5432/db`, or `…/db/schema` for a schema other than `public` |
| BigQuery | `scherlok[bigquery]` | `bigquery://project-id/dataset-name` |
| Snowflake | `scherlok[snowflake]` | `snowflake://account/database/schema` (credentials via `SNOWFLAKE_USER`, `SNOWFLAKE_PASSWORD`, `SNOWFLAKE_WAREHOUSE`) |
| MySQL | `scherlok[mysql]` | `mysql://user:pass@host:3306/dbname` |
| DuckDB | `scherlok[duckdb]` | `duckdb:///path/to/file.db` or `duckdb:///:memory:` |

Every connector profiles through metadata and aggregate queries only (`information_schema`, `COUNT`, `COUNT(DISTINCT)`, `AVG`, `STDDEV`, and `MAX` of a load-time column for freshness). Scherlok never copies rows out of your warehouse, and the profile store keeps aggregates only: see [What Scherlok stores](getting-started.md#what-scherlok-stores).

[`scherlok reconcile`](reconcile.md) works on every connector with the same aggregate-only queries, and accepts schema-qualified names (`raw.orders`). On BigQuery and Snowflake it scans both tables in full; see [its cost](reconcile.md#cost-on-bigquery-and-snowflake).

Want another warehouse? Databricks is tracked in [#37](https://github.com/rbmuller/scherlok/issues/37); the connector interface is five methods in `src/scherlok/connectors/base.py`.

## Freshness

Freshness compares how long ago a table changed (its age) with the age it usually reaches before its next load: WARNING at 2x, CRITICAL at 4x. Each load the recent runs saw counts once, at the oldest age a run read it, and the expected age is the upper quartile of those, at least 1 hour. A stall is one load however many runs it lasts, so it keeps alerting while five earlier loads remain among the latest 30 runs, and a table watched more often than it loads is not overdue on the eve of each load. Until five loads are among the latest 30 runs, the age is compared with the previous run's, and not at all when that was under an hour; a table whose 30 latest runs never hold five loads, such as a daily table watched hourly, stays on that rule.

Where the age comes from depends on the warehouse:

| Database | Read first | Then |
|---|---|---|
| BigQuery | the table's `last_modified_time` | the load-time column |
| Snowflake | `LAST_ALTERED` in `INFORMATION_SCHEMA.TABLES` | the load-time column |
| PostgreSQL | the load-time column | the latest vacuum or analyze in `pg_stat_user_tables`, which only moves after enough rows change |
| MySQL | the load-time column | `UPDATE_TIME` in `information_schema.TABLES`, cached for `information_schema_stats_expiry` (a day by default) |
| DuckDB | the load-time column | nothing: DuckDB keeps no modification time |

The load-time column is `MAX()` of one timestamp or date column, in one query: the first of `updated_at`, `modified_at`, `dbt_updated_at`, `loaded_at`, `_loaded_at`, `_etl_loaded_at`, `synced_at`, `_fivetran_synced`, `_airbyte_extracted_at`, `_sdc_batched_at`, `ingested_at`, `inserted_at` and `created_at` the table has, in any case. No other column is read: the age Scherlok saves gives the column's latest value away, and names such as `expires_at`, `deleted_at` or `last_login_at` say nothing about loads. A timestamp without a time zone is read as UTC; MySQL sessions run in UTC, so `TIMESTAMP` columns arrive in UTC. A column the warehouse can't aggregate, such as a BigQuery table that requires a partition filter, falls back to the metadata.

Each profile records where its age came from (`freshness_source`: `metadata` or `column`, and `freshness_column`), and a run is only compared with a run that read the same thing. The first run after an upgrade that switches a table to its column compares nothing, and the next one does. An alert based on a column names it: `(… 6.0x overdue; based on updated_at)`.

## BigQuery notes

--8<-- "src/scherlok/connectors/bigquery.md"
