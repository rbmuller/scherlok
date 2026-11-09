# dbt Integration

Scherlok reads dbt's `target/manifest.json` to discover models and run anomaly detection on each one — no rules to write, no YAML to maintain. It complements `dbt test`: where dbt tests check assertions you wrote, Scherlok detects drift you didn't think to check.

## Install

```bash
pip install scherlok[dbt]
```

The `[dbt]` extra adds PyYAML for parsing `profiles.yml`. The base scherlok install (`pip install scherlok`) is enough if you always pass `--connection-string` explicitly.

## Quick start

```bash
# After `dbt run` in your dbt project:
scherlok dbt --project-dir ./my_dbt_project
```

Scherlok will:
1. Read `target/manifest.json` and discover every materialized model
2. Resolve the connection from `profiles.yml` (or `--connection-string`)
3. Profile every model and detect anomalies against the stored baseline
4. Print one ✓/✗ line per model + a summary

First run = baseline. Subsequent runs detect drift.

## How it discovers what to profile

- **Materialized models only.** Models with `materialized: ephemeral` are skipped (they don't exist in the warehouse).
- **Tests and seeds are skipped** — only `resource_type: model` (and optionally `source` via `--include-sources`, `snapshot` via `--include-snapshots`) are profiled.
- **Filtering:** `--select fct_orders --select stg_orders` profiles only the listed models.

## Supported adapters

`profiles.yml` is resolved for `postgres`, `bigquery`, `snowflake`, `mysql` and `duckdb`. Any other adapter (Redshift, Databricks, …) works when you point Scherlok at the warehouse directly:

```bash
scherlok dbt --project-dir . --connection-string "postgresql://user:pw@host/db/analytics"
```

## Matching models to tables

A connection lists the tables of one database and schema: the PostgreSQL schema in the connection string (`postgresql://host/db/analytics`, `public` when left out), the MySQL database, DuckDB's `main` schema (the only one Scherlok reads), the BigQuery dataset or the Snowflake schema. A model matches a listed table when its alias does and dbt built it in that schema and database, compared without case when only one listed name fits. A model built in another schema, such as a custom `+schema` folder, is reported as not found instead of being matched to a table of the same name somewhere else.

## Connection resolution

When `--connection-string` is **not** set, Scherlok resolves the connection from `profiles.yml`:

1. Reads `dbt_project.yml` to find the `profile:` name.
2. Looks the profile up in `profiles.yml` (search order: `--profiles-dir` → `$DBT_PROFILES_DIR` → `~/.dbt`).
3. Selects the target via `--target` or the profile's default `target:` key.
4. Renders `{{ env_var('NAME') }}` and `{{ env_var('NAME', 'default') }}` from the environment.
5. Builds a Scherlok connection string for the matched adapter. For PostgreSQL it includes the target's `schema` unless that is `public`; for DuckDB, the `path`, relative to the project.

> Only `env_var` is rendered. Anything else (full Jinja, secrets resolvers, etc.) raises and asks you to pass `--connection-string`.

## CI/CD usage

Add Scherlok as a gate after `dbt run`:

```yaml
# .github/workflows/dbt.yml
- run: dbt run --target prod
- run: |
    pip install scherlok[dbt]
    scherlok config --store s3://my-bucket/scherlok/profiles.db
    scherlok dbt --project-dir . --target prod --fail-on critical \
                 --webhook ${{ secrets.SLACK_WEBHOOK }}
```

`--fail-on critical` exits with code 1 when any CRITICAL anomaly is detected; `--fail-on warning` is stricter and fails on WARNING+ as well.

For CI parsers, pass `--output json` to emit a single JSON document on stdout (status + per-model anomalies) instead of the human-readable text.

### One-shot wrapper: `scherlok dbt-run-and-watch`

If your CI step is just `dbt run` then `scherlok dbt`, collapse them into one invocation.
The wrapper uses `dbt run` by default:

```yaml
- run: |
    pip install scherlok[dbt]
    scherlok dbt-run-and-watch --project-dir . --target prod --fail-on critical
```

The wrapper streams `dbt run` output live. If `dbt run` exits non-zero, the wrapper propagates the exit code and skips the watch (a stale or partial manifest would surface noise, not signal). After a successful run, it reads that invocation's `target/run_results.json` and profiles only successfully built model nodes. `--project-dir`, `--target`, `--profiles-dir`, and `--select` are forwarded to `dbt run`; everything else stays scherlok-only.

Pass `--build` to run `dbt build` instead:

```yaml
- run: scherlok dbt-run-and-watch --project-dir . --target prod --build --fail-on critical
```

`dbt build` runs tests between resources, so a handled test failure (exit 1) can leave
successful upstream models profilable while downstream models are skipped. The wrapper
uses the same `run_results.json` filtering and profiles only successful `model.*` nodes;
it does not reimplement dbt selectors or DAG semantics. On exit 1, it profiles usable
results and still returns dbt's exact exit code. Exit 2 and any other unexpected
nonzero code fail fast without reading the artifact, preserving that exact code.
If the artifact is unavailable on the handled failure path, profiling is skipped and
the failure is reported with exit code 1.

If `run_results.json` is missing or malformed after a successful `dbt run`, the wrapper fails clearly instead of falling back to every model in the manifest. `--include-sources` and `--include-snapshots` remain explicit opt-ins and are not inferred from model execution results.

The wrapper also accepts `--output json`, matching `scherlok dbt --output json`:

```yaml
- run: |
    pip install scherlok[dbt]
    scherlok dbt-run-and-watch --project-dir . --target prod --fail-on critical --output json
```

Under `--output json`, the dbt command's own stdout is rerouted to stderr so it never mixes with the JSON payload. If `dbt run` fails, or `dbt build` exits 2 or another unexpected nonzero code, stdout gets a small JSON error document (`{"project_dir", "error", "returncode"}`) instead of plain text. If `dbt build` exits 1 but produces usable results, stdout contains the profiling JSON and the process still exits 1.

### Read-only check of the last run: `scherlok verify --dbt-project`

`scherlok dbt` and the wrapper learn: their results become the next baseline. To check what the last dbt run built without changing any baseline, as a gate or after an agent's change, use `verify`:

```bash
scherlok verify --dbt-project . --output json
```

It uses the same `run_results.json` filtering and model-to-table matching as the wrapper, takes the connection from `SCHERLOK_CONNECTION` or `scherlok connect` rather than `profiles.yml` (so any adapter Scherlok connects to works), and lists models the connection can't see under `dbt_models_not_found`. The [Claude Code plugin](https://rbmuller.github.io/scherlok/agents/) runs it after every `dbt run` and `dbt build` an agent runs.

## Lineage and downstream impact

Scherlok reads `parent_map` from `manifest.json` to know each model's place in the DAG. Two features use it:

```bash
scherlok dbt --project-dir . --show-lineage
```

prints an ASCII tree of upstream + downstream models under each profiled model's ✓/✗ line:

```
  ✓ stg_customers                  (12,345 rows)
    Upstream of stg_customers:
      stg_customers
      └── raw_customers
    Downstream of stg_customers:
      stg_customers
      ├── fct_orders
      └── dim_customers_inc
```

When an anomaly fires, the message is enriched with downstream impact so the alerter payload tells on-call which marts are about to drift:

```
✗ stg_customers  CRITICAL: Row count dropped 60.0% (1000 → 400) · Affects 2 downstream models: fct_orders, dim_customers_inc
```

When the manifest declares dbt exposures, exposure descendants are shown separately from models. The exposure label is preferred over its manifest name, and useful owner information is included when present:

```
✗ stg_customers  CRITICAL: Row count dropped 42% · Affects 2 downstream models: fct_orders, dim_customers_inc · Downstream exposure: Revenue Dashboard (owner: analytics-team@company.com)
```

Lineage continues to come from `parent_map`, so tests and other non-model resources may remain traversal nodes without being mislabeled as downstream models. Exposure owners are surfaced in the existing alert message only; Scherlok does not automatically notify those addresses unless the configured alert destination already reaches them. Multiple exposures use the same bounded preview convention as downstream models.

Leaf marts (no descendants) get no suffix.

## What's coming next

- GitHub Action wrapper (`uses: rbmuller/scherlok-action@v1`)

## Limitations of v0

- No support for ephemeral models (they're not materialized — nothing to profile).
- No support for adapter-specific Jinja in `profiles.yml` beyond `env_var`.

If you hit one of these, please open an issue.
