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

| Adapter | Status |
|---------|--------|
| `postgres` | ✅ |
| `bigquery` | ✅ |
| `snowflake` | ✅ |
| Others (Redshift, Databricks, DuckDB, …) | ❌ — use `--connection-string` |

For unsupported adapters, point Scherlok at the warehouse directly:

```bash
scherlok dbt --project-dir . --connection-string "postgresql://user:pw@host/db"
```

## Connection resolution

When `--connection-string` is **not** set, Scherlok resolves the connection from `profiles.yml`:

1. Reads `dbt_project.yml` to find the `profile:` name.
2. Looks the profile up in `profiles.yml` (search order: `--profiles-dir` → `$DBT_PROFILES_DIR` → `~/.dbt`).
3. Selects the target via `--target` or the profile's default `target:` key.
4. Renders `{{ env_var('NAME') }}` and `{{ env_var('NAME', 'default') }}` from the environment.
5. Builds a Scherlok connection string for the matched adapter.

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

## What's coming next (Month 5)

- GitHub Action wrapper (`uses: rbmuller/scherlok-action@v1`)
- `scherlok dbt run-and-watch` — runs `dbt run` and `scherlok dbt` in sequence
- ASCII lineage tree from `manifest.parent_map` / `child_map`
- JSON output for CI (`--output json`)

## Limitations of v0

- No support for ephemeral models (they're not materialized — nothing to profile).
- No support for adapter-specific Jinja in `profiles.yml` beyond `env_var`.
- Lineage is **not yet** read from the manifest — anomalies are reported per-model with no downstream impact.

If you hit one of these, please open an issue.
