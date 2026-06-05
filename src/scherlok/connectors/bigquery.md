# BigQuery

Scherlok talks to BigQuery via the official `google-cloud-bigquery` client. It profiles tables in a dataset, learns their baseline, and flags drift on subsequent runs. This guide is for someone wiring Scherlok against a real GCP project — including the parts most quick-starts skip: what gets queried, what it costs, and where the current rough edges are.

## Install

```bash
pip install scherlok[bigquery]
```

The `[bigquery]` extra adds `google-cloud-bigquery>=3.0`. Authentication is handled by that client — no extra deps.

## Authentication

**v0.7.0 supports Application Default Credentials (ADC) only.** Service-account JSON via env var / file is on the roadmap (see [Limitations](#limitations) below). For day-to-day:

```bash
# Interactive (local dev)
gcloud auth application-default login

# CI / non-interactive: rely on Workload Identity Federation (preferred)
# or a service-account JSON key surfaced as ADC:
export GOOGLE_APPLICATION_CREDENTIALS=/path/to/sa-key.json
```

ADC discovery order is the standard `google-auth` library order: env var → gcloud config → metadata server. If the principal lacks `bigquery.dataViewer` on the dataset, `scherlok connect` returns a clear `permission denied — the authenticated identity lacks access to this dataset` error.

## Connect

```bash
scherlok connect bigquery://my-gcp-project/my_dataset
```

The format is `bigquery://<project-id>/<dataset>`. Scherlok stores the connection in `~/.scherlok/config.json` (or in `SCHERLOK_CONNECTION` for the MCP server flow). Connection is verified at this step — a successful `scherlok connect` means the credentials can list tables in the dataset.

## First run

```bash
# Profile every table in the dataset
scherlok investigate

# Run again later — Scherlok compares against the baseline
scherlok watch
```

The first `investigate` establishes the baseline. The next `watch` detects:

- Volume drops / spikes (z-score on row count)
- Schema drift (column added / removed / type changed)
- Freshness misses (table last-modified older than tolerance)
- NULL surge on any column
- Distribution shift (z-score on mean of numeric columns)
- Cardinality explosion (distinct count blowing up)

Output is one ✓/✗ line per table plus a summary. CI gating is `scherlok ci --fail-on critical` (or the `scherlok check` alias).

## What it costs (BigQuery billing transparency)

BigQuery charges by bytes scanned. **An `investigate` run is not free** — knowing what it costs upfront is part of being honest about production use. Per table, Scherlok issues roughly:

| Query | Source | Billed? |
|-------|--------|---------|
| `SELECT COUNT(*) AS cnt FROM <fqn>` | full table scan | **Yes — scans all rows** |
| Schema metadata | `INFORMATION_SCHEMA.COLUMNS` | No (metadata) |
| Last-modified | `__TABLES__` metadata | No |
| Per column: `AVG`, `STDDEV`, `MIN`, `MAX` | scans that column | **Yes** |
| Per column: top 5 values | scans that column with `GROUP BY` | **Yes** |

For a dataset with **T** tables averaging **C** columns each: ~`T + 2·T·C` billed scans per `investigate` / `watch`. A 10-table dataset with 100-column-wide tables ≈ 2,010 queries. Most are tiny because they read a single column, but plan accordingly.

**Recommendations for production:**

- Prototype on a single small table with `scherlok investigate --select <table>` before pointing at the full dataset (the [`--select` filter](../../../README.md) limits scope).
- Monitor cost via `region-X.INFORMATION_SCHEMA.JOBS_BY_USER` filtering `user_email = '<scherlok-sa-email>'` for the first week.
- Schedule `watch` on the cadence that matches your data — daily for slow-moving marts, hourly for ingest tables. Each run repeats the same scans.

A `maximum_bytes_billed` guard is on the roadmap to make per-query budget caps explicit. Until then, control cost via cadence + `--select` scoping.

## dbt + BigQuery

If you already use dbt with BigQuery, skip the `connect` step and let Scherlok read `profiles.yml`:

```bash
# After `dbt run`:
scherlok dbt --project-dir ./my_dbt_project
```

Scherlok auto-resolves the BQ connection from `profiles.yml`, discovers every materialized model (`table`/`incremental`/`view`/`materialized_view`), and profiles them. dbt-style ✓/✗ per model, with the lineage downstream-impact suffix on anomaly messages: *`Affects 3 downstream models: dim_revenue, mart_finance, dashboard_kpis`*.

Full dbt integration docs: [src/scherlok/dbt/README.md](../dbt/README.md).

## MCP + BigQuery (use it from Claude)

Since v0.7.0, `scherlok-mcp` ships built-in. Wire it to BigQuery for an agent that profiles and detects anomalies directly:

**Claude Desktop** — `~/Library/Application Support/Claude/claude_desktop_config.json`:

```json
{
  "mcpServers": {
    "scherlok": {
      "command": "scherlok-mcp",
      "env": {
        "SCHERLOK_CONNECTION": "bigquery://my-gcp-project/my_dataset",
        "GOOGLE_APPLICATION_CREDENTIALS": "/path/to/sa-key.json"
      }
    }
  }
}
```

Restart the client, then prompt: *"Use scherlok to profile every table on this connection and report any CRITICAL anomalies."* Claude calls `investigate` and `watch` directly and reasons over the structured results. The agent never sees the connection string or the SA key — both are resolved by the MCP server from the env block.

## CI/CD (GitHub Actions example)

```yaml
# .github/workflows/data-quality.yml
name: data-quality
on:
  schedule: [{cron: "0 6 * * *"}]
  workflow_dispatch:

jobs:
  scherlok-watch:
    runs-on: ubuntu-latest
    permissions:
      id-token: write   # Workload Identity Federation
      contents: read
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-python@v5
        with: { python-version: "3.12" }
      - uses: google-github-actions/auth@v2
        with:
          workload_identity_provider: projects/123456789/locations/global/workloadIdentityPools/scherlok/providers/github
          service_account: scherlok-runner@my-gcp-project.iam.gserviceaccount.com
      - run: |
          pip install scherlok[bigquery]
          scherlok config --store s3://my-bucket/scherlok/profiles.db  # shared baseline
          scherlok ci bigquery://my-gcp-project/my_dataset --fail-on critical \
                      --webhook ${{ secrets.SLACK_WEBHOOK }}
```

Notes for prod:

- **Profiles baseline** lives in `~/.scherlok/profiles.db` by default. For shared CI baselines across runs, persist it via `scherlok config --store s3://...` (the remote-storage layer is built-in).
- **Workload Identity Federation** beats long-lived SA keys; the workflow above shows the WIF pattern. If you must use a JSON key, mount it via the `auth` action's `credentials_json` input and Scherlok will pick it up through ADC.

## Limitations

These are honest gaps in v0.7.0, tracked openly so prospects can plan around them:

- **Auth: ADC only.** Service-account JSON works via `GOOGLE_APPLICATION_CREDENTIALS` (because google-auth treats that as ADC), but there's no native `--service-account-json` flag. A first-class flag is a likely v0.8 addition — track via a new issue if you need it.
- **Cost cap: not enforced.** No `maximum_bytes_billed` per query. Currently controllable via cadence + `--select` scoping. A `--max-bytes-billed` flag is the cleanest fix.
- **Single dataset per connection.** A connection string addresses one project + one dataset. Cross-dataset profiling needs multiple `connect` invocations or running `dbt` mode where the manifest spans datasets.
- **Region/location.** The connector lets `google-cloud-bigquery` discover the dataset's region. Multi-region datasets are fine; if you hit a region mismatch, the BQ error surfaces directly.

If any of these block your use case, open an issue describing the constraint — they're tracked under [help wanted](https://github.com/rbmuller/scherlok/issues?q=is%3Aopen+label%3A%22help+wanted%22).
