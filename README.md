<!-- mcp-name: io.github.rbmuller/scherlok -->
<div align="center">

<img src="https://img.shields.io/badge/python-3.10+-blue?logo=python&logoColor=white" alt="Python 3.10+">
<img src="https://img.shields.io/pypi/v/scherlok?color=green" alt="PyPI">
<a href="https://pepy.tech/project/scherlok"><img src="https://img.shields.io/pepy/dt/scherlok?color=blue&label=downloads" alt="PyPI downloads"></a>
<img src="https://img.shields.io/badge/license-MIT-blue" alt="MIT License">
<a href="https://github.com/rbmuller/scherlok/actions/workflows/ci.yml"><img src="https://github.com/rbmuller/scherlok/actions/workflows/ci.yml/badge.svg" alt="CI"></a>
<a href="https://glama.ai/mcp/servers/rbmuller/scherlok"><img src="https://glama.ai/mcp/servers/rbmuller/scherlok/badges/score.svg" alt="Glama score"></a>
<a href="https://registry.modelcontextprotocol.io/v0.1/servers?search=io.github.rbmuller/scherlok"><img src="https://img.shields.io/badge/MCP%20Registry-io.github.rbmuller%2Fscherlok-success?logo=anthropic" alt="MCP Registry"></a>
<a href="https://rbmuller.github.io/scherlok/"><img src="https://img.shields.io/badge/docs-rbmuller.github.io%2Fscherlok-blue?logo=materialformkdocs&logoColor=white" alt="Documentation"></a>

<br><br>

<img src="website/assets/scherlok-icon.svg" alt="Scherlok" width="96">

<h1>Scherlok</h1>

<h3><em>"Don't wait for your data pipelines to break."</em></h3>
<p>Scherlok: data reliability for the AI era.</p>

<p><strong>Zero-config anomaly detection for your database tables.</strong><br>
No YAML, no rules, no thresholds. Scherlok learns what "normal" looks like, then tells you when something changes.</p>

</div>

```bash
pip install scherlok
scherlok ci postgres://user:pass@host/db   # first run: baseline + new-table checks; every run after: drift
```

No database handy? The demo seeds one, learns it, breaks it, and catches it, in about a second:

```bash
uvx --from "scherlok[duckdb]" scherlok demo
```

<div align="center">

<img src="examples/demo.svg" alt="Scherlok Demo" width="700">

</div>

Works with **PostgreSQL, BigQuery, Snowflake, MySQL, DuckDB** and **dbt**. Alerts go to Slack, Discord, Teams, email, or your CI exit code.

---

## The Problem

Every data team has the same nightmare:

> A source API silently changes from **dollars to cents**. Revenue dashboards show wrong numbers for **3 weeks** before anyone notices.
>
> A column starts returning **NULLs**. A table stops updating. Row counts drop **40% on a Tuesday**. Nobody knows until the CEO asks why the report looks weird.

Current tools (Great Expectations, Soda, dbt tests) require you to **define what "correct" looks like** before you can detect what's wrong. Hundreds of rules. Dozens of YAML files. And you still miss things — because you can't write rules for problems you haven't imagined yet.

## What It Catches

| Anomaly | What Happened | Severity |
|---------|---------------|----------|
| **Volume drop** | Row count dropped 40% overnight | CRITICAL |
| **Volume spike** | 3x more rows than normal | WARNING |
| **Freshness alert** | Table hasn't updated in 12h (normally every 2h) | CRITICAL |
| **Schema drift** | Column removed or type changed | CRITICAL |
| **NULL surge** | NULL rate jumped from 2% to 45% | CRITICAL |
| **Distribution shift** | Column mean of a numeric column shifted 3+ standard deviations (Shewhart-style control limit) and at least 1% | INFO, WARNING above 5σ |
| **Cardinality explosion** | Status column went from 5 values to 500 | CRITICAL |
| **Broken new table** | First sighting, no baseline yet: the table is empty, a column is NULL in every row, or its `id` / `<table>_id` has duplicates | WARNING (CRITICAL under `verify --table`) |
| **Constant column** | First sighting: one value in 100+ non-NULL rows | INFO |

Every anomaly is auto-scored: **INFO**, **WARNING**, or **CRITICAL**. No thresholds to configure.

A table seen for the first time has nothing to compare against, so it gets the new-table checks instead, from the profile already collected. They report WARNING (INFO for a constant column), so a legitimately empty table doesn't fail `ci` with the default `--fail-on critical`; once the table has a baseline they never run again. `scherlok verify --table` treats the tables it names as just built, so an empty table, an all-NULL column or a duplicate key in one of them is CRITICAL.

## How It Works

Scherlok takes the opposite approach of rule-based tools: **learn first, then detect.**

```bash
scherlok connect postgres://user:pass@host/db   # connect once
scherlok investigate                              # learn your data
scherlok watch                                    # detect anomalies
```

Three commands. Five minutes. Done. (`scherlok ci <url>` runs all three in one step for pipelines.)

After five valid profiles, Scherlok learns per-metric variability from the
latest 30 profiles using robust historical baselines for volume, numeric mean
shifts, NULL rates, and distinct counts. During cold start or when history is
not usable, it keeps the conservative fixed defaults.

Every run is learned, except one that finds a CRITICAL anomaly on a table:
that table keeps its baseline, so the alert repeats on every run until the
data recovers, and the broken state never becomes the new normal. WARNING and
INFO findings are learned. When the change is intended, such as a planned
backfill, a person makes it the new normal with `scherlok accept` (below).

### 1. `investigate` — Learn the patterns

```bash
$ scherlok investigate

  Profiling 12 tables...
  ✓ users         — 45,231 rows, 8 columns
  ✓ orders        — 1,203,847 rows, 15 columns
  ✓ products      — 892 rows, 12 columns
  ...
  Done. Profiles saved.
```

Scherlok profiles every table: row counts, column types, NULL rates, value distributions, freshness cadence, cardinality. Stores everything locally in SQLite, as aggregates only: no value from your rows is saved ([what Scherlok stores](https://rbmuller.github.io/scherlok/getting-started/#what-scherlok-stores)).

### 2. `watch` — Detect anomalies

```bash
$ scherlok watch

  Checking 12 tables against learned profiles...

  🔴 CRITICAL  orders    volume_drop     Row count dropped 52% (1,203,847 → 578,412)
  🟡 WARNING   users     null_increase   Column "email": NULL rate 2.1% → 18.7%
  🔵 INFO      products  distribution    Column "price": mean shifted 3.2σ

  3 anomalies detected. Exit code: 1
```

### 3. Alert — Slack, CI/CD, or both

```bash
# Slack
scherlok watch --webhook https://hooks.slack.com/services/...

# Discord
scherlok watch --webhook https://discord.com/api/webhooks/...

# Microsoft Teams
scherlok watch --webhook https://outlook.office.com/webhook/...

# Any endpoint (generic JSON payload)
scherlok watch --webhook https://my-api.com/alerts

# Repeating watch on the container's local time (install optional extra first)
pip install 'scherlok[schedule]'
scherlok watch --schedule "0 6 * * *"
```

Auto-detects Slack, Discord, and Teams from the URL and formats the payload accordingly. Any other URL receives a generic JSON payload.

## CI/CD Integration

Use Scherlok as a data quality gate on pull requests. The GitHub Action checks the change against the stored baseline, fails the check on anomalies and keeps one comment on the pull request up to date with the table, column, metric, baseline, current value and severity of each:

```yaml
# .github/workflows/data-checks.yml
on: pull_request
permissions:
  contents: read
  pull-requests: write
jobs:
  scherlok:
    runs-on: ubuntu-latest
    steps:
      # build the change first, e.g. dbt run against a CI schema
      - uses: rbmuller/scherlok@v1
        with:
          connection: ${{ secrets.DATABASE_URL }}
          store: s3://my-bucket/scherlok/profiles.db
          tables: orders,customers
          fail-on: critical
```

The connection is passed through the environment and masked, the comment never holds a value read from a row, and fork pull requests, which get no secrets, are skipped with a notice. 📖 [GitHub Action guide →](https://rbmuller.github.io/scherlok/ci-cd/#github-action)

Anywhere else, the `ci` command does it in one line. If Scherlok detects a critical anomaly, the pipeline fails:

```bash
scherlok config --store s3://my-bucket/scherlok/profiles.db
scherlok ci "$DATABASE_URL" --webhook "$SLACK_WEBHOOK" --fail-on critical
```

`ci` learns every run without a CRITICAL anomaly, so after a failure on `--fail-on warning` the new state becomes the baseline and a retry passes. When a retry must keep failing until the data is fixed, as with an AI agent or a CI job that retries, gate with `verify`. It compares against the stored baseline and saves no profiles, no anomalies and no config, and it never uploads a remote store; it only records the run.

```bash
scherlok verify --table orders --fail-on critical   # exit 1 while orders is still broken
scherlok verify --output json                       # one JSON object on stdout
scherlok verify --record scherlok-runs.jsonl        # append the run record, e.g. a CI artifact
scherlok verify --against postgresql://db.example.com/shop --ignore volume  # a dev build vs production
```

`--against` compares the connection's tables, such as a dev build, with another database's baseline in the same store, such as production's, and saves nothing under either: a dropped column fails before the change ships. `--ignore volume` is for builds on sampled data: row counts are not compared, and the column checks only flag what a sample can't explain.

Tables without a baseline are listed under `tables_without_baseline`, never counted as verified. Those you name with `--table` count as just built: an empty table, a column NULL in every row or a duplicate key there is CRITICAL, so an agent can gate on a table it just created. Every `watch`, `ci`, `check` and `verify` run is recorded, passing or not: `scherlok history --runs` lists them. Every anomaly in the JSON also carries `column`, `metric`, `baseline`, `current`, `change_pct` and a fixed `hint`, so it can be acted on without parsing the message. 📖 [CI/CD gate guide →](https://rbmuller.github.io/scherlok/ci-cd/)

## Reconcile output against input

A table's own history can't catch a join that fans out on its first build, or a total that changes while the row count stays the same. `reconcile` compares a pipeline's output with its input, in the same database, and exits 1 when they don't add up:

```bash
scherlok reconcile --source raw.orders --target mart.orders \
  --key order_id --sum amount --tolerance 0.001
```

It reports `row_count_mismatch`, `duplicate_keys` (a key repeated in the target: join fan-out), `missing_keys`, `extra_keys` and `sum_mismatch` (totals apart by more than the absolute `--tolerance`), all CRITICAL, with `--output json` for CI. It needs no baseline and saves nothing, and it reports counts and totals only, never a key value. On BigQuery and Snowflake every run scans both tables in full. 📖 [Reconcile guide →](https://rbmuller.github.io/scherlok/reconcile/)

## Accepting a change

A CRITICAL reading is never learned, so an intended change (a new column, a planned backfill) keeps alerting until a person accepts it:

```bash
scherlok accept --table orders --reason "Backfilled 2025 orders"   # shows before/after, asks to confirm
scherlok accept --table orders --reason "..." --yes                # no prompt, for a workflow a person triggers
scherlok history --approvals                                       # who accepted what, and why
```

`accept` re-profiles the table, shows its rows, columns and anomalies against the current baseline, and on confirmation saves the new state as the baseline with one row in an append-only approvals table: who approved (`git config user.email`, else `$USER`), the reason, and the before/after summary. `--reason` is required.

To keep agents and scheduled runs from moving baselines at all, lock them: `scherlok config --baseline-locked` or `SCHERLOK_BASELINE_LOCKED=1`. Then only `accept` writes a baseline, including a new table's first one; `watch`, `ci`, `check`, `dbt` and the MCP tools compare without saving profiles, and `investigate` refuses. `accept` is deliberately not an MCP tool.

**This is a guardrail, not a security boundary.** An agent with shell access can run `scherlok accept` or edit the SQLite file itself. Real enforcement means running `verify` in CI against a store the agent has no credentials for, with `accept` run from a workflow a person triggers. 📖 [Accepting a change →](https://rbmuller.github.io/scherlok/getting-started/#accepting-a-change)

## Works with dbt

Already running dbt? Scherlok complements `dbt test` with **automatic** anomaly detection — no rules to write.

```bash
pip install scherlok[dbt]

# After `dbt run`, point Scherlok at your project
scherlok dbt --project-dir ./my_dbt_project
```

Scherlok reads `target/manifest.json`, discovers every materialized model (`table`, `incremental`, `view`), auto-resolves the connection from your `profiles.yml`, and profiles each model:

```
Investigating 4 dbt models in ./my_dbt_project (postgres)
  ✓ stg_customers                  (12,345 rows)
  ✓ stg_orders                     (98,765 rows)
  ✗ fct_orders                     CRITICAL: Row count dropped 42% (98,765 → 57,283)
  ✓ dim_customers_inc              (12,300 rows)

Summary: 4 profiled, 1 anomalies (1 critical, 0 warning)
```

Use it as a CI gate after `dbt run`:

```yaml
- run: dbt run --target prod
- run: scherlok dbt --project-dir . --target prod --fail-on critical
```

Or collapse both steps into one with the wrapper:

```yaml
- run: scherlok dbt-run-and-watch --project-dir . --target prod --fail-on critical
```

The wrapper runs `dbt run` by default and uses the successful model nodes recorded in
`target/run_results.json`, so partial runs profile only what dbt actually built. Use
`--build` to run `dbt build`; successful models are still profiled when a test failure
causes downstream models to be skipped on dbt's handled failure path (exit 1), while
the wrapper preserves `dbt build`'s exit code. Unhandled failures fail fast without
reading the artifact.

Both `dbt` and `dbt-run-and-watch` accept `--output json` for CI parsers — a single JSON document on stdout, nothing else.

**Supported adapters:** `postgres`, `bigquery`, `snowflake`, `mysql`, `duckdb`. For others, pass `--connection-string` explicitly.

📖 Full docs: [dbt integration guide →](src/scherlok/dbt/README.md)

## dbt Package — native tests

Prefer staying inside dbt? Install Scherlok as a dbt package for native data quality tests — no Python CLI needed.

```yaml
# packages.yml
packages:
  - git: https://github.com/rbmuller/scherlok.git
    revision: v1.5.0
```

Once the dbt Package Hub listing lands ([dbt-labs/hubcap#456](https://github.com/dbt-labs/hubcap/pull/456)), this becomes `package: rbmuller/scherlok` with `version: [">=1.0.0", "<2.0.0"]`.

```yaml
# schema.yml
models:
  - name: fct_orders
    tests:
      - scherlok.volume_anomaly:
          sensitivity: 3.0
      - scherlok.row_count_between:
          min_value: 100
    columns:
      - name: email
        tests:
          - scherlok.not_null_proportion:
              max_rate: 0.01
      - name: updated_at
        tests:
          - scherlok.recency:
              days: 2
```

**Tier 1 — Instant (no setup):** `not_null_proportion`, `row_count_between`, `recency`, `unique_proportion`

**Tier 2 — Auto-learning (Shewhart control limits):** `volume_anomaly`, `null_anomaly` — require the `scherlok_metrics` model to build baseline history.

📖 Full docs: [dbt package README →](models/_models.yml)

## HTML dashboard

![scherlok dashboard](assets/dashboard-screenshot.png)

```bash
scherlok dashboard --out report.html
```

One self-contained HTML file (~28 KB): KPIs, per-table incidents grouped with first-seen timestamps, `+`/`−`/`~` schema-drift diff, sparklines, and full anomaly history. Auto dark/light theme via `prefers-color-scheme`.

📖 Full docs: [dashboard guide →](src/scherlok/dashboard/README.md)

## Use it from an AI agent (MCP)

Let Claude Code / Claude Desktop run data-quality checks directly.

**Claude Desktop:** download `scherlok-<version>.mcpb` from the [latest release](https://github.com/rbmuller/scherlok/releases/latest) and open it. One click, one setting (your connection string, stored as a secret).

**Any other client:**

```bash
pip install scherlok   # scherlok-mcp ships built-in since v0.7.0
```

```json
{
  "mcpServers": {
    "scherlok": {
      "command": "scherlok-mcp",
      "env": { "SCHERLOK_CONNECTION": "postgresql://user:pass@host/db" }
    }
  }
}
```

The agent gets `list_tables`, `investigate`, `watch`, `status`, `history`, `check`, `verify` and `reconcile` as tools; `verify` is the read-only gate that never changes the baseline, so an agent that retries can't pass a broken table, and `reconcile` checks a table it built against the table it built it from. There is no `accept` tool: a person approves baseline changes, and with baselines locked no tool writes one. Credentials are resolved server-side (never passed by the model), every operation is read-only on the warehouse, and there's no arbitrary-SQL tool.

📖 Full docs: [MCP server guide →](src/scherlok/mcp/README.md)

## Check an agent's dbt changes (Claude Code)

An agent that finishes `dbt run` reports success: the SQL ran. The Claude Code plugin makes it check the data. After every `dbt run` or `dbt build`, a hook runs `scherlok verify` on the models that run built and hands Claude the findings; the `verify-data-pipeline` skill has it fix the cause, verify again, and leave accepting a new baseline to a person.

<!-- DEMO GIF PLACEHOLDER (#113): record it by following examples/claude_code_demo/README.md
("Recording the README GIF"), save it as assets/claude-code-demo.gif, then replace this comment with:
<div align="center"><img src="assets/claude-code-demo.gif" alt="Claude Code adds a join that fans out, Scherlok's hook reports it, Claude fixes the join and the check passes" width="700"></div>
-->

**Demo:** [a dbt project on DuckDB](examples/claude_code_demo/) where the agent adds a join that fans out, the hook fires, the agent fixes the join and the check passes.

```text
/plugin marketplace add rbmuller/scherlok
/plugin install scherlok@scherlok
```

The hook stays silent until Scherlok is installed and has a connection, and it never fails the session. Lock the baselines for agent sessions with `SCHERLOK_BASELINE_LOCKED=1`. Other coding agents get the same instructions from an [`AGENTS.md` snippet](integrations/AGENTS.snippet.md), and `scherlok verify --dbt-project .` scopes a check to the last dbt run anywhere, CI included. 📖 [Coding agents guide →](https://rbmuller.github.io/scherlok/agents/)

## AI-explained alerts (`--explain`)

Your alert says **what** broke. `--explain` adds **why** — and what to check next.

```bash
pip install 'scherlok[explain]'
export ANTHROPIC_API_KEY=sk-ant-...

scherlok watch --webhook https://hooks.slack.com/... --explain
```

When anomalies fire, Scherlok makes **one** Claude call for the whole batch and injects a short root-cause hypothesis into the same Slack/Discord/Teams/email/JSON alert:

<div align="center">
<img src="examples/demo-explain.svg" alt="scherlok watch --explain: anomalies table followed by the AI hypothesis panel" width="760">
</div>

Works on `watch`, `ci`, `check`, `dbt`, and `dbt-run-and-watch`. On dbt projects the hypothesis is **lineage-aware**: upstream parents from `manifest.json` go into the prompt, so cascading failures get traced to the source model instead of alerting on every downstream symptom.

- **What it costs** — one call per fired run (not per anomaly), Claude Haiku 4.5 by default: well under a cent per run (~$0.003). Override the model with `SCHERLOK_EXPLAIN_MODEL`. Runs with zero anomalies make no API call.
- **What it sends** — aggregates only: the anomaly type/severity/message strings already in your alert, dbt model names, detection timestamps. **Never** warehouse rows, cell values, or credentials — the test suite pins this as a contract.
- **How to turn it off** — it's opt-in; don't pass `--explain`. If the API call fails (no key, timeout, rate limit), the original alert is delivered unchanged with a one-line note. Alerting never blocks on the LLM.

📖 Full docs: [explainer guide →](src/scherlok/explainer/README.md)

## Email alerts

```bash
export SCHERLOK_SMTP_HOST=smtp.gmail.com
export SCHERLOK_SMTP_USER=alerts@company.com
export SCHERLOK_SMTP_PASSWORD=app-specific-password

scherlok watch --email team@company.com --email cto@company.com
```

## Connectors

```bash
# PostgreSQL
scherlok connect postgres://user:pass@host:5432/db

# BigQuery — see src/scherlok/connectors/bigquery.md for auth, billing, CI patterns
pip install scherlok[bigquery]
scherlok connect bigquery://project-id/dataset-name

# Snowflake
pip install scherlok[snowflake]
export SNOWFLAKE_USER=...
export SNOWFLAKE_PASSWORD=...
export SNOWFLAKE_WAREHOUSE=...
scherlok connect snowflake://account/database/schema

# MySQL
pip install scherlok[mysql]
scherlok connect mysql://user:pass@host:3306/dbname

# DuckDB
pip install scherlok[duckdb]
scherlok connect duckdb:///path/to/file.db
```

The connection string names the dataset or schema, and every table in it is monitored. To monitor only some, name them when you connect: `scherlok connect bigquery://project-id/dataset-name --table orders --table customers`. 📖 [Choosing the tables →](https://rbmuller.github.io/scherlok/getting-started/#choosing-the-tables)

| Database | Status |
|----------|--------|
| PostgreSQL | Available |
| BigQuery | Available |
| Snowflake | Available |
| MySQL | Available |
| DuckDB | Available |

## Remote Storage

Share profiles across CI runs and team members:

```bash
# AWS S3
scherlok config --store s3://my-bucket/scherlok/profiles.db

# Google Cloud Storage
scherlok config --store gs://my-bucket/scherlok/profiles.db

# Azure Blob Storage
scherlok config --store az://my-container/scherlok/profiles.db
```

One store can hold several databases: baselines are kept per database (adapter, host, port and path of the connection, never credentials), so `orders` in staging never shares a baseline with `orders` in production. 📖 [Where things live →](https://rbmuller.github.io/scherlok/getting-started/#where-things-live)

## How it compares

| | **Scherlok** | Elementary | Soda | Great Expectations | Monte Carlo |
|---|---|---|---|---|---|
| Open-source core | MIT | Apache-2.0 dbt package + CLI | Apache-2.0 Soda Core | Apache-2.0 GX Core | No (SaaS) |
| Config before detection starts | **None** | YAML per anomaly test | SodaCL YAML checks | Expectations you declare | Monitors configured in the product |
| Learns baselines in the free tier | **Yes, automatically** | Yes, with per-test config | No (needs Soda Library + Cloud) | No (validates declared expectations) | n/a |
| Works without dbt | **Yes** | No | Yes | Yes | Yes |
| Self-hosted | **Yes** | OSS yes; Cloud is managed | Core yes; Cloud is managed | Core yes; Cloud is managed | No |
| Pricing | **Free** | OSS free; Cloud by seats and environments | Core free; Cloud has a free plan | Core free; Cloud has a free Developer option | Quote-based |

Every claim links to the other tool's own documentation in the [full comparison](https://rbmuller.github.io/scherlok/comparison/).

## CLI Reference

```
scherlok connect <url> [--table T]  Connect to a database; --table monitors only those tables
scherlok investigate [--table T]  Profile the monitored tables (learn patterns)
scherlok watch [--table T] [-w <url>] [-e <email>]  Detect anomalies and alert
scherlok ci <url> [--table T] [opts]  All-in-one CI/CD command (connect + watch + exit code)
scherlok verify [--table T] [--dbt-project DIR] [--fail-on L] [--output json] [--record PATH] [--against SCOPE] [--ignore volume]  Read-only gate: compare, keep the baseline
scherlok reconcile --source S --target T [--key K] [--sum C] [--tolerance X] [--output json]  Check an output table against its input
scherlok accept --table T --reason "..." [--yes]  Make the current state the baseline, with a recorded approval
scherlok dbt [--project-dir .] [--output json]  Profile dbt models from manifest
scherlok dbt-run-and-watch [--build] [--output json]  Run dbt + profile in one step
scherlok status [--output json] Quick health dashboard
scherlok history [--days N] [--runs | --approvals] [--output json]  Past anomalies, every run, or every approval
scherlok report [--output json] Detailed profile summary
scherlok dashboard [--out .html] Generate self-contained HTML report
scherlok config --store <url>   Set remote storage
scherlok config --baseline-locked  Only `accept` writes baselines (--no-baseline-locked to undo)
scherlok config --output json   Is a connection configured (never printed), the store, the lock
scherlok demo [--keep] [--output json]  Self-contained demo on a sample DuckDB (no database needed)
scherlok version                Show version
```

## Install

```bash
pip install scherlok

# With BigQuery support
pip install scherlok[bigquery]
```

Requires Python 3.10+.

### Run via Docker

A pre-built image with every warehouse extra (`dbt`, `bigquery`, `snowflake`) is published to GitHub Container Registry on every release tag:

```bash
docker run --rm ghcr.io/rbmuller/scherlok:latest version
```

Mount your project directory and inject connection details the same way your CI does it; the entrypoint is the `scherlok` CLI:

```bash
docker run --rm \
  -v "$PWD:/work" -w /work \
  -e SCHERLOK_CONNECTION=postgres://... \
  ghcr.io/rbmuller/scherlok:latest watch
```

The image is built from `python:3.12-slim` and runs unprivileged (`USER scherlok`).

## Contributing

Contributions welcome! See [CONTRIBUTING.md](CONTRIBUTING.md).

We're especially looking for:
- New database connectors (e.g. Databricks — see [#37](https://github.com/rbmuller/scherlok/issues/37))
- Anomaly detection improvements
- Documentation and examples

## Star History

<a href="https://star-history.com/#rbmuller/scherlok&Date"><img src="https://api.star-history.com/svg?repos=rbmuller/scherlok&type=Date" alt="Star History Chart" width="600"></a>

## License

[MIT](LICENSE) — Developed by [Robson Bayer Müller](https://github.com/rbmuller)
