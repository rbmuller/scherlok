# Scherlok

**Zero-config anomaly detection for your database tables.** No YAML, no rules, no thresholds. Scherlok learns what "normal" looks like, then tells you when something changes.

```bash
pip install scherlok
scherlok ci postgres://user:pass@host/db   # profiles on the first run, detects anomalies on every run after
```

No database handy? The demo seeds one, learns it, breaks it, and catches it, in about a second:

```bash
uvx --from "scherlok[duckdb]" scherlok demo
```

<img src="https://raw.githubusercontent.com/rbmuller/scherlok/main/examples/demo.svg" alt="scherlok demo: seed, investigate, bad deploy, watch" width="760">

## What it catches

| Anomaly | What happened | Severity |
|---|---|---|
| Volume drop | Row count dropped 40% overnight | CRITICAL |
| Volume spike | 3x more rows than normal | WARNING |
| Freshness | Table hasn't updated in 12h (normally every 2h) | CRITICAL |
| Schema drift | Column removed or type changed | CRITICAL |
| NULL surge | NULL rate jumped from 2% to 45% | WARNING |
| Distribution shift | Column mean moved 3+ standard deviations | INFO, WARNING above 5σ |
| Cardinality explosion | Status column went from 5 values to 500 | CRITICAL |

Every anomaly is auto-scored **INFO**, **WARNING** or **CRITICAL**. Nothing to configure.

## How it works

1. **`scherlok investigate`** profiles every table: row counts, column types, NULL rates, value distributions, freshness cadence, cardinality. Profiles are stored in SQLite, locally or in S3 / GCS / Azure Blob.
2. **`scherlok watch`** re-profiles and compares against the stored baseline. After five valid profiles it learns per-metric variability from the latest 30 runs (robust median/MAD bands); during cold start it uses conservative fixed thresholds.
3. **Alert** to Slack, Discord, Teams, e-mail, any webhook, or fail the CI pipeline with `--fail-on critical`.

Works with **PostgreSQL, BigQuery, Snowflake, MySQL, DuckDB**, with or without **dbt**.

## Where next

- [Getting started](getting-started.md): install, connect, first run.
- [dbt](dbt.md): run after `dbt run`, gate CI, lineage-aware alerts. Or stay inside dbt with the [dbt package](dbt-package.md).
- [Comparison](comparison.md): how Scherlok differs from Elementary, Soda, Great Expectations and Monte Carlo, with sources.
- [GitHub](https://github.com/rbmuller/scherlok): issues, discussions, source. MIT licensed.
