<div align="center">

<img src="https://img.shields.io/badge/python-3.10+-blue?logo=python&logoColor=white" alt="Python 3.10+">
<img src="https://img.shields.io/pypi/v/scherlok?color=green" alt="PyPI">
<img src="https://img.shields.io/github/license/rbmuller/scherlok" alt="MIT License">
<a href="https://github.com/rbmuller/scherlok/actions/workflows/ci.yml"><img src="https://github.com/rbmuller/scherlok/actions/workflows/ci.yml/badge.svg" alt="CI"></a>

<br><br>

<img src="assets/logo.svg" alt="Scherlok" width="160">

<h1>Scherlok</h1>

<p><strong>Your data broke in production. Again.</strong><br>
Scherlok makes sure it doesn't happen next time.</p>

</div>

<div align="center">

<img src="examples/demo.svg" alt="Scherlok Demo" width="700">

**Zero config. Zero YAML. Zero rules to write.**<br>
Scherlok learns what "normal" looks like, then tells you when something changes.

</div>

---

## The Problem

Every data team has the same nightmare:

> A source API silently changes from **dollars to cents**. Revenue dashboards show wrong numbers for **3 weeks** before anyone notices.
>
> A column starts returning **NULLs**. A table stops updating. Row counts drop **40% on a Tuesday**. Nobody knows until the CEO asks why the report looks weird.

Current tools (Great Expectations, Soda, dbt tests) require you to **define what "correct" looks like** before you can detect what's wrong. Hundreds of rules. Dozens of YAML files. And you still miss things — because you can't write rules for problems you haven't imagined yet.

## The Solution

Scherlok takes the opposite approach: **learn first, then detect.**

```bash
scherlok connect postgres://user:pass@host/db   # connect once
scherlok investigate                              # learn your data
scherlok watch                                    # detect anomalies
```

Three commands. Five minutes. Done.

## What It Catches

| Anomaly | What Happened | Severity |
|---------|---------------|----------|
| **Volume drop** | Row count dropped 40% overnight | CRITICAL |
| **Volume spike** | 3x more rows than normal | WARNING |
| **Freshness alert** | Table hasn't updated in 12h (normally every 2h) | CRITICAL |
| **Schema drift** | Column removed or type changed | CRITICAL |
| **NULL surge** | NULL rate jumped from 2% to 45% | WARNING |
| **Distribution shift** | Column mean shifted 5+ standard deviations | WARNING |
| **Cardinality explosion** | Status column went from 5 values to 500 | CRITICAL |

Every anomaly is auto-scored: **INFO**, **WARNING**, or **CRITICAL**. No thresholds to configure.

## How It Works

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

Scherlok profiles every table: row counts, column types, NULL rates, value distributions, freshness cadence, cardinality. Stores everything locally in SQLite.

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
# Slack alerts
scherlok watch --slack https://hooks.slack.com/services/...

# CI/CD gate (fails pipeline on CRITICAL)
scherlok watch --exit-code --fail-on critical
```

## CI/CD Integration

Use Scherlok as a data quality gate:

```yaml
# GitHub Actions
- name: Data quality check
  run: |
    pip install scherlok
    scherlok connect ${{ secrets.DATABASE_URL }}
    scherlok watch --exit-code --fail-on critical
```

If Scherlok detects a critical anomaly, the pipeline fails. Bad data never reaches production.

## Connectors

```bash
# PostgreSQL
scherlok connect postgres://user:pass@host:5432/db

# BigQuery
pip install scherlok[bigquery]
scherlok connect bigquery://project-id/dataset-name
```

| Database | Status |
|----------|--------|
| PostgreSQL | Available |
| BigQuery | Available |
| Snowflake | Coming soon |
| MySQL | Coming soon |
| DuckDB | Planned |

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

## Why Not [Other Tool]?

| | Great Expectations | Soda | Monte Carlo | **Scherlok** |
|---|---|---|---|---|
| Setup time | Hours | 30 min | Weeks | **5 minutes** |
| Config required | Hundreds of rules | YAML checks | Dashboard setup | **None** |
| Anomaly detection | Manual thresholds | Paid feature | Yes | **Yes, free** |
| Self-hosted | Yes | Limited | No (SaaS) | **Yes** |
| CI/CD gate | Yes | Yes | No | **Yes** |
| Price | Free | Freemium | $50-200K/yr | **Free, forever** |

## CLI Reference

```
scherlok connect <url>          Connect to a database
scherlok investigate            Profile all tables (learn patterns)
scherlok watch                  Detect anomalies and alert
scherlok status                 Quick health dashboard
scherlok report                 Detailed profile summary
scherlok history [--days N]     Timeline of past anomalies
scherlok config --store <url>   Set remote storage
scherlok version                Show version
```

## Install

```bash
pip install scherlok

# With BigQuery support
pip install scherlok[bigquery]
```

Requires Python 3.10+.

## Contributing

Contributions welcome! See [CONTRIBUTING.md](CONTRIBUTING.md).

We're especially looking for:
- New database connectors (Snowflake, MySQL, DuckDB)
- Anomaly detection improvements
- Documentation and examples

## License

[MIT](LICENSE) — Developed by [Robson Bayer Müller](https://github.com/rbmuller)
