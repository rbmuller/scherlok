<div align="center">

# 🔍 Scherlok

**A detective for your data. Zero-config data quality monitoring.**

[![CI](https://github.com/rbmuller/scherlok/actions/workflows/ci.yml/badge.svg)](https://github.com/rbmuller/scherlok/actions/workflows/ci.yml)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)
[![Python](https://img.shields.io/badge/python-3.10+-blue.svg)](https://python.org)

</div>

---

```bash
pip install scherlok
scherlok connect postgres://user:pass@host/db
scherlok investigate    # profiles your data, learns the patterns
scherlok watch          # alerts when something drifts
```

No YAML. No rules to write. No 50-line config files.

Scherlok observes your data, learns what "normal" looks like, and tells you when something changes.

## The Problem

Every data team has the same nightmare: bad data reaching production undetected.

A source API silently changes from dollars to cents. A column starts returning NULLs. Row counts drop 40% on a Tuesday. Revenue dashboards show wrong numbers for 3 weeks before anyone notices.

Current tools require you to **define what "correct" looks like** before you can detect what's wrong. Hundreds of rules, tests, and assertions. And you still miss things — because you can't write rules for problems you haven't imagined yet.

## The Solution

Scherlok takes the opposite approach: **learn first, then detect.**

```bash
scherlok connect postgres://user:pass@host/db   # connect once
scherlok investigate                              # profile your data
scherlok watch --slack https://hooks.slack.com/...  # monitor and alert
```

Three commands. Zero config. Works in 5 minutes.

## What It Detects

| Anomaly | Example | Status |
|---------|---------|--------|
| **Volume drop/spike** | Row count dropped 40% or spiked 300% | ✅ |
| **Freshness alert** | Table hasn't updated in 12h (normally updates every 2h) | ✅ |
| **Schema drift** | Column added, removed, or type changed | ✅ |
| **Nullability shift** | NULL rate jumped from 2% to 45% | ✅ |
| **Distribution shift** | Column mean shifted 5+ standard deviations | ✅ |
| **Cardinality change** | Status column went from 5 unique values to 500 | ✅ |

Every anomaly is scored: **INFO**, **WARNING**, or **CRITICAL** — auto-calibrated, no thresholds to set.

## How It Works

### 1. Investigate

```bash
scherlok investigate
```

Profiles every table: row counts, column types, distributions, nullability patterns, freshness cadence, cardinality. Stores the profile locally.

### 2. Watch

```bash
scherlok watch
```

Compares current state against learned profiles. Detects deviations using statistical methods (z-score). No manual thresholds needed.

### 3. Alert

```bash
# Slack
scherlok watch --slack https://hooks.slack.com/...

# CI/CD (exits 1 on CRITICAL)
scherlok watch --exit-code
```

Alerts include: **what** changed, **when**, **how much** it deviated, and **suggested action**.

## Remote Storage

Profiles are stored locally by default (`~/.scherlok/profiles.db`). For CI/CD and shared environments, store them in the cloud:

```bash
# AWS S3
scherlok config --store s3://my-bucket/scherlok/profiles.db

# Google Cloud Storage
scherlok config --store gs://my-bucket/scherlok/profiles.db

# Azure Blob Storage
scherlok config --store az://my-container/scherlok/profiles.db
```

Scherlok downloads the profiles before each run and uploads them back after. Same SQLite engine, just synced to the cloud. Also supports the `SCHERLOK_STORE` environment variable.

## CI/CD Integration

Use Scherlok as a data quality gate in your pipeline:

```yaml
# GitHub Actions
- name: Check data quality
  run: |
    pip install scherlok
    scherlok connect ${{ secrets.DATABASE_URL }}
    scherlok config --store s3://${{ secrets.S3_BUCKET }}/scherlok/profiles.db
    scherlok watch --exit-code --fail-on critical
```

If Scherlok detects a critical anomaly, the pipeline fails. Bad data never reaches production. Profiles persist across runs via remote storage.

## Connectors

| Database | Status |
|----------|--------|
| PostgreSQL | ✅ Available |
| BigQuery | 🔜 Coming soon |
| Snowflake | 🔜 Coming soon |
| MySQL | 🔜 Planned |
| DuckDB | 🔜 Planned |

## Why Not [Other Tool]?

| | Great Expectations | Soda | Monte Carlo | Scherlok |
|---|---|---|---|---|
| Setup time | Hours | 30 min | Weeks | **5 minutes** |
| Config required | Hundreds of rules | YAML checks | Dashboard setup | **None** |
| Anomaly detection | Manual thresholds | Paid feature | Yes | **Yes, free** |
| Self-hosted | Yes | Limited | No (SaaS only) | **Yes** |
| Price | Free | Freemium | $50-200K/year | **Free** |

## Install

```bash
pip install scherlok
```

Requires Python 3.10+.

## Quick Start

```bash
# 1. Connect to your database
scherlok connect postgres://user:pass@localhost:5432/mydb

# 2. Profile your data
scherlok investigate

# 3. Check for anomalies
scherlok watch

# 4. See the profile summary
scherlok report
```

## CLI Reference

```bash
scherlok connect <url>        # Save database connection
scherlok config --store <url> # Set remote storage (s3://, gs://, az://)
scherlok investigate          # Profile all tables
scherlok watch                # Detect anomalies and alert
scherlok status               # Show table health overview
scherlok history              # Show timeline of past anomalies
scherlok report               # Show profile summary
scherlok version              # Show version
```

### When to use each command

| Command | Purpose | Analogy |
|---------|---------|---------|
| `status` | Quick health dashboard — OK/WARNING/CRITICAL per table | Glance at the car dashboard |
| `watch` | Full investigation — detects anomalies, saves history, sends alerts, returns exit code for CI/CD | Take the car to the mechanic |
| `history` | Timeline of all past anomalies — when, what, how severe | Check the car's service history |
| `report` | Detailed profile of each table — rows, columns, types, freshness | Read the car's spec sheet |

## Contributing

Contributions welcome! See [CONTRIBUTING.md](CONTRIBUTING.md) for guidelines.

We're especially looking for:
- New database connectors
- Anomaly detection improvements
- Documentation and examples

## License

[MIT](LICENSE) — Robson Bayer Müller, 2026
