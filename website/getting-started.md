# Getting started

## Install

```bash
pip install scherlok
```

Requires Python 3.10+. Warehouse drivers are optional extras:

```bash
pip install "scherlok[bigquery]"    # BigQuery
pip install "scherlok[snowflake]"   # Snowflake
pip install "scherlok[mysql]"       # MySQL
pip install "scherlok[duckdb]"      # DuckDB (also needed for `scherlok demo`)
pip install "scherlok[dbt]"         # dbt project support
```

Or run the container image, which bundles every extra:

```bash
docker run --rm ghcr.io/rbmuller/scherlok:latest version
```

## Try it without a database

```bash
uvx --from "scherlok[duckdb]" scherlok demo
```

`scherlok demo` seeds a sample DuckDB warehouse in a temporary directory, learns a baseline, applies a "bad deploy" (60% of orders gone, e-mails nulled, free-text plans, a dropped column) and runs the real detectors. It never touches `~/.scherlok`. Add `--keep` to explore the files afterwards, or `--output json` to script it.

## Three commands on your own data

```bash
scherlok connect postgres://user:pass@host/db   # connect once
scherlok investigate                              # learn your data
scherlok watch                                    # detect anomalies
```

The first `watch` after `investigate` compares against that baseline. Every run saves a new profile, so the baseline keeps learning.

`scherlok ci <url>` runs connect, investigate and watch in one step and sets the exit code; see [CI/CD gate](ci-cd.md).

## Where things live

| What | Where | Override |
|---|---|---|
| Connection string | `~/.scherlok/config.json` | `SCHERLOK_CONNECTION` |
| Profiles and anomaly history | `~/.scherlok/profiles.db` (SQLite) | `scherlok config --store s3://...` or `SCHERLOK_STORE` |

Remote stores (`s3://`, `gs://`, `az://`) let CI runners and teammates share one baseline; the file is synced down before a run and back up after it.

## Everyday commands

```
scherlok status [--output json]    Health per table
scherlok history [--days N]        Past anomalies
scherlok report                    Detailed profile summary
scherlok dashboard --out report.html   Self-contained HTML report
scherlok config show               Current settings
```

Add `--verbose` for timings or `--quiet` to print only anomalies.
