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
pip install "scherlok[schedule]"    # recurring cron watch
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

The first `watch` after `investigate` compares against that baseline. Every run saves a new profile, so the baseline keeps learning, except from a run with a CRITICAL anomaly.

## When the baseline does not learn

A run that finds a CRITICAL anomaly on a table saves the anomaly but none of that table's profiles. The table keeps its baseline, so the alert repeats on every run until the data recovers, and the broken state never becomes the reference. Recovery needs no manual step: once the data is back, the next run passes and is learned again. WARNING and INFO findings are learned, as are the other tables of the same run.

This holds for `watch`, `ci`, `check`, `dbt`, `dbt-run-and-watch` and the MCP `watch`, `check` and `investigate` tools. `verify` never saves profiles at all. When the change is intended, such as a planned backfill or a new column, re-profile with `scherlok investigate` to make it the new normal.

It includes `freshness_critical`. Freshness is compared with the previous run, so a table updated less often than it is watched, such as a weekly table watched daily, reports it on every run until its next update.

## Run watch on a schedule

Install `scherlok[schedule]`, configure the connection and baseline as above, then:

```bash
scherlok watch --schedule "0 6 * * *" --explain
```

This runs at the **next 06:00 in the host's local time**, then daily. An anomaly's normal
exit status is logged for each run; it does not stop subsequent runs. `SIGINT`
or `SIGTERM` requests shutdown after the current run finishes. Missed ticks are
not replayed after a slow run. Without `--schedule`, `watch` still runs once.

The Docker image includes the schedule extra. With a previously configured
`~/.scherlok` directory mounted into the container, for example:

```bash
docker run --rm -v "$HOME/.scherlok:/home/scherlok/.scherlok" \
  ghcr.io/rbmuller/scherlok:latest watch --schedule "0 6 * * *"
```

The container defaults to UTC unless its local timezone has been configured.

`scherlok ci <url>` runs connect, investigate and watch in one step and sets the exit code; see [CI/CD gate](ci-cd.md).

## Where things live

| What | Where | Override |
|---|---|---|
| Connection string | `~/.scherlok/config.json` | `SCHERLOK_CONNECTION` |
| Profiles and anomaly history | `~/.scherlok/profiles.db` (SQLite) | `scherlok config --store s3://...` or `SCHERLOK_STORE` |

Remote stores (`s3://`, `gs://`, `az://`) let CI runners and teammates share one baseline; the file is synced down before a run and back up after it.

One store can serve several databases. Each baseline belongs to the database it was learned from, so an `orders` table in staging and an `orders` table in production keep separate histories, and watching one never moves the other's baseline. `history`, `status`, `report` and `dashboard` show the configured connection's records only.

The database is identified by its *scope*: the adapter, host, port and path of the connection string, such as `postgresql://db.example.com/shop`, `snowflake://my-account/SHOP/PUBLIC` or `duckdb:///data/warehouse.duckdb`. The user, password, query string and fragment are never part of it. Spellings of one database share a scope: the adapter and host are lower-cased, `postgres://` counts as `postgresql://`, and the default port (5432, 3306) is dropped. Another host name or IP address for the same server is a new scope with a new baseline, so give every runner that shares a store the same connection string.

Records saved before scopes existed have none and keep counting for every connection, so an upgrade keeps its history; new records are saved under their scope. A runner still on an older release saves records without a scope, which count for every connection again, so upgrade every runner that shares a store.

## What Scherlok stores

`profiles.db` holds aggregates and metadata only. No value read from a row of your tables is saved:

| Record | Fields |
|---|---|
| Volume | row count, time of the run |
| Schema | column names, types, nullability |
| Freshness | the table's last-modified time as the warehouse reports it, hours since then |
| Distribution, per column | mean, standard deviation, NULL count, NULL rate, distinct count |
| Anomaly history | table, anomaly type, severity, message, detection time, id of the run that found it; the column involved, the metric with its baseline and current values (aggregates or column types), the change in percent and a fixed hint |
| Runs | command, Scherlok version, names of the tables checked, `--fail-on`, passed or failed, anomaly count per severity, start and end time |
| Scope, on every profile, anomaly and run | adapter, host, port and path of the connection, never credentials |

Anomaly messages quote the same aggregates and column names. Under `scherlok dbt` and `scherlok dbt-run-and-watch` they also name the downstream dbt models and exposures, with the exposure owners declared in your dbt project.

An aggregate can still give a value away on a tiny table: the mean of a one-row column is that row's value.

Older releases also saved each column's five most frequent values and its minimum and maximum (`top_values`, `min`, `max`). The first time an upgraded Scherlok opens a store, it deletes those fields and rewrites the file with SQLite's `VACUUM`, so they do not linger in free space; a remote store is cleaned on its next sync. Copies made before that still hold the values, including backups and older object versions in a versioned bucket: delete them yourself. A runner still on an older release that writes to the same store saves new values again, so upgrade every runner that shares it.

## Everyday commands

```
scherlok status [--output json]    Health per table
scherlok history [--days N]        Past anomalies
scherlok history --runs            Every run, passing or not
scherlok report [--output json]    Detailed profile summary
scherlok dashboard --out report.html   Self-contained HTML report
scherlok config show               Current settings
```

Add `--verbose` for timings or `--quiet` to print only anomalies.
