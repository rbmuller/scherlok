# Changelog

All notable changes to Scherlok are documented here.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added
- **GitHub Action: data checks on the pull request** — a failed gate was only a red job, so a reviewer had to dig through the log to learn what broke. `uses: rbmuller/scherlok@v1` (`action.yml` at the repository root, a composite action) runs `scherlok verify --output json` against the baseline in `store`, writes the job summary, fails the step when an anomaly reaches `fail-on`, and creates or updates one pull request comment (found again by a hidden marker) with the table, column, metric, baseline, current value, change and severity of each anomaly, falling back to the type and message for a Scherlok without the structured fields. Inputs: `connection`, `store`, `tables`, `fail-on`, `comment`, `footer` (the "Checked by Scherlok" line), `github-token`, `extras`, `scherlok-version` (pinned to the release of the action tag) and `install-from` (a local checkout, for testing the action); outputs: `result`, `anomaly-count`, `report` and `comment-url`. A missing `store` fails with an error instead of passing with nothing to compare. The connection reaches Scherlok through the environment, never the command line, and is masked in the log with its password and credential query parameters; the step holding the connection never gets the GitHub token, and the step posting the comment never gets the connection. The comment holds names, aggregates and fixed hints only, never a row value. Fork pull requests, which get no secrets, are skipped with a notice, and the docs warn against `pull_request_target` on untrusted code. Needs `contents: read` and `pull-requests: write` only. CI runs the action from the checkout against a DuckDB fixture, on passing and on broken data. The release and Docker workflows now run on `vX.Y.Z` tags only, so moving the action's `v1` tag publishes nothing. See [GitHub Action](https://rbmuller.github.io/scherlok/ci-cd/#github-action). ([#112](https://github.com/rbmuller/scherlok/issues/112))
- **`scherlok reconcile`, an output table checked against its input** — comparing a table with its own history can't see a join that fans out on the first build, or a total that changes while the row count stays the same. `scherlok reconcile --source raw.orders --target mart.orders --key order_id --sum amount --tolerance 0.001` compares the two tables in the same database and reports `row_count_mismatch`, `duplicate_keys` (a key on more than one row of the target), `missing_keys` (source keys the target lacks), `extra_keys` (target keys the source lacks) and `sum_mismatch` (totals apart by more than `--tolerance`), each CRITICAL with the structured fields and a fixed hint; any finding exits 1. `--key` and `--sum` repeat (composite keys, several totals); rows with a NULL key are counted apart and never matched; a total with no value counts as 0; `--tolerance` is absolute and inclusive, compared as decimals so a `DECIMAL` total is exact and a float total is taken at the value it prints as. Table names may be schema-qualified, and every identifier is quoted by the connector. `--output json` prints one object on stdout (`passed`, per-table `row_count`, `null_keys`, `distinct_keys`, `duplicate_keys`, `duplicate_rows` and `sums`, `missing_keys`, `extra_keys`, severity counts, `anomalies`). Only counts and totals are reported, never a key value, and a database error is reported by its type, not its text, which can quote a row value. Nothing is saved and no run is recorded. Connectors gain `get_key_stats`, `get_column_sums` and `count_unmatched_keys`, shared aggregate SQL quoted per dialect, for PostgreSQL, MySQL, DuckDB, BigQuery and Snowflake. On BigQuery and Snowflake every run scans both tables in full; see the [reconcile guide](https://rbmuller.github.io/scherlok/reconcile/). Agents get the same check as the read-only MCP tool `reconcile(source, target, key_columns, sum_columns, tolerance)`, which only accepts tables listed by `list_tables` and columns present in both. ([#111](https://github.com/rbmuller/scherlok/issues/111))
- **`scherlok accept`: baseline changes need a person's approval** — since a CRITICAL reading is no longer learned (see Changed), an intended change, such as a new column or a planned backfill, needs a way to become the new normal, and an agent should not be the one to approve its own work: the MCP `investigate` and `watch` tools wrote baselines with an ordinary tool call. `scherlok accept --table NAME [--table NAME ...] --reason "..."` profiles each table, shows how its state compares with the current baseline (rows and columns before and after, and the anomalies the new state shows), asks for confirmation (`--yes` skips it, for a workflow a person triggers) and saves the new profiles as the baseline. `--reason` is required. A table without a baseline gets its first one. Each table also gets one row in a new append-only `baseline_approvals` table (updates and deletes are refused by triggers): `table_name`, `scope`, `approved_by` (`git config user.email`, else `$USER`), `reason`, `before_summary` and `after_summary` (row and column counts, profiling time, and per anomaly its type, severity and structured fields; never a column value or a connection string) and `approved_at`. `scherlok history --approvals` lists them, with `--output json`. **Locked mode**, off by default: `scherlok config --baseline-locked` or `SCHERLOK_BASELINE_LOCKED=1` (any value but empty, `0`, `false`, `no` or `off`; the variable can't unlock a saved lock). When locked, only `accept` writes baselines, including the first one for a new table: `watch`, `ci`, `check`, `dbt`, `dbt-run-and-watch` and the MCP `watch` and `check` tools compare and still save anomalies and runs, but no profiles; `investigate` and MCP `investigate` refuse. `accept` is deliberately not an MCP tool. **This is a guardrail, not a security boundary**: an agent with shell access can run the CLI or edit the SQLite file. Real enforcement means running `verify` in CI against a store the agent has no credentials for, with `accept` run from a workflow a person triggers. The store gains the table through a migration. ([#109](https://github.com/rbmuller/scherlok/issues/109))
- **Structured anomaly fields** — an anomaly had four keys (`table`, `type`, `severity`, `message`), so the column and the numbers were only in the sentence and an agent had to parse it to learn what broke. Every anomaly now also carries `column`, `metric` (`row_count`, `column_type`, `hours_since_update`, `null_rate`, `null_count`, `mean`, `distinct_count` or `distinct_ratio`), `baseline` (the stored profile, or the learned median once there is enough history), `current`, `change_pct` (signed, one decimal) and `hint`, one fixed sentence per anomaly type on where to look first; a field that does not apply is `null`. `baseline` and `current` are aggregates or column types, never a value read from a row. The existing four keys and every message are unchanged. The fields appear in `verify`, `history`, `dbt` and `dbt-run-and-watch` with `--output json`, in `demo --output json`, and in the MCP `watch`, `verify` and `history` tools, whose output is still capped at 200 anomalies. They are saved in a new nullable `details` column of the anomaly history, added the first time a store is opened; existing rows are kept and read back with every field `null`, and a runner on an older release can still write to the same store. See [Structured anomaly fields](https://rbmuller.github.io/scherlok/ci-cd/#structured-anomaly-fields). ([#108](https://github.com/rbmuller/scherlok/issues/108))
- **New-table checks** — a table seen for the first time has no baseline, so nothing could fire, even on an empty table or a 1,000-row table whose `revenue` column is NULL in every row. Agents often create tables instead of changing existing ones, so the first run is when a check matters most. A table's first profile now gets four checks, computed from the profiles already collected (no extra query): `empty_table` (0 rows, WARNING), `all_null_column` (a column NULL in every row of a table with rows, WARNING), `duplicate_key` (`id` or `<singular table>_id` with fewer distinct values than non-NULL rows, WARNING; a foreign key such as `user_id` in `orders` is left alone) and `constant_column` (one value in 100+ non-NULL rows, INFO). Once a table has a baseline they never run again. `scherlok verify --table` and MCP `verify(tables=...)` treat the tables they name as just built, so their WARNING findings count as CRITICAL and fail the default gate; `investigate` still profiles without reporting anything. ([#107](https://github.com/rbmuller/scherlok/issues/107))
- **Every run is recorded, including runs that pass** — a run that found nothing left no trace, so there was no way to show that a check ran and passed. `watch` (each `--schedule` tick too), `ci`, `check`, `verify` and the MCP tools `watch`, `check` and `verify` now write one row to a new `runs` table: `run_id`, `command` (`mcp:<tool>` for the MCP tools), `scope`, `scherlok_version`, the names of the tables checked, `passed`, `fail_on`, counts per severity, `started_at` and `finished_at`. Each saved anomaly carries the `run_id` of the run that found it, also printed by `history --output json`. `scherlok history --runs` lists runs, with `--output json`. `verify` still saves no profiles or anomalies; it records its run in the local store only, since it never uploads a remote store, and `verify --record PATH` appends the same record as one JSON line, for CI to keep as an artifact. A record holds table names and counts only, never column values or connection strings. The store gains the table through a migration; existing anomalies keep an empty `run_id`. ([#115](https://github.com/rbmuller/scherlok/issues/115))
- **`scherlok verify`, a read-only gate** — compares the current state with the stored baseline and saves no profiles or anomalies, so retrying a failed check keeps failing until the data is fixed. Until now every path (`watch`, `ci`, `check`, MCP `watch` and `check`) saved each run, so the run after a failure passed against the broken state. `--table` (repeatable) limits scope, `--fail-on critical|warning` sets the exit code, and `--output json` prints one object on stdout (`passed`, `fail_on`, `tables_verified`, `tables_without_baseline`, `anomaly_count`, `critical`/`warning`/`info`, `anomalies`) with Rich output, including a one-line verdict when the gate fails, on stderr. Tables without a baseline are listed and never counted as verified (see the new-table checks above for when they fail the gate). The connection comes from `SCHERLOK_CONNECTION` or the saved config and is never written to it; a remote store is downloaded but never uploaded, so parallel jobs can't overwrite each other. Agents get the same gate as the read-only MCP tool `verify(tables, fail_on)`. ([#106](https://github.com/rbmuller/scherlok/issues/106))
- **`scherlok watch --schedule "0 6 * * *"`** — keeps the process alive and runs the regular watch path (alerts, `--explain`) on every five-field cron tick in the host's local time; the Docker image defaults to UTC. Each run's exit code is logged without stopping the loop, `SIGINT`/`SIGTERM` let the current run finish before exiting, and missed ticks are skipped rather than replayed. `croniter` ships behind the optional `scherlok[schedule]` extra (bundled in the Docker image); a missing `croniter` or an invalid expression exits 1 with a one-line hint. Thanks to [@bferanmi806-sketch](https://github.com/bferanmi806-sketch). ([#90](https://github.com/rbmuller/scherlok/issues/90), [#105](https://github.com/rbmuller/scherlok/pull/105))

### Changed
- **Behaviour change: a reading with a CRITICAL anomaly no longer becomes the baseline** — every run of `watch`, `ci`, `check`, `dbt`, `dbt-run-and-watch` and the MCP `watch`, `check` and `investigate` tools saved its profiles as the new baseline, so after a CRITICAL anomaly the broken state became the reference and the alert stopped although nothing was fixed: after one run on a table with a steady row count, after 15 on a table with natural variation, once broken readings filled half of the 30-run window. Now a run that finds a CRITICAL anomaly on a table saves the anomaly but none of that table's profiles, so the alert repeats on every run until the data recovers, which needs no manual step. WARNING and INFO findings are learned as before, and the other tables of the run are unaffected. To make an intended change the new normal, run `scherlok accept` (see Added). `freshness_critical` is the exception and is still learned: freshness compares a table's age with its age on the previous run, so a weekly table watched daily would otherwise alert on every run. `watch`, `ci`, `check` and `dbt` name the tables that kept their baseline, and MCP `investigate` returns `baseline_updated` per table. `verify` is unchanged. ([#116](https://github.com/rbmuller/scherlok/issues/116))
- **First runs can report warnings** — `watch`, `ci`, `check`, `dbt`, `dbt-run-and-watch` and the MCP `watch` and `check` tools used to report nothing for a table they had never seen. They now report the new-table checks above, which are saved to the anomaly history and sent to webhooks and e-mail like any other anomaly. They are WARNING or INFO, so the exit code under the default `--fail-on critical` is unchanged, but a gate on `--fail-on warning` now fails the first time it sees an empty table, an all-NULL column or a duplicate key. ([#107](https://github.com/rbmuller/scherlok/issues/107))
- **Baselines are kept per database** — profiles and anomalies were keyed by table name alone, so two databases with an `orders` table shared one baseline in the same store, and a run against staging moved the production baseline. Every profile and anomaly now carries a `scope` naming its database: the adapter, host, port and path of the connection string (`postgresql://db.example.com/shop`, `snowflake://my-account/SHOP/PUBLIC`, `duckdb:///data/warehouse.duckdb`), built without the user, password, query string or fragment, with `postgres://` read as `postgresql://` and the default port dropped. Comparisons, `history`, `status`, `report`, `dashboard` and the MCP tools use the configured connection's scope. Records saved before the upgrade have no scope and keep counting for every connection, so no history is lost; the store gains the column through a migration the first time it is opened. ([#110](https://github.com/rbmuller/scherlok/issues/110))
- **One `--fail-on` decision** — `ci`, `check`, `dbt`, `dbt-run-and-watch`, `verify` and the MCP `check` and `verify` tools now share one helper instead of three copies. An unknown value such as `--fail-on warnings` exits 1 with a hint before any work, where the CLI used to treat it silently as `critical`. MCP `watch` and `check` results also gain an `info` count. ([#106](https://github.com/rbmuller/scherlok/issues/106))

### Fixed
- **`ci <url>` and `check <url>` profile the database they were given** — with `SCHERLOK_CONNECTION` set, both connected to the URL argument but then profiled, and gated on, the database in the environment variable.
- **Connection strings are fully masked on display** — MCP `status` and the dashboard header showed a password containing an unencoded `/`, `?` or `#` in clear, because the host was cut short and nothing looked like a password. Credential query parameters such as `password` or `motherduck_token` were shown too. Both now come out as `***`, and the dashboard uses the same masking as the MCP server.
- **Column values no longer saved in the profile store** — every connector ran a top-5 `GROUP BY` and a `MIN`/`MAX` on each column, and `profiles.db` kept the results in plain text (`top_values`, `min`, `max`), for example e-mails, document numbers, salaries or birth dates. No detector read them. Both queries are gone (one fewer warehouse query per column) and the store now holds aggregates only; detection is unchanged. Existing stores are cleaned the first time they are opened: the three fields are deleted from every distribution profile and the file is rewritten with `VACUUM`, so they do not linger in free pages; a remote store is cleaned on its next sync. **Older copies of `profiles.db` still hold these values**, including backups and older object versions in a versioned bucket: delete them yourself. A runner still on an older Scherlok that writes to the same store adds new values again, so upgrade every runner that shares it. The store now records a schema version (`PRAGMA user_version`) and applies pending migrations when opened; a store written by a newer release is left untouched. See [What Scherlok stores](https://rbmuller.github.io/scherlok/getting-started/#what-scherlok-stores). ([#114](https://github.com/rbmuller/scherlok/issues/114))
- **`config.json` is owner-only** — `scherlok connect`, `config`, `ci <url>` and `dbt` save the connection string, password included, to `~/.scherlok/config.json`, which was written with the process umask and so was usually readable by every user on a shared machine or CI runner. The file is now created as `0600` rather than tightened after the write, an existing file is tightened on the next save, and a newly created `~/.scherlok` (or the directory of a custom store path) is `0700`. An existing directory keeps its mode. POSIX only; Windows is unchanged. ([#117](https://github.com/rbmuller/scherlok/issues/117))

## [1.0.5] — 2026-10-19

### Added
- **`scherlok report --output json`** — one JSON array on stdout (`table`, `volume`, `schema`, `freshness`, `anomaly_count` per table), `[]` for an empty database, Rich output on stderr; text mode unchanged. `report` was the last read command without a machine-readable mode. Thanks to [@bferanmi806-sketch](https://github.com/bferanmi806-sketch). ([#93](https://github.com/rbmuller/scherlok/issues/93), [#103](https://github.com/rbmuller/scherlok/pull/103))
- **Last anomaly in `scherlok status`** — a new column shows the most recent alert per table (`volume_drop · 2h ago`), and `--output json` gains `last_anomaly` (`type`, `severity`, `detected_at`, or `null`) with every existing key unchanged. Fetched in one window-function query for all tables. Thanks to [@bferanmi806-sketch](https://github.com/bferanmi806-sketch). ([#91](https://github.com/rbmuller/scherlok/issues/91), [#101](https://github.com/rbmuller/scherlok/pull/101))

## [1.0.4] — 2026-09-24

### Fixed
- **Cardinality noise on unique columns** — deleting rows also drops the distinct count of a unique `id` or timestamp column, so a single volume drop fired a `cardinality_change` warning per unique column on top of `volume_drop`. For columns the stored profile shows as near-unique (95% or more of non-NULL rows distinct), the detector now compares the distinct *ratio* rather than the absolute count, with thresholds sized for a bounded ratio (10% warning, 50% critical, inclusive). NULLs are excluded from the denominator, so nulling values in a unique column no longer reads as lost uniqueness — the nullability detector already reports that. Low-cardinality columns and callers without volume profiles keep the previous behaviour. Thanks to [@bferanmi806-sketch](https://github.com/bferanmi806-sketch). ([#94](https://github.com/rbmuller/scherlok/issues/94), [#97](https://github.com/rbmuller/scherlok/pull/97))

## [1.0.3] — 2026-09-22

### Added
- **Claude Desktop extension** — `mcpb/` builds `scherlok-<version>.mcpb`, attached to every GitHub release. Opening it installs the MCP server in one click and asks only for the connection string, which is stored as a secret and never passed by the model. The bundle is under 250 KB and vendors nothing: on first launch `uv` installs the release from PyPI (~90 MB of wheels, about four seconds on a cold cache, cached afterwards), so one bundle serves macOS, Windows and Linux. PostgreSQL, MySQL and DuckDB; BigQuery and Snowflake stay CLI-only because their drivers need a build toolchain on some platforms. `cryptography` is held below 50 in the bundle because 49.0.0 dropped x86_64 macOS wheels, which would break a one-click install on an Intel Mac. CI validates and packs the bundle on every PR, and `tests/test_mcpb.py` fails if the manifest version, the `scherlok` floor or the advertised tool list drift from the package.

## [1.0.2] — 2026-09-18

### Fixed
- **MCP server reported an empty version** — `serverInfo.version` was blank in every `initialize` reply because the package version was never passed to the server class, so clients and registries listed the server without a version. It now reports `__version__` (mcp 2.x; mcp 1.x `FastMCP` takes no version parameter and is unaffected).
- **Misleading error when an unsupported `mcp` is installed** — a release exposing neither `MCPServer` (mcp 2.x) nor `FastMCP` (mcp 1.x) produced "requires the 'mcp' package … re-install scherlok", which sends the operator down the wrong path. The message now names the installed version and the supported range. The `mcp` dependency is pinned to `>=1.2,<3` to match what is actually tested.

### Added
- **stdio handshake test** — `tests/test_mcp_server.py` now spawns the installed `scherlok-mcp` entry point and speaks JSON-RPC over stdio (initialize + tools/list), the way an MCP client or a registry health check does. This is the layer where the two fixes above failed while every in-process test passed.
- **`glama.json`** — declares repository maintainers for the Glama MCP listing.
- **Documentation site** at https://rbmuller.github.io/scherlok/ (MkDocs Material, deployed from `main` by `.github/workflows/docs.yml`, strict build checked in CI). Includes a sourced [comparison](https://rbmuller.github.io/scherlok/comparison/) with Elementary, Soda, Great Expectations and Monte Carlo; the README table now links to it and no longer quotes unpublished competitor prices.
- **`scherlok demo`** — a self-contained walkthrough that needs no database: seeds a sample DuckDB warehouse (3 tables, 22,500 rows), learns a baseline, ships a "bad deploy" (60% of orders gone, e-mails nulled, free-text plans, a dropped column) and catches it with the real detectors, in about a second. Runs entirely inside one temporary directory and never touches `~/.scherlok`; `--keep`/`--dir` preserve the files, `--output json` emits the result for scripts. Requires the `duckdb` extra: `uvx --from "scherlok[duckdb]" scherlok demo`.
- **`DuckDBConnector.close()`** — releases the file handle so another writer can open the database.
- **MCP Registry publishing workflow** — `.github/workflows/publish-mcp.yml` publishes `server.json` to the official MCP Registry after each successful Release run (GitHub OIDC, no stored token), so the `io.github.rbmuller/scherlok` listing no longer lags behind PyPI.

## [1.0.1] — 2026-09-17

### Fixed
- **PyPI release** — v1.0.0 was tagged and released on GitHub but never reached PyPI: the tag still carried `version = "0.9.0"` in `pyproject.toml`, so the publish job tried to re-upload 0.9.0 and PyPI rejected it with HTTP 400. 1.0.1 ships the 1.0.0 changes below with the version metadata (`pyproject.toml`, `dbt_project.yml`, `server.json`, `__version__`) corrected and aligned.

## [1.0.0] — 2026-09-03

### Added
- **Adaptive historical baselines** — after enough valid history, volume and column metric detectors learn per-metric variability with robust median/MAD bands; cold-start and unusable histories retain the existing fixed behavior. ([#62](https://github.com/rbmuller/scherlok/issues/62))
- **`scherlok status --output json`** — emits a JSON array of table health objects (`table`, `rows`, `columns`, `status`, `last_profiled`) for CI parsers. Rich output is routed to stderr in JSON mode so stdout stays machine-readable. ([#14](https://github.com/rbmuller/scherlok/issues/14))
- **`scherlok history --output json`** — emits a JSON array of anomaly records (`detected_at`, `severity`, `table`, `type`, `message`). Empty history returns `[]` instead of a text message. ([#15](https://github.com/rbmuller/scherlok/issues/15))
- **`scherlok dbt-run-and-watch --output json`** — the wrapper now accepts the same `--output json` flag as `scherlok dbt`, so CI users get the wrapper's convenience and a machine-readable stdout payload in one step. `dbt run`'s own stdout is rerouted to stderr in JSON mode so it never mixes with the payload; if `dbt run` fails, stdout gets a small JSON error document (`project_dir`, `error`, `returncode`) instead of plain text. ([#47](https://github.com/rbmuller/scherlok/issues/47))
- **Targeted `dbt-run-and-watch` profiling** — after a successful `dbt run`, the wrapper reads `target/run_results.json` and profiles only successful model nodes from that invocation. Missing or malformed artifacts fail clearly rather than falling back to the full manifest. ([#69](https://github.com/rbmuller/scherlok/issues/69))
- **`dbt build` support in `dbt-run-and-watch`** — pass `--build` to run `dbt build`; on dbt's handled failure path (exit 1), successful model nodes from `run_results.json` are profiled even when tests fail and downstream models are skipped, while unhandled failures fail fast and preserve dbt's exit code. ([#76](https://github.com/rbmuller/scherlok/issues/76))
- **Exposure-aware dbt lineage in anomaly alerts** — downstream models remain identified separately from dbt exposures, whose labels and owner information are surfaced in the existing alert message without changing notification routing. ([#70](https://github.com/rbmuller/scherlok/issues/70))
- **Selective column profiling in dbt package** — the `scherlok_column_metrics` model now scans `graph.nodes` for `null_anomaly` test nodes and profiles only the columns that have tests attached, reducing warehouse queries on wide tables. ([#71](https://github.com/rbmuller/scherlok/issues/71))

### Fixed
- **Clean JSON stdout on dbt early exits** — `console.print` calls that leaked Rich markup to stdout on connection failures and missing manifests now use `out_error` so they route to stderr in `--output json` mode. ([#67](https://github.com/rbmuller/scherlok/issues/67))

## [0.9.0] — 2026-08-10

### Added
- **dbt package** — Scherlok is now installable as a native dbt package (`git:` install from this repo; the dbt Package Hub listing is pending in dbt-labs/hubcap#456). Includes 4 instant tests (`not_null_proportion`, `row_count_between`, `recency`, `unique_proportion`) and 2 auto-learning anomaly detection tests (`volume_anomaly`, `null_anomaly`) backed by Shewhart control limits. Incremental `scherlok_metrics` and `scherlok_column_metrics` models auto-discover and profile all materialized models. One repo, two distribution channels (PyPI + dbt Hub). ([#64](https://github.com/rbmuller/scherlok/pull/64))

### Fixed
- **mcp 2.0 compatibility** — `FastMCP` was renamed to `MCPServer` in mcp 2.0. Import now tries v2 first, falls back to v1. ([#64](https://github.com/rbmuller/scherlok/pull/64))
- **MySQL/DuckDB view discovery** — `list_tables()` now includes views alongside base tables, matching the behavior of Postgres, BigQuery, and Snowflake connectors. ([#63](https://github.com/rbmuller/scherlok/pull/63))

## [0.8.0] — 2026-07-10

### Added
- **`--explain` — Claude-augmented alerts** on `watch`, `ci`, `check`, `dbt`, and `dbt-run-and-watch`. When anomalies fire, one Anthropic API call per run (Claude Haiku 4.5 by default, `SCHERLOK_EXPLAIN_MODEL` to override) turns the batch into a root-cause hypothesis — summary, likely cause, up to three diagnostic steps — injected into the Slack (attachment), Discord/Teams (text), generic-JSON (`explanation` field), email, and console alerts, plus an `explanation` key on `dbt --output json`. dbt runs feed upstream lineage from `manifest.json` into the prompt so cascading failures trace to the source model. Aggregate data only (anomaly strings, model names, timestamps — never warehouse rows); fail-open by contract: any API failure delivers the unaugmented alert with a one-line `(--explain unavailable: …)` note, injected text is escaped and truncated per platform so an oversized hypothesis can never get an alert payload rejected, and the alert path is capped at ~30s worst case. Opt-in via `pip install scherlok[explain]` + `ANTHROPIC_API_KEY`. See [`src/scherlok/explainer/README.md`](src/scherlok/explainer/README.md). ([#58](https://github.com/rbmuller/scherlok/issues/58), [#59](https://github.com/rbmuller/scherlok/pull/59))
- **`--explain` demo recording** — `examples/demo-explain.svg` (asciinema-style cast of a real run), embedded in the README section.

## [0.7.0] — 2026-05-28

### Added
- **MCP server** — `pip install scherlok` now ships a `scherlok-mcp` stdio server that exposes Scherlok to AI coding agents (Claude Code, Claude Desktop, …) as MCP tools: `list_tables`, `investigate`, `watch`, `status`, `history`, `check`. The connection is resolved server-side (`SCHERLOK_CONNECTION` / `scherlok config`) and never passed by the model; every operation is read-only on the warehouse with no arbitrary-SQL tool, and output is bounded. See [`src/scherlok/mcp/README.md`](src/scherlok/mcp/README.md). ([#54](https://github.com/rbmuller/scherlok/issues/54), [#55](https://github.com/rbmuller/scherlok/pull/55))
- **`server.json` + PyPI ownership marker** — repo-root `server.json` and an `mcp-name: io.github.rbmuller/scherlok` marker in the README so Scherlok can be published to the official MCP Registry (registry.modelcontextprotocol.io). ([#56](https://github.com/rbmuller/scherlok/pull/56))

### Changed
- **`mcp` is now a core dependency** (moved out of the `[mcp]` extra) so the registry's `pip install scherlok` install path produces a working `scherlok-mcp` without an extra step. The `[mcp]` extra is kept as a back-compat alias and still resolves cleanly. This adds ~15 transitive packages (anyio, pydantic, httpx, starlette, uvicorn family) to the base install; the trade was deliberate — first-class agent integration over a lean install.
- Extracted the per-table profile-and-detect orchestration into `scherlok.service` so the CLI and the MCP server share one transport-agnostic core (no behavior change).

## [0.6.0] — 2026-05-20

### Added

#### Connectors
- **MySQL connector** — `pip install scherlok[mysql]`, `scherlok connect mysql://user:pass@host:3306/db`. Works with MariaDB and other compatible forks. ([#45](https://github.com/rbmuller/scherlok/pull/45), closes [#18](https://github.com/rbmuller/scherlok/issues/18))
- **DuckDB connector** — `pip install scherlok[duckdb]`, `scherlok connect duckdb:///path/to/file.db` (or `duckdb:///:memory:`). For local-first analytics workflows. ([#50](https://github.com/rbmuller/scherlok/pull/50), closes [#19](https://github.com/rbmuller/scherlok/issues/19))

#### dbt integration
- **`scherlok dbt-run-and-watch`** — wraps the typical CI sequence (`dbt run` -> `scherlok dbt`) into one invocation. Streams `dbt run` output live; if `dbt run` fails, exits with the same code WITHOUT running scherlok against a stale or partial manifest. Passes `--target`, `--profiles-dir`, and `--select` through to `dbt run`. Requires the `dbt` binary on PATH (dbt-core remains an opt-in dependency). ([#40](https://github.com/rbmuller/scherlok/pull/40), closes [#34](https://github.com/rbmuller/scherlok/issues/34))
- **`scherlok dbt --output json`** — emits a single JSON document on stdout (status + per-model anomalies) for CI parsers, with Rich chatter rerouted to stderr so stdout stays machine-readable. ([#41](https://github.com/rbmuller/scherlok/pull/41), closes [#33](https://github.com/rbmuller/scherlok/issues/33))
- **`scherlok dbt --include-snapshots`** — opt-in flag to also profile dbt snapshot nodes. Snapshots are SCD Type 2 tables that physically exist in the warehouse and are profilable like any other materialized model. When the flag is unset, behavior is unchanged. ([#26](https://github.com/rbmuller/scherlok/pull/26), closes [#22](https://github.com/rbmuller/scherlok/issues/22))
- **dbt lineage from `manifest.json`** — new `scherlok.dbt.lineage` module reads the manifest's `parent_map`. Two surfaces consume it: `scherlok dbt --show-lineage` (also on `dbt-run-and-watch`) prints an ASCII upstream/downstream tree (`├──` / `└──` style) under each profiled model; and every anomaly message is suffixed with `· Affects N downstream models: a, b, c` when descendants exist, so webhook and email payloads tell on-call who's about to be paged downstream. Leaf marts get no suffix. ([#49](https://github.com/rbmuller/scherlok/pull/49), closes [#36](https://github.com/rbmuller/scherlok/issues/36))

#### Dashboard
- **14-day anomaly trend barchart** — per-day severity bars in the HTML report. ([#28](https://github.com/rbmuller/scherlok/pull/28), closes [#20](https://github.com/rbmuller/scherlok/issues/20))
- **Stale tables panel** — surfaces tables whose last profile is older than the freshness threshold, to catch silent ETL failures. ([#30](https://github.com/rbmuller/scherlok/pull/30), closes [#21](https://github.com/rbmuller/scherlok/issues/21))

#### CLI
- **`scherlok check`** — alias for `scherlok ci --fail-on critical`, the one-liner CI gate. ([#44](https://github.com/rbmuller/scherlok/pull/44), closes [#32](https://github.com/rbmuller/scherlok/issues/32))
- **`scherlok connect` with no args** prints example connection strings for every supported adapter instead of erroring. ([#24](https://github.com/rbmuller/scherlok/pull/24), closes [#23](https://github.com/rbmuller/scherlok/issues/23))

#### Distribution
- **Docker image** — multi-stage Dockerfile published to GHCR. ([#38](https://github.com/rbmuller/scherlok/pull/38), closes [#35](https://github.com/rbmuller/scherlok/issues/35))

### Changed
- Release workflow fails if the `linux/amd64` image exceeds 100 MiB, so the published image can't silently bloat. ([#43](https://github.com/rbmuller/scherlok/pull/43))

## [0.5.0] — 2026-04-30

### Added
- **dbt integration v0** — new `scherlok dbt` command. Reads `target/manifest.json`, discovers materialized models (`table`/`incremental`/`view`/`materialized_view`), auto-resolves the connection from `profiles.yml` (postgres / bigquery / snowflake), and runs investigate + watch per model with dbt-style ✓/✗ output.
  - Optional dependency: `pip install scherlok[dbt]` (adds PyYAML)
  - Flags: `--project-dir`, `--profiles-dir`, `--target`, `--connection-string`, `--select`, `--include-sources`, `--fail-on`, `--webhook`, `--email`
  - Supports `{{ env_var('NAME', 'default') }}` rendering in `profiles.yml`
  - Requires dbt 1.6+ (manifest schema v10+)
- **HTML dashboard** — new `scherlok dashboard` command. Generates a self-contained HTML report (~28 KB) from the local profile store: KPIs, per-table incidents grouped with summary/threshold/first-seen, schema-drift `+`/`−`/`~` diff, sparklines, and history. Auto dark/light theme via `prefers-color-scheme` (`--theme dark|light` to override). Adds `jinja2>=3.0` to core dependencies.
- **`examples/dbt_smoke/`** — runnable mini dbt project (3 models on top of the existing seeded Postgres in `examples/docker-compose.yml`) for contributors to reproduce the dbt + dashboard flow end-to-end.

### Changed
- **`--verbose` / `-v` and `--quiet` / `-q` global flags** added to all commands. `--quiet` silences progress chatter while keeping anomaly results and errors (CI-friendly). `--verbose` adds per-table profiling timings and column counts.
- **Connector error messages now include actionable hints.** `Failed to connect.` is followed by a one-line explanation of what went wrong, plus a hint when applicable.
  - **Postgres:** classifies "connection refused" / "auth failed" / "database does not exist" / "SSL required" / timeout / unknown host
  - **Snowflake:** detects missing `SNOWFLAKE_USER` / `SNOWFLAKE_PASSWORD`, missing python connector, auth failures, account/warehouse/database not found
  - **BigQuery:** detects missing google-cloud-bigquery, missing Application Default Credentials, permission denied, project/dataset not found, billing not enabled

### Fixed
- **Views were silently skipped** by `scherlok investigate`/`watch`/`dbt` on Postgres and Snowflake (`list_tables` filtered to `BASE TABLE` only). Materialized dbt views (`materialized: view`, the default for `staging/` models in layered dbt projects) are now discovered. ([#7](https://github.com/rbmuller/scherlok/issues/7))
- **Release notes extraction in CI** — the awk in `release.yml` was using regex match against `## [version]`, where `[...]` is interpreted as a character class. Switched to literal-substring match so v0.5.0 release notes are properly extracted from `CHANGELOG.md` instead of falling back to the generic placeholder.

## [0.4.0] — 2026-04-27

### Added
- Snowflake connector (`pip install scherlok[snowflake]`)
- `scherlok ci` command — all-in-one CI/CD (connect + watch + exit code) in one line
- Email alerter — `scherlok watch --email user@company.com` (configure via `SCHERLOK_SMTP_*` env vars)
- Multi-recipient support: `--email` is repeatable
- Issue templates (bug, feature, new connector)
- CHANGELOG.md
- Automatic GitHub Release notes on tag push

## [0.3.0] — 2026-04-22

### Added
- Generic webhook alerter — auto-detects Slack, Discord, Microsoft Teams, or generic JSON endpoints
- `--webhook` / `-w` flag in the `watch` command

### Removed
- Removed legacy Slack-only path; replaced by the generic webhook

## [0.2.2] — 2026-04-22

### Fixed
- Sync `__version__` between `pyproject.toml` and `src/scherlok/__init__.py`

## [0.2.0] — 2026-04-22

### Added
- BigQuery connector (`pip install scherlok[bigquery]`)
- Detective logo (AI-generated illustration)
- Animated SVG demo in README
- Examples folder with Docker + PostgreSQL quick start
- 34 new tests across store, alerter, config, BigQuery (93 total)
- PyPI release workflow with trusted publishing
- Redesigned README

## [0.1.0] — 2026-04-15

### Added
- Initial release
- PostgreSQL connector
- 6 anomaly detectors: volume, schema drift, freshness, nullability, distribution, cardinality
- CLI with Typer (`connect`, `investigate`, `watch`, `report`, `status`, `history`, `version`)
- Local SQLite storage + remote storage (S3, GCS, Azure Blob)
- Slack webhook integration
- 59 unit tests

[Unreleased]: https://github.com/rbmuller/scherlok/compare/v0.7.0...HEAD
[0.7.0]: https://github.com/rbmuller/scherlok/compare/v0.6.0...v0.7.0
[0.6.0]: https://github.com/rbmuller/scherlok/compare/v0.5.0...v0.6.0
[0.5.0]: https://github.com/rbmuller/scherlok/compare/v0.4.0...v0.5.0
[0.4.0]: https://github.com/rbmuller/scherlok/compare/v0.3.0...v0.4.0
[0.3.0]: https://github.com/rbmuller/scherlok/compare/v0.2.2...v0.3.0
[0.2.2]: https://github.com/rbmuller/scherlok/compare/v0.2.0...v0.2.2
[0.2.0]: https://github.com/rbmuller/scherlok/compare/v0.1.0...v0.2.0
[0.1.0]: https://github.com/rbmuller/scherlok/releases/tag/v0.1.0
