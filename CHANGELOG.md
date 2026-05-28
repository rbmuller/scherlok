# Changelog

All notable changes to Scherlok are documented here.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

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
