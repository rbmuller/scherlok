# Changelog

All notable changes to Scherlok are documented here.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

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

[Unreleased]: https://github.com/rbmuller/scherlok/compare/v0.3.0...HEAD
[0.3.0]: https://github.com/rbmuller/scherlok/compare/v0.2.2...v0.3.0
[0.2.2]: https://github.com/rbmuller/scherlok/compare/v0.2.0...v0.2.2
[0.2.0]: https://github.com/rbmuller/scherlok/compare/v0.1.0...v0.2.0
[0.1.0]: https://github.com/rbmuller/scherlok/releases/tag/v0.1.0
