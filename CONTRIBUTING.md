# Contributing to Scherlok

Thanks for considering a contribution. Scherlok is small, opinionated, and pragmatic — most changes can land in a weekend.

## Ways to contribute

- **Pick a [good first issue](https://github.com/rbmuller/scherlok/labels/good%20first%20issue)** — bite-sized tasks (1–2 hours) with clear acceptance criteria
- **Add a new connector** — Postgres / BigQuery / Snowflake live in [`src/scherlok/connectors/`](src/scherlok/connectors/); each implements [`BaseConnector`](src/scherlok/connectors/base.py) (connect, list_tables, get_row_count, get_columns, get_column_stats, get_last_modified)
- **Add a new detector** — [`src/scherlok/detector/`](src/scherlok/detector/) — a function that takes `(table, current_profile, stored_profile)` and returns anomaly dicts built with `make_anomaly` ([`fields.py`](src/scherlok/detector/fields.py)), so they carry the structured fields; a new anomaly type goes in [`anomaly_types.py`](src/scherlok/detector/anomaly_types.py) with a one-sentence hint in [`hints.py`](src/scherlok/detector/hints.py)
- **Improve docs** — module READMEs, examples, error messages
- **Report bugs** — use the [bug template](https://github.com/rbmuller/scherlok/issues/new/choose); include the failing connection scheme + connector version

## Development setup

```bash
git clone https://github.com/rbmuller/scherlok
cd scherlok
python -m venv .venv && source .venv/bin/activate
pip install -e ".[dev]"      # base + tests + lint
pip install -e ".[dbt]"      # if you'll touch dbt integration
ruff check src/ tests/ action/
pytest                        # 200+ tests, runs in <1s
```

To exercise the full flow end-to-end against a real Postgres + real dbt, see [`examples/dbt_smoke/`](examples/dbt_smoke/) — a runnable mini dbt project.

## Code style

- **Python 3.10+**, type hints on public APIs
- **Ruff** for linting (config in `pyproject.toml`); CI fails on warnings
- **Line length 100**
- Prefer **explicit imports**; avoid wildcard
- Use **`pathlib.Path`** over `os.path`
- **String literals**: define constants for anything used twice (no magic strings)
- **No new comments** unless they explain *why* something non-obvious is happening; well-named identifiers carry intent

## Testing

- All new code paths need tests. Aim for ≥1 happy-path test + ≥1 edge case
- Use **mocks** for connectors that need cloud creds (see [`tests/test_bigquery.py`](tests/test_bigquery.py), [`tests/test_snowflake.py`](tests/test_snowflake.py))
- Use **fixtures** for shared test data (see [`tests/fixtures/dbt/`](tests/fixtures/dbt/))
- Snapshot tests for the dashboard guard against accidental UI regressions

## Commit format

[Conventional Commits](https://www.conventionalcommits.org/) lite — Scherlok uses these prefixes:

- `feat:` — user-visible new functionality
- `fix:` — bug fix
- `chore:` — non-user-facing maintenance (build, deps, refactor, release prep)
- `docs:` — docs/comments only
- `test:` — test-only changes

Subject line ≤72 chars. Body explains the *why*, not the *what*. Multi-line is fine.

## Pull request flow

1. **Branch** from `main` — `feat/<short-description>` or `fix/<short-description>`
2. **One PR per logical change.** Don't bundle a feature with unrelated cleanup
4. **Run `ruff check src/ tests/ action/` and `pytest` locally** before pushing — CI mirrors these
5. **Open the PR with a body that covers**: what changed, why, how to test, screenshots if UI
6. **Wait for review** — single approval required to merge
7. **Squash merge** is the default

## Adding a new connector

The fastest path is to read [`postgres.py`](src/scherlok/connectors/postgres.py) and clone its shape. Then:

1. Create `src/scherlok/connectors/<name>.py` extending `BaseConnector`
2. Register the scheme(s) in [`src/scherlok/connectors/__init__.py`](src/scherlok/connectors/__init__.py); wrap the import in `try/except ImportError` if the driver is heavy
4. Add the optional dependency in `pyproject.toml` under `[project.optional-dependencies]`
5. Add tests in `tests/test_<name>.py` using mocks (see existing connector tests)
6. Document in the README's "Supported adapters" section

If your warehouse is one users wire up via dbt, add the adapter mapping in [`src/scherlok/dbt/profiles.py`](src/scherlok/dbt/profiles.py) too.

## Adding a new alerter

[`src/scherlok/alerter/`](src/scherlok/alerter/) hosts webhook + email today. To add another channel:

1. Create `src/scherlok/alerter/<name>.py` with a `send_<name>(target, anomalies)` function
2. Wire it into the dispatch path in [`src/scherlok/cli.py`](src/scherlok/cli.py) (look for `_dispatch_alerts`)
4. Add tests with mocked HTTP / SMTP

## Release process

For maintainers only:

1. Bump the version everywhere it is stated: `version` in `pyproject.toml`, `__version__` in `src/scherlok/__init__.py`, `version` in `dbt_project.yml`, both `version` fields in `server.json`, the `revision: vX.Y.Z` dbt install snippet in `README.md` and `website/dbt-package.md`, and the `scherlok-version` default in `action.yml` (the version the GitHub Action installs). `tests/test_release_versions.py` fails if any of them disagree (a partial bump is how v1.0.0 never reached PyPI)
2. Bump the Claude Desktop bundle: `version` in `mcpb/manifest.json`, plus `version` and the `scherlok[...]>=X.Y.Z` floor in `mcpb/pyproject.toml` (no lockfile — the bundle is packed during the release that publishes this version)
3. Move `[Unreleased]` to `[X.Y.Z] — YYYY-MM-DD` in `CHANGELOG.md`
4. PR + merge to main — `tests/test_mcpb.py` fails the PR if the bundle manifest or its `scherlok` floor drifted from the package version
5. `git tag vX.Y.Z && git push origin vX.Y.Z`
6. `.github/workflows/release.yml` runs tests, publishes to PyPI via trusted publishing, packs `scherlok-X.Y.Z.mcpb` and creates the GitHub Release with auto-extracted notes and the bundle attached
7. `.github/workflows/publish-mcp.yml` then publishes `server.json` to the MCP Registry (GitHub OIDC). Check the listing at https://registry.modelcontextprotocol.io/v0.1/servers?search=io.github.rbmuller/scherlok; re-run it with `gh workflow run publish-mcp.yml --ref main` if needed
8. Once PyPI shows `X.Y.Z` (the action installs it from there), move the GitHub Action's major tag: `git tag -f v1 vX.Y.Z && git push -f origin v1`. Users on `rbmuller/scherlok@v1` get the new release on their next run. Release and Docker only run on `vX.Y.Z` tags, so this publishes nothing. A breaking change to the action's inputs or outputs needs a new `v2` tag instead
9. GitHub Marketplace: the first time, open the release on GitHub, edit it, tick **Publish this Action to the GitHub Marketplace** (two-factor authentication and the Marketplace agreement are required), pick the categories and save. For later releases, check that https://github.com/marketplace/actions/scherlok-verify shows the new version, and tick the box on the release if it does not

## Code of conduct

Be helpful. Assume good faith. Disagree on technical merits, not on people. Threads should leave both sides smarter than they entered.
