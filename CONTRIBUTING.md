# Contributing to Scherlok

Thanks for considering a contribution. Scherlok is small, opinionated, and pragmatic — most changes can land in a weekend.

## Ways to contribute

- **Pick a [good first issue](https://github.com/rbmuller/scherlok/labels/good%20first%20issue)** — bite-sized tasks (1–2 hours) with clear acceptance criteria
- **Add a new connector** — Postgres / BigQuery / Snowflake live in [`src/scherlok/connectors/`](src/scherlok/connectors/); each implements [`BaseConnector`](src/scherlok/connectors/base.py) (connect, list_tables, get_row_count, get_columns, get_column_stats, get_last_modified)
- **Add a new detector** — [`src/scherlok/detector/`](src/scherlok/detector/) — a function that takes `(table, current_profile, stored_profile)` and returns anomaly dicts
- **Improve docs** — module READMEs, examples, error messages
- **Report bugs** — use the [bug template](https://github.com/rbmuller/scherlok/issues/new/choose); include the failing connection scheme + connector version

## Development setup

```bash
git clone https://github.com/rbmuller/scherlok
cd scherlok
python -m venv .venv && source .venv/bin/activate
pip install -e ".[dev]"      # base + tests + lint
pip install -e ".[dbt]"      # if you'll touch dbt integration
ruff check src/ tests/
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
3. **Run `ruff check src/ tests/` and `pytest` locally** before pushing — CI mirrors these
4. **Open the PR with a body that covers**: what changed, why, how to test, screenshots if UI
5. **Wait for review** — single approval required to merge
6. **Squash merge** is the default

## Adding a new connector

The fastest path is to read [`postgres.py`](src/scherlok/connectors/postgres.py) and clone its shape. Then:

1. Create `src/scherlok/connectors/<name>.py` extending `BaseConnector`
2. Register the scheme(s) in [`src/scherlok/connectors/__init__.py`](src/scherlok/connectors/__init__.py); wrap the import in `try/except ImportError` if the driver is heavy
3. Add the optional dependency in `pyproject.toml` under `[project.optional-dependencies]`
4. Add tests in `tests/test_<name>.py` using mocks (see existing connector tests)
5. Document in the README's "Supported adapters" section

If your warehouse is one users wire up via dbt, add the adapter mapping in [`src/scherlok/dbt/profiles.py`](src/scherlok/dbt/profiles.py) too.

## Adding a new alerter

[`src/scherlok/alerter/`](src/scherlok/alerter/) hosts webhook + email today. To add another channel:

1. Create `src/scherlok/alerter/<name>.py` with a `send_<name>(target, anomalies)` function
2. Wire it into the dispatch path in [`src/scherlok/cli.py`](src/scherlok/cli.py) (look for `_dispatch_alerts`)
3. Add tests with mocked HTTP / SMTP

## Release process

For maintainers only:

1. Bump `version` in `pyproject.toml` and `__version__` in `src/scherlok/__init__.py`
2. Move `[Unreleased]` to `[X.Y.Z] — YYYY-MM-DD` in `CHANGELOG.md`
3. PR + merge to main
4. `git tag vX.Y.Z && git push origin vX.Y.Z`
5. `.github/workflows/release.yml` runs tests, publishes to PyPI via trusted publishing, creates GitHub Release with auto-extracted notes

## Code of conduct

Be helpful. Assume good faith. Disagree on technical merits, not on people. Threads should leave both sides smarter than they entered.
