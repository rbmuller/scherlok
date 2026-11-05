# CI/CD gate

Scherlok fails the pipeline when data breaks, before the deploy reaches production.

## One command

`scherlok ci` runs connect, investigate and watch in a single step and turns the result into an exit code:

```bash
scherlok ci "$DATABASE_URL" --webhook "$SLACK_WEBHOOK" --fail-on critical
```

- First run: profiles everything and runs the new-table checks, since there is nothing to compare against yet: an empty table, a column NULL in every row or duplicates in `id` / `<singular table>_id` (WARNING), one value in 100+ non-NULL rows (INFO). With the default `--fail-on critical` it exits 0 and shows them; `--fail-on warning` fails on them.
- Later runs: compares against the stored baseline, exits 1 on any CRITICAL anomaly (`--fail-on warning` to be stricter).
- Baselines must survive between runs: point `scherlok config --store` at an S3, GCS or Azure Blob path so each runner syncs the same profiles file.

## Read-only gate: `scherlok verify`

`ci` learns every run, which is what a monitor should do, but it weakens a gate. A CRITICAL reading is never learned (see [When the baseline does not learn](getting-started.md#when-the-baseline-does-not-learn)), so a CRITICAL failure repeats; a WARNING, or the first sighting of a broken new table, is learned, so a retry on `--fail-on warning` passes. An AI agent or a CI job that retries needs a gate that changes nothing.

`scherlok verify` compares the current state with the stored baseline and never changes it, so a retry keeps failing until the data is fixed:

```bash
scherlok verify                                   # every table, exit 1 on CRITICAL
scherlok verify --table orders --table customers  # only these tables
scherlok verify --fail-on warning                 # exit 1 on WARNING or worse
scherlok verify --output json                     # one JSON object on stdout
scherlok verify --record scherlok-runs.jsonl      # also append the run record to a file
```

- **Never changes the baseline.** No profiles, no anomalies and no config are saved. The connection comes from `SCHERLOK_CONNECTION` or a previous `scherlok connect` and is never saved. Only the run itself is recorded; see [Run records](#run-records).
- **Never uploads a remote store.** With `--store` or `SCHERLOK_STORE` set, the store is downloaded and never uploaded back, so parallel jobs running `verify` can't overwrite each other or a concurrent `ci` run.
- **Tables without a baseline** are listed under `tables_without_baseline` and never counted as verified. They get only the new-table checks, as WARNING (INFO for a constant column). Tables you name with `--table` count as just built, so an empty table, a column NULL in every row or a duplicate key there is CRITICAL and fails the default gate: `scherlok verify --table new_orders` right after creating it. Once the data is right, run `scherlok investigate` (or let a scheduled `watch` run) to give them a baseline.
- **Exit code:** 1 when an anomaly reaches `--fail-on` (`critical` by default), or on an error such as a missing connection, an unknown `--table` or an invalid flag; 0 otherwise.

Keep the baseline learning elsewhere, for example `scherlok watch --schedule` or `scherlok ci` on your main branch, and gate changes with `verify`:

```yaml
- name: Data quality gate
  env:
    SCHERLOK_CONNECTION: ${{ secrets.DATABASE_URL }}
    SCHERLOK_STORE: s3://my-bucket/scherlok/profiles.db
  run: |
    pip install scherlok
    scherlok verify --fail-on critical --output json --record scherlok-runs.jsonl > scherlok-verify.json
- name: Keep the run record
  if: always()
  uses: actions/upload-artifact@v4
  with:
    name: scherlok-runs
    path: scherlok-runs.jsonl
```

With `--output json`, stdout carries a single object. Rich output goes to stderr: errors, plus a one-line verdict when the gate fails, so the CI log says why the step failed even when stdout goes to a file.

```json
{
  "passed": false,
  "fail_on": "critical",
  "tables_verified": ["orders"],
  "tables_without_baseline": ["refunds"],
  "anomaly_count": 1,
  "info": 0,
  "warning": 0,
  "critical": 1,
  "anomalies": [
    {
      "table": "orders",
      "type": "volume_drop",
      "severity": "CRITICAL",
      "message": "Row count dropped 70.0% (1,000 -> 300)",
      "column": null,
      "metric": "row_count",
      "baseline": 1000,
      "current": 300,
      "change_pct": -70.0,
      "hint": "Rows were deleted or filtered out; check joins and WHERE clauses changed in this build."
    }
  ]
}
```

Agents get the same gate as the read-only MCP tool `verify(tables, fail_on)`; see [MCP server](mcp.md).

### Structured anomaly fields

Each anomaly carries the facts behind its `message`, so a script or an agent can act on it without parsing the sentence. A field that does not apply is `null`.

| Field | Meaning |
|---|---|
| `column` | The column involved; `null` for a table-level anomaly such as `volume_drop`. |
| `metric` | What was measured: `row_count`, `column_type`, `hours_since_update`, `null_rate`, `null_count`, `mean`, `distinct_count`, or `distinct_ratio` (distinct values per non-NULL row, compared on near-unique columns); [`reconcile`](reconcile.md) adds `duplicate_keys`, `missing_keys`, `extra_keys` and `sum`. |
| `baseline` | The value compared against: the stored profile, or the median of recent runs once there is enough history. `null` for a table seen for the first time and for an added column. |
| `current` | The value on this run; `null` for a removed column. |
| `change_pct` | Signed change from `baseline` to `current` in percent, one decimal, negative when the value fell. For `null_rate` it is the relative change of the rate, not percentage points. `null` unless both are numbers and `baseline` is not 0. |
| `hint` | One fixed sentence per anomaly type on where to look first. `--explain` gives a reasoned hypothesis instead. |

`baseline` and `current` are aggregates (counts, rates, means, hours) or column types, never a value read from a row. The same fields appear in `scherlok history --output json`, `scherlok dbt --output json`, `scherlok dbt-run-and-watch --output json`, `scherlok reconcile --output json` and the MCP `watch`, `verify`, `history` and `reconcile` tools. Anomalies recorded by an older release have them all `null`.

## Run records

A run that finds nothing still leaves a record, so you can show that a check ran and passed. `watch` (every tick of `--schedule` too), `ci`, `check`, `verify` and the MCP tools `watch`, `check` and `verify` each write one row to the store's `runs` table:

```bash
scherlok history --runs                 # newest first, last 30 days
scherlok history --runs --days 7 --output json
```

Each anomaly a run saves carries the run's `run_id`, which `scherlok history --output json` prints, so an alert can be traced to the run that raised it. A record holds table names and counts only, never a column value or a connection string; the database is named by its [scope](getting-started.md#where-things-live), which has no credentials.

`verify` writes its record to the local store, and never uploads a remote store. On a CI runner that local copy is gone when the job ends, so pass `--record PATH`: it appends the same record as one JSON line, and the file can be kept as a build artifact, as in the workflow above. A record that can't be written exits 1.

```json
{"run_id": "3f9c2a7e5b1d4c8e9a0f6b2d7c4e1a58", "command": "verify", "scope": "postgresql://db.example.com/shop", "scherlok_version": "X.Y.Z", "tables": ["orders", "refunds"], "passed": false, "fail_on": "critical", "critical": 1, "warning": 0, "info": 0, "started_at": "2026-11-04T09:15:02.418310+00:00", "finished_at": "2026-11-04T09:15:03.902114+00:00"}
```

`command` is `watch`, `ci`, `check`, `verify`, or `mcp:` followed by the tool name. `fail_on` is the gate the run was judged by; `watch` has no `--fail-on` and is judged on CRITICAL, as its exit code is. `investigate`, `dbt` and `dbt-run-and-watch` runs are not recorded.

## Accepting a change from CI

A CRITICAL reading is never learned, and with [locked baselines](getting-started.md#locked-baselines) nothing is, so an intended change becomes the baseline only through `scherlok accept`. Locking and `accept` are a guardrail, not a security boundary: an agent with shell access can run the CLI or edit the SQLite file. For a gate an agent can't get past:

- Run `verify` in CI against a store the agent has no credentials for, such as a bucket only the CI role can write.
- Lock the baselines (`SCHERLOK_BASELINE_LOCKED=1`) for every run that writes that store, so scheduled runs compare without learning.
- Run `accept` only from a workflow a person triggers:

```yaml
on:
  workflow_dispatch:
    inputs:
      table: { description: "Table to accept", required: true }
      reason: { description: "Why this state is the new normal", required: true }

jobs:
  accept:
    runs-on: ubuntu-latest
    steps:
      - env:
          SCHERLOK_CONNECTION: ${{ secrets.DATABASE_URL }}
          SCHERLOK_STORE: s3://my-bucket/scherlok/profiles.db
          TABLE: ${{ inputs.table }}
          REASON: ${{ inputs.reason }}
          ACTOR: ${{ github.actor }}
        run: |
          pip install scherlok
          git config --global user.email "$ACTOR@users.noreply.github.com"
          scherlok accept --table "$TABLE" --reason "$REASON" --yes
```

The inputs reach the script as environment variables, never pasted into it, so a reason can't inject shell. `accept` records `git config user.email` as the approver, else `$USER`, so the workflow sets it to whoever triggered the run. `scherlok history --approvals --output json` lists what was accepted.

## GitHub Action

The pull request is where a reviewer looks, so the result belongs there. `rbmuller/scherlok@v1` runs `scherlok verify` against the stored baseline, writes the job summary, fails the check when an anomaly reaches `fail-on`, and keeps one comment on the pull request up to date:

```yaml
name: Data checks
on: pull_request

permissions:
  contents: read
  pull-requests: write

jobs:
  scherlok:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      # Build the change first, e.g. `dbt run` against a CI schema, and sign the
      # runner in to the store's cloud, e.g. aws-actions/configure-aws-credentials.
      - uses: rbmuller/scherlok@v1
        with:
          connection: ${{ secrets.DATABASE_URL }}
          store: s3://my-bucket/scherlok/profiles.db
          tables: orders,customers
          fail-on: critical
```

| Input | Default | Meaning |
|---|---|---|
| `connection` | required | Database connection string, from a secret. |
| `store` | required | The `s3://`, `gs://` or `az://` URL of the `profiles.db` that `scherlok watch` or `scherlok ci` keeps up to date. The runner needs that cloud's CLI (`aws`, `gsutil` or `az`), signed in. The action downloads it and never uploads it. |
| `tables` | every table | Tables to verify, separated by commas or new lines. Named tables count as just built, as with `--table`. |
| `fail-on` | `critical` | `critical` or `warning`. |
| `comment` | `true` | Create or update one comment on the pull request. |
| `footer` | `true` | End the comment and the summary with "Checked by Scherlok"; `false` drops it. |
| `github-token` | `github.token` | Token used to comment; needs `pull-requests: write`. |
| `extras` | none | Extras your warehouse needs: `bigquery`, `snowflake`, `mysql` or `duckdb`. PostgreSQL needs none. |
| `scherlok-version` | the release of the action tag | Scherlok version installed from PyPI. `@v1` moves to each release, together with this default. |
| `install-from` | none | For testing the action: install Scherlok from a local path, such as a checkout, instead of PyPI. |

Outputs: `result` (`passed`, `failed`, `error` when Scherlok could not run the check, or `skipped` for a fork pull request), `anomaly-count`, `report` (the path of the `verify --output json` file) and `comment-url`.

The comment and the job summary show one row per anomaly, most severe first, with the [structured fields](#structured-anomaly-fields); an older Scherlok without them gets the type and the message instead:

| Severity | Table | Column | Metric | Baseline | Current | Change | Details |
|---|---|---|---|---|---|---|---|
| 🔴 CRITICAL | `orders` | — | `row_count` | 1,000 | 300 | -70.0% | Row count dropped 70.0% (1,000 -&gt; 300)<br>_Rows were deleted or filtered out; check joins and WHERE clauses changed in this build._ |

A hidden marker finds the comment again, so a re-run or a new push edits it instead of adding another. A run that could not check, for example because the database was unreachable, also updates it, so a stale "passed" never stays.

- **No store, no check.** The baseline has to survive between runs, so a missing `store` fails the step with an error saying so instead of passing with nothing to compare. A store URL with no file yet is a first run: every table is listed as without a baseline. A path to a `profiles.db` on the runner also works, for example one restored by `actions/cache`; it is copied to `~/.scherlok/profiles.db`, where a remote store is downloaded, and never written back. This repository's CI uses one to test the action against a DuckDB file.
- **The connection stays out of the log and the comment.** Scherlok reads it from `SCHERLOK_CONNECTION`, never from the command line. Before anything runs, the action masks the whole string, its password and any credential query parameter, so a driver error that prints a piece of it shows `***`. Pass the whole string as one secret: GitHub prints an action's inputs at the top of its log and masks only secrets there. The step that talks to the GitHub API never gets the connection, and the step with the connection never gets the token.
- **No row values.** The comment is rendered from the `verify` JSON fields only: table and column names, aggregates and fixed hints.
- **Fork pull requests are skipped** with a notice and a summary saying why: GitHub gives them no repository secrets, so there is no connection to check with. Run the check after merging, or from a branch in the repository.
- **Don't switch to `pull_request_target` to get secrets on forks.** It runs with your secrets and a write token, and a workflow that checks out and builds the pull request's code, such as its dbt models or SQL, hands that code your database credentials. The action skips fork pull requests on `pull_request_target` too.
- **Permissions:** `contents: read` and `pull-requests: write`, nothing else. A comment that can't be posted, for example on a Dependabot pull request with a read-only token, is a warning; the check's result stands.
- **Runner:** Linux or macOS with Python 3.10 or newer as `python3`, as on GitHub-hosted runners. Scherlok goes into a virtualenv of its own, leaving the job's packages alone.

## After `dbt run`

```yaml
- run: dbt run --target prod
- run: scherlok dbt --project-dir . --target prod --fail-on critical
```

Or in one step, profiling only the models dbt actually built:

```yaml
- run: scherlok dbt-run-and-watch --project-dir . --target prod --fail-on critical
```

Both accept `--output json` for parsers. See [dbt](dbt.md) for the details.

## Any other CI

Outside GitHub Actions: install the package, restore the shared store, run `scherlok ci` (or `scherlok verify` for a read-only gate). The exit code does the rest. Worked examples for GitLab CI and CircleCI are welcome as contributions; see the [issue tracker](https://github.com/rbmuller/scherlok/issues).

## Container image

```bash
docker run --rm \
  -v "$PWD:/work" -w /work \
  -e SCHERLOK_CONNECTION=postgres://... \
  ghcr.io/rbmuller/scherlok:latest watch --exit-code --fail-on critical
```

The image is built from `python:3.12-slim`, bundles every warehouse extra and runs unprivileged.
