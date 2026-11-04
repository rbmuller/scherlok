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

`ci` saves every run, which is what a monitor should do, but it weakens a gate: after a failure the broken state becomes the baseline, so a retry passes. An AI agent or a CI job that retries would get a green result on a broken table.

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
      "message": "Row count dropped 70.0% (1,000 -> 300)"
    }
  ]
}
```

Agents get the same gate as the read-only MCP tool `verify(tables, fail_on)`; see [MCP server](mcp.md).

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

## GitHub Actions

```yaml
- name: Data quality check
  run: |
    pip install scherlok
    scherlok config --store s3://my-bucket/scherlok/profiles.db
    scherlok ci ${{ secrets.DATABASE_URL }} \
      --webhook ${{ secrets.SLACK_WEBHOOK }} \
      --fail-on critical
```

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

Nothing above is GitHub-specific: install the package, restore the shared store, run `scherlok ci` (or `scherlok verify` for a read-only gate). The exit code does the rest. Worked examples for GitLab CI and CircleCI are welcome as contributions; see the [issue tracker](https://github.com/rbmuller/scherlok/issues).

## Container image

```bash
docker run --rm \
  -v "$PWD:/work" -w /work \
  -e SCHERLOK_CONNECTION=postgres://... \
  ghcr.io/rbmuller/scherlok:latest watch --exit-code --fail-on critical
```

The image is built from `python:3.12-slim`, bundles every warehouse extra and runs unprivileged.
