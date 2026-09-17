# CI/CD gate

Scherlok fails the pipeline when data breaks, before the deploy reaches production.

## One command

`scherlok ci` runs connect, investigate and watch in a single step and turns the result into an exit code:

```bash
scherlok ci "$DATABASE_URL" --webhook "$SLACK_WEBHOOK" --fail-on critical
```

- First run: profiles everything, exits 0 (nothing to compare against yet).
- Later runs: compares against the stored baseline, exits 1 on any CRITICAL anomaly (`--fail-on warning` to be stricter).
- Baselines must survive between runs: point `scherlok config --store` at an S3, GCS or Azure Blob path so each runner syncs the same profiles file.

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

Nothing above is GitHub-specific: install the package, restore the shared store, run `scherlok ci`. The exit code does the rest. Worked examples for GitLab CI and CircleCI are welcome as contributions; see the [issue tracker](https://github.com/rbmuller/scherlok/issues).

## Container image

```bash
docker run --rm \
  -v "$PWD:/work" -w /work \
  -e SCHERLOK_CONNECTION=postgres://... \
  ghcr.io/rbmuller/scherlok:latest watch --exit-code --fail-on critical
```

The image is built from `python:3.12-slim`, bundles every warehouse extra and runs unprivileged.
