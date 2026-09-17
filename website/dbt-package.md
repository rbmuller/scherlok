# dbt package

Prefer staying inside dbt? Scherlok is also a native dbt package: data tests you attach in `schema.yml`, no Python CLI needed.

## Install

```yaml
# packages.yml
packages:
  - git: https://github.com/rbmuller/scherlok.git
    revision: v1.0.1
```

Once the dbt Package Hub listing lands ([dbt-labs/hubcap#456](https://github.com/dbt-labs/hubcap/pull/456)), this becomes `package: rbmuller/scherlok` with `version: [">=1.0.0", "<2.0.0"]`. Requires dbt 1.6+.

## Tests

```yaml
# schema.yml
models:
  - name: fct_orders
    tests:
      - scherlok.volume_anomaly:
          sensitivity: 3.0
      - scherlok.row_count_between:
          min_value: 100
    columns:
      - name: email
        tests:
          - scherlok.not_null_proportion:
              max_rate: 0.01
      - name: updated_at
        tests:
          - scherlok.recency:
              days: 2
```

**Tier 1, instant (no setup):** `not_null_proportion`, `row_count_between`, `recency`, `unique_proportion`.

**Tier 2, auto-learning:** `volume_anomaly`, `null_anomaly`. Backed by Shewhart control limits over the incremental `scherlok_metrics` and `scherlok_column_metrics` models, which auto-discover materialized models and log row counts and NULL rates on every `dbt run`. The tests pass silently until enough history exists, so first runs are baseline, not false alarms. `scherlok_column_metrics` profiles only the columns that carry a `null_anomaly` test, to keep wide tables cheap.

## Configuration

```yaml
# dbt_project.yml
vars:
  scherlok_exclude_models: []          # skip these models
  scherlok_include_models: []          # or monitor only these
  scherlok_metrics_enabled: true
  scherlok_column_metrics_enabled: false
```

Model and column descriptions live in [`models/_models.yml`](https://github.com/rbmuller/scherlok/blob/main/models/_models.yml); the test macros are in [`macros/`](https://github.com/rbmuller/scherlok/tree/main/macros).
