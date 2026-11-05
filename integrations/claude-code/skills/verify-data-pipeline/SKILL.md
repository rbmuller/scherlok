---
name: verify-data-pipeline
description: Check the data a pipeline change produced before calling the work done. Use after editing dbt models or SQL transformations and running them (dbt run, dbt build), and whenever a Scherlok hook reports findings after a dbt command. Runs the read-only `scherlok verify` gate, fixes the cause, and verifies again; baseline changes are left to a person.
allowed-tools: Bash(scherlok verify *) Bash(scherlok reconcile *)
---

# Verify a data pipeline with Scherlok

A dbt run that exits 0 only says the SQL ran. `scherlok verify` compares the tables with the baseline a person approved (row counts, schema, NULL rates, distinct values, distributions, freshness) and never changes that baseline, so a broken table keeps failing until the data is fixed.

## When

- After every `dbt run` or `dbt build` that changed models, before reporting the work as done.
- With the Scherlok plugin's hook, the check runs on its own after `dbt run` and `dbt build`, and its result arrives next to the command's output. Read it instead of running verify again. If it says Scherlok did not verify the run, run the check yourself or say so in your report.

## The loop

1. Verify what was built:
   - dbt: `scherlok verify --dbt-project <project dir> --output json` checks the models that succeeded in the last run.
   - Other pipelines: `scherlok verify --table <table> --table <table> --output json`.
   - When the Scherlok MCP server is connected, its `verify` tool does the same; pass the tables you built.
2. Read `passed`, then each anomaly: `table`, `type`, `severity`, `column`, `metric`, `baseline`, `current`, `change_pct`, `hint`.
3. Fix the cause in the model or SQL, never the check:
   - A key whose distinct ratio dropped, often with a row-count spike: a join fanned out. Confirm with `scherlok reconcile --source <input table> --target <output table> --key <key> --output json`, then join on the full key, keep one row per key, or aggregate before joining.
   - Row count dropped: a filter or an inner join removed rows.
   - NULL rate rose: a join key stopped matching, or a column mapping changed.
   - Column added, removed or retyped: the model's schema changed.
4. Run the pipeline again, then verify again. Repeat until `passed` is true.
5. Report what verify found, what you changed, and the final result. Name what was not checked: `tables_without_baseline` (a new table gets only the new-table checks) and `dbt_models_not_found`.

## When the change is intended

Some findings are the point of the task: a new column, a planned backfill, a deliberate filter. verify keeps failing on them by design. Stop and ask a person. List the findings, say why each is expected, and suggest the command for them to run: `scherlok accept --table <table> --reason "<why>"`.

## Never

- Never run `scherlok accept`. Accepting a baseline is a person's decision.
- Never make a failing check pass another way: no `scherlok investigate`, `watch`, `ci`, `check`, `dbt` or `dbt-run-and-watch` (they write baselines unless baselines are locked), no edits to `~/.scherlok` or `SCHERLOK_*` variables, no switching the connection, no lower `--fail-on`, no `--table` list that leaves out what you built.
- Never read or print the connection string, `~/.scherlok/config.json` or other credentials.
