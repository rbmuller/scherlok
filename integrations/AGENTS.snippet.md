## Checking data after pipeline changes (Scherlok)

A dbt run that exits 0 only says the SQL ran. After changing a data pipeline (dbt models, SQL transformations) and running it, check the data before reporting the work as done. `scherlok verify` is read-only: it compares tables with the baseline a person approved and never changes it, so a broken table keeps failing until the data is fixed.

1. Verify what was built:
   - dbt: `scherlok verify --dbt-project <project dir> --output json` checks the models that succeeded in the last run.
   - Other pipelines: `scherlok verify --table <table> --output json`, one `--table` per table built.
   - When the Scherlok MCP server is connected, its `verify` tool does the same; pass the tables you built.
2. Read `passed`, then each anomaly: `table`, `type`, `severity`, `column`, `metric`, `baseline`, `current`, `change_pct`, `hint`.
3. Fix the cause in the model or SQL, never the check. A key whose distinct ratio dropped, often with a row-count spike, is a join that fanned out: confirm with `scherlok reconcile --source <input table> --target <output table> --key <key> --output json`, then join on the full key, keep one row per key, or aggregate before joining.
4. Run the pipeline again, then verify again, until `passed` is true.
5. Report what verify found, what you changed, the final result, and what was not checked: `tables_without_baseline` (new tables get only the new-table checks) and `dbt_models_not_found`.

When a finding is the intended result of the task (a new column, a planned backfill, a deliberate filter), stop and ask a person. List the findings, say why each is expected, and suggest the command for them to run: `scherlok accept --table <table> --reason "<why>"`.

Never run `scherlok accept`: accepting a baseline is a person's decision. Never make a failing check pass another way: no `scherlok investigate`, `watch`, `ci`, `check`, `dbt` or `dbt-run-and-watch` (they write baselines unless baselines are locked), no edits to `~/.scherlok` or `SCHERLOK_*` variables, no switching the connection, no lower `--fail-on`. Never read or print the connection string, `~/.scherlok/config.json` or other credentials.
