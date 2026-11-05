# Claude Code demo: a join that fans out

A small dbt project on DuckDB for the [Claude Code plugin](../../integrations/claude-code/): Claude changes a model, the join fans out, the hook reports it after `dbt run`, Claude fixes the join, and the check passes. No server and no credentials: the warehouse is a file next to the project.

## What's here

- `seeds/customers.csv`: 8 customers, with a `region` column the task calls deprecated.
- `seeds/customer_addresses.csv`: 3 addresses per customer. `is_primary` marks one; only the primary address has the customer's region.
- `seeds/orders.csv`: 40 orders.
- `models/stg_orders.sql` (view) and `models/fct_orders.sql` (table): one row per order, with the customer's region.

Joining `customer_addresses` on `customer_id` alone triples every order: 40 rows become 120, and `order_id` is no longer unique. dbt runs it without complaint.

## Set up

From a clone of the repository. Tested with Python 3.12, dbt-core 1.12.6 and dbt-duckdb 1.11.1. Install Scherlok and dbt in the same environment, so both use the same DuckDB version to open `demo.duckdb`:

```bash
cd examples/claude_code_demo
python3 -m venv .venv && source .venv/bin/activate
pip install -e "../..[duckdb]" dbt-duckdb     # once a release ships the plugin: pip install "scherlok[duckdb]" dbt-duckdb

dbt seed && dbt run                            # profiles.yml here points dbt at demo.duckdb
export SCHERLOK_CONNECTION="duckdb:///$PWD/demo.duckdb"
scherlok investigate                           # you set the baseline, while the data is right
export SCHERLOK_BASELINE_LOCKED=1              # nothing the agent runs can move it
scherlok verify --dbt-project .                # Verify passed: 2 verified, ...
```

Baselines are kept per database, and a DuckDB database is known by its path: if `demo.duckdb` moves, verify finds no baseline and only the new-table checks run, which don't see this fan-out. Run `scherlok investigate` again from the new place.

Start Claude Code from this shell, so the hook finds `scherlok` and the connection:

```bash
claude --plugin-dir ../../integrations/claude-code
```

## Check the hook without Claude

Paste the join Claude is expected to write into `models/fct_orders.sql`:

```sql
select
    o.order_id,
    o.customer_id,
    o.ordered_at,
    o.amount,
    a.region
from {{ ref('stg_orders') }} as o
left join {{ ref('customer_addresses') }} as a
    on a.customer_id = o.customer_id
```

Run it, then send the hook the JSON Claude Code sends after a Bash call:

```bash
dbt run
printf '%s' '{"hook_event_name": "PostToolUse", "tool_name": "Bash", "cwd": "'"$PWD"'",
  "tool_input": {"command": "dbt run"},
  "tool_response": {"stdout": "", "stderr": "", "interrupted": false, "isImage": false}}' \
  | python3 ../../integrations/claude-code/scripts/verify_after_dbt.py
```

It prints `"decision": "block"` with this `reason`, which Claude reads next to the output of `dbt run`:

```text
Scherlok verify failed after `dbt run` (2 critical, 1 warning, 0 info; fail-on critical). The baselines are unchanged.
- CRITICAL fct_orders.order_id (cardinality_change): Column 'order_id' distinct ratio decreased: 1.000 -> 0.333 (67% change). Hint: The number of distinct values changed sharply; check for duplicated keys, a broken join or a new set of categories.
- CRITICAL fct_orders.amount (cardinality_change): Column 'amount' distinct ratio decreased: 1.000 -> 0.333 (67% change). Hint: The number of distinct values changed sharply; check for duplicated keys, a broken join or a new set of categories.
- WARNING fct_orders (volume_spike): Row count spiked 200% (40 -> 120). Hint: Rows were duplicated or loaded twice; check for a join that fans out or a repeated load.
`scherlok verify` compares with the stored baseline and never changes it, so it keeps failing until the data is fixed; ...
```

Adding `and a.is_primary` to the join and running `dbt run` again makes the hook answer `Scherlok verify passed after dbt run: 2 of 2 table(s) built by the run compared with their baselines (0 critical, 0 warning, 0 info; fail-on critical).` Put the original model back before recording: `git checkout models/fct_orders.sql && dbt run`.

## Recording the README GIF

1. **Off camera**: run the set-up above, and check the hook as in the previous section, then restore the model and `dbt run`. Size the terminal to about 110x32 and clear it.
2. **Start recording** the terminal, for example `asciinema rec claude-code-demo.cast`.
3. Start Claude Code: `claude --plugin-dir ../../integrations/claude-code`.
4. **Prompt**:

   > customers.region is deprecated. In fct_orders, take each order's region from the customer_addresses seed instead, joining on customer_id. Then run dbt run.

5. **Expected**: Claude edits `models/fct_orders.sql` and runs `dbt run`, which succeeds. The status line shows `Scherlok: checking the data`, then `Scherlok: verify failed after `dbt run` (2 critical, 1 warning, 0 info)`. Claude reads the findings (`order_id` distinct ratio 1.000 -> 0.333, rows 40 -> 120), may confirm with `scherlok reconcile --source stg_orders --target fct_orders --key order_id`, adds `is_primary` to the join, and runs `dbt run` again. The hook passes and Claude reports the fix.
6. **Stop recording** once Claude has reported. Trim the waits, keep it under about a minute, and convert it, for example `agg claude-code-demo.cast claude-code-demo.gif`.
7. Save it as `assets/claude-code-demo.gif` and replace the placeholder comment in the repository README with the image.

Claude may pick the primary address on the first try and never fan the join out. Then reset (`git checkout models/fct_orders.sql && dbt run`) and record again, or paste the join above yourself before starting Claude Code and prompt: `Run dbt run, then make sure fct_orders is still right.`

Between takes, check that nothing moved the baseline: `scherlok verify --dbt-project .` passes after the reset. If it fails, run `scherlok investigate` again (with `SCHERLOK_BASELINE_LOCKED` unset) on the original model.
