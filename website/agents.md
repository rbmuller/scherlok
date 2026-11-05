# Coding agents

An agent that finishes `dbt run` reports success: the SQL ran. Whether the data is right is another question, and nothing makes the agent ask it. Scherlok's answer is `scherlok verify`, a read-only gate that compares the tables with the baseline a person approved and never changes it. This page wires it into the agent's loop: a Claude Code plugin that runs it on its own after every `dbt run` and `dbt build`, and the same instructions as an `AGENTS.md` snippet for other coding agents.

A sample dbt project on DuckDB shows the loop end to end: the agent adds a join that fans out, the hook reports it, the agent fixes the join, the check passes. See [the demo project](https://github.com/rbmuller/scherlok/tree/main/examples/claude_code_demo/).

## Claude Code plugin

The plugin, in [`integrations/claude-code/`](https://github.com/rbmuller/scherlok/tree/main/integrations/claude-code/), has two parts:

- **A `PostToolUse` hook** on Bash. After a `dbt run` or `dbt build` that succeeds, it runs `scherlok verify --dbt-project <project> --output json` on the models that run built and hands the result to Claude.
- **The skill `verify-data-pipeline`**: when to check, the loop of verify, fix and verify again, and the rule that accepting a new baseline is a person's decision.

### Install

In a Claude Code session:

```text
/plugin marketplace add rbmuller/scherlok
/plugin install scherlok@scherlok
```

Claude Code v2.1.275 or later does both in one step: `/plugin install scherlok --marketplace rbmuller/scherlok`. To try it from a clone without installing, start a session with `claude --plugin-dir integrations/claude-code`.

The hook needs, in the environment Claude Code starts in:

1. `python3` on `PATH`. The hook script uses the standard library only.
2. `scherlok` on `PATH`, or its path in `SCHERLOK_BIN`.
3. A connection: `SCHERLOK_CONNECTION`, or one saved with `scherlok connect`.
4. A baseline for the models, set by a person while the data is right: `scherlok investigate`, or `scherlok accept --table <model> --reason "..."`.

Without 2 or 3 the hook does nothing at all. Lock the baselines for agent sessions, so that no Scherlok command the agent runs can move one (see [Locked baselines](getting-started.md#locked-baselines)):

```bash
export SCHERLOK_BASELINE_LOCKED=1
claude
```

To keep Claude from running `scherlok accept` through Bash as well, add a deny rule to the project's `.claude/settings.json`:

```json
{ "permissions": { "deny": ["Bash(scherlok accept *)"] } }
```

### What the hook returns

| Situation | Claude Code receives |
|---|---|
| Not `dbt run` or `dbt build`, a background command, or an interrupted one | nothing |
| `scherlok` not installed, too old for `verify --dbt-project`, or no connection configured | nothing |
| verify failed | `decision: "block"` with the findings as `reason`, shown to Claude next to the command's output, and a one-line `systemMessage` for you |
| verify passed | a summary as `additionalContext`, listing WARNING findings and what was not checked |
| verify could not run: a timeout, a connection error, no `target/manifest.json` | a one-line note, to Claude and to you, that the run was not verified |

A failed check, as Claude reads it:

```text
Scherlok verify failed after `dbt run` (2 critical, 1 warning, 0 info; fail-on critical). The baselines are unchanged.
- CRITICAL fct_orders.order_id (cardinality_change): Column 'order_id' distinct ratio decreased: 1.000 -> 0.333 (67% change). Hint: The number of distinct values changed sharply; check for duplicated keys, a broken join or a new set of categories.
- WARNING fct_orders (volume_spike): Row count spiked 200% (40 -> 120). Hint: Rows were duplicated or loaded twice; check for a join that fans out or a repeated load.
...
```

The hook exits 0 on every path, so it never fails the session. It holds no credentials: it learns whether a connection exists from `scherlok config --output json`, which reports `connection_configured` and `connection_source` but never the connection string, and it never reads `~/.scherlok`. Findings carry table and column names and aggregates, never a value read from a row.

| Setting | Default | |
|---|---|---|
| `SCHERLOK_BIN` | `scherlok` on `PATH` | The scherlok executable, for example in a virtualenv. |
| `SCHERLOK_HOOK_TIMEOUT` | `150` | Seconds for the whole check. Keep it below the hook's `timeout` in `hooks.json` (180), or Claude Code cancels the hook first and Claude learns nothing. |
| `DBT_PROJECT_DIR`, `DBT_TARGET_PATH` | | Read as dbt reads them, after `--project-dir` and `--target-path` on the command. Without either, the project is the working directory or its nearest parent with a `dbt_project.yml`. |

To use the hook without the plugin, copy the script and the skill into the project's `.claude/` directory; the [plugin README](https://github.com/rbmuller/scherlok/blob/main/integrations/claude-code/README.md#without-the-plugin) has the settings to add.

## The loop

The skill, and the snippet below, give the agent one loop:

1. **Verify what was built.** `scherlok verify --dbt-project <dir> --output json` for dbt, `--table` for each table otherwise, or the MCP server's `verify` tool. With the hook, the result arrives on its own.
2. **Read the findings.** Each anomaly carries `table`, `type`, `severity`, `column`, `metric`, `baseline`, `current`, `change_pct` and a `hint` (see [Structured anomaly fields](ci-cd.md#structured-anomaly-fields)).
3. **Fix the cause, not the check.** A key whose distinct ratio dropped, with a row-count spike, is a join that fanned out; [`scherlok reconcile`](reconcile.md) confirms it against the input table.
4. **Run the pipeline and verify again**, until it passes.
5. **Report** what verify found, what changed, the final result, and what was not checked.

When a finding is the point of the task, such as a new column or a planned backfill, verify keeps failing by design. The agent stops and asks a person, and suggests `scherlok accept --table <table> --reason "..."` for them to run. It never runs `accept` itself, never makes the check pass another way, and never reads the connection string.

## Scope a check to a dbt run

`--dbt-project` limits `verify` to what the last dbt invocation built, so a large project isn't profiled in full after every command:

```bash
scherlok verify --dbt-project . --output json
scherlok verify --dbt-project ./analytics --dbt-target-path build/target
```

It reads `run_results.json` for the models that succeeded and `manifest.json` for their names, the same way `dbt-run-and-watch` does, and matches each model to a table the connection lists, by alias, then by name. Those tables count as just built, like `--table`: an empty table, an all-NULL column or a duplicate key there is CRITICAL. A model the connection can't see is listed in the JSON under `dbt_models_not_found` and does not fail the gate. When no model succeeded, nothing is verified, rather than every table. `--table` adds tables to the scope. The adapter in the manifest doesn't matter, since the connection is Scherlok's own: a DuckDB or MySQL project works too.

## Other coding agents: AGENTS.md

Agents that read an [`AGENTS.md`](https://agents.md/) file get the same instructions, without the hook. Paste this into yours; it is also in the repository as [`integrations/AGENTS.snippet.md`](https://github.com/rbmuller/scherlok/blob/main/integrations/AGENTS.snippet.md).

````markdown
--8<-- "integrations/AGENTS.snippet.md"
````

## Limits

- **A guardrail, not a security boundary.** Locked baselines, a deny rule and the skill's instructions keep an agent that follows them from approving its own change. An agent with shell access can still unset a variable, run the CLI another way, or edit `~/.scherlok`. Real enforcement is `scherlok verify` in CI against a store the agent has no credentials for, with `accept` run from a workflow a person triggers; see [Accepting a change from CI](ci-cd.md#accepting-a-change-from-ci).
- **The hook informs; it can't make Claude act.** It puts the findings next to the command's output. What Claude does with them depends on the skill and on you.
- **Only successful commands.** Claude Code runs `PostToolUse` hooks after a tool call succeeds, so a `dbt run` or `dbt build` that exits non-zero gets no check; the failure itself already reaches Claude. To check what a failing `dbt build` did build, run `scherlok verify --dbt-project .`.
- **A baseline is needed.** A model without one gets only the new-table checks, which don't see a join that fans out. Baselines are kept per database, so a new database, or a DuckDB file at a new path, starts without any. `scherlok reconcile` needs no baseline.
- **Tables by name.** Models are matched to the tables the connection lists: the `public` schema on PostgreSQL, `main` on DuckDB, the dataset or schema in the connection string on BigQuery and Snowflake. A model built elsewhere is reported as not checked.
- **Cost and time.** verify profiles every model it checks, which BigQuery and Snowflake bill like any query. A check that doesn't finish within `SCHERLOK_HOOK_TIMEOUT` is reported as not verified.
- **dbt settings.** A `target-path` set in `dbt_project.yml` isn't read; set `DBT_TARGET_PATH`. The command is recognised as text, so `dbt run` inside a shell script the agent calls goes unseen.
