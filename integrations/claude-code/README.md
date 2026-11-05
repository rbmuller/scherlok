# Scherlok for Claude Code

An agent that finishes `dbt run` reports success: the SQL ran. This plugin makes it check the data too.

- **Hook**: after every `dbt run` or `dbt build` that Claude runs, `scripts/verify_after_dbt.py` runs `scherlok verify --dbt-project <project> --output json` on the models that run built and hands the result to Claude. A failure arrives next to the command's output, with every finding; a pass arrives as a one-line summary.
- **Skill `verify-data-pipeline`**: the loop of verify, read the findings, fix the model, verify again, and the rule that accepting a new baseline is a person's decision: Claude never runs `scherlok accept`, it asks.

`verify` is read-only: it compares with the stored baseline and never changes it, so a retry keeps failing until the data is fixed.

## Layout

```text
integrations/claude-code/
├── .claude-plugin/plugin.json          the plugin manifest
├── hooks/hooks.json                    PostToolUse on Bash, filtered to commands that call dbt
├── scripts/verify_after_dbt.py         the hook: standard library only, Python 3.8+
└── skills/verify-data-pipeline/SKILL.md
```

The repository root holds the marketplace, `.claude-plugin/marketplace.json`.

## Install

In a Claude Code session:

```text
/plugin marketplace add rbmuller/scherlok
/plugin install scherlok@scherlok
```

Claude Code v2.1.275 or later does both in one step: `/plugin install scherlok --marketplace rbmuller/scherlok`. From a clone, `claude --plugin-dir integrations/claude-code` loads it for one session without installing it.

The hook needs, in the environment Claude Code starts in:

1. `python3` on `PATH`, for the hook script.
2. `scherlok` on `PATH` (`pip install scherlok`), or its path in `SCHERLOK_BIN`.
3. A connection: `SCHERLOK_CONNECTION`, or a saved one from `scherlok connect`.
4. A baseline for the models, set by a person once the data is right: `scherlok investigate`, or `scherlok accept --table <model> --reason "..."`.

Without 2 or 3 the hook does nothing at all. Lock the baselines for agent sessions, so no Scherlok command the agent runs can move one:

```bash
export SCHERLOK_BASELINE_LOCKED=1   # or once: scherlok config --baseline-locked
claude
```

To also keep Claude from running `scherlok accept` through Bash, add a deny rule to `.claude/settings.json`:

```json
{ "permissions": { "deny": ["Bash(scherlok accept *)"] } }
```

Both are guardrails, not a security boundary: an agent with shell access can unset a variable or edit `~/.scherlok`. Real enforcement is `scherlok verify` in CI against a store the agent has no credentials for.

## What the hook returns

| Situation | Claude Code receives |
| --- | --- |
| Not `dbt run` / `dbt build`, a background command, or an interrupted one | nothing |
| `scherlok` not installed, too old for `verify --dbt-project`, or no connection configured | nothing |
| verify failed | `decision: "block"` and the findings as `reason`, shown to Claude next to the command's output; a one-line `systemMessage` for you |
| verify passed | a summary as `additionalContext`, including WARNING findings and what was not checked |
| verify could not run (timeout, connection error, missing `target/`) | a one-line note, to Claude and to you, that the run was not verified |

The hook exits 0 on every path and never fails the session. It holds no credentials: whether a connection exists is asked with `scherlok config --output json`, which never prints it. Findings carry table and column names and aggregates (row counts, rates, ratios), never a value read from a row.

Settings, read from the environment:

| Variable | Default | |
| --- | --- | --- |
| `SCHERLOK_BIN` | `scherlok` on `PATH` | The scherlok executable, for example inside a virtualenv. |
| `SCHERLOK_HOOK_TIMEOUT` | `150` | Seconds for the whole check. Keep it below the hook's `timeout` in `hooks.json` (180), or Claude Code cancels the hook first. |
| `DBT_PROJECT_DIR`, `DBT_TARGET_PATH` | | Read as dbt reads them, after `--project-dir` and `--target-path` on the command. |

## Without the plugin

Copy `scripts/verify_after_dbt.py` to `.claude/hooks/` in your project, and the skill to `.claude/skills/verify-data-pipeline/SKILL.md`, then add the hook to `.claude/settings.json`:

```json
{
  "hooks": {
    "PostToolUse": [
      {
        "matcher": "Bash",
        "hooks": [
          {
            "type": "command",
            "if": "Bash(*dbt *)",
            "command": "python3",
            "args": ["${CLAUDE_PROJECT_DIR}/.claude/hooks/verify_after_dbt.py"],
            "timeout": 180,
            "statusMessage": "Scherlok: checking the data"
          }
        ]
      }
    ]
  }
}
```

The `if` filter keeps the script from starting after other Bash commands. The script checks the command again itself, so it also works where `if` is not supported.

## Limits

- The hook fires only after a dbt command that exits 0. A failed `dbt run` or `dbt build` is already an error for Claude; to check the models a failing `dbt build` did build, run `scherlok verify --dbt-project .`.
- Models are matched by name to the tables Scherlok's connection lists: the `public` schema on PostgreSQL, `main` on DuckDB, the dataset or schema in the connection string on BigQuery and Snowflake. A model elsewhere is listed as not checked.
- A model without a baseline gets only the new-table checks (empty table, all-NULL column, duplicate key). For a join that fans out on its first build, `scherlok reconcile` compares the table with its input.
- verify profiles each model it checks. On BigQuery and Snowflake that is billed like any query.
- A `target-path` set in `dbt_project.yml` is not read: set `DBT_TARGET_PATH` instead.

📖 [Coding agents guide](https://rbmuller.github.io/scherlok/agents/)
