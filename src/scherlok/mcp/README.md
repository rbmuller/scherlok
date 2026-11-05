# Scherlok MCP server

Expose Scherlok's data-quality checks to an AI coding agent (Claude Code, Claude
Desktop, Cursor, …) as [MCP](https://modelcontextprotocol.io) tools. The agent
can profile your warehouse, detect anomalies, and gate on them — with structured
results it reasons over, instead of shelling out and parsing text.

## Install

### Claude Desktop: one click

Download `scherlok-<version>.mcpb` from the [latest release](https://github.com/rbmuller/scherlok/releases/latest) and open it. Claude Desktop installs the extension, asks for one setting — your database connection string, stored as a secret — and the tools appear in the conversation.

The bundle carries no dependencies of its own: on first launch `uv` installs `scherlok` from PyPI (about 90 MB, a few seconds, cached afterwards). It covers PostgreSQL, MySQL and DuckDB; for BigQuery and Snowflake use the CLI below, whose extras need a build toolchain on some platforms.

Sources live in [`mcpb/`](https://github.com/rbmuller/scherlok/tree/main/mcpb).

### Any other MCP client

```bash
pip install scherlok
```

The MCP server ships built-in since v0.7.0 — the `scherlok-mcp` console script (a stdio server) is installed alongside the `scherlok` CLI. (The legacy `pip install scherlok[mcp]` still works as a back-compat alias.)

Works with **mcp 1.2+ and 2.x** (the server class was renamed from `FastMCP` to `MCPServer` in mcp 2.0; both are supported). Releases from 0.8.0 and earlier only support mcp 1.x, so pin `scherlok>=0.9.0` if your environment resolves mcp 2.x.

## Configure the connection (server-side)

The connection string is resolved **on the server**, never passed by the model.
Set it once, either way:

```bash
# Option A — environment variable (per the MCP client config below)
export SCHERLOK_CONNECTION="postgresql://user:pass@host:5432/db"

# Option B — save it to ~/.scherlok/config.json
scherlok connect "postgresql://user:pass@host:5432/db"
```

Any adapter Scherlok supports works: `postgresql://`, `bigquery://`,
`snowflake://`, `mysql://`, `duckdb:///path/to/file.db`.

## Wire it into your agent

### Claude Desktop

Add to `claude_desktop_config.json`:

```json
{
  "mcpServers": {
    "scherlok": {
      "command": "scherlok-mcp",
      "env": { "SCHERLOK_CONNECTION": "postgresql://user:pass@host:5432/db" }
    }
  }
}
```

### Claude Code

Either drop a project-scoped `.mcp.json` in your repo root:

```json
{
  "mcpServers": {
    "scherlok": {
      "command": "scherlok-mcp",
      "env": { "SCHERLOK_CONNECTION": "postgresql://user:pass@host:5432/db" }
    }
  }
}
```

…or register it from the CLI:

```bash
claude mcp add scherlok --env SCHERLOK_CONNECTION="postgresql://..." -- scherlok-mcp
```

## Tools

| Tool | Args | What it does |
|------|------|--------------|
| `list_tables` | — | tables visible to the connection |
| `investigate` | `tables?` | profile tables, store the baseline; `baseline_updated` is false for a table whose state has a CRITICAL anomaly |
| `watch` | `tables?` | detect anomalies vs baseline (type/severity/message plus structured fields); a table with no baseline yet gets the new-table checks |
| `status` | — | connection (redacted), tables visible, anomalies in last 30 days |
| `history` | `days?` | anomalies recorded in the last N days |
| `check` | `fail_on?` | CI-style pass/fail gate (`critical` default, or `warning`) |
| `verify` | `tables?`, `fail_on?`, `against?`, `ignore?` | read-only pass/fail gate: compares with the baseline, or with another database's (`against`, a scope), saves no profiles or anomalies; lists `tables_without_baseline`, and fails on a named table that is empty or has an all-NULL column or a duplicate key; `ignore=["volume"]` for a build on sampled data |
| `reconcile` | `source`, `target`, `key_columns?`, `sum_columns?`, `tolerance?` | read-only check that an output table adds up against its input: row counts, keys repeated in the target (join fan-out), keys missing or extra, column totals within an absolute `tolerance`; `passed` is false on any mismatch |

Typical agent flow: `investigate` once to set the baseline, then `watch` (or
`check`) on later runs to catch drift. `watch`, `check` and `investigate` save
every run except for a table with a CRITICAL anomaly, which keeps its baseline,
so that anomaly is reported again on every call until the data recovers. A
WARNING is learned, so after one a second call compares against the new state.
To gate a change you just made, call `verify`: it never changes the baseline,
so it keeps failing until the data is fixed. A table you just created has no
baseline yet; name it in `verify(tables=[...])` and an empty table, a column
NULL in every row or a duplicate key in `id` / `<singular table>_id` fails the
gate as CRITICAL.

When the server's connection is a dev schema or database you just built,
`verify(against="postgresql://db.example.com/shop")` compares it with
production's baseline, if production was profiled into the same store. Give
`against` as a scope (adapter, host, port and path): a connection string is
refused, so credentials never travel in a tool call. Only records saved under
that scope count, freshness is not compared, and a scope with no baseline is
an error rather than a pass. Add `ignore=["volume"]` when the build ran on
sampled data: row counts are not compared and the column checks only flag
what a sample can't explain. See
[Compare a build with production](https://rbmuller.github.io/scherlok/ci-cd/#compare-a-build-with-production-verify-against).

`watch`, `check` and `verify` each record their run, passing or not, as
`mcp:watch`, `mcp:check` and `mcp:verify`; `scherlok history --runs` lists
them.

After building a table from another, call `reconcile` to check that it adds
up: `reconcile(source="orders", target="orders_enriched",
key_columns=["order_id"], sum_columns=["amount"])`. Both tables must be
listed by `list_tables`, and every key and sum column must exist in both.
It reads the warehouse only: no profile, anomaly or run is saved. The result
holds counts and totals, never a key value, and a database error names only
its type, since its text can quote a row value. Up to 32 key and 32 sum
columns. See [Reconcile](https://rbmuller.github.io/scherlok/reconcile/).

Baselines and history belong to the database of the configured connection,
so pointing the server at staging never moves the production baseline, even
when both share one profile store.

There is no `accept` tool, deliberately: an agent can't make its own change
the new baseline with a tool call. A person runs `scherlok accept --table NAME
--reason "..."`, which records who approved and why. To stop the tools from
writing baselines at all, set `SCHERLOK_BASELINE_LOCKED=1` in the server's
`env` (or run `scherlok config --baseline-locked`): `watch` and `check` then
compare and record anomalies and runs without saving profiles, and
`investigate` fails with an error naming `scherlok accept`.

Every anomaly from `watch`, `verify`, `history` and `reconcile` carries structured fields
next to `table`, `type`, `severity` and `message`, so the agent does not have
to parse the sentence to learn what broke:

```json
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
```

A field that does not apply is `null`. `baseline` and `current` are
aggregates or column types, never a value read from a row; `hint` is a fixed
sentence per anomaly type. Anomalies recorded by an older release have every
field `null` in `history`. See the
[field reference](https://rbmuller.github.io/scherlok/ci-cd/#structured-anomaly-fields).

## Security model

- **Credentials never reach the model.** The connection string is read
  server-side from `SCHERLOK_CONNECTION` / `scherlok config`. No tool accepts a
  connection string as an argument.
- **Read-only on the warehouse.** Every operation is `SELECT` /
  `information_schema` only; Scherlok writes solely to its local profile store
  (`~/.scherlok/profiles.db`). There is **no arbitrary-SQL tool** — the agent
  can't run statements you didn't expose.
- **Bounded output.** Tool results cap table and anomaly counts so a single
  call can't blow the agent's context budget.
- **No tool approves a baseline.** `accept` is CLI-only and, with baselines
  locked, no tool writes one. This is a guardrail, not a security boundary:
  an agent with shell access can run the CLI or edit the SQLite file. Real
  enforcement means running `scherlok verify` in CI against a store the agent
  has no credentials for, with `accept` run from a workflow a person triggers.
