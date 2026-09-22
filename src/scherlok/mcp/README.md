# Scherlok MCP server

Expose Scherlok's data-quality checks to an AI coding agent (Claude Code, Claude
Desktop, Cursor, …) as [MCP](https://modelcontextprotocol.io) tools. The agent
can profile your warehouse, detect anomalies, and gate on them — with structured
results it reasons over, instead of shelling out and parsing text.

## Install

### Claude Desktop: one click

Download `scherlok-<version>.mcpb` from the [latest release](https://github.com/rbmuller/scherlok/releases/latest) and open it. Claude Desktop installs the extension, asks for one setting — your database connection string, stored as a secret — and the tools appear in the conversation.

The bundle carries no dependencies of its own: on first launch `uv` installs the pinned `scherlok` release from a lockfile shipped inside it (about 90 MB, a few seconds, cached afterwards). It covers PostgreSQL, MySQL and DuckDB; for BigQuery and Snowflake use the CLI below, whose extras need a build toolchain on some platforms.

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
| `investigate` | `tables?` | profile tables, store the baseline |
| `watch` | `tables?` | detect anomalies vs baseline (type/severity/message) |
| `status` | — | connection (redacted), tables visible, anomalies in last 30 days |
| `history` | `days?` | anomalies recorded in the last N days |
| `check` | `fail_on?` | CI-style pass/fail gate (`critical` default, or `warning`) |

Typical agent flow: `investigate` once to set the baseline, then `watch` (or
`check`) on later runs to catch drift.

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
