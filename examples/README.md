# Scherlok Quick Demo

Try Scherlok in 2 minutes with a local PostgreSQL database.

## Setup

```bash
# Start PostgreSQL with sample data
cd examples
docker compose up -d

# Install Scherlok
pip install scherlok

# Connect
scherlok connect postgres://scherlok:scherlok@localhost:5433/demo
```

## Try it

```bash
# 1. Profile your data
scherlok investigate

# 2. Check health
scherlok status

# 3. See the full report
scherlok report

# 4. Watch for anomalies (should be clean on first run)
scherlok watch
```

## Use it from Claude (MCP)

The same demo Postgres works as a Scherlok backend for Claude Code / Claude Desktop. Once `docker compose up -d` is running:

**Claude Desktop** — add to `~/Library/Application Support/Claude/claude_desktop_config.json` (macOS) or the equivalent on Windows/Linux:

```json
{
  "mcpServers": {
    "scherlok": {
      "command": "scherlok-mcp",
      "env": {
        "SCHERLOK_CONNECTION": "postgres://scherlok:scherlok@localhost:5433/demo"
      }
    }
  }
}
```

**Claude Code** — drop a `.mcp.json` in your project root with the same content, or run:

```bash
claude mcp add scherlok --env SCHERLOK_CONNECTION="postgres://scherlok:scherlok@localhost:5433/demo" -- scherlok-mcp
```

Restart the client, then try prompts like:

- *"Use scherlok to list the tables on this connection."*
- *"Profile every table to set the baseline."*
- *"Watch for anomalies and report any CRITICAL ones."*
- *"Did anything break in the last 7 days?"*

Then delete some rows (`docker exec -i examples-postgres-1 psql -U scherlok demo -c "DELETE FROM orders WHERE id > 7"`) and ask again — Claude will call `watch` and surface the volume drop, with `Affects N downstream` context from the lineage layer.

The connection string is read by the MCP server only — Claude never sees the credentials.

## Simulate an anomaly

```bash
# Delete half the orders (simulates a data pipeline failure)
docker exec -i examples-postgres-1 psql -U scherlok demo -c "DELETE FROM orders WHERE id > 7"

# Run watch again — Scherlok will detect the volume drop
scherlok watch
```

## Cleanup

```bash
docker compose down -v
```
