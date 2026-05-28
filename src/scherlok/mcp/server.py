"""Scherlok MCP server.

Exposes Scherlok's read-only data-quality operations as MCP tools so an AI
coding agent (Claude Code, Claude Desktop, …) can profile a warehouse and
detect anomalies directly, with structured results.

Security model (see src/scherlok/mcp/README.md):
  - The connection string is resolved server-side from `SCHERLOK_CONNECTION`
    or `scherlok config`. It is never a tool argument — the model never sees
    or supplies credentials.
  - Every operation is read-only on the warehouse (SELECT / information_schema).
    There is no arbitrary-SQL tool.
  - Tool output is bounded so a single call can't exhaust the agent's context.
"""

from __future__ import annotations

from typing import Any

from scherlok.config import ENV_CONNECTION, ScherlokConfig
from scherlok.connectors import get_connector
from scherlok.connectors.base import BaseConnector
from scherlok.service import anomaly_to_dict, profile_and_detect, redact_connection
from scherlok.store.sqlite import ProfileStore

SERVER_NAME = "scherlok"

# Output caps — keep any single tool result within a sane token budget.
MAX_TABLES = 1000
MAX_ANOMALIES = 200


class ConnectionNotConfiguredError(RuntimeError):
    """Raised when no connection string is available server-side."""


def _resolve_connection() -> str:
    """Return the server-side connection string or raise a clear, actionable error."""
    conn = ScherlokConfig.load().get_connection_string()
    if not conn:
        raise ConnectionNotConfiguredError(
            f"No connection configured. Set the {ENV_CONNECTION} environment "
            "variable on the MCP server, or run `scherlok connect <url>` once "
            "to save it. The model never supplies the connection string."
        )
    return conn


def _connect() -> BaseConnector:
    """Open a connected connector from the server-side connection, or raise."""
    connector = get_connector(_resolve_connection())
    if not connector.connect():
        err = getattr(connector, "last_error", None) or "unknown error"
        raise ConnectionError(f"Failed to connect: {err}")
    return connector


def _select(tables: list[str] | None, visible: list[str]) -> list[str]:
    """Resolve the requested table subset against what the connection can see.

    Unknown names raise rather than silently profiling nothing, so the agent
    gets told it asked for a table that isn't there.
    """
    if not tables:
        return visible
    visible_set = set(visible)
    missing = [t for t in tables if t not in visible_set]
    if missing:
        raise ValueError(f"Tables not found on this connection: {missing}")
    return tables


# --------------------------------------------------------------------------
# Tool implementations — plain functions so they're unit-testable without the
# protocol layer. `build_server` registers them on a FastMCP instance.
# --------------------------------------------------------------------------

def list_tables() -> dict[str, Any]:
    """List the tables visible to the configured warehouse connection.

    Returns the table names (capped) and a total count. Use this to discover
    what can be profiled before calling `investigate` or `watch`.
    """
    connector = _connect()
    tables = connector.list_tables()
    return {
        "count": len(tables),
        "tables": tables[:MAX_TABLES],
        "truncated": len(tables) > MAX_TABLES,
    }


def investigate(tables: list[str] | None = None) -> dict[str, Any]:
    """Profile tables and store them as the baseline for future anomaly checks.

    Pass `tables` to limit to specific tables, or omit to profile everything
    visible. The first profile of a table establishes its baseline; no
    anomalies are reported here. Run `watch` later to detect drift.
    """
    connector = _connect()
    targets = _select(tables, connector.list_tables())
    store = ProfileStore()
    try:
        profiled = []
        for table in targets:
            _anomalies, vol = profile_and_detect(connector, store, table)
            profiled.append({"table": table, "row_count": vol.get("row_count")})
        return {"profiled": len(profiled), "tables": profiled}
    finally:
        store.close()


def watch(tables: list[str] | None = None) -> dict[str, Any]:
    """Profile tables and detect anomalies against the stored baseline.

    Returns the anomalies found (type, severity, message per table), capped.
    Pass `tables` to limit scope, or omit to watch everything. Tables with no
    prior baseline are profiled silently (their baseline is set for next time).
    """
    connector = _connect()
    targets = _select(tables, connector.list_tables())
    store = ProfileStore()
    try:
        all_anomalies: list[dict] = []
        for table in targets:
            anomalies, _vol = profile_and_detect(connector, store, table)
            all_anomalies.extend(anomalies)
        if all_anomalies:
            store.save_anomalies(all_anomalies)
        critical = sum(1 for a in all_anomalies if "CRITICAL" in str(a.get("severity")))
        warning = sum(1 for a in all_anomalies if "WARNING" in str(a.get("severity")))
        return {
            "tables_watched": len(targets),
            "anomaly_count": len(all_anomalies),
            "critical": critical,
            "warning": warning,
            "anomalies": [anomaly_to_dict(a) for a in all_anomalies[:MAX_ANOMALIES]],
            "truncated": len(all_anomalies) > MAX_ANOMALIES,
        }
    finally:
        store.close()


def status() -> dict[str, Any]:
    """Report the current monitoring state.

    Returns the connection target (password redacted), how many tables the
    connection can see, and how many anomalies were recorded in the last 30
    days. A quick health glance without profiling anything.
    """
    connector = _connect()
    store = ProfileStore()
    try:
        recent = store.get_anomaly_history(days=30)
        return {
            "connection": redact_connection(_resolve_connection()),
            "tables_visible": len(connector.list_tables()),
            "anomalies_last_30d": len(recent),
        }
    finally:
        store.close()


def history(days: int = 30) -> dict[str, Any]:
    """Return anomalies recorded in the last `days` days (capped).

    Reads from the local profile store — does not re-profile the warehouse.
    """
    store = ProfileStore()
    try:
        rows = store.get_anomaly_history(days=days)
        return {
            "days": days,
            "count": len(rows),
            "anomalies": [anomaly_to_dict(a) for a in rows[:MAX_ANOMALIES]],
            "truncated": len(rows) > MAX_ANOMALIES,
        }
    finally:
        store.close()


def check(fail_on: str = "critical") -> dict[str, Any]:
    """Run a watch over all tables and return a CI-style pass/fail gate.

    `fail_on` is `"critical"` (default — fail only on CRITICAL) or `"warning"`
    (stricter — fail on WARNING or worse). Mirrors `scherlok ci`. Returns
    `passed` plus the severity counts the decision was based on.
    """
    threshold = fail_on.lower()
    if threshold not in ("critical", "warning"):
        raise ValueError("fail_on must be 'critical' or 'warning'")
    result = watch(tables=None)
    if threshold == "warning":
        passed = result["critical"] == 0 and result["warning"] == 0
    else:
        passed = result["critical"] == 0
    return {
        "passed": passed,
        "fail_on": threshold,
        "critical": result["critical"],
        "warning": result["warning"],
    }


_TOOLS = [list_tables, investigate, watch, status, history, check]


def build_server() -> Any:
    """Construct and return a FastMCP server with all tools registered.

    Imported lazily so `pip install scherlok` (without the `[mcp]` extra)
    doesn't error on a missing `mcp` dependency until the server is built.
    """
    try:
        from mcp.server.fastmcp import FastMCP
    except ImportError as exc:  # pragma: no cover - exercised via packaging
        raise ImportError(
            "The MCP server requires the 'mcp' package, which ships with "
            "scherlok by default. Re-install scherlok to pull it in: "
            "pip install --upgrade scherlok"
        ) from exc

    server = FastMCP(SERVER_NAME)
    for fn in _TOOLS:
        server.tool()(fn)
    return server


def main() -> None:
    """Console-script entry point: build the server and serve over stdio."""
    build_server().run()


if __name__ == "__main__":  # pragma: no cover
    main()
