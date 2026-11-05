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

import inspect
from typing import Any

from scherlok import __version__
from scherlok.alerter.exitcode import FAIL_ON_CRITICAL, fails_gate, normalize_fail_on
from scherlok.config import ENV_CONNECTION, ScherlokConfig
from scherlok.connectors import get_connector
from scherlok.connectors.base import BaseConnector
from scherlok.reconcile import reconcile_tables, validate_reconcile_args
from scherlok.runs import (
    COMMAND_CHECK,
    COMMAND_VERIFY,
    COMMAND_WATCH,
    RunRecord,
    mcp_command,
)
from scherlok.service import (
    ANOMALIES_KEY,
    TABLES_VERIFIED_KEY,
    TABLES_WITHOUT_BASELINE_KEY,
    anomaly_to_dict,
    blocks_baseline,
    connection_scope,
    profile_and_detect,
    redact_connection,
    select_tables,
    severity_counts,
    verify_tables,
)
from scherlok.store.sqlite import ProfileStore

SERVER_NAME = "scherlok"

# Supported `mcp` releases, kept in step with the dependency in pyproject.toml.
SUPPORTED_MCP_RANGE = "mcp >=1.2,<3"
SUPPORTED_MCP_SPEC = "mcp>=1.2,<3"

# Output caps — keep any single tool result within a sane token budget.
MAX_TABLES = 1000
MAX_ANOMALIES = 200
_VERIFY_CAPS = {
    ANOMALIES_KEY: MAX_ANOMALIES,
    TABLES_VERIFIED_KEY: MAX_TABLES,
    TABLES_WITHOUT_BASELINE_KEY: MAX_TABLES,
}

# Per table in `investigate`: false when a CRITICAL anomaly kept the old baseline.
BASELINE_UPDATED_KEY = "baseline_updated"

# Commands the run records of these tools carry.
MCP_WATCH = mcp_command(COMMAND_WATCH)
MCP_CHECK = mcp_command(COMMAND_CHECK)
MCP_VERIFY = mcp_command(COMMAND_VERIFY)


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


def _open_store(connection_string: str | None) -> ProfileStore:
    """Open the profile store for the database a connection points at.

    Baselines are kept per database; see `service.connection_scope`.
    """
    return ProfileStore(scope=connection_scope(connection_string))


def _watch_tables(
    tables: list[str] | None, command: str, fail_on: str = FAIL_ON_CRITICAL
) -> tuple[int, list[dict]]:
    """Profile, detect and save profiles and anomalies for `tables` (None = all).

    A table with a CRITICAL anomaly keeps its baseline: its profiles are not
    saved, only the anomalies (#116). The run is recorded under `command`,
    judged by `fail_on`, passing or not.
    Returns ``(tables_watched, anomalies)`` with the raw anomaly dicts, so
    `check` can decide the gate on every anomaly, not on the capped output.
    """
    connector = _connect()
    targets = select_tables(tables, connector.list_tables())
    store = _open_store(connector.connection_string)
    try:
        run = RunRecord(command=command, scope=store.scope, fail_on=fail_on)
        all_anomalies: list[dict] = []
        for table in targets:
            anomalies, _vol = profile_and_detect(connector, store, table)
            all_anomalies.extend(anomalies)
        if all_anomalies:
            store.save_anomalies(all_anomalies, run_id=run.run_id)
        store.save_run(run.finish(targets, all_anomalies).to_dict())
        return len(targets), all_anomalies
    finally:
        store.close()


# --------------------------------------------------------------------------
# Tool implementations — plain functions so they're unit-testable without the
# protocol layer. `build_server` registers them on an MCPServer instance.
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
    anomalies are reported here. Run `watch` later to detect drift. A table
    whose current state has a CRITICAL anomaly against its baseline keeps that
    baseline, with `baseline_updated` false: a broken state is not learned.
    """
    connector = _connect()
    targets = select_tables(tables, connector.list_tables())
    store = _open_store(connector.connection_string)
    try:
        profiled = []
        for table in targets:
            anomalies, vol = profile_and_detect(connector, store, table)
            profiled.append({
                "table": table,
                "row_count": vol.get("row_count"),
                BASELINE_UPDATED_KEY: not blocks_baseline(anomalies),
            })
        return {"profiled": len(profiled), "tables": profiled}
    finally:
        store.close()


def watch(tables: list[str] | None = None) -> dict[str, Any]:
    """Profile tables and detect anomalies against the stored baseline.

    Returns the anomalies found (type, severity, message per table), capped.
    Each also carries `column`, `metric`, `baseline`, `current`, `change_pct`
    and a fixed `hint` on where to look, null when one does not apply.
    Pass `tables` to limit scope, or omit to watch everything. A table with no
    prior baseline gets the new-table checks instead (`empty_table`,
    `all_null_column`, `duplicate_key` as WARNING, `constant_column` as INFO),
    and its baseline is set for next time. A table with a CRITICAL anomaly
    keeps its baseline, so the anomaly is reported again on every call until
    the data recovers; WARNING and INFO findings are learned.
    """
    tables_watched, anomalies = _watch_tables(tables, MCP_WATCH)
    return {
        "tables_watched": tables_watched,
        "anomaly_count": len(anomalies),
        **severity_counts(anomalies),
        ANOMALIES_KEY: [anomaly_to_dict(a) for a in anomalies[:MAX_ANOMALIES]],
        "truncated": len(anomalies) > MAX_ANOMALIES,
    }


def status() -> dict[str, Any]:
    """Report the current monitoring state.

    Returns the connection target (password redacted), how many tables the
    connection can see, and how many anomalies were recorded in the last 30
    days. A quick health glance without profiling anything.
    """
    connector = _connect()
    store = _open_store(connector.connection_string)
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
    Only the configured connection's anomalies are returned. They carry the
    same fields as `watch`; those recorded before the fields existed have
    them all null.
    """
    store = _open_store(ScherlokConfig.load().get_connection_string())
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


def check(fail_on: str = FAIL_ON_CRITICAL) -> dict[str, Any]:
    """Run a watch over all tables and return a CI-style pass/fail gate.

    `fail_on` is `"critical"` (default — fail only on CRITICAL) or `"warning"`
    (stricter — fail on WARNING or worse). Mirrors `scherlok ci`. Returns
    `passed` plus the severity counts the decision was based on. Like `watch`,
    it saves what it sees except a table with a CRITICAL anomaly, so a check
    failing on CRITICAL keeps failing until the data recovers, while one
    failing on a WARNING passes next time; `verify` saves nothing.
    """
    threshold = normalize_fail_on(fail_on)
    _tables_watched, anomalies = _watch_tables(None, MCP_CHECK, threshold)
    return {
        "passed": not fails_gate(anomalies, threshold),
        "fail_on": threshold,
        **severity_counts(anomalies),
    }


def verify(tables: list[str] | None = None, fail_on: str = FAIL_ON_CRITICAL) -> dict[str, Any]:
    """Compare tables with the stored baseline and return a pass/fail gate, leaving it unchanged.

    Read-only, unlike `watch` and `check`: no profiles or anomalies are
    written, so retrying after a failure keeps failing until the data is
    fixed; only the run itself is recorded. Pass `tables` to limit scope, or
    omit to verify everything. `fail_on` is `"critical"` (default) or
    `"warning"`. Tables with no stored baseline are listed in
    `tables_without_baseline` and never counted as verified. Naming one in
    `tables` says it was just built, so an empty table, an all-NULL column or
    a duplicate key there is CRITICAL; unnamed, they stay WARNING. Once the
    data is right, `investigate` sets a baseline.
    """
    threshold = normalize_fail_on(fail_on)
    connector = _connect()
    store = _open_store(connector.connection_string)
    try:
        run = RunRecord(command=MCP_VERIFY, scope=store.scope, fail_on=threshold)
        result = verify_tables(connector, store, tables, threshold)
        store.save_run(run.finish(result.tables_checked, result.anomalies).to_dict())
    finally:
        store.close()
    payload = result.to_dict()
    payload["truncated"] = any(len(payload[key]) > cap for key, cap in _VERIFY_CAPS.items())
    payload.update({key: payload[key][:cap] for key, cap in _VERIFY_CAPS.items()})
    return payload


def reconcile(
    source: str,
    target: str,
    key_columns: list[str] | None = None,
    sum_columns: list[str] | None = None,
    tolerance: float = 0.0,
) -> dict[str, Any]:
    """Check that an output table adds up against the input it was built from.

    Read-only: compares `target` with `source` on the configured connection
    and saves nothing. Always compares row counts. With `key_columns` (unique
    in the target; several for a composite key) it also counts keys repeated
    in the target (a join that fans out), source keys missing from the target
    and target keys the source lacks. With `sum_columns` it compares each
    column's total, which may differ by at most `tolerance` (absolute).
    Returns counts and totals only, never a key value; `passed` is false on
    any mismatch, each a CRITICAL anomaly with a fix hint. Both tables must
    be listed by `list_tables`.
    """
    keys, sums = validate_reconcile_args(source, target, key_columns, sum_columns, tolerance)
    connector = _connect()
    select_tables([source, target], connector.list_tables())
    for table in (source, target):
        _require_columns(connector, table, [*keys, *sums])
    result = reconcile_tables(
        connector, source, target, key_columns=keys, sum_columns=sums, tolerance=tolerance
    )
    payload = result.to_dict()
    payload["truncated"] = len(payload[ANOMALIES_KEY]) > MAX_ANOMALIES
    payload[ANOMALIES_KEY] = payload[ANOMALIES_KEY][:MAX_ANOMALIES]
    return payload


def _require_columns(connector: BaseConnector, table: str, columns: list[str]) -> None:
    """Raise ValueError naming the columns `table` lacks, compared case-insensitively."""
    visible = {column["name"].lower() for column in connector.get_columns(table)}
    missing = [column for column in columns if column.lower() not in visible]
    if missing:
        raise ValueError(f"Columns not found in {table}: {missing}")


_TOOLS = [list_tables, investigate, watch, status, history, check, verify, reconcile]


def _installed_mcp_version() -> str | None:
    """Return the installed `mcp` version, or None when the package is absent."""
    from importlib.metadata import PackageNotFoundError, version

    try:
        return version("mcp")
    except PackageNotFoundError:
        return None


def _server_class() -> Any:
    """Return the server class of whichever supported `mcp` release is installed.

    `FastMCP` was renamed to `MCPServer` in mcp 2.0, so both names are tried.
    A missing package and an unsupported release are different problems for
    the operator, so they get different messages.
    """
    try:
        from mcp.server import MCPServer

        return MCPServer
    except ImportError:
        pass
    try:
        from mcp.server.fastmcp import FastMCP  # mcp <2.0

        return FastMCP
    except ImportError as exc:
        installed = _installed_mcp_version()
        if installed is None:
            raise ImportError(
                "The MCP server requires the 'mcp' package, which ships with "
                "scherlok by default. Re-install scherlok to pull it in: "
                "pip install --upgrade scherlok"
            ) from exc
        raise ImportError(
            f"mcp {installed} is installed but exposes neither MCPServer "
            f"(mcp 2.x) nor FastMCP (mcp 1.x). Scherlok supports "
            f"{SUPPORTED_MCP_RANGE}: pip install '{SUPPORTED_MCP_SPEC}'"
        ) from exc


def build_server() -> Any:
    """Construct and return an MCP server with all tools registered.

    `mcp` is imported lazily so importing this module never depends on it.
    """
    server_cls = _server_class()

    # mcp 2.x takes the server version and reports it in `serverInfo`; mcp 1.x
    # FastMCP has no such parameter and would reject the keyword.
    kwargs: dict[str, Any] = {}
    if "version" in inspect.signature(server_cls).parameters:
        kwargs["version"] = __version__

    server = server_cls(SERVER_NAME, **kwargs)
    for fn in _TOOLS:
        server.tool()(fn)
    return server


def main() -> None:
    """Console-script entry point: build the server and serve over stdio."""
    build_server().run()


if __name__ == "__main__":  # pragma: no cover
    main()
