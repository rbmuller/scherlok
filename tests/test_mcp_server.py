"""Tests for the Scherlok MCP server.

Strategy: drive the tools end-to-end against a real in-memory DuckDB
connection (shared across calls via a patched `get_connector`) so the
profile→detect→store wiring is exercised for real, not mocked. The protocol
layer is smoke-tested by building the FastMCP server and listing its tools.
"""

from __future__ import annotations

import asyncio
import importlib.util

import pytest

_HAS_DUCKDB = importlib.util.find_spec("duckdb") is not None
_HAS_MCP = importlib.util.find_spec("mcp") is not None

pytestmark = pytest.mark.skipif(not _HAS_DUCKDB, reason="duckdb not installed")


@pytest.fixture()
def duck_connector():
    """A connected in-memory DuckDB connector with two seeded tables."""
    from scherlok.connectors.duckdb import DuckDBConnector

    conn = DuckDBConnector("duckdb:///:memory:")
    assert conn.connect() is True
    conn._conn.execute("CREATE TABLE orders (id INTEGER, amount DOUBLE)")
    conn._conn.execute("INSERT INTO orders VALUES (1, 10.0), (2, 20.0), (3, 30.0)")
    conn._conn.execute("CREATE TABLE users (id INTEGER, email VARCHAR)")
    conn._conn.execute("INSERT INTO users VALUES (1, 'a@x.com'), (2, 'b@x.com')")
    return conn


@pytest.fixture()
def wired(monkeypatch, tmp_path, duck_connector):
    """Point the server at the shared connector + an isolated profile store."""
    from scherlok import mcp as mcp_pkg  # noqa: F401
    from scherlok.mcp import server

    monkeypatch.setenv("SCHERLOK_CONNECTION", "duckdb:///:memory:")
    monkeypatch.setattr(server, "get_connector", lambda _cs: duck_connector)
    monkeypatch.setattr("scherlok.store.sqlite.PROFILES_DB", tmp_path / "profiles.db")
    # The tool's _connect() calls connect() again; on a :memory: DuckDB that
    # would replace the connection with a fresh empty DB and wipe the seeded
    # tables. The connector is already connected, so make re-connect a no-op.
    monkeypatch.setattr(duck_connector, "connect", lambda: True)
    return server


# ----- connection resolution ------------------------------------------------

def test_resolve_connection_raises_when_unset(monkeypatch):
    from scherlok.mcp import server

    monkeypatch.delenv("SCHERLOK_CONNECTION", raising=False)
    monkeypatch.setattr(
        "scherlok.config.ScherlokConfig.get_connection_string", lambda self: ""
    )
    with pytest.raises(server.ConnectionNotConfiguredError):
        server._resolve_connection()


# ----- list_tables ----------------------------------------------------------

def test_list_tables(wired):
    out = wired.list_tables()
    assert out["count"] == 2
    assert set(out["tables"]) == {"orders", "users"}
    assert out["truncated"] is False


# ----- investigate ----------------------------------------------------------

def test_investigate_all(wired):
    out = wired.investigate()
    assert out["profiled"] == 2
    rows = {t["table"]: t["row_count"] for t in out["tables"]}
    assert rows["orders"] == 3
    assert rows["users"] == 2


def test_investigate_subset(wired):
    out = wired.investigate(tables=["orders"])
    assert out["profiled"] == 1
    assert out["tables"][0]["table"] == "orders"


def test_investigate_unknown_table_raises(wired):
    with pytest.raises(ValueError, match="not found"):
        wired.investigate(tables=["ghost"])


# ----- watch ----------------------------------------------------------------

def test_watch_first_run_no_anomalies(wired):
    # First sighting establishes the baseline — nothing to compare against yet.
    out = wired.watch()
    assert out["anomaly_count"] == 0
    assert out["critical"] == 0
    assert out["tables_watched"] == 2


def test_watch_detects_volume_drop_end_to_end(wired, duck_connector):
    # Baseline, then delete most rows so the next watch sees a volume drop.
    wired.investigate(tables=["orders"])
    duck_connector._conn.execute("DELETE FROM orders WHERE id > 1")  # 3 -> 1 row

    out = wired.watch(tables=["orders"])

    assert out["anomaly_count"] >= 1
    assert out["critical"] >= 1
    a = out["anomalies"][0]
    assert a["table"] == "orders"
    assert a["severity"] == "CRITICAL"  # serialized to bare name, not "Severity.CRITICAL"
    assert "type" in a and "message" in a


# ----- status ---------------------------------------------------------------

def test_status_redacts_and_counts(wired, monkeypatch):
    monkeypatch.setenv("SCHERLOK_CONNECTION", "postgresql://alice:secret@h/db")
    out = wired.status()
    assert "secret" not in out["connection"]
    assert "***" in out["connection"]
    assert out["tables_visible"] == 2
    assert out["anomalies_last_30d"] == 0


# ----- history --------------------------------------------------------------

def test_history_reads_store(wired, duck_connector):
    wired.investigate(tables=["orders"])
    duck_connector._conn.execute("DELETE FROM orders WHERE id > 1")
    wired.watch(tables=["orders"])  # persists anomalies

    out = wired.history(days=30)
    assert out["count"] >= 1
    assert out["days"] == 30
    assert all("severity" in a for a in out["anomalies"])


# ----- check ----------------------------------------------------------------

def test_check_passes_when_clean(wired):
    out = wired.check()
    assert out["passed"] is True
    assert out["critical"] == 0


def test_check_fails_on_critical(wired, duck_connector):
    wired.investigate()
    duck_connector._conn.execute("DELETE FROM orders WHERE id > 1")
    out = wired.check(fail_on="critical")
    assert out["passed"] is False
    assert out["critical"] >= 1


def test_check_invalid_fail_on_raises(wired):
    with pytest.raises(ValueError, match="critical.*warning|fail_on"):
        wired.check(fail_on="banana")


# ----- protocol smoke test --------------------------------------------------

@pytest.mark.skipif(not _HAS_MCP, reason="mcp not installed")
def test_build_server_registers_all_tools():
    from scherlok.mcp.server import build_server

    server = build_server()
    tools = asyncio.run(server.list_tools())
    names = {t.name for t in tools}
    assert names == {"list_tables", "investigate", "watch", "status", "history", "check"}
