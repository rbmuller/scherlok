"""Choose the tables to monitor (#156): `connect --table` saves them, `--table` overrides.

Runs the CLI and the MCP tools against a real DuckDB file. The connection is
saved by `scherlok connect`, the way a person sets Scherlok up, so
SCHERLOK_CONNECTION stays unset; ~/.scherlok is the per-test scratch
directory set up by the autouse fixture in conftest.py.
"""

from __future__ import annotations

import importlib.util
import json
import sqlite3
from pathlib import Path

import pytest
from typer.testing import CliRunner, Result

from scherlok.cli import app
from scherlok.config import ScherlokConfig
from scherlok.detector.anomaly_types import EMPTY_TABLE
from scherlok.detector.severity import Severity

pytestmark = pytest.mark.skipif(
    importlib.util.find_spec("duckdb") is None, reason="duckdb not installed"
)

runner = CliRunner(env={"NO_COLOR": "1"})

ORDERS = "orders"
CUSTOMERS = "customers"
EVENTS = "legacy_events"
EVERY_TABLE = {ORDERS, CUSTOMERS, EVENTS}
REFUNDS = "refunds"
ROWS = 100
UNKNOWN_CUSTOMERS = "Tables not found on this connection: ['customers']"


def _sql(path: Path, *statements: str) -> None:
    import duckdb

    conn = duckdb.connect(str(path))
    try:
        for statement in statements:
            conn.execute(statement)
    finally:
        conn.close()


@pytest.fixture
def warehouse(tmp_path) -> Path:
    """A DuckDB file with three 100-row tables."""
    path = tmp_path / "warehouse.duckdb"
    _sql(path, *(
        f"CREATE TABLE {name} AS SELECT i AS id, i * 1.5 AS amount FROM range(1, {ROWS + 1}) t(i)"
        for name in sorted(EVERY_TABLE)
    ))
    return path


def _url(warehouse: Path) -> str:
    return f"duckdb:///{warehouse}"


def _run(*args: str) -> Result:
    return runner.invoke(app, list(args))


def _connect(warehouse: Path, *tables: str) -> Result:
    args = ["connect", _url(warehouse)]
    for table in tables:
        args += ["--table", table]
    return _run(*args)


def _profiled(scherlok_dir: Path) -> set[str]:
    """The tables the store holds a profile for."""
    if not (scherlok_dir / "profiles.db").exists():
        return set()
    conn = sqlite3.connect(scherlok_dir / "profiles.db")
    try:
        return {row[0] for row in conn.execute("SELECT DISTINCT table_name FROM profiles")}
    finally:
        conn.close()


def _flat(result: Result) -> str:
    """The output on one line, whatever width Rich wrapped it to."""
    return " ".join(result.output.split())


def _json(result: Result):
    assert result.stdout.strip(), result.output
    return json.loads(result.stdout)


# ----- connect saves the choice ----------------------------------------------

def test_connect_saves_the_tables_and_says_so(warehouse):
    result = _connect(warehouse, ORDERS, CUSTOMERS)

    assert result.exit_code == 0, result.output
    assert "Found 3 tables; monitoring 2: orders, customers." in _flat(result)
    assert ScherlokConfig.load().saved_tables() == [ORDERS, CUSTOMERS]


def test_connect_without_tables_monitors_every_table_again(warehouse):
    _connect(warehouse, ORDERS)

    result = _connect(warehouse)

    assert result.exit_code == 0, result.output
    assert "Found 3 tables." in _flat(result)
    assert ScherlokConfig.load().saved_tables() is None


def test_connect_with_an_unknown_table_saves_nothing(warehouse, isolated_scherlok_dir):
    _connect(warehouse, ORDERS)
    config_file = isolated_scherlok_dir / "config.json"
    before = config_file.read_bytes()

    result = _connect(warehouse, ORDERS, "ghost")

    assert result.exit_code == 1
    assert "Tables not found on this connection: ['ghost']. Nothing was saved." in _flat(result)
    assert config_file.read_bytes() == before


def test_config_shows_the_saved_tables(warehouse):
    _connect(warehouse, ORDERS, CUSTOMERS)
    assert "Tables: orders, customers" in _flat(_run("config"))

    _connect(warehouse)
    assert "Tables: every table" in _flat(_run("config"))


# ----- --table, else the saved tables, else every table ------------------------

def test_investigate_profiles_only_the_saved_tables(warehouse, isolated_scherlok_dir):
    _connect(warehouse, ORDERS)

    result = _run("investigate")

    assert result.exit_code == 0, result.output
    assert _profiled(isolated_scherlok_dir) == {ORDERS}


def test_without_saved_tables_every_table_is_profiled(warehouse, isolated_scherlok_dir):
    _connect(warehouse)

    assert _run("investigate").exit_code == 0
    assert _profiled(isolated_scherlok_dir) == EVERY_TABLE


def test_watch_covers_the_saved_tables(warehouse, isolated_scherlok_dir):
    _connect(warehouse, ORDERS, CUSTOMERS)

    result = _run("watch")

    assert result.exit_code == 0, result.output
    assert _profiled(isolated_scherlok_dir) == {ORDERS, CUSTOMERS}


@pytest.mark.parametrize("command", ["investigate", "watch"])
def test_table_option_overrides_the_saved_tables(warehouse, isolated_scherlok_dir, command):
    _connect(warehouse, ORDERS)

    result = _run(command, "--table", CUSTOMERS)

    assert result.exit_code == 0, result.output
    assert _profiled(isolated_scherlok_dir) == {CUSTOMERS}


@pytest.mark.parametrize("command", ["ci", "check"])
def test_ci_and_check_cover_the_saved_tables_unless_told_otherwise(
    warehouse, isolated_scherlok_dir, command
):
    _connect(warehouse, ORDERS)

    assert _run(command, _url(warehouse)).exit_code == 0
    assert _profiled(isolated_scherlok_dir) == {ORDERS}

    assert _run(command, _url(warehouse), "--table", EVENTS).exit_code == 0
    assert _profiled(isolated_scherlok_dir) == {ORDERS, EVENTS}


def test_verify_covers_the_saved_tables(warehouse):
    _connect(warehouse, ORDERS)
    _run("investigate")

    payload = _json(_run("verify", "--output", "json"))

    assert payload["tables_verified"] == [ORDERS]
    assert payload["tables_without_baseline"] == []


def test_a_saved_table_is_monitored_not_just_built(warehouse):
    """Only a table named with --table counts as just built, so only it escalates."""
    _sql(warehouse, f"CREATE TABLE {REFUNDS} (id INTEGER)")
    _connect(warehouse, REFUNDS)

    saved = _run("verify", "--output", "json")
    named = _run("verify", "--output", "json", "--table", REFUNDS)

    assert saved.exit_code == 0
    assert [(a["type"], a["severity"]) for a in _json(saved)["anomalies"]] == [
        (EMPTY_TABLE, Severity.WARNING)
    ]
    assert named.exit_code == 1
    assert [(a["type"], a["severity"]) for a in _json(named)["anomalies"]] == [
        (EMPTY_TABLE, Severity.CRITICAL)
    ]


@pytest.mark.parametrize("command", ["status", "report"])
def test_status_and_report_show_the_saved_tables(warehouse, command):
    _connect(warehouse, ORDERS)
    _run("investigate")

    rows = _json(_run(command, "--output", "json"))

    assert [row["table"] for row in rows] == [ORDERS]


def test_dashboard_shows_only_the_saved_tables(warehouse, tmp_path):
    _connect(warehouse)
    _run("investigate")
    _connect(warehouse, ORDERS)
    out = tmp_path / "report.html"

    result = _run("dashboard", "--out", str(out))

    assert result.exit_code == 0, result.output
    html = out.read_text(encoding="utf-8")
    assert ORDERS in html
    assert CUSTOMERS not in html
    assert EVENTS not in html


# ----- a saved table that disappeared ------------------------------------------

@pytest.mark.parametrize("command", ["investigate", "watch", "status", "report"])
def test_a_saved_table_that_disappeared_fails_before_any_work(
    warehouse, isolated_scherlok_dir, command
):
    _connect(warehouse, ORDERS, CUSTOMERS)
    _sql(warehouse, f"DROP TABLE {CUSTOMERS}")

    result = _run(command)

    assert result.exit_code == 1
    assert f"{UNKNOWN_CUSTOMERS}. They were saved with scherlok connect --table" in _flat(result)
    assert _profiled(isolated_scherlok_dir) == set()


def test_verify_fails_on_a_saved_table_that_disappeared(warehouse):
    _connect(warehouse, ORDERS, CUSTOMERS)
    _sql(warehouse, f"DROP TABLE {CUSTOMERS}")

    result = _run("verify")

    assert result.exit_code == 1
    assert UNKNOWN_CUSTOMERS in _flat(result)


# ----- the MCP tools -------------------------------------------------------------

def test_mcp_tools_cover_the_saved_tables(warehouse, isolated_scherlok_dir):
    from scherlok.mcp import server

    _connect(warehouse, ORDERS)

    assert server.investigate()["profiled"] == 1
    assert server.watch()["tables_watched"] == 1
    assert server.check()["passed"] is True
    assert server.verify()["tables_verified"] == [ORDERS]
    assert _profiled(isolated_scherlok_dir) == {ORDERS}

    assert server.watch(tables=[EVENTS])["tables_watched"] == 1
    assert _profiled(isolated_scherlok_dir) == {ORDERS, EVENTS}
