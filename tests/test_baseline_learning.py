"""Tests for #116: a reading with a CRITICAL anomaly never becomes the baseline.

Every run used to save its profiles, so after a CRITICAL anomaly the broken
state became the reference and the alert stopped although nothing was fixed.
The two scenarios from the issue are reproduced with synthetic row counts,
each reading saved the way a scheduled `watch` saves it: 30 prior runs, then
a drop to 30% of the rows.
"""

from __future__ import annotations

import importlib.util
import sqlite3
from itertools import cycle, islice
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
from typer.testing import CliRunner

from scherlok.cli import app
from scherlok.detector.severity import Severity
from scherlok.service import (
    BASELINE_BLOCKING_SEVERITY,
    VOLUME_PROFILE,
    blocks_baseline,
    profile_and_detect,
)
from scherlok.store.sqlite import ProfileStore

_HAS_DUCKDB = importlib.util.find_spec("duckdb") is not None
needs_duckdb = pytest.mark.skipif(not _HAS_DUCKDB, reason="duckdb not installed")

runner = CliRunner(env={"NO_COLOR": "1"})

TABLE = "orders"
OTHER_TABLE = "customers"
STORE_FILENAME = "profiles.db"
PRIOR_RUNS = 30
# Past run 16, where the varying table used to go quiet.
BROKEN_RUNS = 30
BASELINE_ROWS = 1000
BROKEN_ROWS = 300  # 30% of the rows: a CRITICAL volume_drop
WARNING_ROWS = 750  # a 25% drop: a WARNING volume_drop
NATURAL_VARIATION = (-20, -10, 0, 10, 20)
VOLUME_DROP = "volume_drop"
KEPT_BASELINE_HINT = "Baseline kept"
FIXTURES = Path(__file__).parent / "fixtures" / "dbt"
PG_PROJECT = FIXTURES / "jaffle_shop_postgres"


class FakeConnector:
    """Tables whose row counts a test sets between runs; nothing else changes."""

    def __init__(self, **row_counts: int):
        self.row_counts = dict(row_counts)

    def list_tables(self):
        return list(self.row_counts)

    def get_row_count(self, table):
        return self.row_counts[table]

    def get_columns(self, table):
        return [{"name": "id", "type": "integer", "nullable": False}]

    def get_column_stats(self, table, column):
        return {"mean": None, "stddev": None, "null_count": 0, "distinct_count": 100}

    def get_last_modified(self, table):
        return None  # no freshness signal, so only volume can move


@pytest.fixture()
def store(tmp_path):
    profile_store = ProfileStore(tmp_path / STORE_FILENAME)
    yield profile_store
    profile_store.close()


def _run(store: ProfileStore, connector: FakeConnector, rows: int) -> list[dict]:
    """One scheduled run of TABLE at `rows` rows, saved as `watch` saves it."""
    connector.row_counts[TABLE] = rows
    anomalies, _ = profile_and_detect(connector, store, TABLE)
    return anomalies


def _critical_types(anomalies: list[dict]) -> list[str]:
    return [a["type"] for a in anomalies if a["severity"] == Severity.CRITICAL]


def _learned_rows(store: ProfileStore, table: str = TABLE) -> list[int]:
    return [p["row_count"] for p in store.get_profile_history(table, VOLUME_PROFILE, days=None)]


def _learn(store: ProfileStore, connector: FakeConnector, counts: list[int]) -> None:
    for rows in counts:
        _run(store, connector, rows)


STABLE_COUNTS = [BASELINE_ROWS] * PRIOR_RUNS
VARYING_COUNTS = [
    BASELINE_ROWS + offset for offset in islice(cycle(NATURAL_VARIATION), PRIOR_RUNS)
]


# ----- the two scenarios from the issue ---------------------------------------

def test_stable_table_reports_the_drop_on_every_run(store):
    connector = FakeConnector(orders=BASELINE_ROWS)
    _learn(store, connector, STABLE_COUNTS)

    runs = [_run(store, connector, BROKEN_ROWS) for _ in range(BROKEN_RUNS)]

    assert all(VOLUME_DROP in _critical_types(anomalies) for anomalies in runs)
    assert BROKEN_ROWS not in _learned_rows(store)
    assert store.get_latest_profile(TABLE, VOLUME_PROFILE)["row_count"] == BASELINE_ROWS


def test_varying_table_still_reports_the_drop_from_run_16_on(store):
    connector = FakeConnector(orders=BASELINE_ROWS)
    _learn(store, connector, VARYING_COUNTS)

    runs = [_run(store, connector, BROKEN_ROWS) for _ in range(BROKEN_RUNS)]

    quiet_runs = [
        number for number, anomalies in enumerate(runs, 1)
        if VOLUME_DROP not in _critical_types(anomalies)
    ]
    assert quiet_runs == []
    assert BROKEN_ROWS not in _learned_rows(store)


# ----- what is still learned ---------------------------------------------------

def test_a_warning_only_run_still_updates_the_baseline(store):
    connector = FakeConnector(orders=BASELINE_ROWS)
    _learn(store, connector, STABLE_COUNTS)

    warning_run = _run(store, connector, WARNING_ROWS)
    next_run = _run(store, connector, WARNING_ROWS)

    assert [(a["type"], a["severity"]) for a in warning_run] == [(VOLUME_DROP, Severity.WARNING)]
    assert _learned_rows(store)[:2] == [WARNING_ROWS, WARNING_ROWS]
    assert next_run == []  # the new state is the reference now


def test_the_alert_stops_once_the_data_recovers(store):
    connector = FakeConnector(orders=BASELINE_ROWS)
    _learn(store, connector, VARYING_COUNTS)
    for _ in range(5):
        assert _critical_types(_run(store, connector, BROKEN_ROWS))

    recovered = [_run(store, connector, BASELINE_ROWS) for _ in range(3)]

    assert recovered == [[], [], []]
    assert _learned_rows(store)[:3] == [BASELINE_ROWS] * 3


def test_only_the_table_with_the_critical_anomaly_keeps_its_baseline(store):
    connector = FakeConnector(orders=BASELINE_ROWS, customers=BASELINE_ROWS)
    for table in connector.list_tables():
        profile_and_detect(connector, store, table)
    connector.row_counts.update(orders=BROKEN_ROWS, customers=WARNING_ROWS)

    for table in connector.list_tables():
        profile_and_detect(connector, store, table)

    assert _learned_rows(store, TABLE) == [BASELINE_ROWS]
    assert _learned_rows(store, OTHER_TABLE) == [WARNING_ROWS, BASELINE_ROWS]


@pytest.mark.parametrize(
    ("severity", "saved"),
    [
        (Severity.CRITICAL, []),
        ("CRITICAL", []),  # as read back from the anomaly history
        (Severity.WARNING, ["distribution:id", "volume", "schema", "freshness"]),
        (Severity.INFO, ["distribution:id", "volume", "schema", "freshness"]),
    ],
)
def test_a_critical_anomaly_of_any_type_saves_none_of_the_tables_profiles(severity, saved):
    store = MagicMock()
    store.get_profile_history.return_value = []
    store.get_latest_profile.side_effect = lambda table, profile_type: {
        "volume": {"row_count": BASELINE_ROWS},
        "schema": {"columns": []},
        "distribution:id": {
            "mean": None, "stddev": None, "null_count": 0, "null_rate": 0.0,
            "distinct_count": 100,
        },
    }.get(profile_type)
    added = {"table": TABLE, "type": "column_added", "severity": severity, "message": "m"}

    with patch("scherlok.service.detect_schema_drift", return_value=[added]):
        anomalies, _ = profile_and_detect(FakeConnector(orders=BASELINE_ROWS), store, TABLE)

    assert added in anomalies
    assert [c.args[1] for c in store.save_profile.call_args_list] == saved
    store.save_anomalies.assert_not_called()  # anomalies are the caller's to save


def test_blocks_baseline_only_on_critical():
    def anomaly(severity):
        return {"severity": severity}

    assert BASELINE_BLOCKING_SEVERITY is Severity.CRITICAL
    assert blocks_baseline([anomaly(Severity.INFO), anomaly(Severity.CRITICAL)])
    assert not blocks_baseline([anomaly(Severity.WARNING), anomaly(Severity.INFO)])
    assert not blocks_baseline([])


# ----- end to end: watch, ci, check, dbt and the MCP tools ----------------------

def _execute(path: Path, *statements: str) -> None:
    import duckdb

    conn = duckdb.connect(str(path))
    try:
        for sql in statements:
            conn.execute(sql)
    finally:
        conn.close()


@pytest.fixture()
def warehouse(tmp_path, monkeypatch) -> Path:
    path = tmp_path / "warehouse.duckdb"
    _execute(
        path,
        "CREATE TABLE orders (id INTEGER, amount DOUBLE)",
        f"INSERT INTO orders SELECT i, i * 1.5 FROM range(1, {BASELINE_ROWS + 1}) t(i)",
    )
    monkeypatch.setenv("SCHERLOK_CONNECTION", f"duckdb:///{path}")
    return path


def _break(path: Path) -> None:
    _execute(path, f"DELETE FROM orders WHERE id > {BROKEN_ROWS}")


def _recover(path: Path) -> None:
    _execute(
        path,
        "INSERT INTO orders SELECT i, i * 1.5 "
        f"FROM range({BROKEN_ROWS + 1}, {BASELINE_ROWS + 1}) t(i)",
    )


def _count(home: Path, table: str) -> int:
    conn = sqlite3.connect(home / STORE_FILENAME)
    try:
        return conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
    finally:
        conn.close()


@needs_duckdb
def test_watch_keeps_failing_until_the_data_recovers(warehouse, isolated_scherlok_dir):
    assert runner.invoke(app, ["investigate"]).exit_code == 0
    profiles_before = _count(isolated_scherlok_dir, "profiles")
    _break(warehouse)

    broken = [runner.invoke(app, ["watch"]) for _ in range(3)]

    assert [r.exit_code for r in broken] == [1, 1, 1]
    assert all(KEPT_BASELINE_HINT in r.stdout and TABLE in r.stdout for r in broken)
    assert _count(isolated_scherlok_dir, "profiles") == profiles_before
    assert _count(isolated_scherlok_dir, "anomalies") >= 3  # every run saved its alert

    _recover(warehouse)
    recovered = runner.invoke(app, ["watch"])

    assert recovered.exit_code == 0
    assert KEPT_BASELINE_HINT not in recovered.stdout
    assert _count(isolated_scherlok_dir, "profiles") > profiles_before


@needs_duckdb
@pytest.mark.parametrize("command", ["ci", "check"])
def test_ci_and_check_keep_failing_on_retry(warehouse, monkeypatch, command):
    connection = f"duckdb:///{warehouse}"
    monkeypatch.delenv("SCHERLOK_CONNECTION")
    assert runner.invoke(app, [command, connection]).exit_code == 0
    _break(warehouse)

    retries = [runner.invoke(app, [command, connection]) for _ in range(3)]

    assert [r.exit_code for r in retries] == [1, 1, 1]


@patch("scherlok.cli.get_connector")
def test_dbt_names_the_models_that_kept_their_baseline(mock_get_connector, tmp_path):
    fake = MagicMock()
    fake.connect.return_value = True
    fake.list_tables.return_value = ["stg_customers", "stg_orders", "fct_orders"]
    mock_get_connector.return_value = fake
    drop = {
        "table": "fct_orders", "type": VOLUME_DROP, "severity": Severity.CRITICAL,
        "message": "Row count dropped 70.0% (1,000 -> 300)",
    }

    def watch_table(_connector, _store, table):
        return ([dict(drop)] if table == "fct_orders" else []), {"row_count": BASELINE_ROWS}

    with patch("scherlok.cli._watch_table", side_effect=watch_table):
        result = runner.invoke(
            app,
            [
                "dbt", "--project-dir", str(PG_PROJECT),
                "--connection-string", "postgresql://u:p@host/db",
            ],
        )

    assert result.exit_code == 1
    hint = next(line for line in result.output.splitlines() if KEPT_BASELINE_HINT in line)
    assert "(1)" in hint and "fct_orders" in hint


@pytest.fixture()
def wired(monkeypatch, tmp_path):
    """The MCP server on an in-memory DuckDB table and a scratch profile store."""
    if not _HAS_DUCKDB:
        pytest.skip("duckdb not installed")
    from scherlok.connectors.duckdb import DuckDBConnector
    from scherlok.mcp import server

    connector = DuckDBConnector("duckdb:///:memory:")
    assert connector.connect() is True
    connector._conn.execute("CREATE TABLE orders (id INTEGER)")
    connector._conn.execute(f"INSERT INTO orders SELECT i FROM range(1, {BASELINE_ROWS + 1}) t(i)")
    monkeypatch.setenv("SCHERLOK_CONNECTION", "duckdb:///:memory:")
    monkeypatch.setattr(server, "get_connector", lambda _cs: connector)
    monkeypatch.setattr(connector, "connect", lambda: True)
    return server, connector


def test_mcp_watch_check_and_investigate_never_learn_a_critical_reading(wired):
    server, connector = wired
    server.investigate()
    connector._conn.execute(f"DELETE FROM orders WHERE id > {BROKEN_ROWS}")

    watches = [server.watch() for _ in range(2)]
    checks = [server.check() for _ in range(2)]
    investigated = server.investigate()

    assert all(out["critical"] >= 1 for out in watches)
    assert all(out["passed"] is False for out in checks)
    assert investigated["tables"] == [
        {"table": TABLE, "row_count": BROKEN_ROWS, "baseline_updated": False}
    ]
    assert server.check()["passed"] is False  # investigate didn't silence it either
