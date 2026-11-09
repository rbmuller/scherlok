"""#87: freshness from a load-time column when the warehouse has no usable metadata.

DuckDB tracks no per-table modification time, so freshness never fired there.
"""

from __future__ import annotations

import importlib.util
import sqlite3
from datetime import date, datetime
from pathlib import Path

import pytest
from typer.testing import CliRunner

from scherlok.cli import app
from scherlok.detector.anomaly_types import FRESHNESS_CRITICAL

requires_duckdb = pytest.mark.skipif(
    importlib.util.find_spec("duckdb") is None, reason="duckdb not installed"
)

runner = CliRunner(env={"NO_COLOR": "1"})

TABLE = "orders"
STORE_FILENAME = "profiles.db"


def _execute(path: Path, *statements: str) -> None:
    import duckdb

    conn = duckdb.connect(str(path))
    try:
        for sql in statements:
            conn.execute(sql)
    finally:
        conn.close()


def _duckdb_connector(path: Path):
    from scherlok.connectors.duckdb import DuckDBConnector

    connector = DuckDBConnector(f"duckdb:///{path}")
    assert connector.connect()
    return connector


@requires_duckdb
def test_duckdb_latest_timestamp_reads_timestamps_and_dates(tmp_path):
    db = tmp_path / "w.duckdb"
    _execute(
        db,
        "CREATE TABLE t (updated_at TIMESTAMP, loaded_on DATE, empty_at TIMESTAMP)",
        "INSERT INTO t VALUES ('2026-11-01 10:00:00', '2026-11-01', NULL), "
        "('2026-11-02 08:30:00', '2026-11-02', NULL)",
    )
    connector = _duckdb_connector(db)

    assert connector.get_latest_timestamp("t", "updated_at") == datetime(2026, 11, 2, 8, 30)
    assert connector.get_latest_timestamp("t", "loaded_on") == date(2026, 11, 2)
    assert connector.get_latest_timestamp("t", "empty_at") is None


def _anomaly_types(home: Path) -> list[str]:
    conn = sqlite3.connect(home / STORE_FILENAME)
    try:
        rows = conn.execute(
            "SELECT anomaly_type FROM anomalies WHERE table_name = ?", (TABLE,)
        ).fetchall()
    finally:
        conn.close()
    return [anomaly_type for (anomaly_type,) in rows]


@requires_duckdb
def test_watch_flags_a_duckdb_table_that_stopped_updating(
    tmp_path, monkeypatch, isolated_scherlok_dir
):
    db = tmp_path / "warehouse.duckdb"
    _execute(
        db,
        f"CREATE TABLE {TABLE} AS SELECT i AS order_id, "
        "timezone('UTC', now()) - INTERVAL 2 HOUR AS updated_at FROM range(100) t(i)",
    )
    monkeypatch.setenv("SCHERLOK_CONNECTION", f"duckdb:///{db}")
    assert runner.invoke(app, ["investigate"]).exit_code == 0

    # The latest row is now 30 hours old: 15 times the 2 hours of the last run.
    _execute(db, f"UPDATE {TABLE} SET updated_at = updated_at - INTERVAL 28 HOUR")
    result = runner.invoke(app, ["watch"])

    assert result.exit_code == 1
    assert _anomaly_types(isolated_scherlok_dir) == [FRESHNESS_CRITICAL]


def test_report_says_which_column_dated_the_table(monkeypatch):
    from rich.console import Console

    from scherlok.alerter import console as console_module

    recorder = Console(record=True, width=200)
    monkeypatch.setattr(console_module, "console", recorder)
    freshness = {
        "last_modified": "2026-11-09T10:00:00+00:00",
        "hours_since_update": 2.0,
        "freshness_source": "column",
        "freshness_column": "updated_at",
    }

    console_module.print_profile_summary(TABLE, None, None, freshness)

    assert "Freshness: 2.0h ago (latest updated_at: 2026-11-09T10:00:00+00:00)" in (
        recorder.export_text()
    )
