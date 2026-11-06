"""#137: duplicated keys in a unique column are flagged however long the history is.

A join fan-out adds rows but no new key values, so the distinct count stays on
its learned path; only the distinct ratio shows it. The check used to run only
before a table had five readings.
"""

from __future__ import annotations

import importlib.util
import sqlite3
from pathlib import Path

import pytest
from typer.testing import CliRunner

from scherlok.cli import app
from scherlok.detector.anomaly_types import CARDINALITY_CHANGE
from scherlok.detector.severity import Severity

requires_duckdb = pytest.mark.skipif(
    importlib.util.find_spec("duckdb") is None, reason="duckdb not installed"
)

runner = CliRunner(env={"NO_COLOR": "1"})

TABLE = "fct_orders"
INITIAL_ROWS = 10_000
ROWS_PER_RUN = 300
# Every fifth key duplicated: 20% more rows, a distinct ratio of 0.833.
FANOUT_SQL = f"INSERT INTO {TABLE} SELECT order_id, amount FROM {TABLE} WHERE order_id % 5 = 0"
STORE_FILENAME = "profiles.db"


def _execute(path: Path, *statements: str) -> None:
    import duckdb

    conn = duckdb.connect(str(path))
    try:
        for sql in statements:
            conn.execute(sql)
    finally:
        conn.close()


def _cardinality_severities(home: Path) -> list[str]:
    conn = sqlite3.connect(home / STORE_FILENAME)
    try:
        rows = conn.execute(
            "SELECT severity FROM anomalies WHERE anomaly_type = ? AND table_name = ?",
            (CARDINALITY_CHANGE, TABLE),
        ).fetchall()
    finally:
        conn.close()
    return [severity for (severity,) in rows]


@requires_duckdb
@pytest.mark.parametrize("growth_runs", [2, 6])
def test_fanout_is_flagged_with_and_without_learned_history(
    tmp_path, monkeypatch, isolated_scherlok_dir, growth_runs
):
    warehouse = tmp_path / "warehouse.duckdb"
    _execute(
        warehouse,
        f"CREATE TABLE {TABLE} AS "
        f"SELECT i AS order_id, 100.0 + (i % 7) AS amount FROM range({INITIAL_ROWS}) t(i)",
    )
    monkeypatch.setenv("SCHERLOK_CONNECTION", f"duckdb:///{warehouse}")
    assert runner.invoke(app, ["investigate"]).exit_code == 0
    rows = INITIAL_ROWS
    for _ in range(growth_runs):
        _execute(
            warehouse,
            f"INSERT INTO {TABLE} "
            f"SELECT i, 100.0 + (i % 7) FROM range({rows}, {rows + ROWS_PER_RUN}) t(i)",
        )
        rows += ROWS_PER_RUN
        assert runner.invoke(app, ["watch"]).exit_code == 0
    assert _cardinality_severities(isolated_scherlok_dir) == []

    _execute(warehouse, FANOUT_SQL)
    result = runner.invoke(app, ["watch"])

    assert result.exit_code == 0
    assert _cardinality_severities(isolated_scherlok_dir) == [Severity.WARNING.value]
