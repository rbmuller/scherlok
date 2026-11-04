"""Tests for the new-table checks: problems visible before a table has a baseline (#107)."""

from __future__ import annotations

import importlib.util
import inspect
import json
import sqlite3
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path

import pytest
from typer.testing import CliRunner

from scherlok.cli import app
from scherlok.detector.new_table import (
    ALL_NULL_COLUMN,
    CONSTANT_COLUMN,
    CONSTANT_COLUMN_MIN_ROWS,
    DUPLICATE_KEY,
    EMPTY_TABLE,
    NEW_TABLE_ANOMALY_TYPES,
    detect_new_table_anomalies,
    escalate_new_table_anomalies,
    key_column_names,
)
from scherlok.detector.severity import Severity
from scherlok.service import profile_and_detect, verify_tables
from scherlok.store.sqlite import ProfileStore

requires_duckdb = pytest.mark.skipif(
    importlib.util.find_spec("duckdb") is None, reason="duckdb not installed"
)

runner = CliRunner(env={"NO_COLOR": "1"})

ROWS = 1000


def _column(name: str) -> dict:
    return {"name": name, "type": "INTEGER", "nullable": True}


def _dist(null_count: int = 0, distinct_count: int = ROWS) -> dict:
    return {
        "mean": None,
        "stddev": None,
        "null_count": null_count,
        "null_rate": round(null_count / ROWS, 4),
        "distinct_count": distinct_count,
    }


def _detect(table: str = "orders", rows: int = ROWS, **columns: dict) -> list[dict]:
    """Run the checks on a table whose columns map to their distribution profiles."""
    schema = {"columns": [_column(name) for name in columns]}
    return detect_new_table_anomalies(table, {"row_count": rows}, schema, columns)


def _types(anomalies: list[dict]) -> list[str]:
    return [a["type"] for a in anomalies]


# ----- detect_new_table_anomalies ----------------------------------------------

def test_takes_profile_dicts_only():
    params = list(inspect.signature(detect_new_table_anomalies).parameters)
    assert params == ["table", "volume", "schema", "distributions"]


def test_healthy_table_has_no_findings():
    assert _detect(id=_dist(), amount=_dist(distinct_count=37)) == []


def test_empty_table_fires_as_warning_and_skips_column_checks():
    anomalies = _detect(rows=0, id=_dist(distinct_count=0))

    assert anomalies == [{
        "table": "orders",
        "type": EMPTY_TABLE,
        "message": "New table is empty (0 rows)",
        "severity": Severity.WARNING,
    }]


def test_empty_table_does_not_fire_with_one_row():
    assert EMPTY_TABLE not in _types(_detect(rows=1, id=_dist(distinct_count=1)))


def test_all_null_column_fires_as_warning():
    anomalies = _detect(id=_dist(), revenue=_dist(null_count=ROWS, distinct_count=0))

    assert len(anomalies) == 1
    assert anomalies[0]["type"] == ALL_NULL_COLUMN
    assert anomalies[0]["severity"] == Severity.WARNING
    assert "'revenue'" in anomalies[0]["message"]
    assert "1,000 rows" in anomalies[0]["message"]


def test_all_null_column_does_not_fire_with_one_value():
    anomalies = _detect(revenue=_dist(null_count=ROWS - 1, distinct_count=1))

    assert ALL_NULL_COLUMN not in _types(anomalies)


def test_all_null_column_ignores_a_null_rate_rounded_up_to_one():
    rows = 100_000
    almost_all_null = {"null_count": rows - 1, "null_rate": 1.0, "distinct_count": 1}

    anomalies = _detect(rows=rows, revenue=almost_all_null)

    assert ALL_NULL_COLUMN not in _types(anomalies)


def test_all_null_key_column_is_reported_once():
    anomalies = _detect(id=_dist(null_count=ROWS, distinct_count=0))

    assert _types(anomalies) == [ALL_NULL_COLUMN]


@pytest.mark.parametrize("column", ["id", "order_id", "ID", "Order_Id"])
def test_duplicate_key_fires_on_the_tables_own_key(column):
    anomalies = _detect(**{column: _dist(distinct_count=ROWS - 10)})

    assert _types(anomalies) == [DUPLICATE_KEY]
    assert anomalies[0]["severity"] == Severity.WARNING
    assert f"'{column}'" in anomalies[0]["message"]
    assert "990 distinct values in 1,000 non-NULL rows" in anomalies[0]["message"]


def test_duplicate_key_ignores_foreign_keys():
    anomalies = _detect(user_id=_dist(distinct_count=40), product_id=_dist(distinct_count=7))

    assert DUPLICATE_KEY not in _types(anomalies)


def test_duplicate_key_does_not_fire_on_a_unique_key():
    assert _detect(id=_dist(), order_id=_dist()) == []


def test_duplicate_key_does_not_count_nulls_as_duplicates():
    anomalies = _detect(id=_dist(null_count=100, distinct_count=ROWS - 100))

    assert DUPLICATE_KEY not in _types(anomalies)


@pytest.mark.parametrize(
    ("table", "key"),
    [
        ("orders", "order_id"),
        ("categories", "category_id"),
        ("addresses", "address_id"),
        ("boxes", "box_id"),
        ("customer", "customer_id"),
        ("status", "status_id"),
        ("ORDERS", "order_id"),
    ],
)
def test_key_column_names_singularize_the_table(table, key):
    names = key_column_names(table)

    assert key in names
    assert "id" in names


def test_key_column_names_never_include_a_foreign_key():
    assert key_column_names("orders").isdisjoint({"user_id", "customer_id", "product_id"})
    assert "order_id" not in key_column_names("order_items")


def test_constant_column_fires_as_info_from_the_minimum_rows():
    anomalies = _detect(
        rows=CONSTANT_COLUMN_MIN_ROWS, currency=_dist(distinct_count=1)
    )

    assert _types(anomalies) == [CONSTANT_COLUMN]
    assert anomalies[0]["severity"] == Severity.INFO
    assert "'currency'" in anomalies[0]["message"]


def test_constant_column_does_not_fire_below_the_minimum_rows():
    anomalies = _detect(rows=CONSTANT_COLUMN_MIN_ROWS - 1, currency=_dist(distinct_count=1))

    assert anomalies == []


def test_constant_column_does_not_fire_with_two_values():
    assert _detect(currency=_dist(distinct_count=2)) == []


def test_constant_column_counts_only_non_null_rows():
    anomalies = _detect(currency=_dist(null_count=ROWS - 1, distinct_count=1))

    assert CONSTANT_COLUMN not in _types(anomalies)


def test_columns_without_stats_are_skipped():
    schema = {"columns": [_column("id"), _column("blob")]}
    distributions = {"id": {"null_count": None, "distinct_count": None}}

    assert detect_new_table_anomalies("orders", {"row_count": ROWS}, schema, distributions) == []


# ----- escalate_new_table_anomalies --------------------------------------------

def test_escalate_raises_warning_findings_to_critical_only():
    findings = [
        {"table": "t", "type": EMPTY_TABLE, "severity": Severity.WARNING, "message": "m"},
        {"table": "t", "type": ALL_NULL_COLUMN, "severity": Severity.WARNING, "message": "m"},
        {"table": "t", "type": DUPLICATE_KEY, "severity": Severity.WARNING, "message": "m"},
        {"table": "t", "type": CONSTANT_COLUMN, "severity": Severity.INFO, "message": "m"},
        {"table": "t", "type": "volume_drop", "severity": Severity.WARNING, "message": "m"},
    ]

    escalated = escalate_new_table_anomalies(findings)

    assert [a["severity"] for a in escalated] == [
        Severity.CRITICAL, Severity.CRITICAL, Severity.CRITICAL, Severity.INFO, Severity.WARNING,
    ]
    assert findings[0]["severity"] == Severity.WARNING  # input left untouched


# ----- profile_and_detect: only without a baseline -----------------------------

class FakeConnector:
    """One table whose rows and per-column stats a test can change between runs."""

    def __init__(self, row_count: int = ROWS, stats: dict[str, dict] | None = None):
        self.row_count = row_count
        self.stats = stats or {"id": {"null_count": 0, "distinct_count": ROWS}}
        self.calls: Counter[str] = Counter()

    def list_tables(self):
        self.calls["list_tables"] += 1
        return ["orders"]

    def get_row_count(self, table):
        self.calls["get_row_count"] += 1
        return self.row_count

    def get_columns(self, table):
        self.calls["get_columns"] += 1
        return [_column(name) for name in self.stats]

    def get_column_stats(self, table, column):
        self.calls["get_column_stats"] += 1
        return {"mean": None, "stddev": None, **self.stats[column]}

    def get_last_modified(self, table):
        self.calls["get_last_modified"] += 1
        return datetime.now(timezone.utc)


BROKEN_STATES = {
    "empty": (0, {"id": {"null_count": 0, "distinct_count": 0}}),
    "all_null": (ROWS, {"revenue": {"null_count": ROWS, "distinct_count": 0}}),
    "duplicate_key": (ROWS, {"id": {"null_count": 0, "distinct_count": 10}}),
    "constant": (ROWS, {"currency": {"null_count": 0, "distinct_count": 1}}),
}


@pytest.fixture()
def store(tmp_path):
    profile_store = ProfileStore(tmp_path / "profiles.db")
    yield profile_store
    profile_store.close()


@pytest.mark.parametrize("state", sorted(BROKEN_STATES))
def test_first_sighting_reports_each_finding(store, state):
    row_count, stats = BROKEN_STATES[state]

    anomalies, _ = profile_and_detect(FakeConnector(row_count, stats), store, "orders")

    assert len(anomalies) == 1
    assert anomalies[0]["type"] in NEW_TABLE_ANOMALY_TYPES


@pytest.mark.parametrize("state", sorted(BROKEN_STATES))
def test_a_table_with_a_baseline_never_gets_new_table_findings(store, state):
    row_count, stats = BROKEN_STATES[state]
    connector = FakeConnector(row_count, stats)
    profile_and_detect(connector, store, "orders")  # first sighting sets the baseline

    for _ in range(2):
        anomalies, _ = profile_and_detect(connector, store, "orders")
        assert anomalies == []


def test_a_table_that_breaks_after_its_baseline_gets_drift_findings_only(store):
    connector = FakeConnector(
        stats={"id": {"null_count": 0, "distinct_count": ROWS},
               "revenue": {"null_count": 0, "distinct_count": 500}},
    )
    profile_and_detect(connector, store, "orders")
    connector.stats = {"id": {"null_count": 0, "distinct_count": 10},
                       "revenue": {"null_count": ROWS, "distinct_count": 0}}

    anomalies, _ = profile_and_detect(connector, store, "orders")

    assert anomalies  # the regular detectors see the breakage
    assert NEW_TABLE_ANOMALY_TYPES.isdisjoint(_types(anomalies))


def test_new_table_checks_issue_no_extra_queries(store):
    connector = FakeConnector(*BROKEN_STATES["duplicate_key"])
    first, _ = profile_and_detect(connector, store, "orders")
    first_calls = Counter(connector.calls)
    connector.calls.clear()

    second, _ = profile_and_detect(connector, store, "orders")

    assert _types(first) == [DUPLICATE_KEY] and second == []
    assert connector.calls == first_calls


# ----- verify_tables: named tables are just built ------------------------------

def test_verify_escalates_findings_on_named_tables_without_baseline(store):
    connector = FakeConnector(*BROKEN_STATES["empty"])

    named = verify_tables(connector, store, ["orders"])
    unnamed = verify_tables(connector, store)

    assert [(a["type"], a["severity"]) for a in named.anomalies] == [
        (EMPTY_TABLE, Severity.CRITICAL)
    ]
    assert named.passed is False
    assert [(a["type"], a["severity"]) for a in unnamed.anomalies] == [
        (EMPTY_TABLE, Severity.WARNING)
    ]
    assert unnamed.passed is True


def test_verify_keeps_constant_column_as_info_on_named_tables(store):
    connector = FakeConnector(*BROKEN_STATES["constant"])

    result = verify_tables(connector, store, ["orders"])

    assert [(a["type"], a["severity"]) for a in result.anomalies] == [
        (CONSTANT_COLUMN, Severity.INFO)
    ]
    assert result.passed is True


def test_verify_named_table_with_baseline_gets_no_new_table_findings(store):
    connector = FakeConnector(*BROKEN_STATES["empty"])
    profile_and_detect(connector, store, "orders")

    result = verify_tables(connector, store, ["orders"])

    assert result.tables_verified == ["orders"]
    assert result.anomalies == []
    assert result.passed is True


# ----- end to end on DuckDB ----------------------------------------------------

def _execute(path: Path, *statements: str) -> None:
    import duckdb

    conn = duckdb.connect(str(path))
    try:
        for sql in statements:
            conn.execute(sql)
    finally:
        conn.close()


def _anomaly_rows(scherlok_dir: Path) -> int:
    conn = sqlite3.connect(scherlok_dir / "profiles.db")
    try:
        return conn.execute("SELECT COUNT(*) FROM anomalies").fetchone()[0]
    finally:
        conn.close()


@pytest.fixture()
def warehouse(tmp_path, monkeypatch) -> Path:
    """The two first-run cases from #107: an empty table and a 100% NULL column."""
    path = tmp_path / "warehouse.duckdb"
    _execute(
        path,
        "CREATE TABLE refunds (id INTEGER, amount DOUBLE)",
        "CREATE TABLE sales (id INTEGER, revenue DOUBLE)",
        f"INSERT INTO sales SELECT i, NULL FROM range(1, {ROWS + 1}) t(i)",
    )
    monkeypatch.setenv("SCHERLOK_CONNECTION", f"duckdb:///{path}")
    return path


def _verify_json(*args: str):
    result = runner.invoke(app, ["verify", "--output", "json", *args])
    return result, json.loads(result.stdout)


@requires_duckdb
def test_verify_table_on_a_new_empty_table_exits_1(warehouse):
    result, payload = _verify_json("--table", "refunds")

    assert result.exit_code == 1
    assert payload["passed"] is False
    assert payload["tables_without_baseline"] == ["refunds"]
    assert [(a["type"], a["severity"]) for a in payload["anomalies"]] == [
        (EMPTY_TABLE, "CRITICAL")
    ]


@requires_duckdb
def test_verify_table_on_a_new_all_null_column_exits_1(warehouse):
    result, payload = _verify_json("--table", "sales")

    assert result.exit_code == 1
    assert [(a["type"], a["severity"]) for a in payload["anomalies"]] == [
        (ALL_NULL_COLUMN, "CRITICAL")
    ]


@requires_duckdb
def test_verify_text_mode_does_not_suggest_baselining_a_broken_table(warehouse):
    result = runner.invoke(app, ["verify", "--table", "refunds"])

    assert result.exit_code == 1
    assert EMPTY_TABLE in result.stdout
    unwrapped = " ".join(result.stdout.split())  # Rich wraps at the terminal width
    assert "Once the data is right, run scherlok investigate" in unwrapped


@requires_duckdb
def test_verify_without_table_reports_new_table_findings_as_warnings(warehouse):
    result, payload = _verify_json()

    assert result.exit_code == 0
    assert payload["warning"] == 2 and payload["critical"] == 0


@requires_duckdb
def test_ci_on_a_new_empty_table_exits_0_and_shows_the_warning(warehouse, monkeypatch):
    monkeypatch.delenv("SCHERLOK_CONNECTION")  # ci takes the URL as an argument

    result = runner.invoke(app, ["ci", f"duckdb:///{warehouse}"])

    assert result.exit_code == 0, result.output
    assert "WARNING" in result.output
    assert EMPTY_TABLE in result.output
    assert ALL_NULL_COLUMN in result.output


@requires_duckdb
def test_ci_fail_on_warning_fails_on_a_new_empty_table(warehouse, monkeypatch):
    monkeypatch.delenv("SCHERLOK_CONNECTION")

    result = runner.invoke(app, ["ci", f"duckdb:///{warehouse}", "--fail-on", "warning"])

    assert result.exit_code == 1


@requires_duckdb
def test_investigate_keeps_profiling_silently(warehouse, isolated_scherlok_dir):
    result = runner.invoke(app, ["investigate"])

    assert result.exit_code == 0
    assert EMPTY_TABLE not in result.output and ALL_NULL_COLUMN not in result.output
    assert _anomaly_rows(isolated_scherlok_dir) == 0
