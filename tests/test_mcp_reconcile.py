"""Tests for the read-only MCP tool `reconcile` (#111), on a real in-memory DuckDB."""

from __future__ import annotations

import importlib.util
import json

import pytest

from scherlok.detector.anomaly_types import DUPLICATE_KEYS, ROW_COUNT_MISMATCH, SUM_MISMATCH
from scherlok.detector.fields import DETAIL_FIELDS

pytestmark = pytest.mark.skipif(
    importlib.util.find_spec("duckdb") is None, reason="duckdb not installed"
)

SENTINEL = "SENTINEL-KEY"
ROWS = 100


@pytest.fixture()
def duck_connector():
    """`orders` with sentinel keys, a clean copy and a copy that fans out."""
    from scherlok.connectors.duckdb import DuckDBConnector

    conn = DuckDBConnector("duckdb:///:memory:")
    assert conn.connect() is True
    execute = conn._conn.execute
    execute("CREATE TABLE orders (order_id VARCHAR, amount DECIMAL(10, 2))")
    execute(
        f"INSERT INTO orders SELECT '{SENTINEL}-' || i, i * 1.5 FROM range(1, {ROWS + 1}) t(i)"
    )
    execute("CREATE TABLE orders_copy AS SELECT * FROM orders")
    execute("CREATE TABLE orders_fanned AS SELECT * FROM orders UNION ALL "
            "SELECT * FROM orders WHERE amount < 15")
    return conn


@pytest.fixture()
def server(monkeypatch, duck_connector, isolated_scherlok_dir):
    from scherlok.mcp import server

    monkeypatch.setenv("SCHERLOK_CONNECTION", "duckdb:///:memory:")
    monkeypatch.setattr(server, "get_connector", lambda _cs: duck_connector)
    # Re-connecting an in-memory DuckDB would replace it with an empty one.
    monkeypatch.setattr(duck_connector, "connect", lambda: True)
    return server


def test_a_clean_copy_passes(server):
    out = server.reconcile(
        "orders", "orders_copy", key_columns=["order_id"], sum_columns=["amount"]
    )

    assert out["passed"] is True
    assert out["anomalies"] == [] and out["truncated"] is False
    assert out["target"]["row_count"] == ROWS


def test_a_fan_out_fails_with_structured_findings(server):
    out = server.reconcile(
        "orders", "orders_fanned", key_columns=["order_id"], sum_columns=["amount"]
    )

    assert out["passed"] is False
    assert out["critical"] == 3
    assert [a["type"] for a in out["anomalies"]] == [
        ROW_COUNT_MISMATCH, DUPLICATE_KEYS, SUM_MISMATCH,
    ]
    for anomaly in out["anomalies"]:
        assert set(DETAIL_FIELDS) <= set(anomaly)
    assert SENTINEL not in json.dumps(out)


def test_tables_must_be_listed_by_list_tables(server, duck_connector):
    duck_connector._conn.execute("CREATE SCHEMA raw")
    duck_connector._conn.execute("CREATE TABLE raw.orders AS SELECT * FROM orders")

    with pytest.raises(ValueError, match="not found"):
        server.reconcile("orders", "ghost")
    # Visible to SQL, but not listed: the tool only reaches what list_tables shows.
    with pytest.raises(ValueError, match="not found"):
        server.reconcile("raw.orders", "orders_copy")


def test_columns_missing_from_either_table_are_named(server, duck_connector):
    duck_connector._conn.execute("ALTER TABLE orders ADD COLUMN fee DOUBLE")

    with pytest.raises(ValueError, match=r"Columns not found in orders: \['revenue'\]"):
        server.reconcile("orders", "orders_copy", sum_columns=["revenue"])
    # Matched case-insensitively, so `AMOUNT` is found; `fee` is only in the source.
    with pytest.raises(ValueError, match=r"Columns not found in orders_copy: \['fee'\]"):
        server.reconcile("orders", "orders_copy", sum_columns=["AMOUNT", "fee"])


def test_invalid_arguments_fail_before_connecting(server, monkeypatch):
    monkeypatch.setattr(server, "_connect", lambda: pytest.fail("must not connect"))

    with pytest.raises(ValueError, match="different tables"):
        server.reconcile("orders", "orders")
    with pytest.raises(ValueError, match="Tolerance"):
        server.reconcile("orders", "orders_copy", tolerance=-1.0)


def test_nothing_is_saved(server, isolated_scherlok_dir):
    server.reconcile("orders", "orders_fanned", key_columns=["order_id"])

    assert not isolated_scherlok_dir.exists()
