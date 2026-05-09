"""Tests for MySQLConnector — all I/O mocked, no real MySQL needed."""

from datetime import datetime
from unittest.mock import MagicMock, patch
import sys
import types

import pytest


# ---------------------------------------------------------------------------
# Patch pymysql before the connector module is imported
# ---------------------------------------------------------------------------

@pytest.fixture(autouse=True)
def mock_pymysql():
    """Inject a fake pymysql into sys.modules for every test."""
    fake = MagicMock()
    fake.cursors = MagicMock()
    fake.cursors.DictCursor = MagicMock()
    fake.Error = Exception  # so except pymysql.Error works

    sys.modules.setdefault("pymysql", fake)
    sys.modules.setdefault("pymysql.cursors", fake.cursors)

    # Re-import connector with patched module in place
    if "scherlok.connectors.mysql" in sys.modules:
        del sys.modules["scherlok.connectors.mysql"]

    yield fake

    # Cleanup so other test files aren't affected
    sys.modules.pop("pymysql", None)
    sys.modules.pop("pymysql.cursors", None)
    sys.modules.pop("scherlok.connectors.mysql", None)


@pytest.fixture()
def connector(mock_pymysql):
    from scherlok.connectors.mysql import MySQLConnector

    url = "mysql://user:secret@localhost:3306/testdb"
    c = MySQLConnector(url)

    # Attach a pre-connected mock so connect() isn't called implicitly
    fake_conn = MagicMock()
    c._conn = fake_conn
    return c, fake_conn


def _wire_cursor(fake_conn, fetchall=None, fetchone=None):
    """Make fake_conn.cursor() a context manager returning a mock cursor."""
    cur = MagicMock()
    cur.fetchall.return_value = fetchall or []
    cur.fetchone.return_value = fetchone
    ctx = MagicMock()
    ctx.__enter__ = MagicMock(return_value=cur)
    ctx.__exit__ = MagicMock(return_value=False)
    fake_conn.cursor.return_value = ctx
    return cur


# ---------------------------------------------------------------------------
# Test 1 — connect() passes correct kwargs to pymysql.connect
# ---------------------------------------------------------------------------

def test_connect_kwargs(mock_pymysql):
    from scherlok.connectors.mysql import MySQLConnector

    c = MySQLConnector("mysql://alice:pw@db.example.com:3307/mydb")
    result = c.connect()

    assert result is True
    kw = mock_pymysql.connect.call_args.kwargs
    assert kw["host"] == "db.example.com"
    assert kw["port"] == 3307
    assert kw["user"] == "alice"
    assert kw["password"] == "pw"
    assert kw["database"] == "mydb"
    assert kw["autocommit"] is True


# ---------------------------------------------------------------------------
# Test 2 — connect() returns False and sets _last_error on failure
# ---------------------------------------------------------------------------

def test_connect_failure(mock_pymysql):
    from scherlok.connectors.mysql import MySQLConnector

    mock_pymysql.connect.side_effect = Exception("access denied for user")
    c = MySQLConnector("mysql://bad:creds@localhost/db")
    result = c.connect()

    assert result is False
    assert "wrong username or password" in c._last_error


# ---------------------------------------------------------------------------
# Test 3 — list_tables returns table names, queries information_schema
# ---------------------------------------------------------------------------

def test_list_tables(connector):
    c, fake_conn = connector
    _wire_cursor(fake_conn, fetchall=[
        {"TABLE_NAME": "orders"},
        {"TABLE_NAME": "users"},
    ])

    result = c.list_tables()

    assert result == ["orders", "users"]
    sql = fake_conn.cursor().__enter__().execute.call_args[0][0]
    assert "information_schema" in sql.lower()
    assert "TABLE_TYPE" in sql


# ---------------------------------------------------------------------------
# Test 4 — get_row_count issues COUNT(*) and returns integer
# ---------------------------------------------------------------------------

def test_get_row_count(connector):
    c, fake_conn = connector
    _wire_cursor(fake_conn, fetchone={"cnt": 99})

    assert c.get_row_count("orders") == 99


# ---------------------------------------------------------------------------
# Test 5 — get_columns returns structured metadata with nullable bool
# ---------------------------------------------------------------------------

def test_get_columns(connector):
    c, fake_conn = connector
    _wire_cursor(fake_conn, fetchall=[
        {"COLUMN_NAME": "id",    "COLUMN_TYPE": "int(11)",     "IS_NULLABLE": "NO"},
        {"COLUMN_NAME": "email", "COLUMN_TYPE": "varchar(255)", "IS_NULLABLE": "YES"},
    ])

    cols = c.get_columns("users")

    assert len(cols) == 2
    assert cols[0] == {"name": "id",    "type": "int(11)",      "nullable": False}
    assert cols[1] == {"name": "email", "type": "varchar(255)", "nullable": True}


# ---------------------------------------------------------------------------
# Test 6 — get_column_stats returns null_count, distinct_count, mean, etc.
# ---------------------------------------------------------------------------

def test_get_column_stats_numeric(connector):
    c, fake_conn = connector

    cur = MagicMock()
    # fetchone called 3 times: null/distinct, numeric stats, (top_values uses fetchall)
    cur.fetchone.side_effect = [
        {"null_count": 2, "distinct_count": 50},
        {"mean": 10.5, "stddev": 1.2, "min": "1", "max": "99"},
    ]
    cur.fetchall.return_value = [
        {"val": "10", "cnt": 5},
        {"val": "20", "cnt": 3},
    ]
    ctx = MagicMock()
    ctx.__enter__ = MagicMock(return_value=cur)
    ctx.__exit__ = MagicMock(return_value=False)
    fake_conn.cursor.return_value = ctx

    stats = c.get_column_stats("orders", "amount")

    assert stats["null_count"] == 2
    assert stats["distinct_count"] == 50
    assert stats["mean"] == 10.5
    assert stats["min"] == "1"
    assert len(stats["top_values"]) == 2


# ---------------------------------------------------------------------------
# Test 7 — get_last_modified returns datetime when UPDATE_TIME is set
# ---------------------------------------------------------------------------

def test_get_last_modified_returns_datetime(connector):
    c, fake_conn = connector
    ts = datetime(2024, 6, 15, 10, 30, 0)
    _wire_cursor(fake_conn, fetchone={"UPDATE_TIME": ts})

    assert c.get_last_modified("orders") == ts


def test_get_last_modified_returns_none(connector):
    c, fake_conn = connector
    _wire_cursor(fake_conn, fetchone={"UPDATE_TIME": None})

    assert c.get_last_modified("orders") is None


# ---------------------------------------------------------------------------
# Test 8 — _classify_error maps known messages correctly
# ---------------------------------------------------------------------------

def test_classify_error_access_denied(mock_pymysql):
    from scherlok.connectors.mysql import MySQLConnector

    result = MySQLConnector._classify_error("(1045, 'Access denied for user')")
    assert "wrong username or password" in result


def test_classify_error_unknown_database(mock_pymysql):
    from scherlok.connectors.mysql import MySQLConnector

    result = MySQLConnector._classify_error("(1049, 'Unknown database mydb')")
    assert "does not exist" in result


def test_classify_error_fallback(mock_pymysql):
    from scherlok.connectors.mysql import MySQLConnector

    result = MySQLConnector._classify_error("some weird error")
    assert result == "some weird error"