"""Tests for DuckDBConnector using a real in-memory DuckDB database."""

import importlib.util

import pytest

pytestmark = pytest.mark.skipif(
    importlib.util.find_spec("duckdb") is None,
    reason="duckdb not installed",
)


@pytest.fixture()
def connector():
    from scherlok.connectors.duckdb import DuckDBConnector

    c = DuckDBConnector("duckdb://:memory:")
    assert c.connect() is True
    return c


def test_connect_success_and_memory_forms():
    from scherlok.connectors.duckdb import DuckDBConnector

    c = DuckDBConnector("duckdb://:memory:")
    assert c.connect() is True

    c = DuckDBConnector("duckdb:///:memory:")
    assert c.connect() is True


def test_database_path_supported_forms():
    from scherlok.connectors.duckdb import DuckDBConnector

    cases = {
        ":memory:": ":memory:",
        "duckdb://:memory:": ":memory:",
        "duckdb:///:memory:": ":memory:",
        "duckdb:///path/to/file.db": "/path/to/file.db",
        "duckdb:///tmp/file.db?mode=ro": "/tmp/file.db",
    }
    for connection_string, path in cases.items():
        assert DuckDBConnector(connection_string)._database_path() == path


def test_two_slash_relative_url_raises():
    from scherlok.connectors.duckdb import DuckDBConnector

    for connection_string in ["duckdb://file.db", "duckdb://data/file.db"]:
        with pytest.raises(ValueError, match="Use 'duckdb:///path/to/file.db'"):
            DuckDBConnector(connection_string)._database_path()


def test_get_connector_returns_duckdb_connector():
    from scherlok.connectors import get_connector
    from scherlok.connectors.duckdb import DuckDBConnector

    connector = get_connector("duckdb://:memory:")
    assert isinstance(connector, DuckDBConnector)


def test_connect_failure_sets_last_error(tmp_path):
    from scherlok.connectors.duckdb import DuckDBConnector

    missing_dir = tmp_path / "missing" / "database.duckdb"
    c = DuckDBConnector(f"duckdb://{missing_dir}")

    assert c.connect() is False
    assert c.last_error


def test_list_tables(connector):
    connector._conn.execute("CREATE TABLE users (id INTEGER)")
    connector._conn.execute("CREATE TABLE orders (id INTEGER)")

    assert connector.list_tables() == ["orders", "users"]


def test_get_row_count(connector):
    connector._conn.execute("CREATE TABLE orders (id INTEGER)")
    connector._conn.execute("INSERT INTO orders VALUES (1), (2), (3)")

    assert connector.get_row_count("orders") == 3


def test_get_columns(connector):
    connector._conn.execute(
        "CREATE TABLE users (id INTEGER NOT NULL, email VARCHAR, score DOUBLE)"
    )

    columns = connector.get_columns("users")

    assert columns == [
        {"name": "id", "type": "INTEGER", "nullable": False},
        {"name": "email", "type": "VARCHAR", "nullable": True},
        {"name": "score", "type": "DOUBLE", "nullable": True},
    ]


def test_get_column_stats_numeric(connector):
    connector._conn.execute("CREATE TABLE measurements (value DOUBLE)")
    connector._conn.execute("INSERT INTO measurements VALUES (1.0), (2.0), (2.0), (NULL)")

    stats = connector.get_column_stats("measurements", "value")

    assert stats["null_count"] == 1
    assert stats["distinct_count"] == 2
    assert stats["mean"] == pytest.approx(5 / 3)
    assert stats["stddev"] is not None
    assert stats["min"] == "1.0"
    assert stats["max"] == "2.0"
    assert stats["top_values"][0] == {"value": "2.0", "count": 2}


def test_get_last_modified_returns_none(connector):
    assert connector.get_last_modified("orders") is None
