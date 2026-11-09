"""Tests for the Postgres connector using mocks."""

from unittest.mock import MagicMock, patch


def _make_connector_with_mock_cursor(rows: list[dict]) -> tuple[object, MagicMock]:
    """Build a PostgresConnector whose cursor returns the given rows."""
    from scherlok.connectors.postgres import PostgresConnector

    cur = MagicMock()
    cur.fetchall.return_value = rows
    cur.__enter__ = MagicMock(return_value=cur)
    cur.__exit__ = MagicMock(return_value=False)

    conn = MagicMock()
    conn.cursor.return_value = cur

    c = PostgresConnector("postgresql://u:p@h/d")
    c._conn = conn
    return c, cur


def test_list_tables_query_includes_views():
    """Issue #7: list_tables must return both BASE TABLE and VIEW rows."""
    c, cur = _make_connector_with_mock_cursor(
        [
            {"table_name": "fct_orders"},  # BASE TABLE
            {"table_name": "stg_users"},  # VIEW
        ]
    )
    tables = c.list_tables()
    assert "stg_users" in tables
    assert "fct_orders" in tables

    # Query must filter to ('BASE TABLE', 'VIEW')
    sql = cur.execute.call_args.args[0]
    assert "'BASE TABLE'" in sql
    assert "'VIEW'" in sql


@patch("scherlok.connectors.postgres.psycopg2.connect")
def test_connect_success(mock_connect):
    from scherlok.connectors.postgres import PostgresConnector

    mock_connect.return_value = MagicMock()
    c = PostgresConnector("postgresql://u:p@h/d")
    assert c.connect() is True


@patch("scherlok.connectors.postgres.psycopg2.connect")
def test_connect_failure(mock_connect):
    import psycopg2

    from scherlok.connectors.postgres import PostgresConnector

    mock_connect.side_effect = psycopg2.OperationalError("nope")
    c = PostgresConnector("postgresql://u:p@h/d")
    assert c.connect() is False


def test_get_column_stats_queries_no_column_values():
    """Stats are aggregates only: no top-values query, no min/max."""
    from scherlok.store.sqlite import RAW_VALUE_KEYS

    c, cur = _make_connector_with_mock_cursor([])
    cur.fetchone.side_effect = [
        {"null_count": 1, "distinct_count": 3},
        {"mean": 42.5, "stddev": 8.3},
    ]

    stats = c.get_column_stats("users", "score")

    assert stats == {"null_count": 1, "distinct_count": 3, "mean": 42.5, "stddev": 8.3}
    assert stats.keys().isdisjoint(RAW_VALUE_KEYS)
    cur.fetchall.assert_not_called()
    executed = [call.args[0].upper() for call in cur.execute.call_args_list]
    assert len(executed) == 2
    for sql in executed:
        assert "MIN(" not in sql
        assert "MAX(" not in sql
        assert "GROUP BY" not in sql


# ----- a schema in the connection string (#68) --------------------------------

ANALYTICS_URL = "postgresql://u:p@h:5432/shop/analytics?sslmode=require"


def test_a_schema_in_the_url_is_split_from_what_libpq_reads():
    from scherlok.connectors.postgres import split_schema

    assert split_schema(ANALYTICS_URL) == (
        "postgresql://u:p@h:5432/shop?sslmode=require",
        "analytics",
    )
    assert split_schema("postgresql://u:p@h/shop") == ("postgresql://u:p@h/shop", None)
    assert split_schema("dbname=shop host=h") == ("dbname=shop host=h", None)
    assert split_schema("postgresql://u:p@h/shop/My%20Schema")[1] == "My Schema"


@patch("scherlok.connectors.postgres.psycopg2.connect")
def test_connect_sets_the_search_path_to_the_schema(mock_connect):
    from scherlok.connectors.postgres import PostgresConnector

    cur = MagicMock()
    cur.__enter__ = MagicMock(return_value=cur)
    cur.__exit__ = MagicMock(return_value=False)
    mock_connect.return_value.cursor.return_value = cur

    connector = PostgresConnector(ANALYTICS_URL)

    assert connector.connect()
    mock_connect.assert_called_once_with("postgresql://u:p@h:5432/shop?sslmode=require")
    statement = cur.execute.call_args.args[0]
    assert "search_path" in repr(statement)
    assert "analytics" in repr(statement)


@patch("scherlok.connectors.postgres.psycopg2.connect")
def test_connect_without_a_schema_leaves_the_search_path_alone(mock_connect):
    from scherlok.connectors.postgres import PostgresConnector

    assert PostgresConnector("postgresql://u:p@h/shop").connect()
    mock_connect.return_value.cursor.assert_not_called()


def test_metadata_reads_the_schema_of_the_url():
    from scherlok.connectors.base import RelationNamespace
    from scherlok.connectors.postgres import PostgresConnector

    connector = PostgresConnector(ANALYTICS_URL)
    cur = MagicMock()
    cur.__enter__ = MagicMock(return_value=cur)
    cur.__exit__ = MagicMock(return_value=False)
    cur.fetchall.return_value = []
    cur.fetchone.return_value = None
    connector._conn = MagicMock()
    connector._conn.cursor.return_value = cur

    connector.list_tables()
    connector.get_columns("orders")
    connector.get_last_modified("orders")

    params = [call.args[1] for call in cur.execute.call_args_list]
    assert params == [("analytics",), ("analytics", "orders"), ("analytics", "orders")]
    assert connector.namespace() == RelationNamespace(database="shop", schema="analytics")


def test_without_a_schema_the_namespace_is_public():
    from scherlok.connectors.base import RelationNamespace
    from scherlok.connectors.postgres import PostgresConnector

    assert PostgresConnector("postgresql://u:p@h/shop").namespace() == RelationNamespace(
        database="shop", schema="public"
    )


def test_a_schema_gets_its_own_baseline_scope():
    from scherlok.service import connection_scope

    assert connection_scope(ANALYTICS_URL) == "postgresql://h/shop/analytics"
    assert connection_scope("postgresql://u:p@h/shop") == "postgresql://h/shop"
