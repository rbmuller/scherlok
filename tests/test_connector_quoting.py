"""Profiling queries quote every name in the connector's dialect, hostile names included.

A table name comes from `list_tables()`, a dbt manifest or `--table`, and a
column name from the database, so either may hold a quote, a backtick or a
backslash. Each test mocks the driver and checks the exact SQL, and the
parameters, that PostgreSQL, MySQL, BigQuery and Snowflake send. The
reconcile queries are covered in `test_reconcile_connectors.py`.
"""

from __future__ import annotations

import sys
from unittest.mock import MagicMock

import pytest

PLAIN_TABLE = "orders"
PLAIN_COLUMN = "amount"

# Ends a double-quoted identifier, a backtick identifier and a string literal;
# the backslashes before the backtick and the single quote would turn their
# escape into a literal backslash in a dialect with backslash escapes (BigQuery);
# then a second statement. Raw strings, so the SQL reads as sent.
HOSTILE_TABLE = r"""orders"\`\'; DROP TABLE users; --"""
HOSTILE_COLUMN = r"""amount"\`\'; DROP TABLE users; --"""

POSTGRES_TABLE = r'''"orders""\`\'; DROP TABLE users; --"'''
POSTGRES_COLUMN = r'''"amount""\`\'; DROP TABLE users; --"'''

MYSQL_TABLE = r"""`orders"\``\'; DROP TABLE users; --`"""
MYSQL_COLUMN = r"""`amount"\``\'; DROP TABLE users; --`"""

BIGQUERY_TABLE = r"""`proj`.`ds`.`orders"\\\`\\'; DROP TABLE users; --`"""
BIGQUERY_COLUMN = r"""`amount"\\\`\\'; DROP TABLE users; --`"""
BIGQUERY_TABLE_STRING = r"""'orders"\\`\\\'; DROP TABLE users; --'"""

SNOWFLAKE_TABLE = r'''"DB"."SCHEMA"."ORDERS""\`\'; DROP TABLE USERS; --"'''
SNOWFLAKE_COLUMN = r'''"AMOUNT""\`\'; DROP TABLE USERS; --"'''

MYSQL_DATABASE = "shop"
SNOWFLAKE_SCHEMA = "SCHEMA"
SNOWFLAKE_TABLES_SQL = (
    'SELECT table_name FROM "{database}".information_schema.tables '
    "WHERE table_schema = %s AND table_type IN ('BASE TABLE', 'VIEW') ORDER BY table_name"
)
SNOWFLAKE_COLUMNS_SQL = (
    'SELECT column_name, data_type, is_nullable FROM "{database}".information_schema.columns '
    "WHERE table_schema = %s AND table_name = %s ORDER BY ordinal_position"
)
SNOWFLAKE_LAST_ALTERED_SQL = (
    'SELECT last_altered FROM "{database}".information_schema.tables '
    "WHERE table_schema = %s AND table_name = %s"
)

# Test ids for the (plain name, hostile name) cases of the row count tests.
NAME_IDS = ["plain", "hostile"]

NULL_AND_DISTINCT_ROW = {"null_count": 0, "distinct_count": 3}
MEAN_AND_STDDEV_ROW = {"mean": None, "stddev": None}
NUMERIC_TYPE_ROW = {"DATA_TYPE": "decimal", "COLUMN_TYPE": "decimal(10,2)"}
MYSQL_COLUMN_TYPE_SQL = (
    "SELECT DATA_TYPE, COLUMN_TYPE FROM information_schema.COLUMNS "
    "WHERE TABLE_SCHEMA = %s AND TABLE_NAME = %s AND COLUMN_NAME = %s"
)


@pytest.fixture()
def fake_pymysql():
    """A stand-in `pymysql`, so the MySQL connector imports without the driver."""
    fake = MagicMock()
    fake.Error = Exception
    saved = {name: sys.modules.get(name) for name in ("pymysql", "pymysql.cursors")}
    sys.modules["pymysql"] = fake
    sys.modules["pymysql.cursors"] = fake.cursors
    sys.modules.pop("scherlok.connectors.mysql", None)
    yield fake
    sys.modules.pop("scherlok.connectors.mysql", None)
    for name, module in saved.items():
        if module is None:
            sys.modules.pop(name, None)
        else:
            sys.modules[name] = module


def _wire_cursor(connector, rows: list[dict]) -> MagicMock:
    """Give ``connector`` a DB-API connection whose cursor returns ``rows``."""
    cur = MagicMock()
    cur.fetchone.side_effect = rows
    cur.fetchall.return_value = []
    ctx = MagicMock()
    ctx.__enter__ = MagicMock(return_value=cur)
    ctx.__exit__ = MagicMock(return_value=False)
    connector._conn = MagicMock()
    connector._conn.cursor.return_value = ctx
    return cur


def _executed(mock: MagicMock) -> list[tuple]:
    """The positional arguments of every call: ``(sql,)`` or ``(sql, params)``."""
    return [call.args for call in mock.call_args_list]


def _postgres():
    from scherlok.connectors.postgres import PostgresConnector

    return PostgresConnector("postgresql://u:p@h/d")


def _mysql():
    from scherlok.connectors.mysql import MySQLConnector

    return MySQLConnector(f"mysql://u:p@h:3306/{MYSQL_DATABASE}")


def _bigquery(results: list[list[dict]]):
    """A BigQuery connector whose ``_query`` returns ``results``, one per call."""
    from scherlok.connectors.bigquery import BigQueryConnector

    connector = BigQueryConnector("bigquery://proj/ds")
    connector._query = MagicMock(side_effect=results)
    return connector


def _snowflake(results: list[list[dict]], connection_string: str = "snowflake://acct/db/schema"):
    """A Snowflake connector whose ``_query`` returns ``results``, one per call."""
    from scherlok.connectors.snowflake import SnowflakeConnector

    connector = SnowflakeConnector(connection_string)
    connector._query = MagicMock(side_effect=results)
    return connector


# ----- PostgreSQL ------------------------------------------------------------------

@pytest.mark.parametrize(
    ("table", "quoted"),
    [(PLAIN_TABLE, '"orders"'), (HOSTILE_TABLE, POSTGRES_TABLE)],
    ids=NAME_IDS,
)
def test_postgres_row_count_quotes_the_table(table, quoted):
    connector = _postgres()
    cur = _wire_cursor(connector, [{"cnt": 1}])

    connector.get_row_count(table)

    assert _executed(cur.execute) == [(f"SELECT count(*) AS cnt FROM {quoted}",)]


def test_postgres_column_stats_quote_hostile_names():
    connector = _postgres()
    cur = _wire_cursor(connector, [NULL_AND_DISTINCT_ROW, MEAN_AND_STDDEV_ROW])

    connector.get_column_stats(HOSTILE_TABLE, HOSTILE_COLUMN)

    assert _executed(cur.execute) == [
        (
            f"SELECT   count(*) FILTER (WHERE {POSTGRES_COLUMN} IS NULL) AS null_count, "
            f"  count(DISTINCT {POSTGRES_COLUMN}) AS distinct_count FROM {POSTGRES_TABLE}",
        ),
        (
            f"SELECT   avg({POSTGRES_COLUMN})::float AS mean, "
            f"  stddev({POSTGRES_COLUMN})::float AS stddev FROM {POSTGRES_TABLE}",
        ),
    ]


def test_postgres_metadata_lookups_bind_the_table_name():
    connector = _postgres()
    cur = _wire_cursor(connector, [None])

    connector.get_columns(HOSTILE_TABLE)
    connector.get_last_modified(HOSTILE_TABLE)

    for sql, params in _executed(cur.execute):
        assert params == (HOSTILE_TABLE,)
        assert HOSTILE_TABLE not in sql


# ----- MySQL -----------------------------------------------------------------------

@pytest.mark.parametrize(
    ("table", "quoted"),
    [(PLAIN_TABLE, "`orders`"), (HOSTILE_TABLE, MYSQL_TABLE)],
    ids=NAME_IDS,
)
def test_mysql_row_count_quotes_the_table(fake_pymysql, table, quoted):
    connector = _mysql()
    cur = _wire_cursor(connector, [{"cnt": 1}])

    connector.get_row_count(table)

    assert _executed(cur.execute) == [(f"SELECT COUNT(*) AS cnt FROM {quoted}",)]


def test_mysql_column_stats_quote_hostile_names(fake_pymysql):
    connector = _mysql()
    cur = _wire_cursor(
        connector, [NULL_AND_DISTINCT_ROW, NUMERIC_TYPE_ROW, MEAN_AND_STDDEV_ROW]
    )

    connector.get_column_stats(HOSTILE_TABLE, HOSTILE_COLUMN)

    assert _executed(cur.execute) == [
        (
            f"SELECT   SUM(CASE WHEN {MYSQL_COLUMN} IS NULL THEN 1 ELSE 0 END) AS null_count, "
            f"  COUNT(DISTINCT {MYSQL_COLUMN}) AS distinct_count FROM {MYSQL_TABLE}",
        ),
        (MYSQL_COLUMN_TYPE_SQL, (MYSQL_DATABASE, HOSTILE_TABLE, HOSTILE_COLUMN)),
        (
            f"SELECT   AVG({MYSQL_COLUMN}) AS mean, "
            f"  STDDEV({MYSQL_COLUMN}) AS stddev FROM {MYSQL_TABLE}",
        ),
    ]


def test_mysql_metadata_lookups_bind_the_table_name(fake_pymysql):
    connector = _mysql()
    cur = _wire_cursor(connector, [None])

    connector.get_columns(HOSTILE_TABLE)
    connector.get_last_modified(HOSTILE_TABLE)

    for sql, params in _executed(cur.execute):
        assert params == (MYSQL_DATABASE, HOSTILE_TABLE)
        assert HOSTILE_TABLE not in sql


# ----- BigQuery --------------------------------------------------------------------

@pytest.mark.parametrize(
    ("table", "quoted"),
    [(PLAIN_TABLE, "`proj`.`ds`.`orders`"), (HOSTILE_TABLE, BIGQUERY_TABLE)],
    ids=NAME_IDS,
)
def test_bigquery_row_count_quotes_every_part(table, quoted):
    connector = _bigquery([[{"cnt": 1}]])

    connector.get_row_count(table)

    assert _executed(connector._query) == [(f"SELECT COUNT(*) AS cnt FROM {quoted}",)]


def test_bigquery_column_stats_escape_hostile_names():
    connector = _bigquery([[NULL_AND_DISTINCT_ROW], [MEAN_AND_STDDEV_ROW]])

    connector.get_column_stats(HOSTILE_TABLE, HOSTILE_COLUMN)

    assert _executed(connector._query) == [
        (
            f"SELECT   COUNTIF({BIGQUERY_COLUMN} IS NULL) AS null_count, "
            f"  COUNT(DISTINCT {BIGQUERY_COLUMN}) AS distinct_count FROM {BIGQUERY_TABLE}",
        ),
        (
            f"SELECT   AVG(CAST({BIGQUERY_COLUMN} AS FLOAT64)) AS mean, "
            f"  STDDEV(CAST({BIGQUERY_COLUMN} AS FLOAT64)) AS stddev FROM {BIGQUERY_TABLE}",
        ),
    ]


def test_bigquery_metadata_lookups_escape_the_table_name_as_a_string():
    connector = _bigquery([[], []])

    connector.get_columns(HOSTILE_TABLE)
    connector.get_last_modified(HOSTILE_TABLE)

    assert _executed(connector._query) == [
        (
            "SELECT column_name, data_type, is_nullable "
            "FROM `proj`.`ds`.INFORMATION_SCHEMA.COLUMNS "
            f"WHERE table_name = {BIGQUERY_TABLE_STRING} ORDER BY ordinal_position",
        ),
        (
            "SELECT TIMESTAMP_MILLIS(last_modified_time) AS last_mod "
            f"FROM `proj`.`ds`.__TABLES__ WHERE table_id = {BIGQUERY_TABLE_STRING}",
        ),
    ]


# ----- Snowflake -------------------------------------------------------------------

@pytest.mark.parametrize(
    ("table", "quoted"),
    [(PLAIN_TABLE, '"DB"."SCHEMA"."ORDERS"'), (HOSTILE_TABLE, SNOWFLAKE_TABLE)],
    ids=NAME_IDS,
)
def test_snowflake_row_count_quotes_every_part(table, quoted):
    connector = _snowflake([[{"cnt": 1}]])

    connector.get_row_count(table)

    assert _executed(connector._query) == [(f"SELECT COUNT(*) AS cnt FROM {quoted}",)]


def test_snowflake_column_stats_quote_hostile_names():
    connector = _snowflake([[NULL_AND_DISTINCT_ROW], [MEAN_AND_STDDEV_ROW]])

    connector.get_column_stats(HOSTILE_TABLE, HOSTILE_COLUMN)

    assert _executed(connector._query) == [
        (
            f"SELECT   COUNT_IF({SNOWFLAKE_COLUMN} IS NULL) AS null_count, "
            f"  COUNT(DISTINCT {SNOWFLAKE_COLUMN}) AS distinct_count FROM {SNOWFLAKE_TABLE}",
        ),
        (
            f"SELECT   AVG(TRY_CAST({SNOWFLAKE_COLUMN} AS FLOAT)) AS mean, "
            f"  STDDEV(TRY_CAST({SNOWFLAKE_COLUMN} AS FLOAT)) AS stddev FROM {SNOWFLAKE_TABLE}",
        ),
    ]


def test_snowflake_metadata_lookups_bind_schema_and_table_names():
    connector = _snowflake([[], [], []])

    connector.list_tables()
    connector.get_columns(HOSTILE_TABLE)
    connector.get_last_modified(HOSTILE_TABLE)

    table = HOSTILE_TABLE.upper()
    assert _executed(connector._query) == [
        (SNOWFLAKE_TABLES_SQL.format(database="DB"), (SNOWFLAKE_SCHEMA,)),
        (SNOWFLAKE_COLUMNS_SQL.format(database="DB"), (SNOWFLAKE_SCHEMA, table)),
        (SNOWFLAKE_LAST_ALTERED_SQL.format(database="DB"), (SNOWFLAKE_SCHEMA, table)),
    ]


def test_snowflake_lower_case_connection_string_resolves_like_unquoted_names():
    """Quoted Snowflake names are case-sensitive: `"analytics"` is not `analytics`."""
    connector = _snowflake(
        [[{"cnt": 1}], [NULL_AND_DISTINCT_ROW], [MEAN_AND_STDDEV_ROW], [], [], []],
        connection_string="snowflake://acct/analytics/public",
    )

    connector.get_row_count(PLAIN_TABLE)
    connector.get_column_stats(PLAIN_TABLE, PLAIN_COLUMN)
    connector.list_tables()
    connector.get_columns(PLAIN_TABLE)
    connector.get_last_modified(PLAIN_TABLE)

    table = '"ANALYTICS"."PUBLIC"."ORDERS"'
    assert _executed(connector._query) == [
        (f"SELECT COUNT(*) AS cnt FROM {table}",),
        (
            'SELECT   COUNT_IF("AMOUNT" IS NULL) AS null_count, '
            f'  COUNT(DISTINCT "AMOUNT") AS distinct_count FROM {table}',
        ),
        (
            'SELECT   AVG(TRY_CAST("AMOUNT" AS FLOAT)) AS mean, '
            f'  STDDEV(TRY_CAST("AMOUNT" AS FLOAT)) AS stddev FROM {table}',
        ),
        (SNOWFLAKE_TABLES_SQL.format(database="ANALYTICS"), ("PUBLIC",)),
        (SNOWFLAKE_COLUMNS_SQL.format(database="ANALYTICS"), ("PUBLIC", "ORDERS")),
        (SNOWFLAKE_LAST_ALTERED_SQL.format(database="ANALYTICS"), ("PUBLIC", "ORDERS")),
    ]


def test_snowflake_query_hands_params_to_the_driver():
    from scherlok.connectors.snowflake import SnowflakeConnector

    connector = SnowflakeConnector("snowflake://acct/db/schema")
    cur = MagicMock()
    cur.description = [("TABLE_NAME",)]
    cur.fetchall.return_value = [("ORDERS",)]
    connector._conn = MagicMock()
    connector._conn.cursor.return_value = cur

    assert connector.list_tables() == [PLAIN_TABLE]
    assert _executed(cur.execute) == [
        (SNOWFLAKE_TABLES_SQL.format(database="DB"), (SNOWFLAKE_SCHEMA,)),
    ]
