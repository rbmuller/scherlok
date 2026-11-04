"""Reconcile queries on PostgreSQL, MySQL, BigQuery and Snowflake, with the driver mocked (#111).

DuckDB runs them for real in `test_reconcile.py`. Here each connector gets a
mocked driver: the tests check that every identifier is quoted in the
connector's dialect, hostile names included, that schema-qualified names
resolve, and that driver rows become the right counts and findings.
"""

from __future__ import annotations

import sys
from collections.abc import Callable
from datetime import datetime
from decimal import Decimal
from unittest.mock import MagicMock

import pytest

from scherlok.connectors.base import BaseConnector, InvalidTableNameError
from scherlok.detector.anomaly_types import (
    DUPLICATE_KEYS,
    MISSING_KEYS,
    ROW_COUNT_MISMATCH,
    SUM_MISMATCH,
)
from scherlok.reconcile import reconcile_tables

DOUBLE_QUOTE = '"'
BACKTICK = "`"
# Would end a naive identifier and run a second statement.
HOSTILE_DOUBLE_QUOTE = 'orders"; DROP TABLE users; --'
HOSTILE_BACKTICK = "orders`; DROP TABLE users; --"
# A backslash first: escaping only the backtick would leave `\\` + "`", which closes it.
HOSTILE_BACKSLASH = "orders\\`; DROP TABLE users; --"

# Driver rows for one reconcile with a key and a sum column, in query order:
# source stats, source sums, target stats, target sums, missing keys, extra keys.
RECONCILE_ROWS = [
    {"table_rows": 10, "distinct_keys": 10, "duplicate_keys": 0, "keyed_rows": 10},
    {"sum_0": Decimal("100.00")},
    {"table_rows": 12, "distinct_keys": 9, "duplicate_keys": Decimal("2"), "keyed_rows": 12},
    {"sum_0": Decimal("130.00")},
    {"unmatched_keys": 1},
    {"unmatched_keys": 0},
]


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


def _cursor_connector(connector: BaseConnector, rows: list[dict]) -> Callable[[], list[str]]:
    """Wire a DB-API connection whose cursor returns ``rows``; return the executed SQL."""
    cur = MagicMock()
    cur.fetchone.side_effect = rows
    ctx = MagicMock()
    ctx.__enter__ = MagicMock(return_value=cur)
    ctx.__exit__ = MagicMock(return_value=False)
    connector._conn = MagicMock()
    connector._conn.cursor.return_value = ctx
    return lambda: [call.args[0] for call in cur.execute.call_args_list]


def _query_connector(connector: BaseConnector, rows: list[dict]) -> Callable[[], list[str]]:
    """Mock ``_query`` (BigQuery, Snowflake) to return ``rows``; return the executed SQL."""
    query = MagicMock(side_effect=[[row] for row in rows])
    connector._query = query
    return lambda: [call.args[0] for call in query.call_args_list]


def _postgres(rows):
    from scherlok.connectors.postgres import PostgresConnector

    connector = PostgresConnector("postgresql://u:p@h/d")
    return connector, _cursor_connector(connector, rows)


def _mysql(rows):
    from scherlok.connectors.mysql import MySQLConnector

    connector = MySQLConnector("mysql://u:p@h:3306/shop")
    return connector, _cursor_connector(connector, rows)


def _bigquery(rows):
    from scherlok.connectors.bigquery import BigQueryConnector

    connector = BigQueryConnector("bigquery://proj/ds")
    return connector, _query_connector(connector, rows)


def _snowflake(rows):
    from scherlok.connectors.snowflake import SnowflakeConnector

    connector = SnowflakeConnector("snowflake://acct/db/schema")
    return connector, _query_connector(connector, rows)


CONNECTORS = {
    "postgres": _postgres,
    "mysql": _mysql,
    "bigquery": _bigquery,
    "snowflake": _snowflake,
}


@pytest.fixture(params=sorted(CONNECTORS))
def make_connector(request, fake_pymysql):
    return CONNECTORS[request.param]


def _outside_identifiers(sql: str, quote: str, *, backslash_escapes: bool = False) -> str:
    """``sql`` with every quoted identifier cut out, so injected SQL would remain."""
    outside: list[str] = []
    index = 0
    while index < len(sql):
        if sql[index] != quote:
            outside.append(sql[index])
            index += 1
            continue
        index += 1
        while True:
            assert index < len(sql), f"unterminated identifier in {sql!r}"
            char = sql[index]
            if backslash_escapes and char == "\\":
                index += 2
            elif char == quote and not backslash_escapes and sql[index + 1:index + 2] == quote:
                index += 2
            elif char == quote:
                index += 1
                break
            else:
                index += 1
    return "".join(outside)


def _assert_contained(sql: str, quote: str, *, backslash_escapes: bool = False) -> None:
    outside = _outside_identifiers(sql, quote, backslash_escapes=backslash_escapes)
    assert ";" not in outside and "DROP" not in outside.upper(), sql


# ----- every connector ----------------------------------------------------------

def test_full_reconcile_reads_driver_rows_into_findings(make_connector):
    connector, executed = make_connector(RECONCILE_ROWS)

    result = reconcile_tables(
        connector, "raw.orders", "mart.orders", key_columns=["order_id"], sum_columns=["amount"]
    )

    assert len(executed()) == len(RECONCILE_ROWS)
    payload = result.to_dict()
    assert payload["target"] == {
        "table": "mart.orders", "row_count": 12, "null_keys": 0, "distinct_keys": 9,
        "duplicate_keys": 2, "duplicate_rows": 3, "sums": {"amount": 130},
    }
    assert payload["missing_keys"] == 1 and payload["extra_keys"] == 0
    assert [a["type"] for a in payload["anomalies"]] == [
        ROW_COUNT_MISMATCH, DUPLICATE_KEYS, MISSING_KEYS, SUM_MISMATCH,
    ]
    assert payload["passed"] is False


def test_row_count_without_a_key_is_one_query(make_connector):
    connector, executed = make_connector([{"table_rows": 7}])

    stats = connector.get_key_stats("orders")

    assert stats == {
        "row_count": 7, "null_keys": None, "distinct_keys": None,
        "duplicate_keys": None, "duplicate_rows": None,
    }
    assert len(executed()) == 1
    assert "GROUP BY" not in executed()[0]


def test_no_sum_columns_runs_no_query(make_connector):
    connector, executed = make_connector([])

    assert connector.get_column_sums("orders", []) == {}
    assert executed() == []


def test_queries_read_aggregates_only(make_connector):
    connector, executed = make_connector(RECONCILE_ROWS)

    reconcile_tables(connector, "a", "b", key_columns=["k"], sum_columns=["amount"])

    for sql in executed():
        assert sql.startswith("SELECT COUNT(*)") or sql.startswith("SELECT r.") or (
            sql.startswith("SELECT SUM(")
        ), sql
        assert "MIN(" not in sql and "MAX(" not in sql and "LIMIT" not in sql


# ----- quoting per dialect ------------------------------------------------------

def test_postgres_quotes_hostile_names():
    connector, executed = _postgres([{"unmatched_keys": 0}])

    connector.count_unmatched_keys(
        f"raw.{HOSTILE_DOUBLE_QUOTE}", "mart.orders", [HOSTILE_DOUBLE_QUOTE]
    )

    sql = executed()[0]
    assert '"raw"."orders""; DROP TABLE users; --"' in sql
    assert 's."orders""; DROP TABLE users; --" IS NOT NULL' in sql
    _assert_contained(sql, DOUBLE_QUOTE)


def test_mysql_quotes_hostile_names_in_backticks(fake_pymysql):
    connector, executed = _mysql([{"sum_0": Decimal("1.50")}])

    sums = connector.get_column_sums(f"shop.{HOSTILE_BACKTICK}", [HOSTILE_BACKTICK])

    sql = executed()[0]
    assert "`shop`.`orders``; DROP TABLE users; --`" in sql
    assert "SUM(`orders``; DROP TABLE users; --`)" in sql
    assert sums == {HOSTILE_BACKTICK: Decimal("1.50")}
    _assert_contained(sql, BACKTICK)


def test_mysql_rejects_a_three_part_name(fake_pymysql):
    connector, executed = _mysql([])

    with pytest.raises(InvalidTableNameError):
        connector.get_key_stats("db.schema.orders")
    assert executed() == []


@pytest.mark.parametrize("hostile", [HOSTILE_BACKTICK, HOSTILE_BACKSLASH])
def test_bigquery_escapes_backticks_and_backslashes(hostile):
    connector, executed = _bigquery([{"table_rows": 1}])

    connector.get_key_stats(hostile)

    sql = executed()[0]
    assert sql.startswith("SELECT COUNT(*) AS table_rows FROM `proj`.`ds`.`")
    _assert_contained(sql, BACKTICK, backslash_escapes=True)


@pytest.mark.parametrize(
    ("table", "expected"),
    [
        ("orders", "`proj`.`ds`.`orders`"),
        ("other_ds.orders", "`proj`.`other_ds`.`orders`"),
        ("other-proj.other_ds.orders", "`other-proj`.`other_ds`.`orders`"),
    ],
)
def test_bigquery_fills_in_the_project_and_dataset(table, expected):
    connector, executed = _bigquery([{"table_rows": 1}])

    connector.get_key_stats(table)

    assert executed()[0].endswith(f"FROM {expected}")


def test_snowflake_upper_cases_and_quotes_every_part():
    connector, executed = _snowflake([{"unmatched_keys": 2}])

    unmatched = connector.count_unmatched_keys(
        HOSTILE_DOUBLE_QUOTE, "raw.orders", ["order_id", "line_no"]
    )

    sql = executed()[0]
    assert unmatched == 2
    assert '"DB"."SCHEMA"."ORDERS""; DROP TABLE USERS; --"' in sql
    assert '"DB"."RAW"."ORDERS"' in sql
    assert 'o."ORDER_ID" = s."ORDER_ID" AND o."LINE_NO" = s."LINE_NO"' in sql
    _assert_contained(sql, DOUBLE_QUOTE)


def test_snowflake_three_part_name_overrides_the_connection():
    connector, executed = _snowflake([{"table_rows": 1}])

    connector.get_key_stats("other_db.raw.orders")

    assert executed()[0].endswith('FROM "OTHER_DB"."RAW"."ORDERS"')


# ----- a connector without reconcile support ------------------------------------

class _MinimalConnector(BaseConnector):
    """Implements only the abstract methods, like a third-party connector might."""

    def connect(self) -> bool:
        return True

    def list_tables(self) -> list[str]:
        return []

    def get_row_count(self, table: str) -> int:
        return 0

    def get_columns(self, table: str) -> list[dict]:
        return []

    def get_column_stats(self, table: str, column: str) -> dict:
        return {}

    def get_last_modified(self, table: str) -> datetime | None:
        return None


def test_a_connector_without_reconcile_support_says_so():
    with pytest.raises(NotImplementedError, match="_MinimalConnector does not support reconcile"):
        reconcile_tables(_MinimalConnector("x://"), "a", "b")


def test_an_anti_join_needs_a_key(make_connector):
    connector, executed = make_connector([])

    with pytest.raises(ValueError, match="at least one key column"):
        connector.count_unmatched_keys("a", "b", [])
    assert executed() == []
