"""Tests for `scherlok reconcile`, an output table checked against its input (#111).

Driven end-to-end against a real DuckDB file. Every key is a sentinel string,
so a test can assert that no key value ever reaches the output, whatever the
finding or error.
"""

from __future__ import annotations

import importlib.util
import json
import math
from decimal import Decimal
from pathlib import Path

import pytest
from typer.testing import CliRunner

from scherlok.cli import app
from scherlok.connectors.base import InvalidTableNameError, split_table_name
from scherlok.detector.anomaly_types import (
    DUPLICATE_KEYS,
    EXTRA_KEYS,
    MISSING_KEYS,
    ROW_COUNT_MISMATCH,
    SUM_MISMATCH,
)
from scherlok.detector.fields import DETAIL_FIELDS
from scherlok.detector.hints import ANOMALY_HINTS
from scherlok.reconcile import (
    MAX_COLUMNS,
    RECONCILE_TYPES,
    ReconcileError,
    json_number,
    reconcile_tables,
    sums_match,
    validate_reconcile_args,
)

pytestmark = pytest.mark.skipif(
    importlib.util.find_spec("duckdb") is None,
    reason="duckdb not installed",
)

runner = CliRunner(env={"NO_COLOR": "1"})

ROWS = 1000
SENTINEL = "SENTINEL-KEY"
SOURCE = "raw.orders"
TARGET = "mart.orders"
# 1.25 * (1 + 2 + ... + 1000)
AMOUNT_TOTAL = 625625
FAN_OUT_CUSTOMER = 3
ORDERS_PER_CUSTOMER = ROWS // 10
JSON_KEYS = {
    "passed", "source", "target", "key", "sum", "tolerance", "missing_keys", "extra_keys",
    "anomaly_count", "critical", "warning", "info", "anomalies",
}
SIDE_KEYS = {
    "table", "row_count", "null_keys", "distinct_keys", "duplicate_keys", "duplicate_rows", "sums",
}
LEGACY_KEYS = ("table", "type", "severity", "message")

# The target joins a customer table where one customer appears twice.
FAN_OUT = (
    f"INSERT INTO raw.customers VALUES ({FAN_OUT_CUSTOMER}, 'south')",
    f"CREATE TABLE {TARGET} AS "
    f"SELECT o.* FROM {SOURCE} o JOIN raw.customers USING (customer_id)",
)
# A target whose key is an INTEGER: comparing it with the VARCHAR source key
# makes DuckDB fail with an error that quotes the key it could not convert.
INTEGER_KEYED_TARGET = (
    f"CREATE TABLE {TARGET} (order_id INTEGER)",
    f"INSERT INTO {TARGET} VALUES (1)",
)
# Fan-out, plus one customer's orders lost and one key the source lacks.
EVERY_FINDING = (
    *FAN_OUT,
    f"DELETE FROM {TARGET} WHERE customer_id = 1",
    f"INSERT INTO {TARGET} VALUES ('{SENTINEL}-NEW', 1, 1.00, 0.1)",
)


@pytest.fixture()
def warehouse(tmp_path, monkeypatch) -> Path:
    """A DuckDB file with `raw.orders` (1,000 rows, sentinel keys), reachable via the env."""
    path = tmp_path / "warehouse.duckdb"
    _execute(
        path,
        "CREATE SCHEMA raw",
        "CREATE SCHEMA mart",
        "CREATE TABLE raw.orders "
        "(order_id VARCHAR, customer_id INTEGER, amount DECIMAL(18, 2), fee DOUBLE)",
        f"INSERT INTO raw.orders SELECT '{SENTINEL}-' || lpad(i::VARCHAR, 4, '0'), i % 10, "
        f"i * 1.25, i * 0.1 FROM range(1, {ROWS + 1}) t(i)",
        "CREATE TABLE raw.customers (customer_id INTEGER, region VARCHAR)",
        "INSERT INTO raw.customers SELECT i, 'north' FROM range(0, 10) t(i)",
    )
    monkeypatch.setenv("SCHERLOK_CONNECTION", f"duckdb:///{path}")
    return path


def _execute(path: Path, *statements: str) -> None:
    import duckdb

    conn = duckdb.connect(str(path))
    try:
        for sql in statements:
            conn.execute(sql)
    finally:
        conn.close()


def _copy_source(path: Path, target: str = TARGET) -> None:
    _execute(path, f"CREATE TABLE {target} AS SELECT * FROM {SOURCE}")


def _reconcile(*args: str):
    return runner.invoke(
        app, ["reconcile", "--source", SOURCE, "--target", TARGET, *args]
    )


def _reconcile_json(*args: str):
    result = _reconcile("--output", "json", *args)
    return result, json.loads(result.stdout)


def _by_type(payload: dict) -> dict[str, dict]:
    return {anomaly["type"]: anomaly for anomaly in payload["anomalies"]}


def _assert_no_key_values(*outputs: str) -> None:
    for output in outputs:
        assert SENTINEL not in output


# ----- the four cases of the issue ---------------------------------------------

def test_clean_copy_passes(warehouse):
    _copy_source(warehouse)

    result, payload = _reconcile_json("--key", "order_id", "--sum", "amount")

    assert result.exit_code == 0, result.output
    assert payload["passed"] is True
    assert payload["anomalies"] == [] and payload["anomaly_count"] == 0
    assert payload["missing_keys"] == 0 and payload["extra_keys"] == 0
    for side in ("source", "target"):
        assert payload[side]["row_count"] == ROWS
        assert payload[side]["distinct_keys"] == ROWS
        assert payload[side]["duplicate_keys"] == 0
        assert payload[side]["null_keys"] == 0
        assert payload[side]["sums"] == {"amount": AMOUNT_TOTAL}


def test_join_that_fans_out_is_caught(warehouse):
    _execute(warehouse, *FAN_OUT)

    result, payload = _reconcile_json("--key", "order_id", "--sum", "amount")

    assert result.exit_code == 1
    assert payload["passed"] is False
    findings = _by_type(payload)
    assert set(findings) == {ROW_COUNT_MISMATCH, DUPLICATE_KEYS, SUM_MISMATCH}
    assert payload["target"]["duplicate_keys"] == ORDERS_PER_CUSTOMER
    assert payload["target"]["duplicate_rows"] == ORDERS_PER_CUSTOMER
    assert payload["missing_keys"] == 0 and payload["extra_keys"] == 0

    duplicates = findings[DUPLICATE_KEYS]
    assert duplicates["table"] == TARGET
    assert duplicates["severity"] == "CRITICAL"
    assert duplicates["column"] == "order_id"
    assert duplicates["metric"] == "duplicate_keys"
    assert (duplicates["baseline"], duplicates["current"]) == (0, ORDERS_PER_CUSTOMER)
    assert duplicates["change_pct"] is None
    assert duplicates["hint"] == ANOMALY_HINTS[DUPLICATE_KEYS]

    rows = findings[ROW_COUNT_MISMATCH]
    assert rows["column"] is None and rows["metric"] == "row_count"
    assert (rows["baseline"], rows["current"]) == (ROWS, ROWS + ORDERS_PER_CUSTOMER)
    assert rows["change_pct"] == 10.0


def test_lost_rows_are_counted_as_missing_keys(warehouse):
    _execute(
        warehouse,
        f"CREATE TABLE {TARGET} AS SELECT * FROM {SOURCE} WHERE customer_id <> {FAN_OUT_CUSTOMER}",
    )

    result, payload = _reconcile_json("--key", "order_id")

    assert result.exit_code == 1
    findings = _by_type(payload)
    assert set(findings) == {ROW_COUNT_MISMATCH, MISSING_KEYS}
    missing = findings[MISSING_KEYS]
    assert payload["missing_keys"] == ORDERS_PER_CUSTOMER
    assert missing["metric"] == "missing_keys"
    assert (missing["baseline"], missing["current"]) == (0, ORDERS_PER_CUSTOMER)
    assert f"{ORDERS_PER_CUSTOMER} of {ROWS:,} order_id values" in missing["message"]


def test_changed_total_with_the_same_row_count(warehouse):
    """The case a table's own history can't see: same rows, a different total."""
    _execute(
        warehouse,
        f"CREATE TABLE {TARGET} AS SELECT order_id, customer_id, "
        "CASE WHEN customer_id = 0 THEN amount * 100 ELSE amount END AS amount, fee "
        f"FROM {SOURCE}",
    )

    result, payload = _reconcile_json("--key", "order_id", "--sum", "amount", "--sum", "fee")

    assert result.exit_code == 1
    assert [anomaly["type"] for anomaly in payload["anomalies"]] == [SUM_MISMATCH]
    finding = payload["anomalies"][0]
    assert finding["column"] == "amount" and finding["metric"] == "sum"
    assert finding["baseline"] == AMOUNT_TOTAL
    assert finding["current"] > AMOUNT_TOTAL
    assert finding["change_pct"] > 0
    assert payload["source"]["row_count"] == payload["target"]["row_count"] == ROWS


def test_keys_the_source_lacks_are_extra_keys(warehouse):
    _copy_source(warehouse)
    _execute(warehouse, f"UPDATE {TARGET} SET order_id = order_id || '-X' WHERE customer_id = 1")

    result, payload = _reconcile_json("--key", "order_id")

    assert result.exit_code == 1
    findings = _by_type(payload)
    assert set(findings) == {MISSING_KEYS, EXTRA_KEYS}  # row count unchanged
    assert payload["missing_keys"] == payload["extra_keys"] == ORDERS_PER_CUSTOMER
    extra = findings[EXTRA_KEYS]
    assert (extra["baseline"], extra["current"]) == (0, ORDERS_PER_CUSTOMER)


def test_every_finding_has_the_structured_fields(warehouse):
    _execute(warehouse, *EVERY_FINDING)

    _, payload = _reconcile_json("--key", "order_id", "--sum", "amount")

    assert {anomaly["type"] for anomaly in payload["anomalies"]} == set(RECONCILE_TYPES)
    for anomaly in payload["anomalies"]:
        assert list(anomaly) == [*LEGACY_KEYS, *DETAIL_FIELDS]
        assert anomaly["hint"] == ANOMALY_HINTS[anomaly["type"]]


# ----- --tolerance -------------------------------------------------------------

def test_tolerance_is_absolute_and_inclusive_on_a_decimal_total(warehouse):
    _copy_source(warehouse)
    _execute(warehouse, f"UPDATE {TARGET} SET amount = amount + 0.01 WHERE order_id LIKE '%-0001'")

    exact, exact_payload = _reconcile_json("--sum", "amount")
    at_limit, at_limit_payload = _reconcile_json("--sum", "amount", "--tolerance", "0.01")
    below, _ = _reconcile_json("--sum", "amount", "--tolerance", "0.009")

    assert exact.exit_code == 1
    assert _by_type(exact_payload)[SUM_MISMATCH]["current"] == AMOUNT_TOTAL + 0.01
    assert at_limit.exit_code == 0 and at_limit_payload["passed"] is True
    assert at_limit_payload["tolerance"] == 0.01
    assert below.exit_code == 1


def test_tolerance_absorbs_float_rounding_in_a_double_total(warehouse):
    """0.1 + 0.2 sums to 0.30000000000000004, so an exact comparison fails."""
    _execute(
        warehouse,
        "CREATE TABLE raw.fees (fee_id VARCHAR, fee DOUBLE)",
        f"INSERT INTO raw.fees VALUES ('{SENTINEL}-A', 0.1), ('{SENTINEL}-B', 0.2)",
        "CREATE TABLE mart.fees (fee_id VARCHAR, fee DOUBLE)",
        f"INSERT INTO mart.fees VALUES ('{SENTINEL}-A', 0.15), ('{SENTINEL}-B', 0.15)",
    )
    args = ["reconcile", "--source", "raw.fees", "--target", "mart.fees", "--sum", "fee"]

    exact = runner.invoke(app, args)
    tolerant = runner.invoke(app, [*args, "--tolerance", "1e-9"])

    assert exact.exit_code == 1
    assert tolerant.exit_code == 0, tolerant.output


def test_tolerance_compares_a_double_total_at_the_value_it_prints(warehouse):
    """100.01 - 100.0 is 0.010000000000005116 in floats; a 0.01 tolerance must still pass."""
    _execute(
        warehouse,
        "CREATE TABLE raw.fees (fee DOUBLE)",
        "INSERT INTO raw.fees VALUES (100.0)",
        "CREATE TABLE mart.fees (fee DOUBLE)",
        "INSERT INTO mart.fees VALUES (100.01)",
    )
    args = ["reconcile", "--source", "raw.fees", "--target", "mart.fees", "--sum", "fee"]

    assert runner.invoke(app, [*args, "--tolerance", "0.01"]).exit_code == 0
    assert runner.invoke(app, [*args, "--tolerance", "0.0099"]).exit_code == 1


def test_a_double_total_reconciles_against_a_decimal_total(warehouse):
    _execute(
        warehouse,
        f"CREATE TABLE {TARGET} AS SELECT order_id, amount::DOUBLE AS amount FROM {SOURCE}",
    )

    result, payload = _reconcile_json("--sum", "amount", "--tolerance", "1e-6")

    assert result.exit_code == 0, result.output
    assert payload["target"]["sums"]["amount"] == AMOUNT_TOTAL


@pytest.mark.parametrize(
    ("source_total", "target_total", "tolerance", "expected"),
    [
        (Decimal("100.00"), Decimal("100.01"), 0.01, True),
        (Decimal("100.00"), Decimal("100.01"), 0.0099, False),
        (100.0, 100.01, 0.01, True),
        (0.1 + 0.2, 0.3, 0.0, False),
        (0.1 + 0.2, 0.3, 1e-9, True),
        (10, Decimal("10.00"), 0.0, True),
        (None, None, 0.0, True),
        (None, 0, 0.0, True),
        (None, Decimal("5"), 0.0, False),
        (float("nan"), float("nan"), 1.0, False),
        (float("inf"), float("inf"), 0.0, True),
        (float("inf"), 5.0, 1e300, False),
    ],
)
def test_sums_match(source_total, target_total, tolerance, expected):
    assert sums_match(source_total, target_total, tolerance) is expected
    assert sums_match(target_total, source_total, tolerance) is expected


def test_json_number():
    assert json_number(Decimal("6.60")) == 6.6
    assert json_number(Decimal("6.00")) == 6 and isinstance(json_number(Decimal("6.00")), int)
    assert json_number(12345678901234567890) == 12345678901234567890
    assert json_number(Decimal("12345678901234567890")) == 12345678901234567890
    assert json_number(0.30000000000000004) == 0.30000000000000004
    for empty in (None, float("nan"), float("inf"), Decimal("NaN"), True):
        assert json_number(empty) is None


@pytest.mark.parametrize("tolerance", ["-0.5", "nan", "inf"])
def test_a_tolerance_that_could_never_fail_or_never_pass_is_rejected(warehouse, tolerance):
    _copy_source(warehouse)

    result = _reconcile("--sum", "amount", "--tolerance", tolerance)

    assert result.exit_code == 1
    assert "Tolerance must be a finite number of 0 or more" in result.output


# ----- NULLs -------------------------------------------------------------------

def test_null_keys_are_counted_apart_and_never_matched(warehouse):
    _copy_source(warehouse)
    _execute(warehouse, f"UPDATE {TARGET} SET order_id = NULL WHERE customer_id = 2")

    result, payload = _reconcile_json("--key", "order_id")

    assert result.exit_code == 1
    target = payload["target"]
    assert target["null_keys"] == ORDERS_PER_CUSTOMER
    assert target["distinct_keys"] == ROWS - ORDERS_PER_CUSTOMER
    assert target["duplicate_keys"] == 0  # NULLs are not one repeated key
    assert payload["missing_keys"] == ORDERS_PER_CUSTOMER
    assert payload["extra_keys"] == 0
    assert set(_by_type(payload)) == {MISSING_KEYS}


def test_a_total_with_no_values_counts_as_zero(warehouse):
    _execute(
        warehouse,
        "CREATE TABLE raw.fees (fee DECIMAL(10, 2))",
        "INSERT INTO raw.fees VALUES (NULL), (NULL)",
        "CREATE TABLE mart.fees (fee DECIMAL(10, 2))",
        "INSERT INTO mart.fees VALUES (0), (NULL)",
        "CREATE TABLE mart.fees_lost (fee DECIMAL(10, 2))",
        "INSERT INTO mart.fees_lost VALUES (NULL), (NULL)",
    )

    zero = runner.invoke(
        app, ["reconcile", "--source", "raw.fees", "--target", "mart.fees", "--sum", "fee",
              "--output", "json"],
    )
    lost = runner.invoke(
        app, ["reconcile", "--source", "mart.fees", "--target", "mart.fees_lost",
              "--sum", "fee", "--output", "json"],
    )

    assert zero.exit_code == 0, zero.output
    assert json.loads(zero.stdout)["source"]["sums"] == {"fee": None}
    # 0 against no value at all: both count as 0.
    assert lost.exit_code == 0, lost.output


def test_a_total_that_became_null_is_a_mismatch(warehouse):
    _execute(warehouse, f"CREATE TABLE {TARGET} AS SELECT order_id, NULL::DECIMAL(18, 2) AS amount "
             f"FROM {SOURCE}")

    result, payload = _reconcile_json("--sum", "amount")

    assert result.exit_code == 1
    finding = _by_type(payload)[SUM_MISMATCH]
    assert (finding["baseline"], finding["current"]) == (AMOUNT_TOTAL, None)
    assert "-> NULL" in finding["message"]


# ----- keys and table names ----------------------------------------------------

def test_composite_key(warehouse):
    _execute(
        warehouse,
        "CREATE TABLE raw.lines AS SELECT order_id, l.line_no, amount "
        f"FROM {SOURCE}, (SELECT 1 AS line_no UNION ALL SELECT 2) l",
        "CREATE TABLE mart.lines AS SELECT * FROM raw.lines",
    )
    args = ["reconcile", "--source", "raw.lines", "--target", "mart.lines", "--output", "json"]

    unique = runner.invoke(app, [*args, "--key", "order_id", "--key", "line_no"])
    only_order = runner.invoke(app, [*args, "--key", "order_id"])

    assert unique.exit_code == 0, unique.output
    assert json.loads(unique.stdout)["key"] == ["order_id", "line_no"]
    assert only_order.exit_code == 1
    finding = _by_type(json.loads(only_order.stdout))[DUPLICATE_KEYS]
    assert finding["current"] == ROWS
    assert finding["baseline"] == ROWS  # the source repeats order_id too


def test_composite_key_label_names_every_column(warehouse):
    _copy_source(warehouse)
    _execute(warehouse, f"INSERT INTO {TARGET} SELECT * FROM {SOURCE} WHERE customer_id = 4")

    _, payload = _reconcile_json("--key", "order_id", "--key", "customer_id")

    assert _by_type(payload)[DUPLICATE_KEYS]["column"] == "(order_id, customer_id)"


def test_without_a_key_only_the_row_count_is_compared(warehouse):
    _copy_source(warehouse)
    _execute(warehouse, f"UPDATE {TARGET} SET order_id = 'other'")

    result, payload = _reconcile_json()

    assert result.exit_code == 0
    assert payload["missing_keys"] is None and payload["extra_keys"] is None
    assert set(payload["target"]) == SIDE_KEYS
    assert payload["target"]["distinct_keys"] is None


def test_unqualified_and_three_part_names_work(warehouse):
    _execute(
        warehouse,
        f"CREATE TABLE main.orders AS SELECT * FROM {SOURCE}",
        f"CREATE TABLE {TARGET} AS SELECT * FROM {SOURCE}",
    )

    result = runner.invoke(
        app, ["reconcile", "--source", "orders", "--target", "warehouse.mart.orders",
              "--key", "order_id", "--sum", "amount"],
    )

    assert result.exit_code == 0, result.output


def test_hostile_table_and_column_names_are_quoted(warehouse):
    """Names that would end the identifier and run a second statement if not quoted.

    A dot always separates schema and table, so the injected statement names
    an unqualified table.
    """
    hostile_table = 'orders"; DROP TABLE victim; --'
    hostile_column = 'id"; DROP TABLE victim; --'
    quoted_table, quoted_column = (
        '"' + name.replace('"', '""') + '"' for name in (hostile_table, hostile_column)
    )
    _execute(
        warehouse,
        "CREATE TABLE victim (id INTEGER)",
        f"CREATE TABLE raw.renamed AS SELECT order_id AS {quoted_column}, amount FROM {SOURCE}",
        f"CREATE TABLE mart.{quoted_table} AS SELECT * FROM raw.renamed",
    )

    result, payload = _reconcile_json_for(
        "raw.renamed", f"mart.{hostile_table}", "--key", hostile_column, "--sum", "amount"
    )

    assert result.exit_code == 0, result.output
    assert payload["target"]["table"] == f"mart.{hostile_table}"
    assert payload["target"]["row_count"] == payload["target"]["distinct_keys"] == ROWS
    _execute(warehouse, "SELECT COUNT(*) FROM victim")  # raises if it was dropped


def test_rich_markup_in_table_and_column_names_is_printed_not_interpreted(warehouse):
    """`[/]` closes no tag: read as markup, Rich would raise instead of printing it."""
    _execute(
        warehouse,
        f'CREATE TABLE mart."in[/]" AS SELECT order_id, amount AS "a[/]" FROM {SOURCE}',
        'CREATE TABLE mart."out[/]" AS SELECT * FROM mart."in[/]" LIMIT 10',
    )

    result = runner.invoke(
        app, ["reconcile", "--source", "mart.in[/]", "--target", "mart.out[/]",
              "--key", "order_id", "--sum", "a[/]"],
    )

    assert result.exit_code == 1, result.output
    assert "mart.in[/]" in result.output and "mart.out[/]" in result.output
    assert "total a[/]" in result.output
    assert "Reconcile failed" in result.output


@pytest.mark.parametrize("name", ["raw..orders", "a.b.c.d", ".orders", "orders."])
def test_an_unaddressable_table_name_exits_1(warehouse, name):
    result = runner.invoke(app, ["reconcile", "--source", SOURCE, "--target", name])

    assert result.exit_code == 1
    assert "Invalid table name" in result.output


def test_split_table_name():
    assert split_table_name("orders", 3) == ["orders"]
    assert split_table_name("raw.orders", 3) == ["raw", "orders"]
    assert split_table_name("db.raw.orders", 3) == ["db", "raw", "orders"]
    with pytest.raises(InvalidTableNameError):
        split_table_name("db.raw.orders", 2)
    with pytest.raises(InvalidTableNameError):
        split_table_name("", 3)


# ----- no key values in the output ---------------------------------------------

def test_no_key_value_reaches_the_output_on_any_finding(warehouse):
    _execute(warehouse, *EVERY_FINDING)
    args = ("--key", "order_id", "--sum", "amount")

    text = _reconcile(*args)
    as_json = _reconcile("--output", "json", *args)

    assert text.exit_code == as_json.exit_code == 1
    assert len(json.loads(as_json.stdout)["anomalies"]) == len(RECONCILE_TYPES)
    _assert_no_key_values(text.stdout, text.stderr, as_json.stdout, as_json.stderr)


def test_a_database_error_never_quotes_a_key_value(warehouse):
    """DuckDB's own message here is "Could not convert string 'SENTINEL-KEY-0001' ..."."""
    _execute(warehouse, *INTEGER_KEYED_TARGET)

    result = _reconcile("--key", "order_id", "--output", "json")

    assert result.exit_code == 1
    assert result.stdout == ""
    assert "ConversionException" in result.stderr
    assert "same type in both tables" in result.stderr
    _assert_no_key_values(result.stdout, result.stderr)


def test_core_raises_reconcile_error_with_the_error_type_only(warehouse):
    from scherlok.connectors.duckdb import DuckDBConnector

    _execute(warehouse, *INTEGER_KEYED_TARGET)
    connector = DuckDBConnector(f"duckdb:///{warehouse}")
    assert connector.connect()

    with pytest.raises(ReconcileError) as caught:
        reconcile_tables(connector, SOURCE, TARGET, key_columns=["order_id"])

    assert "ConversionException" in str(caught.value)
    assert SENTINEL not in str(caught.value)
    assert SENTINEL in str(caught.value.__cause__)  # why the text is dropped


# ----- JSON contract and errors -----------------------------------------------

def test_json_is_one_object_on_stdout_and_the_verdict_goes_to_stderr(warehouse):
    _execute(warehouse, f"CREATE TABLE {TARGET} AS SELECT * FROM {SOURCE} LIMIT 10")

    result = _reconcile("--key", "order_id", "--sum", "amount", "--output", "json")

    payload = json.loads(result.stdout)
    assert set(payload) == JSON_KEYS
    assert set(payload["source"]) == set(payload["target"]) == SIDE_KEYS
    assert payload["key"] == ["order_id"] and payload["sum"] == ["amount"]
    assert payload["critical"] == payload["anomaly_count"] == len(payload["anomalies"])
    assert "Reconcile failed" in result.stderr


def test_text_mode_shows_the_comparison_and_the_verdict(warehouse):
    _copy_source(warehouse)

    result = _reconcile("--key", "order_id", "--sum", "amount")

    assert result.exit_code == 0
    assert "distinct keys" in result.stdout
    assert "625,625.00" in result.stdout
    assert "Reconcile passed" in result.stdout


def test_nothing_is_saved(warehouse, isolated_scherlok_dir):
    _execute(warehouse, f"CREATE TABLE {TARGET} AS SELECT * FROM {SOURCE} LIMIT 10")

    _reconcile("--key", "order_id", "--sum", "amount")

    assert not isolated_scherlok_dir.exists()


def test_a_missing_table_exits_1(warehouse):
    result = _reconcile("--key", "order_id")

    assert result.exit_code == 1
    assert "CatalogException" in result.output


def test_a_missing_column_exits_1(warehouse):
    _copy_source(warehouse)

    result = _reconcile("--sum", "revenue")

    assert result.exit_code == 1
    assert "BinderException" in result.output


@pytest.mark.parametrize(
    ("args", "message"),
    [
        (["--source", SOURCE, "--target", SOURCE], "must be different tables"),
        (["--source", SOURCE, "--target", TARGET, "--key", ""], "Empty key column"),
        (["--source", SOURCE, "--target", TARGET, "--output", "xml"], "Invalid --output"),
    ],
)
def test_invalid_arguments_exit_1_before_connecting(monkeypatch, args, message):
    monkeypatch.delenv("SCHERLOK_CONNECTION", raising=False)

    result = runner.invoke(app, ["reconcile", *args])

    assert result.exit_code == 1
    assert message in result.output
    assert "No connection configured" not in result.output


def test_validate_reconcile_args_deduplicates_and_caps_columns():
    keys, sums = validate_reconcile_args("a", "b", ["k", "k", "j"], None, 0.0)
    assert keys == ["k", "j"] and sums == []

    too_many = [f"c{index}" for index in range(MAX_COLUMNS + 1)]
    with pytest.raises(ValueError, match=f"At most {MAX_COLUMNS}"):
        validate_reconcile_args("a", "b", None, too_many, 0.0)
    with pytest.raises(ValueError, match="finite"):
        validate_reconcile_args("a", "b", None, None, math.nan)


def _reconcile_json_for(source: str, target: str, *args: str):
    result = runner.invoke(
        app, ["reconcile", "--source", source, "--target", target, "--output", "json", *args]
    )
    return result, json.loads(result.stdout)
