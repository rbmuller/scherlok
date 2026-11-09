"""Tests for `scherlok verify --against` and `--ignore volume` (#110, part 2).

End to end on two DuckDB files sharing one profile store: production, whose
baseline has learned from several runs, and a staging build of the same
table. The staging tables are compared with production's baseline, and
nothing is saved under either database.
"""

from __future__ import annotations

import importlib.util
import json
import sqlite3
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest
from typer.testing import CliRunner

from scherlok.cli import app
from scherlok.detector.anomaly_types import (
    CARDINALITY_CHANGE,
    COLUMN_REMOVED,
    FRESHNESS_CRITICAL,
    TABLE_EMPTY,
    VOLUME_DROP,
)
from scherlok.detector.severity import Severity
from scherlok.runs import COMMAND_VERIFY
from scherlok.service import (
    CHECK_FRESHNESS,
    CHECK_VOLUME,
    AgainstScopeError,
    connection_scope,
    inspect_table,
    profile_and_detect,
    verify_tables,
)
from scherlok.store.sqlite import RUN_COLUMNS, ProfileStore

pytestmark = pytest.mark.skipif(
    importlib.util.find_spec("duckdb") is None, reason="duckdb not installed"
)

runner = CliRunner(env={"NO_COLOR": "1"})

STORE_FILENAME = "profiles.db"
SECRET = "S3CRET-t0ken"
ROWS = 5000
APPENDED_ROWS = 100
# Enough runs for the learned baselines (MIN_ADAPTIVE_SAMPLES), on a table
# that grows between runs as production tables do.
LEARNING_RUNS = 6
PRODUCTION_ROWS = ROWS + LEARNING_RUNS * APPENDED_ROWS
# A pseudo-random tenth of the rows, as a dev build on sampled data keeps.
SAMPLE_FILTER = "hash(i) % 10 = 0"
ORDERS_DDL = (
    "CREATE TABLE orders "
    "(id BIGINT, customer_id BIGINT, amount DOUBLE, status VARCHAR, email VARCHAR)"
)
# One order per i: 2,000 customers, three statuses, every 20th e-mail NULL.
ORDER_ROW = (
    "i, i % 2000, round(50 + (hash(i) % 10000) / 100.0, 2), "
    "CASE i % 3 WHEN 0 THEN 'paid' WHEN 1 THEN 'open' ELSE 'void' END, "
    "CASE WHEN i % 20 = 0 THEN NULL ELSE 'u' || i || '@example.invalid' END"
)


def _execute(path: Path, *statements: str) -> None:
    import duckdb

    conn = duckdb.connect(str(path))
    try:
        for sql in statements:
            conn.execute(sql)
    finally:
        conn.close()


def _insert_orders(path: Path, first: int, last: int, where: str = "TRUE") -> None:
    _execute(path, f"INSERT INTO orders SELECT {ORDER_ROW} FROM range({first}, {last + 1}) t(i) "
                   f"WHERE {where}")


def _invoke(monkeypatch, connection: str, *args: str):
    monkeypatch.setenv("SCHERLOK_CONNECTION", connection)
    return runner.invoke(app, list(args))


def _verify_json(monkeypatch, connection: str, *args: str):
    result = _invoke(monkeypatch, connection, "verify", "--output", "json", *args)
    payload = json.loads(result.stdout) if result.stdout else None
    return result, payload


@pytest.fixture()
def production(tmp_path, monkeypatch) -> str:
    """Production's connection, after an investigate and LEARNING_RUNS watches."""
    path = tmp_path / "prod.duckdb"
    _execute(path, ORDERS_DDL)
    _insert_orders(path, 1, ROWS)
    connection = f"duckdb:///{path}"
    assert _invoke(monkeypatch, connection, "investigate").exit_code == 0
    for run in range(LEARNING_RUNS):
        first = ROWS + run * APPENDED_ROWS + 1
        _insert_orders(path, first, first + APPENDED_ROWS - 1)
        watch = _invoke(monkeypatch, connection, "watch")
        assert watch.exit_code == 0, watch.output
    return connection


def _staging(tmp_path: Path, where: str = SAMPLE_FILTER) -> tuple[Path, str]:
    """A staging build of production's orders, by default on a sample of the rows."""
    path = tmp_path / "staging.duckdb"
    _execute(path, ORDERS_DDL)
    _insert_orders(path, 1, PRODUCTION_ROWS, where)
    return path, f"duckdb:///{path}"


def _unwrapped(text: str) -> str:
    """Output without whitespace, so a scope Rich wrapped mid-word still matches."""
    return "".join(text.split())


def _store_rows(home: Path) -> dict[str, list[tuple]]:
    conn = sqlite3.connect(home / STORE_FILENAME)
    try:
        return {
            table: conn.execute(f"SELECT * FROM {table} ORDER BY id").fetchall()
            for table in ("profiles", "anomalies")
        }
    finally:
        conn.close()


def _runs(home: Path) -> list[dict]:
    return ProfileStore(db_path=home / STORE_FILENAME).get_runs()


# ----- acceptance: a dropped column, nothing written ---------------------------

def test_against_flags_a_dropped_column_and_saves_nothing_under_either_database(
    production, tmp_path, monkeypatch, isolated_scherlok_dir
):
    staging_path, staging = _staging(tmp_path)
    # Staging has a baseline of its own, which --against must neither read nor move.
    assert _invoke(monkeypatch, staging, "investigate").exit_code == 0
    _execute(staging_path, "ALTER TABLE orders DROP COLUMN email")
    before = _store_rows(isolated_scherlok_dir)
    record_path = tmp_path / "runs.jsonl"

    sampled, sampled_payload = _verify_json(
        monkeypatch, staging, "--against", production, "--ignore", CHECK_VOLUME,
        "--record", str(record_path),
    )
    plain, plain_payload = _verify_json(monkeypatch, staging, "--against", production)

    assert sampled.exit_code == 1
    assert sampled_payload["passed"] is False
    assert [(a["type"], a["column"], a["severity"]) for a in sampled_payload["anomalies"]] == [
        (COLUMN_REMOVED, "email", Severity.CRITICAL.value)
    ]
    assert sampled_payload["against"] == connection_scope(production)
    assert sampled_payload["ignored"] == [CHECK_VOLUME]
    # Without --ignore volume the row count is compared with production's
    # learned baseline, not with staging's own sampled one.
    assert plain.exit_code == 1
    [drop] = [a for a in plain_payload["anomalies"] if a["type"] == VOLUME_DROP]
    assert drop["baseline"] > ROWS
    assert COLUMN_REMOVED in {a["type"] for a in plain_payload["anomalies"]}
    assert _store_rows(isolated_scherlok_dir) == before
    verify_runs = [r for r in _runs(isolated_scherlok_dir) if r["command"] == COMMAND_VERIFY]
    assert [(r["scope"], r["against"], r["ignored"]) for r in verify_runs] == [
        (connection_scope(staging), connection_scope(production), []),
        (connection_scope(staging), connection_scope(production), [CHECK_VOLUME]),
    ]
    [record] = [json.loads(line) for line in record_path.read_text().splitlines()]
    assert tuple(record) == RUN_COLUMNS
    assert record == verify_runs[1]


def test_text_mode_names_the_database_compared_with(production, tmp_path, monkeypatch):
    staging_path, staging = _staging(tmp_path)
    _execute(staging_path, "ALTER TABLE orders DROP COLUMN email")

    result = _invoke(monkeypatch, staging, "verify", "--against", production, "--ignore", "volume")

    text = _unwrapped(result.stdout)
    assert result.exit_code == 1
    assert "column_removed" in text
    assert f"Verifyfailed:1verifiedagainst{connection_scope(production)}," in text
    assert "--ignorevolume" in text


# ----- acceptance: a correct build on sampled data -----------------------------

def test_a_correct_build_on_sampled_data_passes_with_ignore_volume(
    production, tmp_path, monkeypatch
):
    _staging_path, staging = _staging(tmp_path)

    sampled, payload = _verify_json(
        monkeypatch, staging, "--against", production, "--ignore", CHECK_VOLUME,
        "--fail-on", "warning",
    )
    unsampled, unsampled_payload = _verify_json(monkeypatch, staging, "--against", production)

    assert sampled.exit_code == 0, sampled.output
    assert payload["passed"] is True
    assert payload["tables_verified"] == ["orders"]
    assert payload["anomalies"] == []
    # Control: the same build fails without --ignore volume, compared with
    # the baseline production learned over its runs.
    assert unsampled.exit_code == 1
    types = {a["type"] for a in unsampled_payload["anomalies"]}
    assert {VOLUME_DROP, CARDINALITY_CHANGE} <= types
    [drop] = [a for a in unsampled_payload["anomalies"] if a["type"] == VOLUME_DROP]
    assert "learned baseline" in drop["message"]


def test_a_new_table_in_the_build_gets_the_new_table_checks(production, tmp_path, monkeypatch):
    staging_path, staging = _staging(tmp_path)
    _execute(staging_path, "CREATE TABLE refunds (id INTEGER)")

    unnamed, unnamed_payload = _verify_json(
        monkeypatch, staging, "--against", production, "--ignore", CHECK_VOLUME
    )
    named, named_payload = _verify_json(
        monkeypatch, staging, "--against", production, "--ignore", CHECK_VOLUME,
        "--table", "refunds",
    )

    assert unnamed.exit_code == 0
    assert unnamed_payload["tables_without_baseline"] == ["refunds"]
    assert [(a["type"], a["severity"]) for a in unnamed_payload["anomalies"]] == [
        ("empty_table", Severity.WARNING.value)
    ]
    assert named.exit_code == 1
    assert named_payload["anomalies"][0]["severity"] == Severity.CRITICAL.value


# ----- which records count as the other database's -----------------------------

def test_records_saved_before_scopes_never_stand_for_the_other_database(
    production, tmp_path, monkeypatch, isolated_scherlok_dir
):
    """An unscoped record may be the verified database's own: --against skips them."""
    from scherlok.connectors.duckdb import DuckDBConnector

    staging_path, staging = _staging(tmp_path)
    _execute(
        staging_path,
        "CREATE TABLE refunds (id INTEGER, amount DOUBLE)",
        "INSERT INTO refunds VALUES (1, 9.5)",
    )
    connector = DuckDBConnector(staging)
    assert connector.connect()
    legacy = ProfileStore(db_path=isolated_scherlok_dir / STORE_FILENAME)  # no scope
    profile_and_detect(connector, legacy, "refunds")
    legacy.close()
    connector.close()
    _execute(staging_path, "ALTER TABLE refunds DROP COLUMN amount")

    own, own_payload = _verify_json(monkeypatch, staging, "--table", "refunds")
    against, against_payload = _verify_json(
        monkeypatch, staging, "--table", "refunds", "--against", production
    )

    # The default comparison counts the unscoped record, as it always has.
    assert own.exit_code == 1
    assert own_payload["anomalies"][0]["type"] == COLUMN_REMOVED
    # Against production, refunds has no baseline: the record isn't production's.
    assert against.exit_code == 0
    assert against_payload["tables_without_baseline"] == ["refunds"]
    assert against_payload["anomalies"] == []


# ----- refusals ----------------------------------------------------------------

def test_a_scope_without_any_baseline_fails_instead_of_verifying_nothing(
    production, tmp_path, monkeypatch, isolated_scherlok_dir
):
    _staging_path, staging = _staging(tmp_path)
    runs_before = len(_runs(isolated_scherlok_dir))

    result, payload = _verify_json(
        monkeypatch, staging, "--against", "duckdb:///elsewhere/prod.duckdb"
    )

    assert result.exit_code == 1
    assert payload is None  # stdout stays empty
    assert "Nobaselineforduckdb:///elsewhere/prod.duckdb" in _unwrapped(result.stderr)
    assert len(_runs(isolated_scherlok_dir)) == runs_before


def test_against_the_database_being_verified_is_refused(tmp_path, monkeypatch):
    connection = f"duckdb:///{tmp_path / 'prod.duckdb'}"

    result = _invoke(monkeypatch, connection, "verify", "--against", connection)

    assert result.exit_code == 1
    assert "isthedatabasebeingverified" in _unwrapped(result.output)


def test_an_against_that_names_no_database_is_refused_without_repeating_it(
    monkeypatch, isolated_scherlok_dir
):
    # An unencoded '/' in the password cuts the host short: no scope can be trusted.
    unreadable = f"postgresql://alice:pa/{SECRET}@db.example.com/shop"

    result = _invoke(monkeypatch, "duckdb:///:memory:", "verify", "--against", unreadable)

    assert result.exit_code == 1
    assert "Invalid --against value" in result.output
    assert SECRET not in result.output
    assert not (isolated_scherlok_dir / STORE_FILENAME).exists()  # refused before any work


def test_an_unknown_check_to_ignore_is_refused_before_any_work(monkeypatch, isolated_scherlok_dir):
    result = _invoke(monkeypatch, "duckdb:///:memory:", "verify", "--ignore", "rows")

    assert result.exit_code == 1
    assert "Invalid --ignore value" in result.output and "'volume'" in result.output
    assert not (isolated_scherlok_dir / STORE_FILENAME).exists()


def test_a_connection_string_is_reduced_to_its_scope_and_never_kept(
    production, tmp_path, monkeypatch, isolated_scherlok_dir
):
    _staging_path, staging = _staging(tmp_path)
    record_path = tmp_path / "runs.jsonl"
    # DuckDB ignores the query string; the token stands for any credential.
    with_token = f"{production}?motherduck_token={SECRET}"

    result, payload = _verify_json(
        monkeypatch, staging, "--against", with_token, "--ignore", "VOLUME",
        "--record", str(record_path),
    )

    assert result.exit_code == 0, result.output
    assert payload["against"] == connection_scope(production)
    assert payload["ignored"] == [CHECK_VOLUME]
    for text in (
        result.stdout,
        result.stderr,
        record_path.read_text(),
        (isolated_scherlok_dir / STORE_FILENAME).read_bytes().decode("utf-8", "replace"),
    ):
        assert SECRET not in text


def test_help_lists_against_and_ignore():
    import re

    own = re.sub(r"\x1b\[[0-9;]*m", "", runner.invoke(app, ["verify", "--help"]).output)

    assert "--against" in own and "--ignore" in own


# ----- the checks left out, at the service level ---------------------------------

class FakeConnector:
    """One `orders` table whose row count and last update the test sets."""

    METADATA_IS_LAST_WRITE = True  # BaseConnector's default

    connection_string = "duckdb:///staging.duckdb"

    def __init__(self, row_count: int = 100, hours_since_update: float = 2.0) -> None:
        self.row_count = row_count
        self.last_modified = datetime.now(timezone.utc) - timedelta(hours=hours_since_update)

    def list_tables(self) -> list[str]:
        return ["orders"]

    def get_row_count(self, table: str) -> int:
        return self.row_count

    def get_columns(self, table: str) -> list[dict]:
        return [{"name": "id", "type": "INTEGER", "nullable": False}]

    def get_column_stats(self, table: str, column: str) -> dict:
        return {"mean": 50.0, "stddev": 29.0, "null_count": 0, "distinct_count": self.row_count}

    def get_last_modified(self, table: str) -> datetime:
        return self.last_modified


PRODUCTION_SCOPE = "duckdb:///prod.duckdb"


@pytest.fixture()
def production_store(tmp_path):
    """A production baseline for `orders`, updated two hours before it was profiled."""
    path = tmp_path / STORE_FILENAME
    learning = ProfileStore(db_path=path, scope=PRODUCTION_SCOPE)
    profile_and_detect(FakeConnector(), learning, "orders")
    learning.close()
    store = ProfileStore(db_path=path, scope=PRODUCTION_SCOPE, include_unscoped=False)
    yield store
    store.close()


def test_freshness_is_not_compared_against_another_database(production_store):
    stale_copy = FakeConnector(hours_since_update=20.0)

    own_clock, _ = inspect_table(stale_copy, production_store, "orders")
    against = verify_tables(stale_copy, production_store, against=PRODUCTION_SCOPE)
    skipped, _ = inspect_table(stale_copy, production_store, "orders", skip={CHECK_FRESHNESS})

    assert [a["type"] for a in own_clock] == [FRESHNESS_CRITICAL]  # control
    assert against.anomalies == [] and against.passed
    assert skipped == []


def test_an_empty_table_is_a_warning_when_volume_is_ignored(production_store):
    empty = FakeConnector(row_count=0)

    compared, _ = inspect_table(empty, production_store, "orders")
    sampled, _ = inspect_table(empty, production_store, "orders", skip={CHECK_VOLUME})

    assert {a["type"]: a["severity"] for a in compared}[TABLE_EMPTY] == Severity.CRITICAL
    assert [(a["type"], a["severity"]) for a in sampled] == [(TABLE_EMPTY, Severity.WARNING)]


def test_verify_tables_refuses_a_scope_the_store_has_no_baseline_for(tmp_path):
    nowhere = "duckdb:///nowhere.duckdb"
    store = ProfileStore(db_path=tmp_path / STORE_FILENAME, scope=nowhere, include_unscoped=False)

    with pytest.raises(AgainstScopeError, match="No baseline"):
        verify_tables(FakeConnector(), store, against=nowhere)
    with pytest.raises(AgainstScopeError, match="being verified"):
        verify_tables(FakeConnector(), store, against=FakeConnector.connection_string)
    with pytest.raises(ValueError, match="Unknown check to ignore"):
        verify_tables(FakeConnector(), store, ignore=["freshness"])
