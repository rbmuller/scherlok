"""Tests for run records (#115): every run leaves one row, passing or not."""

from __future__ import annotations

import importlib.util
import json
import shutil
import sqlite3
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
from typer.testing import CliRunner

from scherlok.cli import app
from scherlok.detector.severity import Severity
from scherlok.runs import COMMAND_VERIFY, COMMAND_WATCH, RunRecord, append_run_record
from scherlok.service import connection_scope
from scherlok.store.sqlite import RUN_COLUMNS, ProfileStore

_HAS_DUCKDB = importlib.util.find_spec("duckdb") is not None
needs_duckdb = pytest.mark.skipif(not _HAS_DUCKDB, reason="duckdb not installed")

runner = CliRunner(env={"NO_COLOR": "1"})

STORE_FILENAME = "profiles.db"
SECRET = "S3CRET-t0ken"
SENTINEL_EMAIL = "sentinel.person@example.invalid"
SENTINEL_AMOUNT = 868686.375
BASELINE_ROWS = 1000
SCOPE = "postgresql://db.example.com/shop"
REMOTE_STORE = "s3://example-bucket/scherlok/profiles.db"


def _anomaly(severity: Severity, message: str = "Row count dropped") -> dict:
    return {"table": "orders", "type": "volume_drop", "severity": severity, "message": message}


# ----- RunRecord and the store ----------------------------------------------

def test_a_finished_record_round_trips_through_the_store(tmp_path):
    store = ProfileStore(db_path=tmp_path / STORE_FILENAME, scope=SCOPE)
    run = RunRecord(command=COMMAND_WATCH, scope=store.scope, fail_on="warning")

    record = run.finish(["orders", "users"], [
        _anomaly(Severity.WARNING), _anomaly(Severity.INFO), _anomaly(Severity.INFO),
    ]).to_dict()
    store.save_run(record)

    assert tuple(record) == RUN_COLUMNS
    assert record["passed"] is False  # a WARNING fails --fail-on warning
    assert (record["critical"], record["warning"], record["info"]) == (0, 1, 2)
    assert record["tables"] == ["orders", "users"]
    assert record["started_at"] <= record["finished_at"]
    assert store.get_runs() == [record]


def test_the_store_refuses_a_record_that_never_finished(tmp_path):
    store = ProfileStore(db_path=tmp_path / STORE_FILENAME)

    with pytest.raises(sqlite3.IntegrityError):
        store.save_run(RunRecord(command=COMMAND_WATCH, scope=None).to_dict())


def test_runs_are_kept_per_connection(tmp_path):
    path = tmp_path / STORE_FILENAME
    for scope in (SCOPE, "postgresql://staging.example.com/shop"):
        store = ProfileStore(db_path=path, scope=scope)
        store.save_run(RunRecord(command=COMMAND_WATCH, scope=scope).finish([], []).to_dict())

    assert [r["scope"] for r in ProfileStore(db_path=path, scope=SCOPE).get_runs()] == [SCOPE]
    assert len(ProfileStore(db_path=path).get_runs()) == 2


def test_anomalies_point_to_their_run(tmp_path):
    store = ProfileStore(db_path=tmp_path / STORE_FILENAME)
    store.save_anomalies([_anomaly(Severity.CRITICAL)], run_id="abc123")
    store.save_anomalies([_anomaly(Severity.WARNING)])

    assert {a["run_id"] for a in store.get_anomaly_history()} == {"abc123", None}


def test_a_record_holds_names_and_counts_only():
    connection = f"postgresql://alice:{SECRET}@db.example.com:5432/shop?sslkey={SECRET}"
    run = RunRecord(command=COMMAND_WATCH, scope=connection_scope(connection))

    record = run.finish(["orders"], [_anomaly(Severity.CRITICAL, f"{SENTINEL_EMAIL} vanished")])
    text = json.dumps(record.to_dict())

    assert SECRET not in text and "alice" not in text
    assert SENTINEL_EMAIL not in text
    assert record.scope == "postgresql://db.example.com/shop"


def test_append_run_record_writes_one_json_line_per_call(tmp_path):
    path = tmp_path / "runs.jsonl"
    first = RunRecord(command=COMMAND_VERIFY, scope=SCOPE).finish([], []).to_dict()
    second = RunRecord(command=COMMAND_VERIFY, scope=SCOPE).finish(["orders"], []).to_dict()

    append_run_record(path, first)
    append_run_record(path, second)

    assert [json.loads(line) for line in path.read_text().splitlines()] == [first, second]


# ----- end to end on DuckDB ---------------------------------------------------

def _execute(path: Path, *statements: str) -> None:
    import duckdb

    conn = duckdb.connect(str(path))
    try:
        for sql in statements:
            conn.execute(sql)
    finally:
        conn.close()


@pytest.fixture()
def warehouse(tmp_path, monkeypatch) -> Path:
    """A DuckDB file whose rows hold sentinel values, reached through a URL with a token."""
    path = tmp_path / "warehouse.duckdb"
    _execute(
        path,
        "CREATE TABLE orders (id INTEGER, amount DOUBLE, email VARCHAR)",
        f"INSERT INTO orders SELECT i, {SENTINEL_AMOUNT}, '{SENTINEL_EMAIL}' "
        f"FROM range(1, {BASELINE_ROWS + 1}) t(i)",
    )
    # The DuckDB connector ignores the query string, so the token is inert here
    # but still part of the connection string a record must never hold.
    monkeypatch.setenv("SCHERLOK_CONNECTION", f"duckdb:///{path}?motherduck_token={SECRET}")
    return path


def _keep_rows(path: Path, rows: int) -> None:
    _execute(path, f"DELETE FROM orders WHERE id > {rows}")


def _rows(home: Path, sql: str) -> list[tuple]:
    conn = sqlite3.connect(home / STORE_FILENAME)
    try:
        return conn.execute(sql).fetchall()
    finally:
        conn.close()


def _runs(home: Path) -> list[dict]:
    return ProfileStore(db_path=home / STORE_FILENAME).get_runs()


def _invoke(*args: str):
    result = runner.invoke(app, list(args))
    assert result.exception is None or isinstance(result.exception, SystemExit), result.output
    return result


@needs_duckdb
def test_a_run_with_no_anomalies_writes_one_row(warehouse, isolated_scherlok_dir):
    _invoke("investigate")  # not a recorded command

    result = _invoke("watch")

    assert result.exit_code == 0
    [run] = _runs(isolated_scherlok_dir)
    assert run["command"] == COMMAND_WATCH
    assert run["passed"] is True
    assert run["tables"] == ["orders"]
    assert (run["critical"], run["warning"], run["info"]) == (0, 0, 0)
    assert run["fail_on"] == "critical"
    assert run["scope"] == connection_scope(f"duckdb:///{warehouse}")


@needs_duckdb
def test_anomalies_saved_by_a_run_carry_its_run_id(warehouse, isolated_scherlok_dir):
    _invoke("investigate")
    _keep_rows(warehouse, 300)

    result = _invoke("watch")

    assert result.exit_code == 1
    [run] = _runs(isolated_scherlok_dir)
    assert run["passed"] is False and run["critical"] >= 1
    anomaly_run_ids = _rows(isolated_scherlok_dir, "SELECT run_id FROM anomalies")
    assert anomaly_run_ids and {row[0] for row in anomaly_run_ids} == {run["run_id"]}


@needs_duckdb
def test_ci_and_check_record_their_own_command(warehouse, isolated_scherlok_dir, monkeypatch):
    connection = f"duckdb:///{warehouse}"
    monkeypatch.delenv("SCHERLOK_CONNECTION")

    _invoke("ci", connection)
    _invoke("check", connection, "--fail-on", "warning")

    runs = _runs(isolated_scherlok_dir)
    assert [(r["command"], r["fail_on"]) for r in runs] == [
        ("check", "warning"), ("ci", "critical"),
    ]


@needs_duckdb
def test_verify_adds_a_run_but_leaves_profiles_and_anomalies_unchanged(
    warehouse, isolated_scherlok_dir
):
    _invoke("investigate")
    _keep_rows(warehouse, 700)
    _invoke("watch")  # a WARNING saved; the baseline learns 700 rows
    _keep_rows(warehouse, 300)
    before = {
        table: _rows(isolated_scherlok_dir, f"SELECT * FROM {table} ORDER BY id")
        for table in ("profiles", "anomalies")
    }

    result = _invoke("verify", "--output", "json")

    after = {
        table: _rows(isolated_scherlok_dir, f"SELECT * FROM {table} ORDER BY id")
        for table in ("profiles", "anomalies")
    }
    assert result.exit_code == 1
    assert json.loads(result.stdout)["critical"] == 1  # found, but not saved
    assert before["anomalies"] and after == before
    runs = _runs(isolated_scherlok_dir)
    assert [r["command"] for r in runs] == [COMMAND_VERIFY, COMMAND_WATCH]
    assert runs[0]["passed"] is False and runs[0]["critical"] == 1


@needs_duckdb
def test_verify_record_appends_one_valid_json_line_per_run(
    warehouse, isolated_scherlok_dir, tmp_path
):
    record_path = tmp_path / "scherlok-runs.jsonl"
    _invoke("investigate")
    _invoke("verify", "--record", str(record_path))
    _keep_rows(warehouse, 300)

    failed = _invoke("verify", "--output", "json", "--record", str(record_path))

    assert failed.exit_code == 1
    assert "passed" in json.loads(failed.stdout)  # stdout is still the payload alone
    lines = record_path.read_text().splitlines()
    records = [json.loads(line) for line in lines]
    assert len(records) == 2
    assert [r["passed"] for r in records] == [True, False]
    assert all(tuple(r) == RUN_COLUMNS for r in records)
    assert list(reversed(_runs(isolated_scherlok_dir))) == records


@needs_duckdb
def test_verify_record_keeps_the_run_when_a_remote_store_is_never_uploaded(
    warehouse, isolated_scherlok_dir, tmp_path, monkeypatch
):
    _invoke("investigate")
    remote_copy = tmp_path / "remote-profiles.db"
    shutil.move(isolated_scherlok_dir / STORE_FILENAME, remote_copy)
    download = MagicMock(side_effect=lambda _url, local: shutil.copy(remote_copy, local))
    upload = MagicMock()
    monkeypatch.setenv("SCHERLOK_STORE", REMOTE_STORE)
    record_path = tmp_path / "runs.jsonl"

    with (
        patch("scherlok.store.remote.download", download),
        patch("scherlok.store.remote.upload", upload),
    ):
        result = _invoke("verify", "--record", str(record_path))

    assert result.exit_code == 0
    upload.assert_not_called()
    [record] = [json.loads(line) for line in record_path.read_text().splitlines()]
    assert record["command"] == COMMAND_VERIFY


@needs_duckdb
def test_verify_exits_1_when_the_record_cannot_be_written(warehouse, tmp_path):
    _invoke("investigate")

    result = _invoke(
        "verify", "--output", "json", "--record", str(tmp_path / "missing-dir" / "runs.jsonl")
    )

    assert result.exit_code == 1
    assert result.stdout == ""
    assert "Could not write the run record" in result.stderr


@needs_duckdb
def test_no_connection_string_or_raw_value_reaches_a_run_record(
    warehouse, isolated_scherlok_dir, tmp_path
):
    connection = f"duckdb:///{warehouse}?motherduck_token={SECRET}"
    record_path = tmp_path / "runs.jsonl"
    _invoke("investigate")
    _execute(warehouse, "UPDATE orders SET email = NULL WHERE id > 100")  # NULL surge
    _keep_rows(warehouse, 300)
    _invoke("watch")
    _invoke("verify", "--record", str(record_path))

    stored = _rows(isolated_scherlok_dir, "SELECT * FROM runs")
    texts = [json.dumps(stored), record_path.read_text(), json.dumps(_runs(isolated_scherlok_dir))]

    assert len(stored) == 2
    for text in texts:
        assert SECRET not in text
        assert connection not in text
        assert SENTINEL_EMAIL not in text
        assert str(SENTINEL_AMOUNT) not in text


@needs_duckdb
def test_history_runs_in_text_and_json(warehouse, isolated_scherlok_dir):
    _invoke("investigate")
    _invoke("watch")
    _keep_rows(warehouse, 300)
    _invoke("verify")

    as_json = _invoke("history", "--runs", "--output", "json")
    as_text = _invoke("history", "--runs")

    assert as_json.exit_code == 0 and as_text.exit_code == 0
    runs = json.loads(as_json.stdout)
    assert [(r["command"], r["passed"]) for r in runs] == [
        (COMMAND_VERIFY, False), (COMMAND_WATCH, True),
    ]
    assert all(tuple(r) == RUN_COLUMNS for r in runs)
    text = as_text.stdout
    assert "Runs (last 30 days)" in text
    verify_line = next(line for line in text.splitlines() if COMMAND_VERIFY in line)
    watch_line = next(line for line in text.splitlines() if COMMAND_WATCH in line)
    assert "failed" in verify_line and "critical" in verify_line
    assert "passed" in watch_line
    assert text.index(COMMAND_VERIFY) < text.index(COMMAND_WATCH)  # newest first


def test_history_runs_without_any_run(isolated_scherlok_dir):
    as_json = _invoke("history", "--runs", "--output", "json")
    as_text = _invoke("history", "--runs", "--days", "7")

    assert json.loads(as_json.stdout) == []
    assert "No runs in the last 7 days" in as_text.stdout
