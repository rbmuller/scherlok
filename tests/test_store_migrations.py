"""Tests for ProfileStore migrations (PRAGMA user_version)."""

from __future__ import annotations

import json
import shutil
import sqlite3
from pathlib import Path

import pytest

from scherlok.approvals import ApprovalRecord
from scherlok.detector.anomaly import detect_volume_anomalies
from scherlok.detector.fields import DETAIL_FIELDS, anomaly_details
from scherlok.runs import COMMAND_WATCH, RunRecord
from scherlok.store import remote as store_remote
from scherlok.store import sqlite as store_sqlite
from scherlok.store.sqlite import (
    ANOMALY_DETAILS_COLUMN,
    APPEND_ONLY_ERROR,
    APPROVALS_TABLE,
    CREATE_ANOMALIES_INDEX_SQL,
    CREATE_ANOMALIES_SQL,
    CREATE_INDEX_SQL,
    CREATE_TABLE_SQL,
    MIGRATIONS,
    RAW_VALUE_KEYS,
    RUN_AGAINST_KEY,
    RUN_COLUMNS,
    RUN_ID_COLUMN,
    RUN_IGNORED_KEY,
    RUN_LIST_COLUMNS,
    SCOPE_COLUMN,
    SCOPED_TABLES,
    ProfileStore,
    _add_anomaly_details_column,
    _add_baseline_approvals,
    _add_run_comparison_columns,
    _add_runs,
    _add_scope_columns,
    _strip_raw_distribution_values,
)

LATEST_VERSION = len(MIGRATIONS)
# What the release with only the #114 migration leaves behind.
SCOPELESS_VERSION = MIGRATIONS.index(_add_scope_columns)
STAGING_SCOPE = "postgresql://staging.example.com/shop"
PRODUCTION_SCOPE = "postgresql://prod.example.com/shop"
RUNS_TABLE = "runs"
# The run columns of the releases before #110, all an older runner inserts.
OLD_RUN_COLUMNS = RUN_COLUMNS[: RUN_COLUMNS.index(RUN_AGAINST_KEY)]
# A store from before anomalies gained a details column (#108): the release
# with only the #114 migration, so every later migration runs on open.
BEFORE_DETAILS_VERSION = SCOPELESS_VERSION
# Wide enough that the fixed CREATED_AT stays inside the history window.
ALL_HISTORY_DAYS = 36500
STORE_FILENAME = "profiles.db"
CREATED_AT = "2026-10-01T06:00:00+00:00"
SENTINEL_TOP_VALUE = "sentinel.person@example.invalid"
SENTINEL_MIN = "424242.125"
SENTINEL_MAX = "868686.375"
SENTINELS = (SENTINEL_TOP_VALUE, SENTINEL_MIN, SENTINEL_MAX)

AGGREGATES = {
    "mean": 548821.4167,
    "stddev": 151234.5,
    "null_count": 1,
    "null_rate": 0.1667,
    "distinct_count": 4,
}
# A distribution profile as releases before #114 saved it.
OLD_NUMERIC_PROFILE = {
    **AGGREGATES,
    "min": SENTINEL_MIN,
    "max": SENTINEL_MAX,
    "top_values": [{"value": "500000.0", "count": 3}],
}
OLD_TEXT_PROFILE = {
    "mean": None,
    "stddev": None,
    "min": None,
    "max": None,
    "null_count": 0,
    "null_rate": 0.0,
    "distinct_count": 3,
    "top_values": [{"value": SENTINEL_TOP_VALUE, "count": 3}],
}
VOLUME_PROFILE = {"row_count": 6, "timestamp": CREATED_AT}
SCHEMA_PROFILE = {"columns": [{"name": "email", "type": "VARCHAR", "nullable": True}]}
OLD_ROWS = (
    ("people", "volume", VOLUME_PROFILE),
    ("people", "schema", SCHEMA_PROFILE),
    ("people", "distribution:salary", OLD_NUMERIC_PROFILE),
    ("people", "distribution:email", OLD_TEXT_PROFILE),
    ("people", "distribution:id", AGGREGATES),
)
ANOMALY = ("people", "volume_drop", "CRITICAL", "Row count dropped 50.0% (6 -> 3)", CREATED_AT)


def _create_old_store(path: Path, user_version: int = 0, runs: int = 1) -> None:
    """Write a store the way an older release left it after `runs` profiling runs."""
    conn = sqlite3.connect(str(path))
    for sql in (
        CREATE_TABLE_SQL, CREATE_INDEX_SQL, CREATE_ANOMALIES_SQL, CREATE_ANOMALIES_INDEX_SQL
    ):
        conn.execute(sql)
    conn.executemany(
        "INSERT INTO profiles (table_name, profile_type, data, created_at) VALUES (?, ?, ?, ?)",
        [(table, kind, json.dumps(data), CREATED_AT) for table, kind, data in OLD_ROWS] * runs,
    )
    conn.execute(
        "INSERT INTO anomalies (table_name, anomaly_type, severity, message, detected_at) "
        "VALUES (?, ?, ?, ?, ?)",
        ANOMALY,
    )
    conn.execute(f"PRAGMA user_version = {user_version}")
    conn.commit()
    conn.close()


def _open_and_close(path: Path) -> None:
    ProfileStore(db_path=path).close()


def _user_version(path: Path) -> int:
    conn = sqlite3.connect(str(path))
    try:
        return conn.execute("PRAGMA user_version").fetchone()[0]
    finally:
        conn.close()


def _profile_rows(path: Path) -> list[tuple[int, str, str]]:
    conn = sqlite3.connect(str(path))
    try:
        return conn.execute(
            "SELECT id, profile_type, created_at FROM profiles ORDER BY id"
        ).fetchall()
    finally:
        conn.close()


def _profiles(path: Path) -> dict[str, dict]:
    conn = sqlite3.connect(str(path))
    try:
        rows = conn.execute("SELECT profile_type, data FROM profiles ORDER BY id").fetchall()
    finally:
        conn.close()
    return {kind: json.loads(data) for kind, data in rows}


def _anomaly_columns(path: Path) -> list[str]:
    conn = sqlite3.connect(str(path))
    try:
        return [row[1] for row in conn.execute("PRAGMA table_info(anomalies)")]
    finally:
        conn.close()


def _anomalies(path: Path) -> list[tuple]:
    conn = sqlite3.connect(str(path))
    try:
        return conn.execute(
            "SELECT table_name, anomaly_type, severity, message, detected_at FROM anomalies"
        ).fetchall()
    finally:
        conn.close()


def test_new_store_starts_at_latest_version(tmp_path):
    path = tmp_path / STORE_FILENAME

    _open_and_close(path)

    assert _user_version(path) == LATEST_VERSION


def test_migration_strips_column_values_and_keeps_everything_else(tmp_path):
    path = tmp_path / STORE_FILENAME
    _create_old_store(path)
    rows_before = _profile_rows(path)

    _open_and_close(path)

    assert _profile_rows(path) == rows_before
    profiles = _profiles(path)
    assert profiles["distribution:salary"] == AGGREGATES
    assert profiles["distribution:email"] == {
        key: value for key, value in OLD_TEXT_PROFILE.items() if key not in RAW_VALUE_KEYS
    }
    assert profiles["distribution:id"] == AGGREGATES
    assert profiles["volume"] == VOLUME_PROFILE
    assert profiles["schema"] == SCHEMA_PROFILE
    assert _anomalies(path) == [ANOMALY]
    assert _user_version(path) == LATEST_VERSION


def test_migration_leaves_no_trace_of_values_in_the_file(tmp_path):
    # Enough history to span several pages: rewritten rows then leave their
    # old bytes in free space, which only the VACUUM clears.
    path = tmp_path / STORE_FILENAME
    _create_old_store(path, runs=30)
    assert all(sentinel.encode() in path.read_bytes() for sentinel in SENTINELS)

    _open_and_close(path)

    stored = path.read_bytes()
    for sentinel in SENTINELS:
        assert sentinel.encode() not in stored, f"{sentinel!r} lingers in the file"


def test_remote_store_is_uploaded_clean_on_next_sync(tmp_path, monkeypatch):
    bucket_copy = tmp_path / "bucket" / STORE_FILENAME
    bucket_copy.parent.mkdir()
    _create_old_store(bucket_copy, runs=30)
    uploads: list[bytes] = []

    def fake_download(remote_url: str, local_path: Path) -> bool:
        shutil.copyfile(bucket_copy, local_path)
        return True

    def fake_upload(local_path: Path, remote_url: str) -> None:
        uploads.append(local_path.read_bytes())

    monkeypatch.setattr(store_remote, "download", fake_download)
    monkeypatch.setattr(store_remote, "upload", fake_upload)
    local = tmp_path / "home" / STORE_FILENAME
    local.parent.mkdir()

    # The sequence `investigate` and `watch` run against a remote store.
    with store_remote.sync_context("s3://bucket/profiles.db", local):
        ProfileStore(db_path=local)

    assert len(uploads) == 1
    for sentinel in SENTINELS:
        assert sentinel.encode() not in uploads[0], f"{sentinel!r} uploaded"


def test_migration_is_safe_to_run_twice(tmp_path):
    path = tmp_path / STORE_FILENAME
    _create_old_store(path)
    _open_and_close(path)
    migrated = _profiles(path)

    conn = sqlite3.connect(str(path))
    conn.row_factory = sqlite3.Row
    _strip_raw_distribution_values(conn)
    conn.commit()
    conn.execute("PRAGMA user_version = 0")
    conn.close()
    _open_and_close(path)

    assert _profiles(path) == migrated
    assert _user_version(path) == LATEST_VERSION


def test_store_from_a_newer_release_is_left_alone(tmp_path):
    # A newer release has every column this one knows, plus some it doesn't,
    # and data this release's migrations would rewrite.
    path = tmp_path / STORE_FILENAME
    newer_version = LATEST_VERSION + 5
    _open_and_close(path)
    conn = sqlite3.connect(str(path))
    conn.execute(
        "INSERT INTO profiles (table_name, profile_type, data, created_at) VALUES (?, ?, ?, ?)",
        ("people", "distribution:salary", json.dumps(OLD_NUMERIC_PROFILE), CREATED_AT),
    )
    conn.execute(f"PRAGMA user_version = {newer_version}")
    conn.commit()
    conn.close()

    store = ProfileStore(db_path=path)
    store.save_profile("people", "volume", VOLUME_PROFILE)
    store.close()

    assert _user_version(path) == newer_version
    assert _profiles(path)["distribution:salary"] == OLD_NUMERIC_PROFILE


def test_failing_migration_rolls_back_and_keeps_the_version(tmp_path, monkeypatch):
    def broken_migration(conn: sqlite3.Connection) -> None:
        raise sqlite3.OperationalError("boom")

    path = tmp_path / STORE_FILENAME
    _create_old_store(path)
    monkeypatch.setattr(store_sqlite, "MIGRATIONS", (*MIGRATIONS, broken_migration))

    with pytest.raises(sqlite3.OperationalError, match="boom"):
        ProfileStore(db_path=path)

    assert _user_version(path) == 0
    assert _profiles(path)["distribution:salary"] == OLD_NUMERIC_PROFILE
    assert ANOMALY_DETAILS_COLUMN not in _anomaly_columns(path)


# ----- scope columns (#110) --------------------------------------------------

def _columns(path: Path, table: str) -> list[str]:
    conn = sqlite3.connect(str(path))
    try:
        return [row[1] for row in conn.execute(f"PRAGMA table_info({table})")]
    finally:
        conn.close()


def _scopes(path: Path, table: str) -> list[str | None]:
    conn = sqlite3.connect(str(path))
    try:
        return [row[0] for row in conn.execute(f"SELECT {SCOPE_COLUMN} FROM {table}")]
    finally:
        conn.close()


def test_store_from_the_previous_release_gains_scope_and_keeps_its_rows(tmp_path):
    path = tmp_path / STORE_FILENAME
    _create_old_store(path, user_version=SCOPELESS_VERSION)
    rows_before = _profile_rows(path)
    profiles_before = _profiles(path)

    _open_and_close(path)

    assert _user_version(path) == LATEST_VERSION
    for table in SCOPED_TABLES:
        assert SCOPE_COLUMN in _columns(path, table)
        assert set(_scopes(path, table)) == {None}
    assert _profile_rows(path) == rows_before
    assert _profiles(path) == profiles_before  # only the pending migration ran
    assert _anomalies(path) == [ANOMALY]


def test_unscoped_rows_from_before_the_upgrade_match_every_connection(tmp_path):
    path = tmp_path / STORE_FILENAME
    _create_old_store(path, user_version=SCOPELESS_VERSION)

    for scope in (STAGING_SCOPE, PRODUCTION_SCOPE, None):
        store = ProfileStore(db_path=path, scope=scope)
        assert store.get_latest_profile("people", "volume") == VOLUME_PROFILE
        assert store.get_latest_anomaly_per_table()["people"]["type"] == ANOMALY[1]
        store.close()


def test_scope_migration_is_safe_to_run_twice(tmp_path):
    path = tmp_path / STORE_FILENAME
    _create_old_store(path, user_version=SCOPELESS_VERSION)
    _open_and_close(path)
    store = ProfileStore(db_path=path, scope=STAGING_SCOPE)
    store.save_profile("people", "volume", {"row_count": 9})
    store.close()
    rows_after_first = _profile_rows(path)

    conn = sqlite3.connect(str(path))
    _add_scope_columns(conn)
    conn.commit()
    conn.execute(f"PRAGMA user_version = {SCOPELESS_VERSION}")
    conn.close()
    _open_and_close(path)

    assert _user_version(path) == LATEST_VERSION
    for table in SCOPED_TABLES:
        assert _columns(path, table).count(SCOPE_COLUMN) == 1
    assert _profile_rows(path) == rows_after_first
    assert _scopes(path, "profiles").count(STAGING_SCOPE) == 1


# ----- runs table and anomalies.run_id (#115) --------------------------------

def _tables(path: Path) -> set[str]:
    conn = sqlite3.connect(str(path))
    try:
        return {row[0] for row in conn.execute("SELECT name FROM sqlite_master WHERE type='table'")}
    finally:
        conn.close()


def test_store_from_the_previous_release_gains_runs_and_keeps_its_rows(tmp_path):
    path = tmp_path / STORE_FILENAME
    _create_old_store(path, user_version=SCOPELESS_VERSION)
    rows_before = _profile_rows(path)

    _open_and_close(path)

    assert _user_version(path) == LATEST_VERSION
    assert RUNS_TABLE in _tables(path)
    assert RUN_ID_COLUMN in _columns(path, "anomalies")
    assert _profile_rows(path) == rows_before
    assert _anomalies(path) == [ANOMALY]
    conn = sqlite3.connect(str(path))
    try:
        assert conn.execute(f"SELECT {RUN_ID_COLUMN} FROM anomalies").fetchall() == [(None,)]
    finally:
        conn.close()


def test_runs_migration_is_safe_to_run_twice(tmp_path):
    path = tmp_path / STORE_FILENAME
    _create_old_store(path, user_version=SCOPELESS_VERSION)
    store = ProfileStore(db_path=path, scope=STAGING_SCOPE)
    run = RunRecord(command=COMMAND_WATCH, scope=STAGING_SCOPE).finish(["people"], [])
    store.save_run(run.to_dict())
    store.close()

    conn = sqlite3.connect(str(path))
    _add_runs(conn)
    conn.commit()
    conn.execute(f"PRAGMA user_version = {SCOPELESS_VERSION}")
    conn.close()
    _open_and_close(path)

    assert _user_version(path) == LATEST_VERSION
    assert _columns(path, "anomalies").count(RUN_ID_COLUMN) == 1
    assert ProfileStore(db_path=path).get_runs() == [run.to_dict()]


# ----- anomalies.details (#108) ---------------------------------------------


def test_details_migration_keeps_every_anomaly_row(tmp_path):
    path = tmp_path / STORE_FILENAME
    _create_old_store(path, user_version=BEFORE_DETAILS_VERSION)
    profiles_before = _profiles(path)
    assert ANOMALY_DETAILS_COLUMN not in _anomaly_columns(path)

    store = ProfileStore(db_path=path)
    try:
        (old_record,) = store.get_anomaly_history(days=ALL_HISTORY_DAYS)
        new_anomaly = detect_volume_anomalies("people", {"row_count": 3}, {"row_count": 6})[0]
        store.save_anomalies([new_anomaly])
        records = store.get_anomaly_history(days=ALL_HISTORY_DAYS)
    finally:
        store.close()

    assert _user_version(path) == LATEST_VERSION
    assert _anomaly_columns(path).count(ANOMALY_DETAILS_COLUMN) == 1
    assert _anomalies(path)[0] == ANOMALY
    assert _profiles(path) == profiles_before
    assert old_record["message"] == ANOMALY[3]
    assert anomaly_details(old_record) == dict.fromkeys(DETAIL_FIELDS)
    assert anomaly_details(records[0]) == anomaly_details(new_anomaly)


def test_details_migration_is_safe_to_run_twice(tmp_path):
    path = tmp_path / STORE_FILENAME
    _create_old_store(path, user_version=BEFORE_DETAILS_VERSION)
    _open_and_close(path)

    conn = sqlite3.connect(str(path))
    _add_anomaly_details_column(conn)
    conn.commit()
    conn.execute(f"PRAGMA user_version = {BEFORE_DETAILS_VERSION}")
    conn.close()
    _open_and_close(path)

    assert _anomaly_columns(path).count(ANOMALY_DETAILS_COLUMN) == 1
    assert _anomalies(path) == [ANOMALY]
    assert _user_version(path) == LATEST_VERSION


def test_a_store_from_before_any_migration_gets_the_details_column(tmp_path):
    path = tmp_path / STORE_FILENAME
    _create_old_store(path)

    _open_and_close(path)

    assert ANOMALY_DETAILS_COLUMN in _anomaly_columns(path)
    assert _anomalies(path) == [ANOMALY]


# ----- baseline_approvals (#109) -----------------------------------------------

# The store the release before #109 leaves: every migration but this one.
BEFORE_APPROVALS_VERSION = MIGRATIONS.index(_add_baseline_approvals)
APPROVAL = ApprovalRecord(
    table_name="people",
    scope=STAGING_SCOPE,
    approved_by="reviewer@example.invalid",
    reason="Planned backfill",
    before_summary=None,
    after_summary={"row_count": 6, "column_count": 1, "anomalies": []},
).to_dict()


def _create_previous_release_store(path: Path) -> None:
    """A store as the release before #109 writes it, with profiles, an anomaly and a run."""
    _create_old_store(path)
    conn = sqlite3.connect(str(path))
    conn.row_factory = sqlite3.Row
    for migration in MIGRATIONS[:BEFORE_APPROVALS_VERSION]:
        migration(conn)
    run = RunRecord(command=COMMAND_WATCH, scope=STAGING_SCOPE).finish(["people"], [])
    record = {**run.to_dict(), "tables": json.dumps(run.tables)}
    conn.execute(
        f"INSERT INTO runs ({', '.join(OLD_RUN_COLUMNS)}) "
        f"VALUES ({', '.join('?' * len(OLD_RUN_COLUMNS))})",
        [record[column] for column in OLD_RUN_COLUMNS],
    )
    conn.execute(f"PRAGMA user_version = {BEFORE_APPROVALS_VERSION}")
    conn.commit()
    conn.close()


def _table_rows(path: Path, table: str) -> list[tuple]:
    """Every row of `table`, in the columns the release before #109 wrote."""
    columns = ", ".join(OLD_RUN_COLUMNS) if table == RUNS_TABLE else "*"
    conn = sqlite3.connect(str(path))
    try:
        return conn.execute(f"SELECT {columns} FROM {table} ORDER BY rowid").fetchall()
    finally:
        conn.close()


def test_store_from_the_previous_release_gains_approvals_and_keeps_its_rows(tmp_path):
    path = tmp_path / STORE_FILENAME
    _create_previous_release_store(path)
    assert APPROVALS_TABLE not in _tables(path)
    before = {table: _table_rows(path, table) for table in ("profiles", "anomalies", RUNS_TABLE)}

    store = ProfileStore(db_path=path, scope=STAGING_SCOPE)
    store.save_approval(APPROVAL)
    approvals = store.get_approvals()
    store.close()

    assert _user_version(path) == LATEST_VERSION
    assert {table: _table_rows(path, table) for table in before} == before
    assert approvals == [APPROVAL]


def test_approvals_migration_is_safe_to_run_twice_and_stays_append_only(tmp_path):
    path = tmp_path / STORE_FILENAME
    _create_previous_release_store(path)
    ProfileStore(db_path=path, scope=STAGING_SCOPE).save_approval(APPROVAL)

    conn = sqlite3.connect(str(path))
    _add_baseline_approvals(conn)
    conn.commit()
    conn.execute(f"PRAGMA user_version = {BEFORE_APPROVALS_VERSION}")
    conn.close()
    _open_and_close(path)

    assert _user_version(path) == LATEST_VERSION
    assert ProfileStore(db_path=path).get_approvals() == [APPROVAL]
    conn = sqlite3.connect(str(path))
    try:
        with pytest.raises(sqlite3.IntegrityError, match=APPEND_ONLY_ERROR):
            conn.execute(f"DELETE FROM {APPROVALS_TABLE}")
    finally:
        conn.close()


# ----- runs.against and runs.ignored (#110) ----------------------------------

BEFORE_RUN_COMPARISON_VERSION = MIGRATIONS.index(_add_run_comparison_columns)


def _store_before_run_comparison(path: Path, monkeypatch) -> None:
    """A store as the release before #110 leaves it: every migration but the last."""
    _create_old_store(path)
    with monkeypatch.context() as patched:
        patched.setattr(store_sqlite, "MIGRATIONS", MIGRATIONS[:BEFORE_RUN_COMPARISON_VERSION])
        _open_and_close(path)


def _insert_run_as_an_older_release(path: Path) -> dict:
    record = RunRecord(command=COMMAND_WATCH, scope=STAGING_SCOPE).finish(["people"], [])
    row = {
        column: json.dumps(value) if column in RUN_LIST_COLUMNS else value
        for column, value in record.to_dict().items()
        if column in OLD_RUN_COLUMNS
    }
    conn = sqlite3.connect(str(path))
    try:
        conn.execute(
            f"INSERT INTO runs ({', '.join(row)}) VALUES ({', '.join('?' for _ in row)})",
            list(row.values()),
        )
        conn.commit()
    finally:
        conn.close()
    return record.to_dict()


def _run_columns(path: Path) -> list[str]:
    conn = sqlite3.connect(str(path))
    try:
        return [row[1] for row in conn.execute("PRAGMA table_info(runs)")]
    finally:
        conn.close()


def test_runs_saved_before_the_upgrade_read_back_as_comparing_nothing(tmp_path, monkeypatch):
    path = tmp_path / STORE_FILENAME
    _store_before_run_comparison(path, monkeypatch)
    assert _user_version(path) == BEFORE_RUN_COMPARISON_VERSION
    assert RUN_AGAINST_KEY not in _run_columns(path)
    old_run = _insert_run_as_an_older_release(path)

    _open_and_close(path)

    assert _user_version(path) == LATEST_VERSION
    [run] = ProfileStore(db_path=path).get_runs()
    assert run == old_run
    assert run[RUN_AGAINST_KEY] is None and run[RUN_IGNORED_KEY] == []
    assert _profile_rows(path) and _anomalies(path) == [ANOMALY]


def test_an_older_runner_can_still_save_runs_after_the_upgrade(tmp_path, monkeypatch):
    path = tmp_path / STORE_FILENAME
    _store_before_run_comparison(path, monkeypatch)
    _open_and_close(path)
    verify_run = RunRecord(
        command="verify", scope=STAGING_SCOPE, against=PRODUCTION_SCOPE, ignored=["volume"]
    ).finish(["people"], [])
    store = ProfileStore(db_path=path)
    store.save_run(verify_run.to_dict())
    store.close()

    older_run = _insert_run_as_an_older_release(path)

    runs = {run["run_id"]: run for run in ProfileStore(db_path=path).get_runs()}
    assert runs[older_run[RUN_ID_COLUMN]] == older_run
    assert runs[verify_run.run_id] == verify_run.to_dict()


def test_run_comparison_migration_is_safe_to_run_twice(tmp_path, monkeypatch):
    path = tmp_path / STORE_FILENAME
    _store_before_run_comparison(path, monkeypatch)
    _open_and_close(path)

    conn = sqlite3.connect(str(path))
    _add_run_comparison_columns(conn)
    conn.commit()
    conn.execute(f"PRAGMA user_version = {BEFORE_RUN_COMPARISON_VERSION}")
    conn.close()
    _open_and_close(path)

    assert _user_version(path) == LATEST_VERSION
    columns = _run_columns(path)
    assert columns.count(RUN_AGAINST_KEY) == 1 and columns.count(RUN_IGNORED_KEY) == 1
