"""Tests for ProfileStore migrations (PRAGMA user_version)."""

from __future__ import annotations

import json
import shutil
import sqlite3
from pathlib import Path

import pytest

from scherlok.store import remote as store_remote
from scherlok.store import sqlite as store_sqlite
from scherlok.store.sqlite import (
    CREATE_ANOMALIES_INDEX_SQL,
    CREATE_ANOMALIES_SQL,
    CREATE_INDEX_SQL,
    CREATE_TABLE_SQL,
    MIGRATIONS,
    RAW_VALUE_KEYS,
    ProfileStore,
    _strip_raw_distribution_values,
)

LATEST_VERSION = len(MIGRATIONS)
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
    path = tmp_path / STORE_FILENAME
    newer_version = LATEST_VERSION + 5
    _create_old_store(path, user_version=newer_version)

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
