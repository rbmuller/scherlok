"""Values read from rows never reach the profile store, only aggregates (#114)."""

from __future__ import annotations

import importlib.util
import sqlite3
from pathlib import Path

import pytest
from typer.testing import CliRunner

from scherlok.cli import app
from scherlok.store.sqlite import DISTRIBUTION_PROFILE_PREFIX

pytestmark = pytest.mark.skipif(
    importlib.util.find_spec("duckdb") is None,
    reason="duckdb not installed",
)

runner = CliRunner(env={"NO_COLOR": "1"})

TABLE = "people"
COLUMNS = ("email", "document", "salary", "birth_date")
SENTINEL_EMAIL = "sentinel.person@example.invalid"
SENTINEL_DOCUMENT = "SENTINEL-DOC-4711"
SENTINEL_BIRTH_DATE = "1931-07-19"
SENTINEL_LOWEST_SALARY = "424242.125"
SENTINEL_HIGHEST_SALARY = "868686.375"
SENTINELS = (
    SENTINEL_EMAIL,
    SENTINEL_DOCUMENT,
    SENTINEL_BIRTH_DATE,
    SENTINEL_LOWEST_SALARY,
    SENTINEL_HIGHEST_SALARY,
)

# Each sentinel is what an older release would have saved: the most frequent
# text and date values (top_values) and the numeric extremes (min, max).
ROWS = (
    (SENTINEL_EMAIL, SENTINEL_DOCUMENT, SENTINEL_LOWEST_SALARY, SENTINEL_BIRTH_DATE),
    (SENTINEL_EMAIL, SENTINEL_DOCUMENT, SENTINEL_HIGHEST_SALARY, SENTINEL_BIRTH_DATE),
    (SENTINEL_EMAIL, SENTINEL_DOCUMENT, "500000.0", SENTINEL_BIRTH_DATE),
    ("other.one@example.invalid", "OTHER-DOC-1", "500000.0", "1990-01-01"),
    ("other.two@example.invalid", "OTHER-DOC-2", "500000.0", "1991-02-02"),
    (None, None, None, None),
)


@pytest.fixture()
def isolated_scherlok(tmp_path, monkeypatch) -> Path:
    """Point config and store at a scratch directory; returns the store path."""
    import scherlok.config as config

    scherlok_dir = tmp_path / ".scherlok"
    profiles_db = scherlok_dir / "profiles.db"
    monkeypatch.setattr(config, "SCHERLOK_DIR", scherlok_dir)
    monkeypatch.setattr(config, "CONFIG_FILE", scherlok_dir / "config.json")
    monkeypatch.setattr(config, "PROFILES_DB", profiles_db)
    monkeypatch.setattr("scherlok.store.sqlite.PROFILES_DB", profiles_db)
    monkeypatch.delenv(config.ENV_STORE, raising=False)
    return profiles_db


@pytest.fixture()
def warehouse(tmp_path, monkeypatch) -> Path:
    """A DuckDB file holding one table of sentinel values, set as the connection."""
    import duckdb

    from scherlok.config import ENV_CONNECTION

    path = tmp_path / "warehouse.duckdb"
    conn = duckdb.connect(str(path))
    try:
        conn.execute(
            f"CREATE TABLE {TABLE} "
            "(email VARCHAR, document VARCHAR, salary DOUBLE, birth_date DATE)"
        )
        conn.executemany(f"INSERT INTO {TABLE} VALUES (?, ?, ?, ?)", ROWS)
    finally:
        conn.close()
    monkeypatch.setenv(ENV_CONNECTION, f"duckdb://{path}")
    return path


def _store_bytes(profiles_db: Path) -> bytes:
    """Every byte SQLite left on disk for the store, journals included."""
    return b"".join(
        path.read_bytes() for path in sorted(profiles_db.parent.glob(f"{profiles_db.name}*"))
    )


def _distribution_profile_types(profiles_db: Path) -> set[str]:
    conn = sqlite3.connect(str(profiles_db))
    try:
        rows = conn.execute(
            "SELECT DISTINCT profile_type FROM profiles WHERE table_name = ?", (TABLE,)
        ).fetchall()
    finally:
        conn.close()
    return {row[0] for row in rows if row[0].startswith(DISTRIBUTION_PROFILE_PREFIX)}


@pytest.mark.parametrize("command", ["investigate", "watch"])
def test_sentinel_values_never_reach_the_store(command, isolated_scherlok, warehouse):
    # The second run has a baseline, so `watch` also goes through detection.
    for _ in range(2):
        result = runner.invoke(app, [command])
        assert result.exit_code == 0, result.output

    assert _distribution_profile_types(isolated_scherlok) == {
        f"{DISTRIBUTION_PROFILE_PREFIX}{column}" for column in COLUMNS
    }
    stored = _store_bytes(isolated_scherlok)
    for sentinel in SENTINELS:
        assert sentinel.encode() not in stored, f"{sentinel!r} found in the store"
