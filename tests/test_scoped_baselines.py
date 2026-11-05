"""Tests for scoped baselines (#110): one store, separate history per database."""

from __future__ import annotations

import importlib.util
import json
import sqlite3
from pathlib import Path

import pytest
from typer.testing import CliRunner

from scherlok.cli import app
from scherlok.detector.severity import Severity
from scherlok.service import connection_scope
from scherlok.store.sqlite import SCOPE_COLUMN, ProfileStore

_HAS_DUCKDB = importlib.util.find_spec("duckdb") is not None
needs_duckdb = pytest.mark.skipif(not _HAS_DUCKDB, reason="duckdb not installed")

runner = CliRunner(env={"NO_COLOR": "1"})

SECRET = "S3CRET-v4lue"
STAGING = "postgresql://staging.example.com/shop"
PRODUCTION = "postgresql://prod.example.com/shop"
STORE_FILENAME = "profiles.db"


# ----- connection_scope -----------------------------------------------------

@pytest.mark.parametrize(
    ("connection_string", "scope"),
    [
        (
            "postgresql://alice:pw@DB.Example.com:5432/shop?sslmode=require",
            "postgresql://db.example.com/shop",
        ),
        ("postgres://alice@db.example.com/shop/", "postgresql://db.example.com/shop"),
        ("postgresql://db.example.com:6543/shop", "postgresql://db.example.com:6543/shop"),
        ("postgresql://alice:pw@[::1]:5433/shop", "postgresql://[::1]:5433/shop"),
        ("mysql://root:pw@127.0.0.1:3306/app", "mysql://127.0.0.1/app"),
        ("mysql://root:pw@127.0.0.1:3307/app", "mysql://127.0.0.1:3307/app"),
        ("duckdb:///data/warehouse.duckdb", "duckdb:///data/warehouse.duckdb"),
        ("duckdb:///:memory:", "duckdb:///:memory:"),
        ("bigquery://my-project/analytics", "bigquery://my-project/analytics"),
        ("snowflake://my-account/SHOP/PUBLIC", "snowflake://my-account/SHOP/PUBLIC"),
    ],
)
def test_scope_keeps_adapter_host_and_path(connection_string, scope):
    assert connection_scope(connection_string) == scope


def test_spellings_of_one_database_share_a_scope():
    # `scherlok dbt` always writes the port; a hand-written URL often doesn't.
    from_dbt = "postgresql://dbt_user:pw@db.example.com:5432/shop"
    by_hand = "postgres://analyst@DB.EXAMPLE.COM/shop"

    assert connection_scope(from_dbt) == connection_scope(by_hand)


@pytest.mark.parametrize(
    ("first", "second"),
    [
        ("postgresql://staging.example.com/shop", "postgresql://prod.example.com/shop"),
        ("postgresql://db.example.com/shop", "postgresql://db.example.com/shop_dev"),
        ("snowflake://acct/SHOP/STAGING", "snowflake://acct/SHOP/PUBLIC"),
        ("bigquery://my-project/dev", "bigquery://my-project/prod"),
        ("duckdb:///tmp/a.duckdb", "duckdb:///tmp/b.duckdb"),
    ],
)
def test_different_databases_get_different_scopes(first, second):
    assert connection_scope(first) != connection_scope(second)


@pytest.mark.parametrize("connection_string", [None, "", "not a url", "host=db password=x"])
def test_no_scope_without_a_url(connection_string):
    assert connection_scope(connection_string) is None


@pytest.mark.parametrize(
    "connection_string",
    [
        # Passwords and tokens in the user info, plain or percent-encoded.
        f"postgresql://alice:{SECRET}@db.example.com:5432/shop",
        f"postgres://{SECRET}@db.example.com/shop",
        f"postgresql://alice:{SECRET}%2F%40%3F@db.example.com/shop",
        f"mysql://root:{SECRET}@127.0.0.1:3306/app",
        f"bigquery://svc:{SECRET}@my-project/analytics",
        f"snowflake://alice:{SECRET}@my-account/SHOP/PUBLIC",
        # Secrets and keys in the query string or the fragment.
        f"postgresql://db.example.com/shop?password={SECRET}&sslkey=/keys/{SECRET}.pem",
        f"mysql://127.0.0.1/app?ssl_key=/keys/{SECRET}.pem",
        f"duckdb:///data/warehouse.duckdb?motherduck_token={SECRET}",
        f"bigquery://my-project/analytics?credentials={SECRET}",
        f"snowflake://my-account/SHOP/PUBLIC?private_key={SECRET}&token={SECRET}",
        f"postgresql://db.example.com/shop#{SECRET}",
        # Unencoded reserved characters in a password cut the host short.
        f"postgresql://alice:1234/{SECRET}@db.example.com/shop",
        f"postgresql://alice:1234?{SECRET}@db.example.com/shop",
        f"postgresql://alice:1234#{SECRET}@db.example.com/shop",
        f"postgresql://alice:pa@ss?{SECRET}@db.example.com/shop",
        f"mysql://root:{SECRET}/x@127.0.0.1/app",
    ],
)
def test_scope_never_holds_a_password_token_or_key(connection_string):
    scope = connection_scope(connection_string) or ""

    assert SECRET not in scope
    assert "1234" not in scope
    assert "@" not in scope
    assert "?" not in scope and "#" not in scope


# ----- ProfileStore -----------------------------------------------------------

def _store(path: Path, scope: str | None) -> ProfileStore:
    return ProfileStore(db_path=path, scope=scope)


def test_same_table_on_two_connections_keeps_separate_baselines(tmp_path):
    path = tmp_path / STORE_FILENAME
    staging, production = _store(path, STAGING), _store(path, PRODUCTION)
    production.save_profile("orders", "volume", {"row_count": 1000})
    staging.save_profile("orders", "volume", {"row_count": 10})  # saved last

    assert production.get_latest_profile("orders", "volume") == {"row_count": 1000}
    assert staging.get_latest_profile("orders", "volume") == {"row_count": 10}
    assert [p["row_count"] for p in production.get_profile_history("orders", "volume")] == [
        1000
    ]


def test_a_store_without_scope_reads_every_connection(tmp_path):
    path = tmp_path / STORE_FILENAME
    _store(path, PRODUCTION).save_profile("orders", "volume", {"row_count": 1000})
    _store(path, STAGING).save_profile("orders", "volume", {"row_count": 10})

    history = _store(path, None).get_profile_history("orders", "volume")

    assert sorted(p["row_count"] for p in history) == [10, 1000]


def test_unscoped_rows_join_every_scope_and_the_newest_row_wins(tmp_path):
    path = tmp_path / STORE_FILENAME
    _store(path, None).save_profile("orders", "volume", {"row_count": 500})  # pre-upgrade
    production = _store(path, PRODUCTION)

    assert production.get_latest_profile("orders", "volume") == {"row_count": 500}
    production.save_profile("orders", "volume", {"row_count": 1000})
    assert production.get_latest_profile("orders", "volume") == {"row_count": 1000}
    assert len(production.get_profile_history("orders", "volume")) == 2
    assert _store(path, STAGING).get_latest_profile("orders", "volume") == {"row_count": 500}


def test_a_store_that_excludes_unscoped_rows_reads_its_own_scope_alone(tmp_path):
    """What `verify --against` reads: an unscoped row may be any database's (#110)."""
    path = tmp_path / STORE_FILENAME
    _store(path, None).save_profile("orders", "volume", {"row_count": 500})  # pre-upgrade
    _store(path, STAGING).save_profile("orders", "volume", {"row_count": 10})
    production_only = ProfileStore(db_path=path, scope=PRODUCTION, include_unscoped=False)

    assert production_only.get_latest_profile("orders", "volume") is None
    assert production_only.get_profile_history("orders", "volume") == []
    assert production_only.has_profiles() is False
    assert _store(path, PRODUCTION).has_profiles() is True  # the unscoped row counts there

    _store(path, PRODUCTION).save_profile("orders", "volume", {"row_count": 1000})

    assert production_only.get_latest_profile("orders", "volume") == {"row_count": 1000}
    assert [p["row_count"] for p in production_only.get_profile_history("orders", "volume")] == [
        1000
    ]
    assert production_only.has_profiles() is True
    assert len(_store(path, PRODUCTION).get_profile_history("orders", "volume")) == 2


def test_anomalies_are_kept_per_connection(tmp_path):
    path = tmp_path / STORE_FILENAME
    anomaly = {
        "table": "orders", "type": "volume_drop",
        "severity": Severity.CRITICAL, "message": "Row count dropped",
    }
    staging = _store(path, STAGING)
    staging.save_anomalies([anomaly])

    assert [a["type"] for a in staging.get_anomaly_history()] == ["volume_drop"]
    assert set(staging.get_latest_anomaly_per_table()) == {"orders"}
    production = _store(path, PRODUCTION)
    assert production.get_anomaly_history() == []
    assert production.get_latest_anomaly_per_table() == {}
    assert len(_store(path, None).get_anomaly_history()) == 1


def test_saves_carry_the_scope(tmp_path):
    path = tmp_path / STORE_FILENAME
    store = _store(path, STAGING)
    store.save_profile("orders", "volume", {"row_count": 1})
    store.save_anomalies([
        {"table": "orders", "type": "t", "severity": Severity.INFO, "message": "m"},
    ])
    store.close()

    conn = sqlite3.connect(str(path))
    try:
        for table in ("profiles", "anomalies"):
            scopes = {row[0] for row in conn.execute(f"SELECT {SCOPE_COLUMN} FROM {table}")}
            assert scopes == {STAGING}
    finally:
        conn.close()


# ----- end to end: two DuckDB files, one store ------------------------------

def _make_orders(path: Path, rows: int) -> None:
    import duckdb

    conn = duckdb.connect(str(path))
    try:
        conn.execute("CREATE TABLE orders (id INTEGER, amount DOUBLE)")
        conn.execute(f"INSERT INTO orders SELECT i, i * 1.5 FROM range(1, {rows + 1}) t(i)")
    finally:
        conn.close()


def _invoke(monkeypatch, connection: str, *args: str):
    monkeypatch.setenv("SCHERLOK_CONNECTION", connection)
    return runner.invoke(app, list(args))


@needs_duckdb
def test_watching_staging_never_touches_the_production_baseline(
    tmp_path, monkeypatch, isolated_scherlok_dir
):
    production_db, staging_db = tmp_path / "prod.duckdb", tmp_path / "staging.duckdb"
    _make_orders(production_db, 1000)
    _make_orders(staging_db, 300)  # a sampled build: 70% fewer rows
    production, staging = f"duckdb:///{production_db}", f"duckdb:///{staging_db}"

    assert _invoke(monkeypatch, production, "investigate").exit_code == 0
    assert _invoke(monkeypatch, staging, "investigate").exit_code == 0
    production_watch = _invoke(monkeypatch, production, "watch")
    verify = _invoke(monkeypatch, production, "verify", "--output", "json")
    staging_watch = _invoke(monkeypatch, staging, "watch")

    assert production_watch.exit_code == 0, production_watch.output
    assert json.loads(verify.stdout)["anomalies"] == []
    assert staging_watch.exit_code == 0, staging_watch.output
    conn = sqlite3.connect(str(isolated_scherlok_dir / STORE_FILENAME))
    try:
        scopes = {row[0] for row in conn.execute(f"SELECT {SCOPE_COLUMN} FROM profiles")}
        anomalies = conn.execute("SELECT COUNT(*) FROM anomalies").fetchone()[0]
    finally:
        conn.close()
    assert scopes == {connection_scope(production), connection_scope(staging)}
    assert anomalies == 0


def test_history_shows_the_configured_connection_only(
    tmp_path, monkeypatch, isolated_scherlok_dir
):
    path = isolated_scherlok_dir / STORE_FILENAME
    production, staging = "duckdb:///prod.duckdb", "duckdb:///staging.duckdb"
    for connection, kind in ((production, "volume_drop"), (staging, "schema_drift")):
        _store(path, connection_scope(connection)).save_anomalies([
            {"table": "orders", "type": kind, "severity": Severity.CRITICAL, "message": "m"},
        ])

    result = _invoke(monkeypatch, staging, "history", "--output", "json")

    assert [a["type"] for a in json.loads(result.stdout)] == ["schema_drift"]
