"""Tests for `scherlok demo` — the self-contained, no-database walkthrough."""

from __future__ import annotations

import importlib.util
import json
from pathlib import Path

import pytest
from typer.testing import CliRunner

from scherlok.cli import app

pytestmark = pytest.mark.skipif(
    importlib.util.find_spec("duckdb") is None,
    reason="duckdb not installed",
)

runner = CliRunner(env={"NO_COLOR": "1"})

EXPECTED_ANOMALY_TYPES = {"volume_drop", "null_rate_change", "cardinality_change", "column_removed"}


@pytest.fixture()
def fake_home(tmp_path, monkeypatch):
    """Redirect everything that could touch ~/.scherlok to a scratch HOME."""
    home = tmp_path / "home"
    home.mkdir()
    monkeypatch.setattr(Path, "home", classmethod(lambda cls: home))
    monkeypatch.setenv("HOME", str(home))
    import scherlok.config as config

    monkeypatch.setattr(config, "SCHERLOK_DIR", home / ".scherlok")
    monkeypatch.setattr(config, "CONFIG_FILE", home / ".scherlok" / "config.json")
    monkeypatch.setattr(config, "PROFILES_DB", home / ".scherlok" / "profiles.db")
    return home


def _row_count(path: Path, table: str) -> int:
    import duckdb

    conn = duckdb.connect(str(path))
    try:
        return conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
    finally:
        conn.close()


def _scalar(path: Path, sql: str):
    import duckdb

    conn = duckdb.connect(str(path))
    try:
        return conn.execute(sql).fetchone()[0]
    finally:
        conn.close()


def test_seed_is_deterministic(tmp_path):
    from scherlok.demo.seed import ORDERS_ROWS, PRODUCTS_ROWS, USERS_ROWS, seed_database

    first, second = tmp_path / "a.duckdb", tmp_path / "b.duckdb"
    rows_a = seed_database(first)
    rows_b = seed_database(second)

    expected = {"users": USERS_ROWS, "orders": ORDERS_ROWS, "products": PRODUCTS_ROWS}
    assert rows_a == rows_b == expected
    sample_sql = "SELECT string_agg(email, ',') FROM (SELECT email FROM users ORDER BY id LIMIT 50)"
    assert _scalar(first, sample_sql) == _scalar(second, sample_sql)
    total_sql = "SELECT SUM(amount) FROM orders"
    assert _scalar(first, total_sql) == _scalar(second, total_sql)


def test_bad_deploy_mutations_hit_each_detector_threshold(tmp_path):
    from scherlok.demo.seed import DROPPED_COLUMN, apply_bad_deploy, seed_database
    from scherlok.detector.cardinality import CARDINALITY_CRITICAL_PCT
    from scherlok.detector.nullability import NULL_RATE_CRITICAL_DELTA, NULL_RATE_WARNING_DELTA

    db = tmp_path / "demo.duckdb"
    before = seed_database(db)
    plans_before = _scalar(db, "SELECT COUNT(DISTINCT plan) FROM users")

    apply_bad_deploy(db)

    orders_after = _row_count(db, "orders")
    drop_pct = (before["orders"] - orders_after) / before["orders"] * 100
    assert drop_pct >= 50, f"volume drop {drop_pct:.1f}% must reach the CRITICAL threshold"

    null_rate = _scalar(db, "SELECT COUNT(*) - COUNT(email) FROM users") / before["users"]
    assert NULL_RATE_WARNING_DELTA <= null_rate < NULL_RATE_CRITICAL_DELTA, (
        "e-mail NULL surge is meant to land as a WARNING, not CRITICAL"
    )

    plans_after = _scalar(db, "SELECT COUNT(DISTINCT plan) FROM users")
    assert (plans_after - plans_before) / plans_before * 100 >= CARDINALITY_CRITICAL_PCT

    columns = _scalar(
        db,
        "SELECT string_agg(column_name, ',') FROM information_schema.columns "
        "WHERE table_name = 'products'",
    )
    assert DROPPED_COLUMN not in columns.split(",")


def test_run_demo_end_to_end_catches_the_bad_deploy(tmp_path, fake_home):
    from scherlok.demo import run_demo
    from scherlok.detector.severity import Severity

    result = run_demo(directory=tmp_path / "demo", console=None)

    types = {a["type"] for a in result.anomalies}
    assert EXPECTED_ANOMALY_TYPES <= types, f"missing detectors in {types}"
    severities = {a["severity"] for a in result.anomalies}
    assert Severity.CRITICAL in severities and Severity.WARNING in severities
    assert result.severity_counts()["CRITICAL"] >= 3
    assert result.kept is True, "an explicit --dir implies --keep"
    assert (tmp_path / "demo" / "demo.duckdb").exists()
    assert (tmp_path / "demo" / "profiles.db").exists()
    assert not (fake_home / ".scherlok").exists(), "the demo must never touch ~/.scherlok"


def test_run_demo_removes_temp_dir_unless_kept(fake_home):
    from scherlok.demo import run_demo

    gone = run_demo(console=None)
    assert gone.kept is False
    assert not gone.directory.exists()

    kept = run_demo(keep=True, console=None)
    try:
        assert kept.directory.exists() and kept.database.exists()
    finally:
        import shutil

        shutil.rmtree(kept.directory, ignore_errors=True)


def test_cli_demo_text_output(fake_home):
    result = runner.invoke(app, ["demo"])

    assert result.exit_code == 0, result.output
    assert "Detected Anomalies" in result.output
    assert "CRITICAL" in result.output and "WARNING" in result.output
    assert "scherlok connect" in result.output
    assert not (fake_home / ".scherlok").exists()


def test_cli_demo_json_output_is_machine_readable(tmp_path, fake_home):
    result = runner.invoke(app, ["demo", "--output", "json", "--dir", str(tmp_path / "d")])

    assert result.exit_code == 0, result.output
    payload = json.loads(result.output)
    assert payload["database"].startswith("duckdb://")
    assert payload["kept"] is True
    assert {a["type"] for a in payload["anomalies"]} >= EXPECTED_ANOMALY_TYPES
    assert payload["summary"]["CRITICAL"] >= 3
    assert all(
        isinstance(a["severity"], str) and "." not in a["severity"] for a in payload["anomalies"]
    )


def test_cli_demo_rejects_unknown_output_format(fake_home):
    result = runner.invoke(app, ["demo", "--output", "yaml"])
    assert result.exit_code == 1
    assert "Invalid --output value" in result.output


def test_cli_demo_explains_missing_duckdb(fake_home, monkeypatch):
    import scherlok.cli as cli

    monkeypatch.setattr(cli, "duckdb_available", lambda: False)
    result = runner.invoke(app, ["demo"])
    assert result.exit_code == 1
    assert "scherlok[duckdb]" in result.output


def test_help_lists_demo():
    result = runner.invoke(app, ["--help"])
    assert result.exit_code == 0
    assert "demo" in result.output
