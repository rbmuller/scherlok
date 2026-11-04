"""Tests for `scherlok verify`, the read-only gate (#106).

Driven end-to-end against a real DuckDB file with synthetic rows, with every
path under ~/.scherlok redirected to a scratch directory, so the assertions
about what is (not) written hold for the real profile store and config file.
"""

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
from scherlok.detector.fields import DETAIL_FIELDS

pytestmark = pytest.mark.skipif(
    importlib.util.find_spec("duckdb") is None,
    reason="duckdb not installed",
)

runner = CliRunner(env={"NO_COLOR": "1"})

BASELINE_ROWS = 1000
REMOTE_STORE = "s3://example-bucket/scherlok/profiles.db"
JSON_KEYS = {
    "passed", "fail_on", "tables_verified", "tables_without_baseline",
    "anomaly_count", "critical", "warning", "info", "anomalies",
}


@pytest.fixture()
def home(tmp_path, monkeypatch) -> Path:
    """Redirect the config file and the profile store to a scratch directory."""
    import scherlok.config as config
    import scherlok.store.sqlite as sqlite_store

    scherlok_dir = tmp_path / ".scherlok"
    monkeypatch.setattr(config, "SCHERLOK_DIR", scherlok_dir)
    monkeypatch.setattr(config, "CONFIG_FILE", scherlok_dir / "config.json")
    monkeypatch.setattr(config, "PROFILES_DB", scherlok_dir / "profiles.db")
    monkeypatch.setattr(sqlite_store, "PROFILES_DB", scherlok_dir / "profiles.db")
    monkeypatch.delenv("SCHERLOK_STORE", raising=False)
    return scherlok_dir


@pytest.fixture()
def warehouse(tmp_path, monkeypatch, home) -> Path:
    """A DuckDB file with one 1,000-row table, reachable via SCHERLOK_CONNECTION."""
    path = tmp_path / "warehouse.duckdb"
    _execute(
        path,
        "CREATE TABLE orders (id INTEGER, amount DOUBLE, status VARCHAR)",
        "INSERT INTO orders SELECT i, i * 1.5, CASE WHEN i % 3 = 0 THEN 'paid' ELSE 'open' END "
        f"FROM range(1, {BASELINE_ROWS + 1}) t(i)",
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


def _keep_rows(path: Path, rows: int) -> None:
    _execute(path, f"DELETE FROM orders WHERE id > {rows}")


def _store_row_counts(home: Path) -> dict[str, int]:
    conn = sqlite3.connect(home / "profiles.db")
    try:
        return {
            name: conn.execute(f"SELECT COUNT(*) FROM {name}").fetchone()[0]
            for name in ("profiles", "anomalies")
        }
    finally:
        conn.close()


def _learn_baseline() -> None:
    result = runner.invoke(app, ["investigate"])
    assert result.exit_code == 0, result.output


def _verify_json(*args: str):
    result = runner.invoke(app, ["verify", "--output", "json", *args])
    return result, json.loads(result.stdout)


# ----- the gate holds on retry ---------------------------------------------

def test_retrying_verify_on_a_broken_table_keeps_failing(warehouse, home):
    _learn_baseline()
    _keep_rows(warehouse, 300)  # lost 70% of rows
    before = _store_row_counts(home)

    runs = [_verify_json() for _ in range(3)]

    for result, payload in runs:
        assert result.exit_code == 1
        assert payload["passed"] is False
        assert payload["critical"] == 1
        assert payload["anomalies"][0]["type"] == "volume_drop"
    assert runs[1][1] == runs[0][1] and runs[2][1] == runs[0][1]
    assert _store_row_counts(home) == before


def test_watch_after_verify_still_sees_the_breakage(warehouse, home):
    """verify must not teach the baseline the broken state; watch does."""
    _learn_baseline()
    _keep_rows(warehouse, 300)
    runner.invoke(app, ["verify"])

    first_watch = runner.invoke(app, ["watch"])
    second_watch = runner.invoke(app, ["watch"])

    assert first_watch.exit_code == 1
    assert second_watch.exit_code == 0  # watch learned the new state; verify didn't


def test_verify_text_mode_reports_the_failure(warehouse):
    _learn_baseline()
    _keep_rows(warehouse, 300)

    result = runner.invoke(app, ["verify"])

    assert result.exit_code == 1
    assert "volume_drop" in result.stdout
    assert "Verify failed" in result.stdout
    assert "No profiles or anomalies were saved" in result.stdout


def test_verify_passes_on_unchanged_data(warehouse, home):
    _learn_baseline()
    before = _store_row_counts(home)

    result, payload = _verify_json()

    assert result.exit_code == 0
    assert payload["passed"] is True
    assert payload["tables_verified"] == ["orders"]
    assert payload["anomalies"] == []
    assert _store_row_counts(home) == before


# ----- --fail-on -----------------------------------------------------------

def test_fail_on_decides_the_exit_code(warehouse):
    _learn_baseline()
    _keep_rows(warehouse, 700)  # lost 30% of rows: WARNING

    lenient, lenient_payload = _verify_json("--fail-on", "critical")
    strict, strict_payload = _verify_json("--fail-on", "warning")

    assert lenient_payload["warning"] == 1 and lenient_payload["critical"] == 0
    assert lenient.exit_code == 0 and lenient_payload["passed"] is True
    assert strict.exit_code == 1 and strict_payload["passed"] is False
    assert strict_payload["fail_on"] == "warning"


def test_invalid_fail_on_exits_1_before_doing_anything(home):
    result = runner.invoke(app, ["verify", "--fail-on", "warnings"])

    assert result.exit_code == 1
    assert "Invalid --fail-on value 'warnings'" in result.output
    assert not (home / "profiles.db").exists()


def test_ci_rejects_an_invalid_fail_on_before_saving_the_connection(home):
    """`ci` uses the same helper: a typo no longer passes as 'critical'."""
    result = runner.invoke(app, ["ci", "duckdb:///:memory:", "--fail-on", "warnings"])

    assert result.exit_code == 1
    assert "Invalid --fail-on value" in result.output
    assert not (home / "config.json").exists()


# ----- --output json -------------------------------------------------------

def test_json_is_one_object_on_stdout_and_rich_goes_to_stderr(warehouse):
    _learn_baseline()
    _keep_rows(warehouse, 300)

    result = runner.invoke(app, ["verify", "--output", "json"])

    payload = json.loads(result.stdout)  # the whole of stdout is one object
    assert set(payload) == JSON_KEYS
    assert payload["fail_on"] == "critical"
    assert payload["anomaly_count"] == len(payload["anomalies"]) == 1
    assert set(payload["anomalies"][0]) == {"table", "type", "severity", "message", *DETAIL_FIELDS}
    assert payload["anomalies"][0]["severity"] == "CRITICAL"
    assert payload["anomalies"][0]["message"] == "Row count dropped 70.0% (1,000 -> 300)"
    assert payload["anomalies"][0]["metric"] == "row_count"
    assert payload["anomalies"][0]["baseline"] == BASELINE_ROWS
    assert payload["anomalies"][0]["current"] == 300
    assert payload["anomalies"][0]["change_pct"] == -70.0
    assert "Detected Anomalies" not in result.stdout
    assert "Verify failed: 1 verified" in result.stderr


def test_json_mode_passing_run_keeps_stderr_quiet(warehouse):
    _learn_baseline()

    result = runner.invoke(app, ["verify", "--output", "json"])

    assert result.exit_code == 0
    assert json.loads(result.stdout)["passed"] is True
    assert result.stderr == ""


def test_json_errors_keep_stdout_empty(warehouse):
    result = runner.invoke(app, ["verify", "--output", "json", "--table", "ghost"])

    assert result.exit_code == 1
    assert result.stdout == ""
    assert "not found" in result.stderr


def test_invalid_output_exits_1():
    result = runner.invoke(app, ["verify", "--output", "xml"])
    assert result.exit_code == 1


# ----- tables without a baseline -------------------------------------------

def test_tables_without_baseline_are_listed_never_verified(warehouse):
    _learn_baseline()
    _execute(warehouse, "CREATE TABLE refunds (id INTEGER)", "INSERT INTO refunds VALUES (1)")

    result, payload = _verify_json()

    assert result.exit_code == 0
    assert payload["tables_verified"] == ["orders"]
    assert payload["tables_without_baseline"] == ["refunds"]


def test_only_unbaselined_tables_pass_without_verifying_anything(warehouse, home):
    result, payload = _verify_json("--table", "orders")

    assert result.exit_code == 0
    assert payload["tables_verified"] == []
    assert payload["tables_without_baseline"] == ["orders"]
    assert _store_row_counts(home) == {"profiles": 0, "anomalies": 0}


def test_text_mode_names_tables_without_baseline(warehouse):
    result = runner.invoke(app, ["verify"])

    assert result.exit_code == 0
    assert "no baseline yet (1): orders" in result.stdout
    assert "0 verified, 1 without baseline" in result.stdout


def test_table_option_limits_scope_and_rejects_unknown_tables(warehouse):
    _learn_baseline()
    _execute(warehouse, "CREATE TABLE refunds (id INTEGER)")

    scoped, payload = _verify_json("--table", "orders")
    unknown = runner.invoke(app, ["verify", "--table", "ghost"])

    assert scoped.exit_code == 0
    assert payload["tables_verified"] == ["orders"]
    assert payload["tables_without_baseline"] == []
    assert unknown.exit_code == 1
    assert "Tables not found on this connection: ['ghost']" in unknown.output


# ----- connection and config -----------------------------------------------

def test_verify_never_writes_the_connection_to_config(warehouse, home):
    _learn_baseline()

    result = runner.invoke(app, ["verify"])

    assert result.exit_code == 0
    assert not (home / "config.json").exists()


def test_verify_leaves_an_existing_config_untouched(warehouse, home, monkeypatch):
    from scherlok.config import ScherlokConfig

    saved = ScherlokConfig(connection_string=f"duckdb:///{warehouse}")
    saved.save()
    original = (home / "config.json").read_bytes()
    monkeypatch.delenv("SCHERLOK_CONNECTION")  # use the saved connection

    _learn_baseline()
    result = runner.invoke(app, ["verify"])

    assert result.exit_code == 0
    assert (home / "config.json").read_bytes() == original


def test_verify_without_a_connection_exits_1(home, monkeypatch):
    monkeypatch.delenv("SCHERLOK_CONNECTION", raising=False)

    result = runner.invoke(app, ["verify", "--output", "json"])

    assert result.exit_code == 1
    assert result.stdout == ""
    assert "No connection configured" in result.stderr


# ----- remote store --------------------------------------------------------

def _fake_remote(tmp_path: Path, home: Path) -> tuple[MagicMock, MagicMock, Path]:
    """Stand-ins for the bucket: download copies a file, upload records calls.

    The local store is moved to the "bucket", so the baseline can only come
    from a download.
    """
    remote_copy = tmp_path / "remote-profiles.db"
    shutil.move(home / "profiles.db", remote_copy)

    def download(_url: str, local_path: Path) -> bool:
        shutil.copy(remote_copy, local_path)
        return True

    return MagicMock(side_effect=download), MagicMock(), remote_copy


def test_verify_downloads_a_remote_store_but_never_uploads_it(
    warehouse, home, tmp_path, monkeypatch
):
    _learn_baseline()
    _keep_rows(warehouse, 300)
    download, upload, remote_copy = _fake_remote(tmp_path, home)
    remote_bytes = remote_copy.read_bytes()
    monkeypatch.setenv("SCHERLOK_STORE", REMOTE_STORE)

    with (
        patch("scherlok.store.remote.download", download),
        patch("scherlok.store.remote.upload", upload),
    ):
        result, payload = _verify_json()

    assert result.exit_code == 1
    assert payload["critical"] == 1  # the downloaded baseline was used
    download.assert_called_once()
    assert download.call_args.args[0] == REMOTE_STORE
    upload.assert_not_called()
    assert remote_copy.read_bytes() == remote_bytes


def test_watch_still_uploads_the_remote_store(warehouse, home, tmp_path, monkeypatch):
    """Control for the test above: the same wiring does see an upload."""
    _learn_baseline()
    download, upload, _remote_copy = _fake_remote(tmp_path, home)
    monkeypatch.setenv("SCHERLOK_STORE", REMOTE_STORE)

    with (
        patch("scherlok.store.remote.download", download),
        patch("scherlok.store.remote.upload", upload),
    ):
        runner.invoke(app, ["watch"])

    upload.assert_called_once()
    assert upload.call_args.args[1] == REMOTE_STORE


# ----- help ----------------------------------------------------------------

def test_help_lists_verify_and_its_options():
    import re

    top = runner.invoke(app, ["--help"])
    own = re.sub(r"\x1b\[[0-9;]*m", "", runner.invoke(app, ["verify", "--help"]).output)

    assert "verify" in top.output
    for option in ("--table", "--fail-on", "--output"):
        assert option in own
