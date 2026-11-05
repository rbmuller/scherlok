"""Tests for `scherlok accept` and locked baselines (#109).

Driven end to end against a DuckDB file whose rows hold sentinel values,
reached through a connection string with a token, so the assertions about
what an approval row holds cover the real store. The approver is always
mocked: no test reads the developer's git identity.
"""

from __future__ import annotations

import importlib.util
import json
import shutil
import sqlite3
import subprocess
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
from typer.testing import CliRunner

import scherlok.config as config
from scherlok import approvals
from scherlok.cli import app
from scherlok.config import (
    BASELINE_LOCKED_SETTING,
    ENV_BASELINE_LOCKED,
    ENV_CONNECTION,
    ENV_STORE,
    ScherlokConfig,
)
from scherlok.detector.anomaly_types import COLUMN_ADDED, EMPTY_TABLE, VOLUME_DROP
from scherlok.store.sqlite import (
    APPEND_ONLY_ERROR,
    APPROVAL_AFTER_KEY,
    APPROVAL_BEFORE_KEY,
    APPROVAL_COLUMNS,
    APPROVALS_TABLE,
    ProfileStore,
)

_HAS_DUCKDB = importlib.util.find_spec("duckdb") is not None
_HAS_MCP = importlib.util.find_spec("mcp") is not None
needs_duckdb = pytest.mark.skipif(not _HAS_DUCKDB, reason="duckdb not installed")

runner = CliRunner(env={"NO_COLOR": "1"})

APPROVER = "reviewer@example.invalid"
REASON = "Planned purge of test orders"
TABLE = "orders"
NEW_TABLE = "refunds"
STORE_FILENAME = "profiles.db"
SECRET = "S3CRET-t0ken"
SENTINEL_EMAIL = "sentinel.person@example.invalid"
SENTINEL_AMOUNT = 868686.375
BASELINE_ROWS = 1000
BROKEN_ROWS = 300  # a 70% drop: CRITICAL
WARNING_ROWS = 750  # a 25% drop: WARNING
REMOTE_STORE = "s3://example-bucket/scherlok/profiles.db"
SCOPE = "postgresql://db.example.com/shop"
OTHER_SCOPE = "postgresql://staging.example.com/shop"
LOCKED = "1"


@pytest.fixture(autouse=True)
def fixed_approver(monkeypatch):
    monkeypatch.setattr("scherlok.cli.approver", lambda: APPROVER)


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
    path = tmp_path / "warehouse.duckdb"
    _execute(
        path,
        "CREATE TABLE orders (id INTEGER, amount DOUBLE, email VARCHAR)",
        f"INSERT INTO orders SELECT i, {SENTINEL_AMOUNT}, '{SENTINEL_EMAIL}' "
        f"FROM range(1, {BASELINE_ROWS + 1}) t(i)",
    )
    # The DuckDB connector ignores the query string, so the token is inert
    # here but still part of the connection string no approval may hold.
    monkeypatch.setenv(ENV_CONNECTION, f"duckdb:///{path}?motherduck_token={SECRET}")
    return path


def _keep_rows(path: Path, rows: int) -> None:
    _execute(path, f"DELETE FROM orders WHERE id > {rows}")


def _invoke(*args: str, input: str | None = None):
    result = runner.invoke(app, list(args), input=input)
    assert result.exception is None or isinstance(result.exception, SystemExit), result.output
    return result


def _accept(*args: str, input: str | None = None):
    return _invoke("accept", "--table", TABLE, "--reason", REASON, *args, input=input)


def _store(home: Path) -> ProfileStore:
    return ProfileStore(db_path=home / STORE_FILENAME)


def _rows(home: Path, sql: str) -> list[tuple]:
    conn = sqlite3.connect(home / STORE_FILENAME)
    try:
        return conn.execute(sql).fetchall()
    finally:
        conn.close()


def _count(home: Path, table: str) -> int:
    return _rows(home, f"SELECT COUNT(*) FROM {table}")[0][0]


def _learned_rows(home: Path, table: str = TABLE) -> int | None:
    profile = _store(home).get_latest_profile(table, "volume")
    return profile["row_count"] if profile else None


# ----- accept -------------------------------------------------------------------

@needs_duckdb
def test_accept_makes_the_new_state_the_baseline_and_the_alert_stops(
    warehouse, isolated_scherlok_dir
):
    _invoke("investigate")
    _keep_rows(warehouse, BROKEN_ROWS)
    assert [_invoke("watch").exit_code for _ in range(2)] == [1, 1]  # #116: not learned

    accepted = _accept("--yes")
    after = _invoke("watch")

    assert accepted.exit_code == 0, accepted.output
    assert after.exit_code == 0 and "No anomalies detected" in after.stdout
    assert _learned_rows(isolated_scherlok_dir) == BROKEN_ROWS
    [approval] = _store(isolated_scherlok_dir).get_approvals()
    assert tuple(approval) == APPROVAL_COLUMNS
    assert approval["table_name"] == TABLE
    assert approval["approved_by"] == APPROVER
    assert approval["reason"] == REASON
    assert approval[APPROVAL_BEFORE_KEY]["row_count"] == BASELINE_ROWS
    assert approval[APPROVAL_AFTER_KEY]["row_count"] == BROKEN_ROWS
    drop = next(a for a in approval[APPROVAL_AFTER_KEY]["anomalies"] if a["type"] == VOLUME_DROP)
    assert drop["severity"] == "CRITICAL"
    assert drop["metric"] == "row_count" and drop["current"] == BROKEN_ROWS


@needs_duckdb
def test_accept_shows_the_summary_and_writes_nothing_when_declined(
    warehouse, isolated_scherlok_dir
):
    _invoke("investigate")
    _keep_rows(warehouse, BROKEN_ROWS)
    _execute(warehouse, "ALTER TABLE orders ADD COLUMN note VARCHAR")
    profiles_before = _count(isolated_scherlok_dir, "profiles")

    declined = _accept(input="n\n")

    assert declined.exit_code == 1
    assert "1,000 → 300" in declined.stdout
    assert "3 → 4" in declined.stdout  # columns: id, amount, email, + note
    assert VOLUME_DROP in declined.stdout and f"{COLUMN_ADDED} note" in declined.stdout
    assert REASON in declined.stdout and APPROVER in declined.stdout
    assert _count(isolated_scherlok_dir, "profiles") == profiles_before
    assert _count(isolated_scherlok_dir, APPROVALS_TABLE) == 0

    confirmed = _accept(input="y\n")

    assert confirmed.exit_code == 0
    assert _count(isolated_scherlok_dir, APPROVALS_TABLE) == 1


@needs_duckdb
@pytest.mark.parametrize(
    ("args", "exit_code"),
    [
        (("--table", TABLE), 2),  # no --reason
        (("--table", TABLE, "--reason", "   "), 1),  # a blank one
        (("--reason", REASON), 2),  # no --table
        (("--table", "ghost", "--reason", REASON, "--yes"), 1),  # unknown table
    ],
)
def test_accept_refuses_and_writes_nothing(warehouse, isolated_scherlok_dir, args, exit_code):
    _invoke("investigate")
    _keep_rows(warehouse, BROKEN_ROWS)
    profiles_before = _count(isolated_scherlok_dir, "profiles")

    result = _invoke("accept", *args)

    assert result.exit_code == exit_code
    assert _count(isolated_scherlok_dir, "profiles") == profiles_before
    assert _count(isolated_scherlok_dir, APPROVALS_TABLE) == 0
    assert _learned_rows(isolated_scherlok_dir) == BASELINE_ROWS


@needs_duckdb
def test_accept_sets_the_first_baseline_of_each_new_table(warehouse, isolated_scherlok_dir):
    _execute(warehouse, "CREATE TABLE refunds (id INTEGER)")

    result = _invoke(
        "accept", "--table", TABLE, "--table", NEW_TABLE, "--reason", REASON, "--yes"
    )

    assert result.exit_code == 0
    assert "no baseline → 1,000" in result.stdout and "no baseline → 0" in result.stdout
    approvals_by_table = {
        a["table_name"]: a for a in _store(isolated_scherlok_dir).get_approvals()
    }
    assert set(approvals_by_table) == {TABLE, NEW_TABLE}
    assert all(a[APPROVAL_BEFORE_KEY] is None for a in approvals_by_table.values())
    refunds_findings = approvals_by_table[NEW_TABLE][APPROVAL_AFTER_KEY]["anomalies"]
    assert [a["type"] for a in refunds_findings] == [EMPTY_TABLE]
    assert _learned_rows(isolated_scherlok_dir, NEW_TABLE) == 0


@needs_duckdb
def test_no_connection_string_or_column_value_reaches_an_approval(
    warehouse, isolated_scherlok_dir
):
    connection = f"duckdb:///{warehouse}?motherduck_token={SECRET}"
    _invoke("investigate")
    _execute(warehouse, "UPDATE orders SET email = NULL WHERE id > 100")  # NULL surge
    _keep_rows(warehouse, BROKEN_ROWS)
    _accept("--yes")

    stored = _rows(isolated_scherlok_dir, f"SELECT * FROM {APPROVALS_TABLE}")
    listed = _invoke("history", "--approvals", "--output", "json").stdout

    assert len(stored) == 1
    for text in (json.dumps(stored), listed):
        assert SECRET not in text
        assert connection not in text
        assert SENTINEL_EMAIL not in text
        assert str(SENTINEL_AMOUNT) not in text


@needs_duckdb
def test_accept_uploads_a_remote_store_once_and_never_when_declined(
    warehouse, isolated_scherlok_dir, tmp_path, monkeypatch
):
    _invoke("investigate")
    remote_copy = tmp_path / "remote-profiles.db"
    shutil.move(isolated_scherlok_dir / STORE_FILENAME, remote_copy)
    _keep_rows(warehouse, BROKEN_ROWS)
    download = MagicMock(side_effect=lambda _url, local: shutil.copy(remote_copy, local))
    upload = MagicMock(side_effect=lambda local, _url: shutil.copy(local, remote_copy))
    monkeypatch.setenv(ENV_STORE, REMOTE_STORE)

    with (
        patch("scherlok.store.remote.download", download),
        patch("scherlok.store.remote.upload", upload),
    ):
        declined = _accept(input="n\n")
        uploads_after_decline = upload.call_count
        accepted = _accept("--yes")

    assert declined.exit_code == 1 and uploads_after_decline == 0
    assert accepted.exit_code == 0 and upload.call_count == 1
    assert len(ProfileStore(db_path=remote_copy).get_approvals()) == 1


# ----- approver -------------------------------------------------------------------

def _git_result(returncode: int, stdout: str) -> subprocess.CompletedProcess:
    return subprocess.CompletedProcess(approvals.GIT_USER_EMAIL_COMMAND, returncode, stdout, "")


@pytest.mark.parametrize(
    ("git", "user", "expected"),
    [
        (_git_result(0, f"{APPROVER}\n"), "someone", APPROVER),
        (_git_result(1, ""), "someone", "someone"),
        (_git_result(0, "  \n"), "someone", "someone"),
        (FileNotFoundError("git"), "someone", "someone"),
        (subprocess.TimeoutExpired("git", 5), None, approvals.UNKNOWN_APPROVER),
    ],
)
def test_approver_prefers_git_email_then_user(monkeypatch, git, user, expected):
    run = MagicMock(side_effect=git) if isinstance(git, Exception) else MagicMock(return_value=git)
    monkeypatch.setattr(approvals.subprocess, "run", run)
    if user is None:
        monkeypatch.delenv(approvals.ENV_USER, raising=False)
    else:
        monkeypatch.setenv(approvals.ENV_USER, user)

    assert approvals.approver() == expected
    assert run.call_args.args[0] == approvals.GIT_USER_EMAIL_COMMAND


# ----- the approvals table ----------------------------------------------------------

def _approval(table: str = TABLE, scope: str | None = SCOPE) -> dict:
    return approvals.ApprovalRecord(
        table_name=table,
        scope=scope,
        approved_by=APPROVER,
        reason=REASON,
        before_summary=None,
        after_summary={"row_count": 1, "column_count": 1, "anomalies": []},
    ).to_dict()


@pytest.mark.parametrize(
    "statement", [f"UPDATE {APPROVALS_TABLE} SET reason = 'x'", f"DELETE FROM {APPROVALS_TABLE}"]
)
def test_approvals_are_append_only(tmp_path, statement):
    path = tmp_path / STORE_FILENAME
    store = ProfileStore(db_path=path)
    store.save_approval(_approval())
    store.close()

    conn = sqlite3.connect(path)
    try:
        with pytest.raises(sqlite3.IntegrityError, match=APPEND_ONLY_ERROR):
            conn.execute(statement)
    finally:
        conn.close()
    assert len(ProfileStore(db_path=path).get_approvals()) == 1


def test_the_store_refuses_an_approval_without_a_reason(tmp_path):
    store = ProfileStore(db_path=tmp_path / STORE_FILENAME)

    with pytest.raises(sqlite3.IntegrityError):
        store.save_approval({**_approval(), "reason": "  "})


def test_approvals_are_kept_per_connection(tmp_path):
    path = tmp_path / STORE_FILENAME
    for scope in (SCOPE, OTHER_SCOPE):
        ProfileStore(db_path=path, scope=scope).save_approval(_approval(scope=scope))

    scoped = ProfileStore(db_path=path, scope=SCOPE).get_approvals()

    assert [a["scope"] for a in scoped] == [SCOPE]
    assert len(ProfileStore(db_path=path).get_approvals()) == 2


@needs_duckdb
def test_history_lists_approvals_in_text_and_json(warehouse, isolated_scherlok_dir):
    _invoke("investigate")
    _keep_rows(warehouse, BROKEN_ROWS)
    _accept("--yes")

    as_json = _invoke("history", "--approvals", "--output", "json")
    as_text = _invoke("history", "--approvals")

    [record] = json.loads(as_json.stdout)
    assert tuple(record) == APPROVAL_COLUMNS
    assert record["reason"] == REASON
    assert "Baseline approvals (last 30 days)" in as_text.stdout
    line = next(line for line in as_text.stdout.splitlines() if TABLE in line)
    assert "1,000 → 300" in line and REASON[:10] in line
    assert APPROVER.split("@")[0] in line  # folded, never cut


def test_history_approvals_when_there_are_none_and_with_runs(isolated_scherlok_dir):
    assert json.loads(_invoke("history", "--approvals", "--output", "json").stdout) == []
    assert "No baseline approvals in the last 7 days" in _invoke(
        "history", "--approvals", "--days", "7"
    ).stdout
    both = _invoke("history", "--runs", "--approvals")
    assert both.exit_code == 1 and "not both" in both.output


# ----- locked mode: the CLI -----------------------------------------------------------

@pytest.mark.parametrize(
    ("env_value", "saved", "locked"),
    [
        (None, False, False),
        (None, True, True),
        ("1", False, True),
        ("true", False, True),
        ("anything", False, True),  # a typo fails closed
        ("0", False, False),
        ("off", False, False),
        ("", False, False),
        ("0", True, True),  # the variable can't unlock a saved lock
    ],
)
def test_baseline_locked_from_env_and_config(monkeypatch, env_value, saved, locked):
    if env_value is not None:
        monkeypatch.setenv(ENV_BASELINE_LOCKED, env_value)
    cfg = ScherlokConfig(settings={**config.DEFAULT_SETTINGS, BASELINE_LOCKED_SETTING: saved})

    assert cfg.baseline_locked() is locked


def test_config_locks_and_unlocks_baselines(isolated_scherlok_dir, monkeypatch):
    locked = _invoke("config", "--baseline-locked")
    shown = _invoke("config")

    assert locked.exit_code == 0 and ScherlokConfig.load().baseline_locked() is True
    assert "Baselines locked: yes" in shown.stdout

    monkeypatch.setenv(ENV_BASELINE_LOCKED, LOCKED)
    unlocked = _invoke("config", "--no-baseline-locked")

    assert ScherlokConfig.load().settings[BASELINE_LOCKED_SETTING] is False
    assert f"{ENV_BASELINE_LOCKED} still locks" in unlocked.stdout


@needs_duckdb
def test_locked_watch_ci_and_check_compare_without_saving_profiles(
    warehouse, isolated_scherlok_dir, monkeypatch
):
    monkeypatch.setenv(ENV_BASELINE_LOCKED, LOCKED)
    connection = f"duckdb:///{warehouse}"

    first_sighting = [_invoke("watch"), _invoke("ci", connection), _invoke("check", connection)]

    assert [r.exit_code for r in first_sighting] == [0, 0, 0]
    assert _count(isolated_scherlok_dir, "profiles") == 0  # not even the first baseline
    assert _count(isolated_scherlok_dir, "runs") == 3  # runs are still recorded


@needs_duckdb
def test_locked_investigate_refuses_and_accept_still_writes(
    warehouse, isolated_scherlok_dir, monkeypatch
):
    monkeypatch.setenv(ENV_BASELINE_LOCKED, LOCKED)

    refused = _invoke("investigate")
    accepted = _accept("--yes")
    _keep_rows(warehouse, BROKEN_ROWS)
    alerts = [_invoke("watch") for _ in range(2)]

    assert refused.exit_code == 1 and "Baselines are locked" in refused.output
    assert accepted.exit_code == 0
    assert _learned_rows(isolated_scherlok_dir) == BASELINE_ROWS
    assert [r.exit_code for r in alerts] == [1, 1]
    assert _count(isolated_scherlok_dir, "anomalies") >= 2  # alerts are still saved


@pytest.mark.parametrize(("env_value", "persist"), [(LOCKED, False), ("0", True)])
def test_dbt_compares_without_saving_when_locked(monkeypatch, env_value, persist):
    monkeypatch.setenv(ENV_BASELINE_LOCKED, env_value)
    project = Path(__file__).parent / "fixtures" / "dbt" / "jaffle_shop_postgres"
    fake = MagicMock()
    fake.connect.return_value = True
    fake.list_tables.return_value = ["stg_orders", "fct_orders"]

    with (
        patch("scherlok.cli.get_connector", return_value=fake),
        patch("scherlok.cli._watch_table", return_value=([], {"row_count": 1})) as watch_table,
    ):
        result = _invoke(
            "dbt", "--project-dir", str(project),
            "--connection-string", "postgresql://u:p@host/db",
        )

    assert result.exit_code == 0
    assert watch_table.call_count == 2
    assert {c.kwargs["persist"] for c in watch_table.call_args_list} == {persist}


# ----- locked mode: the MCP server -------------------------------------------------------

@pytest.fixture()
def wired(monkeypatch):
    """The MCP server on an in-memory DuckDB table and the scratch profile store."""
    if not _HAS_DUCKDB:
        pytest.skip("duckdb not installed")
    from scherlok.connectors.duckdb import DuckDBConnector
    from scherlok.mcp import server

    connector = DuckDBConnector("duckdb:///:memory:")
    assert connector.connect() is True
    connector._conn.execute("CREATE TABLE orders (id INTEGER)")
    connector._conn.execute(f"INSERT INTO orders SELECT i FROM range(1, {BASELINE_ROWS + 1}) t(i)")
    monkeypatch.setenv(ENV_CONNECTION, "duckdb:///:memory:")
    monkeypatch.setattr(server, "get_connector", lambda _cs: connector)
    monkeypatch.setattr(connector, "connect", lambda: True)
    return server, connector


def _profile_rows(home: Path) -> list[tuple]:
    if not (home / STORE_FILENAME).exists():
        return []
    return _rows(home, "SELECT * FROM profiles ORDER BY id")


def test_locked_mcp_investigate_watch_and_check_leave_profiles_unchanged(
    wired, isolated_scherlok_dir, monkeypatch
):
    server, connector = wired
    server.investigate()  # unlocked: the baseline
    before = _profile_rows(isolated_scherlok_dir)
    monkeypatch.setenv(ENV_BASELINE_LOCKED, LOCKED)
    connector._conn.execute(f"DELETE FROM orders WHERE id > {WARNING_ROWS}")

    with pytest.raises(server.BaselineLockedError, match="scherlok accept"):
        server.investigate()
    watched = server.watch()
    checked = server.check(fail_on="warning")

    assert watched["warning"] >= 1  # still compares and reports
    assert checked["passed"] is False
    assert _profile_rows(isolated_scherlok_dir) == before
    assert server.watch()["warning"] >= 1  # the WARNING was not learned either


def test_locked_mcp_watch_saves_no_first_baseline(wired, isolated_scherlok_dir, monkeypatch):
    server, _connector = wired
    monkeypatch.setenv(ENV_BASELINE_LOCKED, LOCKED)

    server.watch()
    server.check()

    assert _profile_rows(isolated_scherlok_dir) == []
    assert server.verify()["tables_without_baseline"] == [TABLE]


def test_unlocked_mcp_matches_the_default(wired, isolated_scherlok_dir, monkeypatch):
    server, connector = wired
    monkeypatch.setenv(ENV_BASELINE_LOCKED, "0")
    server.investigate()

    connector._conn.execute(f"DELETE FROM orders WHERE id > {WARNING_ROWS}")
    warned = [server.watch()["warning"] for _ in range(2)]
    connector._conn.execute(f"DELETE FROM orders WHERE id > {BROKEN_ROWS}")
    critical = [server.check()["passed"] for _ in range(2)]

    assert warned[0] >= 1 and warned[1] == 0  # a WARNING is learned
    assert critical == [False, False]  # a CRITICAL is not (#116)


def test_accept_is_not_an_mcp_tool():
    from scherlok.mcp.server import _TOOLS

    assert "accept" not in {tool.__name__ for tool in _TOOLS}


@pytest.mark.skipif(not _HAS_MCP, reason="mcp not installed")
def test_accept_is_not_registered_on_the_server():
    import asyncio

    from scherlok.mcp.server import build_server

    names = {tool.name for tool in asyncio.run(build_server().list_tools())}
    assert names and not any("accept" in name for name in names)
