"""Tests for the Claude Code PostToolUse hook in integrations/claude-code (#113).

The hook is a standalone script, so it is loaded from its path. Most tests
drive `handle()` with a fake command runner; the end-to-end tests pipe the
JSON Claude Code sends for a Bash call into the script and let it run the
real `scherlok` CLI against a DuckDB file and dbt artifacts written here,
the state `dbt run` leaves behind, with HOME pointed at a scratch directory.
"""

from __future__ import annotations

import importlib.util
import json
import os
import shutil
import stat
import subprocess
import sys
import textwrap
import time
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parents[1]
HOOK_PATH = REPO / "integrations" / "claude-code" / "scripts" / "verify_after_dbt.py"

_spec = importlib.util.spec_from_file_location("verify_after_dbt", HOOK_PATH)
hook = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(hook)

SCHERLOK_BIN = "/opt/fake/bin/scherlok"
CONFIGURED = {"connection_configured": True, "connection_source": "env"}
NOT_CONFIGURED = {"connection_configured": False, "connection_source": None}
SECRET = "hunter2"
PROJECT_NAME = "shop"
MANIFEST_SCHEMA = "https://schemas.getdbt.com/dbt/manifest/v12.json"

FAN_OUT_FINDINGS = [
    {
        "table": "fct_orders", "type": "cardinality_change", "severity": "CRITICAL",
        "message": "Column 'order_id' distinct ratio decreased: 1.000 -> 0.333 (67% change)",
        "column": "order_id", "metric": "distinct_ratio", "baseline": 1.0, "current": 0.333,
        "change_pct": -66.7, "hint": "Check for duplicated keys or a broken join.",
    },
    {
        "table": "fct_orders", "type": "volume_spike", "severity": "WARNING",
        "message": "Row count spiked 200% (40 -> 120)", "column": None, "metric": "row_count",
        "baseline": 40, "current": 120, "change_pct": 200.0,
        "hint": "Check for a join that fans out.",
    },
]


def _payload(passed: bool, anomalies: list[dict] | None = None, **extra) -> dict:
    anomalies = anomalies or []
    counts = {
        name: sum(1 for a in anomalies if a["severity"] == name.upper())
        for name in ("critical", "warning", "info")
    }
    return {
        "passed": passed, "fail_on": "critical",
        "tables_verified": ["stg_orders", "fct_orders"], "tables_without_baseline": [],
        "anomaly_count": len(anomalies), **counts, "anomalies": anomalies,
        "dbt_models_not_found": [], **extra,
    }


def _done(stdout: object = "", returncode: int = 0, stderr: str = ""):
    text = stdout if isinstance(stdout, str) else json.dumps(stdout)
    return subprocess.CompletedProcess([], returncode, stdout=text, stderr=stderr)


class FakeRunner:
    """Answers `scherlok config` and `scherlok verify`; records every call."""

    def __init__(self, config=None, verify=None):
        self.answers = {
            "config": config if config is not None else _done(CONFIGURED),
            "verify": verify if verify is not None else _done(_payload(True)),
        }
        self.calls: list[tuple[list[str], str, dict, float]] = []

    def __call__(self, args, cwd, env, timeout):
        self.calls.append((args, cwd, env, timeout))
        answer = self.answers[args[1]]
        if isinstance(answer, BaseException):
            raise answer
        return answer

    def commands(self) -> list[str]:
        return [args[1] for args, *_ in self.calls]

    def verify_args(self) -> list[str]:
        return next(args for args, *_ in self.calls if args[1] == "verify")


@pytest.fixture
def project(tmp_path) -> Path:
    path = tmp_path / PROJECT_NAME
    path.mkdir()
    (path / hook.DBT_PROJECT_FILE).write_text(f"name: {PROJECT_NAME}\n")
    return path


@pytest.fixture
def env(tmp_path) -> dict[str, str]:
    """An environment whose PATH holds a fake scherlok executable."""
    bin_dir = tmp_path / "bin"
    bin_dir.mkdir()
    fake = bin_dir / "scherlok"
    fake.write_text("#!/bin/sh\nexit 0\n")
    fake.chmod(fake.stat().st_mode | stat.S_IXUSR)
    return {"PATH": str(bin_dir)}


def _event(command: str, cwd: Path, **overrides) -> dict:
    """The JSON Claude Code sends to a PostToolUse hook after a Bash call."""
    event = {
        "session_id": "abc123",
        "transcript_path": "/tmp/transcript.jsonl",
        "cwd": str(cwd),
        "permission_mode": "default",
        "hook_event_name": "PostToolUse",
        "tool_name": "Bash",
        "tool_input": {"command": command, "description": "Build the models"},
        "tool_response": {"stdout": "Done.", "stderr": "", "interrupted": False, "isImage": False},
        "tool_use_id": "toolu_01ABC123",
        "duration_ms": 2100,
    }
    event.update(overrides)
    return event


# ----- which commands fire --------------------------------------------------


@pytest.mark.parametrize(("command", "subcommand"), [
    ("dbt run", "run"),
    ("dbt build --select fct_orders", "build"),
    ("cd shop && dbt run", "run"),
    ("uv run dbt build", "build"),
    ("dbt --debug run", "run"),
    ("dbt --log-format json build", "build"),
    (".venv/bin/dbt run -s fct_orders", "run"),
    ("dbt compile && dbt run; echo done", "run"),
    ("(dbt build)", "build"),
])
def test_dbt_run_and_build_fire(project, command, subcommand):
    assert hook.dbt_subcommand(_event(command, project)) == subcommand


@pytest.mark.parametrize("command", [
    "ls -la",
    "dbt test",
    "dbt seed",
    "dbt compile",
    "dbt run-operation grant_select",
    "scherlok dbt-run-and-watch --project-dir .",
    "cat logs/dbt_run.log",
    "git commit -m 'dbt run fixes'",
    'git commit -m "dbt build tweaks"',
    "mydbt run",
    "dbt runner",
])
def test_other_commands_are_ignored_without_running_anything(project, env, command):
    runner = FakeRunner()
    assert hook.handle(_event(command, project), env, run=runner) is None
    assert runner.calls == []


@pytest.mark.parametrize("overrides", [
    {"hook_event_name": "PreToolUse"},
    {"tool_name": "Write"},
    {"tool_input": {"command": "dbt run", "run_in_background": True}},
    {"tool_input": "dbt run"},
    {"tool_input": {"command": ["dbt", "run"]}},
    {"tool_response": {"stdout": "", "stderr": "", "interrupted": True, "isImage": False}},
])
def test_other_events_and_unfinished_runs_are_ignored(project, env, overrides):
    runner = FakeRunner()
    assert hook.handle(_event("dbt run", project, **overrides), env, run=runner) is None
    assert runner.calls == []


# ----- no-ops: not installed, not configured, too old ---------------------


def test_no_scherlok_on_path_is_a_no_op(project, tmp_path):
    runner = FakeRunner()
    empty = tmp_path / "empty"
    empty.mkdir()
    assert hook.handle(_event("dbt run", project), {"PATH": str(empty)}, run=runner) is None
    assert runner.calls == []


def test_a_missing_scherlok_bin_is_a_no_op(project, env, tmp_path):
    runner = FakeRunner()
    env = {**env, hook.ENV_BIN: str(tmp_path / "nowhere" / "scherlok")}
    assert hook.handle(_event("dbt run", project), env, run=runner) is None
    assert runner.calls == []


def test_scherlok_bin_overrides_path(project, env, tmp_path):
    custom = tmp_path / "venv" / "scherlok"
    custom.parent.mkdir()
    shutil.copy(Path(env["PATH"]) / "scherlok", custom)
    runner = FakeRunner()
    hook.handle(_event("dbt run", project), {**env, hook.ENV_BIN: str(custom)}, run=runner)
    assert {args[0] for args, *_ in runner.calls} == {str(custom)}


def test_no_connection_is_a_no_op(project, env):
    runner = FakeRunner(config=_done(NOT_CONFIGURED))
    assert hook.handle(_event("dbt run", project), env, run=runner) is None
    assert runner.commands() == ["config"]


@pytest.mark.parametrize("probe", [
    _done("", returncode=2, stderr="No such option: --output"),  # predates config --output
    _done("not json"),
    _done(["a", "list"]),
    _done({"connection_configured": "yes"}),
])
def test_a_scherlok_that_cannot_say_it_is_configured_is_a_no_op(project, env, probe):
    runner = FakeRunner(config=probe)
    assert hook.handle(_event("dbt run", project), env, run=runner) is None
    assert runner.commands() == ["config"]


@pytest.mark.parametrize("error", [
    subprocess.TimeoutExpired(["scherlok"], 30), OSError("exec format error"),
])
def test_a_probe_that_fails_is_a_no_op(project, env, error):
    runner = FakeRunner(config=error)
    assert hook.handle(_event("dbt run", project), env, run=runner) is None


# ----- verdicts -------------------------------------------------------------


def test_failed_verify_hands_claude_the_findings(project, env):
    runner = FakeRunner(verify=_done(_payload(False, FAN_OUT_FINDINGS), returncode=1))
    answer = hook.handle(_event("dbt run", project), env, run=runner)

    assert answer["decision"] == "block"
    reason = answer["reason"]
    assert reason.startswith("Scherlok verify failed after `dbt run` (1 critical, 1 warning")
    critical, warning = reason.splitlines()[1:3]
    assert critical.startswith("- CRITICAL fct_orders.order_id (cardinality_change): Column")
    assert critical.endswith("Hint: Check for duplicated keys or a broken join.")
    assert warning.startswith("- WARNING fct_orders (volume_spike): Row count spiked 200%")
    assert "scherlok reconcile" in reason
    assert "(`scherlok accept`) is a decision for a person" in reason
    assert "verify failed" in answer["systemMessage"]
    assert "hookSpecificOutput" not in answer
    json.dumps(answer)


def test_findings_come_most_severe_first(project, env):
    reversed_findings = list(reversed(FAN_OUT_FINDINGS))
    runner = FakeRunner(verify=_done(_payload(False, reversed_findings), returncode=1))
    reason = hook.handle(_event("dbt run", project), env, run=runner)["reason"]
    assert reason.splitlines()[1].startswith("- CRITICAL")


def test_passed_verify_adds_a_summary_for_claude(project, env):
    runner = FakeRunner()
    answer = hook.handle(_event("dbt build", project), env, run=runner)

    assert set(answer) == {"hookSpecificOutput"}
    output = answer["hookSpecificOutput"]
    assert output["hookEventName"] == "PostToolUse"
    assert output["additionalContext"] == (
        "Scherlok verify passed after `dbt build`: 2 of 2 table(s) built by the run compared "
        "with their baselines (0 critical, 0 warning, 0 info; fail-on critical)."
    )


def test_a_pass_still_lists_warnings_and_what_was_not_checked(project, env):
    payload = _payload(
        True, FAN_OUT_FINDINGS[1:],
        tables_without_baseline=["dim_regions"], dbt_models_not_found=["int_orders"],
    )
    runner = FakeRunner(verify=_done(payload))
    context = hook.handle(_event("dbt run", project), env, run=runner)[
        "hookSpecificOutput"]["additionalContext"]

    assert "- WARNING fct_orders (volume_spike)" in context
    assert "only the new-table checks ran (empty table, all-NULL column, duplicate key): " \
        "dim_regions." in context
    assert "not visible on the Scherlok connection, so not checked: int_orders." in context


def test_a_pass_without_baselines_does_not_claim_a_comparison(project, env):
    payload = _payload(
        True, tables_verified=[], tables_without_baseline=["stg_orders", "fct_orders"]
    )
    runner = FakeRunner(verify=_done(payload))
    context = hook.handle(_event("dbt run", project), env, run=runner)[
        "hookSpecificOutput"]["additionalContext"]
    assert context.startswith(
        "Scherlok verify passed after `dbt run`: 0 of 2 table(s) built by the run compared"
    )
    assert "only the new-table checks ran" in context


def test_a_run_that_built_nothing_says_nothing_was_checked(project, env):
    payload = _payload(True, tables_verified=[], tables_without_baseline=[])
    runner = FakeRunner(verify=_done(payload))
    context = hook.handle(_event("dbt run", project), env, run=runner)[
        "hookSpecificOutput"]["additionalContext"]
    assert context == "Scherlok verify checked no table after `dbt run`."


def test_findings_are_capped_below_the_context_limit(project, env):
    many = [dict(FAN_OUT_FINDINGS[0], message="m" * 400) for _ in range(60)]
    runner = FakeRunner(verify=_done(_payload(False, many), returncode=1))
    reason = hook.handle(_event("dbt run", project), env, run=runner)["reason"]

    finding_lines = [line for line in reason.splitlines() if line.startswith("- CRITICAL")]
    assert len(finding_lines) == hook.MAX_FINDINGS
    assert all(len(line) <= hook.MAX_LINE_CHARS for line in finding_lines)
    assert f"- {60 - hook.MAX_FINDINGS} more; `scherlok verify --dbt-project" in reason
    assert reason.endswith(hook.FAILED_FOOTER)
    assert len(reason) <= hook.MAX_TEXT_CHARS < 10_000


# ----- the check could not run: a note, never silence -----------------------


def test_a_verify_timeout_returns_a_note(project, env):
    runner = FakeRunner(verify=subprocess.TimeoutExpired(["scherlok", "verify"], 150))
    answer = hook.handle(_event("dbt run", project), env, run=runner)

    note = "Scherlok did not verify the data after `dbt run`: `scherlok verify` did not " \
        "finish within 150 s."
    assert answer["systemMessage"] == note
    assert answer["hookSpecificOutput"]["additionalContext"] == note
    assert "decision" not in answer


def test_verify_gets_what_is_left_of_the_budget(project, env):
    ticks = iter([0.0, 0.0, 40.0])  # deadline, probe start, verify start
    runner = FakeRunner()
    env = {**env, hook.ENV_TIMEOUT: "100"}
    hook.handle(_event("dbt run", project), env, run=runner, clock=lambda: next(ticks))
    (_, _, _, probe_timeout), (_, _, _, verify_timeout) = runner.calls
    assert probe_timeout == hook.PROBE_TIMEOUT_SECONDS
    assert verify_timeout == 60.0


def test_an_exhausted_budget_returns_a_note_without_running_verify(project, env):
    ticks = iter([0.0, 0.0, 200.0])
    runner = FakeRunner()
    answer = hook.handle(_event("dbt run", project), env, run=runner, clock=lambda: next(ticks))
    assert runner.commands() == ["config"]
    assert "did not finish within 150 s" in answer["systemMessage"]


@pytest.mark.parametrize("value", ["soon", "-5", "0", "nan", "inf"])
def test_a_bad_timeout_setting_falls_back_to_the_default(value):
    assert hook._timeout_seconds({hook.ENV_TIMEOUT: value}) == hook.DEFAULT_TIMEOUT_SECONDS


def test_a_verify_error_returns_a_note_without_credentials(project, env):
    stderr = (
        "\x1b[31mFailed to connect to the database.\x1b[0m\n"
        f"  could not reach postgresql://admin:{SECRET}@db.internal/shop\n"
        "  Hint: check the host\n"
    )
    runner = FakeRunner(verify=_done("", returncode=1, stderr=stderr))
    answer = hook.handle(_event("dbt run", project), env, run=runner)

    note = answer["hookSpecificOutput"]["additionalContext"]
    assert note == (
        "Scherlok did not verify the data after `dbt run`: Failed to connect to the database. "
        "could not reach postgresql://***@db.internal/shop."
    )
    assert SECRET not in json.dumps(answer)


def test_a_silent_verify_failure_still_returns_a_note(project, env):
    runner = FakeRunner(verify=_done("", returncode=3))
    answer = hook.handle(_event("dbt run", project), env, run=runner)
    assert "exited with code 3 and no result" in answer["systemMessage"]


def test_a_verify_that_cannot_start_returns_a_note(project, env):
    runner = FakeRunner(verify=PermissionError("denied"))
    answer = hook.handle(_event("dbt run", project), env, run=runner)
    assert "could not start (PermissionError)" in answer["systemMessage"]


# ----- finding the dbt project, as dbt does --------------------------------


def test_verify_is_scoped_to_the_project_and_target_of_the_run(project, env):
    runner = FakeRunner()
    hook.handle(_event("dbt run", project), env, run=runner)
    args = runner.verify_args()
    assert args[:4] == [str(Path(env["PATH"]) / "scherlok"), "verify", "--output", "json"]
    assert args[4:] == ["--dbt-project", str(project), "--dbt-target-path", "target"]


def test_the_project_is_found_in_a_parent_directory(project, env):
    models = project / "models" / "marts"
    models.mkdir(parents=True)
    runner = FakeRunner()
    hook.handle(_event("dbt run", models), env, run=runner)
    assert runner.verify_args()[5] == str(project)


@pytest.mark.parametrize("flag", ["--project-dir {}", "--project-dir={}"])
def test_project_dir_flag_wins(project, env, flag):
    runner = FakeRunner()
    command = f"dbt run {flag.format(PROJECT_NAME)} --select fct_orders"
    hook.handle(_event(command, project.parent), env, run=runner)
    assert runner.verify_args()[5] == str(project)


def test_dbt_project_dir_variable_is_honoured(project, env, tmp_path):
    elsewhere = tmp_path / "elsewhere"
    elsewhere.mkdir()
    runner = FakeRunner()
    env = {**env, hook.ENV_DBT_PROJECT_DIR: str(project)}
    hook.handle(_event("dbt build", elsewhere), env, run=runner)
    assert runner.verify_args()[5] == str(project)


@pytest.mark.parametrize(("command", "variables", "expected"), [
    ("dbt run --target-path build/dbt", {}, "build/dbt"),
    ("dbt run --target-path=/abs/target", {}, "/abs/target"),
    ("dbt run", {"DBT_TARGET_PATH": "custom"}, "custom"),
    ("dbt run --target-path flag", {"DBT_TARGET_PATH": "variable"}, "flag"),
])
def test_target_path_follows_dbt(project, env, command, variables, expected):
    runner = FakeRunner()
    hook.handle(_event(command, project), {**env, **variables}, run=runner)
    assert runner.verify_args()[-1] == str(Path(expected))


def test_no_dbt_project_returns_a_note(env, tmp_path):
    runner = FakeRunner()
    answer = hook.handle(_event("dbt run", tmp_path), env, run=runner)
    assert runner.commands() == ["config"]
    assert "could not find the dbt project (dbt_project.yml)" in answer["systemMessage"]


def test_a_project_dir_without_dbt_project_yml_returns_a_note(env, tmp_path):
    (tmp_path / "not_dbt").mkdir()
    runner = FakeRunner()
    answer = hook.handle(_event("dbt run --project-dir not_dbt", tmp_path), env, run=runner)
    assert "could not find the dbt project" in answer["systemMessage"]


# ----- the script as Claude Code runs it ------------------------------------


def _run_hook(stdin: str, env: dict[str, str], timeout: float = 120) -> subprocess.CompletedProcess:
    return subprocess.run(
        [sys.executable, str(HOOK_PATH)], input=stdin, capture_output=True, text=True,
        env=env, timeout=timeout, check=False,
    )


@pytest.mark.parametrize("stdin", ["", "not json", "[1, 2]", '{"tool_name": 5}'])
def test_the_script_exits_0_silently_on_bad_input(stdin, tmp_path):
    result = _run_hook(stdin, {"PATH": str(tmp_path)})
    assert (result.returncode, result.stdout) == (0, "")


def test_the_script_exits_0_silently_when_scherlok_is_missing(project, tmp_path):
    result = _run_hook(json.dumps(_event("dbt run", project)), {"PATH": str(tmp_path)})
    assert (result.returncode, result.stdout, result.stderr) == (0, "", "")


def _fake_scherlok(path: Path, verify_seconds: float) -> Path:
    """A scherlok stand-in that is configured but whose verify hangs."""
    path.write_text(textwrap.dedent(f"""\
        #!{sys.executable}
        import json, sys, time
        if sys.argv[1] == "config":
            print(json.dumps({CONFIGURED!r}))
        else:
            time.sleep({verify_seconds})
    """))
    path.chmod(path.stat().st_mode | stat.S_IXUSR)
    return path


@pytest.mark.skipif(os.name != "posix", reason="shebang scripts")
def test_the_script_kills_a_hanging_verify_and_says_so(project, tmp_path):
    fake = _fake_scherlok(tmp_path / "scherlok", verify_seconds=30)
    env = {"PATH": os.environ.get("PATH", ""), hook.ENV_BIN: str(fake), hook.ENV_TIMEOUT: "2"}
    started = time.monotonic()
    result = _run_hook(json.dumps(_event("dbt run", project)), env)

    assert time.monotonic() - started < 20
    assert result.returncode == 0
    assert "did not finish within 2 s" in json.loads(result.stdout)["systemMessage"]


# ----- end to end: real scherlok, DuckDB, the artifacts of a dbt run --------


def _scherlok_executable() -> Path | None:
    candidate = Path(sys.executable).parent / ("scherlok.exe" if os.name == "nt" else "scherlok")
    return candidate if candidate.is_file() else None


needs_cli = pytest.mark.skipif(
    importlib.util.find_spec("duckdb") is None or _scherlok_executable() is None,
    reason="needs duckdb and the scherlok entry point next to this Python",
)


def _write_dbt_artifacts(project: Path, models: dict[str, str]) -> None:
    """manifest.json and run_results.json as `dbt run` writes them, for a DuckDB project."""
    target = project / "target"
    target.mkdir(exist_ok=True)
    nodes = {
        f"model.{PROJECT_NAME}.{name}": {
            "resource_type": "model", "name": name, "alias": name, "database": "warehouse",
            "schema": "main", "config": {"materialized": "table"},
        }
        for name in models
    }
    manifest = {
        "metadata": {"dbt_schema_version": MANIFEST_SCHEMA, "adapter_type": "duckdb"},
        "nodes": nodes, "sources": {},
    }
    results = [
        {"unique_id": f"model.{PROJECT_NAME}.{name}", "status": status}
        for name, status in models.items()
    ]
    (target / "manifest.json").write_text(json.dumps(manifest))
    (target / "run_results.json").write_text(json.dumps({"results": results}))


def _sql(database: Path, *statements: str) -> None:
    import duckdb as duckdb_module

    conn = duckdb_module.connect(str(database))
    try:
        for statement in statements:
            conn.execute(statement)
    finally:
        conn.close()


def _build_fct_orders(database: Path, fan_out: bool) -> None:
    """Rebuild fct_orders the way the model would: one row per order, or three."""
    addresses = "range(3)" if fan_out else "range(1)"
    _sql(
        database,
        "CREATE OR REPLACE TABLE fct_orders AS "
        "SELECT o.i AS order_id, o.i % 8 AS customer_id, o.i * 1.5 AS amount "
        f"FROM range(1, 41) o(i), {addresses} a(n)",
    )


@pytest.fixture
def warehouse(project, tmp_path) -> dict[str, str]:
    """A DuckDB warehouse with a baseline, and the env Claude Code would run the hook in."""
    database = tmp_path / "warehouse.duckdb"
    _build_fct_orders(database, fan_out=False)
    _sql(database, "CREATE TABLE raw_orders AS SELECT i AS order_id FROM range(1, 41) t(i)")
    home = tmp_path / "home"
    home.mkdir()
    env = {
        "PATH": os.environ.get("PATH", ""), "HOME": str(home), "USERPROFILE": str(home),
        "SCHERLOK_CONNECTION": f"duckdb:///{database}",
        hook.ENV_BIN: str(_scherlok_executable()),
    }
    baseline = subprocess.run(
        [env[hook.ENV_BIN], "investigate"], env=env, capture_output=True, text=True, check=False
    )
    assert baseline.returncode == 0, baseline.stderr
    env["DATABASE"] = str(database)
    return env


@needs_cli
def test_end_to_end_fan_out_then_fix(project, warehouse):
    database = Path(warehouse["DATABASE"])
    event = json.dumps(_event("dbt run --select fct_orders", project))

    _build_fct_orders(database, fan_out=True)
    _write_dbt_artifacts(project, {"fct_orders": "success"})
    broken = _run_hook(event, warehouse)
    assert broken.returncode == 0, broken.stderr
    answer = json.loads(broken.stdout)
    assert answer["decision"] == "block"
    assert "- CRITICAL fct_orders.order_id (cardinality_change)" in answer["reason"]
    assert "fct_orders (volume_spike)" in answer["reason"]
    assert "raw_orders" not in answer["reason"]  # not built by the run, not verified

    retry = json.loads(_run_hook(event, warehouse).stdout)
    assert retry["decision"] == "block"  # verify never learned the broken state

    _build_fct_orders(database, fan_out=False)
    fixed = json.loads(_run_hook(event, warehouse).stdout)
    assert fixed["hookSpecificOutput"]["additionalContext"].startswith(
        "Scherlok verify passed after `dbt run`: 1 of 1 table(s) built by the run"
    )


@needs_cli
def test_end_to_end_unconfigured_scherlok_is_silent(project, warehouse, tmp_path):
    _write_dbt_artifacts(project, {"fct_orders": "success"})
    fresh_home = tmp_path / "fresh-home"
    fresh_home.mkdir()
    env = {**warehouse, "HOME": str(fresh_home), "USERPROFILE": str(fresh_home)}
    del env["SCHERLOK_CONNECTION"]
    result = _run_hook(json.dumps(_event("dbt run", project)), env)
    assert (result.returncode, result.stdout) == (0, "")
    assert not (fresh_home / ".scherlok").exists()


@needs_cli
def test_end_to_end_missing_artifacts_return_a_note(project, warehouse):
    answer = json.loads(_run_hook(json.dumps(_event("dbt run", project)), warehouse).stdout)
    assert "manifest.json not found" in answer["hookSpecificOutput"]["additionalContext"]
