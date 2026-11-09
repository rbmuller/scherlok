"""Tests for the GitHub Action (#112): `action.yml` and `action/scherlok_action.py`.

The script is loaded from its file, as the action runs it. The end-to-end
tests run the real `scherlok verify` in a subprocess against a DuckDB file
with synthetic rows, with HOME pointed at the scratch directory from
conftest.py, so they never touch the real ~/.scherlok.
"""

from __future__ import annotations

import importlib.util
import json
import shutil
import sys
import urllib.error
from fnmatch import fnmatchcase
from pathlib import Path
from typing import Any

import pytest
import yaml
from typer.testing import CliRunner

from scherlok.cli import app
from scherlok.service import SECRET_QUERY_KEY_PARTS

REPO = Path(__file__).resolve().parents[1]
SCRIPT = REPO / "action" / "scherlok_action.py"
ACTION_YML = REPO / "action.yml"
# The Marketplace refuses to list an action whose description is longer.
MARKETPLACE_DESCRIPTION_LIMIT = 125


def _load_script():
    spec = importlib.util.spec_from_file_location("scherlok_action", SCRIPT)
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module  # dataclasses resolve annotations through it
    spec.loader.exec_module(module)
    return module


sa = _load_script()

REPOSITORY = "acme/warehouse"
FORK_REPOSITORY = "someone/warehouse"
PR_NUMBER = 7
HEAD_SHA = "0123456789abcdef0123456789abcdef01234567"
TOKEN = "ghs_test_token_value"
CONNECTION = "postgresql://alice:s3cr%2Ft@db.example.com:5432/shop?sslmode=require"

STRUCTURED_ANOMALIES = [
    {
        "table": "orders", "type": "null_rate_change", "severity": "WARNING",
        "message": "Column 'email' NULL rate increased: 0.0% -> 80.0% (Δ80.0%)",
        "column": "email", "metric": "null_rate", "baseline": 0.0, "current": 0.8,
        "change_pct": None, "hint": "Check outer joins.",
    },
    {
        "table": "orders", "type": "volume_drop", "severity": "CRITICAL",
        "message": "Row count dropped 80.0% (500 -> 100)",
        "column": None, "metric": "row_count", "baseline": 500, "current": 100,
        "change_pct": -80.0, "hint": "Rows were deleted or filtered out.",
    },
]
LEGACY_ANOMALIES = [
    {
        "table": "orders", "type": "volume_drop", "severity": "CRITICAL",
        "message": "Row count dropped 80.0% (500 -> 100)",
    },
]


def _payload(anomalies: list[dict], passed: bool = False, **extra: Any) -> dict:
    counts = {name: 0 for name in ("critical", "warning", "info")}
    for anomaly in anomalies:
        counts[anomaly["severity"].lower()] += 1
    return {
        "passed": passed, "fail_on": "critical", "tables_verified": ["orders"],
        "tables_without_baseline": [], "anomaly_count": len(anomalies), **counts,
        "anomalies": anomalies, **extra,
    }


def _render(payload: dict | None, result: str, footer: bool = True) -> str:
    context = sa.ReportContext(run_url="https://github.com/acme/warehouse/actions/runs/1",
                               commit=HEAD_SHA, footer=footer)
    return sa.render_report(payload, result, context)


def _outputs(path: Path) -> dict[str, str]:
    if not path.exists():
        return {}
    return dict(line.split("=", 1) for line in path.read_text().splitlines())


# ----- rendering -----------------------------------------------------------

def test_structured_fields_get_a_column_each():
    body = _render(_payload(STRUCTURED_ANOMALIES), sa.RESULT_FAILED)

    assert "| Severity | Table | Column | Metric | Baseline | Current | Change | Details |" in body
    assert (
        "| 🔴 CRITICAL | `orders` | — | `row_count` | 500 | 100 | -80.0% "
        "| Row count dropped 80.0% (500 -&gt; 100)<br>_Rows were deleted or filtered out._ |"
    ) in body


def test_a_null_rate_is_shown_as_a_percentage():
    body = _render(_payload(STRUCTURED_ANOMALIES), sa.RESULT_FAILED)
    assert "| `email` | `null_rate` | 0.0% | 80.0% | — |" in body


def test_without_structured_fields_the_message_is_shown():
    """A Scherlok without #108 sends four keys; the table falls back to them."""
    body = _render(_payload(LEGACY_ANOMALIES), sa.RESULT_FAILED)

    assert "| Severity | Table | Type | Details |" in body
    assert (
        "| 🔴 CRITICAL | `orders` | `volume_drop` | Row count dropped 80.0% (500 -&gt; 100) |"
    ) in body
    assert "Baseline" not in body


def test_most_severe_anomalies_come_first():
    body = _render(_payload(STRUCTURED_ANOMALIES), sa.RESULT_FAILED)
    assert body.index("🔴 CRITICAL") < body.index("🟡 WARNING")


def test_cells_cannot_break_the_table_or_inject_html():
    anomaly = dict(LEGACY_ANOMALIES[0], table="a|b`c", message="<img src=x>\nnext | line")
    body = _render(_payload([anomaly]), sa.RESULT_FAILED)

    assert "`a\\|b'c`" in body
    assert "&lt;img src=x&gt; next \\| line" in body
    assert "<img" not in body


def test_only_documented_fields_are_rendered():
    """A field verify does not document, such as a sample of rows, never reaches the comment."""
    anomaly = dict(STRUCTURED_ANOMALIES[1], sample_values=["person1@example.com"])
    body = _render(_payload([anomaly], extra_key="SENTINEL-EXTRA"), sa.RESULT_FAILED)

    assert "person1@example.com" not in body
    assert "SENTINEL-EXTRA" not in body


def test_long_lists_are_capped():
    anomalies = [dict(LEGACY_ANOMALIES[0], table=f"t{i}") for i in range(sa.MAX_ANOMALY_ROWS + 5)]
    body = _render(_payload(anomalies), sa.RESULT_FAILED)

    assert body.count("| 🔴 CRITICAL |") == sa.MAX_ANOMALY_ROWS
    assert "5 more not shown" in body


def test_a_pass_lists_the_tables_and_those_without_a_baseline():
    payload = _payload([], passed=True, tables_without_baseline=["refunds"])
    body = _render(payload, sa.RESULT_PASSED)

    assert body.startswith(sa.HEADINGS[sa.RESULT_PASSED])
    assert "1 table verified, 1 without a baseline" in body
    assert "Verified: `orders`" in body
    assert "Not verified, no baseline yet: `refunds`" in body
    assert "| Severity |" not in body


def test_an_error_report_explains_without_details():
    body = _render(None, sa.RESULT_ERROR)
    assert body.startswith(sa.HEADINGS[sa.RESULT_ERROR])
    assert sa.ERROR_TEXT in body


@pytest.mark.parametrize("footer", [True, False])
def test_the_footer_can_be_turned_off(footer):
    body = _render(_payload([], passed=True), sa.RESULT_PASSED, footer=footer)
    assert ("Checked by [Scherlok]" in body) is footer
    assert "[Workflow run](https://github.com/acme/warehouse/actions/runs/1)" in body
    assert "commit `0123456`" in body


def test_the_comment_starts_with_the_hidden_marker():
    body = sa.comment_body("### report")
    assert body.splitlines()[0] == sa.MARKER == "<!-- scherlok-verify -->"


@pytest.mark.parametrize(
    ("passed", "exit_code", "expected"),
    [
        (True, "0", "passed"),
        (False, "1", "failed"),
        (True, "1", "error"),  # verify broke after printing a verdict
        (False, "0", "error"),
        (None, "1", "error"),  # no JSON: a connection error, an unknown table
        (None, "", "error"),   # verify never ran: the install failed
    ],
)
def test_the_result_follows_the_verdict_and_the_exit_code(passed, exit_code, expected):
    payload = None if passed is None else _payload([], passed=passed)
    assert sa.result_for(payload, exit_code) == expected


# ----- masking -----------------------------------------------------------------

def test_the_connection_and_each_credential_are_masked():
    connection = CONNECTION + "&password=qp-secret&api_key=k3y"
    values = sa.secret_values(connection)

    assert connection in values
    assert {"s3cr%2Ft", "s3cr/t", "qp-secret", "k3y"} <= set(values)
    assert not any(value in ("db.example.com", "require", "shop") for value in values)


def test_a_password_that_cuts_the_host_short_is_masked_whole():
    """An unencoded '/' ends the host early; everything before the last '@' is masked."""
    values = sa.secret_values("postgresql://alice:pa/ss@db.example.com/shop")
    assert "alice:pa/ss" in values


def test_an_empty_connection_masks_nothing():
    assert sa.secret_values("") == []


def test_the_secret_query_keys_match_scherlok():
    assert sa.SECRET_QUERY_KEY_PARTS == SECRET_QUERY_KEY_PARTS


def test_workflow_commands_escape_their_data():
    assert sa.workflow_command("add-mask", "a%b\nc") == "::add-mask::a%25b%0Ac"
    assert sa.workflow_command("error", "x", title="a:b,c") == "::error title=a%3Ab%2Cc::x"


# ----- preflight -----------------------------------------------------------------

@pytest.fixture()
def github_env(tmp_path) -> dict[str, str]:
    """The environment a step of the action sees on a same-repository pull request."""
    event = tmp_path / "event.json"
    event.write_text(json.dumps({"pull_request": {
        "number": PR_NUMBER, "head": {"sha": HEAD_SHA, "repo": {"full_name": REPOSITORY}},
    }}))
    return {
        "GITHUB_EVENT_PATH": str(event),
        "GITHUB_REPOSITORY": REPOSITORY,
        "GITHUB_OUTPUT": str(tmp_path / "output"),
        "GITHUB_STEP_SUMMARY": str(tmp_path / "summary.md"),
        "GITHUB_SERVER_URL": "https://github.com",
        "GITHUB_RUN_ID": "1",
        "RUNNER_TEMP": str(tmp_path / "runner-temp"),
        sa.INPUT_CONNECTION: CONNECTION,
        sa.INPUT_STORE: "s3://bucket/scherlok/profiles.db",
        sa.INPUT_SCHERLOK_VERSION: "9.9.9",
        sa.INPUT_COMMENT: "true",
        sa.INPUT_FOOTER: "true",
    }


def _make_fork(env: dict[str, str]) -> None:
    event = json.loads(Path(env["GITHUB_EVENT_PATH"]).read_text())
    event["pull_request"]["head"]["repo"]["full_name"] = FORK_REPOSITORY
    Path(env["GITHUB_EVENT_PATH"]).write_text(json.dumps(event))


def test_preflight_masks_the_connection_before_printing_anything(github_env, capsys):
    github_env[sa.INPUT_STORE] = ""
    sa.preflight(github_env)

    lines = capsys.readouterr().out.splitlines()
    masks = [line for line in lines if line.startswith("::add-mask::")]
    assert lines[: len(masks)] == masks
    # `%` is escaped for the runner, which unescapes it back to the connection.
    assert sa.workflow_command("add-mask", CONNECTION) in masks
    assert "::add-mask::postgresql://alice:s3cr%252Ft@db.example.com" in masks[0]
    assert "::add-mask::s3cr/t" in masks
    assert all(CONNECTION not in line for line in lines[len(masks):])
    assert CONNECTION not in Path(github_env["GITHUB_STEP_SUMMARY"]).read_text()


def test_a_fork_pull_request_is_skipped_with_a_clear_message(github_env, capsys):
    _make_fork(github_env)
    github_env[sa.INPUT_CONNECTION] = ""  # forks get no secrets

    assert sa.preflight(github_env) == 0

    assert _outputs(Path(github_env["GITHUB_OUTPUT"])) == {"proceed": "false", "result": "skipped"}
    assert "::notice title=Scherlok::This pull request comes from a fork" in capsys.readouterr().out
    summary = Path(github_env["GITHUB_STEP_SUMMARY"]).read_text()
    assert summary.startswith(sa.HEADINGS[sa.RESULT_SKIPPED])
    assert "no repository secrets" in summary


def test_a_deleted_fork_counts_as_a_fork():
    event = {"pull_request": {"number": 1, "head": {"repo": None}}}
    assert sa.is_fork_pull_request(event, REPOSITORY)


def test_a_push_is_not_a_fork_pull_request():
    assert not sa.is_fork_pull_request({"ref": "refs/heads/main"}, REPOSITORY)


def test_a_missing_store_fails_with_a_clear_error(github_env, capsys):
    github_env[sa.INPUT_STORE] = ""

    assert sa.preflight(github_env) == 1

    out = capsys.readouterr().out
    assert "::error title=Scherlok::The store input is empty, so there is no baseline" in out
    assert _outputs(Path(github_env["GITHUB_OUTPUT"])) == {"proceed": "false", "result": "error"}
    summary = Path(github_env["GITHUB_STEP_SUMMARY"]).read_text()
    assert summary.startswith(sa.HEADINGS[sa.RESULT_ERROR])
    assert "The store input is empty" in summary


def test_a_missing_connection_fails_with_a_clear_error(github_env, capsys):
    github_env[sa.INPUT_CONNECTION] = ""
    assert sa.preflight(github_env) == 1
    assert "The connection input is empty" in capsys.readouterr().out


@pytest.mark.parametrize(
    ("store", "problem"),
    [
        ("https://example.com/profiles.db", "must be an s3://, gs:// or az:// URL"),
        ("does/not/exist.db", "does not exist on the runner"),
    ],
)
def test_an_unusable_store_fails(github_env, capsys, store, problem):
    github_env[sa.INPUT_STORE] = store
    assert sa.preflight(github_env) == 1
    assert problem in capsys.readouterr().out


@pytest.mark.parametrize(
    ("name", "value", "problem"),
    [
        (sa.INPUT_EXTRAS, "duckdb]==0 --index-url x", "extras input"),
        (sa.INPUT_COMMENT, "yes", "comment input must be true or false"),
        (sa.INPUT_FOOTER, "off", "footer input must be true or false"),
        (sa.INPUT_SCHERLOK_VERSION, "", "scherlok-version input is empty"),
    ],
)
def test_invalid_inputs_fail(github_env, capsys, name, value, problem):
    github_env[name] = value
    assert sa.preflight(github_env) == 1
    assert problem in capsys.readouterr().out


def test_an_old_python_fails(github_env, capsys):
    assert sa.preflight(github_env, python=(3, 9, 0)) == 1
    assert "needs Python 3.10 or newer" in capsys.readouterr().out


def test_a_remote_store_is_handed_to_scherlok(github_env):
    assert sa.preflight(github_env) == 0
    assert _outputs(Path(github_env["GITHUB_OUTPUT"])) == {
        "proceed": "true",
        "requirement": "scherlok==9.9.9",
        "remote-store": "s3://bucket/scherlok/profiles.db",
        "local-store": "",
    }


def test_a_local_store_is_resolved_to_an_absolute_path(github_env, tmp_path, monkeypatch):
    (tmp_path / "profiles.db").write_bytes(b"")
    monkeypatch.chdir(tmp_path)
    github_env[sa.INPUT_STORE] = "profiles.db"

    assert sa.preflight(github_env) == 0

    outputs = _outputs(Path(github_env["GITHUB_OUTPUT"]))
    assert outputs["remote-store"] == ""
    assert outputs["local-store"] == str((tmp_path / "profiles.db").resolve())


@pytest.mark.parametrize(
    ("extras", "install_from", "requirement"),
    [
        ("", "", "scherlok==9.9.9"),
        (" duckdb , bigquery ", "", "scherlok[duckdb,bigquery]==9.9.9"),
        ("duckdb", "/work/scherlok", "/work/scherlok[duckdb]"),
        ("", ".", "."),
    ],
)
def test_the_pip_requirement(github_env, extras, install_from, requirement):
    github_env[sa.INPUT_EXTRAS] = extras
    github_env[sa.INPUT_INSTALL_FROM] = install_from
    assert sa.pip_requirement(github_env) == requirement


# ----- verify arguments ------------------------------------------------------------

def test_tables_become_repeated_table_flags():
    assert sa.verify_arguments(" orders, customers\nrefunds\n\n", "warning") == [
        "verify", "--output", "json", "--fail-on", "warning",
        "--table", "orders", "--table", "customers", "--table", "refunds",
    ]


def test_no_tables_verifies_every_table_on_the_default_gate():
    assert sa.verify_arguments("", "") == ["verify", "--output", "json", "--fail-on", "critical"]


# ----- the pull request comment ------------------------------------------------------

class FakeApi:
    """An in-memory pull request thread behind the GitHubApi interface."""

    def __init__(self, comments: list[dict] | None = None, fail_with: str | None = None):
        self.comments = comments or []
        self.calls: list[tuple[str, str]] = []
        self.fail_with = fail_with
        self.next_id = 1000

    def request(self, method: str, path: str, payload: dict | None = None) -> Any:
        self.calls.append((method, path))
        if self.fail_with:
            raise sa.GitHubApiError(self.fail_with)
        if method == "GET":
            page = int(path.rsplit("page=", 1)[1])
            start = (page - 1) * sa.COMMENTS_PER_PAGE
            return self.comments[start:start + sa.COMMENTS_PER_PAGE]
        if method == "POST":
            self.next_id += 1
            comment = {"id": self.next_id, "body": payload["body"],
                       "html_url": f"https://github.com/c/{self.next_id}"}
            self.comments.append(comment)
            return comment
        if method == "PATCH":
            comment_id = int(path.rsplit("/", 1)[1])
            comment = next(c for c in self.comments if c["id"] == comment_id)
            comment["body"] = payload["body"]
            return comment
        raise AssertionError(method)


def test_the_first_run_creates_the_comment():
    api = FakeApi([{"id": 1, "body": "LGTM"}])
    url = sa.upsert_comment(api, REPOSITORY, PR_NUMBER, sa.comment_body("first"))

    assert url == "https://github.com/c/1001"
    assert [c["id"] for c in api.comments] == [1, 1001]


def test_a_rerun_updates_the_same_comment():
    api = FakeApi([{"id": 1, "body": "LGTM"}])
    first = sa.upsert_comment(api, REPOSITORY, PR_NUMBER, sa.comment_body("first"))
    second = sa.upsert_comment(api, REPOSITORY, PR_NUMBER, sa.comment_body("second"))

    assert first == second
    assert len(api.comments) == 2
    assert api.comments[1]["body"] == sa.comment_body("second")
    assert [method for method, _ in api.calls].count("POST") == 1


def test_the_comment_is_found_on_a_later_page():
    filler = [{"id": i, "body": "chatter"} for i in range(sa.COMMENTS_PER_PAGE)]
    api = FakeApi([*filler, {"id": 555, "body": sa.comment_body("old"), "html_url": "u"}])

    assert sa.upsert_comment(api, REPOSITORY, PR_NUMBER, sa.comment_body("new")) == "u"
    assert ("PATCH", f"/repos/{REPOSITORY}/issues/comments/555") in api.calls


def test_a_comment_that_only_quotes_the_marker_is_left_alone():
    api = FakeApi([{"id": 1, "body": f"> {sa.MARKER}\nquoted"}])
    sa.upsert_comment(api, REPOSITORY, PR_NUMBER, sa.comment_body("new"))
    assert api.comments[0]["body"] == f"> {sa.MARKER}\nquoted"


def _report_env(github_env: dict[str, str], tmp_path: Path, payload: dict | None,
                exit_code: str) -> dict[str, str]:
    env = {key: value for key, value in github_env.items() if key != sa.INPUT_CONNECTION}
    report = tmp_path / "verify.json"
    if payload is not None:
        report.write_text(json.dumps(payload))
    env.update({
        sa.INPUT_GITHUB_TOKEN: TOKEN,
        sa.REPORT_PATH: str(report),
        sa.VERIFY_EXIT_CODE: exit_code,
    })
    return env


def test_rerunning_the_report_keeps_one_comment(github_env, tmp_path):
    api = FakeApi()
    factory_calls = []

    def factory(base_url: str, token: str) -> FakeApi:
        factory_calls.append((base_url, token))
        return api

    failed = _report_env(github_env, tmp_path, _payload(STRUCTURED_ANOMALIES), "1")
    assert sa.report(failed, api_factory=factory) == 1
    passed = _report_env(github_env, tmp_path, _payload([], passed=True), "0")
    assert sa.report(passed, api_factory=factory) == 0

    assert len(api.comments) == 1
    assert sa.HEADINGS[sa.RESULT_PASSED] in api.comments[0]["body"]
    assert factory_calls == [("https://api.github.com", TOKEN)] * 2
    outputs = Path(github_env["GITHUB_OUTPUT"]).read_text().splitlines()
    assert "comment-url=https://github.com/c/1001" in outputs
    assert outputs.count("comment-url=https://github.com/c/1001") == 2


def test_the_report_writes_the_summary_and_outputs(github_env, tmp_path, capsys):
    env = _report_env(github_env, tmp_path, _payload(STRUCTURED_ANOMALIES), "1")
    env[sa.INPUT_COMMENT] = "false"

    assert sa.report(env, api_factory=lambda *_: pytest.fail("no comment wanted")) == 1

    summary = Path(env["GITHUB_STEP_SUMMARY"]).read_text()
    assert summary.startswith(sa.HEADINGS[sa.RESULT_FAILED])
    assert sa.MARKER not in summary
    assert _outputs(Path(env["GITHUB_OUTPUT"])) == {
        "result": "failed", "anomaly-count": "2", "comment-url": "", "report": env[sa.REPORT_PATH],
    }
    assert "::error title=Scherlok::Data checks failed: 2 anomalies, 1 critical" in (
        capsys.readouterr().out
    )


def test_an_error_still_updates_the_comment(github_env, tmp_path):
    """A stale "passed" comment must not survive a run that could not check."""
    api = FakeApi([{"id": 9, "body": sa.comment_body(sa.HEADINGS[sa.RESULT_PASSED]),
                    "html_url": "u"}])
    env = _report_env(github_env, tmp_path, None, "1")

    assert sa.report(env, api_factory=lambda *_: api) == 1
    assert sa.HEADINGS[sa.RESULT_ERROR] in api.comments[0]["body"]
    assert _outputs(Path(env["GITHUB_OUTPUT"])) == {
        "result": "error", "anomaly-count": "", "comment-url": "u", "report": "",
    }


def test_outside_a_pull_request_no_comment_is_posted(github_env, tmp_path, capsys):
    Path(github_env["GITHUB_EVENT_PATH"]).write_text(json.dumps({"ref": "refs/heads/main"}))
    env = _report_env(github_env, tmp_path, _payload([], passed=True), "0")

    assert sa.report(env, api_factory=lambda *_: pytest.fail("no API call wanted")) == 0
    assert "Not a pull request event" in capsys.readouterr().out


def test_a_comment_that_cannot_be_posted_warns_and_keeps_the_verdict(github_env, tmp_path,
                                                                     capsys):
    env = _report_env(github_env, tmp_path, _payload([], passed=True), "0")
    api = FakeApi(fail_with="HTTP 403 on GET /repos/acme/warehouse/issues/7/comments")

    assert sa.report(env, api_factory=lambda *_: api) == 0
    out = capsys.readouterr().out
    assert "::warning title=Scherlok::Could not comment on the pull request (HTTP 403" in out
    assert "pull-requests: write" in out


def test_api_errors_never_carry_the_token():
    def opener(request, timeout):
        assert request.get_header("Authorization") == f"Bearer {TOKEN}"
        raise urllib.error.HTTPError(request.full_url, 403, TOKEN, {}, None)

    api = sa.GitHubApi("https://api.github.com", TOKEN, opener=opener)
    with pytest.raises(sa.GitHubApiError) as excinfo:
        api.request("GET", "/repos/acme/warehouse/issues/7/comments")
    assert TOKEN not in str(excinfo.value)
    assert "HTTP 403" in str(excinfo.value)


def test_the_api_sends_json_with_the_token():
    sent = {}

    class Response:
        def __enter__(self):
            return self

        def __exit__(self, *exc):
            return False

        def read(self):
            return b'{"html_url": "u"}'

    def opener(request, timeout):
        sent.update(method=request.get_method(), url=request.full_url,
                    body=json.loads(request.data), timeout=timeout,
                    version=request.get_header("X-github-api-version"))
        return Response()

    api = sa.GitHubApi("https://ghe.example.com/api/v3/", TOKEN, opener=opener)
    assert api.request("POST", "/repos/a/b/issues/1/comments", {"body": "x"}) == {"html_url": "u"}
    assert sent == {
        "method": "POST", "url": "https://ghe.example.com/api/v3/repos/a/b/issues/1/comments",
        "body": {"body": "x"}, "timeout": sa.API_TIMEOUT_SECONDS, "version": sa.API_VERSION,
    }


# ----- end to end, with the real scherlok verify ---------------------------------------

SENTINEL_EMAIL = "sentinel-person@example.com"
SENTINEL_AMOUNT = 987654.321
SENTINEL_STATUS = "SENTINEL_STATUS_VALUE"


@pytest.fixture()
def warehouse(tmp_path, isolated_scherlok_dir, monkeypatch) -> dict[str, Any]:
    """A DuckDB file with synthetic sentinel rows, and a baseline learned from it."""
    duckdb = pytest.importorskip("duckdb")
    path = tmp_path / "warehouse.duckdb"
    conn = duckdb.connect(str(path))
    conn.execute(
        "CREATE TABLE orders AS SELECT i AS id, "
        f"CASE WHEN i = 1 THEN {SENTINEL_AMOUNT} ELSE i * 1.5 END AS amount, "
        f"'{SENTINEL_EMAIL}' AS email, '{SENTINEL_STATUS}' AS status "
        "FROM range(1, 501) t(i)"
    )
    conn.close()
    connection = f"duckdb://{path}"
    monkeypatch.setenv("SCHERLOK_CONNECTION", connection)
    result = CliRunner().invoke(app, ["investigate"])
    assert result.exit_code == 0, result.output
    baseline = tmp_path / "baseline-profiles.db"
    shutil.copyfile(isolated_scherlok_dir / "profiles.db", baseline)
    (isolated_scherlok_dir / "profiles.db").unlink()
    return {"path": path, "connection": connection, "baseline": baseline}


def _break_orders(path: Path) -> None:
    import duckdb

    conn = duckdb.connect(str(path))
    conn.execute("DELETE FROM orders WHERE id > 100")
    conn.execute("UPDATE orders SET email = NULL WHERE id > 20")
    conn.close()


def _run_action(github_env: dict[str, str], warehouse: dict[str, Any], home: Path,
                api: FakeApi) -> tuple[int, dict[str, str]]:
    """Run the verify and report steps as action.yml does, with a local store."""
    output = Path(github_env["GITHUB_OUTPUT"])
    output.unlink(missing_ok=True)
    verify_env = {
        "PATH": str(Path(sys.executable).parent),
        "HOME": str(home),
        "RUNNER_TEMP": github_env["RUNNER_TEMP"],
        "GITHUB_OUTPUT": str(output),
        "SCHERLOK_CONNECTION": warehouse["connection"],
        "SCHERLOK_STORE": "",
        sa.LOCAL_STORE: str(warehouse["baseline"]),
        sa.INPUT_TABLES: "orders",
        sa.INPUT_FAIL_ON: "critical",
    }
    assert sa.run_verify(verify_env) == 0
    verify_outputs = _outputs(output)
    report_env = {key: value for key, value in github_env.items() if key != sa.INPUT_CONNECTION}
    report_env.update({
        sa.INPUT_GITHUB_TOKEN: TOKEN,
        sa.REPORT_PATH: verify_outputs["report"],
        sa.VERIFY_EXIT_CODE: verify_outputs["exit-code"],
    })
    code = sa.report(report_env, api_factory=lambda *_: api)
    return code, _outputs(output)


def test_end_to_end_a_broken_table_fails_and_the_comment_holds_no_row_values(
    github_env, warehouse, isolated_scherlok_dir
):
    api = FakeApi()
    home = isolated_scherlok_dir.parent  # the subprocess resolves ~/.scherlok from HOME

    passed_code, passed = _run_action(github_env, warehouse, home, api)
    assert (passed_code, passed["result"]) == (0, "passed")

    _break_orders(warehouse["path"])
    failed_code, failed = _run_action(github_env, warehouse, home, api)

    assert (failed_code, failed["result"]) == (1, "failed")
    assert len(api.comments) == 1  # the rerun updated the comment
    body = api.comments[0]["body"]
    assert "| 🔴 CRITICAL | `orders` | — | `row_count` | 500 | 100 | -80.0% |" in body
    assert "| `email` | `null_rate` | 0.0% | 80.0% |" in body
    published = body + Path(github_env["GITHUB_STEP_SUMMARY"]).read_text()
    for raw_value in (SENTINEL_EMAIL, "987654", SENTINEL_STATUS):
        assert raw_value not in published
    assert str(warehouse["path"]) not in published
    assert warehouse["connection"] not in published


def test_end_to_end_the_baseline_file_is_never_written(github_env, warehouse,
                                                       isolated_scherlok_dir):
    before = warehouse["baseline"].read_bytes()
    _run_action(github_env, warehouse, isolated_scherlok_dir.parent, FakeApi())
    assert warehouse["baseline"].read_bytes() == before


# ----- action.yml and the release workflows ----------------------------------------

@pytest.fixture(scope="module")
def action() -> dict:
    return yaml.safe_load(ACTION_YML.read_text())


def _steps(action: dict) -> dict[str, dict]:
    return {step["id"]: step for step in action["runs"]["steps"]}


def test_the_action_is_ready_for_the_marketplace(action):
    assert action["name"] == "Scherlok Verify"
    assert 0 < len(action["description"]) < MARKETPLACE_DESCRIPTION_LIMIT
    assert set(action["branding"]) == {"icon", "color"}
    assert action["runs"]["using"] == "composite"
    assert not (REPO / "action.yaml").exists()


def test_no_input_is_interpolated_into_a_script(action):
    """Inputs reach scripts through env only, so none can inject shell code."""
    for step in action["runs"]["steps"]:
        assert "${{" not in step["run"], step["id"]


def test_the_connection_and_the_token_never_share_a_step(action):
    steps = _steps(action)
    with_connection = {
        step_id for step_id, step in steps.items()
        if "inputs.connection" in json.dumps(step.get("env", {}))
    }
    with_token = {
        step_id for step_id, step in steps.items()
        if "inputs.github-token" in json.dumps(step.get("env", {}))
    }
    assert with_connection == {"preflight", "verify"}
    assert with_token == {"report"}


def test_every_input_the_script_reads_is_passed(action):
    passed = {name for step in action["runs"]["steps"] for name in step.get("env", {})}
    script_inputs = {
        value for name, value in vars(sa).items()
        if name.startswith("INPUT_") and isinstance(value, str)
    }
    assert script_inputs <= passed
    assert {sa.LOCAL_STORE, sa.REPORT_PATH, sa.VERIFY_EXIT_CODE} <= passed


def _tag_patterns(workflow: Path) -> list[str]:
    triggers = yaml.safe_load(workflow.read_text()).get(True, {})  # YAML reads `on` as True
    return (triggers.get("push") or {}).get("tags") or []


@pytest.mark.parametrize("workflow", ["release.yml", "docker.yml"])
def test_moving_the_v1_tag_publishes_nothing(workflow):
    """`v1` is the action's moving tag; only a vX.Y.Z tag may run a release."""
    patterns = _tag_patterns(REPO / ".github" / "workflows" / workflow)
    assert patterns
    assert any(fnmatchcase("v1.2.3", pattern) for pattern in patterns)
    assert not any(fnmatchcase("v1", pattern) for pattern in patterns)
