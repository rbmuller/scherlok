"""Steps of the Scherlok GitHub Action (``action.yml`` at the repository root).

The action runs this script three times, so no step holds both credentials:

- ``preflight`` (the runner's ``python3``, before Scherlok is installed):
  masks the connection string, skips pull requests from forks, checks the
  inputs and works out the pip requirement to install.
- ``verify`` (the action's virtualenv): runs ``scherlok verify --output json``
  with the connection in its environment and no GitHub token.
- ``report`` (the runner's ``python3``): writes the job summary and creates or
  updates one pull request comment, with the token and no connection.

Standard library only, so the report never depends on the Scherlok version
installed. It renders only the fields ``scherlok verify`` documents, which
are table and column names, aggregates and fixed sentences, never a value
read from a row.
"""

from __future__ import annotations

import html
import json
import os
import re
import shutil
import subprocess
import sys
import urllib.error
import urllib.request
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Any
from urllib.parse import parse_qsl, unquote, urlsplit

MIN_PYTHON = (3, 10)
ANNOTATION_TITLE = "Scherlok"

# Inputs, passed by action.yml as environment variables.
INPUT_CONNECTION = "INPUT_CONNECTION"
INPUT_STORE = "INPUT_STORE"
INPUT_TABLES = "INPUT_TABLES"
INPUT_FAIL_ON = "INPUT_FAIL_ON"
INPUT_COMMENT = "INPUT_COMMENT"
INPUT_FOOTER = "INPUT_FOOTER"
INPUT_GITHUB_TOKEN = "INPUT_GITHUB_TOKEN"
INPUT_EXTRAS = "INPUT_EXTRAS"
INPUT_SCHERLOK_VERSION = "INPUT_SCHERLOK_VERSION"
INPUT_INSTALL_FROM = "INPUT_INSTALL_FROM"

# Values handed from one step to the next, also through the environment.
LOCAL_STORE = "SCHERLOK_ACTION_LOCAL_STORE"
REPORT_PATH = "SCHERLOK_ACTION_REPORT"
VERIFY_EXIT_CODE = "SCHERLOK_ACTION_EXIT_CODE"

# Step outputs, named as action.yml reads them.
OUTPUT_PROCEED = "proceed"
OUTPUT_RESULT = "result"
OUTPUT_REQUIREMENT = "requirement"
OUTPUT_REMOTE_STORE = "remote-store"
OUTPUT_LOCAL_STORE = "local-store"
OUTPUT_EXIT_CODE = "exit-code"
OUTPUT_REPORT = "report"
OUTPUT_ANOMALY_COUNT = "anomaly-count"
OUTPUT_COMMENT_URL = "comment-url"

RESULT_PASSED = "passed"
RESULT_FAILED = "failed"
RESULT_ERROR = "error"
RESULT_SKIPPED = "skipped"

TRUE = "true"
FALSE = "false"
BOOLEAN_INPUTS = (INPUT_COMMENT, INPUT_FOOTER)

REMOTE_STORE_SCHEMES = ("s3://", "gs://", "az://")
URL_SEPARATOR = "://"
DEFAULT_FAIL_ON = "critical"
EXTRA_NAME = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]*$")
TABLE_SEPARATORS = re.compile(r"[,\n]")

# Mirrors scherlok.service.SECRET_QUERY_KEY_PARTS (a test keeps them equal):
# preflight runs before Scherlok is installed, so it cannot import it.
SECRET_QUERY_KEY_PARTS = ("password", "passwd", "pwd", "token", "secret", "key")

REPORT_DIR = "scherlok-action"
REPORT_FILE = "verify.json"
PRIVATE_DIR_MODE = 0o700
PRIVATE_FILE_MODE = 0o600

MARKER = "<!-- scherlok-verify -->"
FOOTER = "<sub>Checked by [Scherlok](https://github.com/rbmuller/scherlok)</sub>"
HEADINGS = {
    RESULT_PASSED: "### ✅ Scherlok: data checks passed",
    RESULT_FAILED: "### ❌ Scherlok: data checks failed",
    RESULT_ERROR: "### ⚠️ Scherlok could not check the data",
    RESULT_SKIPPED: "### ⏭️ Scherlok skipped the data checks",
}
SEVERITY_ORDER = ("CRITICAL", "WARNING", "INFO")
SEVERITY_LABELS = {"CRITICAL": "🔴 CRITICAL", "WARNING": "🟡 WARNING", "INFO": "🔵 INFO"}
# The structured fields `scherlok verify` adds to each anomaly (#108). An
# older Scherlok sends none of them, and the table falls back to the message.
DETAIL_FIELDS = ("column", "metric", "baseline", "current", "change_pct", "hint")
PERCENT_METRICS = frozenset({"null_rate"})
EMPTY_CELL = "—"
MAX_ANOMALY_ROWS = 50
MAX_TABLE_NAMES = 10

ERROR_TEXT = (
    "Scherlok could not complete the check, so nothing was verified. "
    "The job log has the error."
)
FORK_SKIP_TEXT = (
    "This pull request comes from a fork. GitHub gives fork pull requests no repository "
    "secrets, so there is no database connection to check with. Skipped. Run the check "
    "after merging, or from a branch in this repository."
)
NOT_A_PULL_REQUEST_TEXT = (
    "Not a pull request event, so no comment was posted; the result is in the job summary."
)
NO_TOKEN_TEXT = "The github-token input is empty, so no comment was posted."
COMMENT_FAILED_TEXT = (
    "Could not comment on the pull request ({detail}). The job needs "
    "`permissions: pull-requests: write`; the check result itself is unaffected."
)

API_VERSION = "2022-11-28"
API_TIMEOUT_SECONDS = 30
COMMENTS_PER_PAGE = 100
MAX_COMMENT_PAGES = 50
DEFAULT_API_URL = "https://api.github.com"
DEFAULT_SERVER_URL = "https://github.com"


# ----- workflow commands, outputs and the job summary ----------------------

def escape_data(value: str) -> str:
    """Escape a workflow command's message the way the runner unescapes it."""
    return value.replace("%", "%25").replace("\r", "%0D").replace("\n", "%0A")


def escape_property(value: str) -> str:
    """Escape a workflow command property (``title=...``)."""
    return escape_data(value).replace(":", "%3A").replace(",", "%2C")


def workflow_command(command: str, message: str, title: str | None = None) -> str:
    """Format ``::command title=...::message`` for the runner to act on."""
    properties = f" title={escape_property(title)}" if title else ""
    return f"::{command}{properties}::{escape_data(message)}"


def annotate(command: str, message: str) -> None:
    """Print an error, warning or notice annotation."""
    print(workflow_command(command, message, title=ANNOTATION_TITLE), flush=True)


def set_outputs(env: Mapping[str, str], **outputs: str) -> None:
    """Append step outputs to ``$GITHUB_OUTPUT``; names use ``_`` for ``-``."""
    path = env.get("GITHUB_OUTPUT")
    if not path:
        return
    with open(path, "a", encoding="utf-8") as handle:
        for name, value in outputs.items():
            if "\n" in value:
                raise ValueError(f"output {name} must be a single line")
            handle.write(f"{name.replace('_', '-')}={value}\n")


def write_summary(env: Mapping[str, str], markdown: str) -> None:
    """Append ``markdown`` to the job summary, when the runner provides one."""
    path = env.get("GITHUB_STEP_SUMMARY")
    if not path:
        return
    with open(path, "a", encoding="utf-8") as handle:
        handle.write(markdown + "\n")


def is_true(value: str | None) -> bool:
    return (value or "").strip().lower() == TRUE


# ----- the GitHub event ----------------------------------------------------

def load_event(env: Mapping[str, str]) -> dict[str, Any]:
    """The webhook payload of the run, or {} when there is none."""
    path = env.get("GITHUB_EVENT_PATH")
    if not path or not Path(path).is_file():
        return {}
    try:
        event = json.loads(Path(path).read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return {}
    return event if isinstance(event, dict) else {}


def _pull_request(event: Mapping[str, Any]) -> dict[str, Any] | None:
    pull_request = event.get("pull_request")
    return pull_request if isinstance(pull_request, dict) else None


def is_fork_pull_request(event: Mapping[str, Any], repository: str) -> bool:
    """True for a pull request whose head lives in another repository.

    A deleted fork has no head repository, which counts as a fork too.
    """
    pull_request = _pull_request(event)
    if pull_request is None:
        return False
    head_repo = (pull_request.get("head") or {}).get("repo") or {}
    return str(head_repo.get("full_name") or "").lower() != repository.lower()


def pull_request_number(event: Mapping[str, Any]) -> int | None:
    pull_request = _pull_request(event)
    number = pull_request.get("number") if pull_request else None
    return number if isinstance(number, int) else None


def head_commit(event: Mapping[str, Any], env: Mapping[str, str]) -> str:
    """The pull request's head commit, else the commit the run checked out."""
    pull_request = _pull_request(event)
    head = (pull_request or {}).get("head") or {}
    return str(head.get("sha") or env.get("GITHUB_SHA") or "")


# ----- preflight -----------------------------------------------------------

def secret_values(connection: str) -> list[str]:
    """Every part of a connection string that must be masked in the log.

    The whole string, the password, and the value of each credential query
    parameter, raw and percent-decoded: a driver error may print a part
    without the rest. When an ``@`` follows the host, an unencoded ``/``,
    ``?`` or ``#`` in the password cut the host short, so everything before
    the last ``@`` is masked.
    """
    values = [connection, connection.strip()]
    if URL_SEPARATOR in connection:
        values.extend(_url_secrets(connection))
    masked: list[str] = []
    for value in values:
        for variant in (value, unquote(value)):
            if variant and variant not in masked:
                masked.append(variant)
    return masked


def _url_secrets(connection: str) -> list[str]:
    """The credentials inside a URL-shaped connection string."""
    after_scheme = connection.split(URL_SEPARATOR, 1)[1]
    userinfo = [after_scheme.rsplit("@", 1)[0]] if "@" in after_scheme else []
    try:
        parts = urlsplit(connection)
    except ValueError:  # e.g. an unbalanced IPv6 bracket: trust none of it
        return userinfo
    host_cut_short = any("@" in piece for piece in (parts.path, parts.query, parts.fragment))
    secrets = userinfo if host_cut_short else []
    secrets.append(parts.password or "")
    secrets.extend(
        value
        for key, value in parse_qsl(parts.query, keep_blank_values=True)
        if any(part in key.lower() for part in SECRET_QUERY_KEY_PARTS)
    )
    return secrets


def normalized_extras(extras: str) -> str:
    """``" duckdb , bigquery "`` -> ``"duckdb,bigquery"``."""
    return ",".join(name.strip() for name in extras.split(",") if name.strip())


def pip_requirement(env: Mapping[str, str]) -> str:
    """What pip installs: the pinned release, or a local checkout for testing."""
    extras = normalized_extras(env.get(INPUT_EXTRAS, ""))
    suffix = f"[{extras}]" if extras else ""
    install_from = env.get(INPUT_INSTALL_FROM, "").strip()
    if install_from:
        return f"{install_from}{suffix}"
    return f"scherlok{suffix}=={env.get(INPUT_SCHERLOK_VERSION, '').strip()}"


def is_remote_store(store: str) -> bool:
    return store.startswith(REMOTE_STORE_SCHEMES)


def check_inputs(env: Mapping[str, str], python: tuple[int, ...]) -> list[str]:
    """Return one plain sentence per problem with the inputs; [] when they are fine."""
    problems: list[str] = []
    if tuple(python[:2]) < MIN_PYTHON:
        found = ".".join(str(part) for part in python[:2])
        problems.append(
            f"Scherlok needs Python 3.10 or newer on the runner (python3 is {found}). "
            "Add actions/setup-python before this step."
        )
    if not env.get(INPUT_CONNECTION, "").strip():
        problems.append(
            "The connection input is empty. Pass the database connection string from a "
            "secret, such as secrets.DATABASE_URL. A run that gets no secrets, such as a "
            "Dependabot pull request, cannot be checked."
        )
    store = env.get(INPUT_STORE, "").strip()
    if not store:
        problems.append(
            "The store input is empty, so there is no baseline to compare against. Set it "
            "to the s3://, gs:// or az:// URL of the profiles.db that scherlok watch or "
            "scherlok ci keeps up to date."
        )
    elif not is_remote_store(store):
        if URL_SEPARATOR in store:
            problems.append(
                "The store input must be an s3://, gs:// or az:// URL, or the path of a "
                "profiles.db file on the runner."
            )
        elif not Path(store).expanduser().is_file():
            problems.append(f"The store file {store} does not exist on the runner.")
    extras = normalized_extras(env.get(INPUT_EXTRAS, ""))
    if extras and not all(EXTRA_NAME.match(name) for name in extras.split(",")):
        problems.append(
            "The extras input must be comma-separated names such as duckdb or bigquery."
        )
    if not env.get(INPUT_INSTALL_FROM, "").strip() and not env.get(
        INPUT_SCHERLOK_VERSION, ""
    ).strip():
        problems.append("The scherlok-version input is empty.")
    for name in BOOLEAN_INPUTS:
        if env.get(name, TRUE).strip().lower() not in (TRUE, FALSE):
            input_name = name.removeprefix("INPUT_").lower()
            problems.append(f"The {input_name} input must be true or false.")
    return problems


def preflight(env: Mapping[str, str], python: tuple[int, ...] = tuple(sys.version_info)) -> int:
    """Mask the connection, skip fork pull requests and check the inputs."""
    for value in secret_values(env.get(INPUT_CONNECTION, "")):
        print(workflow_command("add-mask", value), flush=True)

    event = load_event(env)
    context = report_context(env, event)
    if is_fork_pull_request(event, env.get("GITHUB_REPOSITORY", "")):
        annotate("notice", FORK_SKIP_TEXT)
        write_summary(env, render_notice(RESULT_SKIPPED, [FORK_SKIP_TEXT], context))
        set_outputs(env, proceed=FALSE, result=RESULT_SKIPPED)
        return 0

    problems = check_inputs(env, python)
    if problems:
        for problem in problems:
            annotate("error", problem)
        write_summary(env, render_notice(RESULT_ERROR, problems, context))
        set_outputs(env, proceed=FALSE, result=RESULT_ERROR)
        return 1

    store = env[INPUT_STORE].strip()
    remote = is_remote_store(store)
    set_outputs(
        env,
        proceed=TRUE,
        requirement=pip_requirement(env),
        remote_store=store if remote else "",
        local_store="" if remote else str(Path(store).expanduser().resolve()),
    )
    return 0


# ----- verify ----------------------------------------------------------------

def split_tables(tables: str) -> list[str]:
    """Comma- or newline-separated names, trimmed, in order, without blanks."""
    return [name.strip() for name in TABLE_SEPARATORS.split(tables) if name.strip()]


def verify_arguments(tables: str, fail_on: str) -> list[str]:
    args = ["verify", "--output", "json", "--fail-on", fail_on.strip() or DEFAULT_FAIL_ON]
    for table in split_tables(tables):
        args.extend(["--table", table])
    return args


def copy_local_store(source: Path) -> Path:
    """Copy a profiles.db on the runner to where Scherlok reads its local store.

    A remote store is downloaded to the same place, so this is what the
    download would have done. The path comes from the installed Scherlok,
    the one thing in this script that needs it.
    """
    from scherlok.config import PROFILES_DB

    PROFILES_DB.parent.mkdir(mode=PRIVATE_DIR_MODE, parents=True, exist_ok=True)
    shutil.copyfile(source, PROFILES_DB)
    PROFILES_DB.chmod(PRIVATE_FILE_MODE)
    return PROFILES_DB


def run_verify(env: Mapping[str, str]) -> int:
    """Run ``scherlok verify --output json`` and keep its JSON for the report step.

    Always returns 0: the gate's exit code goes to an output, so the report
    step runs and decides the step's result.
    """
    local_store = env.get(LOCAL_STORE, "")
    if local_store:
        copy_local_store(Path(local_store))
    report_dir = Path(env.get("RUNNER_TEMP") or ".") / REPORT_DIR
    report_dir.mkdir(parents=True, exist_ok=True)
    report_path = report_dir / REPORT_FILE
    scherlok = Path(sys.executable).with_name("scherlok")
    args = verify_arguments(env.get(INPUT_TABLES, ""), env.get(INPUT_FAIL_ON, ""))
    with report_path.open("w", encoding="utf-8") as report:
        completed = subprocess.run([str(scherlok), *args], stdout=report, env=dict(env))
    set_outputs(env, exit_code=str(completed.returncode), report=str(report_path))
    return 0


# ----- rendering ---------------------------------------------------------------

@dataclass(frozen=True)
class ReportContext:
    """What the report says about the run, besides the verification itself."""

    run_url: str = ""
    commit: str = ""
    footer: bool = True


def report_context(env: Mapping[str, str], event: Mapping[str, Any]) -> ReportContext:
    server = env.get("GITHUB_SERVER_URL") or DEFAULT_SERVER_URL
    repository = env.get("GITHUB_REPOSITORY", "")
    run_id = env.get("GITHUB_RUN_ID", "")
    run_url = f"{server}/{repository}/actions/runs/{run_id}" if repository and run_id else ""
    return ReportContext(
        run_url=run_url,
        commit=head_commit(event, env),
        footer=is_true(env.get(INPUT_FOOTER, TRUE)),
    )


def text_cell(value: Any) -> str:
    """One line of plain text, safe inside a Markdown table cell."""
    text = " ".join(str(value).split())
    return html.escape(text, quote=False).replace("|", "\\|")


def name_cell(value: Any) -> str:
    """A table or column name as inline code."""
    if value is None or value == "":
        return EMPTY_CELL
    return "`" + text_cell(value).replace("`", "'") + "`"


def _is_number(value: Any) -> bool:
    return isinstance(value, (int, float)) and not isinstance(value, bool)


def value_cell(value: Any, metric: Any) -> str:
    """A baseline or current value: counts with separators, rates as percentages."""
    if value is None:
        return EMPTY_CELL
    if not _is_number(value):
        return text_cell(value)
    if metric in PERCENT_METRICS:
        return f"{value:.1%}"
    if isinstance(value, float) and value.is_integer():
        value = int(value)
    if isinstance(value, int):
        return f"{value:,}"
    return f"{value:,.4g}"


def change_cell(change_pct: Any) -> str:
    return f"{change_pct:+.1f}%" if _is_number(change_pct) else EMPTY_CELL


def severity_cell(severity: Any) -> str:
    name = str(severity or "").upper()
    return SEVERITY_LABELS.get(name, text_cell(name) or EMPTY_CELL)


def _severity_rank(anomaly: Mapping[str, Any]) -> int:
    name = str(anomaly.get("severity") or "").upper()
    return SEVERITY_ORDER.index(name) if name in SEVERITY_ORDER else len(SEVERITY_ORDER)


def has_detail_fields(anomalies: list[dict[str, Any]]) -> bool:
    return any(field in anomaly for anomaly in anomalies for field in DETAIL_FIELDS)


def anomaly_table(anomalies: list[dict[str, Any]]) -> list[str]:
    """Markdown rows for the anomalies, most severe first.

    With the structured fields, one column each for the column, metric,
    baseline, current value and change, and the message and hint as details.
    Without them, the anomaly type and message.
    """
    ordered = sorted(anomalies, key=_severity_rank)
    shown = ordered[:MAX_ANOMALY_ROWS]
    if has_detail_fields(anomalies):
        lines = [
            "| Severity | Table | Column | Metric | Baseline | Current | Change | Details |",
            "|---|---|---|---|---|---|---|---|",
        ]
        for anomaly in shown:
            metric = anomaly.get("metric")
            details = text_cell(anomaly.get("message") or "")
            if anomaly.get("hint"):
                details += f"<br>_{text_cell(anomaly['hint'])}_"
            lines.append(
                f"| {severity_cell(anomaly.get('severity'))} "
                f"| {name_cell(anomaly.get('table'))} "
                f"| {name_cell(anomaly.get('column'))} "
                f"| {name_cell(metric)} "
                f"| {value_cell(anomaly.get('baseline'), metric)} "
                f"| {value_cell(anomaly.get('current'), metric)} "
                f"| {change_cell(anomaly.get('change_pct'))} "
                f"| {details or EMPTY_CELL} |"
            )
    else:
        lines = ["| Severity | Table | Type | Details |", "|---|---|---|---|"]
        for anomaly in shown:
            lines.append(
                f"| {severity_cell(anomaly.get('severity'))} "
                f"| {name_cell(anomaly.get('table'))} "
                f"| {name_cell(anomaly.get('type'))} "
                f"| {text_cell(anomaly.get('message') or '') or EMPTY_CELL} |"
            )
    hidden = len(ordered) - len(shown)
    if hidden:
        lines.extend(["", f"{hidden} more not shown; the `report` output has the full list."])
    return lines


def table_names(names: list[Any]) -> str:
    shown = ", ".join(name_cell(name) for name in names[:MAX_TABLE_NAMES])
    hidden = len(names) - MAX_TABLE_NAMES
    return f"{shown} and {hidden} more" if hidden > 0 else shown


def _count(payload: Mapping[str, Any], key: str) -> int:
    value = payload.get(key)
    return value if isinstance(value, int) else 0


def _names(payload: Mapping[str, Any], key: str) -> list[Any]:
    value = payload.get(key)
    return value if isinstance(value, list) else []


def verification_lines(payload: Mapping[str, Any]) -> list[str]:
    """The counts, the tables and the anomaly table of one verify result."""
    verified = _names(payload, "tables_verified")
    missing = _names(payload, "tables_without_baseline")
    counts = ", ".join(
        f"{_count(payload, name)} {name}" for name in ("critical", "warning", "info")
    )
    fail_on = text_cell(payload.get("fail_on") or DEFAULT_FAIL_ON)
    tables = "table" if len(verified) == 1 else "tables"
    lines = [
        f"{len(verified)} {tables} verified, {len(missing)} without a baseline. "
        f"Anomalies: {counts}. The check fails on `{fail_on}` or worse.",
    ]
    if verified:
        lines.extend(["", f"Verified: {table_names(verified)}"])
    if missing:
        lines.extend([
            "",
            f"Not verified, no baseline yet: {table_names(missing)}. They got only the "
            "new-table checks.",
        ])
    anomalies = [a for a in _names(payload, "anomalies") if isinstance(a, dict)]
    if anomalies:
        lines.extend(["", *anomaly_table(anomalies)])
    return lines


def context_lines(context: ReportContext) -> list[str]:
    links = []
    if context.run_url:
        links.append(f"[Workflow run]({context.run_url})")
    if context.commit:
        links.append(f"commit `{text_cell(context.commit[:7])}`")
    lines = ["", " · ".join(links)] if links else []
    if context.footer:
        lines.extend(["", FOOTER])
    return lines


def render_report(
    payload: Mapping[str, Any] | None, result: str, context: ReportContext
) -> str:
    """The Markdown shared by the job summary and the pull request comment."""
    lines = [HEADINGS[result], ""]
    if result == RESULT_ERROR or payload is None:
        lines.append(ERROR_TEXT)
    else:
        lines.extend(verification_lines(payload))
    return "\n".join([*lines, *context_lines(context)])


def render_notice(result: str, paragraphs: list[str], context: ReportContext) -> str:
    """A report with no verification: a skipped run or invalid inputs."""
    body = "\n\n".join(text_cell(paragraph) for paragraph in paragraphs)
    return "\n".join([HEADINGS[result], "", body, *context_lines(context)])


def comment_body(report: str) -> str:
    """The report behind the hidden marker that finds this comment next time."""
    return f"{MARKER}\n{report}"


# ----- the GitHub API ------------------------------------------------------------

class GitHubApiError(Exception):
    """A GitHub API call failed; the message never contains the token."""


Opener = Callable[..., Any]


class GitHubApi:
    """The few REST calls the report needs, over urllib."""

    def __init__(self, base_url: str, token: str, opener: Opener = urllib.request.urlopen):
        self._base_url = base_url.rstrip("/")
        self._token = token
        self._open = opener

    def request(self, method: str, path: str, payload: dict[str, Any] | None = None) -> Any:
        data = json.dumps(payload).encode("utf-8") if payload is not None else None
        request = urllib.request.Request(
            f"{self._base_url}{path}",
            data=data,
            method=method,
            headers={
                "Accept": "application/vnd.github+json",
                "Authorization": f"Bearer {self._token}",
                "Content-Type": "application/json",
                "User-Agent": "scherlok-action",
                "X-GitHub-Api-Version": API_VERSION,
            },
        )
        try:
            with self._open(request, timeout=API_TIMEOUT_SECONDS) as response:
                body = response.read()
        except urllib.error.HTTPError as exc:
            raise GitHubApiError(f"HTTP {exc.code} on {method} {path}") from None
        except (urllib.error.URLError, OSError) as exc:
            raise GitHubApiError(f"{type(exc).__name__} on {method} {path}") from None
        return json.loads(body) if body else None


def find_comment(api: GitHubApi, repository: str, number: int) -> dict[str, Any] | None:
    """The first comment on the pull request that starts with the marker."""
    for page in range(1, MAX_COMMENT_PAGES + 1):
        comments = api.request(
            "GET",
            f"/repos/{repository}/issues/{number}/comments"
            f"?per_page={COMMENTS_PER_PAGE}&page={page}",
        )
        if not isinstance(comments, list):
            return None
        for comment in comments:
            if str(comment.get("body") or "").lstrip().startswith(MARKER):
                return comment
        if len(comments) < COMMENTS_PER_PAGE:
            return None
    return None


def upsert_comment(api: GitHubApi, repository: str, number: int, body: str) -> str:
    """Update this action's comment on the pull request, or create it. Returns its URL."""
    existing = find_comment(api, repository, number)
    if existing is not None:
        comment = api.request(
            "PATCH", f"/repos/{repository}/issues/comments/{existing['id']}", {"body": body}
        )
    else:
        comment = api.request(
            "POST", f"/repos/{repository}/issues/{number}/comments", {"body": body}
        )
    return str((comment or {}).get("html_url") or "")


# ----- report ----------------------------------------------------------------

def load_payload(path: str) -> dict[str, Any] | None:
    """The verify JSON, or None when verify printed none (it hit an error)."""
    if not path or not Path(path).is_file():
        return None
    try:
        payload = json.loads(Path(path).read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return None
    if not isinstance(payload, dict) or not isinstance(payload.get("passed"), bool):
        return None
    return payload


def result_for(payload: Mapping[str, Any] | None, exit_code: str) -> str:
    """passed or failed as verify decided; error when it printed no verdict.

    A verdict that disagrees with the exit code means verify broke after
    printing it, which is an error too.
    """
    if payload is None:
        return RESULT_ERROR
    passed = payload["passed"]
    if exit_code.strip() != ("0" if passed else "1"):
        return RESULT_ERROR
    return RESULT_PASSED if passed else RESULT_FAILED


def post_comment(
    env: Mapping[str, str],
    event: Mapping[str, Any],
    body: str,
    api_factory: Callable[[str, str], GitHubApi],
) -> str:
    """Create or update the pull request comment; warn, never fail, when it can't."""
    number = pull_request_number(event)
    if number is None:
        annotate("notice", NOT_A_PULL_REQUEST_TEXT)
        return ""
    token = env.get(INPUT_GITHUB_TOKEN, "").strip()
    if not token:
        annotate("warning", NO_TOKEN_TEXT)
        return ""
    api = api_factory(env.get("GITHUB_API_URL") or DEFAULT_API_URL, token)
    try:
        return upsert_comment(api, env.get("GITHUB_REPOSITORY", ""), number, body)
    except GitHubApiError as exc:
        annotate("warning", COMMENT_FAILED_TEXT.format(detail=exc))
        return ""


def report(
    env: Mapping[str, str], api_factory: Callable[[str, str], GitHubApi] = GitHubApi
) -> int:
    """Write the job summary and the comment; exit 1 unless the check passed."""
    event = load_event(env)
    payload = load_payload(env.get(REPORT_PATH, ""))
    result = result_for(payload, env.get(VERIFY_EXIT_CODE, ""))
    markdown = render_report(payload, result, report_context(env, event))
    write_summary(env, markdown)

    comment_url = ""
    if is_true(env.get(INPUT_COMMENT, TRUE)):
        comment_url = post_comment(env, event, comment_body(markdown), api_factory)

    # Without a verify result nothing was counted, so the count stays empty.
    set_outputs(
        env,
        result=result,
        anomaly_count=str(_count(payload, "anomaly_count")) if payload else "",
        comment_url=comment_url,
        report=env.get(REPORT_PATH, "") if payload else "",
    )
    if result == RESULT_FAILED:
        annotate(
            "error",
            f"Data checks failed: {_count(payload, 'anomaly_count')} anomalies, "
            f"{_count(payload, 'critical')} critical. See the job summary.",
        )
    elif result == RESULT_ERROR:
        annotate("error", ERROR_TEXT)
    return 0 if result == RESULT_PASSED else 1


COMMANDS: dict[str, Callable[[Mapping[str, str]], int]] = {
    "preflight": preflight,
    "verify": run_verify,
    "report": report,
}


def main(argv: list[str], env: Mapping[str, str]) -> int:
    if len(argv) != 2 or argv[1] not in COMMANDS:
        print(f"usage: {Path(argv[0]).name} {{{'|'.join(COMMANDS)}}}", file=sys.stderr)
        return 2
    return COMMANDS[argv[1]](env)


if __name__ == "__main__":
    sys.exit(main(sys.argv, os.environ))
