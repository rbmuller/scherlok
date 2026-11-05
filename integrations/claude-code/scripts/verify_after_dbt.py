#!/usr/bin/env python3
"""Claude Code PostToolUse hook: check the data after `dbt run` and `dbt build`.

Claude Code sends the event as one JSON object on stdin after a Bash command
succeeds. When that command ran `dbt run` or `dbt build` in the foreground and
Scherlok is installed and configured, this hook runs

    scherlok verify --output json --dbt-project <project> --dbt-target-path <target>

which compares the models the run built with their stored baselines and never
changes them, then prints one JSON object on stdout for Claude Code:

- verify failed: `decision: "block"` with the findings as `reason`, which
  Claude Code adds next to the tool result, plus a one-line `systemMessage`
  for the person watching;
- verify passed: the summary as `hookSpecificOutput.additionalContext`;
- the check could not run (timeout, connection error, missing dbt
  artifacts): a one-line note through both channels, so silence is never
  read as a pass.

It prints nothing when the command is not a dbt run or build, when Scherlok
is not installed (or predates `verify --dbt-project`), or when no connection
is configured. It exits 0 on every path, so it can't break the session.

It holds no credentials. Scherlok resolves the connection itself, and whether
one exists is asked with `scherlok config --output json`, which never prints
it; Scherlok's files are never read here.

Standard library only, and Python 3.8+, so it runs under any `python3` on
PATH, independent of the environment Scherlok is installed in. Settings, from
the environment Claude Code runs in:

- SCHERLOK_BIN: path to the scherlok executable when it is not on PATH;
- SCHERLOK_HOOK_TIMEOUT: seconds for the whole check (default 150). Keep it
  below the hook's `timeout` in hooks.json, or Claude Code cancels the hook
  first and Claude learns nothing about the run.
"""

from __future__ import annotations

import json
import math
import os
import re
import shlex
import shutil
import subprocess
import sys
import time
from pathlib import Path
from typing import Any

HOOK_EVENT = "PostToolUse"
BASH_TOOL = "Bash"

SCHERLOK = "scherlok"
ENV_BIN = "SCHERLOK_BIN"
ENV_TIMEOUT = "SCHERLOK_HOOK_TIMEOUT"
DEFAULT_TIMEOUT_SECONDS = 150.0
PROBE_TIMEOUT_SECONDS = 30.0
OUTPUT_JSON_ARGS = ("--output", "json")
CONFIGURED_KEY = "connection_configured"
# Plain, unwrapped messages from Scherlok's console, so a note quotes whole lines.
CHILD_ENV = {"NO_COLOR": "1", "COLUMNS": "1000"}

# dbt resolves its project as --project-dir, then DBT_PROJECT_DIR, then the
# working directory or the nearest parent holding dbt_project.yml; its target
# directory as --target-path, then DBT_TARGET_PATH, then "target" under the
# project (a `target-path` set in dbt_project.yml is not read here).
DBT_PROJECT_FILE = "dbt_project.yml"
DBT_PROJECT_DIR_FLAG = "--project-dir"
DBT_TARGET_PATH_FLAG = "--target-path"
ENV_DBT_PROJECT_DIR = "DBT_PROJECT_DIR"
ENV_DBT_TARGET_PATH = "DBT_TARGET_PATH"
DEFAULT_TARGET_PATH = "target"

# `dbt run` or `dbt build` as a command of its own, global flags allowed
# before the subcommand. Not `dbt run-operation`, `dbt-run-and-watch`,
# `dbt_run.log` or text inside quotes, such as a commit message.
DBT_BUILD_RE = re.compile(
    r"""
    (?:^|[\s;&|(`/])                        # start of a command, or a path to dbt
    dbt
    (?:\s+-[^\s;&|()`]*                     # global flags before the subcommand,
       (?:\s+[^\s;&|()`-][^\s;&|()`]*)?     # each with an optional value
    )*?
    \s+(?P<subcommand>run|build)
    (?=$|[\s;&|)`])                         # the whole word: not run-operation
    """,
    re.VERBOSE,
)

# Keys of `scherlok verify --output json`.
PASSED = "passed"
FAIL_ON = "fail_on"
TABLES_VERIFIED = "tables_verified"
TABLES_WITHOUT_BASELINE = "tables_without_baseline"
DBT_MODELS_NOT_FOUND = "dbt_models_not_found"
ANOMALIES = "anomalies"
SEVERITIES = ("critical", "warning", "info")

# Claude Code moves a context string over 10,000 characters to a file and
# shows Claude only a preview, so the text stays well below that: at most
# MAX_FINDINGS lines of MAX_LINE_CHARS, plus the summary and the footer.
MAX_FINDINGS = 20
MAX_LINE_CHARS = 350
MAX_NAMES = 10
MAX_NOTE_LINES = 2
MAX_NOTE_CHARS = 300
MAX_TEXT_CHARS = 9000

ANSI_RE = re.compile(r"\x1b\[[0-9;]*[A-Za-z]")
URL_USERINFO_RE = re.compile(r"(\w+://)[^/\s@]*@")

FAILED_FOOTER = (
    "`scherlok verify` compares with the stored baseline and never changes it, so it keeps "
    "failing until the data is fixed; this hook runs it again after the next dbt run or build. "
    "When a join may have fanned out, `scherlok reconcile --source <input> --target <output> "
    "--key <key>` compares an output table with its input. Accepting a change as the new "
    "baseline (`scherlok accept`) is a decision for a person, not for the agent."
)


def dbt_subcommand(event: dict[str, Any]) -> str | None:
    """`run` or `build` when the event is a finished foreground dbt run or build, else None."""
    if event.get("hook_event_name") != HOOK_EVENT or event.get("tool_name") != BASH_TOOL:
        return None
    tool_input = event.get("tool_input")
    if not isinstance(tool_input, dict) or tool_input.get("run_in_background"):
        return None  # still running: its run_results.json isn't written yet
    response = event.get("tool_response")
    if isinstance(response, dict) and response.get("interrupted"):
        return None
    command = tool_input.get("command")
    if not isinstance(command, str):
        return None
    match = DBT_BUILD_RE.search(command)
    return match.group("subcommand") if match else None


def find_scherlok(env: dict[str, str]) -> str | None:
    """The scherlok executable: SCHERLOK_BIN when set, else the one on PATH."""
    configured = env.get(ENV_BIN)
    if configured:
        path = Path(configured).expanduser()
        return str(path) if path.is_file() and os.access(path, os.X_OK) else None
    return shutil.which(SCHERLOK, path=env.get("PATH"))


def find_dbt_project(command: str, cwd: Path, env: dict[str, str]) -> Path | None:
    """The dbt project the command ran in, resolved the way dbt resolves it."""
    explicit = _flag_value(_split(command), DBT_PROJECT_DIR_FLAG) or env.get(ENV_DBT_PROJECT_DIR)
    if explicit:
        project = _resolve(cwd, explicit)
        return project if (project / DBT_PROJECT_FILE).is_file() else None
    for candidate in (cwd, *cwd.parents):
        if (candidate / DBT_PROJECT_FILE).is_file():
            return candidate
    return None


def dbt_target_path(command: str, env: dict[str, str]) -> str:
    """dbt's target directory for the command, relative to the project unless absolute."""
    value = (
        _flag_value(_split(command), DBT_TARGET_PATH_FLAG)
        or env.get(ENV_DBT_TARGET_PATH)
        or DEFAULT_TARGET_PATH
    )
    return str(Path(value).expanduser())


def run_command(
    args: list[str], cwd: str, env: dict[str, str], timeout: float
) -> subprocess.CompletedProcess[str]:
    """Run one scherlok command and capture its output; raises TimeoutExpired."""
    return subprocess.run(
        args,
        cwd=cwd,
        env=env,
        stdin=subprocess.DEVNULL,
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
        timeout=timeout,
        check=False,
    )


def handle(
    event: dict[str, Any],
    env: dict[str, str],
    run: Any = run_command,
    clock: Any = time.monotonic,
) -> dict[str, Any] | None:
    """The hook's JSON answer for one event, or None to stay silent."""
    subcommand = dbt_subcommand(event)
    if subcommand is None:
        return None
    scherlok = find_scherlok(env)
    if scherlok is None:
        return None
    cwd = Path(event.get("cwd") or os.getcwd())
    child_env = {**env, **CHILD_ENV}
    deadline = clock() + _timeout_seconds(env)

    probe_timeout = min(PROBE_TIMEOUT_SECONDS, deadline - clock())
    try:
        probe = run([scherlok, "config", *OUTPUT_JSON_ARGS], str(cwd), child_env, probe_timeout)
    except (OSError, subprocess.SubprocessError):
        return None  # can't tell whether Scherlok is set up: stay out of the way
    settings = _json_object(probe.stdout) if probe.returncode == 0 else None
    if not settings or settings.get(CONFIGURED_KEY) is not True:
        return None  # no connection, or a Scherlok too old to say: nothing to check

    label = f"`dbt {subcommand}`"
    command = event["tool_input"]["command"]
    project = find_dbt_project(command, cwd, env)
    if project is None:
        return _note(label, f"could not find the dbt project ({DBT_PROJECT_FILE}) it ran in")
    args = [
        scherlok, "verify", *OUTPUT_JSON_ARGS,
        "--dbt-project", str(project),
        "--dbt-target-path", dbt_target_path(command, env),
    ]
    budget = _timeout_seconds(env)
    remaining = deadline - clock()
    try:
        if remaining <= 0:
            raise subprocess.TimeoutExpired(args, budget)
        result = run(args, str(cwd), child_env, remaining)
    except subprocess.TimeoutExpired:
        return _note(label, f"`scherlok verify` did not finish within {budget:.0f} s")
    except (OSError, subprocess.SubprocessError) as exc:
        return _note(label, f"`scherlok verify` could not start ({type(exc).__name__})")
    payload = _json_object(result.stdout)
    if payload is None or PASSED not in payload:
        return _note(label, _failure_reason(result))
    return verdict(payload, label, project)


def verdict(payload: dict[str, Any], label: str, project: Path) -> dict[str, Any]:
    """Turn a `scherlok verify --output json` result into the hook's answer."""
    anomalies = [a for a in payload.get(ANOMALIES) or [] if isinstance(a, dict)]
    counts = ", ".join(f"{int(payload.get(name) or 0)} {name}" for name in SEVERITIES)
    verified = len(payload.get(TABLES_VERIFIED) or [])
    checked = verified + len(payload.get(TABLES_WITHOUT_BASELINE) or [])
    fail_on = payload.get(FAIL_ON, "critical")
    if payload[PASSED]:
        if checked:
            headline = (
                f"Scherlok verify passed after {label}: {verified} of {checked} table(s) built "
                f"by the run compared with their baselines ({counts}; fail-on {fail_on})."
            )
        else:
            headline = f"Scherlok verify checked no table after {label}."
    else:
        headline = (
            f"Scherlok verify failed after {label} ({counts}; fail-on {fail_on}). "
            "The baselines are unchanged."
        )
    lines = [headline, *_finding_lines(anomalies, project), *_coverage_lines(payload)]
    if payload[PASSED]:
        return {
            "hookSpecificOutput": {
                "hookEventName": HOOK_EVENT,
                "additionalContext": _clip("\n".join(lines), MAX_TEXT_CHARS),
            }
        }
    lines.append(FAILED_FOOTER)
    return {
        "decision": "block",
        "reason": _clip("\n".join(lines), MAX_TEXT_CHARS),
        "systemMessage": f"Scherlok: verify failed after {label} ({counts}). "
        "The findings were passed to Claude.",
    }


def _finding_lines(anomalies: list[dict[str, Any]], project: Path) -> list[str]:
    """One line per anomaly, most severe first, capped at MAX_FINDINGS."""
    rank = {name.upper(): index for index, name in enumerate(SEVERITIES)}
    ordered = sorted(anomalies, key=lambda a: rank.get(str(a.get("severity")), len(rank)))
    lines = []
    for anomaly in ordered[:MAX_FINDINGS]:
        where = str(anomaly.get("table"))
        if anomaly.get("column"):
            where += f".{anomaly['column']}"
        severity, kind = anomaly.get("severity"), anomaly.get("type")
        line = f"- {severity} {where} ({kind}): {_sentence(anomaly.get('message'))}"
        if anomaly.get("hint"):
            line += f" Hint: {_sentence(anomaly['hint'])}"
        lines.append(_clip(line, MAX_LINE_CHARS))
    hidden = len(ordered) - MAX_FINDINGS
    if hidden > 0:
        lines.append(
            f"- {hidden} more; `scherlok verify --dbt-project {shlex.quote(str(project))} "
            "--output json` lists every finding."
        )
    return lines


def _coverage_lines(payload: dict[str, Any]) -> list[str]:
    """What the run built but verify could not compare with a baseline."""
    lines = []
    new_tables = payload.get(TABLES_WITHOUT_BASELINE) or []
    if new_tables:
        lines.append(
            "No baseline stored yet, so only the new-table checks ran (empty table, all-NULL "
            f"column, duplicate key): {_names(new_tables)}."
        )
    not_found = payload.get(DBT_MODELS_NOT_FOUND) or []
    if not_found:
        lines.append(
            "Built by dbt but not visible on the Scherlok connection, so not checked: "
            f"{_names(not_found)}."
        )
    return lines


def _note(label: str, reason: str) -> dict[str, Any]:
    """A one-line notice, to the person and to Claude, that nothing was verified."""
    message = _sentence(f"Scherlok did not verify the data after {label}: {reason}")
    return {
        "systemMessage": message,
        "hookSpecificOutput": {"hookEventName": HOOK_EVENT, "additionalContext": message},
    }


def _failure_reason(result: subprocess.CompletedProcess[str]) -> str:
    """What verify said on stderr, its first lines, without colors or URL credentials."""
    lines = [line.strip() for line in ANSI_RE.sub("", result.stderr or "").splitlines()]
    said = " ".join([line for line in lines if line][:MAX_NOTE_LINES])
    if not said:
        return f"`scherlok verify` exited with code {result.returncode} and no result"
    return URL_USERINFO_RE.sub(r"\1***@", said)[:MAX_NOTE_CHARS]


def _json_object(text: str | None) -> dict[str, Any] | None:
    try:
        value = json.loads(text or "")
    except ValueError:
        return None
    return value if isinstance(value, dict) else None


def _timeout_seconds(env: dict[str, str]) -> float:
    try:
        value = float(env.get(ENV_TIMEOUT, DEFAULT_TIMEOUT_SECONDS))
    except ValueError:
        return DEFAULT_TIMEOUT_SECONDS
    return value if math.isfinite(value) and value > 0 else DEFAULT_TIMEOUT_SECONDS


def _split(command: str) -> list[str]:
    try:
        return shlex.split(command)
    except ValueError:  # unbalanced quotes: fall back to whitespace
        return command.split()


def _flag_value(tokens: list[str], flag: str) -> str | None:
    prefix = f"{flag}="
    for index, token in enumerate(tokens):
        if token == flag and index + 1 < len(tokens):
            return tokens[index + 1]
        if token.startswith(prefix):
            return token[len(prefix):]
    return None


def _resolve(base: Path, value: str) -> Path:
    path = Path(value).expanduser()
    return path if path.is_absolute() else base / path


def _sentence(text: Any) -> str:
    text = str(text).strip()
    return text if text.endswith((".", "!", "?")) else f"{text}."


def _names(names: list[Any]) -> str:
    shown = ", ".join(str(name) for name in names[:MAX_NAMES])
    hidden = len(names) - MAX_NAMES
    return f"{shown} and {hidden} more" if hidden > 0 else shown


def _clip(text: str, limit: int) -> str:
    return text if len(text) <= limit else text[: limit - 1] + "…"


def main() -> int:
    """Read the event on stdin, answer on stdout, exit 0 whatever happens."""
    try:
        event = json.load(sys.stdin)
        answer = handle(event, dict(os.environ)) if isinstance(event, dict) else None
    except Exception:  # a hook must never break the session; silence is the fallback
        answer = None
    if answer:
        sys.stdout.write(json.dumps(answer))
    return 0


if __name__ == "__main__":
    sys.exit(main())
