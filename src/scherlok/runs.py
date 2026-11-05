"""Run records: one per `watch`, `ci`, `check` or `verify` run, passing or not (#115).

A run that finds nothing used to leave no trace, so there was no way to show
that a check ran and passed. A record names the tables a run checked and
counts what it found per severity. It never holds a column value or a
connection string: the connection is reduced to its scope, which carries no
credentials (see `service.connection_scope`).

Usage: create a ``RunRecord`` when the run starts, tag the anomalies it saves
with ``run_id``, then ``finish`` it and save ``to_dict()`` with
``ProfileStore.save_run``. The same dict is what `scherlok verify --record`
appends as one JSON line.
"""

from __future__ import annotations

import json
import uuid
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from scherlok import __version__
from scherlok.alerter.exitcode import FAIL_ON_CRITICAL, fails_gate
from scherlok.service import severity_counts

COMMAND_WATCH = "watch"
COMMAND_CI = "ci"
COMMAND_CHECK = "check"
COMMAND_VERIFY = "verify"
MCP_COMMAND_PREFIX = "mcp:"


def mcp_command(tool: str) -> str:
    """Command name recorded for an MCP tool, e.g. ``mcp:watch``."""
    return f"{MCP_COMMAND_PREFIX}{tool}"


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _new_run_id() -> str:
    # Random rather than a row number: `verify` never uploads a remote store,
    # so ids from parallel CI jobs must not collide in their --record files.
    return uuid.uuid4().hex


@dataclass(kw_only=True)
class RunRecord:
    """One run, in the shape of ``store.sqlite.RUN_COLUMNS``.

    ``fail_on`` is the gate the run was judged by; commands without a
    ``--fail-on`` (`watch`, MCP `watch`) use their exit-code gate, CRITICAL.
    ``passed``, the counts and ``finished_at`` are set by ``finish``; the
    store refuses a record that was never finished. ``against`` is the scope
    a `verify --against` run took its baseline from (``scope`` stays the
    database it checked) and ``ignored`` the checks it skipped.
    """

    run_id: str = field(default_factory=_new_run_id)
    command: str
    scope: str | None
    scherlok_version: str = __version__
    tables: list[str] = field(default_factory=list)
    passed: bool | None = None
    fail_on: str = FAIL_ON_CRITICAL
    critical: int = 0
    warning: int = 0
    info: int = 0
    started_at: str = field(default_factory=_now)
    finished_at: str | None = None
    against: str | None = None
    ignored: list[str] = field(default_factory=list)

    def finish(self, tables: list[str], anomalies: list[dict]) -> RunRecord:
        """Record the tables checked and the anomalies found, and stamp the end."""
        counts = severity_counts(anomalies)
        self.tables = list(tables)
        self.passed = not fails_gate(anomalies, self.fail_on)
        self.critical = counts["critical"]
        self.warning = counts["warning"]
        self.info = counts["info"]
        self.finished_at = _now()
        return self

    def to_dict(self) -> dict[str, Any]:
        """JSON-safe dict, keyed and ordered like ``RUN_COLUMNS``."""
        return asdict(self)


def append_run_record(path: Path, record: dict[str, Any]) -> None:
    """Append ``record`` to ``path`` as one JSON line, creating the file if needed."""
    with path.open("a", encoding="utf-8") as record_file:
        record_file.write(json.dumps(record) + "\n")
