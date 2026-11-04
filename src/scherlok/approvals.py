"""Baseline approvals: what `scherlok accept` records for each table (#109).

A run never learns a reading with a CRITICAL anomaly (#116), and in locked
mode no run learns anything, so an intended change, such as a new column or
a planned backfill, becomes the new normal only when a person accepts it.
`accept` re-profiles the table, shows how the new state compares with the
current baseline, and on confirmation saves the new profiles together with
one approval row: who approved, why, and a before/after summary.

The summary holds row and column counts and, per anomaly the new state shows
against the old baseline, its type, severity and the structured fields of
#108 (aggregates such as counts, rates or means, never a value read from a
row). No connection string is kept: the database is named by its scope.

`accept` is deliberately not an MCP tool, so an agent can't approve its own
change through a tool call. That is a guardrail, not a security boundary.
"""

from __future__ import annotations

import os
import subprocess
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from typing import Any

from scherlok.connectors.base import BaseConnector
from scherlok.detector.fields import BASELINE, CHANGE_PCT, COLUMN, CURRENT, METRIC
from scherlok.service import (
    SCHEMA_PROFILE,
    VOLUME_PROFILE,
    TableReading,
    inspect_table,
    save_reading,
    severity_name,
)
from scherlok.store.sqlite import ProfileStore

GIT_USER_EMAIL_COMMAND = ("git", "config", "user.email")
GIT_TIMEOUT_SECONDS = 5
ENV_USER = "USER"
UNKNOWN_APPROVER = "unknown"

ROW_COUNT_KEY = "row_count"
COLUMN_COUNT_KEY = "column_count"
PROFILED_AT_KEY = "profiled_at"
ANOMALIES_KEY = "anomalies"
# Per anomaly in the "after" summary: what changed and by how much, without
# the message or the hint.
ANOMALY_SUMMARY_FIELDS = (COLUMN, METRIC, BASELINE, CURRENT, CHANGE_PCT)
# Key of the profile_volume timestamp, kept as `profiled_at`.
VOLUME_TIMESTAMP_KEY = "timestamp"


def approver() -> str:
    """Who is approving: `git config user.email`, else $USER, else "unknown"."""
    return _git_user_email() or os.environ.get(ENV_USER) or UNKNOWN_APPROVER


def _git_user_email() -> str | None:
    try:
        result = subprocess.run(  # noqa: S603 - fixed command, no shell
            GIT_USER_EMAIL_COMMAND,
            capture_output=True,
            text=True,
            timeout=GIT_TIMEOUT_SECONDS,
            check=False,
        )
    except (OSError, subprocess.SubprocessError):
        return None
    email = result.stdout.strip()
    return email if result.returncode == 0 and email else None


def baseline_summary(volume: dict | None, schema: dict | None) -> dict[str, Any] | None:
    """Row count, column count and profiling time of one state; None without a volume."""
    if volume is None:
        return None
    return {
        ROW_COUNT_KEY: volume.get(ROW_COUNT_KEY),
        COLUMN_COUNT_KEY: len((schema or {}).get("columns", [])),
        PROFILED_AT_KEY: volume.get(VOLUME_TIMESTAMP_KEY),
    }


def anomaly_summary(anomaly: dict) -> dict[str, Any]:
    """Type, severity and structured fields of one anomaly, without its message."""
    return {
        "type": anomaly.get("type"),
        "severity": severity_name(anomaly.get("severity", "")),
        **{key: anomaly.get(key) for key in ANOMALY_SUMMARY_FIELDS},
    }


@dataclass(kw_only=True)
class ApprovalRecord:
    """One approval, in the shape of ``store.sqlite.APPROVAL_COLUMNS``."""

    table_name: str
    scope: str | None
    approved_by: str
    reason: str
    before_summary: dict[str, Any] | None
    after_summary: dict[str, Any]
    approved_at: str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())

    def to_dict(self) -> dict[str, Any]:
        """JSON-safe dict, keyed and ordered like ``APPROVAL_COLUMNS``."""
        return asdict(self)


@dataclass
class ProposedBaseline:
    """A table's current state, waiting for a person to accept it as the baseline."""

    table: str
    reading: TableReading
    anomalies: list[dict]
    before: dict[str, Any] | None

    @property
    def after(self) -> dict[str, Any]:
        """The new state's summary, with what it shows against the old baseline."""
        summary = baseline_summary(self.reading.volume, self.reading.schema) or {}
        return {**summary, ANOMALIES_KEY: [anomaly_summary(a) for a in self.anomalies]}

    def approval(self, *, scope: str | None, approved_by: str, reason: str) -> ApprovalRecord:
        """The record that accepting this state leaves behind."""
        return ApprovalRecord(
            table_name=self.table,
            scope=scope,
            approved_by=approved_by,
            reason=reason,
            before_summary=self.before,
            after_summary=self.after,
        )


def propose_baseline(
    connector: BaseConnector, store: ProfileStore, table: str
) -> ProposedBaseline:
    """Profile ``table`` and compare it with its baseline, saving nothing."""
    anomalies, reading = inspect_table(connector, store, table)
    before = baseline_summary(
        store.get_latest_profile(table, VOLUME_PROFILE),
        store.get_latest_profile(table, SCHEMA_PROFILE),
    )
    return ProposedBaseline(table=table, reading=reading, anomalies=anomalies, before=before)


def accept_baseline(
    store: ProfileStore, proposal: ProposedBaseline, approval: ApprovalRecord
) -> None:
    """Make the proposed state the baseline and append its approval row."""
    save_reading(store, proposal.table, proposal.reading)
    store.save_approval(approval.to_dict())
