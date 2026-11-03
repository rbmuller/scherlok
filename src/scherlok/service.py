"""Core profile-and-detect orchestration, shared by the CLI and the MCP server.

This module is the single place that knows *how* to profile one table and
detect anomalies against the stored baseline. Both adapters — the Typer CLI
and the MCP server — call into here, so neither owns business logic the other
can't reach. Nothing in this module prints, formats, or knows about a
transport; it takes a connector + store and returns plain data.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any
from urllib.parse import urlsplit, urlunsplit

from scherlok.alerter.exitcode import FAIL_ON_CRITICAL, fails_gate, normalize_fail_on
from scherlok.connectors.base import BaseConnector
from scherlok.detector.adaptive import ADAPTIVE_HISTORY_LIMIT
from scherlok.detector.anomaly import detect_volume_anomalies
from scherlok.detector.cardinality import detect_cardinality_anomalies
from scherlok.detector.distribution_shift import detect_distribution_shift
from scherlok.detector.freshness import detect_freshness_anomalies
from scherlok.detector.nullability import detect_nullability_anomalies
from scherlok.detector.schema_drift import detect_schema_drift
from scherlok.detector.severity import Severity
from scherlok.profiler.distribution import profile_distribution
from scherlok.profiler.freshness import profile_freshness
from scherlok.profiler.schema import profile_schema
from scherlok.profiler.volume import profile_volume
from scherlok.store.sqlite import ProfileStore

VOLUME_PROFILE = "volume"

ANOMALIES_KEY = "anomalies"
TABLES_VERIFIED_KEY = "tables_verified"
TABLES_WITHOUT_BASELINE_KEY = "tables_without_baseline"


class UnknownTablesError(ValueError):
    """Raised when requested tables are not visible on the connection."""


def profile_and_detect(
    connector: BaseConnector, store: ProfileStore, table: str, *, persist: bool = True
) -> tuple[list[dict], dict]:
    """Profile one table, detect anomalies against the stored baseline, save profiles.

    Returns ``(anomalies, current_volume)``. ``current_volume`` is returned
    separately because callers often want the row count inline (e.g. the CLI's
    dbt-style ✓/✗ line) without digging through the saved profile.

    The first time a table is seen there is no baseline, so no anomalies fire —
    the profiles saved on this run become the baseline for the next one.

    With ``persist=False`` nothing is saved: the current state is compared with
    the baseline and discarded. A gate needs this, because a saved failing run
    becomes the next baseline and a simple retry would then pass.
    """
    anomalies: list[dict] = []

    current_vol = profile_volume(connector, table)
    volume_history = store.get_profile_history(
        table, VOLUME_PROFILE, days=None, limit=ADAPTIVE_HISTORY_LIMIT
    )
    stored_vol = store.get_latest_profile(table, VOLUME_PROFILE)
    if stored_vol:
        anomalies.extend(
            detect_volume_anomalies(table, current_vol, stored_vol, history=volume_history)
        )

    current_sch = profile_schema(connector, table)
    stored_sch = store.get_latest_profile(table, "schema")
    if stored_sch:
        anomalies.extend(detect_schema_drift(table, current_sch, stored_sch))

    current_fresh = profile_freshness(connector, table)
    stored_fresh = store.get_latest_profile(table, "freshness")
    if stored_fresh:
        anomalies.extend(detect_freshness_anomalies(table, current_fresh, stored_fresh))

    for col in (current_sch or {}).get("columns", []):
        col_name = col["name"]
        current_dist = profile_distribution(connector, table, col_name)
        distribution_history = store.get_profile_history(
            table,
            f"distribution:{col_name}",
            days=None,
            limit=ADAPTIVE_HISTORY_LIMIT,
        )
        stored_dist = store.get_latest_profile(table, f"distribution:{col_name}")
        if stored_dist:
            anomalies.extend(
                detect_nullability_anomalies(
                    table, col_name, current_dist, stored_dist, history=distribution_history
                )
            )
            anomalies.extend(
                detect_distribution_shift(
                    table, col_name, current_dist, stored_dist, history=distribution_history
                )
            )
            anomalies.extend(
                detect_cardinality_anomalies(
                    table,
                    col_name,
                    current_dist,
                    stored_dist,
                    current_vol,
                    stored_vol,
                    history=distribution_history,
                )
            )
        if persist:
            store.save_profile(table, f"distribution:{col_name}", current_dist)

    if persist:
        store.save_profile(table, VOLUME_PROFILE, current_vol)
        store.save_profile(table, "schema", current_sch)
        store.save_profile(table, "freshness", current_fresh)

    return anomalies, current_vol


def has_baseline(store: ProfileStore, table: str) -> bool:
    """True when the store holds a profile to compare ``table`` against.

    Every profiling run saves a volume profile, so its presence marks a baseline.
    """
    return store.get_latest_profile(table, VOLUME_PROFILE) is not None


def select_tables(requested: list[str] | None, visible: list[str]) -> list[str]:
    """Resolve the requested table subset against what the connection can see.

    ``None`` or an empty list means every visible table. Unknown names raise
    rather than silently checking nothing, so the caller learns it asked for a
    table that isn't there. A name given twice is checked once.
    """
    if not requested:
        return visible
    visible_set = set(visible)
    missing = [t for t in requested if t not in visible_set]
    if missing:
        raise UnknownTablesError(f"Tables not found on this connection: {missing}")
    return list(dict.fromkeys(requested))


@dataclass
class VerificationResult:
    """Outcome of comparing tables with their stored baseline, with nothing saved.

    Plain data for the CLI (text or JSON) and the MCP server to render.
    ``tables_without_baseline`` are profiled but never counted as verified.
    """

    fail_on: str
    tables_verified: list[str] = field(default_factory=list)
    tables_without_baseline: list[str] = field(default_factory=list)
    anomalies: list[dict] = field(default_factory=list)

    @property
    def passed(self) -> bool:
        return not fails_gate(self.anomalies, self.fail_on)

    def to_dict(self) -> dict[str, Any]:
        """JSON-safe form shared by `scherlok verify --output json` and MCP `verify`."""
        return {
            "passed": self.passed,
            "fail_on": self.fail_on,
            TABLES_VERIFIED_KEY: list(self.tables_verified),
            TABLES_WITHOUT_BASELINE_KEY: list(self.tables_without_baseline),
            "anomaly_count": len(self.anomalies),
            **severity_counts(self.anomalies),
            ANOMALIES_KEY: [anomaly_to_dict(a) for a in self.anomalies],
        }


def verify_tables(
    connector: BaseConnector,
    store: ProfileStore,
    tables: list[str] | None = None,
    fail_on: str = FAIL_ON_CRITICAL,
) -> VerificationResult:
    """Compare tables with their stored baseline and decide the gate. Saves nothing.

    ``tables`` limits the run to those names (unknown names raise
    ``UnknownTablesError``); ``None`` verifies every visible table. A table
    without a baseline is still profiled, so anything detectable on a first
    sighting is reported, but it is listed apart and never counted as verified.
    Running this twice on the same data returns the same result.
    """
    result = VerificationResult(fail_on=normalize_fail_on(fail_on))
    for table in select_tables(tables, connector.list_tables()):
        if has_baseline(store, table):
            result.tables_verified.append(table)
        else:
            result.tables_without_baseline.append(table)
        anomalies, _ = profile_and_detect(connector, store, table, persist=False)
        result.anomalies.extend(anomalies)
    return result


def redact_connection(connection_string: str) -> str:
    """Mask the password in a connection string for safe display.

    ``postgresql://alice:secret@host/db`` -> ``postgresql://alice:***@host/db``.
    Returns the input unchanged when there's no password to hide.
    """
    if not connection_string:
        return connection_string
    parts = urlsplit(connection_string)
    if not parts.password:
        return connection_string
    host = parts.hostname or ""
    if parts.port:
        host = f"{host}:{parts.port}"
    userinfo = f"{parts.username}:***" if parts.username else "***"
    return urlunsplit(
        (parts.scheme, f"{userinfo}@{host}", parts.path, parts.query, parts.fragment)
    )


def anomaly_to_dict(anomaly: dict) -> dict[str, Any]:
    """Normalize one anomaly into a JSON-safe dict for structured transports.

    The detector layer stores ``severity`` as a ``Severity`` enum; serialize it
    to its bare name (``"CRITICAL"``) so MCP/JSON consumers get a stable string
    rather than ``"Severity.CRITICAL"``.
    """
    return {
        "table": anomaly.get("table"),
        "type": anomaly.get("type"),
        "severity": _severity_name(anomaly.get("severity", "")),
        "message": anomaly.get("message"),
    }


def severity_counts(anomalies: list[dict]) -> dict[str, int]:
    """Count anomalies per severity, keyed ``critical``, ``warning`` and ``info``."""
    counts = {severity.value.lower(): 0 for severity in Severity}
    for anomaly in anomalies:
        name = _severity_name(anomaly.get("severity", "")).lower()
        if name in counts:
            counts[name] += 1
    return counts


def _severity_name(severity: object) -> str:
    """Bare severity name (``"CRITICAL"``) for an enum member or a stored string."""
    return str(severity).rsplit(".", 1)[-1]
