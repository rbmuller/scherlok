"""Core profile-and-detect orchestration, shared by the CLI and the MCP server.

This module is the single place that knows *how* to profile one table and
detect anomalies against the stored baseline. Both adapters — the Typer CLI
and the MCP server — call into here, so neither owns business logic the other
can't reach. Nothing in this module prints, formats, or knows about a
transport; it takes a connector + store and returns plain data.
"""

from __future__ import annotations

from typing import Any
from urllib.parse import urlsplit, urlunsplit

from scherlok.connectors.base import BaseConnector
from scherlok.detector.anomaly import detect_volume_anomalies
from scherlok.detector.cardinality import detect_cardinality_anomalies
from scherlok.detector.distribution_shift import detect_distribution_shift
from scherlok.detector.freshness import detect_freshness_anomalies
from scherlok.detector.nullability import detect_nullability_anomalies
from scherlok.detector.schema_drift import detect_schema_drift
from scherlok.profiler.distribution import profile_distribution
from scherlok.profiler.freshness import profile_freshness
from scherlok.profiler.schema import profile_schema
from scherlok.profiler.volume import profile_volume
from scherlok.store.sqlite import ProfileStore


def profile_and_detect(
    connector: BaseConnector, store: ProfileStore, table: str
) -> tuple[list[dict], dict]:
    """Profile one table, detect anomalies against the stored baseline, save profiles.

    Returns ``(anomalies, current_volume)``. ``current_volume`` is returned
    separately because callers often want the row count inline (e.g. the CLI's
    dbt-style ✓/✗ line) without digging through the saved profile.

    The first time a table is seen there is no baseline, so no anomalies fire —
    the profiles saved on this run become the baseline for the next one.
    """
    anomalies: list[dict] = []

    current_vol = profile_volume(connector, table)
    stored_vol = store.get_latest_profile(table, "volume")
    if stored_vol:
        anomalies.extend(detect_volume_anomalies(table, current_vol, stored_vol))

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
        stored_dist = store.get_latest_profile(table, f"distribution:{col_name}")
        if stored_dist:
            anomalies.extend(
                detect_nullability_anomalies(table, col_name, current_dist, stored_dist)
            )
            anomalies.extend(
                detect_distribution_shift(table, col_name, current_dist, stored_dist)
            )
            anomalies.extend(
                detect_cardinality_anomalies(table, col_name, current_dist, stored_dist)
            )
        store.save_profile(table, f"distribution:{col_name}", current_dist)

    store.save_profile(table, "volume", current_vol)
    store.save_profile(table, "schema", current_sch)
    store.save_profile(table, "freshness", current_fresh)

    return anomalies, current_vol


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
        "severity": str(anomaly.get("severity", "")).rsplit(".", 1)[-1],
        "message": anomaly.get("message"),
    }
