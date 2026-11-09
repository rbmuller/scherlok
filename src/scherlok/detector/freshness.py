"""Freshness anomaly detection — tables not updated within expected cadence."""

from scherlok.detector.anomaly_types import FRESHNESS_CRITICAL, FRESHNESS_STALE
from scherlok.detector.fields import METRIC_HOURS_SINCE_UPDATE, make_anomaly
from scherlok.detector.severity import Severity
from scherlok.profiler.freshness import (
    FRESHNESS_COLUMN,
    FRESHNESS_SOURCE,
    HOURS_SINCE_UPDATE,
    SOURCE_COLUMN,
    SOURCE_METADATA,
)

# If hours_since_update doubles vs stored profile, something may be wrong
FRESHNESS_WARNING_MULTIPLIER = 2.0
FRESHNESS_CRITICAL_MULTIPLIER = 4.0
# Minimum hours to consider stale (avoids false positives on small tables)
FRESHNESS_MIN_HOURS = 1.0


def detect_freshness_anomalies(
    table: str,
    current_profile: dict,
    stored_profile: dict,
) -> list[dict]:
    """Compare current freshness against stored profile.

    Returns a list of anomaly dicts when a table hasn't been updated
    within its expected cadence. Ages read from different sources (metadata,
    or another column) are not compared; a profile saved before sources were
    recorded counts as metadata.
    """
    anomalies: list[dict] = []

    current_hours = current_profile.get(HOURS_SINCE_UPDATE)
    stored_hours = stored_profile.get(HOURS_SINCE_UPDATE)

    if current_hours is None or stored_hours is None:
        return anomalies

    source, column = _source(current_profile)
    if (source, column) != _source(stored_profile):
        return anomalies

    if stored_hours < FRESHNESS_MIN_HOURS:
        return anomalies

    ratio = current_hours / stored_hours if stored_hours > 0 else 0
    basis = f"; based on {column}" if source == SOURCE_COLUMN else ""

    if ratio >= FRESHNESS_CRITICAL_MULTIPLIER:
        anomaly_type = FRESHNESS_CRITICAL
        severity = Severity.CRITICAL
        message = (
            f"Table has not been updated for {current_hours:.1f}h "
            f"(expected ~{stored_hours:.1f}h, {ratio:.1f}x overdue{basis})"
        )
    elif ratio >= FRESHNESS_WARNING_MULTIPLIER:
        anomaly_type = FRESHNESS_STALE
        severity = Severity.WARNING
        message = (
            f"Table update delayed: {current_hours:.1f}h since last update "
            f"(expected ~{stored_hours:.1f}h, {ratio:.1f}x overdue{basis})"
        )
    else:
        return anomalies

    anomalies.append(make_anomaly(
        table,
        anomaly_type,
        message,
        severity,
        column=column,
        metric=METRIC_HOURS_SINCE_UPDATE,
        baseline=stored_hours,
        current=current_hours,
    ))
    return anomalies


def _source(profile: dict) -> tuple[str, str | None]:
    """Where a profile's age came from: (source, column)."""
    return profile.get(FRESHNESS_SOURCE) or SOURCE_METADATA, profile.get(FRESHNESS_COLUMN)
