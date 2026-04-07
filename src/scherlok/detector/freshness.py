"""Freshness anomaly detection — tables not updated within expected cadence."""

from scherlok.detector.severity import Severity

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
    within its expected cadence.
    """
    anomalies: list[dict] = []

    current_hours = current_profile.get("hours_since_update")
    stored_hours = stored_profile.get("hours_since_update")

    if current_hours is None or stored_hours is None:
        return anomalies

    if stored_hours < FRESHNESS_MIN_HOURS:
        return anomalies

    ratio = current_hours / stored_hours if stored_hours > 0 else 0

    if ratio >= FRESHNESS_CRITICAL_MULTIPLIER:
        anomalies.append({
            "table": table,
            "type": "freshness_critical",
            "message": (
                f"Table has not been updated for {current_hours:.1f}h "
                f"(expected ~{stored_hours:.1f}h, {ratio:.1f}x overdue)"
            ),
            "severity": Severity.CRITICAL,
        })
    elif ratio >= FRESHNESS_WARNING_MULTIPLIER:
        anomalies.append({
            "table": table,
            "type": "freshness_stale",
            "message": (
                f"Table update delayed: {current_hours:.1f}h since last update "
                f"(expected ~{stored_hours:.1f}h, {ratio:.1f}x overdue)"
            ),
            "severity": Severity.WARNING,
        })

    return anomalies
