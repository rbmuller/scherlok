"""Nullability anomaly detection — significant NULL rate changes per column."""

from scherlok.detector.severity import Severity

# Absolute change thresholds for null_rate (0.0 to 1.0)
NULL_RATE_WARNING_DELTA = 0.10   # 10 percentage points
NULL_RATE_CRITICAL_DELTA = 0.30  # 30 percentage points


def detect_nullability_anomalies(
    table: str,
    column: str,
    current_dist: dict,
    stored_dist: dict,
) -> list[dict]:
    """Compare current null rate against stored profile for a column.

    Returns anomalies when NULL rate changes significantly.
    """
    anomalies: list[dict] = []

    current_rate = current_dist.get("null_rate")
    stored_rate = stored_dist.get("null_rate")

    if current_rate is None or stored_rate is None:
        return anomalies

    delta = abs(current_rate - stored_rate)

    if delta >= NULL_RATE_CRITICAL_DELTA:
        direction = "increased" if current_rate > stored_rate else "decreased"
        anomalies.append({
            "table": table,
            "type": "null_rate_change",
            "message": (
                f"Column '{column}' NULL rate {direction}: "
                f"{stored_rate:.1%} -> {current_rate:.1%} "
                f"(Δ{delta:.1%})"
            ),
            "severity": Severity.CRITICAL,
        })
    elif delta >= NULL_RATE_WARNING_DELTA:
        direction = "increased" if current_rate > stored_rate else "decreased"
        anomalies.append({
            "table": table,
            "type": "null_rate_change",
            "message": (
                f"Column '{column}' NULL rate {direction}: "
                f"{stored_rate:.1%} -> {current_rate:.1%} "
                f"(Δ{delta:.1%})"
            ),
            "severity": Severity.WARNING,
        })

    return anomalies
