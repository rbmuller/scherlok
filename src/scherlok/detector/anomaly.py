"""Z-score based anomaly detection for volume metrics."""

from scherlok.detector.severity import Severity, classify_volume_drop

VOLUME_SPIKE_WARNING_PCT = 100  # 2x increase
VOLUME_SPIKE_CRITICAL_PCT = 300  # 4x increase


def z_score(current: float, mean: float, stddev: float) -> float | None:
    """Calculate z-score. Returns None if stddev is zero."""
    if stddev == 0:
        return None
    return (current - mean) / stddev


def detect_volume_anomalies(
    table: str,
    current_profile: dict,
    stored_profile: dict,
    threshold: float = 3.0,
) -> list[dict]:
    """Compare current volume against stored profile.

    Detects both drops AND spikes.
    Returns a list of anomaly dicts with keys: table, type, message, severity.
    """
    anomalies: list[dict] = []
    current_count = current_profile["row_count"]
    previous_count = stored_profile["row_count"]

    if previous_count == 0 and current_count == 0:
        return anomalies

    # Detect drops
    severity = classify_volume_drop(current_count, previous_count)
    if severity is not None:
        drop_pct = ((previous_count - current_count) / previous_count) * 100
        anomalies.append({
            "table": table,
            "type": "volume_drop",
            "message": (
                f"Row count dropped {drop_pct:.1f}% "
                f"({previous_count:,} -> {current_count:,})"
            ),
            "severity": severity,
        })

    # Detect spikes
    if previous_count > 0 and current_count > previous_count:
        spike_pct = ((current_count - previous_count) / previous_count) * 100
        if spike_pct >= VOLUME_SPIKE_CRITICAL_PCT:
            anomalies.append({
                "table": table,
                "type": "volume_spike",
                "message": (
                    f"Row count spiked {spike_pct:.0f}% "
                    f"({previous_count:,} -> {current_count:,})"
                ),
                "severity": Severity.CRITICAL,
            })
        elif spike_pct >= VOLUME_SPIKE_WARNING_PCT:
            anomalies.append({
                "table": table,
                "type": "volume_spike",
                "message": (
                    f"Row count spiked {spike_pct:.0f}% "
                    f"({previous_count:,} -> {current_count:,})"
                ),
                "severity": Severity.WARNING,
            })

    # Detect table going empty
    if previous_count > 0 and current_count == 0:
        anomalies.append({
            "table": table,
            "type": "table_empty",
            "message": f"Table is now empty (was {previous_count:,} rows)",
            "severity": Severity.CRITICAL,
        })

    return anomalies
