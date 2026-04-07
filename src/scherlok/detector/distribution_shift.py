"""Distribution shift detection — mean/stddev changes on numeric columns."""

from scherlok.detector.anomaly import z_score
from scherlok.detector.severity import classify_distribution_shift


def detect_distribution_shift(
    table: str,
    column: str,
    current_dist: dict,
    stored_dist: dict,
) -> list[dict]:
    """Compare current column distribution against stored profile.

    Uses z-score on the mean shift relative to the stored stddev.
    Returns anomalies when distribution shifts significantly.
    """
    anomalies: list[dict] = []

    current_mean = current_dist.get("mean")
    stored_mean = stored_dist.get("mean")
    stored_stddev = stored_dist.get("stddev")

    # Only applies to numeric columns with valid stats
    if current_mean is None or stored_mean is None or stored_stddev is None:
        return anomalies

    z = z_score(current_mean, stored_mean, stored_stddev)
    if z is None:
        return anomalies

    abs_z = abs(z)
    if abs_z <= 3.0:
        return anomalies

    severity = classify_distribution_shift(z)
    direction = "increased" if z > 0 else "decreased"

    anomalies.append({
        "table": table,
        "type": "distribution_shift",
        "message": (
            f"Column '{column}' mean {direction}: "
            f"{stored_mean:.4g} -> {current_mean:.4g} "
            f"(z-score: {z:+.2f})"
        ),
        "severity": severity,
    })

    return anomalies
