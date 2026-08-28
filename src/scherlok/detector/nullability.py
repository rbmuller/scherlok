"""Nullability anomaly detection — significant NULL rate changes per column."""

from collections.abc import Sequence

from scherlok.detector.adaptive import ADAPTIVE_SCORE_THRESHOLD, adaptive_baseline
from scherlok.detector.severity import Severity

# Absolute change thresholds for null_rate (0.0 to 1.0)
NULL_RATE_WARNING_DELTA = 0.10   # 10 percentage points
NULL_RATE_CRITICAL_DELTA = 0.30  # 30 percentage points


def detect_nullability_anomalies(
    table: str,
    column: str,
    current_dist: dict,
    stored_dist: dict,
    *,
    history: Sequence[dict] | None = None,
) -> list[dict]:
    """Compare current null rate against stored profile for a column.

    Returns anomalies when NULL rate changes significantly.
    """
    anomalies: list[dict] = []

    current_rate = current_dist.get("null_rate")
    stored_rate = stored_dist.get("null_rate")

    if current_rate is None or stored_rate is None:
        return anomalies

    baseline = adaptive_baseline(history, "null_rate")
    if baseline is not None:
        score = baseline.score(current_rate)
        if score is None or abs(score) <= ADAPTIVE_SCORE_THRESHOLD:
            return anomalies

        delta = abs(current_rate - baseline.center)
        direction = "increased" if current_rate > baseline.center else "decreased"
        severity = (
            Severity.CRITICAL
            if delta >= NULL_RATE_CRITICAL_DELTA
            else Severity.WARNING
            if delta >= NULL_RATE_WARNING_DELTA
            else Severity.INFO
        )
        anomalies.append({
            "table": table,
            "type": "null_rate_change",
            "message": (
                f"Column '{column}' NULL rate {direction}: "
                f"learned baseline {baseline.center:.1%} -> {current_rate:.1%} "
                f"(Δ{delta:.1%}; robust score: {score:+.2f})"
            ),
            "severity": severity,
        })
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
