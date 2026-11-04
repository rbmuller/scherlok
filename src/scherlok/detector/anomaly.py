"""Z-score based anomaly detection for volume metrics."""

from collections.abc import Sequence

from scherlok.detector.adaptive import ADAPTIVE_SCORE_THRESHOLD, adaptive_baseline
from scherlok.detector.anomaly_types import TABLE_EMPTY, VOLUME_DROP, VOLUME_SPIKE
from scherlok.detector.fields import METRIC_ROW_COUNT, make_anomaly
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
    *,
    history: Sequence[dict] | None = None,
) -> list[dict]:
    """Compare current volume against stored profile.

    Detects both drops AND spikes.
    Returns a list of anomaly dicts (see ``scherlok.detector.fields``); the
    ``baseline`` is the learned median when there is enough history, else the
    stored row count.
    """
    anomalies: list[dict] = []
    current_count = current_profile["row_count"]
    previous_count = stored_profile["row_count"]

    if previous_count == 0 and current_count == 0:
        return anomalies

    baseline = adaptive_baseline(history, "row_count")
    if baseline is not None:
        score = baseline.score(current_count)
        if score is None or abs(score) <= ADAPTIVE_SCORE_THRESHOLD:
            if previous_count > 0 and current_count == 0:
                anomalies.append(_table_empty(table, previous_count))
            return anomalies

        baseline_count = baseline.center
        if current_count < baseline_count and baseline_count > 0:
            change_pct = ((baseline_count - current_count) / baseline_count) * 100
            severity = (
                Severity.CRITICAL
                if change_pct >= 50
                else Severity.WARNING
                if change_pct >= 20
                else Severity.INFO
            )
            anomalies.append(_volume_anomaly(
                table,
                VOLUME_DROP,
                (
                    f"Row count dropped {change_pct:.1f}% "
                    f"(learned baseline {baseline_count:,.0f} -> {current_count:,}; "
                    f"robust score: {score:+.2f})"
                ),
                severity,
                baseline_count,
                current_count,
            ))
        elif current_count > baseline_count:
            if baseline_count > 0:
                change_pct = ((current_count - baseline_count) / baseline_count) * 100
                severity = (
                    Severity.CRITICAL
                    if change_pct >= VOLUME_SPIKE_CRITICAL_PCT
                    else Severity.WARNING
                    if change_pct >= VOLUME_SPIKE_WARNING_PCT
                    else Severity.INFO
                )
                effect = f"{change_pct:.0f}% change"
            else:
                severity = Severity.INFO
                effect = "change from zero"
            anomalies.append(_volume_anomaly(
                table,
                VOLUME_SPIKE,
                (
                    f"Row count spiked ({effect}; learned baseline "
                    f"{baseline_count:,.0f} -> {current_count:,}; "
                    f"robust score: {score:+.2f})"
                ),
                severity,
                baseline_count,
                current_count,
            ))

        if previous_count > 0 and current_count == 0:
            anomalies.append(_table_empty(table, previous_count))
        return anomalies

    # Detect drops
    severity = classify_volume_drop(current_count, previous_count)
    if severity is not None:
        drop_pct = ((previous_count - current_count) / previous_count) * 100
        anomalies.append(_volume_anomaly(
            table,
            VOLUME_DROP,
            (
                f"Row count dropped {drop_pct:.1f}% "
                f"({previous_count:,} -> {current_count:,})"
            ),
            severity,
            previous_count,
            current_count,
        ))

    # Detect spikes
    if previous_count > 0 and current_count > previous_count:
        spike_pct = ((current_count - previous_count) / previous_count) * 100
        if spike_pct >= VOLUME_SPIKE_CRITICAL_PCT:
            spike_severity = Severity.CRITICAL
        elif spike_pct >= VOLUME_SPIKE_WARNING_PCT:
            spike_severity = Severity.WARNING
        else:
            spike_severity = None
        if spike_severity is not None:
            anomalies.append(_volume_anomaly(
                table,
                VOLUME_SPIKE,
                (
                    f"Row count spiked {spike_pct:.0f}% "
                    f"({previous_count:,} -> {current_count:,})"
                ),
                spike_severity,
                previous_count,
                current_count,
            ))

    # Detect table going empty
    if previous_count > 0 and current_count == 0:
        anomalies.append(_table_empty(table, previous_count))

    return anomalies


def _volume_anomaly(
    table: str,
    anomaly_type: str,
    message: str,
    severity: Severity,
    baseline: float,
    current: int,
) -> dict:
    return make_anomaly(
        table,
        anomaly_type,
        message,
        severity,
        metric=METRIC_ROW_COUNT,
        baseline=baseline,
        current=current,
    )


def _table_empty(table: str, previous_count: int) -> dict:
    return _volume_anomaly(
        table,
        TABLE_EMPTY,
        f"Table is now empty (was {previous_count:,} rows)",
        Severity.CRITICAL,
        previous_count,
        0,
    )
