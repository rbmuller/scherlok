"""Distribution shift detection — mean/stddev changes on numeric columns."""

from collections.abc import Sequence

from scherlok.detector.adaptive import ADAPTIVE_SCORE_THRESHOLD, adaptive_baseline
from scherlok.detector.anomaly import z_score
from scherlok.detector.anomaly_types import DISTRIBUTION_SHIFT
from scherlok.detector.fields import METRIC_MEAN, make_anomaly
from scherlok.detector.severity import Severity, classify_distribution_shift

# A mean that moved less than this, relative to the baseline, is not reported,
# whatever its score. On a cumulative table the mean barely moves between runs,
# so a 0.03% move can score far above the threshold (#138).
MEAN_SHIFT_MIN_PCT = 1.0


def detect_distribution_shift(
    table: str,
    column: str,
    current_dist: dict,
    stored_dist: dict,
    *,
    history: Sequence[dict] | None = None,
) -> list[dict]:
    """Compare current column distribution against stored profile.

    Uses z-score on the mean shift relative to the stored stddev.
    Returns anomalies when distribution shifts significantly and the mean
    moved at least ``MEAN_SHIFT_MIN_PCT`` of the baseline. ``baseline`` and
    ``current`` are column means, never a value read from a row.
    """
    anomalies: list[dict] = []

    current_mean = current_dist.get("mean")
    stored_mean = stored_dist.get("mean")
    stored_stddev = stored_dist.get("stddev")

    # Only applies to numeric columns with valid stats
    if current_mean is None:
        return anomalies

    baseline = adaptive_baseline(history, "mean")
    if baseline is not None:
        z = baseline.score(current_mean)
        if z is None or abs(z) <= ADAPTIVE_SCORE_THRESHOLD:
            return anomalies
        if _below_min_shift(current_mean, baseline.center):
            return anomalies

        severity = classify_distribution_shift(z)
        direction = "increased" if z > 0 else "decreased"
        anomalies.append(_shift_anomaly(
            table,
            column,
            (
                f"Column '{column}' mean {direction}: "
                f"learned baseline {baseline.center:.4g} -> {current_mean:.4g} "
                f"(robust score: {z:+.2f})"
            ),
            severity,
            baseline.center,
            current_mean,
        ))
        return anomalies

    if stored_mean is None or stored_stddev is None:
        return anomalies

    z = z_score(current_mean, stored_mean, stored_stddev)
    if z is None:
        return anomalies

    abs_z = abs(z)
    if abs_z <= 3.0 or _below_min_shift(current_mean, stored_mean):
        return anomalies

    severity = classify_distribution_shift(z)
    direction = "increased" if z > 0 else "decreased"

    anomalies.append(_shift_anomaly(
        table,
        column,
        (
            f"Column '{column}' mean {direction}: "
            f"{stored_mean:.4g} -> {current_mean:.4g} "
            f"(z-score: {z:+.2f})"
        ),
        severity,
        stored_mean,
        current_mean,
    ))

    return anomalies


def _shift_anomaly(
    table: str,
    column: str,
    message: str,
    severity: Severity,
    baseline: float,
    current: float,
) -> dict:
    return make_anomaly(
        table,
        DISTRIBUTION_SHIFT,
        message,
        severity,
        column=column,
        metric=METRIC_MEAN,
        baseline=baseline,
        current=current,
    )


def _below_min_shift(current: float, baseline: float) -> bool:
    """Whether the mean moved less than ``MEAN_SHIFT_MIN_PCT`` of a non-zero baseline."""
    if baseline == 0:
        return False
    return abs(current - baseline) / abs(baseline) * 100 < MEAN_SHIFT_MIN_PCT
