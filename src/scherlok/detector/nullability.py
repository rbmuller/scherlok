"""Nullability anomaly detection — significant NULL rate changes per column."""

from collections.abc import Sequence

from scherlok.detector.adaptive import ADAPTIVE_SCORE_THRESHOLD, adaptive_baseline
from scherlok.detector.anomaly_types import NULL_RATE_CHANGE
from scherlok.detector.fields import METRIC_NULL_RATE, make_anomaly
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

    Returns anomalies when NULL rate changes significantly. ``baseline`` and
    ``current`` are NULL rates between 0 and 1, so ``change_pct`` is the
    relative change of the rate, not percentage points.
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
        increased = current_rate > baseline.center
        direction = "increased" if increased else "decreased"
        severity = _severity(delta, increased) or Severity.INFO
        anomalies.append(_null_rate_anomaly(
            table,
            column,
            (
                f"Column '{column}' NULL rate {direction}: "
                f"learned baseline {baseline.center:.1%} -> {current_rate:.1%} "
                f"(Δ{delta:.1%}; robust score: {score:+.2f})"
            ),
            severity,
            baseline.center,
            current_rate,
        ))
        return anomalies

    delta = abs(current_rate - stored_rate)
    increased = current_rate > stored_rate
    severity = _severity(delta, increased)
    if severity is None:
        return anomalies

    direction = "increased" if increased else "decreased"
    anomalies.append(_null_rate_anomaly(
        table,
        column,
        (
            f"Column '{column}' NULL rate {direction}: "
            f"{stored_rate:.1%} -> {current_rate:.1%} "
            f"(Δ{delta:.1%})"
        ),
        severity,
        stored_rate,
        current_rate,
    ))
    return anomalies


def _severity(delta: float, increased: bool) -> Severity | None:
    """Classify a NULL rate change by its size in percentage points.

    A drop is capped at WARNING: fewer NULLs is also what a fixed column looks
    like, and a CRITICAL reading is never learned (#116), so a CRITICAL drop
    would fail every run after the fix.
    """
    if delta >= NULL_RATE_CRITICAL_DELTA:
        return Severity.CRITICAL if increased else Severity.WARNING
    if delta >= NULL_RATE_WARNING_DELTA:
        return Severity.WARNING
    return None


def _null_rate_anomaly(
    table: str,
    column: str,
    message: str,
    severity: Severity,
    baseline: float,
    current: float,
) -> dict:
    return make_anomaly(
        table,
        NULL_RATE_CHANGE,
        message,
        severity,
        column=column,
        metric=METRIC_NULL_RATE,
        baseline=baseline,
        current=current,
    )
