"""Severity scoring for detected anomalies."""

from enum import Enum


class Severity(str, Enum):
    """Anomaly severity levels."""

    INFO = "INFO"
    WARNING = "WARNING"
    CRITICAL = "CRITICAL"


def classify_volume_drop(current: int, previous: int) -> Severity | None:
    """Classify severity of a volume drop.

    Returns None if there is no significant drop.
    """
    if previous == 0:
        return None
    drop_pct = ((previous - current) / previous) * 100
    if drop_pct >= 50:
        return Severity.CRITICAL
    if drop_pct >= 20:
        return Severity.WARNING
    return None


def classify_schema_drift() -> Severity:
    """Schema drift is always critical."""
    return Severity.CRITICAL


def classify_freshness_miss() -> Severity:
    """Freshness misses are warnings."""
    return Severity.WARNING


def classify_distribution_shift(z_score: float) -> Severity:
    """Classify distribution shift based on z-score magnitude.

    Detection starts at |z| > 3 (the classic Shewhart control limit,
    enforced by the detector); 3-5 sigma classifies as INFO, beyond 5
    sigma as WARNING. Deliberately never CRITICAL: a mean shift is a
    symptom for a human to judge, not a CI-blocking failure on its own.
    """
    if abs(z_score) > 5:
        return Severity.WARNING
    return Severity.INFO
