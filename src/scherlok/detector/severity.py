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
    """Classify distribution shift based on z-score magnitude."""
    abs_z = abs(z_score)
    if abs_z > 5:
        return Severity.WARNING
    if abs_z > 3:
        return Severity.INFO
    return Severity.INFO
