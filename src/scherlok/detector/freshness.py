"""Freshness anomaly detection — tables not updated within expected cadence."""

from collections.abc import Iterable, Mapping
from itertools import islice
from math import isfinite
from statistics import quantiles

from scherlok.detector.adaptive import ADAPTIVE_HISTORY_LIMIT, MIN_ADAPTIVE_SAMPLES
from scherlok.detector.anomaly_types import FRESHNESS_CRITICAL, FRESHNESS_STALE
from scherlok.detector.fields import METRIC_HOURS_SINCE_UPDATE, make_anomaly
from scherlok.detector.severity import Severity
from scherlok.profiler.freshness import (
    FRESHNESS_COLUMN,
    FRESHNESS_SOURCE,
    HOURS_SINCE_UPDATE,
    LAST_MODIFIED,
    SOURCE_COLUMN,
    SOURCE_METADATA,
)

# If hours_since_update doubles vs the expected age, something may be wrong
FRESHNESS_WARNING_MULTIPLIER = 2.0
FRESHNESS_CRITICAL_MULTIPLIER = 4.0
# Minimum hours to consider stale (avoids false positives on small tables).
# Before the cadence is learned, a stored age under it is not compared; after,
# it is the least age expected, so a table that loads every hour and is
# watched every hour (age ~0.5 h) is still checked (#149).
FRESHNESS_MIN_HOURS = 1.0
# The learned age is the upper quartile of the age each recent load reached,
# not the median: a table loaded more often than it is watched is seen once
# per load, at any age from 0 to its interval, and twice their median is
# about the interval itself.
QUARTILES = 4
UPPER_QUARTILE_INDEX = 2  # quantiles(n=4) returns [Q1, median, Q3]


def detect_freshness_anomalies(
    table: str,
    current_profile: dict,
    stored_profile: dict,
    history: Iterable[Mapping[str, object]] | None = None,
) -> list[dict]:
    """Compare a table's age with the age it is expected to have.

    The expected age is learned from ``history`` (recent freshness profiles,
    newest first) once it holds ``MIN_ADAPTIVE_SAMPLES`` loads read from the
    same source as the current profile (``learned_age``). Until then it is the
    age on the previous run (``stored_profile``), which is skipped when under
    ``FRESHNESS_MIN_HOURS``.

    Ages read from different sources (metadata, or another column) are never
    compared; a profile saved before sources were recorded counts as
    metadata.
    """
    current_hours = current_profile.get(HOURS_SINCE_UPDATE)
    if current_hours is None:
        return []

    source, column = _source(current_profile)
    learned = learned_age(history, (source, column))
    if learned is not None:
        expected = max(learned, FRESHNESS_MIN_HOURS)
    else:
        stored_hours = stored_profile.get(HOURS_SINCE_UPDATE)
        if stored_hours is None or _source(stored_profile) != (source, column):
            return []
        if stored_hours < FRESHNESS_MIN_HOURS:
            return []
        expected = stored_hours

    ratio = current_hours / expected
    basis = f"; based on {column}" if source == SOURCE_COLUMN else ""

    if ratio >= FRESHNESS_CRITICAL_MULTIPLIER:
        anomaly_type = FRESHNESS_CRITICAL
        severity = Severity.CRITICAL
        message = (
            f"Table has not been updated for {current_hours:.1f}h "
            f"(expected ~{expected:.1f}h, {ratio:.1f}x overdue{basis})"
        )
    elif ratio >= FRESHNESS_WARNING_MULTIPLIER:
        anomaly_type = FRESHNESS_STALE
        severity = Severity.WARNING
        message = (
            f"Table update delayed: {current_hours:.1f}h since last update "
            f"(expected ~{expected:.1f}h, {ratio:.1f}x overdue{basis})"
        )
    else:
        return []

    return [make_anomaly(
        table,
        anomaly_type,
        message,
        severity,
        column=column,
        metric=METRIC_HOURS_SINCE_UPDATE,
        baseline=expected,
        current=current_hours,
    )]


def learned_age(
    history: Iterable[Mapping[str, object]] | None,
    source: tuple[str, str | None],
) -> float | None:
    """The age a table usually reaches before its next load, or None while unknown.

    Each load in ``history`` (one ``last_modified``) counts once, at the oldest
    age a run saw it, so neither a stall nor a table watched more often than
    it loads outweighs the other loads: the runs of a stall are one load
    whatever their number. Only profiles read from ``source`` count. None until
    ``MIN_ADAPTIVE_SAMPLES`` loads are known.
    """
    peaks: dict[object, float] = {}
    for profile in islice(history or (), ADAPTIVE_HISTORY_LIMIT):
        load = profile.get(LAST_MODIFIED)
        hours = profile.get(HOURS_SINCE_UPDATE)
        if load is None or not _is_age(hours) or _source(profile) != source:
            continue
        peaks[load] = max(peaks.get(load, 0.0), float(hours))

    if len(peaks) < MIN_ADAPTIVE_SAMPLES:
        return None
    return quantiles(peaks.values(), n=QUARTILES, method="inclusive")[UPPER_QUARTILE_INDEX]


def _is_age(value: object) -> bool:
    """A stored age: a finite, non-negative number of hours, as JSON reads it."""
    return (
        isinstance(value, (int, float))
        and not isinstance(value, bool)
        and isfinite(value)
        and value >= 0
    )


def _source(profile: Mapping[str, object]) -> tuple[str, str | None]:
    """Where a profile's age came from: (source, column)."""
    return profile.get(FRESHNESS_SOURCE) or SOURCE_METADATA, profile.get(FRESHNESS_COLUMN)
