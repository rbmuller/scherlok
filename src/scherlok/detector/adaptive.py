"""Robust statistics for adaptive anomaly baselines."""

from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from itertools import islice
from math import isfinite
from numbers import Number
from statistics import median

ADAPTIVE_HISTORY_LIMIT = 30
MIN_ADAPTIVE_SAMPLES = 5
ADAPTIVE_SCORE_THRESHOLD = 3.0
MAD_NORMALIZATION = 0.6745


@dataclass(frozen=True)
class AdaptiveBaseline:
    """Median and scaled MAD for one historical metric."""

    center: float
    scale: float

    def score(self, value: Number) -> float | None:
        """Return the signed robust score for a finite numeric value."""
        if isinstance(value, bool) or not isinstance(value, Number):
            return None
        try:
            numeric_value = float(value)
        except (TypeError, ValueError, OverflowError):
            return None
        if not isfinite(numeric_value):
            return None
        return (numeric_value - self.center) / self.scale


def adaptive_baseline(
    history: Iterable[Mapping[str, object]] | None,
    metric: str,
) -> AdaptiveBaseline | None:
    """Build a robust baseline from the latest valid historical values.

    The baseline is unavailable until five finite numeric observations exist,
    or when the historical metric has no usable variation.
    """
    if history is None:
        return None

    values: list[float] = []
    for profile in islice(history, ADAPTIVE_HISTORY_LIMIT):
        if not isinstance(profile, Mapping):
            continue
        value = profile.get(metric)
        if isinstance(value, bool) or not isinstance(value, Number):
            continue
        try:
            numeric_value = float(value)
        except (TypeError, ValueError, OverflowError):
            continue
        if isfinite(numeric_value):
            values.append(numeric_value)

    if len(values) < MIN_ADAPTIVE_SAMPLES:
        return None

    center = float(median(values))
    mad = float(median(abs(value - center) for value in values))
    scale = mad / MAD_NORMALIZATION
    if not isfinite(scale) or scale <= 0:
        return None

    return AdaptiveBaseline(center=center, scale=scale)
