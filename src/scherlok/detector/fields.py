"""Structured anomaly fields (#108) and the builder every detector uses.

Next to ``table``, ``type``, ``message`` and ``severity``, every anomaly
carries the facts its message is written from, so a consumer can act on it
without parsing the sentence:

- ``column``: the column involved, or None for a table-level anomaly.
- ``metric``: what was measured, one of the ``METRIC_*`` names below.
- ``baseline``: the value the detector compared against: the stored profile,
  or the learned median when there is enough history. None when there is no
  baseline (a new table, an added column).
- ``current``: the value measured on this run. None when there is none (a
  removed column).
- ``change_pct``: the signed relative change from ``baseline`` to ``current``
  in percent, rounded to one decimal; negative when ``current`` is lower.
  None unless both are numbers and ``baseline`` is not zero.
- ``hint``: a fixed sentence for the anomaly type, from ``hints.py``.

``baseline`` and ``current`` are aggregates (counts, rates, means, hours) or
schema facts (a column type), never a value read from a row.
"""

from collections.abc import Mapping
from math import isfinite
from typing import Any

from scherlok.detector.hints import hint_for
from scherlok.detector.severity import Severity

COLUMN = "column"
METRIC = "metric"
BASELINE = "baseline"
CURRENT = "current"
CHANGE_PCT = "change_pct"
HINT = "hint"

DETAIL_FIELDS = (COLUMN, METRIC, BASELINE, CURRENT, CHANGE_PCT, HINT)

METRIC_ROW_COUNT = "row_count"
METRIC_COLUMN_TYPE = "column_type"
METRIC_HOURS_SINCE_UPDATE = "hours_since_update"
METRIC_NULL_RATE = "null_rate"
METRIC_NULL_COUNT = "null_count"
METRIC_MEAN = "mean"
METRIC_DISTINCT_COUNT = "distinct_count"
# Distinct values per non-NULL row, compared on near-unique columns.
METRIC_DISTINCT_RATIO = "distinct_ratio"
# Reconcile (scherlok/reconcile.py): key values on more than one row, keys of
# one table the other lacks, and a column's total.
METRIC_DUPLICATE_KEYS = "duplicate_keys"
METRIC_MISSING_KEYS = "missing_keys"
METRIC_EXTRA_KEYS = "extra_keys"
METRIC_SUM = "sum"

CHANGE_PCT_DIGITS = 1


def make_anomaly(
    table: str,
    anomaly_type: str,
    message: str,
    severity: Severity,
    *,
    column: str | None = None,
    metric: str | None = None,
    baseline: Any = None,
    current: Any = None,
) -> dict[str, Any]:
    """Build one anomaly dict; ``change_pct`` and ``hint`` are derived, never passed."""
    return {
        "table": table,
        "type": anomaly_type,
        "message": message,
        "severity": severity,
        COLUMN: column,
        METRIC: metric,
        BASELINE: baseline,
        CURRENT: current,
        CHANGE_PCT: percent_change(baseline, current),
        HINT: hint_for(anomaly_type),
    }


def percent_change(baseline: Any, current: Any) -> float | None:
    """Signed change from ``baseline`` to ``current`` in percent, one decimal.

    Divides by ``abs(baseline)`` so the sign follows the direction of the
    change even for a negative mean. None for a zero or non-numeric value.
    """
    if not (_is_number(baseline) and _is_number(current)) or baseline == 0:
        return None
    return round((current - baseline) / abs(baseline) * 100, CHANGE_PCT_DIGITS)


def anomaly_details(anomaly: Mapping[str, Any]) -> dict[str, Any]:
    """The structured fields of ``anomaly``, each None when it is absent.

    Anomalies saved before these fields existed have none of them.
    """
    return {field: anomaly.get(field) for field in DETAIL_FIELDS}


def _is_number(value: Any) -> bool:
    return isinstance(value, int | float) and not isinstance(value, bool) and isfinite(value)
