"""Cardinality change detection — unexpected changes in distinct value counts."""

from collections.abc import Sequence

from scherlok.detector.adaptive import ADAPTIVE_SCORE_THRESHOLD, adaptive_baseline
from scherlok.detector.anomaly_types import CARDINALITY_CHANGE
from scherlok.detector.fields import METRIC_DISTINCT_COUNT, METRIC_DISTINCT_RATIO, make_anomaly
from scherlok.detector.severity import Severity

# Percentage thresholds for cardinality change
CARDINALITY_WARNING_PCT = 50   # 50% change
CARDINALITY_CRITICAL_PCT = 200  # 3x change (e.g. status went from 5 to 500)

# Ratio threshold for historically near-unique columns.
# When stored distinct_count / row_count >= UNIQUE_RATIO the column is
# considered near-unique and we compare distinct ratios instead of absolute
# distinct counts, so a volume drop does not fire cardinality noise.
UNIQUE_RATIO = 0.95
# Ratio changes are bounded to 0-100%, so the absolute cardinality thresholds
# above cannot classify a near-unique column as critical.
UNIQUE_RATIO_WARNING_PCT = 10
UNIQUE_RATIO_CRITICAL_PCT = 50
# Tolerance for binary floating-point boundaries (e.g. 1 - 0.9 is
# 0.09999999999999998, so a clean 10% loss would fall just under the warning
# band without this).
_RATIO_EPS = 1e-9


def detect_cardinality_anomalies(
    table: str,
    column: str,
    current_dist: dict,
    stored_dist: dict,
    current_vol: dict | None = None,
    stored_vol: dict | None = None,
    *,
    history: Sequence[dict] | None = None,
    sampled: bool = False,
) -> list[dict]:
    """Compare current distinct count against stored profile for a column.

    Returns anomalies when cardinality changes significantly.

    When the stored profile shows the column was near-unique
    (distinct_count / non-null rows >= UNIQUE_RATIO) the distinct ratio
    (distinct / non-null rows) is compared instead of the absolute count. A
    unique column that stays unique after a volume change is then silent,
    while a unique column that suddenly has duplicates still fires.
    The denominator excludes NULLs (rows - null_count from each
    distribution profile) so nulling values in a unique column does not
    read as a uniqueness loss. Falls back to absolute comparison when
    volume profiles are missing or non-null row counts are zero. With enough
    history for a learned baseline, the ratio is still checked first, and the
    learned distinct count is compared only when the ratio held.

    ``metric`` says which was compared: ``distinct_ratio`` on the near-unique
    path, ``distinct_count`` otherwise.

    ``sampled=True`` says the current table may hold a sample of the
    baseline's rows (`scherlok verify --ignore volume`, #110), so only what a
    sample can't explain is compared: the distinct ratio of a near-unique
    column, and a gain in distinct values, since a sample can only lose
    them. ``history`` is not used then: the learned spread is the full
    table's run-to-run variation, far tighter than a sample's.
    """
    anomalies: list[dict] = []

    current_card = current_dist.get("distinct_count")
    stored_card = stored_dist.get("distinct_count")

    if current_card is None or stored_card is None:
        return anomalies

    near_unique = _near_unique_anomalies(
        table, column, current_dist, stored_dist, current_vol, stored_vol
    )

    baseline = None if sampled else adaptive_baseline(history, "distinct_count")
    if baseline is not None:
        # Duplicated keys add rows, not distinct values, so the distinct count
        # stays on its learned path and only the ratio shows them (#137).
        if near_unique:
            return near_unique
        score = baseline.score(current_card)
        if score is None or abs(score) <= ADAPTIVE_SCORE_THRESHOLD:
            return anomalies

        if baseline.center == 0:
            change_pct = 0.0
        else:
            change_pct = abs(current_card - baseline.center) / baseline.center * 100
        direction = "increased" if current_card > baseline.center else "decreased"
        severity = (
            Severity.CRITICAL
            if change_pct >= CARDINALITY_CRITICAL_PCT
            else Severity.WARNING
            if change_pct >= CARDINALITY_WARNING_PCT
            else Severity.INFO
        )
        anomalies.append(_cardinality_anomaly(
            table,
            column,
            (
                f"Column '{column}' distinct values {direction}: "
                f"learned baseline {baseline.center:,.0f} -> {current_card:,} "
                f"({change_pct:.0f}% change; robust score: {score:+.2f})"
            ),
            severity,
            METRIC_DISTINCT_COUNT,
            baseline.center,
            current_card,
        ))
        return anomalies

    if stored_card == 0:
        return anomalies

    if near_unique is not None:
        return near_unique

    if sampled and current_card <= stored_card:
        return anomalies

    change_pct = abs(current_card - stored_card) / stored_card * 100
    direction = "increased" if current_card > stored_card else "decreased"

    if change_pct >= CARDINALITY_CRITICAL_PCT:
        severity = Severity.CRITICAL
    elif change_pct >= CARDINALITY_WARNING_PCT:
        severity = Severity.WARNING
    else:
        return anomalies

    anomalies.append(_cardinality_anomaly(
        table,
        column,
        (
            f"Column '{column}' distinct values {direction}: "
            f"{stored_card:,} -> {current_card:,} "
            f"({change_pct:.0f}% change)"
        ),
        severity,
        METRIC_DISTINCT_COUNT,
        stored_card,
        current_card,
    ))
    return anomalies


def _near_unique_anomalies(
    table: str,
    column: str,
    current_dist: dict,
    stored_dist: dict,
    current_vol: dict | None,
    stored_vol: dict | None,
) -> list[dict] | None:
    """Compare distinct ratios when the stored profile was near-unique.

    Returns ``None`` when the column was not near-unique or the counts can't
    be compared, so the caller compares distinct counts instead; otherwise the
    ratio finding, empty when the ratio held. The denominator is non-null rows
    (rows - null_count) so NULLing values in a unique column does not read as
    a uniqueness loss.
    """
    if current_vol is None or stored_vol is None:
        return None
    stored_rows = stored_vol.get("row_count")
    current_rows = current_vol.get("row_count")
    stored_card = stored_dist.get("distinct_count")
    current_card = current_dist.get("distinct_count")
    if not (
        isinstance(stored_rows, int)
        and isinstance(current_rows, int)
        and stored_rows > 0
        and current_rows > 0
        and isinstance(stored_card, int)
        and isinstance(current_card, int)
    ):
        return None

    stored_denom = stored_rows - _null_count(stored_dist)
    current_denom = current_rows - _null_count(current_dist)
    if stored_denom <= 0 or current_denom <= 0:
        return None
    stored_ratio = stored_card / stored_denom
    if stored_ratio < UNIQUE_RATIO:
        return None

    current_ratio = current_card / current_denom
    change_pct = abs(current_ratio - stored_ratio) / stored_ratio * 100
    direction = "decreased" if current_ratio < stored_ratio else "increased"
    # Inclusive on both bands to match the absolute path; _RATIO_EPS keeps
    # exact decimal boundaries (10%, 50%) from falling just under on binary
    # floats.
    if change_pct + _RATIO_EPS >= UNIQUE_RATIO_CRITICAL_PCT:
        severity = Severity.CRITICAL
    elif change_pct + _RATIO_EPS >= UNIQUE_RATIO_WARNING_PCT:
        severity = Severity.WARNING
    else:
        return []
    return [_cardinality_anomaly(
        table,
        column,
        (
            f"Column '{column}' distinct ratio {direction}: "
            f"{stored_ratio:.3f} -> {current_ratio:.3f} "
            f"({change_pct:.0f}% change)"
        ),
        severity,
        METRIC_DISTINCT_RATIO,
        stored_ratio,
        current_ratio,
    )]


def _null_count(dist: dict) -> int:
    nulls = dist.get("null_count", 0) or 0
    return nulls if isinstance(nulls, int) else 0


def _cardinality_anomaly(
    table: str,
    column: str,
    message: str,
    severity: Severity,
    metric: str,
    baseline: float,
    current: float,
) -> dict:
    return make_anomaly(
        table,
        CARDINALITY_CHANGE,
        message,
        severity,
        column=column,
        metric=metric,
        baseline=baseline,
        current=current,
    )
