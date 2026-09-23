"""Cardinality change detection — unexpected changes in distinct value counts."""

from collections.abc import Sequence

from scherlok.detector.adaptive import ADAPTIVE_SCORE_THRESHOLD, adaptive_baseline
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
    volume profiles are missing or non-null row counts are zero.
    """
    anomalies: list[dict] = []

    current_card = current_dist.get("distinct_count")
    stored_card = stored_dist.get("distinct_count")

    if current_card is None or stored_card is None:
        return anomalies

    baseline = adaptive_baseline(history, "distinct_count")
    if baseline is not None:
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
        anomalies.append({
            "table": table,
            "type": "cardinality_change",
            "message": (
                f"Column '{column}' distinct values {direction}: "
                f"learned baseline {baseline.center:,.0f} -> {current_card:,} "
                f"({change_pct:.0f}% change; robust score: {score:+.2f})"
            ),
            "severity": severity,
        })
        return anomalies

    if stored_card == 0:
        return anomalies

    # Near-unique path: use distinct ratio when stored was near-unique
    # and both volume profiles provide valid row counts. The denominator is
    # non-null rows (rows - null_count) so NULLing values in a unique column
    # does not read as a uniqueness loss.
    if current_vol is not None and stored_vol is not None:
        stored_rows = stored_vol.get("row_count")
        current_rows = current_vol.get("row_count")
        if (
            isinstance(stored_rows, int)
            and isinstance(current_rows, int)
            and stored_rows > 0
            and current_rows > 0
            and isinstance(stored_card, int)
            and isinstance(current_card, int)
        ):
            stored_nulls = stored_dist.get("null_count", 0) or 0
            current_nulls = current_dist.get("null_count", 0) or 0
            if not isinstance(stored_nulls, int):
                stored_nulls = 0
            if not isinstance(current_nulls, int):
                current_nulls = 0
            stored_denom = stored_rows - stored_nulls
            current_denom = current_rows - current_nulls
            if stored_denom <= 0 or current_denom <= 0:
                pass
            else:
                stored_ratio = stored_card / stored_denom
                if stored_ratio >= UNIQUE_RATIO:
                    current_ratio = current_card / current_denom
                    change_pct = abs(current_ratio - stored_ratio) / stored_ratio * 100
                    direction = "decreased" if current_ratio < stored_ratio else "increased"
                    # Inclusive on both bands to match the absolute path below;
                    # _RATIO_EPS keeps exact decimal boundaries (10%, 50%)
                    # from falling just under on binary floats.
                    if change_pct + _RATIO_EPS >= UNIQUE_RATIO_CRITICAL_PCT:
                        anomalies.append({
                            "table": table,
                            "type": "cardinality_change",
                            "message": (
                                f"Column '{column}' distinct ratio {direction}: "
                                f"{stored_ratio:.3f} -> {current_ratio:.3f} "
                                f"({change_pct:.0f}% change)"
                            ),
                            "severity": Severity.CRITICAL,
                        })
                    elif change_pct + _RATIO_EPS >= UNIQUE_RATIO_WARNING_PCT:
                        anomalies.append({
                            "table": table,
                            "type": "cardinality_change",
                            "message": (
                                f"Column '{column}' distinct ratio {direction}: "
                                f"{stored_ratio:.3f} -> {current_ratio:.3f} "
                                f"({change_pct:.0f}% change)"
                            ),
                            "severity": Severity.WARNING,
                        })
                    return anomalies

    change_pct = abs(current_card - stored_card) / stored_card * 100
    direction = "increased" if current_card > stored_card else "decreased"

    if change_pct >= CARDINALITY_CRITICAL_PCT:
        anomalies.append({
            "table": table,
            "type": "cardinality_change",
            "message": (
                f"Column '{column}' distinct values {direction}: "
                f"{stored_card:,} -> {current_card:,} "
                f"({change_pct:.0f}% change)"
            ),
            "severity": Severity.CRITICAL,
        })
    elif change_pct >= CARDINALITY_WARNING_PCT:
        anomalies.append({
            "table": table,
            "type": "cardinality_change",
            "message": (
                f"Column '{column}' distinct values {direction}: "
                f"{stored_card:,} -> {current_card:,} "
                f"({change_pct:.0f}% change)"
            ),
            "severity": Severity.WARNING,
        })

    return anomalies
