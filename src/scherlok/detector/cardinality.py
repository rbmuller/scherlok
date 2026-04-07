"""Cardinality change detection — unexpected changes in distinct value counts."""

from scherlok.detector.severity import Severity

# Percentage thresholds for cardinality change
CARDINALITY_WARNING_PCT = 50   # 50% change
CARDINALITY_CRITICAL_PCT = 200  # 3x change (e.g. status went from 5 to 500)


def detect_cardinality_anomalies(
    table: str,
    column: str,
    current_dist: dict,
    stored_dist: dict,
) -> list[dict]:
    """Compare current distinct count against stored profile for a column.

    Returns anomalies when cardinality changes significantly.
    """
    anomalies: list[dict] = []

    current_card = current_dist.get("distinct_count")
    stored_card = stored_dist.get("distinct_count")

    if current_card is None or stored_card is None:
        return anomalies

    if stored_card == 0:
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
