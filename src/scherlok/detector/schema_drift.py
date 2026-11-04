"""Schema change detection — added/removed columns, type changes."""

from scherlok.detector.anomaly_types import COLUMN_ADDED, COLUMN_REMOVED, TYPE_CHANGED
from scherlok.detector.fields import METRIC_COLUMN_TYPE, make_anomaly
from scherlok.detector.severity import classify_schema_drift


def detect_schema_drift(
    table: str,
    current_schema: dict,
    stored_schema: dict,
) -> list[dict]:
    """Compare current schema vs stored schema.

    Returns a list of anomaly dicts for each detected drift. ``baseline`` and
    ``current`` are the column's stored and current types, None on the side
    where the column does not exist.
    """
    anomalies: list[dict] = []

    current_cols = {c["name"]: c for c in current_schema.get("columns", [])}
    stored_cols = {c["name"]: c for c in stored_schema.get("columns", [])}

    current_names = set(current_cols.keys())
    stored_names = set(stored_cols.keys())

    # Removed columns
    for col in sorted(stored_names - current_names):
        anomalies.append(_schema_anomaly(
            table,
            COLUMN_REMOVED,
            f"Column '{col}' was removed",
            col,
            stored_cols[col]["type"],
            None,
        ))

    # Added columns
    for col in sorted(current_names - stored_names):
        col_info = current_cols[col]
        anomalies.append(_schema_anomaly(
            table,
            COLUMN_ADDED,
            f"Column '{col}' was added (type: {col_info['type']})",
            col,
            None,
            col_info["type"],
        ))

    # Type changes
    for col in sorted(current_names & stored_names):
        if current_cols[col]["type"] != stored_cols[col]["type"]:
            anomalies.append(_schema_anomaly(
                table,
                TYPE_CHANGED,
                (
                    f"Column '{col}' type changed: "
                    f"{stored_cols[col]['type']} -> {current_cols[col]['type']}"
                ),
                col,
                stored_cols[col]["type"],
                current_cols[col]["type"],
            ))

    return anomalies


def _schema_anomaly(
    table: str,
    anomaly_type: str,
    message: str,
    column: str,
    stored_type: str | None,
    current_type: str | None,
) -> dict:
    return make_anomaly(
        table,
        anomaly_type,
        message,
        classify_schema_drift(),
        column=column,
        metric=METRIC_COLUMN_TYPE,
        baseline=stored_type,
        current=current_type,
    )
