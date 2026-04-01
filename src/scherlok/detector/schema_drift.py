"""Schema change detection — added/removed columns, type changes."""

from scherlok.detector.severity import Severity, classify_schema_drift


def detect_schema_drift(
    table: str,
    current_schema: dict,
    stored_schema: dict,
) -> list[dict]:
    """Compare current schema vs stored schema.

    Returns a list of anomaly dicts for each detected drift.
    """
    anomalies: list[dict] = []

    current_cols = {c["name"]: c for c in current_schema.get("columns", [])}
    stored_cols = {c["name"]: c for c in stored_schema.get("columns", [])}

    current_names = set(current_cols.keys())
    stored_names = set(stored_cols.keys())

    # Removed columns
    for col in sorted(stored_names - current_names):
        anomalies.append({
            "table": table,
            "type": "column_removed",
            "message": f"Column '{col}' was removed",
            "severity": classify_schema_drift(),
        })

    # Added columns
    for col in sorted(current_names - stored_names):
        col_info = current_cols[col]
        anomalies.append({
            "table": table,
            "type": "column_added",
            "message": f"Column '{col}' was added (type: {col_info['type']})",
            "severity": classify_schema_drift(),
        })

    # Type changes
    for col in sorted(current_names & stored_names):
        if current_cols[col]["type"] != stored_cols[col]["type"]:
            anomalies.append({
                "table": table,
                "type": "type_changed",
                "message": (
                    f"Column '{col}' type changed: "
                    f"{stored_cols[col]['type']} -> {current_cols[col]['type']}"
                ),
                "severity": classify_schema_drift(),
            })

    return anomalies
