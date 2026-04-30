"""Tests for the per-table incident grouping."""

from scherlok.dashboard.grouping import group_anomalies


def _anomaly(table, atype, severity, message=""):
    return {"table": table, "type": atype, "severity": severity, "message": message}


def test_single_table_volume_drop():
    incidents = group_anomalies([
        _anomaly("orders", "volume_drop", "CRITICAL", "Row count dropped 60%"),
    ])
    assert len(incidents) == 1
    inc = incidents[0]
    assert inc.table == "orders"
    assert inc.severity == "CRITICAL"
    assert len(inc.symptoms) == 1


def test_max_severity_wins_for_card():
    """Card severity = max severity of any symptom on the table."""
    incidents = group_anomalies([
        _anomaly("orders", "volume_drop", "CRITICAL"),
        _anomaly("orders", "cardinality_change", "WARNING"),
        _anomaly("orders", "cardinality_change", "WARNING"),
    ])
    assert len(incidents) == 1
    assert incidents[0].severity == "CRITICAL"
    assert len(incidents[0].symptoms) == 3


def test_schema_drift_anomalies_collapse_into_single_symptom():
    """3 schema-drift anomalies for one table -> 1 symptom with 3 diff entries."""
    incidents = group_anomalies([
        _anomaly("products", "type_changed", "CRITICAL",
                 "Column 'price' type changed: numeric -> numeric(12,2)"),
        _anomaly("products", "column_added", "CRITICAL",
                 "Column 'refunded_at' was added (type: timestamptz)"),
        _anomaly("products", "column_removed", "CRITICAL",
                 "Column 'stock' was removed"),
    ])
    assert len(incidents) == 1
    inc = incidents[0]
    assert len(inc.symptoms) == 1
    sym = inc.symptoms[0]
    assert sym.type == "schema_drift"
    assert len(sym.schema_diff) == 3
    kinds = {e.kind for e in sym.schema_diff}
    assert kinds == {"added", "removed", "changed"}


def test_multiple_tables_yield_multiple_incidents():
    incidents = group_anomalies([
        _anomaly("orders", "volume_drop", "CRITICAL"),
        _anomaly("users", "cardinality_change", "WARNING"),
    ])
    assert len(incidents) == 2
    tables = {i.table for i in incidents}
    assert tables == {"orders", "users"}


def test_sort_critical_first_then_alphabetical():
    incidents = group_anomalies([
        _anomaly("alpha", "volume_drop", "WARNING"),
        _anomaly("zulu", "volume_drop", "CRITICAL"),
        _anomaly("bravo", "cardinality_change", "WARNING"),
    ])
    assert [i.table for i in incidents] == ["zulu", "alpha", "bravo"]


def test_summary_for_volume_plus_cardinality():
    incidents = group_anomalies([
        _anomaly("orders", "volume_drop", "CRITICAL"),
        _anomaly("orders", "cardinality_change", "WARNING"),
    ])
    assert "Volume drop" in incidents[0].summary
    assert "cardinality" in incidents[0].summary.lower()


def test_summary_for_pure_schema_drift():
    incidents = group_anomalies([
        _anomaly("products", "column_removed", "CRITICAL", "Column 'stock' was removed"),
    ])
    assert "Schema drift" in incidents[0].summary
