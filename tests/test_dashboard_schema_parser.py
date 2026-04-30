"""Tests for the schema-drift message parser (path A regex helpers)."""

from scherlok.dashboard.schema_parser import (
    is_schema_anomaly,
    parse_schema_anomaly,
)


def test_parse_column_removed():
    a = {"type": "column_removed", "message": "Column 'stock' was removed"}
    entry = parse_schema_anomaly(a)
    assert entry is not None
    assert entry.kind == "removed"
    assert entry.column == "stock"
    assert entry.old_type is None
    assert entry.new_type is None


def test_parse_column_added():
    a = {
        "type": "column_added",
        "message": "Column 'refunded_at' was added (type: timestamp with time zone)",
    }
    entry = parse_schema_anomaly(a)
    assert entry is not None
    assert entry.kind == "added"
    assert entry.column == "refunded_at"
    assert entry.new_type == "timestamp with time zone"


def test_parse_type_changed():
    a = {
        "type": "type_changed",
        "message": "Column 'price' type changed: numeric -> numeric(12,2)",
    }
    entry = parse_schema_anomaly(a)
    assert entry is not None
    assert entry.kind == "changed"
    assert entry.column == "price"
    assert entry.old_type == "numeric"
    assert entry.new_type == "numeric(12,2)"


def test_parse_unrelated_anomaly_returns_none():
    a = {"type": "volume_drop", "message": "Row count dropped 60.0% (10 -> 4)"}
    assert parse_schema_anomaly(a) is None


def test_parse_malformed_message_returns_none():
    a = {"type": "column_removed", "message": "this is not the right format"}
    assert parse_schema_anomaly(a) is None


def test_is_schema_anomaly_true_for_three_types():
    assert is_schema_anomaly({"type": "column_added"})
    assert is_schema_anomaly({"type": "column_removed"})
    assert is_schema_anomaly({"type": "type_changed"})


def test_is_schema_anomaly_false_for_other():
    assert not is_schema_anomaly({"type": "volume_drop"})
    assert not is_schema_anomaly({"type": "freshness_miss"})
    assert not is_schema_anomaly({})
