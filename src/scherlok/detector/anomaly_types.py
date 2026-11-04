"""Names of every anomaly type a detector can report, the ``type`` of an anomaly."""

# Volume (anomaly.py)
VOLUME_DROP = "volume_drop"
VOLUME_SPIKE = "volume_spike"
TABLE_EMPTY = "table_empty"

# Schema drift (schema_drift.py)
COLUMN_REMOVED = "column_removed"
COLUMN_ADDED = "column_added"
TYPE_CHANGED = "type_changed"

# Freshness (freshness.py)
FRESHNESS_STALE = "freshness_stale"
FRESHNESS_CRITICAL = "freshness_critical"

# Per-column distribution (nullability.py, distribution_shift.py, cardinality.py)
NULL_RATE_CHANGE = "null_rate_change"
DISTRIBUTION_SHIFT = "distribution_shift"
CARDINALITY_CHANGE = "cardinality_change"

# First profile of a table, no baseline yet (new_table.py)
EMPTY_TABLE = "empty_table"
ALL_NULL_COLUMN = "all_null_column"
DUPLICATE_KEY = "duplicate_key"
CONSTANT_COLUMN = "constant_column"

ANOMALY_TYPES = frozenset({
    VOLUME_DROP,
    VOLUME_SPIKE,
    TABLE_EMPTY,
    COLUMN_REMOVED,
    COLUMN_ADDED,
    TYPE_CHANGED,
    FRESHNESS_STALE,
    FRESHNESS_CRITICAL,
    NULL_RATE_CHANGE,
    DISTRIBUTION_SHIFT,
    CARDINALITY_CHANGE,
    EMPTY_TABLE,
    ALL_NULL_COLUMN,
    DUPLICATE_KEY,
    CONSTANT_COLUMN,
})
