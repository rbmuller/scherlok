"""Deterministic fix hints: one fixed sentence per anomaly type (#108).

A hint says where to look first, so an agent can act on an anomaly without
an LLM call; ``--explain`` remains the place for a reasoned hypothesis. Hints
are constant strings, never formatted with a profile, so they cannot carry a
value read from the warehouse.
"""

from scherlok.detector.anomaly_types import (
    ALL_NULL_COLUMN,
    CARDINALITY_CHANGE,
    COLUMN_ADDED,
    COLUMN_REMOVED,
    CONSTANT_COLUMN,
    DISTRIBUTION_SHIFT,
    DUPLICATE_KEY,
    DUPLICATE_KEYS,
    EMPTY_TABLE,
    EXTRA_KEYS,
    FRESHNESS_CRITICAL,
    FRESHNESS_STALE,
    MISSING_KEYS,
    NULL_RATE_CHANGE,
    ROW_COUNT_MISMATCH,
    SUM_MISMATCH,
    TABLE_EMPTY,
    TYPE_CHANGED,
    VOLUME_DROP,
    VOLUME_SPIKE,
)

ANOMALY_HINTS: dict[str, str] = {
    VOLUME_DROP: (
        "Rows were deleted or filtered out; check joins and WHERE clauses changed in this build."
    ),
    VOLUME_SPIKE: (
        "Rows were duplicated or loaded twice; check for a join that fans out or a repeated load."
    ),
    TABLE_EMPTY: (
        "The table lost every row; check whether its last load truncated it or read an empty "
        "source."
    ),
    COLUMN_REMOVED: (
        "A column was dropped or renamed; restore it or update every query that still reads it."
    ),
    COLUMN_ADDED: (
        "A new column appeared; confirm the change is intended and update models or contracts "
        "that list columns."
    ),
    TYPE_CHANGED: (
        "A column changed type; check casts, comparisons and joins that rely on the old type."
    ),
    FRESHNESS_STALE: (
        "The table is updating later than usual; check the schedule and run time of the job "
        "that loads it."
    ),
    FRESHNESS_CRITICAL: (
        "The table has stopped updating; check whether the job that loads it failed or was "
        "disabled."
    ),
    NULL_RATE_CHANGE: (
        "The share of NULLs in this column moved sharply; check outer joins, renamed source "
        "fields and COALESCE defaults."
    ),
    DISTRIBUTION_SHIFT: (
        "The column's mean moved sharply; check unit or currency conversions and source "
        "segments added or lost."
    ),
    CARDINALITY_CHANGE: (
        "The number of distinct values changed sharply; check for duplicated keys, a broken "
        "join or a new set of categories."
    ),
    EMPTY_TABLE: (
        "The new table has no rows; check that its build reads the right source and that its "
        "filters keep data."
    ),
    ALL_NULL_COLUMN: (
        "The column is NULL in every row; check that it is mapped from a source field that "
        "exists and is populated."
    ),
    DUPLICATE_KEY: (
        "The key column repeats values; check for a join that fans out and deduplicate before "
        "loading."
    ),
    CONSTANT_COLUMN: (
        "The column holds one value in every row; check for a hard-coded default or a join "
        "that matched a single row."
    ),
    ROW_COUNT_MISMATCH: (
        "Rows were added or lost between source and target; check the joins and filters of the "
        "build."
    ),
    DUPLICATE_KEYS: (
        "A join fans out; check that every table joined into the target is unique on its join key."
    ),
    MISSING_KEYS: (
        "Source rows were dropped; check inner joins and WHERE filters that can exclude them."
    ),
    EXTRA_KEYS: (
        "The target holds keys the source lacks; check for a stale incremental load, a union or a "
        "bad join."
    ),
    SUM_MISMATCH: (
        "A total changed; check joins that drop or repeat rows, unit or currency conversions and "
        "rounding."
    ),
}


def hint_for(anomaly_type: str | None) -> str | None:
    """The fixed hint for ``anomaly_type``, or None for a type without one."""
    if anomaly_type is None:
        return None
    return ANOMALY_HINTS.get(anomaly_type)
