"""Check that an output table adds up against its input (`scherlok reconcile`, #111).

Comparing a table with its own history can't see a join that fans out on the
first build, or a total that changes while the row count stays the same.
This compares a pipeline's output (the target) with its input (the source),
in the same database: row counts, a key repeated in the target, keys lost or
added, and column totals.

It reads the warehouse only: nothing is saved to the profile store and no
run is recorded. Every number reported is a count or a total; no query reads
a key value back. A database error is reported by its type and never by its
text, because a conversion error quotes the value it could not convert.

Every finding is CRITICAL: an output that doesn't add up is wrong, not
suspicious, so any finding fails the run.
"""

from __future__ import annotations

import math
from collections.abc import Callable, Sequence
from dataclasses import dataclass, field
from decimal import Decimal
from typing import Any, TypeVar

from scherlok.alerter.exitcode import FAIL_ON_CRITICAL, fails_gate
from scherlok.connectors.base import KEY_STATS, BaseConnector, InvalidTableNameError
from scherlok.detector.anomaly_types import (
    DUPLICATE_KEYS,
    EXTRA_KEYS,
    MISSING_KEYS,
    ROW_COUNT_MISMATCH,
    SUM_MISMATCH,
)
from scherlok.detector.fields import (
    METRIC_DUPLICATE_KEYS,
    METRIC_EXTRA_KEYS,
    METRIC_MISSING_KEYS,
    METRIC_ROW_COUNT,
    METRIC_SUM,
    make_anomaly,
)
from scherlok.detector.severity import Severity
from scherlok.service import ANOMALIES_KEY, anomaly_to_dict, severity_counts

# The finding types, in the order they are reported.
RECONCILE_TYPES = (ROW_COUNT_MISMATCH, DUPLICATE_KEYS, MISSING_KEYS, EXTRA_KEYS, SUM_MISMATCH)

# Row counts, repeated keys and totals compare the source (`baseline`) with
# the target (`current`). Missing and extra keys compare the count found
# (`current`) with the zero expected (`baseline`).
EXPECTED_UNMATCHED_KEYS = 0

# Result keys, shared by `scherlok reconcile --output json` and MCP `reconcile`.
SOURCE_KEY = "source"
TARGET_KEY = "target"
TABLE_KEY = "table"
SUMS_KEY = "sums"
KEY_COLUMNS_KEY = "key"
SUM_COLUMNS_KEY = "sum"

# Upper bound on key and on sum columns, which also bounds the output size.
MAX_COLUMNS = 32

KEY_SEPARATOR = ", "
NULL_TOTAL = "NULL"

T = TypeVar("T")


class ReconcileError(RuntimeError):
    """Raised when a reconcile query fails; the message never quotes the database."""


@dataclass
class TableTotals:
    """Counts and totals of one side of a reconcile, as the connector returned them."""

    table: str
    row_count: int
    null_keys: int | None = None
    distinct_keys: int | None = None
    duplicate_keys: int | None = None
    duplicate_rows: int | None = None
    sums: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        """JSON-safe form: integers stay exact, other totals become floats."""
        return {
            TABLE_KEY: self.table,
            **{name: getattr(self, name) for name in KEY_STATS},
            SUMS_KEY: {column: json_number(total) for column, total in self.sums.items()},
        }


@dataclass
class ReconcileResult:
    """Outcome of reconciling a target table against its source. Nothing is saved."""

    source: TableTotals
    target: TableTotals
    key_columns: list[str]
    sum_columns: list[str]
    tolerance: float
    missing_keys: int | None = None
    extra_keys: int | None = None
    anomalies: list[dict] = field(default_factory=list)

    @property
    def passed(self) -> bool:
        return not fails_gate(self.anomalies, FAIL_ON_CRITICAL)

    def to_dict(self) -> dict[str, Any]:
        """JSON-safe form shared by `scherlok reconcile --output json` and MCP `reconcile`."""
        return {
            "passed": self.passed,
            SOURCE_KEY: self.source.to_dict(),
            TARGET_KEY: self.target.to_dict(),
            KEY_COLUMNS_KEY: list(self.key_columns),
            SUM_COLUMNS_KEY: list(self.sum_columns),
            "tolerance": self.tolerance,
            MISSING_KEYS: self.missing_keys,
            EXTRA_KEYS: self.extra_keys,
            "anomaly_count": len(self.anomalies),
            **severity_counts(self.anomalies),
            ANOMALIES_KEY: [anomaly_to_dict(anomaly) for anomaly in self.anomalies],
        }


def validate_reconcile_args(
    source: str,
    target: str,
    key_columns: Sequence[str] | None,
    sum_columns: Sequence[str] | None,
    tolerance: float,
) -> tuple[list[str], list[str]]:
    """Check the arguments before any query; return the key and sum columns, deduplicated.

    Raises ValueError for an empty or identical table pair, an empty or
    excess column list entry, or a tolerance that is negative or not finite
    (NaN would compare false and pass every total).
    """
    if not source or not target:
        raise ValueError("Both a source and a target table are required.")
    if source == target:
        raise ValueError("Source and target must be different tables.")
    if not math.isfinite(tolerance) or tolerance < 0:
        raise ValueError(f"Tolerance must be a finite number of 0 or more, got {tolerance}.")
    keys = _unique_columns(key_columns, "key")
    sums = _unique_columns(sum_columns, "sum")
    return keys, sums


def reconcile_tables(
    connector: BaseConnector,
    source: str,
    target: str,
    *,
    key_columns: Sequence[str] | None = None,
    sum_columns: Sequence[str] | None = None,
    tolerance: float = 0.0,
) -> ReconcileResult:
    """Compare ``target`` with the ``source`` it was built from, in one database.

    Always compares row counts. With ``key_columns``: keys repeated in the
    target, source keys missing from it and target keys the source lacks;
    rows with a NULL in a key column are counted apart (``null_keys``) and
    never matched. With ``sum_columns``: each column's total, which may
    differ by at most ``tolerance`` (absolute, inclusive; a total with no
    non-NULL value counts as 0). Saves nothing.

    Raises ValueError for invalid arguments or table names, and
    ReconcileError when a query fails.
    """
    keys, sums = validate_reconcile_args(source, target, key_columns, sum_columns, tolerance)
    source_totals = _table_totals(connector, source, keys, sums)
    target_totals = _table_totals(connector, target, keys, sums)
    result = ReconcileResult(
        source=source_totals,
        target=target_totals,
        key_columns=keys,
        sum_columns=sums,
        tolerance=tolerance,
    )
    if keys:
        result.missing_keys = _run_query(
            f"count the keys of {source} missing from {target}",
            connector.count_unmatched_keys, source, target, keys,
        )
        result.extra_keys = _run_query(
            f"count the keys of {target} missing from {source}",
            connector.count_unmatched_keys, target, source, keys,
        )
    result.anomalies = _findings(result)
    return result


def json_number(value: Any) -> int | float | None:
    """A driver's count or total as a JSON number; None for NULL, NaN or infinity."""
    if value is None or isinstance(value, bool):
        return None
    if isinstance(value, int):
        return value
    number = value if isinstance(value, Decimal) else Decimal(repr(float(value)))
    if not number.is_finite():
        return None
    if number == number.to_integral_value():
        return int(number)
    return float(number)


def sums_match(source_total: Any, target_total: Any, tolerance: float) -> bool:
    """True when two totals differ by at most ``tolerance``.

    Compared as decimals, so a DECIMAL column is exact and a float total is
    taken at the value it prints as: 100.01 against 100.0 differs by exactly
    0.01. A NULL total (no non-NULL value) counts as 0. NaN never matches;
    infinity matches only the same infinity.
    """
    source_value = _as_decimal(source_total)
    target_value = _as_decimal(target_total)
    if not (source_value.is_finite() and target_value.is_finite()):
        return source_value == target_value
    return abs(target_value - source_value) <= _as_decimal(tolerance)


def _unique_columns(columns: Sequence[str] | None, label: str) -> list[str]:
    unique = list(dict.fromkeys(columns or ()))
    if not all(unique):
        raise ValueError(f"Empty {label} column name.")
    if len(unique) > MAX_COLUMNS:
        raise ValueError(f"At most {MAX_COLUMNS} {label} columns, got {len(unique)}.")
    return unique


def _run_query(step: str, query: Callable[..., T], *args: Any) -> T:
    """Run one connector query; a failure names the step and the error type only.

    The database's message is dropped because it can quote a row value: a
    key compared across types fails with the value that wouldn't convert.
    """
    try:
        return query(*args)
    except (InvalidTableNameError, NotImplementedError):
        raise
    except Exception as exc:
        raise ReconcileError(
            f"Could not {step} ({type(exc).__name__}). Check that both tables and every "
            "key and sum column exist, and that each key column has the same type in both "
            "tables. The database's message is not shown because it can quote row values."
        ) from exc


def _table_totals(
    connector: BaseConnector, table: str, keys: list[str], sums: list[str]
) -> TableTotals:
    stats = _run_query(
        f"count the rows and keys of {table}", connector.get_key_stats, table, keys
    )
    totals = TableTotals(table=table, **stats)
    if sums:
        totals.sums = _run_query(
            f"sum the columns of {table}", connector.get_column_sums, table, sums
        )
    return totals


def _findings(result: ReconcileResult) -> list[dict]:
    source, target = result.source, result.target
    findings: list[dict] = []

    if source.row_count != target.row_count:
        findings.append(_finding(
            result, ROW_COUNT_MISMATCH,
            f"Row count differs from {source.table}: {source.row_count:,} -> "
            f"{target.row_count:,} ({target.row_count - source.row_count:+,})",
            metric=METRIC_ROW_COUNT, baseline=source.row_count, current=target.row_count,
        ))

    if result.key_columns:
        key_label = _key_label(result.key_columns)
        if target.duplicate_keys:
            findings.append(_finding(
                result, DUPLICATE_KEYS,
                f"{target.duplicate_keys:,} {key_label} values appear more than once "
                f"({target.duplicate_rows:,} extra rows); {source.table} has "
                f"{source.duplicate_keys:,}",
                column=key_label, metric=METRIC_DUPLICATE_KEYS,
                baseline=source.duplicate_keys, current=target.duplicate_keys,
            ))
        if result.missing_keys:
            findings.append(_finding(
                result, MISSING_KEYS,
                f"{result.missing_keys:,} of {source.distinct_keys:,} {key_label} values in "
                f"{source.table} are missing",
                column=key_label, metric=METRIC_MISSING_KEYS,
                baseline=EXPECTED_UNMATCHED_KEYS, current=result.missing_keys,
            ))
        if result.extra_keys:
            findings.append(_finding(
                result, EXTRA_KEYS,
                f"{result.extra_keys:,} of {target.distinct_keys:,} {key_label} values are "
                f"not in {source.table}",
                column=key_label, metric=METRIC_EXTRA_KEYS,
                baseline=EXPECTED_UNMATCHED_KEYS, current=result.extra_keys,
            ))

    for column in result.sum_columns:
        source_total, target_total = source.sums[column], target.sums[column]
        if sums_match(source_total, target_total, result.tolerance):
            continue
        findings.append(_finding(
            result, SUM_MISMATCH,
            f"Total of {column} differs from {source.table}: {format_total(source_total)} -> "
            f"{format_total(target_total)} (tolerance {result.tolerance:g})",
            column=column, metric=METRIC_SUM,
            baseline=json_number(source_total), current=json_number(target_total),
        ))
    return findings


def _finding(
    result: ReconcileResult,
    finding_type: str,
    message: str,
    *,
    metric: str,
    baseline: int | float | None,
    current: int | float | None,
    column: str | None = None,
) -> dict[str, Any]:
    """One CRITICAL anomaly on the target, with the structured fields (#108) filled in."""
    return make_anomaly(
        result.target.table, finding_type, message, Severity.CRITICAL,
        column=column, metric=metric, baseline=baseline, current=current,
    )


def format_total(value: Any) -> str:
    """A total for a message: grouped digits, or NULL when the column had no value."""
    if value is None:
        return NULL_TOTAL
    return f"{value:,}"


def _key_label(key_columns: list[str]) -> str:
    if len(key_columns) == 1:
        return key_columns[0]
    return f"({KEY_SEPARATOR.join(key_columns)})"


def _as_decimal(value: Any) -> Decimal:
    if value is None:
        return Decimal(0)
    if isinstance(value, Decimal):
        return value
    if isinstance(value, int):
        return Decimal(value)
    return Decimal(repr(float(value)))
