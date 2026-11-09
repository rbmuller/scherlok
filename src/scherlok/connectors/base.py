"""Abstract base connector for database backends."""

from abc import ABC, abstractmethod
from collections.abc import Iterable, Sequence
from datetime import datetime
from typing import Any

# Keys of `get_key_stats`. Counts only: a key value is never read back.
ROW_COUNT = "row_count"
NULL_KEYS = "null_keys"
DISTINCT_KEYS = "distinct_keys"
DUPLICATE_KEYS = "duplicate_keys"
DUPLICATE_ROWS = "duplicate_rows"
KEY_STATS = (ROW_COUNT, NULL_KEYS, DISTINCT_KEYS, DUPLICATE_KEYS, DUPLICATE_ROWS)

TABLE_NAME_SEPARATOR = "."
_DOUBLE_QUOTE = '"'

# Result aliases of the reconcile queries, lower case so every driver returns
# them under the same name.
_TABLE_ROWS = "table_rows"
_KEYED_ROWS = "keyed_rows"
_KEY_ROWS = "key_rows"
_UNMATCHED_KEYS = "unmatched_keys"
_SUM_ALIAS = "sum_{}"
_LATEST_ALIAS = "latest"


class InvalidTableNameError(ValueError):
    """Raised for a table name a connector cannot address, such as `raw..orders`."""


def split_table_name(table: str, max_parts: int) -> list[str]:
    """Split a dotted table name (`orders`, `raw.orders`, `db.raw.orders`) into parts.

    A dot always separates parts, so a table whose own name holds a dot
    can't be addressed this way. Empty parts and more than ``max_parts``
    parts raise ``InvalidTableNameError``.
    """
    parts = table.split(TABLE_NAME_SEPARATOR)
    if len(parts) > max_parts or not all(parts):
        raise InvalidTableNameError(
            f"Invalid table name {table!r}: use up to {max_parts} non-empty, "
            "dot-separated parts, such as schema.table."
        )
    return parts


def quote_ansi_identifier(identifier: str) -> str:
    """Quote an identifier the SQL-standard way: in double quotes, doubling any inside."""
    return f"{_DOUBLE_QUOTE}{identifier.replace(_DOUBLE_QUOTE, _DOUBLE_QUOTE * 2)}{_DOUBLE_QUOTE}"


class BaseConnector(ABC):
    """Base class that all database connectors must implement."""

    # Most dot-separated parts a table name may have in `reconcile`
    # (database.schema.table); see `split_table_name`.
    MAX_TABLE_NAME_PARTS = 3

    def __init__(self, connection_string: str) -> None:
        self.connection_string = connection_string
        self._last_error: str | None = None

    @property
    def last_error(self) -> str | None:
        """Friendly, actionable description of the most recent failure, or None."""
        return self._last_error

    @abstractmethod
    def connect(self) -> bool:
        """Validate and establish a connection. Return True on success.

        On failure, implementations should set ``self._last_error`` to a
        short, actionable string (no traceback) before returning False.
        """
        ...

    @abstractmethod
    def list_tables(self) -> list[str]:
        """List all user tables in the database."""
        ...

    @abstractmethod
    def get_row_count(self, table: str) -> int:
        """Return the row count for a given table."""
        ...

    @abstractmethod
    def get_columns(self, table: str) -> list[dict]:
        """Return column metadata: [{"name", "type", "nullable"}]."""
        ...

    @abstractmethod
    def get_column_stats(self, table: str, column: str) -> dict:
        """Return column statistics: null_count, distinct_count, mean, stddev.

        Aggregates only. Never return a value read from a row (top values,
        min, max): the profile store would keep a plain-text copy of it.
        """
        ...

    @abstractmethod
    def get_last_modified(self, table: str) -> datetime | None:
        """Return the last known modification time for a table, or None."""
        ...

    # Whether `get_last_modified` is the time of the last write. A connector
    # whose metadata is a maintenance time or a cached value says False, and
    # freshness reads a load-time column first (#139).
    METADATA_IS_LAST_WRITE = True

    def get_latest_timestamp(self, table: str, column: str) -> Any:
        """Return ``MAX(column)`` of ``table`` as the driver returns it, in one query.

        Freshness reads it from a load-time column such as ``updated_at`` when
        the warehouse has no usable last-modified time (#87).
        """
        row = self._fetch_one(
            f"SELECT MAX({self._quote_column(column)}) AS {_LATEST_ALIAS} "
            f"FROM {self._quote_table(table)}"
        )
        return row[_LATEST_ALIAS]

    # ------------------------------------------------------------------
    # Reconcile (`scherlok reconcile`): compare an output table with its
    # input. The SQL below is shared and plain ANSI. Each connector quotes
    # identifiers in its own dialect (`_quote_identifier`), so a name is never
    # interpolated raw, and runs the query (`_fetch_one`). Every query returns
    # counts or totals; none reads a key value back.
    # ------------------------------------------------------------------

    def _quote_identifier(self, identifier: str) -> str:
        """Return one identifier quoted in this connector's SQL dialect."""
        raise NotImplementedError(f"{type(self).__name__} does not support reconcile")

    def _fetch_one(self, sql: str) -> dict[str, Any]:
        """Run ``sql`` and return its single row keyed by lower-case column alias."""
        raise NotImplementedError(f"{type(self).__name__} does not support reconcile")

    def _table_name_parts(self, table: str) -> list[str]:
        """The parts of ``table`` to quote; connectors may prepend their defaults."""
        return split_table_name(table, self.MAX_TABLE_NAME_PARTS)

    def _quote_table(self, table: str) -> str:
        """Return ``table`` (optionally dot-qualified) as quoted SQL identifiers."""
        return self._quote_name_parts(self._table_name_parts(table))

    def _quote_name_parts(self, parts: Iterable[str]) -> str:
        """Return ``parts`` as one dot-qualified name, each part quoted on its own."""
        return TABLE_NAME_SEPARATOR.join(self._quote_identifier(part) for part in parts)

    def _quote_column(self, column: str) -> str:
        """Return ``column`` as a quoted SQL identifier."""
        return self._quote_identifier(column)

    def get_key_stats(self, table: str, key_columns: Sequence[str] = ()) -> dict[str, int | None]:
        """Count the rows of ``table`` and, with ``key_columns``, how its key repeats.

        Returns ``row_count``; with key columns also ``null_keys`` (rows with a
        NULL in any key column, which can't match anything), ``distinct_keys``,
        ``duplicate_keys`` (key values on more than one row) and
        ``duplicate_rows`` (rows beyond the first for each key). Without key
        columns those four are None. One query per call.
        """
        quoted_table = self._quote_table(table)
        if not key_columns:
            row = self._fetch_one(f"SELECT COUNT(*) AS {_TABLE_ROWS} FROM {quoted_table}")
            return {ROW_COUNT: int(row[_TABLE_ROWS]), **dict.fromkeys(KEY_STATS[1:])}

        keys = [self._quote_column(column) for column in key_columns]
        key_list = ", ".join(keys)
        keys_present = " AND ".join(f"{key} IS NOT NULL" for key in keys)
        row = self._fetch_one(
            f"SELECT r.{_TABLE_ROWS}, g.{DISTINCT_KEYS}, g.{DUPLICATE_KEYS}, g.{_KEYED_ROWS} "
            f"FROM (SELECT COUNT(*) AS {_TABLE_ROWS} FROM {quoted_table}) AS r "
            "CROSS JOIN ("
            f"SELECT COUNT(*) AS {DISTINCT_KEYS}, "
            f"COALESCE(SUM(CASE WHEN k.{_KEY_ROWS} > 1 THEN 1 ELSE 0 END), 0) AS {DUPLICATE_KEYS}, "
            f"COALESCE(SUM(k.{_KEY_ROWS}), 0) AS {_KEYED_ROWS} "
            f"FROM (SELECT COUNT(*) AS {_KEY_ROWS} FROM {quoted_table} "
            f"WHERE {keys_present} GROUP BY {key_list}) AS k"
            ") AS g"
        )
        row_count = int(row[_TABLE_ROWS])
        distinct_keys = int(row[DISTINCT_KEYS])
        keyed_rows = int(row[_KEYED_ROWS])
        return {
            ROW_COUNT: row_count,
            NULL_KEYS: row_count - keyed_rows,
            DISTINCT_KEYS: distinct_keys,
            DUPLICATE_KEYS: int(row[DUPLICATE_KEYS]),
            DUPLICATE_ROWS: keyed_rows - distinct_keys,
        }

    def get_column_sums(self, table: str, columns: Sequence[str]) -> dict[str, Any]:
        """Return ``{column: SUM(column)}`` for ``table``, in one query.

        Values are what the driver returns (int, float or Decimal), or None
        when a column has no non-NULL value.
        """
        if not columns:
            return {}
        sums = ", ".join(
            f"SUM({self._quote_column(column)}) AS {_SUM_ALIAS.format(index)}"
            for index, column in enumerate(columns)
        )
        row = self._fetch_one(f"SELECT {sums} FROM {self._quote_table(table)}")
        return {column: row[_SUM_ALIAS.format(index)] for index, column in enumerate(columns)}

    def count_unmatched_keys(
        self, table: str, other_table: str, key_columns: Sequence[str]
    ) -> int:
        """Count the distinct keys of ``table`` that ``other_table`` doesn't have.

        An anti-join on every key column. Keys with a NULL part are left out,
        since NULL never equals anything.
        """
        if not key_columns:
            raise ValueError("count_unmatched_keys needs at least one key column.")
        keys = [self._quote_column(column) for column in key_columns]
        selected = ", ".join(f"s.{key}" for key in keys)
        keys_present = " AND ".join(f"s.{key} IS NOT NULL" for key in keys)
        matches = " AND ".join(f"o.{key} = s.{key}" for key in keys)
        row = self._fetch_one(
            f"SELECT COUNT(*) AS {_UNMATCHED_KEYS} FROM ("
            f"SELECT DISTINCT {selected} FROM {self._quote_table(table)} AS s "
            f"WHERE {keys_present} AND NOT EXISTS "
            f"(SELECT 1 FROM {self._quote_table(other_table)} AS o WHERE {matches})"
            ") AS u"
        )
        return int(row[_UNMATCHED_KEYS])
