"""DuckDB connector using duckdb."""

from datetime import datetime
from typing import Any
from urllib.parse import unquote, urlparse

try:
    import duckdb
except ImportError as exc:
    raise ImportError(
        "duckdb is required for the DuckDB connector. "
        "Install it with: pip install scherlok[duckdb]"
    ) from exc

from scherlok.connectors.base import BaseConnector


class DuckDBConnector(BaseConnector):
    """Connector for local DuckDB databases."""

    def __init__(self, connection_string: str) -> None:
        super().__init__(connection_string)
        self._conn: Any = None

    def connect(self) -> bool:
        """Validate and establish connection to DuckDB."""
        try:
            self._conn = duckdb.connect(self._database_path())
            return True
        except duckdb.Error as exc:
            self._last_error = self._classify_error(str(exc))
            return False

    def _database_path(self) -> str:
        """Return the DuckDB database path from a duckdb:// connection string."""
        parsed = urlparse(self.connection_string)
        if parsed.netloc == ":memory:" or parsed.path == "/:memory:":
            return ":memory:"
        if parsed.scheme == "duckdb" and parsed.netloc:
            raise ValueError(
                f"Invalid duckdb URL: {self.connection_string!r}. "
                "Use 'duckdb:///path/to/file.db' or 'duckdb:///:memory:'."
            )
        return unquote(parsed.path)

    @staticmethod
    def _classify_error(message: str) -> str:
        """Map DuckDB error text to a short, actionable hint."""
        msg = message.strip()
        lowered = msg.lower()
        if "permission denied" in lowered:
            return "permission denied — check database file permissions"
        if "database lock" in lowered or "conflicting lock" in lowered:
            return "database is locked — close other DuckDB connections and retry"
        if "no such file" in lowered or "does not exist" in lowered:
            return "database path does not exist — check the path in your connection string"
        first_line = msg.splitlines()[0] if msg else "unknown error"
        return first_line

    @staticmethod
    def _quote_identifier(identifier: str) -> str:
        """Quote a DuckDB identifier."""
        return f'"{identifier.replace(chr(34), chr(34) * 2)}"'

    def list_tables(self) -> list[str]:
        """List all base tables in the main schema."""
        rows = self._conn.execute(
            "SELECT table_name FROM information_schema.tables "
            "WHERE table_schema = 'main' AND table_type = 'BASE TABLE' "
            "ORDER BY table_name"
        ).fetchall()
        return [row[0] for row in rows]

    def get_row_count(self, table: str) -> int:
        """Return exact row count for a table."""
        quoted_table = self._quote_identifier(table)
        row = self._conn.execute(f"SELECT COUNT(*) AS cnt FROM {quoted_table}").fetchone()
        return int(row[0])

    def get_columns(self, table: str) -> list[dict]:
        """Return column metadata from information_schema."""
        rows = self._conn.execute(
            "SELECT column_name, data_type, is_nullable "
            "FROM information_schema.columns "
            "WHERE table_schema = 'main' AND table_name = ? "
            "ORDER BY ordinal_position",
            (table,),
        ).fetchall()
        return [
            {
                "name": row[0],
                "type": row[1],
                "nullable": row[2] == "YES",
            }
            for row in rows
        ]

    def get_column_stats(self, table: str, column: str) -> dict:
        """Calculate statistics for a numeric or text column."""
        stats: dict[str, Any] = {}
        quoted_table = self._quote_identifier(table)
        quoted_column = self._quote_identifier(column)

        row = self._conn.execute(
            f"SELECT "
            f"  COUNT(*) - COUNT({quoted_column}) AS null_count, "
            f"  COUNT(DISTINCT {quoted_column}) AS distinct_count "
            f"FROM {quoted_table}"
        ).fetchone()
        stats["null_count"] = int(row[0] or 0)
        stats["distinct_count"] = int(row[1] or 0)

        try:
            num_row = self._conn.execute(
                f"SELECT "
                f"  AVG({quoted_column}) AS mean, "
                f"  STDDEV_SAMP({quoted_column}) AS stddev, "
                f"  CAST(MIN({quoted_column}) AS VARCHAR) AS min, "
                f"  CAST(MAX({quoted_column}) AS VARCHAR) AS max "
                f"FROM {quoted_table}"
            ).fetchone()
            stats["mean"] = float(num_row[0]) if num_row[0] is not None else None
            stats["stddev"] = float(num_row[1]) if num_row[1] is not None else None
            stats["min"] = num_row[2]
            stats["max"] = num_row[3]
        except duckdb.Error:
            stats["mean"] = None
            stats["stddev"] = None
            stats["min"] = None
            stats["max"] = None

        try:
            rows = self._conn.execute(
                f"SELECT CAST({quoted_column} AS VARCHAR) AS val, COUNT(*) AS cnt "
                f"FROM {quoted_table} "
                f"WHERE {quoted_column} IS NOT NULL "
                f"GROUP BY {quoted_column} ORDER BY cnt DESC LIMIT 5"
            ).fetchall()
            stats["top_values"] = [{"value": row[0], "count": int(row[1])} for row in rows]
        except duckdb.Error:
            stats["top_values"] = []

        return stats

    def get_last_modified(self, table: str) -> datetime | None:
        """Return None because DuckDB does not track per-table modification time."""
        return None
