"""MySQL connector using pymysql."""

from datetime import datetime
from typing import Any
from urllib.parse import urlparse

try:
    import pymysql
    import pymysql.cursors
except ImportError as exc:
    raise ImportError(
        "pymysql is required for the MySQL connector. "
        "Install it with: pip install scherlok[mysql]"
    ) from exc

from scherlok.connectors.base import BaseConnector


class MySQLConnector(BaseConnector):
    """Connector for MySQL (and compatible forks such as MariaDB)."""

    def __init__(self, connection_string: str) -> None:
        super().__init__(connection_string)
        self._conn: Any = None

    def connect(self) -> bool:
        """Validate and establish connection to MySQL."""
        try:
            parsed = urlparse(self.connection_string)
            self._conn = pymysql.connect(
                host=parsed.hostname or "127.0.0.1",
                port=parsed.port or 3306,
                user=parsed.username,
                password=parsed.password or "",
                database=parsed.path.lstrip("/"),
                autocommit=True,
                cursorclass=pymysql.cursors.DictCursor,
            )
            return True
        except pymysql.Error as exc:
            self._last_error = self._classify_error(str(exc))
            return False

    @staticmethod
    def _classify_error(message: str) -> str:
        """Map pymysql error text to a short, actionable hint."""
        msg = message.strip()
        lowered = msg.lower()
        if "connection refused" in lowered or "can't connect" in lowered:
            return (
                "connection refused — is the server reachable?\n"
                "  Hint: check host/port and that MySQL is running"
            )
        if "access denied" in lowered:
            return "wrong username or password — check the credentials in your connection string"
        if "unknown database" in lowered:
            return (
                "database does not exist on the server\n"
                "  Hint: list databases with `SHOW DATABASES;`"
            )
        if "timeout" in lowered:
            return "connection timed out — check the host/port and any firewall in front"
        if "unknown host" in lowered or "name or service not known" in lowered:
            return "host not found — check the hostname in your connection string"
        first_line = msg.splitlines()[0] if msg else "unknown error"
        return first_line

    def _cursor(self) -> Any:
        """Return a DictCursor."""
        return self._conn.cursor()

    def list_tables(self) -> list[str]:
        """List all base tables in the current database."""
        db = urlparse(self.connection_string).path.lstrip("/")
        with self._cursor() as cur:
            cur.execute(
                "SELECT TABLE_NAME FROM information_schema.TABLES "
                "WHERE TABLE_SCHEMA = %s AND TABLE_TYPE = 'BASE TABLE' "
                "ORDER BY TABLE_NAME",
                (db,),
            )
            return [row["TABLE_NAME"] for row in cur.fetchall()]

    def get_row_count(self, table: str) -> int:
        """Return exact row count for a table."""
        with self._cursor() as cur:
            cur.execute(f"SELECT COUNT(*) AS cnt FROM `{table}`")  # noqa: S608
            return cur.fetchone()["cnt"]

    def get_columns(self, table: str) -> list[dict]:
        """Return column metadata from information_schema."""
        db = urlparse(self.connection_string).path.lstrip("/")
        with self._cursor() as cur:
            cur.execute(
                "SELECT COLUMN_NAME, COLUMN_TYPE, IS_NULLABLE "
                "FROM information_schema.COLUMNS "
                "WHERE TABLE_SCHEMA = %s AND TABLE_NAME = %s "
                "ORDER BY ORDINAL_POSITION",
                (db, table),
            )
            return [
                {
                    "name": row["COLUMN_NAME"],
                    "type": row["COLUMN_TYPE"],
                    "nullable": row["IS_NULLABLE"] == "YES",
                }
                for row in cur.fetchall()
            ]

    def get_column_stats(self, table: str, column: str) -> dict:
        """Calculate statistics for a numeric or text column."""
        stats: dict[str, Any] = {}
        with self._cursor() as cur:
            cur.execute(
                f"SELECT "
                f"  SUM(CASE WHEN `{column}` IS NULL THEN 1 ELSE 0 END) AS null_count, "
                f"  COUNT(DISTINCT `{column}`) AS distinct_count "
                f"FROM `{table}`"
            )
            row = cur.fetchone()
            stats["null_count"] = int(row["null_count"] or 0)
            stats["distinct_count"] = int(row["distinct_count"] or 0)


            try:
                cur.execute(
                    f"SELECT "
                    f"  AVG(`{column}`) AS mean, "
                    f"  STDDEV(`{column}`) AS stddev, "
                    f"  MIN(`{column}`) AS min, "
                    f"  MAX(`{column}`) AS max "
                    f"FROM `{table}`"
                )
                num_row = cur.fetchone()
                mean = num_row["mean"]
                stddev = num_row["stddev"]
                stats["mean"] = float(mean) if mean is not None else None
                stats["stddev"] = float(stddev) if stddev is not None else None
                stats["min"] = str(num_row["min"]) if num_row["min"] is not None else None
                stats["max"] = str(num_row["max"]) if num_row["max"] is not None else None
            except pymysql.Error:
                stats["mean"] = None
                stats["stddev"] = None
                stats["min"] = None
                stats["max"] = None

            # Top values
            try:
                cur.execute(
                    f"SELECT `{column}` AS val, COUNT(*) AS cnt "  # noqa: S608
                    f"FROM `{table}` "
                    f"WHERE `{column}` IS NOT NULL "
                    f"GROUP BY `{column}` ORDER BY cnt DESC LIMIT 5"
                )
                stats["top_values"] = [
                    {"value": str(r["val"]), "count": r["cnt"]} for r in cur.fetchall()
                ]
            except pymysql.Error:
                stats["top_values"] = []

        return stats

    def get_last_modified(self, table: str) -> datetime | None:
        """Return last modification time from information_schema.TABLES."""
        db = urlparse(self.connection_string).path.lstrip("/")
        with self._cursor() as cur:
            cur.execute(
                "SELECT UPDATE_TIME FROM information_schema.TABLES "
                "WHERE TABLE_SCHEMA = %s AND TABLE_NAME = %s",
                (db, table),
            )
            row = cur.fetchone()
            if row and row["UPDATE_TIME"]:
                ts = row["UPDATE_TIME"]
                return ts if isinstance(ts, datetime) else datetime.fromisoformat(str(ts))
            return None
