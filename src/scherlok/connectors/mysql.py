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

from scherlok.connectors.base import BaseConnector, RelationNamespace

# DATA_TYPE values whose mean means something. MySQL's AVG also accepts dates,
# text and booleans (TINYINT(1)) without an error, so their means were noise (#138).
NUMERIC_DATA_TYPES = frozenset(
    {"tinyint", "smallint", "mediumint", "int", "bigint", "decimal", "float", "double"}
)
BOOLEAN_COLUMN_TYPE = "tinyint(1)"
# TIMESTAMP values arrive in the session's time zone; in UTC, freshness reads
# them right. DATETIME values arrive as written either way.
UTC_SESSION_SQL = "SET time_zone = '+00:00'"


class MySQLConnector(BaseConnector):
    """Connector for MySQL (and compatible forks such as MariaDB)."""

    # database.table: in MySQL a schema is a database.
    MAX_TABLE_NAME_PARTS = 2

    # information_schema.TABLES.UPDATE_TIME is cached for
    # information_schema_stats_expiry (a day by default) (#139).
    METADATA_IS_LAST_WRITE = False

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
                init_command=UTC_SESSION_SQL,
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

    @staticmethod
    def _quote_identifier(identifier: str) -> str:
        """Quote a MySQL identifier in backticks, doubling any embedded backtick."""
        return f"`{identifier.replace('`', '``')}`"

    def _database(self) -> str:
        """The database named in the connection string."""
        return urlparse(self.connection_string).path.lstrip("/")

    def namespace(self) -> RelationNamespace:
        """In MySQL a schema is a database; dbt names it as the schema."""
        return RelationNamespace(schema=self._database())

    def _fetch_one(self, sql: str) -> dict[str, Any]:
        """Run ``sql`` and return its single row keyed by column alias."""
        with self._cursor() as cur:
            cur.execute(sql)
            return dict(cur.fetchone())

    def list_tables(self) -> list[str]:
        """List all tables and views in the current database."""
        db = self._database()
        with self._cursor() as cur:
            cur.execute(
                "SELECT TABLE_NAME FROM information_schema.TABLES "
                "WHERE TABLE_SCHEMA = %s AND TABLE_TYPE IN ('BASE TABLE', 'VIEW') "
                "ORDER BY TABLE_NAME",
                (db,),
            )
            return [row["TABLE_NAME"] for row in cur.fetchall()]

    def get_row_count(self, table: str) -> int:
        """Return exact row count for a table."""
        quoted_table = self._quote_listed_table(table)
        with self._cursor() as cur:
            cur.execute(f"SELECT COUNT(*) AS cnt FROM {quoted_table}")  # noqa: S608
            return cur.fetchone()["cnt"]

    def get_columns(self, table: str) -> list[dict]:
        """Return column metadata from information_schema."""
        db = self._database()
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
        """Calculate statistics for a column; mean and stddev only for numeric types."""
        quoted_table = self._quote_listed_table(table)
        quoted_column = self._quote_identifier(column)
        stats: dict[str, Any] = {}
        with self._cursor() as cur:
            cur.execute(
                f"SELECT "
                f"  SUM(CASE WHEN {quoted_column} IS NULL THEN 1 ELSE 0 END) AS null_count, "
                f"  COUNT(DISTINCT {quoted_column}) AS distinct_count "
                f"FROM {quoted_table}"
            )
            row = cur.fetchone()
            stats["null_count"] = int(row["null_count"] or 0)
            stats["distinct_count"] = int(row["distinct_count"] or 0)

            if not self._is_numeric(cur, table, column):
                stats["mean"] = None
                stats["stddev"] = None
                return stats

            try:
                cur.execute(
                    f"SELECT "
                    f"  AVG({quoted_column}) AS mean, "
                    f"  STDDEV({quoted_column}) AS stddev "
                    f"FROM {quoted_table}"
                )
                num_row = cur.fetchone()
                mean = num_row["mean"]
                stddev = num_row["stddev"]
                stats["mean"] = float(mean) if mean is not None else None
                stats["stddev"] = float(stddev) if stddev is not None else None
            except pymysql.Error:
                stats["mean"] = None
                stats["stddev"] = None

        return stats

    def _is_numeric(self, cur: Any, table: str, column: str) -> bool:
        """Whether the column's type has a meaningful mean."""
        cur.execute(
            "SELECT DATA_TYPE, COLUMN_TYPE FROM information_schema.COLUMNS "
            "WHERE TABLE_SCHEMA = %s AND TABLE_NAME = %s AND COLUMN_NAME = %s",
            (self._database(), table, column),
        )
        row = cur.fetchone()
        if not row:
            return False
        return (
            row["DATA_TYPE"].lower() in NUMERIC_DATA_TYPES
            and row["COLUMN_TYPE"].lower() != BOOLEAN_COLUMN_TYPE
        )

    def get_last_modified(self, table: str) -> datetime | None:
        """Return last modification time from information_schema.TABLES."""
        db = self._database()
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
