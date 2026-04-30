"""Snowflake connector using snowflake-connector-python."""

from datetime import datetime
from typing import Any

from scherlok.connectors.base import BaseConnector


class SnowflakeConnector(BaseConnector):
    """Connector for Snowflake.

    Connection string format:
        snowflake://account/database/schema

    Requires snowflake-connector-python:
        pip install scherlok[snowflake]

    Authentication via environment variables:
        SNOWFLAKE_USER       — required
        SNOWFLAKE_PASSWORD   — required (or use private key auth)
        SNOWFLAKE_WAREHOUSE  — recommended (default warehouse)
        SNOWFLAKE_ROLE       — optional
    """

    def __init__(self, connection_string: str) -> None:
        super().__init__(connection_string)
        self._conn: Any = None
        self._account: str = ""
        self._database: str = ""
        self._schema: str = ""
        self._parse_connection_string()

    def _parse_connection_string(self) -> None:
        """Parse snowflake://account/database/schema into components."""
        path = self.connection_string.replace("snowflake://", "")
        parts = path.strip("/").split("/")
        if len(parts) < 3:
            raise ValueError(
                "Snowflake connection string must be: snowflake://account/database/schema"
            )
        self._account = parts[0]
        self._database = parts[1]
        self._schema = parts[2]

    def connect(self) -> bool:
        """Validate and establish connection to Snowflake."""
        import os

        try:
            import snowflake.connector
        except ImportError:
            self._last_error = (
                "snowflake-connector-python not installed\n"
                "  Hint: pip install scherlok[snowflake]"
            )
            return False

        user = os.environ.get("SNOWFLAKE_USER")
        password = os.environ.get("SNOWFLAKE_PASSWORD")
        if not user or not password:
            missing = []
            if not user:
                missing.append("SNOWFLAKE_USER")
            if not password:
                missing.append("SNOWFLAKE_PASSWORD")
            self._last_error = (
                f"missing required env var{'s' if len(missing) > 1 else ''}: {', '.join(missing)}\n"
                f"  Hint: export {missing[0]}=... (and SNOWFLAKE_WAREHOUSE/ROLE if needed)"
            )
            return False

        try:
            self._conn = snowflake.connector.connect(
                account=self._account,
                user=user,
                password=password,
                database=self._database,
                schema=self._schema,
                warehouse=os.environ.get("SNOWFLAKE_WAREHOUSE"),
                role=os.environ.get("SNOWFLAKE_ROLE"),
            )
            # Validate connection
            cur = self._conn.cursor()
            cur.execute("SELECT CURRENT_VERSION()")
            cur.fetchone()
            cur.close()
            return True
        except Exception as exc:
            self._last_error = self._classify_error(str(exc))
            return False

    @staticmethod
    def _classify_error(message: str) -> str:
        """Map Snowflake error text to a short, actionable hint."""
        msg = message.strip()
        lowered = msg.lower()
        if "incorrect username or password" in lowered or "authentication" in lowered:
            return (
                "authentication failed — check SNOWFLAKE_USER and SNOWFLAKE_PASSWORD\n"
                "  Hint: if your account has SSO/MFA, key-pair auth is required"
            )
        if "account" in lowered and ("not found" in lowered or "does not exist" in lowered):
            return (
                "account not found — check the connection string format\n"
                "  Hint: snowflake://<account>/<database>/<schema> "
                "(account looks like 'xy12345.us-east-1')"
            )
        if "warehouse" in lowered and ("not found" in lowered or "does not exist" in lowered):
            return (
                "warehouse not found — set SNOWFLAKE_WAREHOUSE to a valid warehouse\n"
                "  Hint: SHOW WAREHOUSES in Snowflake to list available ones"
            )
        if "database" in lowered and "does not exist" in lowered:
            return "database not found — check the database name in your connection string"
        first_line = msg.splitlines()[0] if msg else "unknown error"
        return first_line

    def _query(self, sql: str) -> list[dict]:
        """Execute a query and return results as list of dicts."""
        cur = self._conn.cursor(dict_cursor=True) if hasattr(
            self._conn.cursor(), "dict_cursor"
        ) else self._conn.cursor()
        cur.execute(sql)
        cols = [c[0].lower() for c in cur.description]
        rows = [dict(zip(cols, row, strict=False)) for row in cur.fetchall()]
        cur.close()
        return rows

    def list_tables(self) -> list[str]:
        """List all tables and views in the schema."""
        sql = (
            f"SELECT table_name FROM {self._database}.information_schema.tables "
            f"WHERE table_schema = '{self._schema.upper()}' "
            f"AND table_type IN ('BASE TABLE', 'VIEW') "
            f"ORDER BY table_name"
        )
        rows = self._query(sql)
        return [r["table_name"].lower() for r in rows]

    def get_row_count(self, table: str) -> int:
        """Return row count for a table."""
        fqn = f'"{self._database}"."{self._schema}"."{table.upper()}"'
        rows = self._query(f"SELECT COUNT(*) AS cnt FROM {fqn}")
        return int(rows[0]["cnt"]) if rows else 0

    def get_columns(self, table: str) -> list[dict]:
        """Return column metadata from INFORMATION_SCHEMA."""
        sql = (
            f"SELECT column_name, data_type, is_nullable "
            f"FROM {self._database}.information_schema.columns "
            f"WHERE table_schema = '{self._schema.upper()}' "
            f"AND table_name = '{table.upper()}' "
            f"ORDER BY ordinal_position"
        )
        rows = self._query(sql)
        return [
            {
                "name": r["column_name"].lower(),
                "type": r["data_type"],
                "nullable": r["is_nullable"] == "YES",
            }
            for r in rows
        ]

    def get_column_stats(self, table: str, column: str) -> dict:
        """Calculate statistics for a column."""
        fqn = f'"{self._database}"."{self._schema}"."{table.upper()}"'
        col = f'"{column.upper()}"'
        stats: dict[str, Any] = {}

        # Null and distinct counts
        sql = (
            f"SELECT "
            f"  COUNT_IF({col} IS NULL) AS null_count, "
            f"  COUNT(DISTINCT {col}) AS distinct_count "
            f"FROM {fqn}"
        )
        rows = self._query(sql)
        if rows:
            stats["null_count"] = int(rows[0]["null_count"])
            stats["distinct_count"] = int(rows[0]["distinct_count"])
        else:
            stats["null_count"] = 0
            stats["distinct_count"] = 0

        # Numeric stats
        try:
            sql = (
                f"SELECT "
                f"  AVG(TRY_CAST({col} AS FLOAT)) AS mean, "
                f"  STDDEV(TRY_CAST({col} AS FLOAT)) AS stddev, "
                f"  CAST(MIN({col}) AS VARCHAR) AS min, "
                f"  CAST(MAX({col}) AS VARCHAR) AS max "
                f"FROM {fqn}"
            )
            rows = self._query(sql)
            if rows:
                stats["mean"] = rows[0]["mean"]
                stats["stddev"] = rows[0]["stddev"]
                stats["min"] = rows[0]["min"]
                stats["max"] = rows[0]["max"]
            else:
                stats["mean"] = stats["stddev"] = stats["min"] = stats["max"] = None
        except Exception:
            stats["mean"] = stats["stddev"] = stats["min"] = stats["max"] = None

        # Top values
        try:
            sql = (
                f"SELECT CAST({col} AS VARCHAR) AS val, COUNT(*) AS cnt "
                f"FROM {fqn} "
                f"WHERE {col} IS NOT NULL "
                f"GROUP BY {col} ORDER BY cnt DESC LIMIT 5"
            )
            rows = self._query(sql)
            stats["top_values"] = [
                {"value": r["val"], "count": int(r["cnt"])} for r in rows
            ]
        except Exception:
            stats["top_values"] = []

        return stats

    def get_last_modified(self, table: str) -> datetime | None:
        """Return last modification time from INFORMATION_SCHEMA.TABLES."""
        sql = (
            f"SELECT last_altered "
            f"FROM {self._database}.information_schema.tables "
            f"WHERE table_schema = '{self._schema.upper()}' "
            f"AND table_name = '{table.upper()}'"
        )
        try:
            rows = self._query(sql)
            if rows and rows[0]["last_altered"]:
                ts = rows[0]["last_altered"]
                if isinstance(ts, datetime):
                    return ts
                return datetime.fromisoformat(str(ts))
        except Exception:
            pass
        return None
