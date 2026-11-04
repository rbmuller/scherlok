"""Local SQLite profile storage."""

import json
import sqlite3
from collections.abc import Callable
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from scherlok.config import SCHERLOK_DIR, make_private_dir

PROFILES_DB = SCHERLOK_DIR / "profiles.db"

DISTRIBUTION_PROFILE_PREFIX = "distribution:"
RAW_VALUE_KEYS = ("top_values", "min", "max")

SCOPE_COLUMN = "scope"
SCOPED_TABLES = ("profiles", "anomalies")
# Limits a read to the store's scope plus the unscoped rows older releases
# wrote; a store opened without a scope reads every row. Takes the scope twice.
SCOPE_FILTER_SQL = f"(? IS NULL OR {SCOPE_COLUMN} IS NULL OR {SCOPE_COLUMN} = ?)"

# The base schema, as the first release wrote it. Never edit it: later columns
# and tables come from MIGRATIONS, so a store of any age ends up the same.
CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS profiles (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    table_name TEXT NOT NULL,
    profile_type TEXT NOT NULL,
    data TEXT NOT NULL,
    created_at TEXT NOT NULL
)
"""

CREATE_INDEX_SQL = """
CREATE INDEX IF NOT EXISTS idx_profiles_lookup
ON profiles (table_name, profile_type, created_at DESC)
"""

CREATE_ANOMALIES_SQL = """
CREATE TABLE IF NOT EXISTS anomalies (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    table_name TEXT NOT NULL,
    anomaly_type TEXT NOT NULL,
    severity TEXT NOT NULL,
    message TEXT NOT NULL,
    detected_at TEXT NOT NULL
)
"""

CREATE_ANOMALIES_INDEX_SQL = """
CREATE INDEX IF NOT EXISTS idx_anomalies_lookup
ON anomalies (table_name, detected_at DESC)
"""


def _strip_raw_distribution_values(conn: sqlite3.Connection) -> None:
    """Drop the column values older releases kept in distribution profiles (#114)."""
    rows = conn.execute(
        "SELECT id, data FROM profiles WHERE profile_type LIKE ?",
        (f"{DISTRIBUTION_PROFILE_PREFIX}%",),
    ).fetchall()
    for row in rows:
        profile = json.loads(row["data"])
        if not any(key in profile for key in RAW_VALUE_KEYS):
            continue
        for key in RAW_VALUE_KEYS:
            profile.pop(key, None)
        conn.execute(
            "UPDATE profiles SET data = ? WHERE id = ?",
            (json.dumps(profile, default=str), row["id"]),
        )


def _add_column(conn: sqlite3.Connection, table: str, column: str, declaration: str) -> None:
    """Add a column unless the table already has it, so a migration can run twice."""
    existing = {row[1] for row in conn.execute(f"PRAGMA table_info({table})")}
    if column not in existing:
        conn.execute(f"ALTER TABLE {table} ADD COLUMN {column} {declaration}")


def _add_scope_columns(conn: sqlite3.Connection) -> None:
    """Record which database each profile and anomaly came from (#110).

    Rows already in the store keep a NULL scope, which matches every
    connection, so an upgrade keeps the history it had.
    """
    for table in SCOPED_TABLES:
        _add_column(conn, table, SCOPE_COLUMN, "TEXT")


# Ordered store migrations. PRAGMA user_version records how many a store has
# applied, so add a migration by appending a function that takes the
# connection; never reorder or remove one. Every migration must be idempotent:
# it also runs on brand-new stores, and runs again if a later step fails.
MIGRATIONS: tuple[Callable[[sqlite3.Connection], None], ...] = (
    _strip_raw_distribution_values,  # user_version 1
    _add_scope_columns,  # user_version 2
)


class ProfileStore:
    """SQLite-backed storage for data profiles.

    ``scope`` names the database the profiles come from (see
    ``service.connection_scope``), so two databases with an ``orders`` table
    keep separate baselines in one store. Saves are tagged with it, and reads
    see the rows of that scope plus the unscoped rows older releases wrote.
    Without a scope, saves are unscoped and reads see every row.
    """

    def __init__(self, db_path: Path | None = None, *, scope: str | None = None) -> None:
        self._db_path = db_path or PROFILES_DB
        self._scope = scope
        make_private_dir(self._db_path.parent)
        self._conn = sqlite3.connect(str(self._db_path))
        self._conn.row_factory = sqlite3.Row
        self._init_db()
        self._migrate()

    @property
    def scope(self) -> str | None:
        """The database this store reads and writes for, or None for every one."""
        return self._scope

    def _scope_params(self) -> tuple[str | None, str | None]:
        """Parameters for one SCOPE_FILTER_SQL."""
        return self._scope, self._scope

    def _init_db(self) -> None:
        """Create tables and indexes if they don't exist."""
        self._conn.execute(CREATE_TABLE_SQL)
        self._conn.execute(CREATE_INDEX_SQL)
        self._conn.execute(CREATE_ANOMALIES_SQL)
        self._conn.execute(CREATE_ANOMALIES_INDEX_SQL)
        self._conn.commit()

    def _migrate(self) -> None:
        """Apply pending MIGRATIONS in one transaction, then VACUUM.

        A store written by a newer Scherlok has a higher user_version than
        this code knows about; it is left untouched, never downgraded.
        """
        version = self._conn.execute("PRAGMA user_version").fetchone()[0]
        pending = MIGRATIONS[version:]
        if not pending:
            return
        with self._conn:
            self._conn.execute("BEGIN IMMEDIATE")
            for migration in pending:
                migration(self._conn)
        # VACUUM rewrites the file so replaced data does not linger in free
        # pages. It cannot run inside a transaction, and the version is only
        # bumped once it succeeds, so a failed VACUUM is retried on next open.
        self._conn.execute("VACUUM")
        self._conn.execute(f"PRAGMA user_version = {len(MIGRATIONS)}")

    def save_profile(self, table_name: str, profile_type: str, data: dict) -> None:
        """Save a profile snapshot to the store."""
        self._conn.execute(
            f"INSERT INTO profiles (table_name, profile_type, data, created_at, {SCOPE_COLUMN}) "
            "VALUES (?, ?, ?, ?, ?)",
            (
                table_name,
                profile_type,
                json.dumps(data, default=str),
                datetime.now(timezone.utc).isoformat(),
                self._scope,
            ),
        )
        self._conn.commit()

    def get_latest_profile(self, table_name: str, profile_type: str) -> dict[str, Any] | None:
        """Get the most recent profile for a table and type."""
        row = self._conn.execute(
            "SELECT data FROM profiles "
            f"WHERE table_name = ? AND profile_type = ? AND {SCOPE_FILTER_SQL} "
            "ORDER BY created_at DESC LIMIT 1",
            (table_name, profile_type, *self._scope_params()),
        ).fetchone()
        if row:
            return json.loads(row["data"])
        return None

    def get_profile_history(
        self,
        table_name: str,
        profile_type: str,
        days: int | None = 30,
        *,
        limit: int | None = None,
    ) -> list[dict[str, Any]]:
        """Get historical profiles, optionally bounded to the latest rows."""
        query = (
            "SELECT data, created_at FROM profiles "
            f"WHERE table_name = ? AND profile_type = ? AND {SCOPE_FILTER_SQL}"
        )
        params: list[Any] = [table_name, profile_type, *self._scope_params()]
        if days is not None:
            cutoff = datetime.now(timezone.utc).isoformat()
            query += " AND created_at >= datetime(?, '-' || ? || ' days')"
            params.extend([cutoff, days])
        query += " ORDER BY created_at DESC"
        if limit is not None:
            query += " LIMIT ?"
            params.append(limit)

        rows = self._conn.execute(query, params).fetchall()
        return [
            {**json.loads(row["data"]), "_created_at": row["created_at"]}
            for row in rows
        ]

    def save_anomalies(self, anomalies: list[dict]) -> None:
        """Save detected anomalies to history."""
        now = datetime.now(timezone.utc).isoformat()
        for a in anomalies:
            self._conn.execute(
                "INSERT INTO anomalies "
                f"(table_name, anomaly_type, severity, message, detected_at, {SCOPE_COLUMN}) "
                "VALUES (?, ?, ?, ?, ?, ?)",
                (a["table"], a["type"], a["severity"].value, a["message"], now, self._scope),
            )
        self._conn.commit()

    def get_anomaly_history(self, days: int = 30) -> list[dict[str, Any]]:
        """Get anomaly history for the last N days."""
        cutoff = datetime.now(timezone.utc).isoformat()
        rows = self._conn.execute(
            "SELECT table_name, anomaly_type, severity, message, detected_at "
            "FROM anomalies "
            f"WHERE detected_at >= datetime(?, '-' || ? || ' days') AND {SCOPE_FILTER_SQL} "
            "ORDER BY detected_at DESC",
            (cutoff, days, *self._scope_params()),
        ).fetchall()
        return [
            {
                "table": row["table_name"],
                "type": row["anomaly_type"],
                "severity": row["severity"],
                "message": row["message"],
                "detected_at": row["detected_at"],
            }
            for row in rows
        ]

    def get_latest_anomaly_per_table(self) -> dict[str, dict[str, str]]:
        """Latest anomaly for every table in one query.

        Latest means highest detected_at, with the anomaly id as the
        deterministic tie-breaker when timestamps match. No history
        window: a table that fired long ago still reports it.
        """
        rows = self._conn.execute(
            "SELECT table_name, anomaly_type, severity, detected_at FROM ("
            "SELECT table_name, anomaly_type, severity, detected_at, "
            "ROW_NUMBER() OVER (PARTITION BY table_name "
            f"ORDER BY detected_at DESC, id DESC) AS rn FROM anomalies WHERE {SCOPE_FILTER_SQL}"
            ") WHERE rn = 1",
            self._scope_params(),
        ).fetchall()
        return {
            row["table_name"]: {
                "type": row["anomaly_type"],
                "severity": row["severity"],
                "detected_at": row["detected_at"],
            }
            for row in rows
        }

    def close(self) -> None:
        """Close the database connection."""
        self._conn.close()
