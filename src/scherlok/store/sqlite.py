"""Local SQLite profile storage."""

import json
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from scherlok.config import SCHERLOK_DIR

PROFILES_DB = SCHERLOK_DIR / "profiles.db"

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


class ProfileStore:
    """SQLite-backed storage for data profiles."""

    def __init__(self, db_path: Path | None = None) -> None:
        self._db_path = db_path or PROFILES_DB
        self._db_path.parent.mkdir(parents=True, exist_ok=True)
        self._conn = sqlite3.connect(str(self._db_path))
        self._conn.row_factory = sqlite3.Row
        self._init_db()

    def _init_db(self) -> None:
        """Create tables and indexes if they don't exist."""
        self._conn.execute(CREATE_TABLE_SQL)
        self._conn.execute(CREATE_INDEX_SQL)
        self._conn.commit()

    def save_profile(self, table_name: str, profile_type: str, data: dict) -> None:
        """Save a profile snapshot to the store."""
        self._conn.execute(
            "INSERT INTO profiles (table_name, profile_type, data, created_at) "
            "VALUES (?, ?, ?, ?)",
            (
                table_name,
                profile_type,
                json.dumps(data, default=str),
                datetime.now(timezone.utc).isoformat(),
            ),
        )
        self._conn.commit()

    def get_latest_profile(self, table_name: str, profile_type: str) -> dict[str, Any] | None:
        """Get the most recent profile for a table and type."""
        row = self._conn.execute(
            "SELECT data FROM profiles "
            "WHERE table_name = ? AND profile_type = ? "
            "ORDER BY created_at DESC LIMIT 1",
            (table_name, profile_type),
        ).fetchone()
        if row:
            return json.loads(row["data"])
        return None

    def get_profile_history(
        self,
        table_name: str,
        profile_type: str,
        days: int = 30,
    ) -> list[dict[str, Any]]:
        """Get historical profiles for the last N days."""
        cutoff = datetime.now(timezone.utc).isoformat()
        rows = self._conn.execute(
            "SELECT data, created_at FROM profiles "
            "WHERE table_name = ? AND profile_type = ? "
            "AND created_at >= datetime(?, '-' || ? || ' days') "
            "ORDER BY created_at DESC",
            (table_name, profile_type, cutoff, days),
        ).fetchall()
        return [
            {**json.loads(row["data"]), "_created_at": row["created_at"]}
            for row in rows
        ]

    def close(self) -> None:
        """Close the database connection."""
        self._conn.close()
