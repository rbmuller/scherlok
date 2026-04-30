"""Abstract base connector for database backends."""

from abc import ABC, abstractmethod
from datetime import datetime


class BaseConnector(ABC):
    """Base class that all database connectors must implement."""

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
        """Return column statistics: mean, stddev, min, max, null_count, etc."""
        ...

    @abstractmethod
    def get_last_modified(self, table: str) -> datetime | None:
        """Return the last known modification time for a table, or None."""
        ...
