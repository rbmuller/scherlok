"""Row count profiling."""

from datetime import datetime, timezone

from scherlok.connectors.base import BaseConnector


def profile_volume(connector: BaseConnector, table: str) -> dict:
    """Profile the volume (row count) of a table.

    Returns:
        Dict with keys: row_count (int), timestamp (str ISO format).
    """
    row_count = connector.get_row_count(table)
    return {
        "row_count": row_count,
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }
