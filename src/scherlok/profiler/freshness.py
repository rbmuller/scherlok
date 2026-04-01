"""Table freshness profiling — last update detection."""

from datetime import datetime, timezone

from scherlok.connectors.base import BaseConnector


def profile_freshness(connector: BaseConnector, table: str) -> dict:
    """Profile the freshness of a table.

    Returns:
        Dict with keys: last_modified (str ISO or None), hours_since_update (float or None).
    """
    last_mod = connector.get_last_modified(table)
    hours_since: float | None = None

    if last_mod is not None:
        if last_mod.tzinfo is None:
            last_mod = last_mod.replace(tzinfo=timezone.utc)
        delta = datetime.now(timezone.utc) - last_mod
        hours_since = round(delta.total_seconds() / 3600, 2)

    return {
        "last_modified": last_mod.isoformat() if last_mod else None,
        "hours_since_update": hours_since,
    }
