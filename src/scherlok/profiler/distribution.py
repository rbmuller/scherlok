"""Column distribution profiling — mean, stddev, nulls, cardinality."""

from scherlok.connectors.base import BaseConnector


def profile_distribution(connector: BaseConnector, table: str, column: str) -> dict:
    """Profile the distribution of a single column.

    Only aggregates are copied from the connector's stats, so a value read
    from a row never reaches the profile store, whatever the connector returns.

    Returns:
        Dict with keys: mean, stddev, null_count, null_rate, distinct_count.
    """
    stats = connector.get_column_stats(table, column)
    row_count = connector.get_row_count(table)
    null_rate = stats["null_count"] / row_count if row_count > 0 else 0.0

    return {
        "mean": stats.get("mean"),
        "stddev": stats.get("stddev"),
        "null_count": stats["null_count"],
        "null_rate": round(null_rate, 4),
        "distinct_count": stats["distinct_count"],
    }
