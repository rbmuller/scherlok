"""Schema profiling — columns, types, nullability."""

from scherlok.connectors.base import BaseConnector


def profile_schema(connector: BaseConnector, table: str) -> dict:
    """Profile the schema of a table.

    Returns:
        Dict with key: columns (list of {"name", "type", "nullable"}).
    """
    columns = connector.get_columns(table)
    return {"columns": columns}
