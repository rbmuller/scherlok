"""Connector registry for Scherlok."""

from scherlok.connectors.base import BaseConnector
from scherlok.connectors.postgres import PostgresConnector

CONNECTOR_SCHEMES: dict[str, type[BaseConnector]] = {
    "postgresql": PostgresConnector,
    "postgres": PostgresConnector,
}

# Optional connectors — register only if dependencies are installed
try:
    from scherlok.connectors.bigquery import BigQueryConnector
    CONNECTOR_SCHEMES["bigquery"] = BigQueryConnector
except ImportError:
    pass  # google-cloud-bigquery not installed

try:
    from scherlok.connectors.snowflake import SnowflakeConnector
    CONNECTOR_SCHEMES["snowflake"] = SnowflakeConnector
except ImportError:
    pass  # snowflake-connector-python not installed


def get_connector(connection_string: str) -> BaseConnector:
    """Return the appropriate connector for a given connection string.

    Raises:
        ValueError: If the connection scheme is not supported.
    """
    scheme = connection_string.split("://")[0].lower() if "://" in connection_string else ""
    connector_cls = CONNECTOR_SCHEMES.get(scheme)
    if connector_cls is None:
        supported = ", ".join(sorted(CONNECTOR_SCHEMES.keys()))
        raise ValueError(
            f"Unsupported connection scheme '{scheme}'. Supported: {supported}"
        )
    return connector_cls(connection_string)
