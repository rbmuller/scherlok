"""Tests for the connector registry."""

import pytest

from scherlok.connectors import get_connector
from scherlok.connectors.base import BaseConnector
from scherlok.connectors.postgres import PostgresConnector


def test_get_postgres_connector():
    """Test that postgresql:// scheme returns PostgresConnector."""
    connector = get_connector("postgresql://user:pass@localhost/db")
    assert isinstance(connector, PostgresConnector)


def test_get_postgres_connector_alt_scheme():
    """Test that postgres:// scheme also works."""
    connector = get_connector("postgres://user:pass@localhost/db")
    assert isinstance(connector, PostgresConnector)


def test_unsupported_scheme_raises():
    """Test that unsupported schemes raise ValueError."""
    with pytest.raises(ValueError, match="Unsupported connection scheme"):
        get_connector("mysql://user:pass@localhost/db")


def test_no_scheme_raises():
    """Test that a connection string without :// raises ValueError."""
    with pytest.raises(ValueError, match="Unsupported connection scheme"):
        get_connector("just-a-string")


def test_base_connector_is_abstract():
    """Test that BaseConnector cannot be instantiated directly."""
    with pytest.raises(TypeError):
        BaseConnector("test")  # type: ignore[abstract]
