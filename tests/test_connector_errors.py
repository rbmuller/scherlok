"""Tests for the connector last_error hints introduced for v0.5.0 polish."""

import sys
from unittest.mock import MagicMock, patch

import pytest


def _psycopg2_error(msg: str):
    """Build a real psycopg2.Error subclass instance with the given message."""
    import psycopg2
    return psycopg2.OperationalError(msg)


@patch("scherlok.connectors.postgres.psycopg2.connect")
def test_postgres_connection_refused_hint(mock_connect):
    from scherlok.connectors.postgres import PostgresConnector

    mock_connect.side_effect = _psycopg2_error(
        "could not connect to server: Connection refused\n"
        '\tIs the server running on host "localhost" and accepting TCP/IP connections '
        "on port 5432?"
    )
    c = PostgresConnector("postgresql://u:p@localhost:5432/db")
    assert c.connect() is False
    assert c.last_error is not None
    assert "connection refused" in c.last_error.lower()
    assert "Hint" in c.last_error


@patch("scherlok.connectors.postgres.psycopg2.connect")
def test_postgres_auth_failed_hint(mock_connect):
    from scherlok.connectors.postgres import PostgresConnector

    mock_connect.side_effect = _psycopg2_error(
        'FATAL:  password authentication failed for user "alice"'
    )
    c = PostgresConnector("postgresql://alice:wrong@host/db")
    assert c.connect() is False
    assert "username or password" in (c.last_error or "")


@patch("scherlok.connectors.postgres.psycopg2.connect")
def test_postgres_database_not_found_hint(mock_connect):
    from scherlok.connectors.postgres import PostgresConnector

    mock_connect.side_effect = _psycopg2_error('FATAL:  database "nope" does not exist')
    c = PostgresConnector("postgresql://u:p@host/nope")
    assert c.connect() is False
    assert "database does not exist" in (c.last_error or "").lower()


@patch("scherlok.connectors.postgres.psycopg2.connect")
def test_postgres_unknown_error_falls_back_to_first_line(mock_connect):
    from scherlok.connectors.postgres import PostgresConnector

    mock_connect.side_effect = _psycopg2_error("some bizarre internal error\nwith a 2nd line")
    c = PostgresConnector("postgresql://u:p@host/db")
    assert c.connect() is False
    assert "bizarre internal error" in (c.last_error or "")
    assert "2nd line" not in (c.last_error or "")


def test_base_connector_starts_with_no_error():
    from scherlok.connectors.postgres import PostgresConnector

    c = PostgresConnector("postgresql://u:p@h/d")
    assert c.last_error is None


# --- Snowflake ---------------------------------------------------------------

def test_snowflake_connector_not_installed_hint():
    """When snowflake-connector-python isn't installed, last_error names the extra."""
    # Ensure the module is NOT in sys.modules
    sys.modules.pop("snowflake", None)
    sys.modules.pop("snowflake.connector", None)

    from scherlok.connectors.snowflake import SnowflakeConnector

    c = SnowflakeConnector("snowflake://acc/db/schema")
    assert c.connect() is False
    err = c.last_error or ""
    # If snowflake is not installed locally, we should hit the import-error branch
    assert "scherlok[snowflake]" in err or "missing required env var" in err


@pytest.fixture
def fake_snowflake(monkeypatch):
    """Inject a fake snowflake.connector module so connect() can proceed past import."""
    fake_module = MagicMock()
    fake_pkg = MagicMock()
    fake_pkg.connector = fake_module
    monkeypatch.setitem(sys.modules, "snowflake", fake_pkg)
    monkeypatch.setitem(sys.modules, "snowflake.connector", fake_module)
    return fake_module


def test_snowflake_missing_env_vars(monkeypatch, fake_snowflake):
    """When SNOWFLAKE_USER/PASSWORD aren't set, last_error names them."""
    from scherlok.connectors.snowflake import SnowflakeConnector

    monkeypatch.delenv("SNOWFLAKE_USER", raising=False)
    monkeypatch.delenv("SNOWFLAKE_PASSWORD", raising=False)
    c = SnowflakeConnector("snowflake://acc/db/schema")
    assert c.connect() is False
    err = c.last_error or ""
    assert "SNOWFLAKE_USER" in err
    assert "SNOWFLAKE_PASSWORD" in err


def test_snowflake_auth_error_hint(monkeypatch, fake_snowflake):
    from scherlok.connectors.snowflake import SnowflakeConnector

    monkeypatch.setenv("SNOWFLAKE_USER", "alice")
    monkeypatch.setenv("SNOWFLAKE_PASSWORD", "wrong")
    fake_snowflake.connect.side_effect = Exception(
        "Incorrect username or password was specified"
    )
    c = SnowflakeConnector("snowflake://acc/db/schema")
    assert c.connect() is False
    assert "authentication" in (c.last_error or "").lower()


# --- BigQuery ----------------------------------------------------------------

def test_bigquery_module_not_installed_hint():
    """Without google-cloud-bigquery, last_error names the extra."""
    sys.modules.pop("google", None)
    sys.modules.pop("google.cloud", None)
    sys.modules.pop("google.cloud.bigquery", None)

    from scherlok.connectors.bigquery import BigQueryConnector

    c = BigQueryConnector("bigquery://my-proj/my-dataset")
    assert c.connect() is False
    err = c.last_error or ""
    # If bigquery is not installed locally, the import-error hint fires
    assert "scherlok[bigquery]" in err or "credentials" in err.lower()


@pytest.fixture
def fake_bigquery(monkeypatch):
    """Inject a fake google.cloud.bigquery module so connect() can proceed past import."""
    fake_module = MagicMock()
    fake_cloud = MagicMock()
    fake_cloud.bigquery = fake_module
    fake_google = MagicMock()
    fake_google.cloud = fake_cloud
    monkeypatch.setitem(sys.modules, "google", fake_google)
    monkeypatch.setitem(sys.modules, "google.cloud", fake_cloud)
    monkeypatch.setitem(sys.modules, "google.cloud.bigquery", fake_module)
    return fake_module


def test_bigquery_adc_missing_hint(fake_bigquery):
    fake_bigquery.Client.side_effect = Exception(
        "Could not automatically determine credentials. "
        "Please set GOOGLE_APPLICATION_CREDENTIALS or run gcloud auth"
    )
    from scherlok.connectors.bigquery import BigQueryConnector

    c = BigQueryConnector("bigquery://my-proj/my-dataset")
    assert c.connect() is False
    err = c.last_error or ""
    assert "Application Default Credentials" in err
    assert "gcloud auth application-default login" in err


def test_bigquery_dataset_not_found_hint(fake_bigquery):
    client_instance = MagicMock()
    client_instance.list_tables.side_effect = Exception(
        "Not found: Dataset my-proj:nope was not found"
    )
    fake_bigquery.Client.return_value = client_instance
    from scherlok.connectors.bigquery import BigQueryConnector

    c = BigQueryConnector("bigquery://my-proj/nope")
    assert c.connect() is False
    assert "dataset not found" in (c.last_error or "").lower()
