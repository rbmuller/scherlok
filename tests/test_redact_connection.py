"""`redact_connection` never shows a credential, whatever the URL looks like."""

from pathlib import Path

import pytest

from scherlok.dashboard.assembler import assemble_view
from scherlok.service import connection_scope, redact_connection
from scherlok.store.sqlite import ProfileStore

SECRET = "s3cret"


@pytest.mark.parametrize(
    ("connection", "expected"),
    [
        ("postgresql://alice:s3cret@host/db", "postgresql://alice:***@host/db"),
        ("postgresql://alice:s3cret@host:5432/demo", "postgresql://alice:***@host:5432/demo"),
        ("mysql://:s3cret@host/db", "mysql://***@host/db"),
        (
            "postgresql://alice:s3cret@host/db?sslmode=require",
            "postgresql://alice:***@host/db?sslmode=require",
        ),
    ],
)
def test_masks_the_password(connection, expected):
    assert redact_connection(connection) == expected


@pytest.mark.parametrize(
    "connection",
    [
        "bigquery://my-proj/my-dataset",
        "snowflake://acc/db/schema",
        "duckdb:///data/warehouse.duckdb",
        "postgresql://alice@host/db?sslmode=require",
        "",
    ],
)
def test_returns_a_url_without_credentials_unchanged(connection):
    assert redact_connection(connection) == connection


@pytest.mark.parametrize("separator", ["/", "?", "#"])
def test_masks_a_password_that_cut_the_host_short(separator):
    # An unencoded separator ends the host early, so urlsplit sees no password
    # and the rest of it lands in the path, query or fragment.
    connection = f"postgresql://alice:s3{separator}cret@host/db"

    redacted = redact_connection(connection)

    assert redacted == "postgresql://***@host/db"
    assert connection_scope(connection) is None


@pytest.mark.parametrize(
    "key", ["password", "sslpassword", "motherduck_token", "api_key", "client_secret", "PWD"]
)
def test_masks_credential_query_parameters(key):
    connection = f"duckdb:///data/w.duckdb?{key}={SECRET}&threads=4"

    redacted = redact_connection(connection)

    assert SECRET not in redacted
    assert f"{key}=***" in redacted
    assert "threads=4" in redacted


def test_the_dashboard_shows_the_redacted_connection(tmp_path: Path):
    store = ProfileStore(db_path=tmp_path / "profiles.db")
    try:
        view = assemble_view(store, connection_string=f"postgresql://alice:s3/{SECRET}@host/db")
    finally:
        store.close()

    assert SECRET not in view["meta"]["connection"]
