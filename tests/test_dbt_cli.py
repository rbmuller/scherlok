"""Tests for `scherlok dbt` CLI command."""

from pathlib import Path
from unittest.mock import MagicMock, patch

from typer.testing import CliRunner

from scherlok.cli import app

runner = CliRunner()
FIXTURES = Path(__file__).parent / "fixtures" / "dbt"
PG_PROJECT = FIXTURES / "jaffle_shop_postgres"
PROFILES_PG = FIXTURES / "profiles_postgres"


def test_dbt_help():
    result = runner.invoke(app, ["dbt", "--help"])
    assert result.exit_code == 0
    assert "Profile and watch dbt models" in result.output


def test_dbt_missing_manifest():
    """Pointing at a non-dbt directory should fail with a clear message."""
    result = runner.invoke(app, ["dbt", "--project-dir", "/tmp/nonexistent-dbt-project"])
    assert result.exit_code == 1
    assert "manifest.json not found" in result.output


def test_dbt_select_filter_no_match():
    """--select with a name that doesn't exist exits with a clear error."""
    result = runner.invoke(
        app,
        [
            "dbt",
            "--project-dir", str(PG_PROJECT),
            "--connection-string", "postgresql://u:p@host/db",
            "--select", "ghost_model",
        ],
    )
    assert result.exit_code == 1
    assert "No models matched" in result.output


@patch("scherlok.cli.get_connector")
def test_dbt_with_explicit_connection(mock_get_connector, tmp_path, monkeypatch):
    """End-to-end: explicit --connection-string skips profiles.yml entirely."""
    # Force config to live in tmp_path so we don't pollute ~/.scherlok
    monkeypatch.setenv("HOME", str(tmp_path))

    fake = MagicMock()
    fake.connect.return_value = True
    # Match dbt model identifiers exactly
    fake.list_tables.return_value = [
        "stg_customers", "stg_orders", "fct_orders", "dim_customers_inc",
    ]
    mock_get_connector.return_value = fake

    with patch("scherlok.cli._watch_table") as mock_watch:
        mock_watch.return_value = ([], {"row_count": 100})
        result = runner.invoke(
            app,
            [
                "dbt",
                "--project-dir", str(PG_PROJECT),
                "--connection-string", "postgresql://u:p@host/db",
            ],
        )

    assert result.exit_code == 0, result.output
    # 4 materialized models in the postgres fixture
    assert mock_watch.call_count == 4
    assert "Summary:" in result.output


@patch("scherlok.cli.get_connector")
def test_dbt_resolves_from_profiles(mock_get_connector, tmp_path, monkeypatch):
    """When --connection-string is omitted, profiles.yml drives resolution."""
    monkeypatch.setenv("HOME", str(tmp_path))
    monkeypatch.setenv("PG_USER", "alice")
    monkeypatch.setenv("PG_PASSWORD", "wonderland")

    fake = MagicMock()
    fake.connect.return_value = True
    fake.list_tables.return_value = [
        "stg_customers", "stg_orders", "fct_orders", "dim_customers_inc",
    ]
    mock_get_connector.return_value = fake

    with patch("scherlok.cli._watch_table") as mock_watch:
        mock_watch.return_value = ([], {"row_count": 50})
        result = runner.invoke(
            app,
            [
                "dbt",
                "--project-dir", str(PG_PROJECT),
                "--profiles-dir", str(PROFILES_PG),
            ],
        )

    assert result.exit_code == 0, result.output
    mock_get_connector.assert_called_with(
        "postgresql://alice:wonderland@localhost:5432/jaffle_db"
    )


@patch("scherlok.cli.get_connector")
def test_dbt_select_filters_models(mock_get_connector, tmp_path, monkeypatch):
    monkeypatch.setenv("HOME", str(tmp_path))

    fake = MagicMock()
    fake.connect.return_value = True
    fake.list_tables.return_value = ["stg_customers", "stg_orders", "fct_orders"]
    mock_get_connector.return_value = fake

    with patch("scherlok.cli._watch_table") as mock_watch:
        mock_watch.return_value = ([], {"row_count": 1})
        result = runner.invoke(
            app,
            [
                "dbt",
                "--project-dir", str(PG_PROJECT),
                "--connection-string", "postgresql://u:p@h/d",
                "--select", "fct_orders",
            ],
        )

    assert result.exit_code == 0
    assert mock_watch.call_count == 1
    # Exactly fct_orders profiled
    called_table = mock_watch.call_args.args[2]
    assert called_table == "fct_orders"
