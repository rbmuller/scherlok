"""Tests for the Scherlok CLI."""

from typer.testing import CliRunner

from scherlok.cli import app

runner = CliRunner()


def test_version_command():
    """Test that version command outputs the version string."""
    result = runner.invoke(app, ["version"])
    assert result.exit_code == 0
    from scherlok import __version__
    assert __version__ in result.output


def test_help_shows_all_commands():
    """Test that help text lists all expected commands."""
    result = runner.invoke(app, ["--help"])
    assert result.exit_code == 0
    for cmd in ["connect", "investigate", "watch", "report", "status", "version"]:
        assert cmd in result.output


def test_connect_help():
    """Test that connect command has help text."""
    result = runner.invoke(app, ["connect", "--help"])
    assert result.exit_code == 0
    assert "connection" in result.output.lower()


def test_connect_no_args_lists_examples():
    """`scherlok connect` with no args prints an example per adapter and exits 0.

    Regression for #23 — previously Typer rejected the missing argument with
    exit code 2, which is unfriendly for a first-time user trying to discover
    what a connection string even looks like.
    """
    result = runner.invoke(app, ["connect"])
    assert result.exit_code == 0, (
        f"expected exit 0 for `scherlok connect` (no args), got "
        f"{result.exit_code}: {result.output!r}"
    )
    output = result.output
    # One example per supported adapter
    assert "postgresql://" in output
    assert "bigquery://" in output
    assert "snowflake://" in output
    # Each adapter is labelled
    assert "postgres" in output.lower()
    assert "bigquery" in output.lower()
    assert "snowflake" in output.lower()


def test_connect_no_args_examples_use_canonical_formats():
    """The printed examples must match the format each connector actually parses.

    BigQueryConnector requires `bigquery://project/dataset` and
    SnowflakeConnector requires `snowflake://account/database/schema`.
    A help message that ships invalid examples is worse than no help at all.
    """
    from scherlok.cli import CONNECT_EXAMPLES

    examples = dict(CONNECT_EXAMPLES)
    bq = examples["bigquery"]
    sf = examples["snowflake"]

    # BigQuery: bigquery://<project>/<dataset> => 2 path parts
    bq_parts = bq.replace("bigquery://", "").strip("/").split("/")
    assert len(bq_parts) >= 2, f"bigquery example missing dataset: {bq}"

    # Snowflake: snowflake://<account>/<database>/<schema> => 3 path parts
    sf_parts = sf.replace("snowflake://", "").strip("/").split("/")
    assert len(sf_parts) >= 3, f"snowflake example missing database/schema: {sf}"


def test_connect_with_argument_still_attempts_connection():
    """Happy-path regression: passing a connection string must still try to connect.

    Uses an obviously-bogus postgres URL so we don't hit a real database;
    the assertion is that the code path goes through `get_connector` and
    fails-to-connect (exit 1), NOT through the no-args examples branch
    (which would exit 0).
    """
    result = runner.invoke(app, ["connect", "postgresql://nobody:nobody@127.0.0.1:1/none"])
    # Either exit 1 (connection failed, expected) or exit 0 if a real DB
    # somehow listens at 127.0.0.1:1 (it doesn't). The point is: the examples
    # banner must NOT appear -- that would mean the no-args branch swallowed
    # the explicit argument.
    assert "Examples for each supported adapter" not in result.output


def test_investigate_help():
    """Test that investigate command has help text."""
    result = runner.invoke(app, ["investigate", "--help"])
    assert result.exit_code == 0


def test_watch_help():
    """Test that watch command has help text."""
    result = runner.invoke(app, ["watch", "--help"])
    assert result.exit_code == 0


def test_report_help():
    """Test that report command has help text."""
    result = runner.invoke(app, ["report", "--help"])
    assert result.exit_code == 0


def test_status_help():
    """Test that status command has help text."""
    result = runner.invoke(app, ["status", "--help"])
    assert result.exit_code == 0
