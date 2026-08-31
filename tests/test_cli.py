"""Tests for the Scherlok CLI."""

import json
import re
from unittest.mock import MagicMock, patch

from typer.testing import CliRunner

from scherlok.cli import app

runner = CliRunner(env={"NO_COLOR": "1"})

# Rich emits bold/dim styling via ANSI even when NO_COLOR strips colors, and
# CI runners typically have TERM=xterm so terminal-detection keeps styling on.
# Strip before substring assertions on help output.
ANSI_RE = re.compile(r"\x1b\[[0-9;]*m")


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
    for cmd in ["connect", "investigate", "watch", "check", "report", "status", "version"]:
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


def test_check_help():
    """Test that check command has help text."""
    result = runner.invoke(app, ["check", "--help"])
    assert result.exit_code == 0


def test_check_and_ci_share_options():
    """Test that check and ci expose the same CI options."""
    ci_help = ANSI_RE.sub("", runner.invoke(app, ["ci", "--help"]).output)
    check_help = ANSI_RE.sub("", runner.invoke(app, ["check", "--help"]).output)

    for option in ["--fail-on", "--webhook", "--email"]:
        assert option in ci_help
        assert option in check_help


def test_report_help():
    """Test that report command has help text."""
    result = runner.invoke(app, ["report", "--help"])
    assert result.exit_code == 0


def test_status_help():
    """Test that status command has help text."""
    result = runner.invoke(app, ["status", "--help"])
    assert result.exit_code == 0
    assert "--output" in result.output


def test_history_help_shows_output_flag():
    result = runner.invoke(app, ["history", "--help"])
    assert result.exit_code == 0
    assert "--output" in result.output


def _mock_connector(tables=None, row_count=100, columns=None):
    c = MagicMock()
    c.list_tables.return_value = tables or ["users"]
    c.get_row_count.return_value = row_count
    c.get_columns.return_value = columns or [
        {"name": "id", "type": "integer", "nullable": False},
    ]
    c.get_last_modified.return_value = None
    return c


class TestStatusJson:
    def test_produces_valid_json_array(self):
        connector = _mock_connector()
        with (
            patch("scherlok.cli._get_connector_or_exit", return_value=connector),
            patch("scherlok.cli.ProfileStore") as mock_store_cls,
            patch("scherlok.cli._table_health", return_value="healthy"),
        ):
            store = MagicMock()
            mock_store_cls.return_value = store
            store.get_latest_profile.side_effect = lambda _t, pt: {
                "volume": {"row_count": 100, "timestamp": "2026-08-31T12:00:00+00:00"},
                "schema": {"columns": [{"name": "id"}]},
            }.get(pt)

            result = runner.invoke(app, ["status", "--output", "json"])

        assert result.exit_code == 0
        data = json.loads(result.output)
        assert isinstance(data, list)
        assert len(data) == 1
        assert data[0] == {
            "table": "users",
            "rows": 100,
            "columns": 1,
            "status": "healthy",
            "last_profiled": "2026-08-31T12:00:00+00:00",
        }

    def test_unprofiled_table_has_null_fields(self):
        connector = _mock_connector()
        with (
            patch("scherlok.cli._get_connector_or_exit", return_value=connector),
            patch("scherlok.cli.ProfileStore") as mock_store_cls,
            patch("scherlok.cli._table_health", return_value="unknown"),
        ):
            store = MagicMock()
            mock_store_cls.return_value = store
            store.get_latest_profile.return_value = None

            result = runner.invoke(app, ["status", "--output", "json"])

        data = json.loads(result.output)
        assert data[0]["rows"] is None
        assert data[0]["columns"] is None
        assert data[0]["last_profiled"] is None
        assert data[0]["status"] == "unknown"

    def test_multiple_tables(self):
        connector = _mock_connector(tables=["orders", "users"])
        with (
            patch("scherlok.cli._get_connector_or_exit", return_value=connector),
            patch("scherlok.cli.ProfileStore") as mock_store_cls,
            patch("scherlok.cli._table_health", side_effect=["critical", "healthy"]),
        ):
            store = MagicMock()
            mock_store_cls.return_value = store
            store.get_latest_profile.side_effect = lambda _t, pt: {
                "volume": {"row_count": 50, "timestamp": "2026-08-30T00:00:00+00:00"},
                "schema": {"columns": [{"name": "id"}, {"name": "name"}]},
            }.get(pt)

            result = runner.invoke(app, ["status", "--output", "json"])

        data = json.loads(result.output)
        assert len(data) == 2
        assert data[0]["table"] == "orders"
        assert data[0]["status"] == "critical"
        assert data[1]["table"] == "users"
        assert data[1]["status"] == "healthy"

    def test_text_mode_still_works(self):
        connector = _mock_connector()
        with (
            patch("scherlok.cli._get_connector_or_exit", return_value=connector),
            patch("scherlok.cli.ProfileStore") as mock_store_cls,
            patch("scherlok.cli._table_health", return_value="healthy"),
        ):
            store = MagicMock()
            mock_store_cls.return_value = store
            store.get_latest_profile.side_effect = lambda _t, pt: {
                "volume": {"row_count": 100, "timestamp": "2026-08-31T12:00:00+00:00"},
                "schema": {"columns": [{"name": "id"}]},
            }.get(pt)

            result = runner.invoke(app, ["status"])

        assert result.exit_code == 0
        output = ANSI_RE.sub("", result.output)
        assert "Table Health" in output
        assert "users" in output

    def test_invalid_output_value_exits_1(self):
        result = runner.invoke(app, ["status", "--output", "xml"])
        assert result.exit_code == 1


class TestHistoryJson:
    def test_produces_valid_json_array(self):
        with patch("scherlok.cli.ProfileStore") as mock_store_cls:
            store = MagicMock()
            mock_store_cls.return_value = store
            store.get_anomaly_history.return_value = [
                {
                    "table": "users",
                    "type": "volume_drop",
                    "severity": "CRITICAL",
                    "message": "Row count dropped 50%",
                    "detected_at": "2026-08-31T00:00:00+00:00",
                },
            ]

            result = runner.invoke(app, ["history", "--output", "json"])

        assert result.exit_code == 0
        data = json.loads(result.output)
        assert len(data) == 1
        assert data[0]["severity"] == "CRITICAL"
        assert data[0]["table"] == "users"
        assert data[0]["type"] == "volume_drop"
        assert data[0]["detected_at"] == "2026-08-31T00:00:00+00:00"

    def test_empty_history_returns_empty_array(self):
        with patch("scherlok.cli.ProfileStore") as mock_store_cls:
            store = MagicMock()
            mock_store_cls.return_value = store
            store.get_anomaly_history.return_value = []

            result = runner.invoke(app, ["history", "--output", "json"])

        assert result.exit_code == 0
        assert json.loads(result.output) == []

    def test_respects_days_flag(self):
        with patch("scherlok.cli.ProfileStore") as mock_store_cls:
            store = MagicMock()
            mock_store_cls.return_value = store
            store.get_anomaly_history.return_value = []

            runner.invoke(app, ["history", "--days", "7", "--output", "json"])

        store.get_anomaly_history.assert_called_once_with(days=7)

    def test_text_mode_still_works(self):
        with patch("scherlok.cli.ProfileStore") as mock_store_cls:
            store = MagicMock()
            mock_store_cls.return_value = store
            store.get_anomaly_history.return_value = [
                {
                    "table": "users",
                    "type": "volume_drop",
                    "severity": "CRITICAL",
                    "message": "Row count dropped",
                    "detected_at": "2026-08-31T12:00:00+00:00",
                },
            ]

            result = runner.invoke(app, ["history"])

        assert result.exit_code == 0
        output = ANSI_RE.sub("", result.output)
        assert "Anomaly History" in output
        assert "users" in output

    def test_invalid_output_value_exits_1(self):
        result = runner.invoke(app, ["history", "--output", "xml"])
        assert result.exit_code == 1
