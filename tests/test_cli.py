"""Tests for the Scherlok CLI."""

from typer.testing import CliRunner

from scherlok.cli import app

runner = CliRunner()


def test_version_command():
    """Test that version command outputs the version string."""
    result = runner.invoke(app, ["version"])
    assert result.exit_code == 0
    assert "0.1.0" in result.output


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
