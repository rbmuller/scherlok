"""Tests for `scherlok dashboard` CLI command."""

from pathlib import Path

from typer.testing import CliRunner

from scherlok.cli import app

runner = CliRunner()


def test_dashboard_help():
    result = runner.invoke(app, ["dashboard", "--help"])
    assert result.exit_code == 0
    assert "dashboard" in result.output.lower()
    assert "self-contained HTML" in result.output


def test_dashboard_invalid_theme(tmp_path: Path, monkeypatch):
    monkeypatch.setenv("HOME", str(tmp_path))
    out = tmp_path / "report.html"
    result = runner.invoke(
        app, ["dashboard", "--out", str(out), "--theme", "neon-pink"]
    )
    assert result.exit_code == 1
    assert "theme must be one of" in result.output


def test_dashboard_writes_file(tmp_path: Path, monkeypatch):
    monkeypatch.setenv("HOME", str(tmp_path))
    out = tmp_path / "report.html"
    result = runner.invoke(app, ["dashboard", "--out", str(out)])
    assert result.exit_code == 0, result.output
    assert out.is_file()
    html = out.read_text()
    assert "<!DOCTYPE html>" in html
    assert "scherlok" in html
    assert "Dashboard written to" in result.output
