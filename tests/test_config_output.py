"""`scherlok config --output json`: is Scherlok set up, without the secret (#113).

The Claude Code hook asks this before running verify, so it can stay silent
when no connection is configured without reading ~/.scherlok itself.
"""

from __future__ import annotations

import json

import pytest
from typer.testing import CliRunner

import scherlok.config as config
from scherlok.cli import app
from scherlok.config import ENV_BASELINE_LOCKED, ENV_CONNECTION, ENV_STORE, ScherlokConfig

runner = CliRunner(env={"NO_COLOR": "1"})

SECRET = "hunter2"
SAVED = f"postgresql://alice:{SECRET}@db.example.com/shop"
FROM_ENV = f"duckdb:///tmp/warehouse.duckdb?motherduck_token={SECRET}"


def _config_json() -> dict:
    result = runner.invoke(app, ["config", "--output", "json"])
    assert result.exit_code == 0, result.output
    assert SECRET not in result.output
    return json.loads(result.stdout)


def test_nothing_configured():
    assert _config_json() == {
        "connection_configured": False,
        "connection_source": None,
        "store": None,
        "baseline_locked": False,
    }


def test_a_saved_connection():
    ScherlokConfig(connection_string=SAVED).save()
    payload = _config_json()
    assert (payload["connection_configured"], payload["connection_source"]) == (True, "config")


def test_the_environment_wins_over_the_saved_connection(monkeypatch):
    ScherlokConfig(connection_string=SAVED).save()
    monkeypatch.setenv(ENV_CONNECTION, FROM_ENV)
    payload = _config_json()
    assert (payload["connection_configured"], payload["connection_source"]) == (True, "env")


def test_an_empty_variable_means_no_connection(monkeypatch):
    """As get_connection_string reads it: the variable, once set, hides the saved one."""
    ScherlokConfig(connection_string=SAVED).save()
    monkeypatch.setenv(ENV_CONNECTION, "")
    assert _config_json()["connection_configured"] is False


def test_store_and_lock(monkeypatch):
    monkeypatch.setenv(ENV_STORE, f"gs://reader:{SECRET}@bucket/scherlok/profiles.db")
    monkeypatch.setenv(ENV_BASELINE_LOCKED, "1")
    payload = _config_json()
    assert payload["store"] == "gs://reader:***@bucket/scherlok/profiles.db"
    assert payload["baseline_locked"] is True


@pytest.mark.parametrize("change", [["--store", "s3://b/p.db"], ["--baseline-locked"]])
def test_json_output_refuses_to_change_settings(change):
    result = runner.invoke(app, ["config", "--output", "json", *change])
    assert result.exit_code == 1
    assert not config.CONFIG_FILE.exists()


def test_invalid_output_value():
    result = runner.invoke(app, ["config", "--output", "yaml"])
    assert result.exit_code == 1
    assert "Invalid --output value 'yaml'" in result.output


def test_text_output_is_unchanged():
    result = runner.invoke(app, ["config"])
    assert result.exit_code == 0
    assert "Current store: local (~/.scherlok/)" in result.output
    assert "Baselines locked: no" in result.output
