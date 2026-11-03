"""Shared fixtures: keep every test away from the developer's real ~/.scherlok.

`scherlok.config` resolves `~/.scherlok` once, at import, so setting `HOME`
inside a test does not move it. Without this fixture, commands such as `dbt`
or `ci` save a test connection string over the real `config.json`, and
`watch` writes test profiles and anomalies into the real `profiles.db`.
"""

from __future__ import annotations

from pathlib import Path

import pytest

import scherlok.config as config
import scherlok.store.sqlite as sqlite_store

# Captured before any test patches them, so tests can assert they never touch it.
REAL_SCHERLOK_DIR = config.SCHERLOK_DIR

# Read by ScherlokConfig on every call; a value exported in the developer's
# shell would point tests at a real database or a real remote store.
SCHERLOK_ENV_VARS = (config.ENV_CONNECTION, config.ENV_STORE)


@pytest.fixture(autouse=True)
def isolated_scherlok_dir(tmp_path_factory, monkeypatch) -> Path:
    """Point the config file and the profile store at a per-test scratch directory."""
    scherlok_dir = tmp_path_factory.mktemp("home") / ".scherlok"
    monkeypatch.setattr(config, "SCHERLOK_DIR", scherlok_dir)
    monkeypatch.setattr(config, "CONFIG_FILE", scherlok_dir / "config.json")
    monkeypatch.setattr(config, "PROFILES_DB", scherlok_dir / "profiles.db")
    monkeypatch.setattr(sqlite_store, "PROFILES_DB", scherlok_dir / "profiles.db")
    for name in SCHERLOK_ENV_VARS:
        monkeypatch.delenv(name, raising=False)
    return scherlok_dir
