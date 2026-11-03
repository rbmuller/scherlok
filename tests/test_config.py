"""Tests for ScherlokConfig."""

import json
import os
import stat
import tempfile
from pathlib import Path
from unittest.mock import patch

import pytest

from scherlok import config as config_module
from scherlok.config import DEFAULT_SETTINGS, ScherlokConfig, make_private_dir
from scherlok.store.sqlite import ProfileStore

SECRET_CONNECTION = "postgres://user:secret@localhost/db"
SHARED_DIR_MODE = 0o755
SHARED_FILE_MODE = 0o644

posix_only = pytest.mark.skipif(os.name != "posix", reason="POSIX file modes")


def _mode(path: Path) -> int:
    return stat.S_IMODE(path.stat().st_mode)


@pytest.fixture
def scherlok_dir(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    """Point the config module at a not-yet-created ~/.scherlok under tmp_path."""
    directory = tmp_path / ".scherlok"
    monkeypatch.setattr(config_module, "SCHERLOK_DIR", directory)
    monkeypatch.setattr(config_module, "CONFIG_FILE", directory / "config.json")
    return directory


def test_save_round_trips(scherlok_dir: Path):
    ScherlokConfig(connection_string=SECRET_CONNECTION, settings={"store": "s3://b/p.db"}).save()
    loaded = ScherlokConfig.load()
    assert loaded.connection_string == SECRET_CONNECTION
    assert loaded.settings["store"] == "s3://b/p.db"
    assert loaded.settings["z_score_threshold"] == DEFAULT_SETTINGS["z_score_threshold"]


@posix_only
def test_save_creates_owner_only_file_and_dir(scherlok_dir: Path):
    ScherlokConfig(connection_string=SECRET_CONNECTION).save()
    assert _mode(scherlok_dir / "config.json") == config_module.PRIVATE_FILE_MODE
    assert _mode(scherlok_dir) == config_module.PRIVATE_DIR_MODE


@posix_only
def test_save_tightens_existing_world_readable_file(scherlok_dir: Path):
    scherlok_dir.mkdir()
    config_file = scherlok_dir / "config.json"
    config_file.write_text(json.dumps({"connection_string": "old"}))
    config_file.chmod(SHARED_FILE_MODE)

    ScherlokConfig(connection_string=SECRET_CONNECTION).save()

    assert _mode(config_file) == config_module.PRIVATE_FILE_MODE
    assert json.loads(config_file.read_text())["connection_string"] == SECRET_CONNECTION


@posix_only
def test_save_keeps_mode_of_existing_dir(scherlok_dir: Path):
    scherlok_dir.mkdir()
    scherlok_dir.chmod(SHARED_DIR_MODE)
    ScherlokConfig().save()
    assert _mode(scherlok_dir) == SHARED_DIR_MODE


@posix_only
def test_make_private_dir_creates_parents(tmp_path: Path):
    target = tmp_path / "a" / "b"
    make_private_dir(target)
    assert _mode(target) == config_module.PRIVATE_DIR_MODE


@posix_only
def test_profile_store_creates_owner_only_dir(tmp_path: Path):
    store = ProfileStore(db_path=tmp_path / "new" / "profiles.db")
    store.close()
    assert _mode(tmp_path / "new") == config_module.PRIVATE_DIR_MODE


class TestScherlokConfig:
    def test_default_config(self):
        config = ScherlokConfig()
        assert config.connection_string == ""
        assert config.settings["z_score_threshold"] == 3.0

    def test_save_and_load(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            config_file = Path(tmpdir) / "config.json"
            config = ScherlokConfig(connection_string="postgres://localhost/test")
            # Write directly to temp location
            config_file.write_text(json.dumps({
                "connection_string": config.connection_string,
                "settings": config.settings,
            }, indent=2))

            data = json.loads(config_file.read_text())
            loaded = ScherlokConfig(
                connection_string=data["connection_string"],
                settings={**DEFAULT_SETTINGS, **data.get("settings", {})},
            )
            assert loaded.connection_string == "postgres://localhost/test"

    @patch.dict("os.environ", {"SCHERLOK_CONNECTION": "postgres://env-var/db"})
    def test_env_var_overrides_config(self):
        config = ScherlokConfig(connection_string="postgres://config/db")
        assert config.get_connection_string() == "postgres://env-var/db"

    def test_get_connection_string_from_config(self):
        config = ScherlokConfig(connection_string="postgres://config/db")
        assert config.get_connection_string() == "postgres://config/db"

    @patch.dict("os.environ", {"SCHERLOK_STORE": "s3://my-bucket/profiles.db"})
    def test_store_env_var(self):
        config = ScherlokConfig()
        assert config.get_store() == "s3://my-bucket/profiles.db"

    def test_store_from_settings(self):
        config = ScherlokConfig()
        config.settings["store"] = "gs://bucket/profiles.db"
        assert config.get_store() == "gs://bucket/profiles.db"

    def test_store_default_none(self):
        config = ScherlokConfig()
        assert config.get_store() is None

    def test_default_settings_complete(self):
        assert "z_score_threshold" in DEFAULT_SETTINGS
        assert "volume_critical_drop_pct" in DEFAULT_SETTINGS
        assert "freshness_tolerance_hours" in DEFAULT_SETTINGS
        assert "slack_webhook_url" in DEFAULT_SETTINGS
        assert "store" in DEFAULT_SETTINGS
