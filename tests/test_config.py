"""Tests for ScherlokConfig."""

import json
import tempfile
from pathlib import Path
from unittest.mock import patch

from scherlok.config import DEFAULT_SETTINGS, ScherlokConfig


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
