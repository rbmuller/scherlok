"""The autouse fixture in conftest.py keeps tests out of the real ~/.scherlok."""

from pathlib import Path

import scherlok.config as config
import scherlok.store.sqlite as sqlite_store
from scherlok.config import ScherlokConfig
from scherlok.store.sqlite import ProfileStore

REAL_SCHERLOK_DIR = Path.home() / ".scherlok"


def test_paths_point_at_the_scratch_directory(isolated_scherlok_dir):
    assert config.SCHERLOK_DIR == isolated_scherlok_dir
    assert config.CONFIG_FILE.parent == isolated_scherlok_dir
    assert config.PROFILES_DB.parent == isolated_scherlok_dir
    assert sqlite_store.PROFILES_DB.parent == isolated_scherlok_dir
    assert not isolated_scherlok_dir.is_relative_to(REAL_SCHERLOK_DIR)


def test_saved_config_lands_in_the_scratch_directory(isolated_scherlok_dir):
    ScherlokConfig(connection_string="duckdb:///:memory:").save()
    assert (isolated_scherlok_dir / "config.json").exists()


def test_default_profile_store_lands_in_the_scratch_directory(isolated_scherlok_dir):
    store = ProfileStore()
    try:
        store.save_profile("orders", "volume", {"row_count": 1})
    finally:
        store.close()
    assert (isolated_scherlok_dir / "profiles.db").exists()


def test_scherlok_env_vars_are_cleared():
    config_from_env = ScherlokConfig()
    assert config_from_env.get_connection_string() == ""
    assert config_from_env.get_store() is None
