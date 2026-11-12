"""Configuration management for Scherlok.

Stores config in ~/.scherlok/config.json and profiles in ~/.scherlok/profiles.db.
Supports SCHERLOK_CONNECTION env var as alternative to the connect command.
"""

import json
import os
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any

SCHERLOK_DIR = Path.home() / ".scherlok"
CONFIG_FILE = SCHERLOK_DIR / "config.json"
PROFILES_DB = SCHERLOK_DIR / "profiles.db"
ENV_CONNECTION = "SCHERLOK_CONNECTION"

ENV_STORE = "SCHERLOK_STORE"

# Where the connection string comes from, as `scherlok config --output json` reports it.
CONNECTION_SOURCE_ENV = "env"
CONNECTION_SOURCE_CONFIG = "config"

# Locked mode (#109): only `scherlok accept` writes baselines. Either the saved
# setting or the variable locks. The variable can't unlock a saved lock, and
# any value but these locks, so a typo fails closed.
ENV_BASELINE_LOCKED = "SCHERLOK_BASELINE_LOCKED"
BASELINE_LOCKED_SETTING = "baseline_locked"
UNLOCKED_ENV_VALUES = frozenset({"", "0", "false", "no", "off"})

# The tables `scherlok connect --table` chose to monitor (#156); None means
# every table the connection lists.
TABLES_SETTING = "tables"

# config.json can hold a connection string with a password, so Scherlok's files
# are owner-only rather than inheriting the umask (often world-readable).
PRIVATE_DIR_MODE = 0o700
PRIVATE_FILE_MODE = 0o600

DEFAULT_SETTINGS: dict[str, Any] = {
    "z_score_threshold": 3.0,
    "volume_critical_drop_pct": 50,
    "volume_warning_drop_pct": 20,
    "freshness_tolerance_hours": 24,
    "slack_webhook_url": None,
    "store": None,
    BASELINE_LOCKED_SETTING: False,
    TABLES_SETTING: None,
}


def make_private_dir(path: Path) -> None:
    """Create ``path`` and its parents if missing, the new directory owner-only.

    An existing directory keeps its mode: Scherlok tightens only what it creates.
    The mode is ignored on Windows, where POSIX permissions don't apply.
    """
    path.mkdir(mode=PRIVATE_DIR_MODE, parents=True, exist_ok=True)


@dataclass
class ScherlokConfig:
    """Main configuration for Scherlok."""

    connection_string: str = ""
    settings: dict[str, Any] = field(default_factory=lambda: dict(DEFAULT_SETTINGS))

    def save(self) -> None:
        """Persist config to disk, readable by the owner only.

        A new file is created as 0600 instead of being tightened after the
        write, so the connection string is never briefly exposed. An existing
        file is tightened before it is rewritten.
        """
        make_private_dir(SCHERLOK_DIR)
        if os.name == "posix" and CONFIG_FILE.exists():
            CONFIG_FILE.chmod(PRIVATE_FILE_MODE)
        fd = os.open(CONFIG_FILE, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, PRIVATE_FILE_MODE)
        with os.fdopen(fd, "w") as config_file:
            config_file.write(json.dumps(asdict(self), indent=2))

    @classmethod
    def load(cls) -> "ScherlokConfig":
        """Load config from disk, falling back to defaults."""
        if CONFIG_FILE.exists():
            data = json.loads(CONFIG_FILE.read_text())
            return cls(
                connection_string=data.get("connection_string", ""),
                settings={**DEFAULT_SETTINGS, **data.get("settings", {})},
            )
        return cls()

    def get_connection_string(self) -> str:
        """Return connection string from env var or config file."""
        return os.environ.get(ENV_CONNECTION, self.connection_string)

    def connection_source(self) -> str | None:
        """Where `get_connection_string` finds a connection: "env", "config", or None.

        SCHERLOK_CONNECTION wins when it is set, even to an empty string,
        which then means no connection.
        """
        if not self.get_connection_string():
            return None
        return CONNECTION_SOURCE_ENV if ENV_CONNECTION in os.environ else CONNECTION_SOURCE_CONFIG

    def get_store(self) -> str | None:
        """Return remote store URL from env var or config."""
        return os.environ.get(ENV_STORE, self.settings.get("store"))

    def saved_tables(self) -> list[str] | None:
        """The tables saved with `scherlok connect --table`, or None for every table.

        They are names, not a database: they apply to whichever connection
        is in use, including SCHERLOK_CONNECTION.
        """
        return self.settings.get(TABLES_SETTING) or None

    def baseline_locked(self) -> bool:
        """True when only `scherlok accept` may write baselines (#109).

        Set by `scherlok config --baseline-locked` or by SCHERLOK_BASELINE_LOCKED
        holding anything but an empty string, 0, false, no or off.
        """
        env_value = os.environ.get(ENV_BASELINE_LOCKED)
        env_locked = (
            env_value is not None and env_value.strip().lower() not in UNLOCKED_ENV_VALUES
        )
        return env_locked or bool(self.settings.get(BASELINE_LOCKED_SETTING))
