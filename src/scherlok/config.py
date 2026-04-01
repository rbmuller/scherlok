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

DEFAULT_SETTINGS: dict[str, Any] = {
    "z_score_threshold": 3.0,
    "volume_critical_drop_pct": 50,
    "volume_warning_drop_pct": 20,
    "freshness_tolerance_hours": 24,
    "slack_webhook_url": None,
}


@dataclass
class ScherlokConfig:
    """Main configuration for Scherlok."""

    connection_string: str = ""
    settings: dict[str, Any] = field(default_factory=lambda: dict(DEFAULT_SETTINGS))

    def save(self) -> None:
        """Persist config to disk."""
        SCHERLOK_DIR.mkdir(parents=True, exist_ok=True)
        CONFIG_FILE.write_text(json.dumps(asdict(self), indent=2))

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
