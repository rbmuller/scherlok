"""Tests for ProfileStore (SQLite storage)."""

import tempfile
from pathlib import Path

from scherlok.detector.severity import Severity
from scherlok.store.sqlite import ProfileStore


def _temp_store() -> ProfileStore:
    """Create a ProfileStore with a temporary database file."""
    tmp = tempfile.mktemp(suffix=".db")
    return ProfileStore(db_path=Path(tmp))


class TestProfileStore:
    def test_save_and_load_profile(self):
        store = _temp_store()
        store.save_profile("users", "volume", {"row_count": 1000})
        result = store.get_latest_profile("users", "volume")
        assert result is not None
        assert result["row_count"] == 1000
        store.close()

    def test_latest_profile_returns_most_recent(self):
        store = _temp_store()
        store.save_profile("users", "volume", {"row_count": 100})
        store.save_profile("users", "volume", {"row_count": 200})
        result = store.get_latest_profile("users", "volume")
        assert result["row_count"] == 200
        store.close()

    def test_get_latest_returns_none_when_empty(self):
        store = _temp_store()
        result = store.get_latest_profile("nonexistent", "volume")
        assert result is None
        store.close()

    def test_different_tables_are_independent(self):
        store = _temp_store()
        store.save_profile("users", "volume", {"row_count": 100})
        store.save_profile("orders", "volume", {"row_count": 500})
        assert store.get_latest_profile("users", "volume")["row_count"] == 100
        assert store.get_latest_profile("orders", "volume")["row_count"] == 500
        store.close()

    def test_different_types_are_independent(self):
        store = _temp_store()
        store.save_profile("users", "volume", {"row_count": 100})
        store.save_profile("users", "schema", {"columns": [{"name": "id"}]})
        assert store.get_latest_profile("users", "volume")["row_count"] == 100
        assert store.get_latest_profile("users", "schema")["columns"][0]["name"] == "id"
        store.close()

    def test_profile_history(self):
        store = _temp_store()
        store.save_profile("users", "volume", {"row_count": 100})
        store.save_profile("users", "volume", {"row_count": 200})
        store.save_profile("users", "volume", {"row_count": 300})
        history = store.get_profile_history("users", "volume", days=30)
        assert len(history) == 3
        assert history[0]["row_count"] == 300  # most recent first
        store.close()

    def test_save_and_get_anomalies(self):
        store = _temp_store()
        anomalies = [
            {
                "table": "users",
                "type": "volume_drop",
                "severity": Severity.CRITICAL,
                "message": "Row count dropped 60%",
            },
            {
                "table": "orders",
                "type": "schema_drift",
                "severity": Severity.WARNING,
                "message": "Column added: email",
            },
        ]
        store.save_anomalies(anomalies)
        history = store.get_anomaly_history(days=30)
        assert len(history) == 2
        assert history[0]["severity"] == "CRITICAL" or history[1]["severity"] == "CRITICAL"
        store.close()

    def test_anomaly_history_empty(self):
        store = _temp_store()
        history = store.get_anomaly_history(days=30)
        assert history == []
        store.close()
