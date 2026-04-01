"""Tests for profiler modules using mock connectors."""

from datetime import datetime, timezone
from unittest.mock import MagicMock

from scherlok.profiler.distribution import profile_distribution
from scherlok.profiler.freshness import profile_freshness
from scherlok.profiler.schema import profile_schema
from scherlok.profiler.volume import profile_volume


def _mock_connector() -> MagicMock:
    """Create a mock connector with realistic return values."""
    connector = MagicMock()
    connector.get_row_count.return_value = 1000
    connector.get_columns.return_value = [
        {"name": "id", "type": "integer", "nullable": False},
        {"name": "name", "type": "character varying", "nullable": True},
        {"name": "score", "type": "numeric", "nullable": True},
    ]
    connector.get_column_stats.return_value = {
        "mean": 50.5,
        "stddev": 10.2,
        "min": "1",
        "max": "100",
        "null_count": 5,
        "distinct_count": 95,
        "top_values": [{"value": "42", "count": 20}],
    }
    connector.get_last_modified.return_value = datetime(
        2026, 3, 31, 12, 0, 0, tzinfo=timezone.utc
    )
    return connector


def test_profile_volume_returns_row_count():
    """Test that volume profiling returns row_count and timestamp."""
    connector = _mock_connector()
    result = profile_volume(connector, "users")
    assert result["row_count"] == 1000
    assert "timestamp" in result


def test_profile_schema_returns_columns():
    """Test that schema profiling returns column list."""
    connector = _mock_connector()
    result = profile_schema(connector, "users")
    assert len(result["columns"]) == 3
    assert result["columns"][0]["name"] == "id"
    assert result["columns"][1]["nullable"] is True


def test_profile_distribution_returns_stats():
    """Test that distribution profiling returns all expected stats."""
    connector = _mock_connector()
    result = profile_distribution(connector, "users", "score")
    assert result["mean"] == 50.5
    assert result["stddev"] == 10.2
    assert result["null_count"] == 5
    assert result["null_rate"] == 0.005
    assert result["distinct_count"] == 95
    assert len(result["top_values"]) == 1


def test_profile_distribution_zero_rows():
    """Test distribution profiling with zero rows handles null_rate."""
    connector = _mock_connector()
    connector.get_row_count.return_value = 0
    connector.get_column_stats.return_value = {
        "mean": None,
        "stddev": None,
        "min": None,
        "max": None,
        "null_count": 0,
        "distinct_count": 0,
        "top_values": [],
    }
    result = profile_distribution(connector, "empty_table", "col")
    assert result["null_rate"] == 0.0


def test_profile_freshness_returns_last_modified():
    """Test that freshness profiling returns last_modified info."""
    connector = _mock_connector()
    result = profile_freshness(connector, "users")
    assert result["last_modified"] is not None
    assert result["hours_since_update"] is not None
    assert result["hours_since_update"] > 0


def test_profile_freshness_handles_none():
    """Test freshness profiling when last_modified is None."""
    connector = _mock_connector()
    connector.get_last_modified.return_value = None
    result = profile_freshness(connector, "users")
    assert result["last_modified"] is None
    assert result["hours_since_update"] is None
