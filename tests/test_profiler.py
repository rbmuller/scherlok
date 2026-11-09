"""Tests for profiler modules using mock connectors."""

from datetime import datetime, timedelta, timezone
from unittest.mock import MagicMock

import pytest

from scherlok.profiler.distribution import profile_distribution
from scherlok.profiler.freshness import (
    FRESHNESS_COLUMN,
    FRESHNESS_SOURCE,
    SOURCE_COLUMN,
    SOURCE_METADATA,
    freshness_column,
    profile_freshness,
)
from scherlok.profiler.schema import profile_schema
from scherlok.profiler.volume import profile_volume
from scherlok.store.sqlite import RAW_VALUE_KEYS


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
        "null_count": 5,
        "distinct_count": 95,
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
    assert result.keys().isdisjoint(RAW_VALUE_KEYS)


def test_profile_distribution_drops_column_values_from_connector():
    """A connector that still returns row values cannot get them into a profile."""
    connector = _mock_connector()
    connector.get_column_stats.return_value = {
        "mean": 50.5,
        "stddev": 10.2,
        "min": "sentinel-min",
        "max": "sentinel-max",
        "null_count": 5,
        "distinct_count": 95,
        "top_values": [{"value": "sentinel-top", "count": 20}],
    }
    result = profile_distribution(connector, "users", "score")
    assert result == {
        "mean": 50.5,
        "stddev": 10.2,
        "null_count": 5,
        "null_rate": 0.005,
        "distinct_count": 95,
    }


def test_profile_distribution_zero_rows():
    """Test distribution profiling with zero rows handles null_rate."""
    connector = _mock_connector()
    connector.get_row_count.return_value = 0
    connector.get_column_stats.return_value = {
        "mean": None,
        "stddev": None,
        "null_count": 0,
        "distinct_count": 0,
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


# ----- freshness from a load-time column (#87, #139) -------------------------

class _FreshnessConnector:
    """Just what `profile_freshness` reads: metadata, the schema and MAX(column)."""

    def __init__(self, *, metadata=None, columns=(), latest=None, metadata_is_last_write=True):
        self.METADATA_IS_LAST_WRITE = metadata_is_last_write
        self.metadata = metadata
        self.columns = list(columns)
        self.latest = latest
        self.latest_reads: list[tuple[str, str]] = []

    def get_last_modified(self, table):
        return self.metadata

    def get_columns(self, table):
        return self.columns

    def get_latest_timestamp(self, table, column):
        self.latest_reads.append((table, column))
        return self.latest


def _hours_ago(hours: float) -> datetime:
    return datetime.now(timezone.utc) - timedelta(hours=hours)


UPDATED_AT = {"name": "updated_at", "type": "timestamp without time zone", "nullable": False}
CREATED_AT = {"name": "created_at", "type": "timestamp without time zone", "nullable": False}
BIRTH_DATE = {"name": "birth_date", "type": "date", "nullable": True}
SHIPPED_AT = {"name": "shipped_at", "type": "datetime", "nullable": True}
STATUS = {"name": "status", "type": "text", "nullable": False}


class TestFreshnessColumn:
    def test_prefers_the_update_time_over_the_creation_time(self):
        assert freshness_column([CREATED_AT, UPDATED_AT]) == "updated_at"

    def test_reads_no_timestamp_outside_the_load_time_names(self):
        """`expires_at` is always in the future, `deleted_at` rarely moves and
        `last_login_at` is someone's login: none dates a load (#144 review)."""
        others = [SHIPPED_AT, {"name": "expires_at", "type": "timestamp"},
                  {"name": "last_login_at", "type": "timestamp"}]
        assert freshness_column([STATUS, *others]) is None

    def test_reads_the_etl_tools_load_columns(self):
        synced = {"name": "_fivetran_synced", "type": "TIMESTAMP_TZ"}
        assert freshness_column([CREATED_AT, synced]) == "_fivetran_synced"

    def test_never_reads_a_business_date(self):
        """The stored age gives the column's latest value away, so a birth date is
        never a candidate."""
        assert freshness_column([BIRTH_DATE, STATUS]) is None

    def test_needs_a_temporal_type(self):
        assert freshness_column([{"name": "updated_at", "type": "text"}]) is None

    @pytest.mark.parametrize(
        "column_type",
        ["TIMESTAMP_NTZ", "TIMESTAMP WITH TIME ZONE", "datetime(6)", "DATE", "timestamp"],
    )
    def test_accepts_every_dialect_of_timestamp_and_date(self, column_type):
        assert freshness_column([{"name": "loaded_at", "type": column_type}]) == "loaded_at"

    def test_matches_names_case_insensitively(self):
        assert freshness_column([{"name": "UPDATED_AT", "type": "TIMESTAMP_NTZ"}]) == "UPDATED_AT"


class TestFreshnessSources:
    def test_trusted_metadata_comes_first(self):
        connector = _FreshnessConnector(
            metadata=_hours_ago(2), columns=[UPDATED_AT], latest=_hours_ago(9)
        )

        result = profile_freshness(connector, "orders")

        assert result[FRESHNESS_SOURCE] == SOURCE_METADATA
        assert result[FRESHNESS_COLUMN] is None
        assert result["hours_since_update"] == pytest.approx(2, abs=0.05)
        assert connector.latest_reads == []

    def test_no_metadata_falls_back_to_the_column(self):
        """#87: DuckDB has no per-table modification time."""
        connector = _FreshnessConnector(columns=[UPDATED_AT], latest=_hours_ago(3))

        result = profile_freshness(connector, "orders")

        assert result[FRESHNESS_SOURCE] == SOURCE_COLUMN
        assert result[FRESHNESS_COLUMN] == "updated_at"
        assert result["hours_since_update"] == pytest.approx(3, abs=0.05)

    def test_untrusted_metadata_reads_the_column_first(self):
        """#139: PostgreSQL reports vacuum/analyze times, MySQL a cached value."""
        connector = _FreshnessConnector(
            metadata=_hours_ago(1), columns=[UPDATED_AT], latest=_hours_ago(26),
            metadata_is_last_write=False,
        )

        result = profile_freshness(connector, "orders")

        assert result[FRESHNESS_SOURCE] == SOURCE_COLUMN
        assert result["hours_since_update"] == pytest.approx(26, abs=0.05)

    def test_untrusted_metadata_is_still_used_without_a_column(self):
        connector = _FreshnessConnector(
            metadata=_hours_ago(5), columns=[STATUS], metadata_is_last_write=False
        )

        result = profile_freshness(connector, "orders")

        assert result[FRESHNESS_SOURCE] == SOURCE_METADATA
        assert result["hours_since_update"] == pytest.approx(5, abs=0.05)

    def test_a_column_the_warehouse_cannot_aggregate_falls_back_to_metadata(self):
        """A BigQuery table that needs a partition filter, a missing driver module:
        one table's freshness query must not stop the run (#144 review)."""

        class Failing(_FreshnessConnector):
            def get_latest_timestamp(self, table, column):
                raise RuntimeError("Cannot query over table without a filter")

        connector = Failing(
            metadata=_hours_ago(5), columns=[UPDATED_AT], metadata_is_last_write=False
        )

        result = profile_freshness(connector, "orders")

        assert result[FRESHNESS_SOURCE] == SOURCE_METADATA
        assert result["hours_since_update"] == pytest.approx(5, abs=0.05)

    def test_an_empty_column_falls_back_to_metadata(self):
        connector = _FreshnessConnector(
            metadata=_hours_ago(5), columns=[UPDATED_AT], latest=None,
            metadata_is_last_write=False,
        )

        assert profile_freshness(connector, "orders")[FRESHNESS_SOURCE] == SOURCE_METADATA

    def test_nothing_to_read_reports_no_source(self):
        result = profile_freshness(_FreshnessConnector(columns=[STATUS]), "orders")

        assert result == {
            "last_modified": None,
            "hours_since_update": None,
            FRESHNESS_SOURCE: None,
            FRESHNESS_COLUMN: None,
        }

    def test_uses_the_schema_the_caller_already_has(self):
        connector = _FreshnessConnector(latest=_hours_ago(4))

        result = profile_freshness(connector, "orders", columns=[UPDATED_AT])

        assert result[FRESHNESS_COLUMN] == "updated_at"

    def test_a_date_column_counts_from_midnight_utc(self):
        yesterday = (datetime.now(timezone.utc) - timedelta(days=1)).date()
        connector = _FreshnessConnector(
            columns=[{"name": "loaded_at", "type": "date"}], latest=yesterday
        )

        result = profile_freshness(connector, "orders")

        assert 24 <= result["hours_since_update"] < 48

    def test_a_naive_timestamp_is_read_as_utc(self):
        naive = _hours_ago(6).replace(tzinfo=None)
        connector = _FreshnessConnector(columns=[UPDATED_AT], latest=naive)

        assert profile_freshness(connector, "orders")["hours_since_update"] == pytest.approx(
            6, abs=0.05
        )

    def test_a_timestamp_in_the_future_is_an_age_of_zero(self):
        """A naive timestamp in a zone ahead of UTC reads as the future."""
        connector = _FreshnessConnector(columns=[UPDATED_AT], latest=_hours_ago(-2))

        assert profile_freshness(connector, "orders")["hours_since_update"] == 0.0
