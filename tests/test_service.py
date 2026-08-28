"""Tests for profile-and-detect orchestration."""

from datetime import datetime, timezone
from unittest.mock import MagicMock, call, patch

from scherlok.service import profile_and_detect


class FakeConnector:
    def get_row_count(self, table):
        return 100

    def get_columns(self, table):
        return [{"name": "id", "type": "integer", "nullable": False}]

    def get_column_stats(self, table, column):
        return {
            "mean": 10,
            "stddev": 1,
            "min": 1,
            "max": 20,
            "null_count": 0,
            "distinct_count": 100,
        }

    def get_last_modified(self, table):
        return datetime.now(timezone.utc)


def test_profile_and_detect_reads_and_reuses_distribution_history():
    distribution_history = [{"mean": 10, "null_rate": 0, "distinct_count": 100}]
    store = MagicMock()
    store.get_profile_history.side_effect = [[], distribution_history]
    store.get_latest_profile.side_effect = lambda table, profile_type: {
        "volume": {"row_count": 100},
        "distribution:id": {
            "mean": 10,
            "stddev": 1,
            "null_rate": 0,
            "distinct_count": 100,
        },
    }.get(profile_type)

    with (
        patch("scherlok.service.detect_volume_anomalies", return_value=[]) as volume,
        patch("scherlok.service.detect_nullability_anomalies", return_value=[]) as nullability,
        patch("scherlok.service.detect_distribution_shift", return_value=[]) as distribution,
        patch("scherlok.service.detect_cardinality_anomalies", return_value=[]) as cardinality,
    ):
        profile_and_detect(FakeConnector(), store, "users")

    assert store.get_profile_history.call_args_list == [
        call("users", "volume", days=None, limit=30),
        call("users", "distribution:id", days=None, limit=30),
    ]
    assert volume.call_args.kwargs["history"] == []
    assert nullability.call_args.kwargs["history"] is distribution_history
    assert distribution.call_args.kwargs["history"] is distribution_history
    assert cardinality.call_args.kwargs["history"] is distribution_history
    assert store.method_calls[0] == call.get_profile_history(
        "users", "volume", days=None, limit=30
    )
    assert next(
        index for index, item in enumerate(store.method_calls) if item[0] == "save_profile"
    ) > 0
    assert store.save_profile.call_args_list[0][0][:2] == ("users", "distribution:id")
    assert store.save_profile.call_args_list[1][0][:2] == ("users", "volume")
