"""Tests for profile-and-detect orchestration."""

import sqlite3
from datetime import datetime, timezone
from unittest.mock import MagicMock, call, patch

import pytest

from scherlok.detector.fields import DETAIL_FIELDS
from scherlok.detector.severity import Severity
from scherlok.service import (
    UnknownTablesError,
    VerificationResult,
    profile_and_detect,
    select_tables,
    severity_counts,
    verify_tables,
)
from scherlok.store.sqlite import ProfileStore

NO_DETAILS = dict.fromkeys(DETAIL_FIELDS)


class FakeConnector:
    def __init__(self, tables=("users",), row_count=100):
        self.tables = list(tables)
        self.row_count = row_count

    def list_tables(self):
        return self.tables

    def get_row_count(self, table):
        return self.row_count

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


def _store_with_distribution_baseline():
    store = MagicMock()
    store.get_profile_history.return_value = []
    store.get_latest_profile.side_effect = lambda table, profile_type: {
        "volume": {"row_count": 100},
        "distribution:id": {"mean": 10, "stddev": 1, "null_rate": 0, "distinct_count": 100},
    }.get(profile_type)
    return store


def test_persist_false_detects_but_saves_nothing():
    store = _store_with_distribution_baseline()
    drop = {"table": "users", "type": "volume_drop", "severity": Severity.CRITICAL, "message": "m"}

    with patch("scherlok.service.detect_volume_anomalies", return_value=[drop]):
        anomalies, volume = profile_and_detect(FakeConnector(), store, "users", persist=False)

    assert anomalies == [drop]
    assert volume["row_count"] == 100
    store.save_profile.assert_not_called()
    store.save_anomalies.assert_not_called()


def test_persist_defaults_to_saving_every_profile():
    store = _store_with_distribution_baseline()

    profile_and_detect(FakeConnector(), store, "users")

    saved = [c.args[1] for c in store.save_profile.call_args_list]
    assert saved == ["distribution:id", "volume", "schema", "freshness"]


# ----- verify_tables ---------------------------------------------------------

def _row_counts(db_path):
    conn = sqlite3.connect(db_path)
    try:
        return {
            name: conn.execute(f"SELECT COUNT(*) FROM {name}").fetchone()[0]
            for name in ("profiles", "anomalies")
        }
    finally:
        conn.close()


@pytest.fixture()
def store(tmp_path):
    profile_store = ProfileStore(tmp_path / "profiles.db")
    yield profile_store
    profile_store.close()


def test_verify_tables_detects_without_saving_and_repeats_exactly(store, tmp_path):
    connector = FakeConnector(tables=["users", "orders"])
    profile_and_detect(connector, store, "users")  # baseline for users only
    connector.row_count = 30  # lost 70% of rows
    before = _row_counts(tmp_path / "profiles.db")

    first = verify_tables(connector, store)
    second = verify_tables(connector, store)

    assert first.tables_verified == ["users"]
    assert first.tables_without_baseline == ["orders"]
    assert first.anomalies[0]["type"] == "volume_drop"
    assert {a["table"] for a in first.anomalies} == {"users"}
    assert first.passed is False
    assert second.to_dict() == first.to_dict()
    assert _row_counts(tmp_path / "profiles.db") == before


def test_verify_tables_scope_and_threshold(store):
    connector = FakeConnector(tables=["users", "orders"])
    profile_and_detect(connector, store, "users")
    connector.row_count = 70  # lost 30%: WARNING

    lenient = verify_tables(connector, store, ["users"], fail_on="critical")
    strict = verify_tables(connector, store, ["users"], fail_on="WARNING")

    assert lenient.tables_verified == ["users"] and lenient.tables_without_baseline == []
    assert lenient.passed is True
    assert strict.passed is False
    assert strict.fail_on == "warning"


def test_verify_tables_rejects_unknown_tables_and_thresholds(store):
    connector = FakeConnector()
    with pytest.raises(UnknownTablesError, match="ghost"):
        verify_tables(connector, store, ["ghost"])
    with pytest.raises(ValueError, match="fail_on"):
        verify_tables(connector, store, fail_on="banana")


def test_table_without_baseline_never_fails_the_gate_on_its_own(store):
    result = verify_tables(FakeConnector(row_count=0), store)

    assert result.tables_verified == []
    assert result.tables_without_baseline == ["users"]
    assert result.passed is True


def test_verification_result_to_dict():
    result = VerificationResult(
        fail_on="critical",
        tables_verified=["users"],
        tables_without_baseline=["orders"],
        anomalies=[
            {"table": "users", "type": "volume_drop", "severity": Severity.CRITICAL,
             "message": "m"},
            {"table": "users", "type": "null_rate", "severity": Severity.WARNING,
             "message": "n"},
        ],
    )

    assert result.to_dict() == {
        "passed": False,
        "fail_on": "critical",
        "against": None,
        "ignored": [],
        "tables_verified": ["users"],
        "tables_without_baseline": ["orders"],
        "anomaly_count": 2,
        "critical": 1,
        "warning": 1,
        "info": 0,
        "anomalies": [
            {"table": "users", "type": "volume_drop", "severity": "CRITICAL", "message": "m",
             **NO_DETAILS},
            {"table": "users", "type": "null_rate", "severity": "WARNING", "message": "n",
             **NO_DETAILS},
        ],
    }


def test_select_tables():
    assert select_tables(None, ["a", "b"]) == ["a", "b"]
    assert select_tables([], ["a", "b"]) == ["a", "b"]
    assert select_tables(["b"], ["a", "b"]) == ["b"]
    assert select_tables(["b", "a", "b"], ["a", "b"]) == ["b", "a"]
    with pytest.raises(ValueError, match="not found"):
        select_tables(["c"], ["a", "b"])


def test_severity_counts_accepts_enums_and_stored_strings():
    anomalies = [
        {"severity": Severity.CRITICAL},
        {"severity": "CRITICAL"},
        {"severity": Severity.INFO},
    ]
    assert severity_counts(anomalies) == {"info": 1, "warning": 0, "critical": 2}
