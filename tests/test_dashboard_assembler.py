"""Tests for the dashboard view-model assembler."""

from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

from scherlok.dashboard.assembler import (
    _humanize_iso,
    _redact_connection,
    assemble_view,
)
from scherlok.detector.severity import Severity
from scherlok.store.sqlite import ProfileStore


@pytest.fixture
def store(tmp_path: Path):
    s = ProfileStore(db_path=tmp_path / "profiles.db")
    yield s
    s.close()


def _seed_profile(
    store: ProfileStore,
    table: str,
    rows: int,
    timestamp: str = "2026-04-30T12:00:00Z",
):
    store.save_profile(table, "volume", {"row_count": rows, "timestamp": timestamp})
    store.save_profile(table, "schema", {"columns": [{"name": "id", "type": "int"}]})
    store.save_profile(table, "freshness", {})


def test_assemble_empty_store_returns_empty_view(store):
    view = assemble_view(store, project_name="test")
    assert view["meta"]["project_name"] == "test"
    assert view["kpis"]["tables_monitored"] == 0
    assert view["incidents"] == []
    assert view["history"] == []


def test_assemble_with_profiles_no_anomalies(store):
    _seed_profile(store, "orders", 1000)
    _seed_profile(store, "users", 500)
    view = assemble_view(store)
    assert view["kpis"]["tables_monitored"] == 2
    assert view["kpis"]["critical"] == 0
    assert view["kpis"]["warnings"] == 0
    assert {t["name"] for t in view["tables"]} == {"orders", "users"}


def test_assemble_stale_tables_lists_only_tables_over_threshold(store):
    now = datetime.now(timezone.utc)
    _seed_profile(store, "fresh_orders", 1000, (now - timedelta(hours=1)).isoformat())
    _seed_profile(store, "stale_orders", 900, (now - timedelta(hours=48)).isoformat())

    view = assemble_view(store)

    assert [table["name"] for table in view["stale_tables"]] == ["stale_orders"]
    assert view["stale_tables"][0]["age_hours"] >= 48


def test_assemble_stale_tables_empty_when_all_tables_are_fresh(store):
    now = datetime.now(timezone.utc)
    _seed_profile(store, "orders", 1000, (now - timedelta(hours=1)).isoformat())
    _seed_profile(store, "users", 500, (now - timedelta(hours=2)).isoformat())

    view = assemble_view(store)

    assert view["stale_tables"] == []


def test_assemble_active_anomalies_split_from_history(store):
    """Anomalies from the most recent batch -> incidents; older -> history."""
    _seed_profile(store, "orders", 1000)
    # Two batches of anomalies, different timestamps
    store.save_anomalies([
        {"table": "orders", "type": "volume_drop", "severity": Severity.CRITICAL,
         "message": "Row count dropped 60.0% (10 -> 4)"},
    ])
    # Bump the wall clock by sleeping briefly so the second insert has a later
    # detected_at — easier than mucking with datetime mocks.
    import time
    time.sleep(0.05)
    store.save_anomalies([
        {"table": "orders", "type": "volume_drop", "severity": Severity.CRITICAL,
         "message": "Row count dropped 80.0% (10 -> 2)"},
    ])

    view = assemble_view(store)
    assert len(view["incidents"]) == 1  # only the latest run is "active"
    assert view["incidents"][0].severity == "CRITICAL"
    assert len(view["history"]) == 1  # the older one is history


def test_assemble_dbt_context_passthrough(store):
    ctx = {"project_name": "jaffle_shop", "adapter": "postgres",
           "models_count": 3, "sources_count": 1}
    view = assemble_view(store, dbt_context=ctx)
    assert view["dbt_context"] == ctx


def test_assemble_no_dbt_context_default(store):
    view = assemble_view(store)
    assert view["dbt_context"] is None


def test_redact_connection_masks_password():
    out = _redact_connection("postgresql://alice:s3cret@host:5432/demo")
    assert "s3cret" not in out
    assert "alice" in out
    assert "***" in out


def test_redact_connection_passthrough_when_no_password():
    assert _redact_connection("bigquery://my-proj/my-dataset") == "bigquery://my-proj/my-dataset"
    assert _redact_connection("snowflake://acc/db/schema") == "snowflake://acc/db/schema"


def test_humanize_iso_recent():
    iso = (datetime.now(timezone.utc) - timedelta(minutes=2)).isoformat()
    assert "min ago" in _humanize_iso(iso)


def test_humanize_iso_old():
    iso = (datetime.now(timezone.utc) - timedelta(days=30)).isoformat()
    out = _humanize_iso(iso)
    assert ":" in out  # falls back to ISO-like format


def test_humanize_iso_none():
    assert _humanize_iso(None) == "—"
