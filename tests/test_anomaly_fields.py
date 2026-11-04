"""Structured anomaly fields: column, metric, baseline, current, change_pct, hint (#108)."""

from __future__ import annotations

import importlib.util
import json
import sqlite3
from pathlib import Path
from types import SimpleNamespace

import pytest
from typer.testing import CliRunner

from scherlok.cli import _dbt_model_payload, app
from scherlok.detector.anomaly import detect_volume_anomalies
from scherlok.detector.anomaly_types import (
    ALL_NULL_COLUMN,
    ANOMALY_TYPES,
    CARDINALITY_CHANGE,
    COLUMN_ADDED,
    COLUMN_REMOVED,
    CONSTANT_COLUMN,
    DISTRIBUTION_SHIFT,
    DUPLICATE_KEY,
    EMPTY_TABLE,
    FRESHNESS_CRITICAL,
    FRESHNESS_STALE,
    NULL_RATE_CHANGE,
    TABLE_EMPTY,
    TYPE_CHANGED,
    VOLUME_DROP,
    VOLUME_SPIKE,
)
from scherlok.detector.cardinality import detect_cardinality_anomalies
from scherlok.detector.distribution_shift import detect_distribution_shift
from scherlok.detector.fields import (
    BASELINE,
    CHANGE_PCT,
    COLUMN,
    CURRENT,
    DETAIL_FIELDS,
    HINT,
    METRIC,
    METRIC_COLUMN_TYPE,
    METRIC_DISTINCT_COUNT,
    METRIC_DISTINCT_RATIO,
    METRIC_HOURS_SINCE_UPDATE,
    METRIC_MEAN,
    METRIC_NULL_COUNT,
    METRIC_NULL_RATE,
    METRIC_ROW_COUNT,
    anomaly_details,
    make_anomaly,
    percent_change,
)
from scherlok.detector.freshness import detect_freshness_anomalies
from scherlok.detector.hints import ANOMALY_HINTS, hint_for
from scherlok.detector.new_table import detect_new_table_anomalies
from scherlok.detector.nullability import detect_nullability_anomalies
from scherlok.detector.schema_drift import detect_schema_drift
from scherlok.detector.severity import Severity
from scherlok.service import anomaly_to_dict
from scherlok.store.sqlite import ProfileStore

requires_duckdb = pytest.mark.skipif(
    importlib.util.find_spec("duckdb") is None, reason="duckdb not installed"
)

runner = CliRunner(env={"NO_COLOR": "1"})

LEGACY_KEYS = ["table", "type", "severity", "message"]
MAX_HINT_LENGTH = 120
TABLE = "orders"


def _history(metric: str, values: list[float]) -> list[dict]:
    return [{metric: value} for value in values]


def _only(anomalies: list[dict], anomaly_type: str) -> dict:
    matching = [a for a in anomalies if a["type"] == anomaly_type]
    assert len(matching) == 1, anomalies
    return matching[0]


def _assert_fields(anomaly: dict, *, column, metric, baseline, current, change_pct) -> None:
    assert anomaly[COLUMN] == column
    assert anomaly[METRIC] == metric
    assert anomaly[BASELINE] == baseline
    assert anomaly[CURRENT] == current
    assert anomaly[CHANGE_PCT] == change_pct
    assert anomaly[HINT] == ANOMALY_HINTS[anomaly["type"]]


# ----- volume ------------------------------------------------------------------

class TestVolumeFields:
    def test_drop(self):
        anomalies = detect_volume_anomalies(TABLE, {"row_count": 300}, {"row_count": 1000})

        drop = _only(anomalies, VOLUME_DROP)
        assert drop["message"] == "Row count dropped 70.0% (1,000 -> 300)"
        _assert_fields(
            drop, column=None, metric=METRIC_ROW_COUNT, baseline=1000, current=300,
            change_pct=-70.0,
        )

    def test_spike(self):
        anomalies = detect_volume_anomalies(TABLE, {"row_count": 5000}, {"row_count": 1000})

        spike = _only(anomalies, VOLUME_SPIKE)
        assert spike["message"] == "Row count spiked 400% (1,000 -> 5,000)"
        _assert_fields(
            spike, column=None, metric=METRIC_ROW_COUNT, baseline=1000, current=5000,
            change_pct=400.0,
        )

    def test_table_empty(self):
        anomalies = detect_volume_anomalies(TABLE, {"row_count": 0}, {"row_count": 1000})

        empty = _only(anomalies, TABLE_EMPTY)
        assert empty["message"] == "Table is now empty (was 1,000 rows)"
        _assert_fields(
            empty, column=None, metric=METRIC_ROW_COUNT, baseline=1000, current=0,
            change_pct=-100.0,
        )

    def test_learned_baseline_is_the_history_median(self):
        anomalies = detect_volume_anomalies(
            TABLE,
            {"row_count": 105},
            {"row_count": 100},
            history=_history("row_count", [98, 99, 100, 101, 102, 100]),
        )

        spike = _only(anomalies, VOLUME_SPIKE)
        assert spike["message"].startswith("Row count spiked (5% change; learned baseline 100 ")
        _assert_fields(
            spike, column=None, metric=METRIC_ROW_COUNT, baseline=100.0, current=105,
            change_pct=5.0,
        )


# ----- schema drift ------------------------------------------------------------

class TestSchemaDriftFields:
    STORED = {"columns": [
        {"name": "amount", "type": "VARCHAR"},
        {"name": "placed_at", "type": "DATE"},
    ]}
    CURRENT = {"columns": [
        {"name": "amount", "type": "DOUBLE"},
        {"name": "channel", "type": "VARCHAR"},
    ]}

    def _detect(self) -> list[dict]:
        return detect_schema_drift(TABLE, self.CURRENT, self.STORED)

    def test_column_removed(self):
        removed = _only(self._detect(), COLUMN_REMOVED)

        assert removed["message"] == "Column 'placed_at' was removed"
        _assert_fields(
            removed, column="placed_at", metric=METRIC_COLUMN_TYPE, baseline="DATE",
            current=None, change_pct=None,
        )

    def test_column_added(self):
        added = _only(self._detect(), COLUMN_ADDED)

        assert added["message"] == "Column 'channel' was added (type: VARCHAR)"
        _assert_fields(
            added, column="channel", metric=METRIC_COLUMN_TYPE, baseline=None,
            current="VARCHAR", change_pct=None,
        )

    def test_type_changed(self):
        changed = _only(self._detect(), TYPE_CHANGED)

        assert changed["message"] == "Column 'amount' type changed: VARCHAR -> DOUBLE"
        _assert_fields(
            changed, column="amount", metric=METRIC_COLUMN_TYPE, baseline="VARCHAR",
            current="DOUBLE", change_pct=None,
        )


# ----- freshness ---------------------------------------------------------------

class TestFreshnessFields:
    def test_stale(self):
        anomalies = detect_freshness_anomalies(
            TABLE, {"hours_since_update": 10.0}, {"hours_since_update": 4.0}
        )

        stale = _only(anomalies, FRESHNESS_STALE)
        assert stale["message"] == (
            "Table update delayed: 10.0h since last update (expected ~4.0h, 2.5x overdue)"
        )
        _assert_fields(
            stale, column=None, metric=METRIC_HOURS_SINCE_UPDATE, baseline=4.0, current=10.0,
            change_pct=150.0,
        )

    def test_critical(self):
        anomalies = detect_freshness_anomalies(
            TABLE, {"hours_since_update": 20.0}, {"hours_since_update": 4.0}
        )

        critical = _only(anomalies, FRESHNESS_CRITICAL)
        assert critical["message"] == (
            "Table has not been updated for 20.0h (expected ~4.0h, 5.0x overdue)"
        )
        _assert_fields(
            critical, column=None, metric=METRIC_HOURS_SINCE_UPDATE, baseline=4.0,
            current=20.0, change_pct=400.0,
        )


# ----- nullability -------------------------------------------------------------

class TestNullabilityFields:
    def test_stored_baseline(self):
        anomalies = detect_nullability_anomalies(
            TABLE, "email", {"null_rate": 0.45}, {"null_rate": 0.02}
        )

        change = _only(anomalies, NULL_RATE_CHANGE)
        assert change["message"] == "Column 'email' NULL rate increased: 2.0% -> 45.0% (Δ43.0%)"
        _assert_fields(
            change, column="email", metric=METRIC_NULL_RATE, baseline=0.02, current=0.45,
            change_pct=2150.0,
        )

    def test_learned_baseline(self):
        anomalies = detect_nullability_anomalies(
            TABLE,
            "email",
            {"null_rate": 0.5},
            {"null_rate": 0.1},
            history=_history("null_rate", [0.09, 0.1, 0.11, 0.1, 0.12, 0.08]),
        )

        change = _only(anomalies, NULL_RATE_CHANGE)
        assert "learned baseline 10.0% -> 50.0%" in change["message"]
        _assert_fields(
            change, column="email", metric=METRIC_NULL_RATE, baseline=0.1, current=0.5,
            change_pct=400.0,
        )


# ----- distribution shift ------------------------------------------------------

class TestDistributionShiftFields:
    def test_stored_baseline(self):
        anomalies = detect_distribution_shift(
            TABLE, "amount", {"mean": 150.0}, {"mean": 100.0, "stddev": 5.0}
        )

        shift = _only(anomalies, DISTRIBUTION_SHIFT)
        assert shift["message"] == "Column 'amount' mean increased: 100 -> 150 (z-score: +10.00)"
        _assert_fields(
            shift, column="amount", metric=METRIC_MEAN, baseline=100.0, current=150.0,
            change_pct=50.0,
        )

    def test_change_pct_sign_follows_direction_for_a_negative_mean(self):
        anomalies = detect_distribution_shift(
            TABLE, "balance", {"mean": -150.0}, {"mean": -100.0, "stddev": 5.0}
        )

        shift = _only(anomalies, DISTRIBUTION_SHIFT)
        assert "decreased" in shift["message"]
        assert shift[CHANGE_PCT] == -50.0

    def test_learned_baseline(self):
        anomalies = detect_distribution_shift(
            TABLE,
            "amount",
            {"mean": 150.0},
            {"mean": 100.0, "stddev": 5.0},
            history=_history("mean", [99, 100, 101, 100, 102, 98]),
        )

        shift = _only(anomalies, DISTRIBUTION_SHIFT)
        _assert_fields(
            shift, column="amount", metric=METRIC_MEAN, baseline=100.0, current=150.0,
            change_pct=50.0,
        )


# ----- cardinality -------------------------------------------------------------

class TestCardinalityFields:
    def test_distinct_count(self):
        anomalies = detect_cardinality_anomalies(
            TABLE, "status", {"distinct_count": 15}, {"distinct_count": 5}
        )

        change = _only(anomalies, CARDINALITY_CHANGE)
        assert change["message"] == (
            "Column 'status' distinct values increased: 5 -> 15 (200% change)"
        )
        _assert_fields(
            change, column="status", metric=METRIC_DISTINCT_COUNT, baseline=5, current=15,
            change_pct=200.0,
        )

    def test_distinct_ratio_on_a_near_unique_column(self):
        anomalies = detect_cardinality_anomalies(
            TABLE,
            "order_id",
            {"distinct_count": 500, "null_count": 0},
            {"distinct_count": 1000, "null_count": 0},
            {"row_count": 1000},
            {"row_count": 1000},
        )

        change = _only(anomalies, CARDINALITY_CHANGE)
        assert change["message"] == (
            "Column 'order_id' distinct ratio decreased: 1.000 -> 0.500 (50% change)"
        )
        _assert_fields(
            change, column="order_id", metric=METRIC_DISTINCT_RATIO, baseline=1.0,
            current=0.5, change_pct=-50.0,
        )

    def test_learned_baseline(self):
        anomalies = detect_cardinality_anomalies(
            TABLE,
            "status",
            {"distinct_count": 300},
            {"distinct_count": 100},
            history=_history("distinct_count", [98, 99, 100, 101, 102, 100]),
        )

        change = _only(anomalies, CARDINALITY_CHANGE)
        _assert_fields(
            change, column="status", metric=METRIC_DISTINCT_COUNT, baseline=100.0,
            current=300, change_pct=200.0,
        )


# ----- new-table checks (#107): no baseline yet --------------------------------

class TestNewTableFields:
    def test_empty_table(self):
        empty = _only(detect_new_table_anomalies(TABLE, {"row_count": 0}, {}, {}), EMPTY_TABLE)

        _assert_fields(
            empty, column=None, metric=METRIC_ROW_COUNT, baseline=None, current=0,
            change_pct=None,
        )

    def test_column_checks(self):
        schema = {"columns": [
            {"name": "id", "type": "INTEGER"},
            {"name": "revenue", "type": "DOUBLE"},
            {"name": "status", "type": "VARCHAR"},
        ]}
        distributions = {
            "id": {"null_count": 0, "distinct_count": 900},
            "revenue": {"null_count": 1000, "distinct_count": 0},
            "status": {"null_count": 0, "distinct_count": 1},
        }

        anomalies = detect_new_table_anomalies(
            TABLE, {"row_count": 1000}, schema, distributions
        )

        _assert_fields(
            _only(anomalies, DUPLICATE_KEY), column="id", metric=METRIC_DISTINCT_COUNT,
            baseline=None, current=900, change_pct=None,
        )
        _assert_fields(
            _only(anomalies, ALL_NULL_COLUMN), column="revenue", metric=METRIC_NULL_COUNT,
            baseline=None, current=1000, change_pct=None,
        )
        _assert_fields(
            _only(anomalies, CONSTANT_COLUMN), column="status", metric=METRIC_DISTINCT_COUNT,
            baseline=None, current=1, change_pct=None,
        )


# ----- hints -------------------------------------------------------------------

def test_every_anomaly_type_has_exactly_one_hint():
    assert set(ANOMALY_HINTS) == ANOMALY_TYPES


@pytest.mark.parametrize("anomaly_type", sorted(ANOMALY_TYPES))
def test_hint_is_one_short_fixed_sentence(anomaly_type):
    hint = hint_for(anomaly_type)

    assert hint == ANOMALY_HINTS[anomaly_type]
    assert len(hint) <= MAX_HINT_LENGTH
    assert hint.endswith(".") and hint.count(".") == 1
    # A template placeholder would mean the text is formatted with profile data.
    assert "{" not in hint and "}" not in hint


def test_hint_does_not_depend_on_the_data():
    small = _only(detect_volume_anomalies("a", {"row_count": 1}, {"row_count": 9}), VOLUME_DROP)
    large = _only(
        detect_volume_anomalies("b", {"row_count": 10}, {"row_count": 9_000}), VOLUME_DROP
    )

    assert small[HINT] == large[HINT] == ANOMALY_HINTS[VOLUME_DROP]


def test_unknown_type_has_no_hint():
    assert hint_for("not_a_type") is None
    assert hint_for(None) is None
    assert make_anomaly(TABLE, "not_a_type", "m", Severity.INFO)[HINT] is None


# ----- percent_change ----------------------------------------------------------

@pytest.mark.parametrize(
    ("baseline", "current", "expected"),
    [
        (1000, 300, -70.0),
        (1000, 5000, 400.0),
        (3, 4, 33.3),
        (-100.0, -150.0, -50.0),
        (-100.0, -50.0, 50.0),
        (0, 10, None),
        (0.0, 0.0, None),
        (None, 10, None),
        (10, None, None),
        ("VARCHAR", "DOUBLE", None),
        (True, 2, None),
        (float("nan"), 1, None),
        (1, float("inf"), None),
    ],
)
def test_percent_change(baseline, current, expected):
    assert percent_change(baseline, current) == expected


# ----- serialisation -----------------------------------------------------------

def test_anomaly_to_dict_keeps_the_four_keys_first_and_adds_the_fields():
    anomaly = detect_volume_anomalies(TABLE, {"row_count": 300}, {"row_count": 1000})[0]

    out = anomaly_to_dict(anomaly)

    assert list(out) == [*LEGACY_KEYS, *DETAIL_FIELDS]
    assert out["severity"] == "CRITICAL"
    assert out["message"] == "Row count dropped 70.0% (1,000 -> 300)"
    assert anomaly_details(out) == anomaly_details(anomaly)
    json.dumps(out)  # JSON-safe


def test_anomaly_to_dict_sets_absent_fields_to_none():
    out = anomaly_to_dict(
        {"table": TABLE, "type": VOLUME_DROP, "severity": "CRITICAL", "message": "m"}
    )

    assert out == {
        "table": TABLE, "type": VOLUME_DROP, "severity": "CRITICAL", "message": "m",
        **dict.fromkeys(DETAIL_FIELDS),
    }


def test_dbt_payload_carries_the_fields():
    node = SimpleNamespace(name="orders", resource_type="model")
    anomaly = detect_volume_anomalies(TABLE, {"row_count": 300}, {"row_count": 1000})[0]

    payload = _dbt_model_payload(node, "orders", {"row_count": 300}, [anomaly])

    (out,) = payload["anomalies"]
    assert list(out) == ["severity", "type", "message", *DETAIL_FIELDS]
    assert out[BASELINE] == 1000 and out[CURRENT] == 300 and out[CHANGE_PCT] == -70.0


# ----- store -------------------------------------------------------------------

def _anomaly_fixtures() -> list[dict]:
    return [
        *detect_volume_anomalies(TABLE, {"row_count": 300}, {"row_count": 1000}),
        *detect_schema_drift(
            TABLE,
            {"columns": [{"name": "amount", "type": "DOUBLE"}]},
            {"columns": [{"name": "amount", "type": "VARCHAR"}]},
        ),
        *detect_nullability_anomalies(
            TABLE, "email", {"null_rate": 0.45}, {"null_rate": 0.02}
        ),
    ]


def test_store_round_trips_the_fields(tmp_path):
    anomalies = _anomaly_fixtures()
    store = ProfileStore(db_path=tmp_path / "profiles.db")
    try:
        store.save_anomalies(anomalies)
        history = store.get_anomaly_history(days=1)
    finally:
        store.close()

    by_type = {record["type"]: record for record in history}
    assert set(by_type) == {a["type"] for a in anomalies}
    for anomaly in anomalies:
        record = by_type[anomaly["type"]]
        assert anomaly_details(record) == anomaly_details(anomaly)
        assert record["message"] == anomaly["message"]


def test_a_row_saved_without_details_reads_back_with_null_fields(tmp_path):
    path = tmp_path / "profiles.db"
    ProfileStore(db_path=path).close()
    conn = sqlite3.connect(path)
    conn.execute(
        "INSERT INTO anomalies (table_name, anomaly_type, severity, message, detected_at) "
        "VALUES (?, ?, ?, ?, datetime('now'))",
        (TABLE, VOLUME_DROP, "CRITICAL", "Row count dropped 50.0% (6 -> 3)"),
    )
    conn.commit()
    conn.close()

    store = ProfileStore(db_path=path)
    try:
        (record,) = store.get_anomaly_history(days=1)
    finally:
        store.close()

    assert record["message"] == "Row count dropped 50.0% (6 -> 3)"
    assert anomaly_details(record) == dict.fromkeys(DETAIL_FIELDS)
    assert anomaly_to_dict(record)[HINT] is None


# ----- end to end: CLI and MCP, and no row value anywhere ----------------------

PEOPLE = "people"
PEOPLE_ROWS = 200
KEPT_ROWS = 60
SENTINEL_EMAIL = "sentinel.person@example.invalid"
SENTINEL_PLAN = "SENTINEL-PLAN-7781"
SENTINEL_NICKNAME = "SENTINEL-NICK-5521"
SENTINEL_LOWEST_SALARY = 424242.125
SENTINEL_HIGHEST_SALARY = 868686.375
SENTINELS = (
    SENTINEL_EMAIL,
    SENTINEL_PLAN,
    SENTINEL_NICKNAME,
    str(SENTINEL_LOWEST_SALARY),
    str(SENTINEL_HIGHEST_SALARY),
)
EXPECTED_FIRST_RUN = {CONSTANT_COLUMN, ALL_NULL_COLUMN}
EXPECTED_AFTER_CHANGES = {
    VOLUME_DROP,
    NULL_RATE_CHANGE,
    CARDINALITY_CHANGE,
    DISTRIBUTION_SHIFT,
    COLUMN_REMOVED,
    COLUMN_ADDED,
    TYPE_CHANGED,
}


def _duck(path: Path, *statements: str) -> None:
    import duckdb

    conn = duckdb.connect(str(path))
    try:
        for sql in statements:
            conn.execute(sql)
    finally:
        conn.close()


@pytest.fixture()
def people_warehouse(tmp_path, monkeypatch) -> Path:
    """A DuckDB table whose text and extreme values are sentinels."""
    path = tmp_path / "warehouse.duckdb"
    _duck(
        path,
        f"CREATE TABLE {PEOPLE} (id INTEGER, email VARCHAR, plan VARCHAR, "
        "document VARCHAR, salary DOUBLE, score INTEGER)",
        f"INSERT INTO {PEOPLE} SELECT i, "
        f"CASE WHEN i <= 100 THEN '{SENTINEL_EMAIL}' ELSE 'other' || i || '@example.invalid' END, "
        f"'{SENTINEL_PLAN}', NULL, "
        f"CASE i WHEN 1 THEN {SENTINEL_LOWEST_SALARY} WHEN 2 THEN {SENTINEL_HIGHEST_SALARY} "
        "ELSE 500000.0 + i END, "
        f"i % 7 FROM range(1, {PEOPLE_ROWS + 1}) t(i)",
    )
    monkeypatch.setenv("SCHERLOK_CONNECTION", f"duckdb:///{path}")
    return path


def _break_people(path: Path) -> None:
    _duck(
        path,
        f"DELETE FROM {PEOPLE} WHERE id > {KEPT_ROWS}",
        f"UPDATE {PEOPLE} SET email = NULL WHERE id % 2 = 0",
        f"UPDATE {PEOPLE} SET salary = 900000.0 + id WHERE id > 2",
        f"ALTER TABLE {PEOPLE} DROP COLUMN document",
        f"ALTER TABLE {PEOPLE} ADD COLUMN nickname VARCHAR",
        f"UPDATE {PEOPLE} SET nickname = '{SENTINEL_NICKNAME}'",
        f"ALTER TABLE {PEOPLE} ALTER score TYPE VARCHAR",
    )


def _store_bytes(scherlok_dir: Path) -> bytes:
    return b"".join(path.read_bytes() for path in sorted(scherlok_dir.glob("profiles.db*")))


def _assert_structured(anomalies: list[dict]) -> None:
    for anomaly in anomalies:
        assert list(anomaly)[:4] == LEGACY_KEYS
        assert anomaly[HINT] == ANOMALY_HINTS[anomaly["type"]]
        assert anomaly[METRIC] is not None


@requires_duckdb
def test_fields_reach_every_output_and_never_carry_a_row_value(
    people_warehouse, isolated_scherlok_dir
):
    from scherlok.mcp import server

    first = runner.invoke(app, ["watch"])
    assert first.exit_code == 0, first.output
    _break_people(people_warehouse)

    verify = runner.invoke(app, ["verify", "--output", "json"])
    watch = runner.invoke(app, ["watch"])
    history = runner.invoke(app, ["history", "--output", "json"])
    mcp_history = server.history(days=1)

    assert verify.exit_code == 1 and watch.exit_code == 1
    verify_anomalies = json.loads(verify.stdout)["anomalies"]
    history_records = json.loads(history.stdout)
    assert {a["type"] for a in verify_anomalies} == EXPECTED_AFTER_CHANGES
    assert {r["type"] for r in history_records} == EXPECTED_FIRST_RUN | EXPECTED_AFTER_CHANGES
    _assert_structured(verify_anomalies)
    _assert_structured(mcp_history["anomalies"])
    for record in history_records:
        assert set(DETAIL_FIELDS) <= set(record)
        assert record[HINT] == ANOMALY_HINTS[record["type"]]

    drop = _only(verify_anomalies, VOLUME_DROP)
    assert (drop[BASELINE], drop[CURRENT], drop[CHANGE_PCT]) == (PEOPLE_ROWS, KEPT_ROWS, -70.0)
    retyped = _only(verify_anomalies, TYPE_CHANGED)
    assert (retyped[COLUMN], retyped[BASELINE], retyped[CURRENT]) == ("score", "INTEGER", "VARCHAR")

    outputs = {
        "verify --output json": verify.stdout,
        "history --output json": history.stdout,
        "MCP history": json.dumps(mcp_history),
        "watch": watch.output,
    }
    for name, text in outputs.items():
        for sentinel in SENTINELS:
            assert sentinel not in text, f"{sentinel!r} found in {name}"
    stored = _store_bytes(isolated_scherlok_dir)
    for sentinel in SENTINELS:
        assert sentinel.encode() not in stored, f"{sentinel!r} found in the store"


# ----- MCP caps ----------------------------------------------------------------

@pytest.fixture()
def broken_people_mcp(people_warehouse, monkeypatch):
    """The MCP server after a baseline of `people` and the changes that break it."""
    from scherlok.mcp import server

    server.investigate()
    _break_people(people_warehouse)
    monkeypatch.setattr(server, "MAX_ANOMALIES", 1)
    return server


@requires_duckdb
def test_max_anomalies_still_caps_watch(broken_people_mcp):
    out = broken_people_mcp.watch()

    assert out["anomaly_count"] > 1
    assert len(out["anomalies"]) == 1
    assert out["truncated"] is True
    assert list(out["anomalies"][0]) == [*LEGACY_KEYS, *DETAIL_FIELDS]


@requires_duckdb
def test_max_anomalies_still_caps_history(broken_people_mcp):
    broken_people_mcp.watch()

    out = broken_people_mcp.history(days=1)

    assert out["count"] > 1
    assert len(out["anomalies"]) == 1
    assert out["truncated"] is True
    assert list(out["anomalies"][0]) == [*LEGACY_KEYS, *DETAIL_FIELDS]
