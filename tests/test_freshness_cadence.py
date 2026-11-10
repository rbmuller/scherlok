"""#149: freshness compares a table's age with the cadence learned from its loads.

Each scenario runs the detector the way consecutive `watch` runs do: every
reading becomes part of the history of the next run, since freshness findings
never block learning (#116). A load is one `last_modified`; a run during a
stall reads the same load as the run before it, older.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

from scherlok.cli import _table_health
from scherlok.detector.adaptive import ADAPTIVE_HISTORY_LIMIT, MIN_ADAPTIVE_SAMPLES
from scherlok.detector.anomaly_types import FRESHNESS_CRITICAL
from scherlok.detector.freshness import (
    FRESHNESS_MIN_HOURS,
    detect_freshness_anomalies,
    learned_age,
)
from scherlok.detector.severity import Severity
from scherlok.profiler.freshness import (
    FRESHNESS_COLUMN,
    FRESHNESS_SOURCE,
    HOURS_SINCE_UPDATE,
    LAST_MODIFIED,
    SOURCE_COLUMN,
    SOURCE_METADATA,
)
from scherlok.profiler.schema import profile_schema
from scherlok.service import FRESHNESS_PROFILE, VOLUME_PROFILE, inspect_table
from scherlok.store.sqlite import ProfileStore

TABLE = "orders"
EPOCH = datetime(2026, 11, 1, tzinfo=timezone.utc)
METADATA = (SOURCE_METADATA, None)
UPDATED_AT = (SOURCE_COLUMN, "updated_at")
WARNING, CRITICAL = Severity.WARNING, Severity.CRITICAL


def _load(index: int) -> str:
    """The `last_modified` of a table's index-th load."""
    return (EPOCH + timedelta(hours=index)).isoformat()


def _reading(load: int, hours: float | None, source: tuple[str, str | None] = METADATA) -> dict:
    return {
        LAST_MODIFIED: _load(load),
        HOURS_SINCE_UPDATE: hours,
        FRESHNESS_SOURCE: source[0],
        FRESHNESS_COLUMN: source[1],
    }


def _watch(runs: list[tuple[int, float]], source=METADATA) -> list[Severity | None]:
    """The freshness severity of each run of ``(load, age)``; the first run has no baseline."""
    history: list[dict] = []  # newest first, as the store returns it
    severities: list[Severity | None] = []
    for load, hours in runs:
        current = _reading(load, hours, source)
        anomalies = (
            detect_freshness_anomalies(
                TABLE, current, history[0], history=history[:ADAPTIVE_HISTORY_LIMIT]
            )
            if history
            else []
        )
        severities.append(anomalies[0]["severity"] if anomalies else None)
        history.insert(0, current)
    return severities


def _stall(last_load: int, ages: list[float]) -> list[tuple[int, float]]:
    """Runs that keep reading ``last_load``, each older than the one before."""
    return [(last_load, hours) for hours in ages]


@pytest.mark.parametrize("prior_loads", [MIN_ADAPTIVE_SAMPLES, ADAPTIVE_HISTORY_LIMIT])
def test_a_table_watched_as_often_as_it_loads_alerts_on_a_stall(prior_loads):
    """The issue's scenario: hourly loads, `watch` hourly at :30. The previous
    run's age (0.5 h, then the stall's own ages) never raised anything."""
    loading = [(load, 0.5) for load in range(prior_loads)]
    stalled = _stall(prior_loads - 1, [1.5, 2.5, 3.5, 4.5])

    severities = _watch(loading + stalled)[prior_loads:]

    assert severities == [None, WARNING, WARNING, CRITICAL]


def test_the_stall_keeps_alerting_while_its_runs_are_one_load():
    loading = [(load, 0.5) for load in range(10)]
    stalled = _stall(9, [float(hours) + 0.5 for hours in range(1, 16)])

    severities = _watch(loading + stalled)[10:]

    assert severities[3:] == [CRITICAL] * 12


def test_a_daily_table_watched_daily_behaves_as_before():
    """Loaded at 06:00, watched at 08:00; the load stops for two days."""
    loading = [(day, 2.0 + 0.1 * (day % 3)) for day in range(10)]
    stalled = _stall(9, [26.0, 50.0])

    severities = _watch(loading + stalled)

    assert severities[1:10] == [None] * 9
    assert severities[10] == CRITICAL
    previous_run_rule = detect_freshness_anomalies(
        TABLE, _reading(9, 26.0), _reading(9, 2.0)
    )
    assert [a["severity"] for a in previous_run_rule] == [CRITICAL]
    # The second day used to read 50 h against 26 h, under 2x: no alert.
    assert severities[11] == CRITICAL


def test_a_weekly_table_watched_daily_is_quiet_once_its_loads_are_known():
    """Ages climb from 12 h to 156 h and drop on each load; against the previous
    run, the day after every load read as 3x overdue."""
    week = [12.0, 36.0, 60.0, 84.0, 108.0, 132.0, 156.0]
    runs = [(load, hours) for load in range(8) for hours in week]

    severities = _watch(runs)

    assert severities[1] == WARNING  # cold start: 36 h against 12 h
    first_known = MIN_ADAPTIVE_SAMPLES * len(week)
    assert severities[first_known:] == [None] * (len(runs) - first_known)


def test_a_table_watched_more_often_than_it_loads_alerts_on_a_stall():
    """Three-hourly loads watched hourly read 0.5 h, 1.5 h and 2.5 h; against the
    previous run, the stall's ages only ever climbed by less than 2x."""
    runs = [(load, hours) for load in range(10) for hours in (0.5, 1.5, 2.5)]
    stalled = _stall(9, [3.5, 4.5, 5.5, 6.5, 7.5, 8.5, 9.5, 10.5])

    severities = _watch(runs + stalled)

    assert severities[len(runs):] == [None, None] + [WARNING] * 5 + [CRITICAL]
    learned = len(runs) - ADAPTIVE_HISTORY_LIMIT
    assert severities[learned:len(runs)] == [None] * ADAPTIVE_HISTORY_LIMIT


def test_an_hourly_table_watched_daily_alerts_when_the_loads_stop():
    """Its age is under an hour on every run; the previous run's age was never
    compared, since under FRESHNESS_MIN_HOURS it was skipped."""
    ages = [0.2, 0.7, 0.4, 0.9, 0.1, 0.6, 0.3]
    loading = [(day * 24, hours) for day, hours in enumerate(ages)]
    last_load = loading[-1][0]

    severities = _watch(loading + _stall(last_load, [24.3]))

    assert severities[:-1] == [None] * len(ages)
    assert severities[-1] == CRITICAL


def test_a_table_loaded_more_often_than_watched_is_quiet_once_its_loads_are_known():
    """Six-hourly loads watched daily read any age from 0 h to 6 h; against the
    previous run, 3 h after 1 h read as 3x overdue."""
    ages = [1.0, 3.0, 5.0, 5.9, 0.5, 4.0, 2.0, 5.5, 0.2, 6.0]
    runs = [(day * 24, hours) for day, hours in enumerate(ages)]

    severities = _watch(runs)

    assert severities[MIN_ADAPTIVE_SAMPLES:] == [None] * (len(ages) - MIN_ADAPTIVE_SAMPLES)


def test_ages_from_another_source_are_not_learned():
    """#144: the first runs after an upgrade read a column; the metadata ages
    learned before don't count, and the previous run read another source."""
    metadata_loads = [_reading(load, 0.5) for load in range(ADAPTIVE_HISTORY_LIMIT)]
    history = list(reversed(metadata_loads))
    current = _reading(ADAPTIVE_HISTORY_LIMIT, 4.5, UPDATED_AT)

    assert learned_age(history, UPDATED_AT) is None
    assert detect_freshness_anomalies(TABLE, current, history[0], history=history) == []


def test_only_ages_from_the_current_source_are_learned():
    column_loads = [_reading(load, 0.5, UPDATED_AT) for load in range(MIN_ADAPTIVE_SAMPLES)]
    metadata_loads = [_reading(load, 50.0) for load in range(MIN_ADAPTIVE_SAMPLES)]
    history = column_loads + metadata_loads
    current = _reading(MIN_ADAPTIVE_SAMPLES, 4.5, UPDATED_AT)

    assert learned_age(history, UPDATED_AT) == 0.5
    anomalies = detect_freshness_anomalies(TABLE, current, history[0], history=history)

    assert [a["severity"] for a in anomalies] == [CRITICAL]
    assert anomalies[0]["column"] == "updated_at"
    assert "based on updated_at" in anomalies[0]["message"]


def test_fewer_loads_than_needed_keep_the_previous_run_rule():
    history = [_reading(load, 0.5) for load in range(MIN_ADAPTIVE_SAMPLES - 1)]

    assert learned_age(history, METADATA) is None
    # The previous run read 0.5 h, under FRESHNESS_MIN_HOURS: not compared.
    assert detect_freshness_anomalies(TABLE, _reading(3, 1.5), history[0], history=history) == []


def test_a_timestamp_in_the_future_counts_as_a_load_at_age_zero():
    """Saved as 0 h; the next run used to skip its comparison."""
    history = [_reading(load, 0.0) for load in range(MIN_ADAPTIVE_SAMPLES)]

    assert learned_age(history, METADATA) == 0.0
    anomalies = detect_freshness_anomalies(TABLE, _reading(4, 2.5), history[0], history=history)

    assert [a["severity"] for a in anomalies] == [WARNING]
    assert anomalies[0]["baseline"] == FRESHNESS_MIN_HOURS


def test_a_load_counts_at_the_oldest_age_a_run_saw_it():
    history = [_reading(0, 9.0), _reading(0, 1.0), _reading(0, 5.0)]
    history += [_reading(load, 9.0) for load in range(1, MIN_ADAPTIVE_SAMPLES)]

    assert learned_age(history, METADATA) == 9.0


@pytest.mark.parametrize("bad", [None, True, -1.0, float("nan"), float("inf"), "2.0"])
def test_ages_that_are_not_hours_are_ignored(bad):
    history = [_reading(load, 0.5) for load in range(MIN_ADAPTIVE_SAMPLES)]
    history.append(_reading(MIN_ADAPTIVE_SAMPLES, 0.5) | {HOURS_SINCE_UPDATE: bad})
    history.append({HOURS_SINCE_UPDATE: 0.5})  # no last_modified: no load to count

    assert learned_age(history, METADATA) == 0.5
    assert learned_age(history[MIN_ADAPTIVE_SAMPLES - 1:], METADATA) is None


def test_the_message_names_the_learned_age():
    history = [_reading(load, 3.0) for load in range(MIN_ADAPTIVE_SAMPLES)]

    anomaly = detect_freshness_anomalies(TABLE, _reading(4, 13.0), history[0], history=history)[0]

    assert anomaly["type"] == FRESHNESS_CRITICAL
    assert anomaly["baseline"] == 3.0
    assert "expected ~3.0h, 4.3x overdue" in anomaly["message"]


# ----- through the store, as `watch` reads it -----------------------------------

class StaleConnector:
    """A table last written ``hours`` ago; nothing else is profiled."""

    METADATA_IS_LAST_WRITE = True

    def __init__(self, hours: float):
        self.last_write = datetime.now(timezone.utc) - timedelta(hours=hours)

    def get_row_count(self, table):
        return 100

    def get_columns(self, table):
        return []

    def get_last_modified(self, table):
        return self.last_write


@pytest.fixture()
def stalled_store(tmp_path):
    """Six hourly loads and a stall of 1.5 h, as earlier `watch` runs saved them."""
    store = ProfileStore(tmp_path / "profiles.db")
    readings = [_reading(load, 0.5) for load in range(6)] + [_reading(5, 1.5)]
    for reading in readings:
        store.save_profile(TABLE, VOLUME_PROFILE, {"row_count": 100})
        store.save_profile(TABLE, FRESHNESS_PROFILE, reading)
    yield store
    store.close()


def test_inspect_table_learns_the_cadence_from_the_store(stalled_store):
    """At 2.5 h the previous run's rule read 1.7x; the learned cadence reads 2.5x."""
    anomalies, _ = inspect_table(StaleConnector(hours=2.5), stalled_store, TABLE)

    freshness = [a for a in anomalies if a["metric"] == "hours_since_update"]
    assert [a["severity"] for a in freshness] == [WARNING]
    assert freshness[0]["baseline"] == FRESHNESS_MIN_HOURS


def test_status_uses_the_learned_cadence(stalled_store):
    connector = StaleConnector(hours=2.5)
    volume = stalled_store.get_latest_profile(TABLE, VOLUME_PROFILE)
    schema = profile_schema(connector, TABLE)

    health = _table_health(connector, stalled_store, TABLE, volume, schema)

    assert health == "warning"
