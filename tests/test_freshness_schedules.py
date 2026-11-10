"""Freshness on load schedules that pause: a pause the schedule always has is no stall.

Each scenario runs the detector the way consecutive hourly `watch` runs do,
each run compared with the reading saved before it. A first attempt at #149
learned an expected age from the latest loads and read every night of a
business-hours pipeline as a stall, about nine alerts a night; it was
reverted. Whatever replaces it must keep these schedules quiet.
"""

from __future__ import annotations

from collections.abc import Iterable
from datetime import datetime, timedelta, timezone

import pytest

from scherlok.detector.freshness import detect_freshness_anomalies
from scherlok.profiler.freshness import (
    FRESHNESS_COLUMN,
    FRESHNESS_SOURCE,
    HOURS_SINCE_UPDATE,
    LAST_MODIFIED,
    SOURCE_METADATA,
)

TABLE = "orders"
MONDAY = datetime(2026, 11, 2, tzinfo=timezone.utc)
DAYS = 14
DAYS_PER_WEEK = 7
WEEKDAYS = 5
BUSINESS_HOURS = range(8, 21)  # loads at 08:00 through 20:00
WATCH_MINUTE = 30
SECONDS_PER_HOUR = 3600


def _hourly_loads(days: Iterable[int], hours: Iterable[int]) -> list[datetime]:
    return [MONDAY + timedelta(days=day, hours=hour) for day in days for hour in hours]


def _hourly_watches() -> list[datetime]:
    return [
        MONDAY + timedelta(hours=hour, minutes=WATCH_MINUTE) for hour in range(DAYS * 24)
    ]


def _alerts(loads: list[datetime], watches: list[datetime]) -> list[dict]:
    """Every freshness anomaly over ``watches``, each run against the previous reading."""
    anomalies: list[dict] = []
    previous: dict | None = None
    for now in watches:
        last_load = max((load for load in loads if load <= now), default=None)
        if last_load is None:
            continue
        reading = {
            LAST_MODIFIED: last_load.isoformat(),
            HOURS_SINCE_UPDATE: round((now - last_load).total_seconds() / SECONDS_PER_HOUR, 2),
            FRESHNESS_SOURCE: SOURCE_METADATA,
            FRESHNESS_COLUMN: None,
        }
        if previous is not None:
            anomalies.extend(detect_freshness_anomalies(TABLE, reading, previous))
        previous = reading
    return anomalies


@pytest.mark.parametrize(
    ("days", "hours"),
    [
        pytest.param(range(DAYS), range(24), id="hourly-around-the-clock"),
        pytest.param(range(DAYS), BUSINESS_HOURS, id="hourly-business-hours"),
        pytest.param(
            [day for day in range(DAYS) if day % DAYS_PER_WEEK < WEEKDAYS],
            BUSINESS_HOURS,
            id="hourly-business-hours-on-weekdays",
        ),
    ],
)
def test_a_pause_the_schedule_always_has_raises_nothing(days, hours):
    loads = _hourly_loads(days, hours)

    assert _alerts(loads, _hourly_watches()) == []
