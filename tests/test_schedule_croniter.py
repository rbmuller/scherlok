"""Integration checks using the real optional croniter dependency."""

from __future__ import annotations

from datetime import datetime, timedelta

import pytest

from scherlok import schedule

croniter_module = pytest.importorskip("croniter")


def test_real_croniter_accepts_five_fields_and_computes_daily_ticks():
    assert schedule._load_croniter("0 6 * * *") is croniter_module.croniter
    with pytest.raises(schedule.ScheduleError, match="five-field"):
        schedule._load_croniter("0 6 * * * *")
    with pytest.raises(schedule.ScheduleError, match="five-field"):
        schedule._load_croniter("61 * * * *")

    before_tick = datetime(2026, 10, 27, 5, 59)
    next_tick = croniter_module.croniter("0 6 * * *", before_tick).get_next(datetime)
    assert next_tick == datetime(2026, 10, 27, 6, 0)


def test_real_croniter_drives_scheduler_without_sleeping(monkeypatch):
    monkeypatch.setattr(schedule.signal, "getsignal", lambda _sig: None)
    monkeypatch.setattr(schedule.signal, "signal", lambda _sig, _handler: None)
    clock = [datetime(2026, 10, 27, 5, 59)]
    waits = []
    results = []

    def wait(seconds):
        waits.append(seconds)
        if len(waits) == 1:
            clock[0] += timedelta(seconds=seconds)
            return False
        return True

    schedule.run_schedule(
        "0 6 * * *", lambda: 1, results.append, now=lambda: clock[0], wait=wait
    )

    assert waits == [60.0, 24 * 60 * 60]
    assert results == [1]
