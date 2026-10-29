"""Scheduled watch regressions: no real cron sleeps or database connections."""

from __future__ import annotations

import signal
import sys
from datetime import datetime, timedelta
from types import ModuleType
from unittest.mock import patch

import pytest
from typer.testing import CliRunner

from scherlok import schedule
from scherlok.cli import app


class FakeCroniter:
    def __init__(self, expression, now):
        self.when = now

    @staticmethod
    def is_valid(value):
        return value == "*/5 * * * *"

    def get_next(self, cls):
        return self.when + timedelta(minutes=5)


@pytest.fixture(autouse=True)
def fake_croniter(monkeypatch):
    module = ModuleType("croniter")
    module.croniter = FakeCroniter
    monkeypatch.setitem(sys.modules, "croniter", module)


def test_invalid_expression_rejected_before_work():
    with pytest.raises(schedule.ScheduleError, match="five-field"):
        schedule.run_schedule("* * *", lambda: 0, lambda _: None)


def test_missing_dependency_gives_install_hint(monkeypatch):
    monkeypatch.setitem(sys.modules, "croniter", None)
    with pytest.raises(schedule.ScheduleError, match=r"scherlok\[schedule\]"):
        schedule.run_schedule("*/5 * * * *", lambda: 0, lambda _: None)


def test_successive_ticks_and_exit_code_reporting(monkeypatch):
    captured = {}
    monkeypatch.setattr(schedule.signal, "getsignal", lambda sig: f"old-{sig}")
    monkeypatch.setattr(
        schedule.signal, "signal", lambda sig, handler: captured.update({sig: handler})
    )
    clock = [datetime(2026, 10, 27, 9, 0)]
    intervals = []
    exits = []

    def wait(seconds):
        intervals.append(seconds)
        if len(intervals) > 2:
            return True
        clock[0] += timedelta(seconds=seconds)
        return False

    schedule.run_schedule("*/5 * * * *", lambda: 1, exits.append, now=lambda: clock[0], wait=wait)
    assert intervals == [300.0, 300.0, 300.0]
    assert exits == [1, 1]  # detection severity does not stop the daemon
    assert captured[signal.SIGINT] == f"old-{signal.SIGINT}"
    assert captured[signal.SIGTERM] == f"old-{signal.SIGTERM}"


def test_shutdown_during_run_finishes_current_tick(monkeypatch):
    captured = {}
    monkeypatch.setattr(schedule.signal, "getsignal", lambda sig: f"old-{sig}")
    monkeypatch.setattr(
        schedule.signal, "signal", lambda sig, handler: captured.update({sig: handler})
    )
    clock = [datetime(2026, 10, 27, 9, 0)]
    runs = []
    reports = []

    def wait(seconds):
        clock[0] += timedelta(seconds=seconds)
        return False

    def run_once():
        runs.append(1)
        captured[signal.SIGTERM](signal.SIGTERM, None)
        return 0

    schedule.run_schedule("*/5 * * * *", run_once, reports.append, now=lambda: clock[0], wait=wait)
    assert runs == [1]
    assert reports == [0]
    assert captured[signal.SIGTERM] == f"old-{signal.SIGTERM}"


def test_skips_missed_intervals_after_long_run(monkeypatch):
    monkeypatch.setattr(schedule.signal, "getsignal", lambda sig: None)
    monkeypatch.setattr(schedule.signal, "signal", lambda sig, handler: None)
    clock = [datetime(2026, 10, 27, 9, 0)]
    intervals = []

    def wait(seconds):
        intervals.append(seconds)
        if len(intervals) == 2:
            return True
        clock[0] += timedelta(seconds=seconds)
        return False

    def run_once():
        clock[0] += timedelta(hours=2)
        return 0

    schedule.run_schedule("*/5 * * * *", run_once, lambda _: None, now=lambda: clock[0], wait=wait)
    assert intervals == [300.0, 300.0]


def test_cli_without_schedule_retains_single_run():
    with patch("scherlok.cli._run_watch", return_value=[]) as one_watch:
        result = CliRunner().invoke(app, ["watch"])
    assert result.exit_code == 0
    one_watch.assert_called_once_with(webhook=None, emails=None, explain=False)


def test_cli_scheduled_ticks_reuse_run_watch():
    def fake_schedule(expression, on_tick, log_result):
        assert expression == "*/5 * * * *"
        log_result(on_tick())

    with (
        patch("scherlok.schedule.run_schedule", side_effect=fake_schedule) as scheduler,
        patch("scherlok.cli._run_watch", return_value=[]) as one_watch,
    ):
        result = CliRunner().invoke(app, ["watch", "--schedule", "*/5 * * * *", "--explain"])
    assert result.exit_code == 0
    scheduler.assert_called_once()
    one_watch.assert_called_once_with(webhook=None, emails=None, explain=True)


def test_cli_missing_croniter_exits_with_hint(monkeypatch):
    monkeypatch.setitem(sys.modules, "croniter", None)
    result = CliRunner().invoke(app, ["watch", "--schedule", "*/5 * * * *"])
    assert result.exit_code == 1
    assert "pip install 'scherlok[schedule]'" in result.output


def test_cli_scheduled_connection_exit_is_reported_without_stopping():
    def fake_schedule(expression, on_tick, log_result):
        assert expression == "*/5 * * * *"
        log_result(on_tick())

    with (
        patch("scherlok.schedule.run_schedule", side_effect=fake_schedule),
        patch("scherlok.cli._run_watch", side_effect=__import__("typer").Exit(code=1)),
    ):
        result = CliRunner().invoke(app, ["watch", "--schedule", "*/5 * * * *"])
    assert result.exit_code == 0
    assert "exit code 1" in result.output


def test_cli_scheduled_unexpected_error_is_reported_and_next_tick_can_run():
    def fake_schedule(expression, on_tick, log_result):
        assert expression == "*/5 * * * *"
        log_result(on_tick())
        log_result(on_tick())

    with (
        patch("scherlok.schedule.run_schedule", side_effect=fake_schedule),
        patch(
            "scherlok.cli._run_watch",
            side_effect=[RuntimeError("database unavailable"), []],
        ) as one_watch,
    ):
        result = CliRunner().invoke(app, ["watch", "--schedule", "*/5 * * * *"])

    assert result.exit_code == 0
    assert one_watch.call_count == 2
    assert "RuntimeError" in result.output
    assert result.output.count("exit code 1") == 1
    assert result.output.count("exit code 0") == 1
