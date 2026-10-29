"""Optional cron scheduling for recurring watch runs.

The regular watch command remains the source of truth for detection and alerts.
"""

from __future__ import annotations

import signal
import threading
from collections.abc import Callable
from datetime import datetime
from typing import Any


class ScheduleError(ValueError):
    """User-facing error in the optional scheduling configuration."""


def _load_croniter(expression: str) -> Any:
    try:
        from croniter import croniter
    except ImportError as exc:
        raise ScheduleError(
            "Scheduling requires croniter. Install with: pip install 'scherlok[schedule]'"
        ) from exc

    if len(expression.split()) != 5 or not croniter.is_valid(expression):
        raise ScheduleError("Invalid --schedule: expected a valid five-field cron expression")
    return croniter


def run_schedule(
    expression: str,
    run_once: Callable[[], int],
    report_result: Callable[[int], None],
    *,
    now: Callable[[], datetime] | None = None,
    wait: Callable[[float], bool] | None = None,
) -> None:
    """Run on future cron ticks; let a running tick finish before shutdown.

    `now` and `wait` are injected for deterministic tests. Cron is interpreted
    in the machine's local time. Missed ticks are skipped. The callback returns
    the normal watch exit status, which is reported without terminating the loop.
    """
    croniter = _load_croniter(expression)
    current_time = now or datetime.now
    stop = threading.Event()
    wait_for_stop = wait or stop.wait
    old_handlers: dict[signal.Signals, Any] = {}

    def request_stop(_signum: int, _frame: Any) -> None:
        stop.set()

    try:
        for sig in (signal.SIGINT, signal.SIGTERM):
            old_handlers[sig] = signal.getsignal(sig)
            signal.signal(sig, request_stop)

        while not stop.is_set():
            # Recalculate from now after every run: do not replay missed ticks.
            scheduled_from = current_time()
            next_tick = croniter(expression, scheduled_from).get_next(datetime)
            seconds = max(0.0, (next_tick - scheduled_from).total_seconds())
            if wait_for_stop(seconds) or stop.is_set():
                break
            result = run_once()
            report_result(result)
    finally:
        for sig, handler in old_handlers.items():
            signal.signal(sig, handler)
