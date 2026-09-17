"""Orchestration for `scherlok demo`.

Runs the whole Scherlok loop against a throwaway DuckDB file in four acts:
seed, investigate (baseline), bad deploy, watch. Everything lives in one
directory; nothing under ``~/.scherlok`` is read or written. Rendering goes
through the same console helpers as ``scherlok watch`` so the demo looks
exactly like the real thing.
"""

from __future__ import annotations

import importlib.util
import shutil
import tempfile
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from rich.console import Console

from scherlok.alerter.console import print_anomalies
from scherlok.demo.seed import (
    DATABASE_FILENAME,
    DROPPED_COLUMN,
    TABLE_ORDERS,
    TABLE_PRODUCTS,
    TABLE_USERS,
    apply_bad_deploy,
    database_url,
    seed_database,
)
from scherlok.detector.severity import Severity
from scherlok.service import anomaly_to_dict, profile_and_detect
from scherlok.store.sqlite import ProfileStore

PROFILES_FILENAME = "profiles.db"
TEMP_DIR_PREFIX = "scherlok-demo-"

DUCKDB_MISSING_HINT = (
    "The demo needs DuckDB. Install it with: pip install 'scherlok[duckdb]'\n"
    "  (or run it in one go: uvx --from 'scherlok[duckdb]' scherlok demo)"
)

TITLE = "[bold]Scherlok demo[/bold] — a bad deploy, caught before anyone noticed"
STEP_SEED = "[1/4] Seeding a sample warehouse"
STEP_INVESTIGATE = "[2/4] [cyan]scherlok investigate[/cyan] — learning what \"normal\" looks like"
STEP_DEPLOY = "[3/4] [red]02:14 AM — a bad deploy ships[/red]"
STEP_WATCH = "[4/4] [cyan]scherlok watch[/cyan] — comparing against the baseline"
BASELINE_NOTE = (
    "      Baseline saved. Nothing fires on a first run: "
    "there is nothing to compare against yet."
)
DEPLOY_NOTES = (
    f"      · {TABLE_ORDERS}: the loader stopped after 40% of the nightly batch",
    f"      · {TABLE_USERS}: a migration nulled e-mails and wrote free-text plans",
    f"      · {TABLE_PRODUCTS}: someone dropped the `{DROPPED_COLUMN}` column",
)
CI_NOTE = "In CI, [cyan]scherlok ci[/cyan] would exit 1 here and block the deploy."
NEXT_STEPS = (
    "",
    "[bold]Next: point it at your own database[/bold]",
    "  scherlok connect postgres://user:pass@host/db",
    "  scherlok investigate",
    "  scherlok watch",
)
CLEANED_NOTE = "Demo files removed. Re-run with --keep to explore them."
KEPT_NOTE = (
    "Demo files kept in {directory}. Explore them with:\n"
    "  scherlok connect {url} && scherlok status"
)


@dataclass
class DemoResult:
    """What the demo produced, for the CLI (text or JSON) and for tests."""

    directory: Path
    database: Path
    kept: bool
    baseline_rows: dict[str, int]
    anomalies: list[dict] = field(default_factory=list)
    elapsed_seconds: float = 0.0

    def severity_counts(self) -> dict[str, int]:
        counts = {severity.value: 0 for severity in Severity}
        for anomaly in self.anomalies:
            counts[str(anomaly["severity"]).rsplit(".", 1)[-1]] += 1
        return counts

    def to_dict(self) -> dict[str, Any]:
        return {
            "directory": str(self.directory),
            "database": database_url(self.database),
            "kept": self.kept,
            "baseline_rows": dict(self.baseline_rows),
            "anomalies": [anomaly_to_dict(a) for a in self.anomalies],
            "summary": self.severity_counts(),
            "elapsed_seconds": round(self.elapsed_seconds, 2),
        }


def duckdb_available() -> bool:
    return importlib.util.find_spec("duckdb") is not None


def _profile_all(connector, store: ProfileStore) -> tuple[list[dict], dict[str, int]]:
    """Profile every table once; returns (anomalies, row counts)."""
    anomalies: list[dict] = []
    rows: dict[str, int] = {}
    for table in connector.list_tables():
        found, volume = profile_and_detect(connector, store, table)
        anomalies.extend(found)
        rows[table] = int(volume.get("row_count", 0))
    return anomalies, rows


def _print_rows(console: Console, rows: dict[str, int], columns: dict[str, int]) -> None:
    width = max(len(name) for name in rows)
    for table, count in rows.items():
        console.print(
            f"      [green]✓[/green] {table:<{width}}  {count:>7,} rows · {columns[table]} columns"
        )


def run_demo(
    directory: Path | None = None,
    keep: bool = False,
    console: Console | None = None,
) -> DemoResult:
    """Run the four-act demo. A ``console`` renders the narrative; ``None`` is silent.

    ``directory`` defaults to a fresh temporary directory that is removed at the
    end unless ``keep`` is set; passing an explicit directory implies ``keep``.
    """
    from scherlok.connectors.duckdb import DuckDBConnector  # optional dependency

    started = time.perf_counter()
    if directory is None:
        directory = Path(tempfile.mkdtemp(prefix=TEMP_DIR_PREFIX))
    else:
        directory = Path(directory)
        directory.mkdir(parents=True, exist_ok=True)
        keep = True
    database = directory / DATABASE_FILENAME
    say = console.print if console is not None else (lambda *_args, **_kwargs: None)

    say(TITLE)
    say("")
    say(STEP_SEED)
    baseline_rows = seed_database(database)
    total_rows = sum(baseline_rows.values())
    say(f"      {len(baseline_rows)} tables, {total_rows:,} rows (DuckDB, temporary)")

    store = ProfileStore(db_path=directory / PROFILES_FILENAME)
    connector = DuckDBConnector(database_url(database))
    try:
        say(STEP_INVESTIGATE)
        connector.connect()
        _, rows = _profile_all(connector, store)
        if console is not None:
            columns = {t: len(connector.get_columns(t)) for t in rows}
            _print_rows(console, rows, columns)
        say(BASELINE_NOTE)
        connector.close()

        say(STEP_DEPLOY)
        apply_bad_deploy(database)
        for note in DEPLOY_NOTES:
            say(note)

        say(STEP_WATCH)
        connector.connect()
        anomalies, _ = _profile_all(connector, store)
        store.save_anomalies(anomalies)
        connector.close()
    finally:
        store.close()

    result = DemoResult(
        directory=directory,
        database=database,
        kept=keep,
        baseline_rows=baseline_rows,
        anomalies=anomalies,
        elapsed_seconds=time.perf_counter() - started,
    )

    if console is not None:
        print_anomalies(anomalies)
        counts = result.severity_counts()
        say(
            f"{len(anomalies)} anomalies ({counts[Severity.CRITICAL.value]} critical, "
            f"{counts[Severity.WARNING.value]} warning) in {result.elapsed_seconds:.1f}s."
        )
        say(CI_NOTE)
        for line in NEXT_STEPS:
            say(line)
        say("")
        if keep:
            say(KEPT_NOTE.format(directory=directory, url=database_url(database)))
        else:
            say(f"[dim]{CLEANED_NOTE}[/dim]")

    if not keep:
        shutil.rmtree(directory, ignore_errors=True)
    return result
