"""CLI entry point for Scherlok. Built with Typer and Rich."""

from __future__ import annotations

import json
import shutil
import subprocess
import sys
import time
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import TYPE_CHECKING

import typer
from rich.console import Console
from rich.markup import escape as rich_escape
from rich.table import Table

from scherlok import __version__
from scherlok.alerter.console import print_anomalies, print_explanation, print_profile_summary
from scherlok.alerter.email import send_email_alert
from scherlok.alerter.exitcode import (
    FAIL_ON_CHOICES,
    FAIL_ON_CRITICAL,
    exit_code_for,
    fails_gate,
    normalize_fail_on,
)
from scherlok.alerter.webhook import send_webhook
from scherlok.approvals import (
    ANOMALIES_KEY,
    COLUMN_COUNT_KEY,
    ROW_COUNT_KEY,
    ProposedBaseline,
    accept_baseline,
    approver,
    propose_baseline,
)
from scherlok.config import BASELINE_LOCKED_SETTING, ENV_BASELINE_LOCKED, ScherlokConfig
from scherlok.connectors import get_connector
from scherlok.connectors.base import UNKNOWN_NAMESPACE, RelationNamespace
from scherlok.dbt.manifest import DEFAULT_TARGET_PATH
from scherlok.dbt.relations import (
    VisibleTables,
    ambiguous_identifiers,
    manifest_relations,
    resolve_relation,
)
from scherlok.demo import DUCKDB_MISSING_HINT, duckdb_available, run_demo
from scherlok.detector.anomaly import detect_volume_anomalies
from scherlok.detector.fields import anomaly_details
from scherlok.detector.freshness import detect_freshness_anomalies
from scherlok.detector.schema_drift import detect_schema_drift
from scherlok.detector.severity import Severity
from scherlok.explainer import (
    HISTORY_LOOKBACK_DAYS,
    describe_error,
    format_unavailable_note,
)
from scherlok.output import error as out_error
from scherlok.output import get_console, is_quiet, is_verbose, verbose_info
from scherlok.output import info as out_info
from scherlok.profiler.distribution import profile_distribution
from scherlok.profiler.freshness import profile_freshness
from scherlok.profiler.schema import profile_schema
from scherlok.profiler.volume import profile_volume
from scherlok.reconcile import (
    ReconcileError,
    ReconcileResult,
    format_total,
    reconcile_tables,
    validate_reconcile_args,
)
from scherlok.runs import (
    COMMAND_CHECK,
    COMMAND_CI,
    COMMAND_VERIFY,
    COMMAND_WATCH,
    RunRecord,
    append_run_record,
)
from scherlok.service import (
    AGAINST_EXAMPLE,
    AgainstScopeError,
    UnknownTablesError,
    VerificationResult,
    against_scope,
    blocks_baseline,
    connection_scope,
    normalize_ignore,
    open_baseline_store,
    profile_and_detect,
    redact_connection,
    select_tables,
    severity_counts,
    verify_tables,
)
from scherlok.store.remote import sync_context
from scherlok.store.sqlite import (
    APPROVAL_AFTER_KEY,
    APPROVAL_BEFORE_KEY,
    RUN_PASSED_KEY,
    RUN_TABLES_KEY,
    ProfileStore,
)

OUTPUT_TEXT = "text"
OUTPUT_JSON = "json"
OUTPUT_FORMATS = (OUTPUT_TEXT, OUTPUT_JSON)

# `verify --dbt-project --output json`: built models the connection can't see.
DBT_MODELS_NOT_FOUND_KEY = "dbt_models_not_found"


if TYPE_CHECKING:
    from scherlok.dbt.manifest import DbtExposure, DbtNode


app = typer.Typer(
    name="scherlok",
    help="A detective for your data. Zero-config data quality monitoring.",
    add_completion=False,
)
console = Console()

EXPLAIN_FLAG_HELP = (
    "Augment fired alerts with a Claude-generated root-cause hypothesis. "
    "Opt-in; requires ANTHROPIC_API_KEY and `pip install 'scherlok[explain]'`. "
    "Sends aggregate anomaly data only — never warehouse rows."
)


@app.callback()
def _root_callback(
    verbose: bool = typer.Option(
        False, "--verbose", "-v",
        help="Show detailed progress (per-table timings, query previews).",
    ),
    quiet: bool = typer.Option(
        False, "--quiet", "-q",
        help="Suppress non-essential output. Errors and exit codes still surface.",
    ),
) -> None:
    """Root callback: wire global verbosity flags into scherlok.output."""
    from scherlok.output import set_verbosity
    set_verbosity(verbose=verbose, quiet=quiet)


def _get_connector_or_exit() -> object:
    """Load config and return a connected connector, or exit with error.

    Reads the connection from SCHERLOK_CONNECTION or the saved config and
    never writes it back.
    """
    cfg = ScherlokConfig.load()
    conn_str = cfg.get_connection_string()
    if not conn_str:
        # out_error, not console.print: under --output json this must reach
        # stderr, never the stdout reserved for the payload.
        out_error(
            "[red]No connection configured.[/red] "
            "Run [bold]scherlok connect <connection_string>[/bold] first."
        )
        raise typer.Exit(code=1)
    connector = get_connector(conn_str)
    if not connector.connect():
        _print_connect_failure(connector)
        raise typer.Exit(code=1)
    return connector


def _open_store(connection_string: str | None) -> ProfileStore:
    """Open the local profile store for the database a connection points at.

    Baselines are kept per database, so the same table name on two
    connections never shares one; see `service.connection_scope`.
    """
    return ProfileStore(scope=connection_scope(connection_string))


def _fail_on_or_exit(fail_on: str) -> str:
    """Normalize --fail-on, or exit 1 before any work: a typo must not loosen a gate."""
    try:
        return normalize_fail_on(fail_on)
    except ValueError as exc:
        choices = " or ".join(f"'{choice}'" for choice in FAIL_ON_CHOICES)
        out_error(f"[red]Invalid --fail-on value '{rich_escape(fail_on)}'. Use {choices}.[/red]")
        raise typer.Exit(code=1) from exc


def _against_or_exit(against: str | None) -> str | None:
    """The scope --against names, or exit 1 before any work.

    The value is never printed: it may be a connection string with a password.
    """
    if against is None:
        return None
    try:
        return against_scope(against)
    except ValueError as exc:
        out_error(f"[red]Invalid --against value. {rich_escape(str(exc))}[/red]")
        raise typer.Exit(code=1) from exc


def _ignore_or_exit(ignore: list[str] | None) -> list[str]:
    """The checks --ignore names, normalized, or exit 1 before any work."""
    try:
        return normalize_ignore(ignore)
    except ValueError as exc:
        out_error(f"[red]Invalid --ignore value. {rich_escape(str(exc))}[/red]")
        raise typer.Exit(code=1) from exc


def _print_connect_failure(connector: object) -> None:
    """Render a 'failed to connect' header plus connector.last_error if available."""
    out_error("[red]Failed to connect to the database.[/red]")
    err = getattr(connector, "last_error", None)
    if err:
        out_error(f"  [dim]{err}[/dim]")


CONNECT_EXAMPLES: list[tuple[str, str]] = [
    ("postgres", "postgresql://user:pass@localhost:5432/mydb"),
    ("bigquery", "bigquery://my-gcp-project/my_dataset"),
    ("snowflake", "snowflake://my-account/my_database/PUBLIC"),
]


def _print_connect_examples() -> None:
    """Print one example connection string per supported adapter."""
    console.print("[bold]Usage:[/bold] scherlok connect <connection_string>")
    console.print()
    console.print("Examples for each supported adapter:")
    for scheme, example in CONNECT_EXAMPLES:
        console.print(f"  [cyan]{scheme:<10}[/cyan] [dim]{example}[/dim]")
    console.print()
    console.print(
        "Re-run with one of these as the argument to validate the connection "
        "and save it to config."
    )


@app.command()
def connect(
    connection_string: str = typer.Argument(
        None, help="Database connection string (e.g. postgresql://user:pass@host/db)"
    ),
) -> None:
    """Validate a database connection and save it to config.

    Run with no argument to print example connection strings for each
    supported adapter.

    Example:
        scherlok connect postgresql://user:pass@localhost:5432/mydb
    """
    if not connection_string:
        _print_connect_examples()
        raise typer.Exit(code=0)

    connector = get_connector(connection_string)
    with console.status("Connecting..."):
        ok = connector.connect()

    if ok:
        cfg = ScherlokConfig.load()
        cfg.connection_string = connection_string
        cfg.save()
        tables = connector.list_tables()
        console.print(f"[green]Connected successfully.[/green] Found {len(tables)} tables.")
    else:
        _print_connect_failure(connector)
        raise typer.Exit(code=1)


@app.command()
def config(
    store: str = typer.Option(
        None, "--store", help="Remote store URL (s3://, gs://, az://)"
    ),
    baseline_locked: bool | None = typer.Option(
        None, "--baseline-locked/--no-baseline-locked",
        help="Locked: only `scherlok accept` writes baselines; watch, ci, check, dbt "
        "and the MCP tools compare without saving profiles.",
    ),
    output: str = typer.Option(
        OUTPUT_TEXT, "--output",
        help="Output format: 'text' (default) or 'json' (one object on stdout, "
        "never the connection string)",
    ),
) -> None:
    """Configure Scherlok settings.

    With no option, show them. `--output json` prints whether a connection is
    configured and where it comes from, never the connection string itself,
    so a script or an agent hook can check the setup without reading
    ~/.scherlok/config.json.

    Example:
        scherlok config --store s3://my-bucket/scherlok/profiles.db
        scherlok config --store gs://my-bucket/scherlok/profiles.db
        scherlok config --store az://my-container/scherlok/profiles.db
        scherlok config --baseline-locked
        scherlok config --output json
    """
    output_lower = output.lower()
    if output_lower not in OUTPUT_FORMATS:
        out_error(
            f"[red]Invalid --output value '{rich_escape(output)}'. Use 'text' or 'json'.[/red]"
        )
        raise typer.Exit(code=1)
    cfg = ScherlokConfig.load()
    if output_lower == OUTPUT_JSON:
        if store is not None or baseline_locked is not None:
            out_error("[red]--output json only shows the settings; it can't change them.[/red]")
            raise typer.Exit(code=1)
        print(json.dumps(_config_summary(cfg)))
        return
    if store is None and baseline_locked is None:
        current = cfg.settings.get("store") or "local (~/.scherlok/)"
        console.print(f"Current store: [cyan]{current}[/cyan]")
        locked = "yes" if cfg.baseline_locked() else "no"
        console.print(f"Baselines locked: [cyan]{locked}[/cyan]")
        return

    if store is not None:
        cfg.settings["store"] = store
    if baseline_locked is not None:
        cfg.settings[BASELINE_LOCKED_SETTING] = baseline_locked
    cfg.save()
    if store is not None:
        console.print(f"[green]Store set to:[/green] {store}")
    if baseline_locked:
        console.print(
            "[green]Baselines locked:[/green] only [bold]scherlok accept[/bold] writes them."
        )
    elif baseline_locked is not None:
        console.print("[green]Baselines unlocked:[/green] runs learn again.")
        if cfg.baseline_locked():
            console.print(
                f"[yellow]{ENV_BASELINE_LOCKED} still locks them in this environment.[/yellow]"
            )


def _config_summary(cfg: ScherlokConfig) -> dict:
    """`config --output json`: what is set up, never the connection string itself."""
    source = cfg.connection_source()
    store = cfg.get_store()
    return {
        "connection_configured": source is not None,
        "connection_source": source,
        "store": redact_connection(store) if store else None,
        BASELINE_LOCKED_SETTING: cfg.baseline_locked(),
    }


def _exit_if_baseline_locked(cfg: ScherlokConfig) -> None:
    """Refuse a command whose only job is writing baselines while they are locked."""
    if cfg.baseline_locked():
        out_error(
            "[red]Baselines are locked: only [bold]scherlok accept --table NAME "
            '--reason "..."[/bold] writes them.[/red]'
        )
        raise typer.Exit(code=1)


@app.command()
def investigate() -> None:
    """Profile all tables and store results.

    Overwrites every table's baseline, without asking or recording an
    approval; `scherlok accept` does both, for the tables you name. Refused
    while baselines are locked.

    Example:
        scherlok investigate
    """
    _exit_if_baseline_locked(ScherlokConfig.load())
    connector = _get_connector_or_exit()
    cfg = ScherlokConfig.load()

    from scherlok.config import PROFILES_DB
    with sync_context(cfg.get_store(), PROFILES_DB):
        store = _open_store(connector.connection_string)
        tables = connector.list_tables()

        if not tables:
            out_info("[yellow]No tables found.[/yellow]")
            raise typer.Exit(code=0)

        out_info(f"Investigating [bold]{len(tables)}[/bold] tables...")

        for table in tables:
            t_start = time.perf_counter()
            out_info(f"  Profiling [cyan]{table}[/cyan]...")

            vol = profile_volume(connector, table)
            store.save_profile(table, "volume", vol)

            sch = profile_schema(connector, table)
            store.save_profile(table, "schema", sch)

            fresh = profile_freshness(connector, table, sch.get("columns"))
            store.save_profile(table, "freshness", fresh)

            for col in sch.get("columns", []):
                dist = profile_distribution(connector, table, col["name"])
                store.save_profile(table, f"distribution:{col['name']}", dist)

            verbose_info(
                f"{table}: {vol.get('row_count', 0)} rows · "
                f"{len(sch.get('columns', []))} cols · "
                f"{time.perf_counter() - t_start:.2f}s"
            )

        out_info(
            f"[green]Investigation complete.[/green] {len(tables)} tables profiled."
        )


def _watch_table(
    connector: object, store: ProfileStore, table: str, *, persist: bool = True
) -> tuple[list[dict], dict]:
    """Profile one table + detect anomalies against stored baseline. Saves new profiles,
    unless a CRITICAL anomaly keeps the baseline (#116) or `persist` is False
    (baselines locked, #109).

    Thin CLI-facing alias over `service.profile_and_detect` — kept so existing
    call sites and tests that patch `scherlok.cli._watch_table` stay valid.
    Returns (anomalies, current_volume).
    """
    return profile_and_detect(connector, store, table, persist=persist)


# Table names a one-line summary lists before it cuts the list short.
PREVIEW_TABLES = 5


def _table_preview(tables: list[str]) -> str:
    """The first few table names, escaped for Rich, with an ellipsis when cut."""
    preview = ", ".join(rich_escape(t) for t in tables[:PREVIEW_TABLES])
    return f"{preview} …" if len(tables) > PREVIEW_TABLES else preview


def _print_kept_baselines(tables: list[str], locked: bool = False) -> None:
    """Name the tables whose reading was not learned because of a CRITICAL anomaly (#116).

    With baselines locked (#109) nothing was learned, which only --verbose says.
    """
    if locked:
        verbose_info(f"Baselines are locked ({ENV_BASELINE_LOCKED} or config): nothing learned.")
    if not tables:
        return
    out_info(
        f"[yellow]Baseline kept, CRITICAL reading not learned ({len(tables)}): "
        f"{_table_preview(tables)}. The alert repeats on every run until the data "
        "recovers; if the change is intended, [bold]scherlok accept --table NAME "
        '--reason "..."[/bold] makes it the new normal.[/yellow]'
    )


def _dispatch_alerts(
    anomalies: list[dict],
    store: ProfileStore,
    webhook: str | None,
    emails: list[str] | None,
    *,
    explain: bool = False,
    explain_context: dict | None = None,
    run_id: str | None = None,
) -> tuple[dict | None, str | None]:
    """Persist anomalies, tagged with `run_id`, and fan out to webhook/email alerters.

    With `explain`, one Claude call augments the whole batch with a
    root-cause hypothesis that rides along on every alert channel; when it
    fails, the unaugmented alert goes out with a one-line note instead.
    Returns (explanation, explain_error) so callers with their own output
    sink (e.g. `dbt --output json`) can embed the result.
    """
    if not anomalies:
        out_info("[green]No anomalies detected.[/green]")
        return None, None
    explanation: dict | None = None
    explain_error: str | None = None
    if explain:
        # Must run before save_anomalies so the bundle's recent-history
        # window contains only prior runs, not this very batch.
        explanation, explain_error = _explain_or_note(anomalies, store, explain_context)
    store.save_anomalies(anomalies, run_id=run_id)
    if not is_quiet():
        print_anomalies(anomalies)
        if explanation:
            print_explanation(explanation)
    if explain_error:
        # rich_escape: error text may carry bracketed sequences that Rich
        # would swallow as markup tags (or crash on) — e.g. 'scherlok[explain]'.
        out_error(f"[yellow]{rich_escape(format_unavailable_note(explain_error))}[/yellow]")
    if webhook:
        ok = send_webhook(webhook, anomalies, explanation=explanation, explain_note=explain_error)
        if ok:
            verbose_info("Webhook delivered.")
        else:
            out_error("[red]Webhook delivery failed.[/red]")
    if emails:
        ok = send_email_alert(
            emails, anomalies, explanation=explanation, explain_note=explain_error
        )
        if ok:
            verbose_info("Email delivered.")
        else:
            out_error("[red]Email delivery failed.[/red]")
    return explanation, explain_error


def _explain_or_note(
    anomalies: list[dict],
    store: ProfileStore,
    context: dict | None,
) -> tuple[dict | None, str | None]:
    """Run the one-call explainer for this batch.

    Returns (explanation, None) on success or (None, error_note) on failure.
    Never raises — --explain is fail-open by contract and must not block
    the original alert.
    """
    from scherlok.explainer import ExplainUnavailableError, build_bundle, explain_anomalies

    context = context or {}
    try:
        history = store.get_anomaly_history(days=HISTORY_LOOKBACK_DAYS)
    except Exception as exc:  # fail-open: a history read must not block alerting
        verbose_info(f"--explain: anomaly history unavailable ({rich_escape(str(exc))})")
        history = []
    try:
        tables = {a.get("table") for a in anomalies}
        bundle = build_bundle(
            anomalies,
            recent_history=[h for h in history if h.get("table") in tables],
            lineage=context.get("lineage"),
            meta=context.get("meta"),
        )
        return explain_anomalies(bundle), None
    except ExplainUnavailableError as exc:
        return None, str(exc)
    except Exception as exc:  # safety net — --explain must never block the alert
        return None, describe_error(exc)


def _explain_context(
    adapter: str | None = None,
    fail_on: str | None = None,
    lineage: dict[str, list[str]] | None = None,
) -> dict:
    """Assemble the run-level context forwarded to the explainer bundle."""
    meta: dict = {"run_at": datetime.now(timezone.utc).isoformat(timespec="seconds")}
    if adapter:
        meta["adapter"] = adapter
    if fail_on:
        meta["fail_on"] = fail_on
    context: dict = {"meta": meta}
    if lineage:
        context["lineage"] = lineage
    return context


def _run_watch(
    webhook: str | None = None,
    emails: list[str] | None = None,
    tables: list[str] | None = None,
    connector: object | None = None,
    explain: bool = False,
    fail_on: str | None = None,
    command: str = COMMAND_WATCH,
) -> list[dict]:
    """Run the watch logic: profile + detect + alert. Returns all anomalies.

    Reused by `watch`, `ci` and `check`, recorded as a run under `command`,
    passing or not. When `tables` is None, all tables visible to the
    connector are used. When `connector` is None, one is loaded from the
    saved config. `fail_on` feeds the --explain context and the run record
    (CRITICAL when None, as `watch` exits); exit-code policy stays with the
    callers. With baselines locked (#109) no profile is saved; anomalies and
    the run record still are.
    """
    if connector is None:
        connector = _get_connector_or_exit()
    cfg = ScherlokConfig.load()

    from scherlok.config import PROFILES_DB
    with sync_context(cfg.get_store(), PROFILES_DB):
        store = _open_store(connector.connection_string)
        run = RunRecord(
            command=command, scope=store.scope, fail_on=fail_on or FAIL_ON_CRITICAL
        )
        if tables is None:
            tables = connector.list_tables()
        all_anomalies: list[dict] = []
        kept_baselines: list[str] = []
        locked = cfg.baseline_locked()

        out_info(f"Watching [bold]{len(tables)}[/bold] tables...")

        for table in tables:
            anomalies, _ = _watch_table(connector, store, table, persist=not locked)
            all_anomalies.extend(anomalies)
            if blocks_baseline(anomalies):
                kept_baselines.append(table)

        _dispatch_alerts(
            all_anomalies,
            store,
            webhook,
            emails,
            explain=explain,
            explain_context=_explain_context(
                adapter=_adapter_from_connection(cfg.get_connection_string()),
                fail_on=fail_on,
            ),
            run_id=run.run_id,
        )
        _print_kept_baselines(kept_baselines, locked)
        store.save_run(run.finish(tables, all_anomalies).to_dict())

    return all_anomalies


def _adapter_from_connection(conn_str: str | None) -> str | None:
    """Extract the adapter scheme ('postgresql', 'bigquery', …) from a URL."""
    if not conn_str or "://" not in conn_str:
        return None
    return conn_str.split("://", 1)[0]


@app.command()
def watch(
    webhook: str = typer.Option(
        None, "--webhook", "-w",
        help="Webhook URL to send alerts (auto-detects Slack, Discord, Teams).",
    ),
    email: list[str] = typer.Option(
        None, "--email", "-e",
        help="Email recipient(s) for alerts. Repeat to send to multiple.",
    ),
    explain: bool = typer.Option(False, "--explain", help=EXPLAIN_FLAG_HELP),
    schedule: str | None = typer.Option(
        None, "--schedule", help="Repeat on a five-field cron schedule (local time).",
    ),
) -> None:
    """Compare current state vs stored profile. Detect anomalies.

    Example:
        scherlok watch
        scherlok watch --webhook https://hooks.slack.com/...
        scherlok watch --email alice@company.com --email bob@company.com
        scherlok watch --webhook https://hooks.slack.com/... --explain
        scherlok watch --schedule "0 6 * * *"
    """
    if schedule is None:
        anomalies = _run_watch(webhook=webhook, emails=email or None, explain=explain)
        raise typer.Exit(code=exit_code_for(anomalies))

    from scherlok.schedule import ScheduleError, run_schedule

    def run_once() -> int:
        try:
            anomalies = _run_watch(webhook=webhook, emails=email or None, explain=explain)
            return exit_code_for(anomalies)
        except typer.Exit as exc:
            # A failed connection should be logged as a run failure, rather than
            # silently stopping all subsequent scheduled checks.
            return int(exc.exit_code)
        except Exception as exc:
            typer.echo(
                f"Scheduled watch raised {type(exc).__name__}; continuing at the next tick.",
                err=True,
            )
            return 1

    try:
        run_schedule(
            schedule,
            run_once,
            lambda code: typer.echo(f"Scheduled watch completed (exit code {code})."),
        )
    except ScheduleError as exc:
        typer.echo(str(exc), err=True)
        raise typer.Exit(code=1) from exc


def _run_ci(
    connection_string: str,
    webhook: str | None,
    email: list[str] | None,
    fail_on: str,
    explain: bool = False,
    command: str = COMMAND_CI,
) -> None:
    fail_on = _fail_on_or_exit(fail_on)

    # Save connection
    cfg = ScherlokConfig.load()
    cfg.connection_string = connection_string
    cfg.save()

    # Validate connection
    connector = get_connector(connection_string)
    if not connector.connect():
        _print_connect_failure(connector)
        raise typer.Exit(code=1)

    # Run watch (does profile + detect + alert in one) on the database given
    # here, not on SCHERLOK_CONNECTION, which would otherwise take precedence.
    anomalies = _run_watch(
        webhook=webhook,
        emails=email or None,
        connector=connector,
        explain=explain,
        fail_on=fail_on,
        command=command,
    )

    if fails_gate(anomalies, fail_on):
        raise typer.Exit(code=1)


@app.command()
def ci(
    connection_string: str = typer.Argument(
        ..., help="Database connection string"
    ),
    webhook: str = typer.Option(
        None, "--webhook", "-w", help="Webhook URL for alerts"
    ),
    email: list[str] = typer.Option(
        None, "--email", "-e", help="Email recipient(s) for alerts"
    ),
    fail_on: str = typer.Option(
        "critical", "--fail-on",
        help="Severity that triggers exit code 1: 'critical' (default) or 'warning'",
    ),
    explain: bool = typer.Option(False, "--explain", help=EXPLAIN_FLAG_HELP),
) -> None:
    """All-in-one CI/CD command: connect + investigate + watch + exit code.

    Designed for use in pipelines. On its first run a table has no baseline, so
    it only gets the new-table checks (WARNING, or INFO for a constant column),
    then later runs detect against the stored baseline (use remote storage).

    Example:
        # GitHub Actions
        - run: |
            pip install scherlok
            scherlok config --store s3://my-bucket/scherlok/profiles.db
            scherlok ci $DATABASE_URL --webhook $SLACK_URL --fail-on critical
    """
    _run_ci(connection_string, webhook, email, fail_on, explain)


@app.command()
def check(
    connection_string: str = typer.Argument(
        ..., help="Database connection string"
    ),
    webhook: str = typer.Option(
        None, "--webhook", "-w", help="Webhook URL for alerts"
    ),
    email: list[str] = typer.Option(
        None, "--email", "-e", help="Email recipient(s) for alerts"
    ),
    fail_on: str = typer.Option(
        "critical", "--fail-on",
        help="Severity that triggers exit code 1: 'critical' (default) or 'warning'",
    ),
    explain: bool = typer.Option(False, "--explain", help=EXPLAIN_FLAG_HELP),
) -> None:
    """Assertion-only CI alias for `scherlok ci --fail-on critical`."""
    _run_ci(connection_string, webhook, email, fail_on, explain, command=COMMAND_CHECK)


@dataclass
class DbtScope:
    """The models a dbt run built, for `verify --dbt-project`.

    `resolve` maps them to the tables the connection can see, the way
    `dbt-run-and-watch` does; a model it can't find lands in `not_found`.
    """

    project_dir: Path
    models: list[DbtNode]
    ambiguous: set[str] = field(default_factory=set)
    tables: list[str] = field(default_factory=list)
    not_found: list[str] = field(default_factory=list)

    def resolve(self, visible: set[str], namespace: RelationNamespace) -> None:
        tables = VisibleTables(visible)
        for node in self.models:
            physical = resolve_relation(node, tables, namespace, self.ambiguous)
            if physical is None:
                self.not_found.append(node.name)
            else:
                self.tables.append(physical)


def _dbt_scope_or_exit(project_dir: Path, target_path: str) -> DbtScope:
    """Read the models that succeeded in the project's last dbt run, or exit 1."""
    from scherlok.dbt import built_models, load_manifest

    try:
        manifest = load_manifest(project_dir, target_path=target_path)
        models = built_models(project_dir, target_path=target_path, manifest=manifest)
    except (OSError, ValueError) as exc:
        out_error(f"[red]{rich_escape(str(exc))}[/red]")
        raise typer.Exit(code=1) from exc
    return DbtScope(
        project_dir=project_dir,
        models=models,
        ambiguous=ambiguous_identifiers(manifest_relations(manifest)),
    )


def _print_dbt_scope(scope: DbtScope) -> None:
    """Say which dbt models `verify --dbt-project` could not check."""
    if not scope.models:
        project = rich_escape(str(scope.project_dir))
        out_info(
            f"[yellow]No model succeeded in the last dbt run in {project}; "
            "nothing to verify from it.[/yellow]"
        )
    if scope.not_found:
        out_info(
            f"[yellow]dbt models not on this connection, not verified ({len(scope.not_found)}): "
            f"{_table_preview(scope.not_found)}.[/yellow]"
        )


def _run_verify(
    tables: list[str] | None,
    fail_on: str,
    record_path: Path | None = None,
    dbt_scope: DbtScope | None = None,
    *,
    against: str | None = None,
    ignore: list[str] | None = None,
) -> VerificationResult:
    """Verify tables against the stored baseline without changing it.

    No profiles, anomalies or config are saved. The run itself is recorded
    in the local store, and appended to `record_path` as one JSON line when
    given. A remote store is downloaded but never uploaded back, so with one
    the record file is the only copy of the run that outlives the job.
    With `against` (a scope) the baseline is that database's; the run is
    still recorded under the connection's scope, noting `against`.

    `tables=None` verifies every table. With `dbt_scope`, the tables its
    models resolve to are added to `tables`, and when that leaves none,
    nothing is verified: a dbt run that built nothing must not check the
    whole warehouse.
    """
    connector = _get_connector_or_exit()
    cfg = ScherlokConfig.load()
    if dbt_scope is not None:
        dbt_scope.resolve(set(connector.list_tables()), _connector_namespace(connector))
        tables = [*(tables or []), *dbt_scope.tables]
    nothing_to_verify = tables is not None and not tables

    from scherlok.config import PROFILES_DB
    with sync_context(cfg.get_store(), PROFILES_DB, upload_on_exit=False):
        store = open_baseline_store(connector.connection_string, against)
        run = RunRecord(
            command=COMMAND_VERIFY,
            scope=connection_scope(connector.connection_string),
            fail_on=fail_on,
            against=against,
            ignored=list(ignore or []),
        )
        try:
            result = (
                VerificationResult(
                    fail_on=fail_on, against=against, ignored=normalize_ignore(ignore)
                )
                if nothing_to_verify
                else verify_tables(
                    connector, store, tables, fail_on, against=against, ignore=ignore
                )
            )
            record = run.finish(result.tables_checked, result.anomalies).to_dict()
            store.save_run(record)
        except (UnknownTablesError, AgainstScopeError) as exc:
            out_error(f"[red]{rich_escape(str(exc))}[/red]")
            raise typer.Exit(code=1) from exc
        finally:
            store.close()

    if record_path is not None:
        try:
            append_run_record(record_path, record)
        except OSError as exc:
            out_error(f"[red]Could not write the run record: {rich_escape(str(exc))}[/red]")
            raise typer.Exit(code=1) from exc
    return result


def _print_verification(result: VerificationResult) -> None:
    """Render a verification for people: anomalies, unverified tables, verdict.

    A failed verdict goes through out_error so it still shows under --quiet.
    """
    if result.anomalies and not is_quiet():
        print_anomalies(result.anomalies)
    missing = result.tables_without_baseline
    against = rich_escape(result.against) if result.against else None
    if missing and against:
        out_info(
            f"[yellow]Not verified, no baseline in {against} ({len(missing)}): "
            f"{_table_preview(missing)}. Only the new-table checks ran on them.[/yellow]"
        )
    elif missing:
        out_info(
            f"[yellow]Not verified, no baseline yet ({len(missing)}): {_table_preview(missing)}. "
            "Once the data is right, run [bold]scherlok investigate[/bold] to set one.[/yellow]"
        )
    counts = severity_counts(result.anomalies)
    severities = ", ".join(f"{count} {name}" for name, count in reversed(counts.items()))
    compared = f" against {against}" if against else ""
    flags = ", ".join(
        [f"--fail-on {result.fail_on}", *(f"--ignore {check}" for check in result.ignored)]
    )
    summary = (
        f"{len(result.tables_verified)} verified{compared}, {len(missing)} without baseline, "
        f"{severities} ({flags}). No profiles or anomalies were saved."
    )
    if result.passed:
        out_info(f"[green]Verify passed:[/green] {summary}")
    else:
        out_error(f"[red]Verify failed:[/red] {summary}")


@app.command()
def verify(
    table: list[str] = typer.Option(
        None, "--table",
        help="Only verify this table. Repeat to verify several; omit for every table.",
    ),
    fail_on: str = typer.Option(
        FAIL_ON_CRITICAL, "--fail-on",
        help="Severity that fails the gate with exit code 1: 'critical' (default) or 'warning'",
    ),
    output: str = typer.Option(
        OUTPUT_TEXT, "--output",
        help="Output format: 'text' (default) or 'json' (one object on stdout)",
    ),
    record: Path | None = typer.Option(
        None, "--record", dir_okay=False,
        help="Also append this run's record to PATH as one JSON line, e.g. a CI artifact.",
    ),
    against: str | None = typer.Option(
        None, "--against",
        help="Compare with another database's baseline, e.g. production's: its scope "
        f"({AGAINST_EXAMPLE}) or connection string. Saves nothing to either.",
    ),
    ignore: list[str] = typer.Option(
        None, "--ignore",
        help="Leave a check out: 'volume' for a build on sampled data (row counts are "
        "not compared, and column checks allow for a sample).",
    ),
    dbt_project: Path | None = typer.Option(
        None, "--dbt-project",
        help="Verify the models that succeeded in this dbt project's last run "
        "(run_results.json and manifest.json) instead of every table.",
    ),
    dbt_target_path: str = typer.Option(
        DEFAULT_TARGET_PATH, "--dbt-target-path",
        help="dbt's target directory, relative to --dbt-project unless absolute.",
    ),
) -> None:
    """Compare tables with the stored baseline without changing it.

    A read-only gate: unlike `watch` and `ci`, it writes no profiles, no
    anomalies and no config, and never uploads a remote store, so retrying
    a failed check keeps failing until the data is fixed. Tables without a
    baseline are listed and never count as verified; those named with
    --table count as just built, so an empty table, an all-NULL column or a
    duplicate key there is CRITICAL. The connection comes from
    SCHERLOK_CONNECTION or `scherlok connect`. Each run is recorded in the
    local store (`scherlok history --runs`); `--record` also appends it to a
    file, which survives a CI job when the store is remote.

    --against compares the connection's tables, e.g. a staging build, with
    the baseline another database left in the same store, e.g. production's.
    Only that database's own records count, and freshness is not compared.
    `--dbt-project` scopes the run to what dbt just built: the models that
    succeeded in the last run_results.json, matched to tables by name, count
    as just built like --table, and --table adds to them. A model the
    connection can't see is listed under `dbt_models_not_found` in the JSON;
    when no model succeeded, nothing is verified.

    Example:
        scherlok verify
        scherlok verify --table orders --table customers --fail-on warning
        scherlok verify --output json --record scherlok-runs.jsonl
        scherlok verify --against postgresql://db.example.com/shop --ignore volume
        scherlok verify --dbt-project . --output json
    """
    output_lower = output.lower()
    if output_lower not in OUTPUT_FORMATS:
        out_error(f"[red]Invalid --output value '{output}'. Use 'text' or 'json'.[/red]")
        raise typer.Exit(code=1)
    json_mode = output_lower == OUTPUT_JSON
    fail_on = _fail_on_or_exit(fail_on)
    against = _against_or_exit(against)
    ignore = _ignore_or_exit(ignore)

    from scherlok.output import set_stderr, set_verbosity

    prev_quiet = is_quiet()
    if json_mode:
        # Same contract as `report --output json`: stdout carries only the
        # payload, and every Rich line goes to stderr.
        set_stderr(True)
        set_verbosity(quiet=True)

    try:
        dbt_scope = (
            _dbt_scope_or_exit(dbt_project, dbt_target_path) if dbt_project is not None else None
        )
        result = _run_verify(
            table or None, fail_on, record, dbt_scope, against=against, ignore=ignore
        )
        if json_mode:
            payload = result.to_dict()
            if dbt_scope is not None:
                payload[DBT_MODELS_NOT_FOUND_KEY] = dbt_scope.not_found
            print(json.dumps(payload))
        if dbt_scope is not None:
            _print_dbt_scope(dbt_scope)
        # Under the forced quiet of JSON mode only a failed verdict gets
        # through, on stderr, so a CI log still says why the step failed.
        _print_verification(result)
    finally:
        if json_mode:
            set_stderr(False)
            set_verbosity(quiet=prev_quiet)

    if not result.passed:
        raise typer.Exit(code=1)


def _reconcile_comparison(result: ReconcileResult) -> Table:
    """Side-by-side counts and totals of the source and the target."""
    source, target = result.source, result.target
    tbl = Table(title=f"Reconcile {rich_escape(source.table)} -> {rich_escape(target.table)}")
    tbl.add_column("")
    tbl.add_column(rich_escape(source.table), justify="right")
    tbl.add_column(rich_escape(target.table), justify="right")
    tbl.add_row("rows", f"{source.row_count:,}", f"{target.row_count:,}")
    if result.key_columns:
        tbl.add_row("rows with a NULL key", f"{source.null_keys:,}", f"{target.null_keys:,}")
        tbl.add_row("distinct keys", f"{source.distinct_keys:,}", f"{target.distinct_keys:,}")
        tbl.add_row(
            "keys on more than one row", f"{source.duplicate_keys:,}", f"{target.duplicate_keys:,}"
        )
        tbl.add_row(
            "keys the other table lacks", f"{result.missing_keys:,}", f"{result.extra_keys:,}"
        )
    for column in result.sum_columns:
        tbl.add_row(
            f"total {rich_escape(column)}",
            rich_escape(format_total(source.sums[column])),
            rich_escape(format_total(target.sums[column])),
        )
    return tbl


def _print_reconcile(result: ReconcileResult) -> None:
    """Render a reconcile for people: the comparison, any findings, the verdict.

    A failed verdict goes through out_error so it still shows under --quiet.
    Table and column names are escaped: they are user input and could hold
    Rich markup.
    """
    if not is_quiet():
        get_console().print(_reconcile_comparison(result))
        if result.anomalies:
            print_anomalies([
                {**anomaly, "table": rich_escape(anomaly["table"]),
                 "message": rich_escape(anomaly["message"])}
                for anomaly in result.anomalies
            ])
    pair = f"{rich_escape(result.target.table)} against {rich_escape(result.source.table)}"
    if result.passed:
        checked = ["row count"]
        if result.key_columns:
            checked.append(f"key {', '.join(result.key_columns)}")
        if result.sum_columns:
            checked.append(f"totals of {', '.join(result.sum_columns)}")
        out_info(
            f"[green]Reconcile passed:[/green] {pair} "
            f"({rich_escape('; '.join(checked))}). Nothing was saved."
        )
    else:
        count = len(result.anomalies)
        types = ", ".join(dict.fromkeys(anomaly["type"] for anomaly in result.anomalies))
        out_error(
            f"[red]Reconcile failed:[/red] {pair}: {count} "
            f"{'mismatch' if count == 1 else 'mismatches'} ({types}). Nothing was saved."
        )


@app.command()
def reconcile(
    source: str = typer.Option(
        ..., "--source",
        help="The input table, e.g. raw.orders. A dot separates schema and table.",
    ),
    target: str = typer.Option(
        ..., "--target", help="The output table built from the source, e.g. mart.orders.",
    ),
    key: list[str] = typer.Option(
        None, "--key",
        help="Key column, unique in the target. Repeat for a composite key. "
        "Checks repeated, missing and extra keys.",
    ),
    sum_columns: list[str] = typer.Option(
        None, "--sum", help="Column whose total must match. Repeat for several.",
    ),
    tolerance: float = typer.Option(
        0.0, "--tolerance",
        help="Largest absolute difference allowed between two totals (default 0: exact).",
    ),
    output: str = typer.Option(
        OUTPUT_TEXT, "--output",
        help="Output format: 'text' (default) or 'json' (one object on stdout)",
    ),
) -> None:
    """Check that an output table adds up against its input, in the same database.

    Compares row counts; with --key, keys repeated in the target (a join that
    fans out), source keys missing from it and target keys the source lacks;
    with --sum, column totals, which may differ by at most --tolerance. Any
    mismatch exits 1. Reads the warehouse only: nothing is saved, and only
    counts and totals are reported, never a key value. On BigQuery and
    Snowflake every check scans both tables in full.

    Example:
        scherlok reconcile --source raw.orders --target mart.orders --key order_id
        scherlok reconcile --source raw.orders --target mart.orders \\
            --key order_id --sum amount --tolerance 0.001 --output json
    """
    output_lower = output.lower()
    if output_lower not in OUTPUT_FORMATS:
        out_error(
            f"[red]Invalid --output value '{rich_escape(output)}'. Use 'text' or 'json'.[/red]"
        )
        raise typer.Exit(code=1)
    json_mode = output_lower == OUTPUT_JSON

    from scherlok.output import set_stderr, set_verbosity

    prev_verbose, prev_quiet = is_verbose(), is_quiet()
    if json_mode:
        # Same contract as `verify --output json`: stdout carries only the
        # payload, and every Rich line goes to stderr.
        set_stderr(True)
        set_verbosity(quiet=True)

    try:
        try:
            keys, sums = validate_reconcile_args(source, target, key, sum_columns, tolerance)
        except ValueError as exc:
            out_error(f"[red]{rich_escape(str(exc))}[/red]")
            raise typer.Exit(code=1) from exc
        connector = _get_connector_or_exit()
        try:
            result = reconcile_tables(
                connector, source, target,
                key_columns=keys, sum_columns=sums, tolerance=tolerance,
            )
        except (ValueError, ReconcileError, NotImplementedError) as exc:
            out_error(f"[red]{rich_escape(str(exc))}[/red]")
            raise typer.Exit(code=1) from exc
        if json_mode:
            print(json.dumps(result.to_dict()))
        _print_reconcile(result)
    finally:
        if json_mode:
            set_stderr(False)
            set_verbosity(verbose=prev_verbose, quiet=prev_quiet)

    if not result.passed:
        raise typer.Exit(code=1)


NO_BASELINE = "no baseline"
# Anomalies listed per table in the `accept` summary before it cuts the list.
PREVIEW_FINDINGS = 3


def _before_after(before: dict | None, after: dict | None, key: str) -> str:
    """``1,000 → 300`` for one summary key, ``no baseline → 300`` for a new table."""
    def fmt(summary: dict | None) -> str:
        if summary is None:
            return NO_BASELINE
        value = summary.get(key)
        return f"{value:,}" if isinstance(value, int) else str(value)

    return f"{fmt(before)} → {fmt(after)}"


def _findings_preview(anomalies: list[dict]) -> str:
    """``volume_drop (CRITICAL), null_rate_change email (WARNING)``, cut after a few."""
    if not anomalies:
        return "none"
    labels = [
        " ".join(filter(None, (a["type"], a.get("column")))) + f" ({a['severity']})"
        for a in anomalies
    ]
    overflow = len(labels) - PREVIEW_FINDINGS
    preview = ", ".join(rich_escape(label) for label in labels[:PREVIEW_FINDINGS])
    return f"{preview}, +{overflow} more" if overflow > 0 else preview


def _print_proposals(proposals: list[ProposedBaseline], reason: str, approved_by: str) -> None:
    """The before/after summary `accept` asks a person to confirm."""
    tbl = Table(title="Accept as the new baseline")
    tbl.add_column("Table", style="cyan")
    tbl.add_column("Rows", justify="right")
    tbl.add_column("Columns", justify="right")
    tbl.add_column("Against the current baseline")
    for proposal in proposals:
        after = proposal.after
        tbl.add_row(
            rich_escape(proposal.table),
            _before_after(proposal.before, after, ROW_COUNT_KEY),
            _before_after(proposal.before, after, COLUMN_COUNT_KEY),
            _findings_preview(after[ANOMALIES_KEY]),
        )
    console.print(tbl)
    console.print(f"Reason: {rich_escape(reason)}")
    console.print(f"Approved by: {rich_escape(approved_by)}")


@app.command()
def accept(
    table: list[str] = typer.Option(
        ..., "--table",
        help="Table whose current state becomes its baseline. Repeat to accept several.",
    ),
    reason: str = typer.Option(
        ..., "--reason", help="Why this state is the new normal. Required, and recorded.",
    ),
    yes: bool = typer.Option(
        False, "--yes", "-y",
        help="Skip the confirmation prompt, for a workflow a person triggers.",
    ),
) -> None:
    """Make the current state of tables their baseline, with a recorded approval.

    A run never learns a reading with a CRITICAL anomaly, and with baselines
    locked no run learns at all, so an intended change (a new column, a
    planned backfill) becomes the new normal here. Each table is profiled and
    compared with its baseline, the before/after summary is shown, and on
    confirmation the new profiles are saved with one approval row per table:
    who approved (`git config user.email`, else $USER), the reason, and the
    summary. `scherlok history --approvals` lists them. A table without a
    baseline gets its first one. Works whether baselines are locked or not.

    Deliberately not an MCP tool, so an agent can't approve its own change
    with a tool call. A guardrail, not a security boundary: anything that can
    run this CLI or write the store can still change the baseline.

    Example:
        scherlok accept --table orders --reason "Backfilled 2025 orders"
        scherlok accept --table orders --table refunds --reason "New refunds flow" --yes
    """
    reason = reason.strip()
    if not reason:
        out_error("[red]--reason is empty. Say why this state is the new normal.[/red]")
        raise typer.Exit(code=1)
    connector = _get_connector_or_exit()
    cfg = ScherlokConfig.load()
    store_url = cfg.get_store()

    from scherlok.config import PROFILES_DB
    # Compare without uploading: the prompt may wait for minutes, and putting
    # the copy back after a "no" would overwrite what other jobs saved since.
    with sync_context(store_url, PROFILES_DB, upload_on_exit=False):
        store = _open_store(connector.connection_string)
        try:
            targets = select_tables(table, connector.list_tables())
            proposals = [propose_baseline(connector, store, name) for name in targets]
        except UnknownTablesError as exc:
            out_error(f"[red]{rich_escape(str(exc))}[/red]")
            raise typer.Exit(code=1) from exc
        finally:
            store.close()

    approved_by = approver()
    _print_proposals(proposals, reason, approved_by)
    prompt = f"Make this the new baseline of {len(proposals)} table(s)?"
    if not yes and not typer.confirm(prompt, default=False):
        out_error("[yellow]Nothing accepted.[/yellow]")
        raise typer.Exit(code=1)

    # Downloaded again so the new rows land on the latest copy of a remote store.
    with sync_context(store_url, PROFILES_DB):
        store = _open_store(connector.connection_string)
        try:
            for proposal in proposals:
                approval = proposal.approval(
                    scope=store.scope, approved_by=approved_by, reason=reason
                )
                accept_baseline(store, proposal, approval)
        finally:
            store.close()

    out_info(
        f"[green]Accepted ({len(proposals)}):[/green] {_table_preview(targets)}. "
        "See [bold]scherlok history --approvals[/bold]."
    )


@app.command()
def dbt(
    project_dir: str = typer.Option(
        ".", "--project-dir", help="Path to dbt project root (containing dbt_project.yml)"
    ),
    profiles_dir: str = typer.Option(
        None, "--profiles-dir", help="Path to dir containing profiles.yml (default: ~/.dbt)"
    ),
    target: str = typer.Option(
        None, "--target", help="dbt target to use (default: profile's `target:` key)"
    ),
    connection_string: str = typer.Option(
        None, "--connection-string",
        help="Override profiles.yml resolution and use this connection string directly",
    ),
    select: list[str] = typer.Option(
        None, "--select", "-s",
        help="Only profile these models (by name). Repeat to select multiple.",
    ),
    include_sources: bool = typer.Option(
        False, "--include-sources", help="Also profile dbt sources, not only models"
    ),
    include_snapshots: bool = typer.Option(
        False,
        "--include-snapshots",
        help="Also profile dbt snapshots. Snapshots are SCD Type 2 tables that exist "
        "in the warehouse and are profilable like regular materialized models.",
    ),
    webhook: str = typer.Option(
        None, "--webhook", "-w", help="Webhook URL for alerts"
    ),
    email: list[str] = typer.Option(
        None, "--email", "-e", help="Email recipient(s) for alerts"
    ),
    fail_on: str = typer.Option(
        "critical", "--fail-on",
        help="Severity that triggers exit code 1: 'critical' (default) or 'warning'",
    ),
    output: str = typer.Option(
        "text", "--output",
        help="Output format: 'text' (default) or 'json' for CI parsers",
    ),
    show_lineage: bool = typer.Option(
        False, "--show-lineage",
        help="After each model's result, print an ASCII tree of its upstream "
        "and downstream models from manifest.json.",
    ),
    explain: bool = typer.Option(False, "--explain", help=EXPLAIN_FLAG_HELP),
) -> None:
    """Profile and watch dbt models. Reads target/manifest.json.

    Run this after `dbt run` to detect anomalies in materialized models.
    First run profiles a baseline; subsequent runs detect drift.

    Example:
        scherlok dbt --project-dir ./my_dbt_project
        scherlok dbt --project-dir . --select stg_orders --select fct_orders
        scherlok dbt --project-dir . --connection-string postgresql://...
        scherlok dbt --project-dir . --output json  # CI-parseable stdout
        scherlok dbt --project-dir . --show-lineage  # render lineage trees
    """
    output_lower = output.lower()
    if output_lower not in ("text", "json"):
        console.print(
            f"[red]Invalid --output value '{output}'. Use 'text' or 'json'.[/red]"
        )
        raise typer.Exit(code=1)
    json_mode = output_lower == "json"
    fail_on = _fail_on_or_exit(fail_on)

    from scherlok.output import is_quiet, set_stderr, set_verbosity
    prev_quiet = is_quiet()
    if json_mode:
        # Reroute Rich chatter to stderr so stdout stays exclusively for the
        # JSON payload emitted at the end of this command. The module-level
        # console is shared, so the restore in the finally block below is
        # what keeps subsequent commands (and subsequent tests) untouched.
        set_stderr(True)
        # _dispatch_alerts gates its Rich anomaly-table print on is_quiet();
        # flip that on for the duration of the json run so the table never
        # appears on stderr alongside the JSON payload either.
        set_verbosity(quiet=True)

    try:
        _dbt_impl(
            project_dir=project_dir,
            profiles_dir=profiles_dir,
            target=target,
            connection_string=connection_string,
            select=select,
            include_sources=include_sources,
            include_snapshots=include_snapshots,
            webhook=webhook,
            email=email,
            fail_on=fail_on,
            json_mode=json_mode,
            show_lineage=show_lineage,
            explain=explain,
        )
    finally:
        if json_mode:
            set_stderr(False)
            set_verbosity(quiet=prev_quiet)


def _dbt_impl(
    *,
    project_dir: str,
    profiles_dir: str | None,
    target: str | None,
    connection_string: str | None,
    select: list[str] | None,
    include_sources: bool,
    include_snapshots: bool,
    webhook: str | None,
    email: list[str] | None,
    fail_on: str,
    json_mode: bool,
    show_lineage: bool = False,
    explain: bool = False,
    executed_model_ids: set[str] | None = None,
) -> None:
    """The actual dbt-command body. Extracted so the `dbt()` entry point can
    wrap it in a try/finally that restores the shared output console after
    JSON-mode rerouting -- the entry point keeps the Typer surface and the
    flag parsing; this function does the work."""

    from scherlok.dbt import (
        ProfileResolutionError,
        build_dependency_graph,
        discover_exposures,
        discover_models,
        discover_sources,
        load_manifest,
        resolve_connection_string,
    )

    # 1. Load manifest
    try:
        manifest = load_manifest(project_dir)
    except (FileNotFoundError, ValueError) as exc:
        out_error(f"[red]{exc}[/red]")
        raise typer.Exit(code=1) from exc

    adapter = manifest.get("metadata", {}).get("adapter_type", "?")
    nodes: list[DbtNode] = discover_models(manifest, include_snapshots=include_snapshots)
    if executed_model_ids is not None:
        nodes = [
            node
            for node in nodes
            if node.resource_type != "model" or node.unique_id in executed_model_ids
        ]
    if include_sources:
        nodes.extend(discover_sources(manifest))

    if select:
        wanted = set(select)
        if executed_model_ids is None:
            nodes = [n for n in nodes if n.name in wanted or n.identifier in wanted]
        else:
            # dbt already expanded the selector. Keep model nodes from the
            # artifact and retain literal matching for explicitly included
            # sources/snapshots.
            nodes = [
                n
                for n in nodes
                if n.resource_type == "model" or n.name in wanted or n.identifier in wanted
            ]
        if not nodes and executed_model_ids is None:
            out_error(f"[red]No models matched --select {list(wanted)}.[/red]")
            raise typer.Exit(code=1)

    if not nodes:
        if executed_model_ids is not None:
            message = "No successfully executed dbt models found in run_results.json."
            if json_mode:
                print(
                    json.dumps(
                        {
                            "project_dir": project_dir,
                            "adapter": adapter,
                            "models": [],
                            "summary": {
                                "profiled": 0,
                                "anomalies": 0,
                                "critical": 0,
                                "warning": 0,
                            },
                        }
                    )
                )
            else:
                out_info(f"[yellow]{message}[/yellow]")
        else:
            out_error("[yellow]No materialized models found in manifest.[/yellow]")
        raise typer.Exit(code=0)

    # 2. Resolve connection string
    if connection_string:
        conn_str = connection_string
    else:
        try:
            conn_str = resolve_connection_string(
                project_dir, profiles_dir=profiles_dir, target=target
            )
        except ProfileResolutionError as exc:
            out_error(f"[red]{exc}[/red]")
            raise typer.Exit(code=1) from exc

    cfg = ScherlokConfig.load()
    cfg.connection_string = conn_str
    cfg.save()

    connector = get_connector(conn_str)
    if not connector.connect():
        _print_connect_failure(connector)
        raise typer.Exit(code=1)

    # 3. Match models to physical tables visible to the connector
    visible = VisibleTables(connector.list_tables())
    namespace = _connector_namespace(connector)
    ambiguous = ambiguous_identifiers(manifest_relations(manifest))
    matched: list[tuple[DbtNode, str]] = []
    missing: list[DbtNode] = []
    for node in nodes:
        physical = resolve_relation(node, visible, namespace, ambiguous)
        if physical is None:
            missing.append(node)
        else:
            matched.append((node, physical))

    node_label = "nodes" if (include_sources or include_snapshots) else "models"
    out_info(
        f"Investigating [bold]{len(matched)}[/bold] dbt {node_label} "
        f"in [cyan]{project_dir}[/cyan] ([dim]{adapter}[/dim])"
    )
    if missing:
        out_info(
            f"[yellow]Skipped {len(missing)} not found in {adapter}: "
            f"{', '.join(n.name for n in missing[:5])}"
            f"{' …' if len(missing) > 5 else ''}[/yellow]"
        )

    # 4. Build lineage graph once for downstream enrichment + --show-lineage
    lineage_graph = build_dependency_graph(manifest)
    exposures = discover_exposures(manifest)

    # 5. Per-model investigate + watch
    from scherlok.config import PROFILES_DB
    all_anomalies: list[dict] = []
    json_models: list[dict] = []
    explain_lineage: dict[str, list[str]] = {}
    kept_baselines: list[str] = []
    locked = cfg.baseline_locked()
    with sync_context(cfg.get_store(), PROFILES_DB):
        store = _open_store(conn_str)
        for node, physical in matched:
            table_anomalies, current_vol = _watch_table(
                connector, store, physical, persist=not locked
            )
            if blocks_baseline(table_anomalies):
                kept_baselines.append(physical)
            # Enrich each anomaly's message with downstream-impact info so the
            # downstream context follows the anomaly through prints, JSON, and
            # alerter payloads alike.
            _enrich_anomalies_with_lineage(
                table_anomalies, node.unique_id, lineage_graph, exposures
            )
            all_anomalies.extend(table_anomalies)
            if explain and table_anomalies:
                # Upstream parents feed the explainer bundle: broken parents
                # usually explain broken children.
                explain_lineage[physical] = _upstream_preview(lineage_graph, node.unique_id)
            if json_mode:
                json_models.append(_dbt_model_payload(node, physical, current_vol, table_anomalies))
            else:
                _print_dbt_model_result(node, physical, current_vol, table_anomalies)
                if show_lineage:
                    _print_lineage_block(lineage_graph, node.unique_id)
        explanation, explain_error = _dispatch_alerts(
            all_anomalies,
            store,
            webhook,
            email or None,
            explain=explain,
            explain_context=_explain_context(
                adapter=adapter, fail_on=fail_on, lineage=explain_lineage or None
            ),
        )
        _print_kept_baselines(kept_baselines, locked)

    # 5. Summary + exit code
    crit = sum(1 for a in all_anomalies if str(a.get("severity")).endswith("CRITICAL"))
    warn = sum(1 for a in all_anomalies if str(a.get("severity")).endswith("WARNING"))
    if json_mode:
        payload = {
            "project_dir": project_dir,
            "adapter": adapter,
            "models": json_models,
            "summary": {
                "profiled": len(matched),
                "anomalies": len(all_anomalies),
                "critical": crit,
                "warning": warn,
            },
        }
        # Stdout is the only sink in json mode (console prints are quiet) —
        # without this, a billed --explain hypothesis would be discarded.
        if explanation:
            payload["explanation"] = explanation
        elif explain_error:
            payload["explanation_error"] = explain_error
        print(json.dumps(payload))
    else:
        out_info(
            f"\n[bold]Summary:[/bold] {len(matched)} profiled, "
            f"{len(all_anomalies)} anomalies "
            f"([red]{crit} critical[/red], [yellow]{warn} warning[/yellow])"
        )

    if fails_gate(all_anomalies, fail_on):
        raise typer.Exit(code=1)


@app.command(name="dbt-run-and-watch")
def dbt_run_and_watch(
    project_dir: str = typer.Option(
        ".", "--project-dir", help="Path to dbt project root (containing dbt_project.yml)"
    ),
    profiles_dir: str = typer.Option(
        None, "--profiles-dir", help="Path to dir containing profiles.yml (default: ~/.dbt)"
    ),
    target: str = typer.Option(
        None, "--target", help="dbt target to use (default: profile's `target:` key)"
    ),
    connection_string: str = typer.Option(
        None, "--connection-string",
        help="Override profiles.yml resolution and use this connection string directly",
    ),
    select: list[str] = typer.Option(
        None, "--select", "-s",
        help=(
            "Only profile these models. Also passed to the dbt command as --select. "
            "Repeat to select multiple."
        ),
    ),
    build: bool = typer.Option(
        False, "--build", help="Run `dbt build` instead of `dbt run`."
    ),
    include_sources: bool = typer.Option(
        False, "--include-sources", help="Also profile dbt sources, not only models"
    ),
    include_snapshots: bool = typer.Option(
        False,
        "--include-snapshots",
        help="Also profile dbt snapshots. Snapshots are SCD Type 2 tables that exist "
        "in the warehouse and are profilable like regular materialized models.",
    ),
    webhook: str = typer.Option(
        None, "--webhook", "-w", help="Webhook URL for alerts"
    ),
    email: list[str] = typer.Option(
        None, "--email", "-e", help="Email recipient(s) for alerts"
    ),
    fail_on: str = typer.Option(
        "critical", "--fail-on",
        help="Severity that triggers exit code 1: 'critical' (default) or 'warning'",
    ),
    show_lineage: bool = typer.Option(
        False, "--show-lineage",
        help="After each model's result, print an ASCII tree of its upstream "
        "and downstream models from manifest.json.",
    ),
    output: str = typer.Option(
        "text", "--output",
        help="Output format: 'text' (default) or 'json' for CI parsers",
    ),
    explain: bool = typer.Option(False, "--explain", help=EXPLAIN_FLAG_HELP),
) -> None:
    """Run `dbt run` then immediately profile the freshly built models.

    Pass `--build` to use `dbt build` instead. The normal `dbt run` path
    remains unchanged. For `dbt build`, successfully executed models from
    run_results.json are profiled when a handled build failure (exit 1) skips
    downstream resources, and the build's exit code is preserved.

    Wraps the typical CI sequence (`dbt run` -> `scherlok dbt`) into one
    command. If `dbt run` fails, exits with the same code WITHOUT running
    scherlok -- the manifest is stale or partial and watching it would
    surface noise instead of signal.

    Requires the `dbt` binary on PATH. dbt-core is not a hard dependency
    of scherlok; install it (or the appropriate dbt adapter) separately.

    Example:
        scherlok dbt-run-and-watch --project-dir . --target prod --fail-on critical
        scherlok dbt-run-and-watch --project-dir . --build --fail-on critical
        scherlok dbt-run-and-watch --project-dir . --output json  # CI-parseable stdout
    """
    output_lower = output.lower()
    if output_lower not in ("text", "json"):
        console.print(
            f"[red]Invalid --output value '{output}'. Use 'text' or 'json'.[/red]"
        )
        raise typer.Exit(code=1)
    json_mode = output_lower == "json"
    fail_on = _fail_on_or_exit(fail_on)

    from scherlok.output import is_quiet, set_stderr, set_verbosity
    prev_quiet = is_quiet()
    if json_mode:
        # Same rationale as `dbt --output json`: keep stdout exclusively for
        # the JSON payload, whether that payload is the profiling result or
        # the dbt-run-failure error document below.
        set_stderr(True)
        set_verbosity(quiet=True)

    try:
        dbt_bin = shutil.which("dbt")
        if dbt_bin is None:
            message = (
                "`dbt` binary not found on PATH. Install dbt-core "
                "(or your adapter) and re-run."
            )
            if json_mode:
                payload = {
                    "project_dir": project_dir,
                    "error": message,
                    "returncode": 1,
                }
                print(json.dumps(payload))
            else:
                out_error(f"[red]{message}[/red]")
            raise typer.Exit(code=1)

        dbt_action = "build" if build else "run"
        cmd: list[str] = [dbt_bin, dbt_action, "--project-dir", project_dir]
        if profiles_dir:
            cmd.extend(["--profiles-dir", profiles_dir])
        if target:
            cmd.extend(["--target", target])
        for s in select or []:
            cmd.extend(["--select", s])

        out_info(f"[bold]Running:[/bold] {' '.join(cmd)}")
        # Stream stdout live so long dbt runs surface progress in CI logs.
        # We don't capture output -- piping it through scherlok would buffer it.
        # Under --output json, dbt's own stdout is rerouted straight to our
        # stderr fd (not captured through Python) so it keeps streaming live
        # without ever touching the stdout channel reserved for the JSON payload.
        completed = subprocess.run(
            cmd, check=False, stdout=sys.stderr if json_mode else None
        )
        dbt_returncode = completed.returncode
        if dbt_returncode != 0 and (not build or dbt_returncode != 1):
            if json_mode:
                payload = {
                    "project_dir": project_dir,
                    "error": f"dbt {dbt_action} failed",
                    "returncode": dbt_returncode,
                }
                print(json.dumps(payload))
            else:
                out_error(
                    f"[red]`dbt {dbt_action}` failed with exit code {dbt_returncode}. "
                    f"Skipping scherlok watch (manifest may be stale).[/red]"
                )
            raise typer.Exit(code=dbt_returncode)

        if dbt_returncode == 0:
            out_info(f"[green]dbt {dbt_action} succeeded.[/green] Reading execution results...")
        from scherlok.dbt import load_run_results, successful_model_unique_ids_from_results

        try:
            run_results = load_run_results(project_dir)
            executed_model_ids = successful_model_unique_ids_from_results(
                run_results["results"]
            )
        except (FileNotFoundError, OSError, ValueError) as exc:
            if dbt_returncode == 1:
                message = (
                    f"`dbt {dbt_action}` failed with exit code {dbt_returncode}, and its "
                    f"run_results.json artifact could not be used: {exc}"
                )
                error_code = dbt_returncode
            else:
                message = (
                    f"`dbt {dbt_action}` succeeded, but its run_results.json artifact "
                    f"could not be used: {exc}"
                )
                error_code = 1
            if json_mode:
                print(
                    json.dumps(
                        {"project_dir": project_dir, "error": message, "returncode": error_code}
                    )
                )
            else:
                out_error(f"[red]{message}[/red]")
            raise typer.Exit(code=error_code) from exc

        if dbt_returncode == 1:
            out_error(
                f"[red]`dbt build` failed with exit code {dbt_returncode}. "
                "Profiling successfully built models from run_results.json; "
                "preserving the dbt exit code.[/red]"
            )

        # Call the implementation function (not the Typer-decorated `dbt`) so the
        # parameter defaults resolve to actual values instead of `OptionInfo`
        # sentinels. Pass every kwarg explicitly; `_dbt_impl` is keyword-only.
        try:
            _dbt_impl(
                project_dir=project_dir,
                profiles_dir=profiles_dir,
                target=target,
                connection_string=connection_string,
                select=select,
                include_sources=include_sources,
                include_snapshots=include_snapshots,
                webhook=webhook,
                email=email or None,
                fail_on=fail_on,
                json_mode=json_mode,
                show_lineage=show_lineage,
                explain=explain,
                executed_model_ids=executed_model_ids,
            )
        except typer.Exit:
            if dbt_returncode == 1:
                raise typer.Exit(code=dbt_returncode)
            raise
        if dbt_returncode == 1:
            raise typer.Exit(code=dbt_returncode)
    finally:
        if json_mode:
            set_stderr(False)
            set_verbosity(quiet=prev_quiet)


def _connector_namespace(connector: object) -> RelationNamespace:
    """Where the connector lists tables; unknown for one that can't say (#68)."""
    namespace = connector.namespace()  # type: ignore[attr-defined]
    return namespace if isinstance(namespace, RelationNamespace) else UNKNOWN_NAMESPACE


def _dbt_model_payload(
    node: object, physical: str, current_vol: dict, anomalies: list[dict]
) -> dict:
    """Serialize one dbt model's profile + anomalies for `--output json`.

    Enum severities serialize as their bare name (e.g. `"CRITICAL"` rather
    than `"Severity.CRITICAL"`) so the JSON contract is stable regardless
    of how the detector module spells the value. The structured fields
    (`column`, `metric`, `baseline`, `current`, `change_pct`, `hint`) follow.
    """
    return {
        "name": node.name,  # type: ignore[attr-defined]
        "physical": physical,
        "resource_type": getattr(node, "resource_type", "model"),
        "row_count": current_vol.get("row_count"),
        "anomalies": [
            {
                "severity": str(a.get("severity")).rsplit(".", 1)[-1],
                "type": a.get("type"),
                "message": a.get("message"),
                **anomaly_details(a),
            }
            for a in anomalies
        ],
    }


def _print_dbt_model_result(
    node: object, physical: str, current_vol: dict, anomalies: list[dict]
) -> None:
    """Print one ✓/✗ line for a dbt model with row count or anomaly summary.

    ✓ rows respect --quiet (gone with quiet); ✗ rows always print so CI logs
    surface the failures even on --quiet.
    """
    name = node.name  # type: ignore[attr-defined]
    if not anomalies:
        rows = current_vol.get("row_count", "?")
        out_info(f"  [green]✓[/green] {name:<30} ({rows:,} rows)")
        return
    severities = [str(a.get("severity")) for a in anomalies]
    worst = "CRITICAL" if any("CRITICAL" in s for s in severities) else "WARNING"
    color = "red" if worst == "CRITICAL" else "yellow"
    msg = anomalies[0].get("message", "anomaly detected")
    out_error(f"  [{color}]✗[/{color}] {name:<30} {worst}: {msg}")


def _enrich_anomalies_with_lineage(
    anomalies: list[dict],
    unique_id: str,
    graph: dict[str, list[str]],
    exposures: list[DbtExposure] | None = None,
) -> None:
    """Append a downstream-impact note to each anomaly's message in place.

    Resource types are classified for presentation only. The full transitive
    graph is still traversed so tests and other intermediate resources cannot
    hide an exposure farther downstream.
    """
    from scherlok.dbt import display_name as _display_name
    from scherlok.dbt import downstream_of as _downstream_of

    downstream = _downstream_of(graph, unique_id)
    if not downstream:
        return

    model_ids = [uid for uid in downstream if uid.split(".", 1)[0] == "model"]
    exposure_ids = [uid for uid in downstream if uid.split(".", 1)[0] == "exposure"]
    suffixes: list[str] = []
    if model_ids:
        model_names = [_display_name(uid) for uid in model_ids]
        preview = ", ".join(model_names[:3])
        overflow = f" (+{len(model_names) - 3} more)" if len(model_names) > 3 else ""
        plural = "s" if len(model_names) != 1 else ""
        suffixes.append(
            f"Affects {len(model_names)} downstream model{plural}: {preview}{overflow}"
        )

    exposure_by_id = {
        exposure.unique_id: exposure for exposure in exposures or []
    }
    if exposure_ids:
        exposure_names = [
            _format_exposure(uid, exposure_by_id.get(uid), _display_name)
            for uid in exposure_ids
        ]
        preview = ", ".join(exposure_names[:3])
        overflow = f" (+{len(exposure_names) - 3} more)" if len(exposure_names) > 3 else ""
        plural = "s" if len(exposure_names) != 1 else ""
        suffixes.append(
            f"Downstream exposure{plural}: {preview}{overflow}"
        )

    if not suffixes:
        return
    suffix = " · " + " · ".join(suffixes)
    for a in anomalies:
        msg = a.get("message", "")
        a["message"] = f"{msg}{suffix}"


def _format_exposure(
    unique_id: str,
    exposure: DbtExposure | None,
    fallback_name: Callable[[str], str],
) -> str:
    """Render one exposure with its useful owner information."""
    if exposure is None:
        label = fallback_name(unique_id)
        return label

    label = exposure.display_name
    label = label or fallback_name(unique_id)
    if exposure.owner_emails:
        return f"{label} (owner: {', '.join(exposure.owner_emails)})"
    if exposure.owner_name:
        return f"{label} (owner: {exposure.owner_name})"
    return label


def _upstream_preview(graph: dict[str, list[str]], unique_id: str) -> list[str]:
    """Ancestor display names, nearest first, for the explainer bundle.

    Delegates to lineage.upstream_of (BFS, cycle-safe); build_bundle caps
    the list at MAX_LINEAGE_PARENTS, so nearest ancestors survive the cut.
    """
    from scherlok.dbt import display_name as _display_name
    from scherlok.dbt import upstream_of as _upstream_of

    return [_display_name(uid) for uid in _upstream_of(graph, unique_id)]


def _print_lineage_block(graph: dict[str, list[str]], unique_id: str) -> None:
    """Print the ASCII lineage tree for one model, indented to align under ✓/✗."""
    from scherlok.dbt import render_lineage_tree as _render

    tree = _render(graph, unique_id)
    if not tree:
        return
    for line in tree.splitlines():
        out_info(f"    {line}")


@app.command()
def dashboard(
    out: str = typer.Option(
        "scherlok-report.html", "--out", "-o",
        help="Output path for the generated HTML report.",
    ),
    days: int = typer.Option(
        14, "--days", "-d",
        help="Anomaly history window (in days).",
    ),
    theme: str = typer.Option(
        "auto", "--theme",
        help="Theme: auto (follows OS), dark, or light.",
    ),
    project_name: str = typer.Option(
        None, "--project-name",
        help="Display name in the report header (default: derived from connection).",
    ),
) -> None:
    """Generate a self-contained HTML dashboard report from the local profile store.

    Reads ~/.scherlok/profiles.db (or the configured remote store) and writes
    a single HTML file you can open in a browser, share via Slack, or screenshot.

    Example:
        scherlok dashboard --out report.html
        scherlok dashboard --theme light --days 30
    """
    from scherlok.dashboard import render_dashboard

    cfg = ScherlokConfig.load()
    conn_str = cfg.get_connection_string() or ""
    display_name = project_name or _project_name_from_connection(conn_str)

    from scherlok.config import PROFILES_DB
    with sync_context(cfg.get_store(), PROFILES_DB):
        store = _open_store(conn_str)
        try:
            html = render_dashboard(
                store,
                days=days,
                theme=theme,
                project_name=display_name,
                connection_string=conn_str,
            )
        except ValueError as exc:
            console.print(f"[red]{exc}[/red]")
            raise typer.Exit(code=1) from exc
        finally:
            store.close()

    out_path = Path(out)
    out_path.write_text(html, encoding="utf-8")
    size_kb = len(html.encode("utf-8")) / 1024
    console.print(
        f"[green]Dashboard written to[/green] [bold]{out_path}[/bold] "
        f"([dim]{size_kb:.0f} KB[/dim])"
    )


def _project_name_from_connection(conn: str) -> str:
    """Derive a friendly project name from a connection URL, fallback to 'scherlok'."""
    if not conn:
        return "scherlok"
    if "://" in conn:
        rest = conn.split("://", 1)[1]
        # Last path segment, strip query
        last = rest.rstrip("/").split("/")[-1].split("?")[0]
        if last:
            return last
    return "scherlok"


@app.command()
def report(
    output: str = typer.Option(
        "text", "--output",
        help="Output format: 'text' (default) or 'json' for CI parsers",
    ),
) -> None:
    """Show profile summary for all monitored tables.

    Examples:
        scherlok report
        scherlok report --output json
    """
    output_lower = output.lower()
    if output_lower not in OUTPUT_FORMATS:
        console.print(
            f"[red]Invalid --output value '{output}'. Use 'text' or 'json'.[/red]"
        )
        raise typer.Exit(code=1)
    json_mode = output_lower == OUTPUT_JSON

    from scherlok.output import set_stderr, set_verbosity

    prev_quiet = is_quiet()
    if json_mode:
        set_stderr(True)
        set_verbosity(quiet=True)

    try:
        connector = _get_connector_or_exit()
        store = _open_store(connector.connection_string)
        tables = connector.list_tables()

        if not tables:
            if json_mode:
                print("[]")
            else:
                console.print("[yellow]No tables found.[/yellow]")
            return

        all_anomalies = store.get_anomaly_history(days=30)
        records: list[dict] = []

        for table in tables:
            vol = store.get_latest_profile(table, "volume")
            sch = store.get_latest_profile(table, "schema")
            fresh = store.get_latest_profile(table, "freshness")
            table_anomaly_count = sum(1 for a in all_anomalies if a["table"] == table)
            if json_mode:
                records.append({
                    "table": table,
                    "volume": vol,
                    "schema": sch,
                    "freshness": fresh,
                    "anomaly_count": table_anomaly_count,
                })
            else:
                print_profile_summary(
                    table, vol, sch, fresh, anomaly_count=table_anomaly_count
                )

        if json_mode:
            print(json.dumps(records))
    finally:
        if json_mode:
            set_stderr(False)
            set_verbosity(quiet=prev_quiet)


def _table_health(
    connector: object,
    store: ProfileStore,
    table: str,
    vol: dict | None,
    sch: dict | None,
) -> str:
    """Determine health status for one table via live anomaly check.

    Returns 'healthy', 'warning', 'critical', or 'unknown' (not profiled).
    """
    if not vol:
        return "unknown"
    current_vol = profile_volume(connector, table)
    current_sch = profile_schema(connector, table)
    current_fresh = profile_freshness(connector, table, current_sch.get("columns"))
    stored_fresh = store.get_latest_profile(table, "freshness")
    anomalies = detect_volume_anomalies(table, current_vol, vol)
    anomalies.extend(detect_schema_drift(table, current_sch, sch))
    if stored_fresh:
        anomalies.extend(
            detect_freshness_anomalies(table, current_fresh, stored_fresh)
        )
    if any(a["severity"].value == "CRITICAL" for a in anomalies):
        return "critical"
    if any(a["severity"].value == "WARNING" for a in anomalies):
        return "warning"
    return "healthy"


_STATUS_RICH = {
    "healthy": "[green]OK[/green]",
    "warning": "[yellow]WARNING[/yellow]",
    "critical": "[red]CRITICAL[/red]",
    "unknown": "[yellow]Not profiled[/yellow]",
}


def _relative_age(detected_at: str, now: datetime | None = None) -> str:
    """Short relative age ("2h ago") for a stored ISO timestamp."""
    past = datetime.fromisoformat(detected_at)
    if past.tzinfo is None:
        past = past.replace(tzinfo=timezone.utc)
    seconds = max(0, int(((now or datetime.now(timezone.utc)) - past).total_seconds()))
    minutes, _ = divmod(seconds, 60)
    if minutes < 1:
        return "just now"
    if minutes < 60:
        return f"{minutes}m ago"
    hours, _ = divmod(minutes, 60)
    if hours < 24:
        return f"{hours}h ago"
    return f"{hours // 24}d ago"


@app.command()
def status(
    output: str = typer.Option(
        "text", "--output",
        help="Output format: 'text' (default) or 'json' for CI parsers",
    ),
) -> None:
    """Show health of monitored tables.

    Example:
        scherlok status
        scherlok status --output json
    """
    output_lower = output.lower()
    if output_lower not in ("text", "json"):
        console.print(
            f"[red]Invalid --output value '{output}'. Use 'text' or 'json'.[/red]"
        )
        raise typer.Exit(code=1)
    json_mode = output_lower == "json"

    from scherlok.output import set_stderr, set_verbosity
    prev_quiet = is_quiet()
    if json_mode:
        set_stderr(True)
        set_verbosity(quiet=True)

    try:
        connector = _get_connector_or_exit()
        store = _open_store(connector.connection_string)
        tables = connector.list_tables()

        records: list[dict] = []
        latest = store.get_latest_anomaly_per_table()
        for table in tables:
            vol = store.get_latest_profile(table, "volume")
            sch = store.get_latest_profile(table, "schema")
            records.append({
                "table": table,
                "rows": vol["row_count"] if vol else None,
                "columns": len(sch["columns"]) if sch else None,
                "status": _table_health(connector, store, table, vol, sch),
                "last_profiled": vol.get("timestamp") if vol else None,
                "last_anomaly": latest.get(table),
            })

        if json_mode:
            print(json.dumps(records))
        else:
            tbl = Table(title="Table Health")
            tbl.add_column("Table", style="cyan")
            tbl.add_column("Rows", justify="right")
            tbl.add_column("Columns", justify="right")
            tbl.add_column("Last Profiled")
            tbl.add_column("Status")
            tbl.add_column("Last Anomaly")
            for r in records:
                anomaly = r["last_anomaly"]
                last = (
                    f"{anomaly['type']} · {_relative_age(anomaly['detected_at'])}"
                    if anomaly
                    else ""
                )
                tbl.add_row(
                    r["table"],
                    str(r["rows"]) if r["rows"] is not None else "—",
                    str(r["columns"]) if r["columns"] is not None else "—",
                    r["last_profiled"] or "—",
                    _STATUS_RICH[r["status"]],
                    last,
                )
            console.print(tbl)
    finally:
        if json_mode:
            set_stderr(False)
            set_verbosity(quiet=prev_quiet)


# Anomaly counts of `history --runs`, most severe first.
RUN_COUNT_KEYS = tuple(severity.value.lower() for severity in reversed(Severity))


def _print_runs(records: list[dict], days: int, json_mode: bool) -> None:
    """Render `history --runs`: one JSON array, or a table, newest run first."""
    if json_mode:
        print(json.dumps(records))
        return
    if not records:
        console.print(f"[yellow]No runs in the last {days} days.[/yellow]")
        return

    # Sized for an 80-column terminal: only the anomaly counts wrap. The run
    # id and the table names are in `--output json`.
    tbl = Table(title=f"Runs (last {days} days)")
    tbl.add_column("Started At", no_wrap=True)
    tbl.add_column("Command", style="cyan", no_wrap=True)
    tbl.add_column("Result", no_wrap=True)
    tbl.add_column("--fail-on", no_wrap=True)
    tbl.add_column("Tables", justify="right", no_wrap=True)
    tbl.add_column("Anomalies")

    for r in records:
        result = "[green]passed[/green]" if r[RUN_PASSED_KEY] else "[red]failed[/red]"
        anomalies = ", ".join(f"{r[key]} {key}" for key in RUN_COUNT_KEYS)
        tbl.add_row(
            r["started_at"][:19].replace("T", " "),
            rich_escape(r["command"]),
            result,
            r["fail_on"],
            str(len(r[RUN_TABLES_KEY])),
            anomalies,
        )
    console.print(tbl)


def _print_approvals(records: list[dict], days: int, json_mode: bool) -> None:
    """Render `history --approvals`: one JSON array, or a table, newest first."""
    if json_mode:
        print(json.dumps(records))
        return
    if not records:
        console.print(f"[yellow]No baseline approvals in the last {days} days.[/yellow]")
        return

    # The summaries in full, with the anomalies each approval accepted, are
    # in `--output json`.
    tbl = Table(title=f"Baseline approvals (last {days} days)")
    tbl.add_column("Approved At", no_wrap=True)
    tbl.add_column("Table", style="cyan")
    tbl.add_column("Rows", justify="right")
    tbl.add_column("Approved By", overflow="fold")  # an e-mail has no space to wrap at
    tbl.add_column("Reason")
    for r in records:
        tbl.add_row(
            r["approved_at"][:19].replace("T", " "),
            rich_escape(r["table_name"]),
            _before_after(r[APPROVAL_BEFORE_KEY], r[APPROVAL_AFTER_KEY], ROW_COUNT_KEY),
            rich_escape(r["approved_by"]),
            rich_escape(r["reason"]),
        )
    console.print(tbl)


@app.command()
def history(
    days: int = typer.Option(30, "--days", "-d", help="Number of days to look back"),
    runs: bool = typer.Option(
        False, "--runs",
        help="List runs (watch, ci, check, verify, MCP tools), passing or not, "
        "instead of anomalies.",
    ),
    approvals: bool = typer.Option(
        False, "--approvals",
        help="List baseline approvals made with `scherlok accept` instead of anomalies.",
    ),
    output: str = typer.Option(
        "text", "--output",
        help="Output format: 'text' (default) or 'json' for CI parsers",
    ),
) -> None:
    """Show timeline of detected anomalies, or every recorded run or baseline approval.

    Example:
        scherlok history
        scherlok history --days 7
        scherlok history --output json
        scherlok history --runs --output json
        scherlok history --approvals --output json
    """
    output_lower = output.lower()
    if output_lower not in ("text", "json"):
        console.print(
            f"[red]Invalid --output value '{output}'. Use 'text' or 'json'.[/red]"
        )
        raise typer.Exit(code=1)
    json_mode = output_lower == "json"
    if runs and approvals:
        out_error("[red]Use --runs or --approvals, not both.[/red]")
        raise typer.Exit(code=1)

    # Scoped to the configured connection, which is read but never opened.
    store = _open_store(ScherlokConfig.load().get_connection_string())
    if runs:
        _print_runs(store.get_runs(days=days), days, json_mode)
        return
    if approvals:
        _print_approvals(store.get_approvals(days=days), days, json_mode)
        return
    records = store.get_anomaly_history(days=days)

    if json_mode:
        print(json.dumps(records))
        return

    if not records:
        console.print(f"[green]No anomalies in the last {days} days.[/green]")
        raise typer.Exit(code=0)

    tbl = Table(title=f"Anomaly History (last {days} days)")
    tbl.add_column("Detected At")
    tbl.add_column("Severity")
    tbl.add_column("Table", style="cyan")
    tbl.add_column("Type")
    tbl.add_column("Message")

    for r in records:
        sev = r["severity"]
        if sev == "CRITICAL":
            sev_styled = "[red]CRITICAL[/red]"
        elif sev == "WARNING":
            sev_styled = "[yellow]WARNING[/yellow]"
        else:
            sev_styled = f"[dim]{sev}[/dim]"

        detected = r["detected_at"][:19].replace("T", " ")
        tbl.add_row(detected, sev_styled, r["table"], r["type"], r["message"])

    console.print(tbl)


@app.command()
def demo(
    directory: Path = typer.Option(
        None, "--dir", help="Directory for the demo files (implies --keep).",
    ),
    keep: bool = typer.Option(
        False, "--keep", help="Keep the demo database and profiles after the run.",
    ),
    output: str = typer.Option(
        "text", "--output", help="Output format: text (default) or json.",
    ),
) -> None:
    """Seed a sample warehouse, learn it, break it, catch it. No database needed.

    Runs the whole loop against a throwaway DuckDB file in one directory;
    nothing under ~/.scherlok is touched. Always exits 0 — it is a
    demonstration, not a gate.

    Example:
        uvx --from "scherlok[duckdb]" scherlok demo
        scherlok demo --keep
    """
    if output.lower() not in OUTPUT_FORMATS:
        out_error(f"[red]Invalid --output value '{output}'. Use 'text' or 'json'.[/red]")
        raise typer.Exit(code=1)
    output = output.lower()
    if not duckdb_available():
        out_error(f"[red]{rich_escape(DUCKDB_MISSING_HINT)}[/red]")
        raise typer.Exit(code=1)

    console = get_console() if output == OUTPUT_TEXT else None
    result = run_demo(directory=directory, keep=keep, console=console)
    if output == OUTPUT_JSON:
        typer.echo(json.dumps(result.to_dict(), indent=2))


@app.command()
def version() -> None:
    """Show Scherlok version.

    Example:
        scherlok version
    """
    console.print(f"scherlok [bold]{__version__}[/bold]")


if __name__ == "__main__":
    app()
