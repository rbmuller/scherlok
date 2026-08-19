"""CLI entry point for Scherlok. Built with Typer and Rich."""

from __future__ import annotations

import json
import shutil
import subprocess
import sys
import time
from collections.abc import Callable
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
from scherlok.alerter.exitcode import exit_code_for
from scherlok.alerter.webhook import send_webhook
from scherlok.config import ScherlokConfig
from scherlok.connectors import get_connector
from scherlok.detector.anomaly import detect_volume_anomalies
from scherlok.detector.freshness import detect_freshness_anomalies
from scherlok.detector.schema_drift import detect_schema_drift
from scherlok.explainer import (
    HISTORY_LOOKBACK_DAYS,
    describe_error,
    format_unavailable_note,
)
from scherlok.output import error as out_error
from scherlok.output import info as out_info
from scherlok.output import is_quiet, verbose_info
from scherlok.profiler.distribution import profile_distribution
from scherlok.profiler.freshness import profile_freshness
from scherlok.profiler.schema import profile_schema
from scherlok.profiler.volume import profile_volume
from scherlok.service import profile_and_detect
from scherlok.store.remote import sync_context
from scherlok.store.sqlite import ProfileStore

if TYPE_CHECKING:
    from scherlok.dbt.manifest import DbtExposure


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
    """Load config and return a connected connector, or exit with error."""
    cfg = ScherlokConfig.load()
    conn_str = cfg.get_connection_string()
    if not conn_str:
        console.print(
            "[red]No connection configured.[/red] "
            "Run [bold]scherlok connect <connection_string>[/bold] first."
        )
        raise typer.Exit(code=1)
    connector = get_connector(conn_str)
    if not connector.connect():
        _print_connect_failure(connector)
        raise typer.Exit(code=1)
    return connector


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
) -> None:
    """Configure Scherlok settings.

    Example:
        scherlok config --store s3://my-bucket/scherlok/profiles.db
        scherlok config --store gs://my-bucket/scherlok/profiles.db
        scherlok config --store az://my-container/scherlok/profiles.db
    """
    cfg = ScherlokConfig.load()
    if store is not None:
        cfg.settings["store"] = store
        cfg.save()
        console.print(f"[green]Store set to:[/green] {store}")
    else:
        current = cfg.settings.get("store") or "local (~/.scherlok/)"
        console.print(f"Current store: [cyan]{current}[/cyan]")


@app.command()
def investigate() -> None:
    """Profile all tables and store results.

    Example:
        scherlok investigate
    """
    connector = _get_connector_or_exit()
    cfg = ScherlokConfig.load()

    from scherlok.config import PROFILES_DB
    with sync_context(cfg.get_store(), PROFILES_DB):
        store = ProfileStore()
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

            fresh = profile_freshness(connector, table)
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


def _watch_table(connector: object, store: ProfileStore, table: str) -> tuple[list[dict], dict]:
    """Profile one table + detect anomalies against stored baseline. Saves new profiles.

    Thin CLI-facing alias over `service.profile_and_detect` — kept so existing
    call sites and tests that patch `scherlok.cli._watch_table` stay valid.
    Returns (anomalies, current_volume).
    """
    return profile_and_detect(connector, store, table)


def _dispatch_alerts(
    anomalies: list[dict],
    store: ProfileStore,
    webhook: str | None,
    emails: list[str] | None,
    *,
    explain: bool = False,
    explain_context: dict | None = None,
) -> tuple[dict | None, str | None]:
    """Persist anomalies and fan out to webhook/email alerters.

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
    store.save_anomalies(anomalies)
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
) -> list[dict]:
    """Run the watch logic: profile + detect + alert. Returns all anomalies.

    Reused by `watch`, `ci`, and `dbt` commands. When `tables` is None, all
    tables visible to the connector are used. When `connector` is None, one
    is loaded from the saved config. `fail_on` only feeds the --explain
    context; exit-code policy stays with the callers.
    """
    if connector is None:
        connector = _get_connector_or_exit()
    cfg = ScherlokConfig.load()

    from scherlok.config import PROFILES_DB
    with sync_context(cfg.get_store(), PROFILES_DB):
        store = ProfileStore()
        if tables is None:
            tables = connector.list_tables()
        all_anomalies: list[dict] = []

        out_info(f"Watching [bold]{len(tables)}[/bold] tables...")

        for table in tables:
            anomalies, _ = _watch_table(connector, store, table)
            all_anomalies.extend(anomalies)

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
        )

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
) -> None:
    """Compare current state vs stored profile. Detect anomalies.

    Example:
        scherlok watch
        scherlok watch --webhook https://hooks.slack.com/...
        scherlok watch --email alice@company.com --email bob@company.com
        scherlok watch --webhook https://hooks.slack.com/... --explain
    """
    anomalies = _run_watch(webhook=webhook, emails=email or None, explain=explain)
    raise typer.Exit(code=exit_code_for(anomalies))


def _run_ci(
    connection_string: str,
    webhook: str | None,
    email: list[str] | None,
    fail_on: str,
    explain: bool = False,
) -> None:
    # Save connection
    cfg = ScherlokConfig.load()
    cfg.connection_string = connection_string
    cfg.save()

    # Validate connection
    connector = get_connector(connection_string)
    if not connector.connect():
        _print_connect_failure(connector)
        raise typer.Exit(code=1)

    # Run watch (does profile + detect + alert in one)
    anomalies = _run_watch(
        webhook=webhook, emails=email or None, explain=explain, fail_on=fail_on
    )

    # Custom exit code based on --fail-on
    fail_on_lower = fail_on.lower()
    if fail_on_lower == "warning":
        from scherlok.detector.severity import Severity
        if any(a["severity"] in (Severity.WARNING, Severity.CRITICAL) for a in anomalies):
            raise typer.Exit(code=1)
    else:  # critical (default)
        if exit_code_for(anomalies) == 1:
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

    Designed for use in pipelines. Profiles tables on first run (no anomalies),
    then detects on subsequent runs against stored baseline (use remote storage).

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
    _run_ci(connection_string, webhook, email, fail_on, explain)


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
        DbtNode,
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
    visible = set(connector.list_tables())
    matched: list[tuple[DbtNode, str]] = []
    missing: list[DbtNode] = []
    for node in nodes:
        physical = _resolve_physical_table(node, visible)
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
    with sync_context(cfg.get_store(), PROFILES_DB):
        store = ProfileStore()
        for node, physical in matched:
            table_anomalies, current_vol = _watch_table(connector, store, physical)
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

    fail_on_lower = fail_on.lower()
    if fail_on_lower == "warning":
        from scherlok.detector.severity import Severity
        if any(
            a["severity"] in (Severity.WARNING, Severity.CRITICAL) for a in all_anomalies
        ):
            raise typer.Exit(code=1)
    else:
        if exit_code_for(all_anomalies) == 1:
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
            "Only profile these models. Also passed to `dbt run` as --select. "
            "Repeat to select multiple."
        ),
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

    Wraps the typical CI sequence (`dbt run` -> `scherlok dbt`) into one
    command. If `dbt run` fails, exits with the same code WITHOUT running
    scherlok -- the manifest is stale or partial and watching it would
    surface noise instead of signal.

    Requires the `dbt` binary on PATH. dbt-core is not a hard dependency
    of scherlok; install it (or the appropriate dbt adapter) separately.

    Example:
        scherlok dbt-run-and-watch --project-dir . --target prod --fail-on critical
        scherlok dbt-run-and-watch --project-dir . --output json  # CI-parseable stdout
    """
    output_lower = output.lower()
    if output_lower not in ("text", "json"):
        console.print(
            f"[red]Invalid --output value '{output}'. Use 'text' or 'json'.[/red]"
        )
        raise typer.Exit(code=1)
    json_mode = output_lower == "json"

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

        cmd: list[str] = [dbt_bin, "run", "--project-dir", project_dir]
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
        if completed.returncode != 0:
            if json_mode:
                payload = {
                    "project_dir": project_dir,
                    "error": "dbt run failed",
                    "returncode": completed.returncode,
                }
                print(json.dumps(payload))
            else:
                out_error(
                    f"[red]`dbt run` failed with exit code {completed.returncode}. "
                    f"Skipping scherlok watch (manifest may be stale).[/red]"
                )
            raise typer.Exit(code=completed.returncode)

        out_info("[green]dbt run succeeded.[/green] Reading execution results...")
        from scherlok.dbt import load_run_results
        from scherlok.dbt.run_results import _successful_model_unique_ids_from_validated

        try:
            run_results = load_run_results(project_dir)
            executed_model_ids = _successful_model_unique_ids_from_validated(
                run_results["results"]
            )
        except (FileNotFoundError, OSError, ValueError) as exc:
            message = (
                "`dbt run` succeeded, but its run_results.json artifact could not be "
                f"used: {exc}"
            )
            if json_mode:
                print(
                    json.dumps(
                        {"project_dir": project_dir, "error": message, "returncode": 1}
                    )
                )
            else:
                out_error(f"[red]{message}[/red]")
            raise typer.Exit(code=1) from exc

        # Call the implementation function (not the Typer-decorated `dbt`) so the
        # parameter defaults resolve to actual values instead of `OptionInfo`
        # sentinels. Pass every kwarg explicitly; `_dbt_impl` is keyword-only.
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
    finally:
        if json_mode:
            set_stderr(False)
            set_verbosity(quiet=prev_quiet)


def _resolve_physical_table(node: object, visible: set[str]) -> str | None:
    """Match a dbt node to a physical table name visible to the connector.

    Tries identifier first, then name; for Snowflake also tries uppercase variants.
    """
    candidates = [node.identifier, node.name]  # type: ignore[attr-defined]
    if node.adapter == "snowflake":  # type: ignore[attr-defined]
        candidates.extend([c.upper() for c in candidates if c])
    for cand in candidates:
        if cand and cand in visible:
            return cand
    return None


def _dbt_model_payload(
    node: object, physical: str, current_vol: dict, anomalies: list[dict]
) -> dict:
    """Serialize one dbt model's profile + anomalies for `--output json`.

    Enum severities serialize as their bare name (e.g. `"CRITICAL"` rather
    than `"Severity.CRITICAL"`) so the JSON contract is stable regardless
    of how the detector module spells the value.
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
        store = ProfileStore()
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
def report() -> None:
    """Show profile summary for all monitored tables.

    Example:
        scherlok report
    """
    store = ProfileStore()
    connector = _get_connector_or_exit()
    tables = connector.list_tables()

    if not tables:
        console.print("[yellow]No tables found.[/yellow]")
        raise typer.Exit(code=0)

    all_anomalies = store.get_anomaly_history(days=30)

    for table in tables:
        vol = store.get_latest_profile(table, "volume")
        sch = store.get_latest_profile(table, "schema")
        fresh = store.get_latest_profile(table, "freshness")
        table_anomaly_count = sum(1 for a in all_anomalies if a["table"] == table)
        print_profile_summary(table, vol, sch, fresh, anomaly_count=table_anomaly_count)


@app.command()
def status() -> None:
    """Show health of monitored tables.

    Example:
        scherlok status
    """
    store = ProfileStore()
    connector = _get_connector_or_exit()
    tables = connector.list_tables()

    tbl = Table(title="Table Health")
    tbl.add_column("Table", style="cyan")
    tbl.add_column("Rows", justify="right")
    tbl.add_column("Columns", justify="right")
    tbl.add_column("Last Profiled")
    tbl.add_column("Status")

    for table in tables:
        vol = store.get_latest_profile(table, "volume")
        sch = store.get_latest_profile(table, "schema")

        row_count = str(vol["row_count"]) if vol else "—"
        col_count = str(len(sch["columns"])) if sch else "—"
        last_profiled = vol.get("timestamp", "—") if vol else "—"

        if not vol:
            health = "[yellow]Not profiled[/yellow]"
        else:
            # Run quick anomaly check against current state
            current_vol = profile_volume(connector, table)
            current_sch = profile_schema(connector, table)
            current_fresh = profile_freshness(connector, table)
            stored_fresh = store.get_latest_profile(table, "freshness")
            table_anomalies = detect_volume_anomalies(table, current_vol, vol)
            table_anomalies.extend(detect_schema_drift(table, current_sch, sch))
            if stored_fresh:
                table_anomalies.extend(
                    detect_freshness_anomalies(table, current_fresh, stored_fresh)
                )
            if any(a["severity"].value == "CRITICAL" for a in table_anomalies):
                health = "[red]CRITICAL[/red]"
            elif any(a["severity"].value == "WARNING" for a in table_anomalies):
                health = "[yellow]WARNING[/yellow]"
            else:
                health = "[green]OK[/green]"

        tbl.add_row(table, row_count, col_count, str(last_profiled), health)

    console.print(tbl)


@app.command()
def history(
    days: int = typer.Option(30, "--days", "-d", help="Number of days to look back"),
) -> None:
    """Show timeline of detected anomalies.

    Example:
        scherlok history
        scherlok history --days 7
    """
    store = ProfileStore()
    records = store.get_anomaly_history(days=days)

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
def version() -> None:
    """Show Scherlok version.

    Example:
        scherlok version
    """
    console.print(f"scherlok [bold]{__version__}[/bold]")


if __name__ == "__main__":
    app()
