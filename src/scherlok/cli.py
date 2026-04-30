"""CLI entry point for Scherlok. Built with Typer and Rich."""

from pathlib import Path

import typer
from rich.console import Console
from rich.table import Table

from scherlok import __version__
from scherlok.alerter.console import print_anomalies, print_profile_summary
from scherlok.alerter.email import send_email_alert
from scherlok.alerter.exitcode import exit_code_for
from scherlok.alerter.webhook import send_webhook
from scherlok.config import ScherlokConfig
from scherlok.connectors import get_connector
from scherlok.detector.anomaly import detect_volume_anomalies
from scherlok.detector.cardinality import detect_cardinality_anomalies
from scherlok.detector.distribution_shift import detect_distribution_shift
from scherlok.detector.freshness import detect_freshness_anomalies
from scherlok.detector.nullability import detect_nullability_anomalies
from scherlok.detector.schema_drift import detect_schema_drift
from scherlok.profiler.distribution import profile_distribution
from scherlok.profiler.freshness import profile_freshness
from scherlok.profiler.schema import profile_schema
from scherlok.profiler.volume import profile_volume
from scherlok.store.remote import sync_context
from scherlok.store.sqlite import ProfileStore

app = typer.Typer(
    name="scherlok",
    help="A detective for your data. Zero-config data quality monitoring.",
    add_completion=False,
)
console = Console()


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
        console.print("[red]Failed to connect to the database.[/red]")
        raise typer.Exit(code=1)
    return connector


@app.command()
def connect(
    connection_string: str = typer.Argument(
        ..., help="Database connection string (e.g. postgresql://user:pass@host/db)"
    ),
) -> None:
    """Validate a database connection and save it to config.

    Example:
        scherlok connect postgresql://user:pass@localhost:5432/mydb
    """
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
        console.print("[red]Connection failed.[/red] Check your connection string.")
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
            console.print("[yellow]No tables found.[/yellow]")
            raise typer.Exit(code=0)

        console.print(f"Investigating [bold]{len(tables)}[/bold] tables...")

        for table in tables:
            console.print(f"  Profiling [cyan]{table}[/cyan]...")

            vol = profile_volume(connector, table)
            store.save_profile(table, "volume", vol)

            sch = profile_schema(connector, table)
            store.save_profile(table, "schema", sch)

            fresh = profile_freshness(connector, table)
            store.save_profile(table, "freshness", fresh)

            for col in sch.get("columns", []):
                dist = profile_distribution(connector, table, col["name"])
                store.save_profile(table, f"distribution:{col['name']}", dist)

        console.print(
            f"[green]Investigation complete.[/green] {len(tables)} tables profiled."
        )


def _watch_table(connector: object, store: ProfileStore, table: str) -> tuple[list[dict], dict]:
    """Profile one table + detect anomalies against stored baseline. Saves new profiles.

    Returns (anomalies, current_volume) — `current_volume` is handy for callers
    that want to display row counts inline (e.g. dbt-style ✓/✗ output).
    """
    anomalies: list[dict] = []

    current_vol = profile_volume(connector, table)
    stored_vol = store.get_latest_profile(table, "volume")
    if stored_vol:
        anomalies.extend(detect_volume_anomalies(table, current_vol, stored_vol))

    current_sch = profile_schema(connector, table)
    stored_sch = store.get_latest_profile(table, "schema")
    if stored_sch:
        anomalies.extend(detect_schema_drift(table, current_sch, stored_sch))

    current_fresh = profile_freshness(connector, table)
    stored_fresh = store.get_latest_profile(table, "freshness")
    if stored_fresh:
        anomalies.extend(
            detect_freshness_anomalies(table, current_fresh, stored_fresh)
        )

    for col in (current_sch or {}).get("columns", []):
        col_name = col["name"]
        current_dist = profile_distribution(connector, table, col_name)
        stored_dist = store.get_latest_profile(table, f"distribution:{col_name}")
        if stored_dist:
            anomalies.extend(
                detect_nullability_anomalies(table, col_name, current_dist, stored_dist)
            )
            anomalies.extend(
                detect_distribution_shift(table, col_name, current_dist, stored_dist)
            )
            anomalies.extend(
                detect_cardinality_anomalies(table, col_name, current_dist, stored_dist)
            )
        store.save_profile(table, f"distribution:{col_name}", current_dist)

    store.save_profile(table, "volume", current_vol)
    store.save_profile(table, "schema", current_sch)
    store.save_profile(table, "freshness", current_fresh)

    return anomalies, current_vol


def _dispatch_alerts(
    anomalies: list[dict],
    store: ProfileStore,
    webhook: str | None,
    emails: list[str] | None,
) -> None:
    """Persist anomalies and fan out to webhook/email alerters."""
    if not anomalies:
        console.print("[green]No anomalies detected.[/green]")
        return
    store.save_anomalies(anomalies)
    print_anomalies(anomalies)
    if webhook:
        ok = send_webhook(webhook, anomalies)
        console.print(
            "[dim]Webhook sent.[/dim]" if ok else "[red]Webhook delivery failed.[/red]"
        )
    if emails:
        ok = send_email_alert(emails, anomalies)
        console.print(
            "[dim]Email sent.[/dim]" if ok else "[red]Email delivery failed.[/red]"
        )


def _run_watch(
    webhook: str | None = None,
    emails: list[str] | None = None,
    tables: list[str] | None = None,
    connector: object | None = None,
) -> list[dict]:
    """Run the watch logic: profile + detect + alert. Returns all anomalies.

    Reused by `watch`, `ci`, and `dbt` commands. When `tables` is None, all
    tables visible to the connector are used. When `connector` is None, one
    is loaded from the saved config.
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

        console.print(f"Watching [bold]{len(tables)}[/bold] tables...")

        for table in tables:
            anomalies, _ = _watch_table(connector, store, table)
            all_anomalies.extend(anomalies)

        _dispatch_alerts(all_anomalies, store, webhook, emails)

    return all_anomalies


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
) -> None:
    """Compare current state vs stored profile. Detect anomalies.

    Example:
        scherlok watch
        scherlok watch --webhook https://hooks.slack.com/...
        scherlok watch --email alice@company.com --email bob@company.com
    """
    anomalies = _run_watch(webhook=webhook, emails=email or None)
    raise typer.Exit(code=exit_code_for(anomalies))


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
    # Save connection
    cfg = ScherlokConfig.load()
    cfg.connection_string = connection_string
    cfg.save()

    # Validate connection
    connector = get_connector(connection_string)
    if not connector.connect():
        console.print("[red]Failed to connect.[/red]")
        raise typer.Exit(code=1)

    # Run watch (does profile + detect + alert in one)
    anomalies = _run_watch(webhook=webhook, emails=email or None)

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
) -> None:
    """Profile and watch dbt models. Reads target/manifest.json.

    Run this after `dbt run` to detect anomalies in materialized models.
    First run profiles a baseline; subsequent runs detect drift.

    Example:
        scherlok dbt --project-dir ./my_dbt_project
        scherlok dbt --project-dir . --select stg_orders --select fct_orders
        scherlok dbt --project-dir . --connection-string postgresql://...
    """
    from scherlok.dbt import (
        DbtNode,
        ProfileResolutionError,
        discover_models,
        discover_sources,
        load_manifest,
        resolve_connection_string,
    )

    # 1. Load manifest
    try:
        manifest = load_manifest(project_dir)
    except (FileNotFoundError, ValueError) as exc:
        console.print(f"[red]{exc}[/red]")
        raise typer.Exit(code=1) from exc

    adapter = manifest.get("metadata", {}).get("adapter_type", "?")
    nodes: list[DbtNode] = discover_models(manifest)
    if include_sources:
        nodes.extend(discover_sources(manifest))

    if select:
        wanted = set(select)
        nodes = [n for n in nodes if n.name in wanted or n.identifier in wanted]
        if not nodes:
            console.print(f"[red]No models matched --select {list(wanted)}.[/red]")
            raise typer.Exit(code=1)

    if not nodes:
        console.print("[yellow]No materialized models found in manifest.[/yellow]")
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
            console.print(f"[red]{exc}[/red]")
            raise typer.Exit(code=1) from exc

    cfg = ScherlokConfig.load()
    cfg.connection_string = conn_str
    cfg.save()

    connector = get_connector(conn_str)
    if not connector.connect():
        console.print("[red]Failed to connect using the resolved connection.[/red]")
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

    console.print(
        f"Investigating [bold]{len(matched)}[/bold] dbt {'nodes' if include_sources else 'models'} "
        f"in [cyan]{project_dir}[/cyan] ([dim]{adapter}[/dim])"
    )
    if missing:
        console.print(
            f"[yellow]Skipped {len(missing)} not found in {adapter}: "
            f"{', '.join(n.name for n in missing[:5])}"
            f"{' …' if len(missing) > 5 else ''}[/yellow]"
        )

    # 4. Per-model investigate + watch
    from scherlok.config import PROFILES_DB
    all_anomalies: list[dict] = []
    with sync_context(cfg.get_store(), PROFILES_DB):
        store = ProfileStore()
        for node, physical in matched:
            table_anomalies, current_vol = _watch_table(connector, store, physical)
            all_anomalies.extend(table_anomalies)
            _print_dbt_model_result(node, physical, current_vol, table_anomalies)
        _dispatch_alerts(all_anomalies, store, webhook, email or None)

    # 5. Summary + exit code
    crit = sum(1 for a in all_anomalies if str(a.get("severity")).endswith("CRITICAL"))
    warn = sum(1 for a in all_anomalies if str(a.get("severity")).endswith("WARNING"))
    console.print(
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


def _print_dbt_model_result(
    node: object, physical: str, current_vol: dict, anomalies: list[dict]
) -> None:
    """Print one ✓/✗ line for a dbt model with row count or anomaly summary."""
    name = node.name  # type: ignore[attr-defined]
    if not anomalies:
        rows = current_vol.get("row_count", "?")
        console.print(f"  [green]✓[/green] {name:<30} ({rows:,} rows)")
        return
    # Show the worst anomaly summary on the line
    severities = [str(a.get("severity")) for a in anomalies]
    worst = "CRITICAL" if any("CRITICAL" in s for s in severities) else "WARNING"
    color = "red" if worst == "CRITICAL" else "yellow"
    msg = anomalies[0].get("message", "anomaly detected")
    console.print(f"  [{color}]✗[/{color}] {name:<30} {worst}: {msg}")


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
