"""CLI entry point for Scherlok. Built with Typer and Rich."""


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


def _run_watch(
    webhook: str | None = None,
    emails: list[str] | None = None,
) -> list[dict]:
    """Run the watch logic: profile + detect + alert. Returns all anomalies.

    Reused by `watch` and `ci` commands.
    """
    connector = _get_connector_or_exit()
    cfg = ScherlokConfig.load()

    from scherlok.config import PROFILES_DB
    with sync_context(cfg.get_store(), PROFILES_DB):
        store = ProfileStore()
        tables = connector.list_tables()
        all_anomalies: list[dict] = []

        console.print(f"Watching [bold]{len(tables)}[/bold] tables...")

        for table in tables:
            current_vol = profile_volume(connector, table)
            stored_vol = store.get_latest_profile(table, "volume")
            if stored_vol:
                all_anomalies.extend(
                    detect_volume_anomalies(table, current_vol, stored_vol)
                )

            current_sch = profile_schema(connector, table)
            stored_sch = store.get_latest_profile(table, "schema")
            if stored_sch:
                all_anomalies.extend(
                    detect_schema_drift(table, current_sch, stored_sch)
                )

            current_fresh = profile_freshness(connector, table)
            stored_fresh = store.get_latest_profile(table, "freshness")
            if stored_fresh:
                all_anomalies.extend(
                    detect_freshness_anomalies(table, current_fresh, stored_fresh)
                )

            for col in (current_sch or {}).get("columns", []):
                col_name = col["name"]
                current_dist = profile_distribution(connector, table, col_name)
                stored_dist = store.get_latest_profile(
                    table, f"distribution:{col_name}"
                )
                if stored_dist:
                    all_anomalies.extend(
                        detect_nullability_anomalies(
                            table, col_name, current_dist, stored_dist
                        )
                    )
                    all_anomalies.extend(
                        detect_distribution_shift(
                            table, col_name, current_dist, stored_dist
                        )
                    )
                    all_anomalies.extend(
                        detect_cardinality_anomalies(
                            table, col_name, current_dist, stored_dist
                        )
                    )
                store.save_profile(table, f"distribution:{col_name}", current_dist)

            store.save_profile(table, "volume", current_vol)
            store.save_profile(table, "schema", current_sch)
            store.save_profile(table, "freshness", current_fresh)

        if all_anomalies:
            store.save_anomalies(all_anomalies)
            print_anomalies(all_anomalies)
            if webhook:
                ok = send_webhook(webhook, all_anomalies)
                console.print(
                    "[dim]Webhook sent.[/dim]" if ok else "[red]Webhook delivery failed.[/red]"
                )
            if emails:
                ok = send_email_alert(emails, all_anomalies)
                console.print(
                    "[dim]Email sent.[/dim]" if ok else "[red]Email delivery failed.[/red]"
                )
        else:
            console.print("[green]No anomalies detected.[/green]")

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
