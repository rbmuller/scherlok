"""CLI entry point for Scherlok. Built with Typer and Rich."""

from typing import Optional

import typer
from rich.console import Console
from rich.table import Table

from scherlok import __version__
from scherlok.alerter.console import print_anomalies, print_profile_summary
from scherlok.alerter.exitcode import exit_code_for
from scherlok.config import ScherlokConfig
from scherlok.connectors import get_connector
from scherlok.detector.anomaly import detect_volume_anomalies
from scherlok.detector.schema_drift import detect_schema_drift
from scherlok.detector.severity import Severity
from scherlok.profiler.distribution import profile_distribution
from scherlok.profiler.freshness import profile_freshness
from scherlok.profiler.schema import profile_schema
from scherlok.profiler.volume import profile_volume
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
def investigate() -> None:
    """Profile all tables and store results.

    Example:
        scherlok investigate
    """
    connector = _get_connector_or_exit()
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

    console.print(f"[green]Investigation complete.[/green] {len(tables)} tables profiled.")


@app.command()
def watch() -> None:
    """Compare current state vs stored profile. Detect anomalies.

    Example:
        scherlok watch
    """
    connector = _get_connector_or_exit()
    store = ProfileStore()
    tables = connector.list_tables()
    all_anomalies: list[dict] = []

    console.print(f"Watching [bold]{len(tables)}[/bold] tables...")

    for table in tables:
        current_vol = profile_volume(connector, table)
        stored_vol = store.get_latest_profile(table, "volume")
        if stored_vol:
            anomalies = detect_volume_anomalies(table, current_vol, stored_vol)
            all_anomalies.extend(anomalies)

        current_sch = profile_schema(connector, table)
        stored_sch = store.get_latest_profile(table, "schema")
        if stored_sch:
            drifts = detect_schema_drift(table, current_sch, stored_sch)
            all_anomalies.extend(drifts)

        store.save_profile(table, "volume", current_vol)
        store.save_profile(table, "schema", current_sch)

    if all_anomalies:
        print_anomalies(all_anomalies)
    else:
        console.print("[green]No anomalies detected.[/green]")

    raise typer.Exit(code=exit_code_for(all_anomalies))


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

    for table in tables:
        vol = store.get_latest_profile(table, "volume")
        sch = store.get_latest_profile(table, "schema")
        fresh = store.get_latest_profile(table, "freshness")
        print_profile_summary(table, vol, sch, fresh)


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
        health = "[green]OK[/green]" if vol else "[yellow]Not profiled[/yellow]"

        tbl.add_row(table, row_count, col_count, str(last_profiled), health)

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
