"""Rich terminal output for anomalies and profiles."""

from rich.console import Console
from rich.table import Table

from scherlok.detector.severity import Severity

console = Console()

SEVERITY_STYLES: dict[Severity, str] = {
    Severity.INFO: "blue",
    Severity.WARNING: "yellow",
    Severity.CRITICAL: "red bold",
}


def print_anomalies(anomalies: list[dict]) -> None:
    """Print detected anomalies as a Rich table."""
    tbl = Table(title="Detected Anomalies")
    tbl.add_column("Severity", style="bold")
    tbl.add_column("Table", style="cyan")
    tbl.add_column("Type")
    tbl.add_column("Message")

    for a in sorted(anomalies, key=lambda x: list(Severity).index(x["severity"])):
        severity = a["severity"]
        style = SEVERITY_STYLES.get(severity, "white")
        tbl.add_row(
            f"[{style}]{severity.value}[/{style}]",
            a["table"],
            a["type"],
            a["message"],
        )

    console.print(tbl)


def print_profile_summary(
    table: str,
    volume: dict | None,
    schema: dict | None,
    freshness: dict | None,
    anomaly_count: int = 0,
) -> None:
    """Print a profile summary for a single table."""
    console.print(f"\n[bold cyan]{table}[/bold cyan]")

    if volume:
        console.print(f"  Rows: {volume['row_count']:,}")
        console.print(f"  Last profiled: {volume.get('timestamp', '—')}")

    if schema:
        cols = schema.get("columns", [])
        console.print(f"  Columns: {len(cols)}")
        for col in cols[:10]:
            nullable = "nullable" if col.get("nullable") else "not null"
            console.print(f"    - {col['name']} ({col['type']}, {nullable})")
        if len(cols) > 10:
            console.print(f"    ... and {len(cols) - 10} more")

    if freshness:
        last_mod = freshness.get("last_modified", "unknown")
        hours = freshness.get("hours_since_update")
        hours_str = f"{hours:.1f}h ago" if hours is not None else "unknown"
        console.print(f"  Freshness: {hours_str} (last: {last_mod})")

    if anomaly_count > 0:
        console.print(
            f"  Anomalies: [red]{anomaly_count} detected[/red] "
            "(run [bold]scherlok history[/bold] for details)"
        )
    else:
        console.print("  Anomalies: [green]0[/green]")
