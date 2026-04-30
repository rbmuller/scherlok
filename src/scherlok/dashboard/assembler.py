"""Build the dashboard view-model from a ProfileStore."""

from __future__ import annotations

from datetime import datetime, timezone

from scherlok import __version__
from scherlok.dashboard.grouping import Incident, group_anomalies
from scherlok.store.sqlite import ProfileStore

SPARKLINE_POINTS = 8
DEFAULT_HISTORY_DAYS = 14


def assemble_view(
    store: ProfileStore,
    days: int = DEFAULT_HISTORY_DAYS,
    project_name: str = "scherlok",
    connection_string: str = "",
    dbt_context: dict | None = None,
) -> dict:
    """Build the full view-model dict consumed by the Jinja template."""
    anomalies = store.get_anomaly_history(days=days)
    last_watch_iso = anomalies[0]["detected_at"] if anomalies else None

    # "Active" = the most recent batch of detections (same timestamp).
    # "History" = everything older within the window.
    active, history = _split_active_and_history(anomalies, last_watch_iso)
    incidents = group_anomalies(active)

    tables = _build_table_health(store, days, incidents)

    kpis = _kpis(tables, incidents, days)
    meta = _meta(
        project_name=project_name,
        connection_string=connection_string,
        last_watch_iso=last_watch_iso,
        days=days,
    )

    return {
        "meta": meta,
        "kpis": kpis,
        "dbt_context": dbt_context,
        "incidents": incidents,
        "tables": tables,
        "history": history,
    }


def _split_active_and_history(
    anomalies: list[dict],
    last_watch_iso: str | None,
) -> tuple[list[dict], list[dict]]:
    """Partition anomalies into 'active now' (latest run) and 'historical'."""
    if not last_watch_iso:
        return [], []
    active = [a for a in anomalies if a["detected_at"] == last_watch_iso]
    history = [a for a in anomalies if a["detected_at"] != last_watch_iso]
    return active, history


def _build_table_health(
    store: ProfileStore,
    days: int,
    incidents: list[Incident],
) -> list[dict]:
    """Return one health row per table that has a stored profile."""
    incident_by_table = {i.table: i for i in incidents}
    tables_with_profiles = _discover_tables(store)

    out: list[dict] = []
    for table in sorted(tables_with_profiles):
        vol = store.get_latest_profile(table, "volume")
        sch = store.get_latest_profile(table, "schema")
        if not vol:
            continue

        history_pts = _volume_sparkline_points(store, table, days)
        status = (
            incident_by_table[table].severity.lower()
            if table in incident_by_table
            else "healthy"
        )

        out.append({
            "name": table,
            "status": status,
            "rows": vol.get("row_count", 0),
            "cols": len(sch.get("columns", [])) if sch else 0,
            "last_profiled": _humanize_iso(vol.get("timestamp")),
            "sparkline_path": _sparkline_path(history_pts, status),
            "trend_points": history_pts,
        })
    return out


def _discover_tables(store: ProfileStore) -> set[str]:
    """Find every table that has at least one volume profile in the store."""
    rows = store._conn.execute(  # noqa: SLF001 — internal helper, fine for module
        "SELECT DISTINCT table_name FROM profiles WHERE profile_type = 'volume'"
    ).fetchall()
    return {row["table_name"] for row in rows}


def _volume_sparkline_points(store: ProfileStore, table: str, days: int) -> list[int]:
    """Return up to SPARKLINE_POINTS row_count values, oldest -> newest."""
    history = store.get_profile_history(table, "volume", days=days)
    history.reverse()  # store returns DESC; sparkline reads left-to-right oldest first
    counts = [int(h.get("row_count", 0)) for h in history]
    if len(counts) > SPARKLINE_POINTS:
        # Downsample to SPARKLINE_POINTS by even striding
        step = len(counts) / SPARKLINE_POINTS
        counts = [counts[int(i * step)] for i in range(SPARKLINE_POINTS)]
    return counts


def _sparkline_path(points: list[int], status: str) -> str:
    """Render a list of integers as an SVG path `d` attribute (100x20 viewbox)."""
    if not points or len(points) == 1:
        return "M0,10 L100,10"
    lo, hi = min(points), max(points)
    span = hi - lo or 1
    width = 100 / (len(points) - 1)
    coords = []
    for i, v in enumerate(points):
        x = i * width
        # Invert Y so larger values are higher on screen; pad 2px top/bottom
        y = 18 - ((v - lo) / span) * 16
        coords.append(f"{x:.1f},{y:.1f}")
    return "M" + " L".join(coords)


def _kpis(tables: list[dict], incidents: list[Incident], days: int) -> dict:
    critical = sum(1 for i in incidents if i.severity == "CRITICAL")
    warnings = sum(1 for i in incidents if i.severity == "WARNING")
    return {
        "tables_monitored": len(tables),
        "critical": critical,
        "warnings": warnings,
        "coverage_label": f"{len(tables)} baselined",
        "history_days": days,
    }


def _meta(
    project_name: str,
    connection_string: str,
    last_watch_iso: str | None,
    days: int,
) -> dict:
    now = datetime.now(timezone.utc)
    return {
        "project_name": project_name,
        "connection": _redact_connection(connection_string),
        "generated_at": now.strftime("%Y-%m-%d %H:%M:%S UTC"),
        "last_watch_iso": last_watch_iso,
        "last_watch_human": _humanize_iso(last_watch_iso),
        "baseline_window_days": days,
        "version": __version__,
    }


def _redact_connection(connection: str) -> str:
    """Mask password in URL-style connection strings for screenshot safety."""
    if "://" not in connection or "@" not in connection:
        return connection
    scheme, rest = connection.split("://", 1)
    if "@" not in rest:
        return connection
    creds, host = rest.split("@", 1)
    if ":" in creds:
        user, _ = creds.split(":", 1)
        return f"{scheme}://{user}:***@{host}"
    return connection


def _humanize_iso(iso: str | None) -> str:
    """Render an ISO timestamp as e.g. '2 min ago' / '3 hours ago' / fallback to date."""
    if not iso:
        return "—"
    try:
        ts = datetime.fromisoformat(iso.replace("Z", "+00:00"))
    except ValueError:
        return iso
    now = datetime.now(timezone.utc)
    if ts.tzinfo is None:
        ts = ts.replace(tzinfo=timezone.utc)
    delta = now - ts
    seconds = delta.total_seconds()
    if seconds < 60:
        return "just now"
    if seconds < 3600:
        mins = int(seconds // 60)
        return f"{mins} min ago"
    if seconds < 86400:
        hours = int(seconds // 3600)
        return f"{hours} hour{'s' if hours != 1 else ''} ago"
    days = int(seconds // 86400)
    if days < 14:
        return f"{days} day{'s' if days != 1 else ''} ago"
    return ts.strftime("%Y-%m-%d %H:%M")
