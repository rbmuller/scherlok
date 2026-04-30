"""Group raw anomalies into per-table incident view-models for the dashboard."""

from __future__ import annotations

from dataclasses import dataclass, field

from scherlok.dashboard.schema_parser import (
    SchemaDiffEntry,
    is_schema_anomaly,
    parse_schema_anomaly,
)

SEVERITY_RANK = {"CRITICAL": 3, "WARNING": 2, "INFO": 1}


@dataclass
class Symptom:
    """One observable inside an incident card."""

    severity: str  # CRITICAL | WARNING | INFO
    type: str
    message: str
    detected_at: str = ""  # ISO timestamp of detection, "" if unknown
    schema_diff: list[SchemaDiffEntry] = field(default_factory=list)


@dataclass
class Incident:
    """All active anomalies for one table, grouped together."""

    table: str
    severity: str  # max of symptoms
    symptoms: list[Symptom]
    summary: str  # auto-generated headline
    first_seen: str = ""  # earliest detected_at among symptoms
    last_seen: str = ""   # latest detected_at among symptoms


def group_anomalies(anomalies: list[dict]) -> list[Incident]:
    """Turn a flat list of anomaly dicts into per-table incident cards.

    Sort: CRITICAL incidents first, then WARNING, then INFO. Within the
    same severity bucket, alphabetical by table for stable output.
    """
    by_table: dict[str, list[dict]] = {}
    for a in anomalies:
        by_table.setdefault(a["table"], []).append(a)

    incidents: list[Incident] = []
    for table, table_anomalies in by_table.items():
        symptoms = _build_symptoms(table_anomalies)
        if not symptoms:
            continue
        severity = max(symptoms, key=lambda s: SEVERITY_RANK.get(s.severity, 0)).severity
        timestamps = [s.detected_at for s in symptoms if s.detected_at]
        incidents.append(
            Incident(
                table=table,
                severity=severity,
                symptoms=symptoms,
                summary=_summary_for(symptoms),
                first_seen=min(timestamps) if timestamps else "",
                last_seen=max(timestamps) if timestamps else "",
            )
        )

    incidents.sort(key=lambda i: (-SEVERITY_RANK.get(i.severity, 0), i.table))
    return incidents


def _build_symptoms(anomalies: list[dict]) -> list[Symptom]:
    """Convert raw anomaly dicts to Symptoms, merging schema-drift rows.

    All schema-drift anomalies for a table collapse into a single Symptom
    with a populated `schema_diff` list — that's the +/-/~ block.
    """
    symptoms: list[Symptom] = []
    schema_entries: list[SchemaDiffEntry] = []
    schema_first_detected: str = ""

    for a in anomalies:
        if is_schema_anomaly(a):
            entry = parse_schema_anomaly(a)
            if entry:
                schema_entries.append(entry)
                ts = a.get("detected_at", "")
                if ts and (not schema_first_detected or ts < schema_first_detected):
                    schema_first_detected = ts
            continue
        symptoms.append(
            Symptom(
                severity=_severity_str(a.get("severity")),
                type=a.get("type", ""),
                message=a.get("message", ""),
                detected_at=a.get("detected_at", ""),
            )
        )

    if schema_entries:
        symptoms.append(
            Symptom(
                severity="CRITICAL",  # schema_drift is always CRITICAL per detector
                type="schema_drift",
                message=_schema_diff_message(schema_entries),
                detected_at=schema_first_detected,
                schema_diff=sorted(schema_entries, key=lambda e: (e.kind, e.column)),
            )
        )

    return symptoms


def _severity_str(severity: object) -> str:
    """Coerce Severity enum or string to its string label."""
    if hasattr(severity, "value"):
        return str(severity.value)
    return str(severity) if severity is not None else "INFO"


def _schema_diff_message(entries: list[SchemaDiffEntry]) -> str:
    added = sum(1 for e in entries if e.kind == "added")
    removed = sum(1 for e in entries if e.kind == "removed")
    changed = sum(1 for e in entries if e.kind == "changed")
    parts = []
    if changed:
        parts.append(f"{changed} column{'s' if changed != 1 else ''} changed")
    if added:
        parts.append(f"{added} added")
    if removed:
        parts.append(f"{removed} removed")
    return "Schema drift — " + ", ".join(parts) if parts else "Schema drift"


def _summary_for(symptoms: list[Symptom]) -> str:
    """Pick a one-line headline that describes the incident shape."""
    types = {s.type for s in symptoms}
    if "schema_drift" in types and len(types) == 1:
        return "Schema drift detected — column type and/or set has changed"
    if "volume_drop" in types and "cardinality_change" in types:
        return "Volume drop with consistent cardinality decrease — check upstream"
    if "schema_drift" in types and len(types) > 1:
        return "Schema drift detected alongside data anomalies"
    if "volume_drop" in types:
        return "Volume drop detected"
    if "volume_spike" in types:
        return "Volume spike detected"
    if "freshness_miss" in types:
        return "Freshness SLA missed"
    return "Multiple anomalies detected"
