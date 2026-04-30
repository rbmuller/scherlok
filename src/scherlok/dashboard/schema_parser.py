"""Parse `detect_schema_drift` anomaly messages into structured diff entries.

Path A from the spec — the existing detector returns stable human messages;
the dashboard extracts column name + types via small regexes. If a future
PR adds structured fields to the anomaly dict, swap this module out.
"""

from __future__ import annotations

import re
from dataclasses import dataclass

REMOVED_RE = re.compile(r"^Column '(?P<name>[^']+)' was removed$")
ADDED_RE = re.compile(
    r"^Column '(?P<name>[^']+)' was added \(type: (?P<type>[^)]+)\)$"
)
TYPE_CHANGED_RE = re.compile(
    r"^Column '(?P<name>[^']+)' type changed: (?P<old>.+?) -> (?P<new>.+)$"
)


@dataclass(frozen=True)
class SchemaDiffEntry:
    """One column-level entry in a schema diff."""

    kind: str  # "added" | "removed" | "changed"
    column: str
    old_type: str | None = None
    new_type: str | None = None


def parse_schema_anomaly(anomaly: dict) -> SchemaDiffEntry | None:
    """Parse one schema-drift anomaly dict into a SchemaDiffEntry, or None.

    Returns None for anomalies whose `type` is not one of the schema-drift
    types, or for messages that don't match the expected format.
    """
    atype = anomaly.get("type", "")
    msg = anomaly.get("message", "")

    if atype == "column_removed":
        m = REMOVED_RE.match(msg)
        if m:
            return SchemaDiffEntry(kind="removed", column=m["name"])
        return None

    if atype == "column_added":
        m = ADDED_RE.match(msg)
        if m:
            return SchemaDiffEntry(kind="added", column=m["name"], new_type=m["type"])
        return None

    if atype == "type_changed":
        m = TYPE_CHANGED_RE.match(msg)
        if m:
            return SchemaDiffEntry(
                kind="changed", column=m["name"], old_type=m["old"], new_type=m["new"]
            )
        return None

    return None


def is_schema_anomaly(anomaly: dict) -> bool:
    """True when the anomaly is one of the three schema-drift types."""
    return anomaly.get("type") in {"column_removed", "column_added", "type_changed"}
