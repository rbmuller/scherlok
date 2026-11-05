"""Core profile-and-detect orchestration, shared by the CLI and the MCP server.

This module is the single place that knows *how* to profile one table and
detect anomalies against the stored baseline. Both adapters — the Typer CLI
and the MCP server — call into here, so neither owns business logic the other
can't reach. Nothing in this module prints, formats, or knows about a
transport; it takes a connector + store and returns plain data.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any
from urllib.parse import SplitResult, parse_qsl, urlencode, urlsplit, urlunsplit

from scherlok.alerter.exitcode import FAIL_ON_CRITICAL, fails_gate, normalize_fail_on
from scherlok.connectors.base import BaseConnector
from scherlok.detector.adaptive import ADAPTIVE_HISTORY_LIMIT
from scherlok.detector.anomaly import detect_volume_anomalies
from scherlok.detector.anomaly_types import FRESHNESS_CRITICAL
from scherlok.detector.cardinality import detect_cardinality_anomalies
from scherlok.detector.distribution_shift import detect_distribution_shift
from scherlok.detector.fields import anomaly_details
from scherlok.detector.freshness import detect_freshness_anomalies
from scherlok.detector.new_table import (
    detect_new_table_anomalies,
    escalate_new_table_anomalies,
)
from scherlok.detector.nullability import detect_nullability_anomalies
from scherlok.detector.schema_drift import detect_schema_drift
from scherlok.detector.severity import Severity
from scherlok.profiler.distribution import profile_distribution
from scherlok.profiler.freshness import profile_freshness
from scherlok.profiler.schema import profile_schema
from scherlok.profiler.volume import profile_volume
from scherlok.store.sqlite import DISTRIBUTION_PROFILE_PREFIX, ProfileStore

VOLUME_PROFILE = "volume"
SCHEMA_PROFILE = "schema"
FRESHNESS_PROFILE = "freshness"

# A reading with an anomaly of this severity never becomes the baseline (#116).
# Learning it would make the broken state the reference and silence the alert,
# so the alert repeats on every run until the data recovers.
BASELINE_BLOCKING_SEVERITY = Severity.CRITICAL
# Freshness is the exception: it compares a table's age with its age on the
# previous run, so keeping the old reading would report a table updated less
# often than it is watched (a weekly table watched daily) on every run, and
# block learning its other profiles meanwhile.
NON_BLOCKING_ANOMALY_TYPES = frozenset({FRESHNESS_CRITICAL})

ANOMALIES_KEY = "anomalies"
TABLES_VERIFIED_KEY = "tables_verified"
TABLES_WITHOUT_BASELINE_KEY = "tables_without_baseline"

REDACTED = "***"
# Query parameters whose value is a credential, matched as part of the
# lower-cased key: `password`, `sslpassword`, `motherduck_token`, `api_key`.
SECRET_QUERY_KEY_PARTS = ("password", "passwd", "pwd", "token", "secret", "key")

# Spellings of one adapter that must share a scope.
SCOPE_SCHEME_ALIASES = {"postgres": "postgresql"}
# A port equal to the adapter's default is left out of the scope, so
# `host/db` and `host:5432/db` (as `scherlok dbt` writes it) are one database.
SCOPE_DEFAULT_PORTS = {"postgresql": 5432, "mysql": 3306}


class UnknownTablesError(ValueError):
    """Raised when requested tables are not visible on the connection."""


@dataclass
class TableReading:
    """One table's current profiles, collected before any of them is saved."""

    volume: dict
    schema: dict
    freshness: dict
    distributions: dict[str, dict] = field(default_factory=dict)

    def profiles(self) -> list[tuple[str, dict]]:
        """``(profile_type, data)`` pairs, in the order a run saves them."""
        return [
            *(
                (f"{DISTRIBUTION_PROFILE_PREFIX}{column}", profile)
                for column, profile in self.distributions.items()
            ),
            (VOLUME_PROFILE, self.volume),
            (SCHEMA_PROFILE, self.schema),
            (FRESHNESS_PROFILE, self.freshness),
        ]


def blocks_baseline(anomalies: list[dict]) -> bool:
    """True when a reading with these anomalies must not become the baseline (#116)."""
    return any(
        anomaly.get("severity") == BASELINE_BLOCKING_SEVERITY
        and anomaly.get("type") not in NON_BLOCKING_ANOMALY_TYPES
        for anomaly in anomalies
    )


def save_reading(store: ProfileStore, table: str, reading: TableReading) -> None:
    """Save every profile of ``reading``, making it the baseline of ``table``."""
    for profile_type, data in reading.profiles():
        store.save_profile(table, profile_type, data)


def profile_and_detect(
    connector: BaseConnector, store: ProfileStore, table: str, *, persist: bool = True
) -> tuple[list[dict], dict]:
    """Profile one table, detect anomalies against the stored baseline, save profiles.

    Returns ``(anomalies, current_volume)``. ``current_volume`` is returned
    separately because callers often want the row count inline (e.g. the CLI's
    dbt-style ✓/✗ line) without digging through the saved profile.

    The profiles saved on this run become the baseline for the next one,
    unless the run finds a CRITICAL anomaly on the table: then none of the
    table's profiles are saved, so the broken state never becomes the
    reference and the anomaly is reported again on every run until the data
    recovers or the table is re-profiled (#116). WARNING and INFO findings
    are learned.

    With ``persist=False`` nothing is saved: the current state is compared with
    the baseline and discarded. A gate needs this, because a saved failing run
    becomes the next baseline and a simple retry would then pass.
    """
    anomalies, reading = inspect_table(connector, store, table)
    if persist and not blocks_baseline(anomalies):
        save_reading(store, table, reading)
    return anomalies, reading.volume


def inspect_table(
    connector: BaseConnector, store: ProfileStore, table: str
) -> tuple[list[dict], TableReading]:
    """Profile one table and detect anomalies against its stored baseline. Saves nothing.

    Returns ``(anomalies, reading)``; ``save_reading`` makes the reading the
    baseline. The first time a table is seen there is no baseline to compare
    against, so only the new-table checks run (empty table, all-NULL column,
    duplicate key, constant column), on the profiles already collected.
    """
    anomalies: list[dict] = []

    current_vol = profile_volume(connector, table)
    volume_history = store.get_profile_history(
        table, VOLUME_PROFILE, days=None, limit=ADAPTIVE_HISTORY_LIMIT
    )
    stored_vol = store.get_latest_profile(table, VOLUME_PROFILE)
    if stored_vol:
        anomalies.extend(
            detect_volume_anomalies(table, current_vol, stored_vol, history=volume_history)
        )

    current_sch = profile_schema(connector, table)
    stored_sch = store.get_latest_profile(table, SCHEMA_PROFILE)
    if stored_sch:
        anomalies.extend(detect_schema_drift(table, current_sch, stored_sch))

    current_fresh = profile_freshness(connector, table)
    stored_fresh = store.get_latest_profile(table, FRESHNESS_PROFILE)
    if stored_fresh:
        anomalies.extend(detect_freshness_anomalies(table, current_fresh, stored_fresh))

    current_dists: dict[str, dict] = {}
    for col in (current_sch or {}).get("columns", []):
        col_name = col["name"]
        current_dist = profile_distribution(connector, table, col_name)
        current_dists[col_name] = current_dist
        distribution_history = store.get_profile_history(
            table,
            f"distribution:{col_name}",
            days=None,
            limit=ADAPTIVE_HISTORY_LIMIT,
        )
        stored_dist = store.get_latest_profile(table, f"distribution:{col_name}")
        if stored_dist:
            anomalies.extend(
                detect_nullability_anomalies(
                    table, col_name, current_dist, stored_dist, history=distribution_history
                )
            )
            anomalies.extend(
                detect_distribution_shift(
                    table, col_name, current_dist, stored_dist, history=distribution_history
                )
            )
            anomalies.extend(
                detect_cardinality_anomalies(
                    table,
                    col_name,
                    current_dist,
                    stored_dist,
                    current_vol,
                    stored_vol,
                    history=distribution_history,
                )
            )

    if stored_vol is None:
        anomalies.extend(
            detect_new_table_anomalies(table, current_vol, current_sch or {}, current_dists)
        )

    return anomalies, TableReading(
        volume=current_vol,
        schema=current_sch,
        freshness=current_fresh,
        distributions=current_dists,
    )


def has_baseline(store: ProfileStore, table: str) -> bool:
    """True when the store holds a profile to compare ``table`` against.

    Every profiling run saves a volume profile, so its presence marks a baseline.
    """
    return store.get_latest_profile(table, VOLUME_PROFILE) is not None


def select_tables(requested: list[str] | None, visible: list[str]) -> list[str]:
    """Resolve the requested table subset against what the connection can see.

    ``None`` or an empty list means every visible table. Unknown names raise
    rather than silently checking nothing, so the caller learns it asked for a
    table that isn't there. A name given twice is checked once.
    """
    if not requested:
        return visible
    visible_set = set(visible)
    missing = [t for t in requested if t not in visible_set]
    if missing:
        raise UnknownTablesError(f"Tables not found on this connection: {missing}")
    return list(dict.fromkeys(requested))


@dataclass
class VerificationResult:
    """Outcome of comparing tables with their stored baseline, with nothing saved.

    Plain data for the CLI (text or JSON) and the MCP server to render.
    ``tables_without_baseline`` are profiled but never counted as verified.
    """

    fail_on: str
    tables_verified: list[str] = field(default_factory=list)
    tables_without_baseline: list[str] = field(default_factory=list)
    anomalies: list[dict] = field(default_factory=list)

    @property
    def passed(self) -> bool:
        return not fails_gate(self.anomalies, self.fail_on)

    @property
    def tables_checked(self) -> list[str]:
        """Every table the verification profiled, with or without a baseline."""
        return [*self.tables_verified, *self.tables_without_baseline]

    def to_dict(self) -> dict[str, Any]:
        """JSON-safe form shared by `scherlok verify --output json` and MCP `verify`."""
        return {
            "passed": self.passed,
            "fail_on": self.fail_on,
            TABLES_VERIFIED_KEY: list(self.tables_verified),
            TABLES_WITHOUT_BASELINE_KEY: list(self.tables_without_baseline),
            "anomaly_count": len(self.anomalies),
            **severity_counts(self.anomalies),
            ANOMALIES_KEY: [anomaly_to_dict(a) for a in self.anomalies],
        }


def verify_tables(
    connector: BaseConnector,
    store: ProfileStore,
    tables: list[str] | None = None,
    fail_on: str = FAIL_ON_CRITICAL,
) -> VerificationResult:
    """Compare tables with their stored baseline and decide the gate. Saves nothing.

    ``tables`` limits the run to those names (unknown names raise
    ``UnknownTablesError``); ``None`` verifies every visible table. A table
    without a baseline still gets the new-table checks, but it is listed apart
    and never counted as verified. Naming it in ``tables`` says it was just
    built, so its WARNING new-table findings (empty table, all-NULL column,
    duplicate key) count as CRITICAL. Running this twice on the same data
    returns the same result.
    """
    result = VerificationResult(fail_on=normalize_fail_on(fail_on))
    for table in select_tables(tables, connector.list_tables()):
        if has_baseline(store, table):
            result.tables_verified.append(table)
        else:
            result.tables_without_baseline.append(table)
        anomalies, _ = profile_and_detect(connector, store, table, persist=False)
        if tables:
            anomalies = escalate_new_table_anomalies(anomalies)
        result.anomalies.extend(anomalies)
    return result


def redact_connection(connection_string: str) -> str:
    """Mask the credentials in a connection string for safe display.

    ``postgresql://alice:secret@host/db`` -> ``postgresql://alice:***@host/db``,
    and the value of a query parameter such as ``password`` or ``token`` is
    masked too. When an ``@`` follows the host, an unencoded ``/``, ``?`` or
    ``#`` in the password may have cut the host short, leaving part of the
    password after it, so everything up to the last ``@`` is masked. Returns
    the input unchanged when there's nothing to hide.
    """
    if not connection_string or "://" not in connection_string:
        return connection_string
    parts = urlsplit(connection_string)
    if _at_sign_after_host(parts):
        scheme, rest = connection_string.split("://", 1)
        return f"{scheme}://{REDACTED}@{rest.rsplit('@', 1)[1]}"
    netloc = parts.netloc
    if parts.password:
        host = parts.hostname or ""
        if parts.port:
            host = f"{host}:{parts.port}"
        userinfo = f"{parts.username}:{REDACTED}" if parts.username else REDACTED
        netloc = f"{userinfo}@{host}"
    query = _redact_query(parts.query)
    if netloc == parts.netloc and query == parts.query:
        return connection_string
    return urlunsplit((parts.scheme, netloc, parts.path, query, parts.fragment))


def _at_sign_after_host(parts: SplitResult) -> bool:
    """True when an ``@`` follows the host, so the parsed host can't be trusted."""
    return bool(parts.netloc) and any(
        "@" in piece for piece in (parts.path, parts.query, parts.fragment)
    )


def _redact_query(query: str) -> str:
    """Mask the values of credential parameters, keeping the others readable."""
    pairs = parse_qsl(query, keep_blank_values=True)
    if not any(_is_secret_query_key(key) for key, _ in pairs):
        return query
    masked = [(key, REDACTED if _is_secret_query_key(key) else value) for key, value in pairs]
    return urlencode(masked, safe="*")


def _is_secret_query_key(key: str) -> bool:
    lowered = key.lower()
    return any(part in lowered for part in SECRET_QUERY_KEY_PARTS)


def connection_scope(connection_string: str | None) -> str | None:
    """Name the database a connection points at, so its baselines stay apart.

    Built only from the adapter, host, port and path (database, schema,
    dataset or DuckDB file). The user, password, query string and fragment,
    where tokens and keys travel, are never copied. The adapter and host are
    lower-cased, ``postgres`` becomes ``postgresql`` and a default port is
    dropped, so spellings of one database share a scope:
    ``postgresql://alice:secret@DB.example.com:5432/shop?sslmode=require``
    -> ``postgresql://db.example.com/shop``.

    Returns None, unscoped as before scopes existed, when the string is not a
    URL or when an ``@`` follows the host. A password holding an unencoded
    ``/``, ``?`` or ``#`` ends the host early and leaves the rest of the
    password after it, so the parsed host can't be trusted to hold no secret.
    """
    if not connection_string or "://" not in connection_string:
        return None
    parts = urlsplit(connection_string)
    if _at_sign_after_host(parts):
        return None
    try:
        port = parts.port
    except ValueError:  # not a number, so the host was cut short too
        return None
    scheme = parts.scheme.lower()
    scheme = SCOPE_SCHEME_ALIASES.get(scheme, scheme)
    host = parts.hostname or ""
    if ":" in host:  # an IPv6 literal keeps its brackets
        host = f"[{host}]"
    if port is not None and port != SCOPE_DEFAULT_PORTS.get(scheme):
        host = f"{host}:{port}"
    return f"{scheme}://{host}{parts.path.rstrip('/')}"


def anomaly_to_dict(anomaly: dict) -> dict[str, Any]:
    """Normalize one anomaly into a JSON-safe dict for structured transports.

    The detector layer stores ``severity`` as a ``Severity`` enum; serialize it
    to its bare name (``"CRITICAL"``) so MCP/JSON consumers get a stable string
    rather than ``"Severity.CRITICAL"``. The structured fields (``column``,
    ``metric``, ``baseline``, ``current``, ``change_pct``, ``hint``) follow,
    each None when it does not apply.
    """
    return {
        "table": anomaly.get("table"),
        "type": anomaly.get("type"),
        "severity": _severity_name(anomaly.get("severity", "")),
        "message": anomaly.get("message"),
        **anomaly_details(anomaly),
    }


def severity_counts(anomalies: list[dict]) -> dict[str, int]:
    """Count anomalies per severity, keyed ``critical``, ``warning`` and ``info``."""
    counts = {severity.value.lower(): 0 for severity in Severity}
    for anomaly in anomalies:
        name = _severity_name(anomaly.get("severity", "")).lower()
        if name in counts:
            counts[name] += 1
    return counts


def _severity_name(severity: object) -> str:
    """Bare severity name (``"CRITICAL"``) for an enum member or a stored string."""
    return str(severity).rsplit(".", 1)[-1]
