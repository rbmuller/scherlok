"""Table freshness profiling — last update detection."""

from collections.abc import Sequence
from datetime import date, datetime, time, timezone

from scherlok.connectors.base import BaseConnector

LAST_MODIFIED = "last_modified"
HOURS_SINCE_UPDATE = "hours_since_update"
# Profile keys saying where the age came from, so a run is only compared with
# a run that read the same thing.
FRESHNESS_SOURCE = "freshness_source"
FRESHNESS_COLUMN = "freshness_column"
SOURCE_METADATA = "metadata"
SOURCE_COLUMN = "column"

# Load and update timestamps, most telling first (#87). No other column is
# read: the age saved for a column gives its latest value away, and a name
# like `expires_at`, `deleted_at` or `last_login_at` says nothing about loads.
FRESHNESS_COLUMN_NAMES = (
    "updated_at",
    "modified_at",
    "dbt_updated_at",
    "loaded_at",
    "_loaded_at",
    "_etl_loaded_at",
    "synced_at",
    "_fivetran_synced",
    "_airbyte_extracted_at",
    "_sdc_batched_at",
    "ingested_at",
    "inserted_at",
    "created_at",
)
# Column types across the dialects: timestamp, timestamptz, TIMESTAMP_NTZ,
# datetime(6), DATE...
TEMPORAL_TYPE_PREFIXES = ("timestamp", "datetime")
DATE_TYPE = "date"

SECONDS_PER_HOUR = 3600

Reading = tuple[object, str, str | None]


def freshness_column(columns: Sequence[dict]) -> str | None:
    """Return the load-time column whose latest value dates the table, or None.

    The first name of ``FRESHNESS_COLUMN_NAMES`` the table has as a timestamp
    or date column, matched case-insensitively; the column's own spelling is
    returned.
    """
    by_lower: dict[str, str] = {}
    for column in columns:
        if _is_temporal(column.get("type")):
            by_lower.setdefault(column["name"].lower(), column["name"])
    return next((by_lower[name] for name in FRESHNESS_COLUMN_NAMES if name in by_lower), None)


def profile_freshness(
    connector: BaseConnector,
    table: str,
    columns: Sequence[dict] | None = None,
) -> dict:
    """Profile the freshness of a table.

    The age comes from the warehouse's last-modified metadata or from the
    latest value of a load-time column (``freshness_column``). A connector
    whose metadata is the time of the last write (``METADATA_IS_LAST_WRITE``)
    reads the metadata first and the column when there is none (#87); the
    others, whose metadata is a maintenance time or a cached value, read the
    column first and the metadata when the table has none (#139). ``columns``
    is the table's schema, when the caller already has it.

    A timestamp without a time zone is read as UTC; one in the future counts
    as just updated.

    Returns:
        Dict with keys: last_modified (str ISO or None), hours_since_update
        (float or None), freshness_source (``metadata``, ``column`` or None)
        and freshness_column (the column read, or None).
    """
    readers = (
        (_metadata_reading, _column_reading)
        if connector.METADATA_IS_LAST_WRITE
        else (_column_reading, _metadata_reading)
    )
    for read in readers:
        reading = read(connector, table, columns)
        if reading is None:
            continue
        value, source, column = reading
        last_mod = _as_utc_datetime(value)
        if last_mod is None:
            continue
        age = (datetime.now(timezone.utc) - last_mod).total_seconds() / SECONDS_PER_HOUR
        return _freshness(last_mod.isoformat(), max(0.0, round(age, 2)), source, column)

    return _freshness(None, None, None, None)


def _metadata_reading(
    connector: BaseConnector, table: str, columns: Sequence[dict] | None
) -> Reading | None:
    last_mod = connector.get_last_modified(table)
    return None if last_mod is None else (last_mod, SOURCE_METADATA, None)


def _column_reading(
    connector: BaseConnector, table: str, columns: Sequence[dict] | None
) -> Reading | None:
    column = freshness_column(connector.get_columns(table) if columns is None else columns)
    if column is None:
        return None
    try:
        latest = connector.get_latest_timestamp(table, column)
    except Exception:
        # Freshness is one reading among many: a column the warehouse won't
        # aggregate (a BigQuery table that requires a partition filter, a
        # missing driver module) falls back to the metadata instead of
        # stopping the run for every table.
        return None
    return latest, SOURCE_COLUMN, column


def _freshness(
    last_modified: str | None, hours: float | None, source: str | None, column: str | None
) -> dict:
    return {
        LAST_MODIFIED: last_modified,
        HOURS_SINCE_UPDATE: hours,
        FRESHNESS_SOURCE: source,
        FRESHNESS_COLUMN: column,
    }


def _is_temporal(column_type: object) -> bool:
    if not isinstance(column_type, str):
        return False
    normalized = column_type.strip().lower()
    return normalized.startswith(TEMPORAL_TYPE_PREFIXES) or normalized == DATE_TYPE


def _as_utc_datetime(value: object) -> datetime | None:
    """A driver's timestamp or date as an aware UTC datetime; None for anything else."""
    if isinstance(value, datetime):
        moment = value
    elif isinstance(value, date):
        moment = datetime.combine(value, time.min)
    elif isinstance(value, str):
        try:
            moment = datetime.fromisoformat(value)
        except ValueError:
            return None
    else:
        return None
    if moment.tzinfo is None:
        return moment.replace(tzinfo=timezone.utc)
    return moment.astimezone(timezone.utc)
