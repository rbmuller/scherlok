"""New-table checks — problems visible on a table's first profile, with no baseline yet.

Every other detector compares against a stored profile, so a table seen for
the first time could not raise anything, even empty or with a column that is
NULL in every row. These checks read only the profiles the run has already
collected (no extra warehouse query) and run only while the table has no
baseline.
"""

from collections.abc import Iterable

from scherlok.detector.severity import Severity

EMPTY_TABLE = "empty_table"
ALL_NULL_COLUMN = "all_null_column"
DUPLICATE_KEY = "duplicate_key"
CONSTANT_COLUMN = "constant_column"

NEW_TABLE_ANOMALY_TYPES = frozenset({EMPTY_TABLE, ALL_NULL_COLUMN, DUPLICATE_KEY, CONSTANT_COLUMN})

# One distinct value is unremarkable on a handful of rows.
CONSTANT_COLUMN_MIN_ROWS = 100

KEY_COLUMN = "id"
KEY_COLUMN_SUFFIX = "_id"


def detect_new_table_anomalies(
    table: str,
    volume: dict,
    schema: dict,
    distributions: dict[str, dict],
) -> list[dict]:
    """Check a table seen for the first time for problems that need no baseline.

    ``volume``, ``schema`` and ``distributions`` (column name -> distribution
    profile) are the profiles this run already collected. Returns anomaly
    dicts with keys: table, type, message, severity.
    """
    row_count = volume.get("row_count")
    if row_count is None:
        return []
    if row_count == 0:
        return [_anomaly(table, EMPTY_TABLE, "New table is empty (0 rows)", Severity.WARNING)]

    key_columns = key_column_names(table)
    anomalies: list[dict] = []
    for column in schema.get("columns", []):
        name = column["name"]
        dist = distributions.get(name) or {}
        null_count = dist.get("null_count")
        distinct_count = dist.get("distinct_count")
        if null_count is None or distinct_count is None:
            continue

        # COUNT(DISTINCT) skips NULLs and both counts come from one query, so
        # this is exact; null_rate is rounded and reads 1.0 from 99.995% on.
        if null_count > 0 and distinct_count == 0:
            anomalies.append(_anomaly(
                table,
                ALL_NULL_COLUMN,
                f"Column '{name}' is NULL in all {null_count:,} rows",
                Severity.WARNING,
            ))
            continue

        non_null_rows = row_count - null_count
        if name.lower() in key_columns and distinct_count < non_null_rows:
            anomalies.append(_anomaly(
                table,
                DUPLICATE_KEY,
                f"Key column '{name}' has duplicates: {distinct_count:,} distinct values "
                f"in {non_null_rows:,} non-NULL rows",
                Severity.WARNING,
            ))
        if distinct_count == 1 and non_null_rows >= CONSTANT_COLUMN_MIN_ROWS:
            anomalies.append(_anomaly(
                table,
                CONSTANT_COLUMN,
                f"Column '{name}' holds one value in all {non_null_rows:,} non-NULL rows",
                Severity.INFO,
            ))
    return anomalies


def key_column_names(table: str) -> frozenset[str]:
    """Lower-cased names that mark a column as ``table``'s own key.

    ``id`` and ``<singular table>_id``: ``order_id`` in ``orders``, never a
    foreign key such as ``user_id``. The singular is guessed with English
    suffix rules (``categories`` -> ``category_id``, ``addresses`` ->
    ``address_id``) and the name itself is kept for singular table names. An
    irregular plural such as ``people`` is not matched by ``person_id``.
    """
    name = table.lower()
    singulars = {name}
    if name.endswith("ies"):
        singulars.add(name[:-3] + "y")
    if name.endswith("es"):
        singulars.add(name[:-2])
    if name.endswith("s") and not name.endswith("ss"):
        singulars.add(name[:-1])
    return frozenset({KEY_COLUMN, *(f"{singular}{KEY_COLUMN_SUFFIX}" for singular in singulars)})


def escalate_new_table_anomalies(anomalies: Iterable[dict]) -> list[dict]:
    """Raise WARNING new-table findings to CRITICAL, for a table the caller just built.

    ``scherlok verify --table`` names tables it expects to be correct now, so
    an empty table, an all-NULL column or a duplicate key there is a failure,
    not a hint. INFO findings (``constant_column``) and every other anomaly
    are returned unchanged.
    """
    return [
        {**anomaly, "severity": Severity.CRITICAL}
        if anomaly.get("type") in NEW_TABLE_ANOMALY_TYPES
        and anomaly.get("severity") == Severity.WARNING
        else anomaly
        for anomaly in anomalies
    ]


def _anomaly(table: str, anomaly_type: str, message: str, severity: Severity) -> dict:
    return {"table": table, "type": anomaly_type, "message": message, "severity": severity}
