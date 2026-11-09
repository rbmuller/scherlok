"""BigQuery connector using google-cloud-bigquery."""

from datetime import datetime, timezone
from typing import Any

from scherlok.connectors.base import TABLE_NAME_SEPARATOR, BaseConnector, RelationNamespace

_BACKSLASH = "\\"
_BACKTICK = "`"
_SINGLE_QUOTE = "'"

# Metadata of the connection's dataset, read by `get_columns` and `get_last_modified`.
_COLUMNS_VIEW = "INFORMATION_SCHEMA.COLUMNS"
_TABLES_METADATA = "__TABLES__"


def _escape(text: str, quote: str) -> str:
    """Escape ``quote`` and every backslash in ``text`` with a backslash.

    BigQuery reads the same escape sequences inside backticks (identifiers)
    and single quotes (strings), so a backslash is escaped first: otherwise
    a trailing backslash would escape the closing quote.
    """
    return text.replace(_BACKSLASH, _BACKSLASH * 2).replace(quote, _BACKSLASH + quote)


class BigQueryConnector(BaseConnector):
    """Connector for Google BigQuery.

    Connection string format:
        bigquery://project-id/dataset-name

    Requires google-cloud-bigquery:
        pip install scherlok[bigquery]

    Authentication via Application Default Credentials (ADC):
        gcloud auth application-default login
    """

    def __init__(self, connection_string: str) -> None:
        super().__init__(connection_string)
        self._client: Any = None
        self._project: str = ""
        self._dataset: str = ""
        self._parse_connection_string()

    def _parse_connection_string(self) -> None:
        """Parse bigquery://project-id/dataset-name into components."""
        path = self.connection_string.replace("bigquery://", "")
        parts = path.strip("/").split("/")
        if len(parts) < 2:
            raise ValueError(
                "BigQuery connection string must be: bigquery://project-id/dataset-name"
            )
        self._project = parts[0]
        self._dataset = parts[1]

    def connect(self) -> bool:
        """Validate connection to BigQuery."""
        try:
            from google.cloud import bigquery
        except ImportError:
            self._last_error = (
                "google-cloud-bigquery not installed\n"
                "  Hint: pip install scherlok[bigquery]"
            )
            return False

        try:
            self._client = bigquery.Client(project=self._project)
            # Validate by listing tables (lightweight call)
            dataset_ref = self._client.dataset(self._dataset)
            list(self._client.list_tables(dataset_ref, max_results=1))
            return True
        except Exception as exc:
            self._last_error = self._classify_error(str(exc))
            return False

    @staticmethod
    def _classify_error(message: str) -> str:
        """Map google-cloud-bigquery error text to a short, actionable hint."""
        msg = message.strip()
        lowered = msg.lower()
        if (
            "default credentials" in lowered
            or "could not automatically determine credentials" in lowered
        ):
            return (
                "Application Default Credentials not found\n"
                "  Hint: gcloud auth application-default login"
            )
        if "permission denied" in lowered or "forbidden" in lowered or "403" in lowered:
            return (
                "permission denied — the authenticated identity lacks access to this dataset\n"
                "  Hint: grant `roles/bigquery.dataViewer` and `roles/bigquery.jobUser`"
            )
        if "not found" in lowered and "dataset" in lowered:
            return (
                "dataset not found — check the dataset name and project\n"
                "  Hint: bigquery://<project>/<dataset>"
            )
        if "not found" in lowered and "project" in lowered:
            return "project not found — check the project ID in your connection string"
        if "billing" in lowered:
            return (
                "billing not enabled on the project\n"
                "  Hint: enable billing in the GCP console for this project"
            )
        first_line = msg.splitlines()[0] if msg else "unknown error"
        return first_line

    def _query(self, sql: str) -> list[dict]:
        """Execute a query and return results as list of dicts."""
        result = self._client.query(sql).result()
        return [dict(row) for row in result]

    @staticmethod
    def _quote_identifier(identifier: str) -> str:
        """Quote a BigQuery identifier in backticks.

        Inside backticks BigQuery reads the escape sequences of a string
        literal, so a backslash and a backtick are escaped with a backslash.
        """
        return f"{_BACKTICK}{_escape(identifier, _BACKTICK)}{_BACKTICK}"

    @staticmethod
    def _quote_string(value: str) -> str:
        """Return ``value`` as a single-quoted BigQuery string literal."""
        return f"{_SINGLE_QUOTE}{_escape(value, _SINGLE_QUOTE)}{_SINGLE_QUOTE}"

    def _dataset_metadata(self, view: str) -> str:
        """The metadata ``view`` (`_COLUMNS_VIEW`, `_TABLES_METADATA`) of the dataset."""
        dataset = self._quote_name_parts((self._project, self._dataset))
        return f"{dataset}{TABLE_NAME_SEPARATOR}{view}"

    def _quote_listed_table(self, table: str) -> str:
        """A listed table, in the connection's dataset."""
        return self._quote_dataset_table(table)

    def _quote_dataset_table(self, table: str) -> str:
        """``table``, one name as `list_tables` returns it, in the connection's dataset."""
        return self._quote_name_parts((self._project, self._dataset, table))

    def _table_name_parts(self, table: str) -> list[str]:
        """``table`` in the connection's dataset, ``dataset.table`` or ``project.dataset.table``."""
        parts = super()._table_name_parts(table)
        defaults = [self._project, self._dataset][: self.MAX_TABLE_NAME_PARTS - len(parts)]
        return [*defaults, *parts]

    def _fetch_one(self, sql: str) -> dict[str, Any]:
        """Run ``sql`` and return its single row keyed by column alias."""
        return self._query(sql)[0]

    def namespace(self) -> RelationNamespace:
        """The project and dataset of the connection string."""
        return RelationNamespace(database=self._project, schema=self._dataset)

    def list_tables(self) -> list[str]:
        """List all tables in the dataset."""
        dataset_ref = self._client.dataset(self._dataset)
        tables = self._client.list_tables(dataset_ref)
        return sorted(t.table_id for t in tables)

    def get_row_count(self, table: str) -> int:
        """Return row count for a table."""
        fqn = self._quote_dataset_table(table)
        rows = self._query(f"SELECT COUNT(*) AS cnt FROM {fqn}")
        return rows[0]["cnt"] if rows else 0

    def get_columns(self, table: str) -> list[dict]:
        """Return column metadata from INFORMATION_SCHEMA."""
        sql = (
            f"SELECT column_name, data_type, is_nullable "
            f"FROM {self._dataset_metadata(_COLUMNS_VIEW)} "
            f"WHERE table_name = {self._quote_string(table)} "
            f"ORDER BY ordinal_position"
        )
        rows = self._query(sql)
        return [
            {
                "name": r["column_name"],
                "type": r["data_type"],
                "nullable": r["is_nullable"] == "YES",
            }
            for r in rows
        ]

    def get_column_stats(self, table: str, column: str) -> dict:
        """Calculate statistics for a column."""
        fqn = self._quote_dataset_table(table)
        col = self._quote_identifier(column)
        stats: dict[str, Any] = {}

        # Null and distinct counts
        sql = (
            f"SELECT "
            f"  COUNTIF({col} IS NULL) AS null_count, "
            f"  COUNT(DISTINCT {col}) AS distinct_count "
            f"FROM {fqn}"
        )
        rows = self._query(sql)
        if rows:
            stats["null_count"] = rows[0]["null_count"]
            stats["distinct_count"] = rows[0]["distinct_count"]
        else:
            stats["null_count"] = 0
            stats["distinct_count"] = 0

        # Numeric stats
        try:
            sql = (
                f"SELECT "
                f"  AVG(CAST({col} AS FLOAT64)) AS mean, "
                f"  STDDEV(CAST({col} AS FLOAT64)) AS stddev "
                f"FROM {fqn}"
            )
            rows = self._query(sql)
            if rows:
                stats["mean"] = rows[0]["mean"]
                stats["stddev"] = rows[0]["stddev"]
            else:
                stats["mean"] = stats["stddev"] = None
        except Exception:
            stats["mean"] = stats["stddev"] = None

        return stats

    def get_last_modified(self, table: str) -> datetime | None:
        """Return last modification time from __TABLES__ metadata."""
        sql = (
            f"SELECT TIMESTAMP_MILLIS(last_modified_time) AS last_mod "
            f"FROM {self._dataset_metadata(_TABLES_METADATA)} "
            f"WHERE table_id = {self._quote_string(table)}"
        )
        try:
            rows = self._query(sql)
            if rows and rows[0]["last_mod"]:
                ts = rows[0]["last_mod"]
                if isinstance(ts, datetime):
                    return ts
                return datetime.fromisoformat(str(ts)).replace(tzinfo=timezone.utc)
        except Exception:
            pass
        return None
