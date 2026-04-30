"""BigQuery connector using google-cloud-bigquery."""

from datetime import datetime, timezone
from typing import Any

from scherlok.connectors.base import BaseConnector


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

    def list_tables(self) -> list[str]:
        """List all tables in the dataset."""
        dataset_ref = self._client.dataset(self._dataset)
        tables = self._client.list_tables(dataset_ref)
        return sorted(t.table_id for t in tables)

    def get_row_count(self, table: str) -> int:
        """Return row count for a table."""
        fqn = f"`{self._project}.{self._dataset}.{table}`"
        rows = self._query(f"SELECT COUNT(*) AS cnt FROM {fqn}")
        return rows[0]["cnt"] if rows else 0

    def get_columns(self, table: str) -> list[dict]:
        """Return column metadata from INFORMATION_SCHEMA."""
        sql = (
            f"SELECT column_name, data_type, is_nullable "
            f"FROM `{self._project}.{self._dataset}.INFORMATION_SCHEMA.COLUMNS` "
            f"WHERE table_name = '{table}' "
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
        fqn = f"`{self._project}.{self._dataset}.{table}`"
        stats: dict[str, Any] = {}

        # Null and distinct counts
        sql = (
            f"SELECT "
            f"  COUNTIF(`{column}` IS NULL) AS null_count, "
            f"  COUNT(DISTINCT `{column}`) AS distinct_count "
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
                f"  AVG(CAST(`{column}` AS FLOAT64)) AS mean, "
                f"  STDDEV(CAST(`{column}` AS FLOAT64)) AS stddev, "
                f"  CAST(MIN(`{column}`) AS STRING) AS min, "
                f"  CAST(MAX(`{column}`) AS STRING) AS max "
                f"FROM {fqn}"
            )
            rows = self._query(sql)
            if rows:
                stats["mean"] = rows[0]["mean"]
                stats["stddev"] = rows[0]["stddev"]
                stats["min"] = rows[0]["min"]
                stats["max"] = rows[0]["max"]
            else:
                stats["mean"] = stats["stddev"] = stats["min"] = stats["max"] = None
        except Exception:
            stats["mean"] = stats["stddev"] = stats["min"] = stats["max"] = None

        # Top values
        try:
            sql = (
                f"SELECT CAST(`{column}` AS STRING) AS val, COUNT(*) AS cnt "
                f"FROM {fqn} "
                f"WHERE `{column}` IS NOT NULL "
                f"GROUP BY `{column}` ORDER BY cnt DESC LIMIT 5"
            )
            rows = self._query(sql)
            stats["top_values"] = [{"value": r["val"], "count": r["cnt"]} for r in rows]
        except Exception:
            stats["top_values"] = []

        return stats

    def get_last_modified(self, table: str) -> datetime | None:
        """Return last modification time from __TABLES__ metadata."""
        sql = (
            f"SELECT TIMESTAMP_MILLIS(last_modified_time) AS last_mod "
            f"FROM `{self._project}.{self._dataset}.__TABLES__` "
            f"WHERE table_id = '{table}'"
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
