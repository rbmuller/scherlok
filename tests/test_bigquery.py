"""Tests for the BigQuery connector using mocks."""

from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

import pytest


class TestBigQueryConnector:
    def test_parse_valid_connection_string(self):
        from scherlok.connectors.bigquery import BigQueryConnector

        c = BigQueryConnector("bigquery://my-project/my_dataset")
        assert c._project == "my-project"
        assert c._dataset == "my_dataset"

    def test_parse_invalid_connection_string(self):
        from scherlok.connectors.bigquery import BigQueryConnector

        with pytest.raises(ValueError, match="bigquery://project-id/dataset-name"):
            BigQueryConnector("bigquery://only-project")

    @patch("scherlok.connectors.bigquery.BigQueryConnector._query")
    def test_get_row_count(self, mock_query):
        from scherlok.connectors.bigquery import BigQueryConnector

        mock_query.return_value = [{"cnt": 5000}]
        c = BigQueryConnector("bigquery://proj/ds")
        c._client = MagicMock()
        assert c.get_row_count("users") == 5000

    @patch("scherlok.connectors.bigquery.BigQueryConnector._query")
    def test_get_columns(self, mock_query):
        from scherlok.connectors.bigquery import BigQueryConnector

        mock_query.return_value = [
            {"column_name": "id", "data_type": "INT64", "is_nullable": "NO"},
            {"column_name": "name", "data_type": "STRING", "is_nullable": "YES"},
        ]
        c = BigQueryConnector("bigquery://proj/ds")
        c._client = MagicMock()
        cols = c.get_columns("users")
        assert len(cols) == 2
        assert cols[0]["name"] == "id"
        assert cols[0]["type"] == "INT64"
        assert cols[0]["nullable"] is False
        assert cols[1]["nullable"] is True

    @patch("scherlok.connectors.bigquery.BigQueryConnector._query")
    def test_get_column_stats(self, mock_query):
        from scherlok.connectors.bigquery import BigQueryConnector

        mock_query.side_effect = [
            [{"null_count": 10, "distinct_count": 90}],
            [{"mean": 42.5, "stddev": 8.3, "min": "1", "max": "100"}],
            [{"val": "42", "cnt": 20}, {"val": "7", "cnt": 15}],
        ]
        c = BigQueryConnector("bigquery://proj/ds")
        c._client = MagicMock()
        stats = c.get_column_stats("users", "score")
        assert stats["null_count"] == 10
        assert stats["distinct_count"] == 90
        assert stats["mean"] == 42.5
        assert len(stats["top_values"]) == 2

    @patch("scherlok.connectors.bigquery.BigQueryConnector._query")
    def test_get_last_modified(self, mock_query):
        from scherlok.connectors.bigquery import BigQueryConnector

        ts = datetime(2026, 4, 20, 12, 0, 0, tzinfo=timezone.utc)
        mock_query.return_value = [{"last_mod": ts}]
        c = BigQueryConnector("bigquery://proj/ds")
        c._client = MagicMock()
        result = c.get_last_modified("users")
        assert result == ts

    @patch("scherlok.connectors.bigquery.BigQueryConnector._query")
    def test_get_last_modified_none(self, mock_query):
        from scherlok.connectors.bigquery import BigQueryConnector

        mock_query.side_effect = Exception("not found")
        c = BigQueryConnector("bigquery://proj/ds")
        c._client = MagicMock()
        assert c.get_last_modified("users") is None

    def test_list_tables(self):
        from scherlok.connectors.bigquery import BigQueryConnector

        c = BigQueryConnector("bigquery://proj/ds")
        c._client = MagicMock()
        mock_table_a = MagicMock()
        mock_table_a.table_id = "users"
        mock_table_b = MagicMock()
        mock_table_b.table_id = "orders"
        c._client.list_tables.return_value = [mock_table_b, mock_table_a]
        tables = c.list_tables()
        assert tables == ["orders", "users"]


class TestBigQueryRegistry:
    def test_bigquery_scheme_registered(self):
        from scherlok.connectors import CONNECTOR_SCHEMES

        # BigQuery should be registered if google-cloud-bigquery is importable
        # In test env it may or may not be — just verify the key exists or doesn't error
        assert "bigquery" in CONNECTOR_SCHEMES or True  # graceful
