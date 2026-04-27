"""Tests for the Snowflake connector using mocks."""

from datetime import datetime
from unittest.mock import MagicMock, patch

import pytest


class TestSnowflakeConnector:
    def test_parse_valid_connection_string(self):
        from scherlok.connectors.snowflake import SnowflakeConnector

        c = SnowflakeConnector("snowflake://myaccount/mydb/myschema")
        assert c._account == "myaccount"
        assert c._database == "mydb"
        assert c._schema == "myschema"

    def test_parse_invalid_connection_string(self):
        from scherlok.connectors.snowflake import SnowflakeConnector

        with pytest.raises(ValueError, match="snowflake://account/database/schema"):
            SnowflakeConnector("snowflake://only-account/database")

    @patch("scherlok.connectors.snowflake.SnowflakeConnector._query")
    def test_get_row_count(self, mock_query):
        from scherlok.connectors.snowflake import SnowflakeConnector

        mock_query.return_value = [{"cnt": 5000}]
        c = SnowflakeConnector("snowflake://acc/db/schema")
        c._conn = MagicMock()
        assert c.get_row_count("users") == 5000

    @patch("scherlok.connectors.snowflake.SnowflakeConnector._query")
    def test_list_tables(self, mock_query):
        from scherlok.connectors.snowflake import SnowflakeConnector

        mock_query.return_value = [
            {"table_name": "USERS"},
            {"table_name": "ORDERS"},
        ]
        c = SnowflakeConnector("snowflake://acc/db/schema")
        c._conn = MagicMock()
        tables = c.list_tables()
        # Result is lowercased
        assert tables == ["users", "orders"]

    @patch("scherlok.connectors.snowflake.SnowflakeConnector._query")
    def test_get_columns(self, mock_query):
        from scherlok.connectors.snowflake import SnowflakeConnector

        mock_query.return_value = [
            {"column_name": "ID", "data_type": "NUMBER", "is_nullable": "NO"},
            {"column_name": "NAME", "data_type": "VARCHAR", "is_nullable": "YES"},
        ]
        c = SnowflakeConnector("snowflake://acc/db/schema")
        c._conn = MagicMock()
        cols = c.get_columns("users")
        assert len(cols) == 2
        assert cols[0]["name"] == "id"
        assert cols[0]["type"] == "NUMBER"
        assert cols[0]["nullable"] is False
        assert cols[1]["nullable"] is True

    @patch("scherlok.connectors.snowflake.SnowflakeConnector._query")
    def test_get_column_stats(self, mock_query):
        from scherlok.connectors.snowflake import SnowflakeConnector

        mock_query.side_effect = [
            [{"null_count": 10, "distinct_count": 90}],
            [{"mean": 42.5, "stddev": 8.3, "min": "1", "max": "100"}],
            [{"val": "42", "cnt": 20}, {"val": "7", "cnt": 15}],
        ]
        c = SnowflakeConnector("snowflake://acc/db/schema")
        c._conn = MagicMock()
        stats = c.get_column_stats("users", "score")
        assert stats["null_count"] == 10
        assert stats["distinct_count"] == 90
        assert stats["mean"] == 42.5
        assert len(stats["top_values"]) == 2

    @patch("scherlok.connectors.snowflake.SnowflakeConnector._query")
    def test_get_last_modified(self, mock_query):
        from scherlok.connectors.snowflake import SnowflakeConnector

        ts = datetime(2026, 4, 20, 12, 0, 0)
        mock_query.return_value = [{"last_altered": ts}]
        c = SnowflakeConnector("snowflake://acc/db/schema")
        c._conn = MagicMock()
        result = c.get_last_modified("users")
        assert result == ts

    @patch("scherlok.connectors.snowflake.SnowflakeConnector._query")
    def test_get_last_modified_none(self, mock_query):
        from scherlok.connectors.snowflake import SnowflakeConnector

        mock_query.side_effect = Exception("not found")
        c = SnowflakeConnector("snowflake://acc/db/schema")
        c._conn = MagicMock()
        assert c.get_last_modified("users") is None
