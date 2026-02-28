"""Tests for streamlit_app.db module."""

from __future__ import annotations

from unittest.mock import MagicMock, call, patch

import psycopg2
import pytest

from streamlit_app.db import execute_query, validate_table_name


class TestValidateTableName:
    """Test table name validation."""

    def test_valid_name(self) -> None:
        assert validate_table_name("fct_shots_synced") == "fct_shots_synced"

    def test_valid_underscore_prefix(self) -> None:
        assert validate_table_name("_internal") == "_internal"

    def test_rejects_semicolon(self) -> None:
        with pytest.raises(ValueError, match="Invalid table name"):
            validate_table_name("shots; DROP TABLE--")

    def test_rejects_space(self) -> None:
        with pytest.raises(ValueError, match="Invalid table name"):
            validate_table_name("shots table")

    def test_rejects_empty(self) -> None:
        with pytest.raises(ValueError, match="Invalid table name"):
            validate_table_name("")

    def test_rejects_leading_number(self) -> None:
        with pytest.raises(ValueError, match="Invalid table name"):
            validate_table_name("1shots")


class TestExecuteQuery:
    """Test parameterized query execution with mocked connection pool."""

    @patch("streamlit_app.db._get_pool")
    def test_returns_dataframe(self, mock_get_pool: MagicMock) -> None:
        mock_pool = MagicMock()
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.fetchall.return_value = [{"id": 1, "name": "test"}]
        mock_conn.cursor.return_value.__enter__ = MagicMock(return_value=mock_cursor)
        mock_conn.cursor.return_value.__exit__ = MagicMock(return_value=False)
        mock_pool.getconn.return_value = mock_conn
        mock_get_pool.return_value = mock_pool

        result = execute_query("SELECT id, name FROM t WHERE id = %s", (1,))
        assert len(result) == 1
        assert result.iloc[0]["id"] == 1
        mock_cursor.execute.assert_called_once_with("SELECT id, name FROM t WHERE id = %s", (1,))

    @patch("streamlit_app.db._get_pool")
    def test_returns_empty_dataframe_on_no_results(self, mock_get_pool: MagicMock) -> None:
        mock_pool = MagicMock()
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.fetchall.return_value = []
        mock_conn.cursor.return_value.__enter__ = MagicMock(return_value=mock_cursor)
        mock_conn.cursor.return_value.__exit__ = MagicMock(return_value=False)
        mock_pool.getconn.return_value = mock_conn
        mock_get_pool.return_value = mock_pool

        result = execute_query("SELECT * FROM t WHERE 1=0")
        assert result.empty

    @patch("streamlit_app.db._get_pool")
    def test_psycopg2_error_sanitized(self, mock_get_pool: MagicMock) -> None:
        """Non-retryable psycopg2.Error is caught and re-raised as a sanitized RuntimeError."""
        mock_pool = MagicMock()
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.execute.side_effect = psycopg2.ProgrammingError("syntax error")
        mock_conn.cursor.return_value.__enter__ = MagicMock(return_value=mock_cursor)
        mock_conn.cursor.return_value.__exit__ = MagicMock(return_value=False)
        mock_pool.getconn.return_value = mock_conn
        mock_get_pool.return_value = mock_pool

        with pytest.raises(RuntimeError, match="Database query failed"):
            execute_query("SELECT * FROM t")
        mock_pool.putconn.assert_called_once_with(mock_conn, close=True)

    @patch("streamlit_app.db._get_pool")
    def test_non_psycopg2_error_propagates(self, mock_get_pool: MagicMock) -> None:
        """Non-psycopg2 errors propagate unchanged but still return the connection."""
        mock_pool = MagicMock()
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.execute.side_effect = RuntimeError("unexpected")
        mock_conn.cursor.return_value.__enter__ = MagicMock(return_value=mock_cursor)
        mock_conn.cursor.return_value.__exit__ = MagicMock(return_value=False)
        mock_pool.getconn.return_value = mock_conn
        mock_get_pool.return_value = mock_pool

        with pytest.raises(RuntimeError, match="unexpected"):
            execute_query("SELECT * FROM t")
        mock_pool.putconn.assert_called_once_with(mock_conn)

    @patch("streamlit_app.db._get_pool")
    def test_connection_returned_on_success(self, mock_get_pool: MagicMock) -> None:
        mock_pool = MagicMock()
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.fetchall.return_value = [{"x": 1}]
        mock_conn.cursor.return_value.__enter__ = MagicMock(return_value=mock_cursor)
        mock_conn.cursor.return_value.__exit__ = MagicMock(return_value=False)
        mock_pool.getconn.return_value = mock_conn
        mock_get_pool.return_value = mock_pool

        execute_query("SELECT 1")
        mock_pool.putconn.assert_called_once_with(mock_conn)


class TestScaleToZeroRetry:
    """Test retry logic for Lakebase Autoscaling scale-to-zero wake-up."""

    @patch("streamlit_app.db.time.sleep")
    @patch("streamlit_app.db._get_pool")
    def test_retry_then_succeed(self, mock_get_pool: MagicMock, mock_sleep: MagicMock) -> None:
        """First attempt fails with OperationalError, second succeeds."""
        mock_pool = MagicMock()
        mock_conn_good = MagicMock()
        mock_cursor_good = MagicMock()
        mock_cursor_good.fetchall.return_value = [{"id": 1}]
        mock_conn_good.cursor.return_value.__enter__ = MagicMock(return_value=mock_cursor_good)
        mock_conn_good.cursor.return_value.__exit__ = MagicMock(return_value=False)

        # First getconn raises OperationalError, second succeeds
        mock_pool.getconn.side_effect = [
            psycopg2.OperationalError("endpoint is waking up"),
            mock_conn_good,
        ]
        mock_get_pool.return_value = mock_pool

        result = execute_query("SELECT id FROM t")
        assert len(result) == 1
        mock_sleep.assert_called_once_with(3)

    @patch("streamlit_app.db.time.sleep")
    @patch("streamlit_app.db._get_pool")
    def test_exhausted_retries(self, mock_get_pool: MagicMock, mock_sleep: MagicMock) -> None:
        """All retry attempts fail — raises RuntimeError."""
        mock_pool = MagicMock()
        mock_pool.getconn.side_effect = psycopg2.OperationalError("endpoint suspended")
        mock_get_pool.return_value = mock_pool

        with pytest.raises(RuntimeError, match="Database query failed"):
            execute_query("SELECT 1")

        assert mock_sleep.call_count == 2  # 3 attempts, 2 sleeps between them
        mock_sleep.assert_has_calls([call(3), call(3)])
