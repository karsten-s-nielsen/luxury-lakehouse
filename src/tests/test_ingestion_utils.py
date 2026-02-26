"""Tests for ingestion.utils — CLI validation, HTTP client, logging, and validation."""

from __future__ import annotations

import json
import logging
from unittest.mock import MagicMock, patch

import pytest
import requests
from ingestion.utils import (
    configure_logging,
    fetch_url,
    parse_ingestion_args,
    validate_dataframe,
    write_delta_table,
)

# ---------------------------------------------------------------------------
# CLI Argument Parsing
# ---------------------------------------------------------------------------


class TestParseIngestionArgs:
    """Tests for parse_ingestion_args."""

    def test_valid_args(self) -> None:
        with patch("sys.argv", ["prog", "--catalog", "soccer_analytics", "--schema", "bronze"]):
            args = parse_ingestion_args("test")
            assert args.catalog == "soccer_analytics"
            assert args.schema == "bronze"

    def test_rejects_sql_injection_catalog(self) -> None:
        with patch("sys.argv", ["prog", "--catalog", "DROP TABLE;--", "--schema", "bronze"]):
            with pytest.raises(SystemExit):
                parse_ingestion_args("test")

    def test_rejects_sql_injection_schema(self) -> None:
        with patch("sys.argv", ["prog", "--catalog", "valid", "--schema", "'; DROP TABLE--"]):
            with pytest.raises(SystemExit):
                parse_ingestion_args("test")

    def test_rejects_spaces(self) -> None:
        with patch("sys.argv", ["prog", "--catalog", "has spaces", "--schema", "bronze"]):
            with pytest.raises(SystemExit):
                parse_ingestion_args("test")

    def test_rejects_numeric_start(self) -> None:
        with patch("sys.argv", ["prog", "--catalog", "123abc", "--schema", "bronze"]):
            with pytest.raises(SystemExit):
                parse_ingestion_args("test")

    def test_allows_underscores(self) -> None:
        with patch("sys.argv", ["prog", "--catalog", "_my_catalog", "--schema", "my_schema_01"]):
            args = parse_ingestion_args("test")
            assert args.catalog == "_my_catalog"
            assert args.schema == "my_schema_01"


# ---------------------------------------------------------------------------
# Structured Logging
# ---------------------------------------------------------------------------


class TestConfigureLogging:
    """Tests for configure_logging."""

    def test_returns_named_logger(self) -> None:
        logger = configure_logging("test_source")
        assert logger.name == "ingestion.test_source"
        assert logger.level == logging.INFO

    def test_json_format(self) -> None:
        logger = configure_logging("json_test")
        handler = logger.handlers[0]
        record = logging.LogRecord(
            name="ingestion.json_test",
            level=logging.INFO,
            pathname="",
            lineno=0,
            msg="Test message",
            args=(),
            exc_info=None,
        )
        formatted = handler.formatter.format(record)  # type: ignore[union-attr]
        parsed = json.loads(formatted)
        assert parsed["level"] == "INFO"
        assert parsed["source"] == "ingestion.json_test"
        assert parsed["message"] == "Test message"
        assert "timestamp" in parsed

    def test_no_duplicate_handlers(self) -> None:
        logger = configure_logging("dedup_test")
        initial_count = len(logger.handlers)
        configure_logging("dedup_test")
        assert len(logger.handlers) == initial_count


# ---------------------------------------------------------------------------
# HTTP Client
# ---------------------------------------------------------------------------


class TestFetchUrl:
    """Tests for fetch_url."""

    def test_rejects_http_url(self) -> None:
        with pytest.raises(ValueError, match="Only HTTPS URLs are allowed"):
            fetch_url("http://example.com/data.json")

    def test_rejects_ftp_url(self) -> None:
        with pytest.raises(ValueError, match="Only HTTPS URLs are allowed"):
            fetch_url("ftp://example.com/data.json")

    @patch("ingestion.utils.requests.get")
    def test_successful_request(self, mock_get: MagicMock) -> None:
        mock_response = MagicMock(spec=requests.Response)
        mock_response.status_code = 200
        mock_get.return_value = mock_response

        result = fetch_url("https://example.com/data.json")
        assert result == mock_response
        mock_get.assert_called_once_with("https://example.com/data.json", timeout=(10, 30), verify=True)

    @patch("ingestion.utils.time.sleep")
    @patch("ingestion.utils.requests.get")
    def test_retries_on_429(self, mock_get: MagicMock, mock_sleep: MagicMock) -> None:
        mock_429 = MagicMock(spec=requests.Response)
        mock_429.status_code = 429
        mock_200 = MagicMock(spec=requests.Response)
        mock_200.status_code = 200
        mock_get.side_effect = [mock_429, mock_200]

        result = fetch_url("https://example.com/data.json")
        assert result == mock_200
        assert mock_get.call_count == 2
        mock_sleep.assert_called_once_with(1)  # 2**0 = 1

    @patch("ingestion.utils.time.sleep")
    @patch("ingestion.utils.requests.get")
    def test_retries_on_503(self, mock_get: MagicMock, mock_sleep: MagicMock) -> None:
        mock_503 = MagicMock(spec=requests.Response)
        mock_503.status_code = 503
        mock_200 = MagicMock(spec=requests.Response)
        mock_200.status_code = 200
        mock_get.side_effect = [mock_503, mock_503, mock_200]

        result = fetch_url("https://example.com/data.json")
        assert result == mock_200
        assert mock_get.call_count == 3

    @patch("ingestion.utils.time.sleep")
    @patch("ingestion.utils.requests.get")
    def test_raises_after_max_retries(self, mock_get: MagicMock, mock_sleep: MagicMock) -> None:
        mock_500 = MagicMock(spec=requests.Response)
        mock_500.status_code = 500
        mock_500.raise_for_status.side_effect = requests.HTTPError("Server Error")
        mock_get.return_value = mock_500

        with pytest.raises(requests.HTTPError):
            fetch_url("https://example.com/data.json", max_retries=3)

    @patch("ingestion.utils.requests.get")
    def test_raises_immediately_on_404(self, mock_get: MagicMock) -> None:
        mock_404 = MagicMock(spec=requests.Response)
        mock_404.status_code = 404
        mock_404.raise_for_status.side_effect = requests.HTTPError("Not Found")
        mock_get.return_value = mock_404

        with pytest.raises(requests.HTTPError):
            fetch_url("https://example.com/missing.json")

    @patch("ingestion.utils.requests.get")
    def test_custom_timeout(self, mock_get: MagicMock) -> None:
        mock_response = MagicMock(spec=requests.Response)
        mock_response.status_code = 200
        mock_get.return_value = mock_response

        fetch_url("https://example.com/data.json", timeout=(5, 15))
        mock_get.assert_called_once_with("https://example.com/data.json", timeout=(5, 15), verify=True)


# ---------------------------------------------------------------------------
# Content Validation
# ---------------------------------------------------------------------------


class TestValidateDataframe:
    """Tests for validate_dataframe."""

    def test_valid_dataframe(self) -> None:
        mock_df = MagicMock()
        mock_df.columns = ["id", "name", "value"]
        mock_df.count.return_value = 10

        # Should not raise
        validate_dataframe(mock_df, ["id", "name"], "test_source")

    def test_missing_columns_raises(self) -> None:
        mock_df = MagicMock()
        mock_df.columns = ["id", "name"]

        with pytest.raises(ValueError, match="Missing required columns"):
            validate_dataframe(mock_df, ["id", "name", "missing_col"], "test_source")

    def test_empty_dataframe_raises(self) -> None:
        mock_df = MagicMock()
        mock_df.columns = ["id", "name"]
        mock_df.count.return_value = 0

        with pytest.raises(ValueError, match="DataFrame is empty"):
            validate_dataframe(mock_df, ["id", "name"], "test_source")

    def test_logs_on_success(self) -> None:
        mock_df = MagicMock()
        mock_df.columns = ["id", "name"]
        mock_df.count.return_value = 5
        mock_logger = MagicMock()

        validate_dataframe(mock_df, ["id"], "test_source", mock_logger)
        mock_logger.info.assert_called_once()


# ---------------------------------------------------------------------------
# Delta Write (mocked Spark)
# ---------------------------------------------------------------------------


class TestWriteDeltaTable:
    """Tests for write_delta_table with mocked Spark."""

    @patch("ingestion.utils.add_audit_columns", side_effect=lambda df: df)
    def test_write_with_default_mode(self, _mock_audit: MagicMock) -> None:
        mock_df = MagicMock()
        mock_df.count.return_value = 42
        mock_writer = MagicMock()
        mock_df.write = mock_writer
        mock_writer.format.return_value = mock_writer
        mock_writer.option.return_value = mock_writer
        mock_writer.mode.return_value = mock_writer

        row_count = write_delta_table(mock_df, "cat", "sch", "tbl")
        assert row_count == 42
        mock_writer.mode.assert_called_with("overwrite")
        mock_writer.saveAsTable.assert_called_with("cat.sch.tbl")

    @patch("ingestion.utils.add_audit_columns", side_effect=lambda df: df)
    def test_write_with_replace_where(self, _mock_audit: MagicMock) -> None:
        mock_df = MagicMock()
        mock_df.count.return_value = 10
        mock_writer = MagicMock()
        mock_df.write = mock_writer
        mock_writer.format.return_value = mock_writer
        mock_writer.option.return_value = mock_writer
        mock_writer.mode.return_value = mock_writer

        row_count = write_delta_table(mock_df, "cat", "sch", "tbl", replace_where="id = 1")
        assert row_count == 10
        mock_writer.mode.assert_called_with("overwrite")

    @patch("ingestion.utils.add_audit_columns", side_effect=lambda df: df)
    def test_logs_row_count(self, _mock_audit: MagicMock) -> None:
        mock_df = MagicMock()
        mock_df.count.return_value = 100
        mock_writer = MagicMock()
        mock_df.write = mock_writer
        mock_writer.format.return_value = mock_writer
        mock_writer.option.return_value = mock_writer
        mock_writer.mode.return_value = mock_writer
        mock_logger = MagicMock()

        write_delta_table(mock_df, "cat", "sch", "tbl", logger=mock_logger)
        mock_logger.info.assert_called_once_with("Wrote %d rows to %s", 100, "cat.sch.tbl")
