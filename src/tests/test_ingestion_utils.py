"""Tests for ingestion.utils — CLI validation, HTTP client, logging, and validation."""

from __future__ import annotations

import json
import logging
from unittest.mock import MagicMock, patch

import pytest
import requests

import ingestion.utils as utils_mod
from ingestion.utils import (
    _get_session,
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


class TestGetSession:
    """Tests for the _get_session helper."""

    def test_returns_requests_session(self) -> None:
        session = _get_session()
        assert isinstance(session, requests.Session)

    def test_ssl_verification_enabled(self) -> None:
        session = _get_session()
        assert session.verify is True

    def test_returns_same_session_on_repeated_calls(self) -> None:
        """Session object must be reused — proves TCP keep-alive is in effect."""
        session_a = _get_session()
        session_b = _get_session()
        assert session_a is session_b

    def test_session_reset_creates_new_instance(self) -> None:
        """After resetting the module-level singleton, a fresh session is created."""
        original = utils_mod._session
        try:
            utils_mod._session = None
            new_session = _get_session()
            assert isinstance(new_session, requests.Session)
            assert new_session.verify is True
        finally:
            utils_mod._session = original


class TestFetchUrl:
    """Tests for fetch_url.

    fetch_url uses the module-level session (_get_session), so we patch
    session.get rather than requests.get directly.
    """

    def _make_mock_session(self) -> MagicMock:
        """Return a mock that replaces the module-level session."""
        mock_session = MagicMock(spec=requests.Session)
        return mock_session

    def test_rejects_http_url(self) -> None:
        with pytest.raises(ValueError, match="Only HTTPS URLs are allowed"):
            fetch_url("http://example.com/data.json")

    def test_rejects_ftp_url(self) -> None:
        with pytest.raises(ValueError, match="Only HTTPS URLs are allowed"):
            fetch_url("ftp://example.com/data.json")

    def test_successful_request(self) -> None:
        mock_session = self._make_mock_session()
        mock_response = MagicMock(spec=requests.Response)
        mock_response.status_code = 200
        mock_session.get.return_value = mock_response

        with patch("ingestion.utils._get_session", return_value=mock_session):
            result = fetch_url("https://example.com/data.json")

        assert result == mock_response
        mock_session.get.assert_called_once_with(
            "https://example.com/data.json", timeout=(10, 30), headers=None, stream=False
        )

    @patch("ingestion.utils.time.sleep")
    def test_retries_on_429(self, mock_sleep: MagicMock) -> None:
        mock_session = self._make_mock_session()
        mock_429 = MagicMock(spec=requests.Response)
        mock_429.status_code = 429
        mock_200 = MagicMock(spec=requests.Response)
        mock_200.status_code = 200
        mock_session.get.side_effect = [mock_429, mock_200]

        with patch("ingestion.utils._get_session", return_value=mock_session):
            result = fetch_url("https://example.com/data.json")

        assert result == mock_200
        assert mock_session.get.call_count == 2
        mock_sleep.assert_called_once_with(1)  # 2**0 = 1

    @patch("ingestion.utils.time.sleep")
    def test_retries_on_503(self, mock_sleep: MagicMock) -> None:
        mock_session = self._make_mock_session()
        mock_503 = MagicMock(spec=requests.Response)
        mock_503.status_code = 503
        mock_200 = MagicMock(spec=requests.Response)
        mock_200.status_code = 200
        mock_session.get.side_effect = [mock_503, mock_503, mock_200]

        with patch("ingestion.utils._get_session", return_value=mock_session):
            result = fetch_url("https://example.com/data.json")

        assert result == mock_200
        assert mock_session.get.call_count == 3

    @patch("ingestion.utils.time.sleep")
    def test_raises_after_max_retries(self, mock_sleep: MagicMock) -> None:
        mock_session = self._make_mock_session()
        mock_500 = MagicMock(spec=requests.Response)
        mock_500.status_code = 500
        mock_500.raise_for_status.side_effect = requests.HTTPError("Server Error")
        mock_session.get.return_value = mock_500

        with patch("ingestion.utils._get_session", return_value=mock_session):
            with pytest.raises(requests.HTTPError):
                fetch_url("https://example.com/data.json", max_retries=3)

    def test_raises_immediately_on_404(self) -> None:
        mock_session = self._make_mock_session()
        mock_404 = MagicMock(spec=requests.Response)
        mock_404.status_code = 404
        mock_404.raise_for_status.side_effect = requests.HTTPError("Not Found")
        mock_session.get.return_value = mock_404

        with patch("ingestion.utils._get_session", return_value=mock_session):
            with pytest.raises(requests.HTTPError):
                fetch_url("https://example.com/missing.json")

    def test_custom_timeout(self) -> None:
        mock_session = self._make_mock_session()
        mock_response = MagicMock(spec=requests.Response)
        mock_response.status_code = 200
        mock_session.get.return_value = mock_response

        with patch("ingestion.utils._get_session", return_value=mock_session):
            fetch_url("https://example.com/data.json", timeout=(5, 15))

        mock_session.get.assert_called_once_with(
            "https://example.com/data.json", timeout=(5, 15), headers=None, stream=False
        )


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


# ---------------------------------------------------------------------------
# Delta concurrent-commit retry (ADR-038)
# ---------------------------------------------------------------------------

from ingestion.utils import (  # noqa: E402
    _COMMIT_BACKOFF_BASE_S,
    _COMMIT_BACKOFF_CAP_S,
    _COMMIT_MAX_ATTEMPTS,
    _commit_with_retry,
    _is_concurrent_commit_error,
)

# Real observed S3-400-at-commit message snippet (run 730644476818402, worker 0).
_S3_COMMIT_400 = (
    "(shaded.databricks.awssdk.com.amazonaws.services.s3.model.AmazonS3Exception) Bad Request; "
    "request: HEAD https://dbstorage-prod-1huff.s3.amazonaws.com uc/.../__unitystorage/catalogs/"
    ".../tables/.../_delta_log/00000000000000000035.json ... "
    "com.databricks.tahoe.store.EnhancedS3AFileSystem.nativeS3PutIfAbsent ... "
    "com.databricks.tahoe.store.MultiClusterLogStore.writeCommit ... Status Code: 400; "
    "Error Code: 400 Bad Request"
)


def test_matcher_true_on_delta_conflict_markers() -> None:
    for marker in (
        "ConcurrentAppendException",
        "ConcurrentDeleteReadException",
        "ConcurrentDeleteDeleteException",
        "ConcurrentModificationException",
        "ProtocolChangedException",
        "CommitFailedException",
    ):
        assert _is_concurrent_commit_error(RuntimeError(f"Job aborted: {marker}: files added")), marker


def test_matcher_true_on_s3_400_at_commit() -> None:
    assert _is_concurrent_commit_error(RuntimeError(_S3_COMMIT_400))


def test_matcher_true_on_412_precondition_at_commit() -> None:
    # canonical conditional-write conflict code; the broad "Status Code: 4" marker is DELIBERATE.
    msg = (
        "AmazonS3Exception Precondition Failed; HEAD .../_delta_log/00035.json "
        "com.databricks.tahoe.store.MultiClusterLogStore.writeCommit Status Code: 412"
    )
    assert _is_concurrent_commit_error(RuntimeError(msg))


def test_matcher_false_on_unrelated_400() -> None:
    plain = "AmazonS3Exception Bad Request; request: GET s3://bucket/data/file.parquet Status Code: 400"
    assert not _is_concurrent_commit_error(RuntimeError(plain))


def test_matcher_false_on_delta_log_read_400() -> None:
    read = "AmazonS3Exception Bad Request; HEAD .../_delta_log/00000000000000000010.json Status Code: 400"
    assert not _is_concurrent_commit_error(RuntimeError(read))


def test_matcher_false_on_commit_path_4xx_without_delta_log() -> None:
    msg = "AmazonS3Exception Bad Request; MultiClusterLogStore.writeCommit Status Code: 400 (no log path)"
    assert not _is_concurrent_commit_error(RuntimeError(msg))


def test_matcher_false_on_other_errors() -> None:
    assert not _is_concurrent_commit_error(RuntimeError("[TABLE_OR_VIEW_NOT_FOUND] cannot find table"))
    assert not _is_concurrent_commit_error(ValueError("boom"))


def test_commit_with_retry_succeeds_after_conflicts(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr("ingestion.utils.time.sleep", lambda *_: None)
    calls = {"n": 0}

    def _commit() -> None:
        calls["n"] += 1
        if calls["n"] < 3:
            raise RuntimeError("Job aborted due to ConcurrentAppendException: files added")

    _commit_with_retry(_commit, "cat.bronze.t", logger=None)
    assert calls["n"] == 3


def test_commit_with_retry_reraises_non_retryable(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr("ingestion.utils.time.sleep", lambda *_: None)
    calls = {"n": 0}

    def _commit() -> None:
        calls["n"] += 1
        raise ValueError("boom")

    with pytest.raises(ValueError, match="boom"):
        _commit_with_retry(_commit, "cat.bronze.t", logger=None)
    assert calls["n"] == 1


def test_commit_with_retry_exhausts_then_raises(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr("ingestion.utils.time.sleep", lambda *_: None)
    calls = {"n": 0}

    def _commit() -> None:
        calls["n"] += 1
        raise RuntimeError("ConcurrentAppendException always")

    with pytest.raises(RuntimeError, match="ConcurrentAppendException"):
        _commit_with_retry(_commit, "cat.bronze.t", logger=None)
    assert calls["n"] == _COMMIT_MAX_ATTEMPTS


def test_commit_with_retry_jitter_is_bounded(monkeypatch: pytest.MonkeyPatch) -> None:
    slept: list[float] = []
    monkeypatch.setattr("ingestion.utils.time.sleep", lambda s: slept.append(s))

    def _commit() -> None:
        raise RuntimeError("ConcurrentAppendException")

    with pytest.raises(RuntimeError):
        _commit_with_retry(_commit, "cat.bronze.t", logger=None)
    assert len(slept) == _COMMIT_MAX_ATTEMPTS - 1
    for attempt, s in enumerate(slept, start=1):
        ceiling = min(_COMMIT_BACKOFF_CAP_S, _COMMIT_BACKOFF_BASE_S * 2 ** (attempt - 1))
        assert 0.0 <= s <= ceiling, (attempt, s, ceiling)


def test_commit_with_retry_always_attempts_once_even_if_misconfigured(monkeypatch: pytest.MonkeyPatch) -> None:
    # invariant: a write helper must NEVER silently no-op. Even if _COMMIT_MAX_ATTEMPTS is misconfigured
    # to 0, the commit must still be attempted exactly once (the max(..., 1) guard).
    monkeypatch.setattr("ingestion.utils._COMMIT_MAX_ATTEMPTS", 0)
    monkeypatch.setattr("ingestion.utils.time.sleep", lambda *_: None)
    calls = {"n": 0}

    def _commit() -> None:
        calls["n"] += 1

    _commit_with_retry(_commit, "cat.bronze.t", logger=None)
    assert calls["n"] == 1  # attempted once despite the bad constant -- no silent no-op


def test_write_delta_table_routes_commit_through_retry(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(utils_mod, "add_audit_columns", lambda df: df)
    writer = MagicMock()
    df = MagicMock()
    df.write.format.return_value.option.return_value.option.return_value.mode.return_value = writer

    captured: dict[str, object] = {}

    def _fake_retry(commit_fn: object, table: str, logger: object) -> None:
        captured["table"] = table
        commit_fn()  # type: ignore[operator]

    monkeypatch.setattr(utils_mod, "_commit_with_retry", _fake_retry)

    rows = write_delta_table(df, "cat", "bronze", "spadl_action_context", replace_where="match_id = 'X'", row_count=7)
    assert rows == 7
    assert captured["table"] == "cat.bronze.spadl_action_context"
    writer.saveAsTable.assert_called_once_with("cat.bronze.spadl_action_context")
