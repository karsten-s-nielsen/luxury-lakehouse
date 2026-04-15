"""Tests for ingestion.utils helpers."""

from __future__ import annotations

import logging
from unittest.mock import MagicMock

import pytest

from ingestion.utils import tolerate_missing_table


class TestTolerateMissingTable:
    """``tolerate_missing_table`` must suppress ONLY table-not-found errors.

    Regression guard for the session-40 silent-swallow remediation: the
    previous bare ``except Exception:`` pattern in 20+ bootstrap sites hid
    permission errors, schema mismatches, and connection errors as "table
    missing". This helper narrows the catch to specific error-message
    markers so only genuine table-missing cases are suppressed.
    """

    def _mk_logger(self) -> MagicMock:
        mock = MagicMock(spec=logging.Logger)
        return mock

    def test_suppresses_table_or_view_not_found(self) -> None:
        """Spark 3.4+ TABLE_OR_VIEW_NOT_FOUND error class is suppressed."""
        logger = self._mk_logger()
        with tolerate_missing_table(logger, "no existing x"):
            raise RuntimeError("[TABLE_OR_VIEW_NOT_FOUND] Table `foo` cannot be found")
        logger.info.assert_called_once_with("no existing x")

    def test_suppresses_older_table_or_view_message(self) -> None:
        """Older Spark 'Table or view not found' message is suppressed."""
        logger = self._mk_logger()
        with tolerate_missing_table(logger, "first run"):
            raise Exception("Table or view not found: soccer_analytics.bronze.foo")
        logger.info.assert_called_once_with("first run")

    def test_suppresses_path_does_not_exist(self) -> None:
        """Delta 'Path does not exist' is suppressed."""
        logger = self._mk_logger()
        with tolerate_missing_table(logger, "delta not seeded"):
            raise OSError("Path does not exist: /Volumes/foo")
        logger.info.assert_called_once_with("delta not seeded")

    def test_suppresses_delta_missing_delta_table(self) -> None:
        """DELTA_MISSING_DELTA_TABLE error class is suppressed."""
        logger = self._mk_logger()
        with tolerate_missing_table(logger, "first run"):
            raise Exception("[DELTA_MISSING_DELTA_TABLE] The table does not exist")
        logger.info.assert_called_once_with("first run")

    def test_suppresses_table_not_found_exception(self) -> None:
        """Unity Catalog TableNotFoundException is suppressed."""
        logger = self._mk_logger()
        with tolerate_missing_table(logger, "first run"):
            raise Exception("TableNotFoundException: soccer_analytics.bronze.foo")
        logger.info.assert_called_once_with("first run")

    def test_propagates_permission_denied(self) -> None:
        """Permission errors must NOT be suppressed — they indicate a real problem."""
        logger = self._mk_logger()
        with pytest.raises(PermissionError, match="access denied"):
            with tolerate_missing_table(logger, "first run"):
                raise PermissionError("access denied for workflow_cost_live")
        logger.info.assert_not_called()

    def test_propagates_merge_unresolved_expression(self) -> None:
        """The exact error class that caused the warm-tier blocker must propagate.

        This is the ``DELTA_MERGE_UNRESOLVED_EXPRESSION`` that the 2026-04-12
        schema drift caused. The OLD bare ``except Exception:`` pattern would
        have silently suppressed it. The new helper must let it propagate.
        """
        logger = self._mk_logger()
        with pytest.raises(RuntimeError, match="DELTA_MERGE_UNRESOLVED_EXPRESSION"):
            with tolerate_missing_table(logger, "no existing x"):
                raise RuntimeError(
                    "[DELTA_MERGE_UNRESOLVED_EXPRESSION] Cannot resolve task_key "
                    "in UPDATE clause given columns s.workflow_id, s.phase, ..."
                )
        logger.info.assert_not_called()

    def test_propagates_connection_error(self) -> None:
        """Connection errors must NOT be suppressed."""
        logger = self._mk_logger()
        with pytest.raises(ConnectionError):
            with tolerate_missing_table(logger, "first run"):
                raise ConnectionError("thrift server unreachable")
        logger.info.assert_not_called()

    def test_propagates_value_error_with_unrelated_message(self) -> None:
        """Generic ValueError without any table-missing marker must propagate."""
        logger = self._mk_logger()
        with pytest.raises(ValueError, match="bad input"):
            with tolerate_missing_table(logger, "first run"):
                raise ValueError("bad input")
        logger.info.assert_not_called()

    def test_yield_value_is_none(self) -> None:
        """Context manager yields None, not a fixture or state object."""
        logger = self._mk_logger()
        with tolerate_missing_table(logger, "first run") as value:
            assert value is None

    def test_body_runs_on_happy_path(self) -> None:
        """When no exception is raised, the helper is a pure pass-through."""
        logger = self._mk_logger()
        executed = False
        with tolerate_missing_table(logger, "first run"):
            executed = True
        assert executed
        logger.info.assert_not_called()
