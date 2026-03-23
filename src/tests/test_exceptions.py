"""Tests for workflow exceptions."""

from __future__ import annotations

from workflows.exceptions import WorkflowFailedError, WorkflowSkippedError, WorkflowTimeoutError


def test_workflow_skipped_error_stores_reason() -> None:
    err = WorkflowSkippedError("all processed")
    assert err.reason == "all processed"
    assert str(err) == "all processed"


def test_workflow_failed_error_is_exception() -> None:
    err = WorkflowFailedError("disk full")
    assert isinstance(err, Exception)


def test_workflow_timeout_error_is_exception() -> None:
    err = WorkflowTimeoutError("exceeded 15 min")
    assert isinstance(err, Exception)
