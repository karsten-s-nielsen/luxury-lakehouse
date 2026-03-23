"""Tests for lifecycle hooks."""

from __future__ import annotations

import logging

import pytest

from workflows.context import WorkflowContext
from workflows.hooks import LoggingHook


def _make_ctx() -> WorkflowContext:
    return WorkflowContext(
        workflow_id="wf-test",
        phase="inference",
        workflow_name="Test Workflow",
    )


def test_logging_hook_on_start(caplog: pytest.LogCaptureFixture) -> None:
    logger = logging.getLogger("test.hooks.start")
    hook = LoggingHook(logger)
    ctx = _make_ctx()
    with caplog.at_level(logging.INFO, logger="test.hooks.start"):
        hook.on_start(ctx)
    assert "Workflow started" in caplog.text


def test_logging_hook_on_complete(caplog: pytest.LogCaptureFixture) -> None:
    logger = logging.getLogger("test.hooks.complete")
    hook = LoggingHook(logger)
    ctx = _make_ctx()
    with caplog.at_level(logging.INFO, logger="test.hooks.complete"):
        hook.on_complete(ctx, row_count=42)
    assert "Workflow completed" in caplog.text


def test_logging_hook_on_skip(caplog: pytest.LogCaptureFixture) -> None:
    logger = logging.getLogger("test.hooks.skip")
    hook = LoggingHook(logger)
    ctx = _make_ctx()
    with caplog.at_level(logging.INFO, logger="test.hooks.skip"):
        hook.on_skip(ctx, reason="All items already processed")
    assert "skipped" in caplog.text.lower()


def test_logging_hook_on_error(caplog: pytest.LogCaptureFixture) -> None:
    logger = logging.getLogger("test.hooks.error")
    hook = LoggingHook(logger)
    ctx = _make_ctx()
    with caplog.at_level(logging.ERROR, logger="test.hooks.error"):
        hook.on_error(ctx, ValueError("bad data"))
    assert "failed" in caplog.text.lower()
