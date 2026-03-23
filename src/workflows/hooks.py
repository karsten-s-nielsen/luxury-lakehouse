"""Lifecycle hooks for workflow execution."""

from __future__ import annotations

import logging
from typing import Protocol, runtime_checkable

from workflows.context import WorkflowContext


@runtime_checkable
class LifecycleHook(Protocol):
    """Extension point for cross-cutting concerns."""

    def on_start(self, ctx: WorkflowContext) -> None: ...
    def on_complete(self, ctx: WorkflowContext, row_count: int | None) -> None: ...
    def on_skip(self, ctx: WorkflowContext, reason: str) -> None: ...
    def on_error(self, ctx: WorkflowContext, error: Exception) -> None: ...


class LoggingHook:
    """Default hook — structured logging with workflow context."""

    def __init__(self, logger: logging.Logger) -> None:
        self._logger = logger

    def on_start(self, ctx: WorkflowContext) -> None:
        self._logger.info(
            "Workflow started",
            extra=ctx.log_extra() | {"event": "workflow_start"},
        )

    def on_complete(self, ctx: WorkflowContext, row_count: int | None) -> None:
        self._logger.info(
            "Workflow completed",
            extra=ctx.log_extra()
            | {
                "event": "workflow_complete",
                "row_count": str(row_count) if row_count is not None else "unknown",
            },
        )

    def on_skip(self, ctx: WorkflowContext, reason: str) -> None:
        self._logger.info(
            "Workflow skipped: %s",
            reason,
            extra=ctx.log_extra() | {"event": "workflow_skip"},
        )

    def on_error(self, ctx: WorkflowContext, error: Exception) -> None:
        self._logger.error(
            "Workflow failed: %s",
            error,
            extra=ctx.log_extra() | {"event": "workflow_error", "error_type": type(error).__name__},
        )
