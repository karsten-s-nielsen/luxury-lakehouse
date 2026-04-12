"""Workflow lifecycle runner — wraps pipeline functions with hook dispatch."""

from __future__ import annotations

import inspect
import logging
from typing import Any

from workflows.context import WorkflowContext
from workflows.exceptions import WorkflowSkippedError
from workflows.hooks import LifecycleHook, LoggingHook
from workflows.registry import WorkflowEntry

_logger = logging.getLogger(__name__)

# Module-level hook registry
_hooks: list[LifecycleHook] = []


def register_hook(hook: LifecycleHook) -> None:
    """Register a lifecycle hook for all workflow executions."""
    _hooks.append(hook)


def _dispatch(hooks: list[LifecycleHook], method: str, *args: Any) -> None:
    """Call a lifecycle method on all hooks, swallowing individual failures."""
    for hook in hooks:
        try:
            getattr(hook, method)(*args)
        except Exception:
            _logger.warning(
                "Hook %s.%s failed — continuing pipeline execution",
                type(hook).__name__,
                method,
                exc_info=True,
            )


def run_workflow(entry: WorkflowEntry, *args: Any, **kwargs: Any) -> int | None:
    """Wrap a pipeline function with lifecycle management.

    Builds a :class:`WorkflowContext`, optionally injects it as ``ctx``
    if the function's signature accepts it, dispatches ``on_start`` /
    ``on_complete`` / ``on_error`` hooks, and returns the function's
    result.
    """
    # Extract entity_count and guard_duration from filter_result(s) for observability
    entity_count: int | None = None
    guard_duration_seconds: int | None = None
    fr = kwargs.get("filter_result")
    if fr is not None and hasattr(fr, "count"):
        entity_count = fr.count
    if fr is not None and hasattr(fr, "guard_duration_seconds"):
        guard_duration_seconds = fr.guard_duration_seconds
    # defcon_lite special case: sum of filter_360 + filter_tracking
    if entity_count is None:
        total = 0
        found = False
        for key in ("filter_360", "filter_tracking"):
            fr_multi = kwargs.get(key)
            if fr_multi is not None and hasattr(fr_multi, "count"):
                total += fr_multi.count
                found = True
        if found:
            entity_count = total

    ctx = WorkflowContext(
        workflow_id=entry.workflow_id,
        phase=entry.phase,
        workflow_name=entry.card.name if entry.card else entry.workflow_id,
        workflow_type=entry.card.type if entry.card else "",
        entity_count=entity_count,
        guard_duration_seconds=guard_duration_seconds,
    )

    # Inject context into kwargs if the function accepts it
    sig = inspect.signature(entry.func)
    if "ctx" in sig.parameters:
        kwargs["ctx"] = ctx

    # Auto-register LoggingHook if a logger is in args.
    # Convention: all pipelines use (spark, catalog, schema, logger) positional signature.
    # The isinstance guard prevents false matches on non-Logger args[3].
    active_hooks: list[LifecycleHook] = list(_hooks)
    logger_arg = kwargs.get("logger") or (args[3] if len(args) > 3 else None)
    if logger_arg and isinstance(logger_arg, logging.Logger):
        active_hooks.append(LoggingHook(logger_arg))

    _dispatch(active_hooks, "on_start", ctx)

    try:
        result = entry.func(*args, **kwargs)
        _dispatch(active_hooks, "on_complete", ctx, result)
        return result  # type: ignore[return-value]
    except WorkflowSkippedError as exc:
        _dispatch(active_hooks, "on_skip", ctx, str(exc))
        return None
    except Exception as exc:
        _dispatch(active_hooks, "on_error", ctx, exc)
        raise
