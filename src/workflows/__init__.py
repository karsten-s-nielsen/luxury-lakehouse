"""Workflow framework — registry, context, and lifecycle hooks."""

from workflows.context import WorkflowContext
from workflows.exceptions import WorkflowFailedError, WorkflowSkippedError, WorkflowTimeoutError
from workflows.hooks import LifecycleHook, LoggingHook
from workflows.registry import WorkflowEntry, WorkflowRegistry, _set_runner, workflow
from workflows.runner import register_hook, run_workflow

# Inject runner into registry to break the circular dependency.
# Both modules are fully loaded at this point.
_set_runner(run_workflow)

__all__ = [
    "LifecycleHook",
    "LoggingHook",
    "WorkflowContext",
    "WorkflowEntry",
    "WorkflowFailedError",
    "WorkflowRegistry",
    "WorkflowSkippedError",
    "WorkflowTimeoutError",
    "register_hook",
    "workflow",
]
