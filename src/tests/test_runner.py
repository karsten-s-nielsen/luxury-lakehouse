"""Tests for the workflow lifecycle runner."""

from __future__ import annotations

import logging
from typing import Any
from unittest.mock import MagicMock

import pytest

from workflows.context import WorkflowContext
from workflows.registry import WorkflowEntry
from workflows.runner import _dispatch, _hooks, register_hook, run_workflow

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_entry(func: Any, workflow_id: str = "wf-test", phase: str = "inference") -> WorkflowEntry:
    return WorkflowEntry(
        workflow_id=workflow_id,
        phase=phase,
        func=func,
        module="test_runner",
    )


class _RecordingHook:
    """Hook that records which lifecycle methods were called."""

    def __init__(self) -> None:
        self.calls: list[str] = []
        self.last_result: int | None = None
        self.last_error: Exception | None = None

    def on_start(self, ctx: WorkflowContext) -> None:
        self.calls.append("on_start")

    def on_complete(self, ctx: WorkflowContext, row_count: int | None) -> None:
        self.calls.append("on_complete")
        self.last_result = row_count

    def on_skip(self, ctx: WorkflowContext, reason: str) -> None:
        self.calls.append("on_skip")

    def on_error(self, ctx: WorkflowContext, error: Exception) -> None:
        self.calls.append("on_error")
        self.last_error = error


class _FailingHook:
    """Hook that raises on every method — for fault-tolerance testing."""

    def on_start(self, ctx: WorkflowContext) -> None:
        msg = "Hook start failure"
        raise RuntimeError(msg)

    def on_complete(self, ctx: WorkflowContext, row_count: int | None) -> None:
        msg = "Hook complete failure"
        raise RuntimeError(msg)

    def on_skip(self, ctx: WorkflowContext, reason: str) -> None:
        msg = "Hook skip failure"
        raise RuntimeError(msg)

    def on_error(self, ctx: WorkflowContext, error: Exception) -> None:
        msg = "Hook error failure"
        raise RuntimeError(msg)


# ---------------------------------------------------------------------------
# 1. run_workflow calls original function and returns value
# ---------------------------------------------------------------------------


def test_run_workflow_returns_function_value() -> None:
    saved_hooks = _hooks.copy()
    _hooks.clear()
    try:

        def my_func() -> int:
            return 42

        entry = _make_entry(my_func)
        result = run_workflow(entry)
        assert result == 42
    finally:
        _hooks.clear()
        _hooks.extend(saved_hooks)


# ---------------------------------------------------------------------------
# 2. ctx injection when function accepts it
# ---------------------------------------------------------------------------


def test_run_workflow_injects_ctx() -> None:
    saved_hooks = _hooks.copy()
    _hooks.clear()
    try:
        captured: dict[str, Any] = {}

        def fn_with_ctx(*, ctx: WorkflowContext | None = None) -> int:
            captured["ctx"] = ctx
            return 10

        entry = _make_entry(fn_with_ctx, workflow_id="wf-ctx-test", phase="training")
        result = run_workflow(entry)
        assert result == 10
        assert captured["ctx"] is not None
        assert isinstance(captured["ctx"], WorkflowContext)
        assert captured["ctx"].workflow_id == "wf-ctx-test"
        assert captured["ctx"].phase == "training"
    finally:
        _hooks.clear()
        _hooks.extend(saved_hooks)


# ---------------------------------------------------------------------------
# 3. ctx NOT injected when function doesn't accept it
# ---------------------------------------------------------------------------


def test_run_workflow_does_not_inject_ctx_when_not_accepted() -> None:
    saved_hooks = _hooks.copy()
    _hooks.clear()
    try:

        def fn_no_ctx(x: int = 5) -> int:
            return x * 2

        entry = _make_entry(fn_no_ctx)
        result = run_workflow(entry)
        assert result == 10
    finally:
        _hooks.clear()
        _hooks.extend(saved_hooks)


# ---------------------------------------------------------------------------
# 4. Hook dispatch fault tolerance
# ---------------------------------------------------------------------------


def test_failing_hook_does_not_crash_pipeline(caplog: pytest.LogCaptureFixture) -> None:
    saved_hooks = _hooks.copy()
    _hooks.clear()
    try:
        failing_hook = _FailingHook()
        recording_hook = _RecordingHook()
        register_hook(failing_hook)
        register_hook(recording_hook)

        def fn() -> int:
            return 99

        entry = _make_entry(fn)
        with caplog.at_level(logging.WARNING, logger="workflows.runner"):
            result = run_workflow(entry)

        assert result == 99
        # Recording hook still got called despite failing hook before it
        assert "on_start" in recording_hook.calls
        assert "on_complete" in recording_hook.calls
        # Warning was logged for the failure
        assert "failed" in caplog.text.lower()
    finally:
        _hooks.clear()
        _hooks.extend(saved_hooks)


# ---------------------------------------------------------------------------
# 5. on_error called when function raises, then re-raises
# ---------------------------------------------------------------------------


def test_on_error_called_on_exception() -> None:
    saved_hooks = _hooks.copy()
    _hooks.clear()
    try:
        recording_hook = _RecordingHook()
        register_hook(recording_hook)

        def failing_fn() -> int:
            msg = "pipeline exploded"
            raise ValueError(msg)

        entry = _make_entry(failing_fn)
        raised = False
        try:
            run_workflow(entry)
        except ValueError:
            raised = True

        assert raised, "Exception should have been re-raised"
        assert "on_start" in recording_hook.calls
        assert "on_error" in recording_hook.calls
        assert "on_complete" not in recording_hook.calls
        assert isinstance(recording_hook.last_error, ValueError)
        assert str(recording_hook.last_error) == "pipeline exploded"
    finally:
        _hooks.clear()
        _hooks.extend(saved_hooks)


# ---------------------------------------------------------------------------
# 6. on_complete called with return value on success
# ---------------------------------------------------------------------------


def test_on_complete_receives_return_value() -> None:
    saved_hooks = _hooks.copy()
    _hooks.clear()
    try:
        recording_hook = _RecordingHook()
        register_hook(recording_hook)

        def fn() -> int:
            return 123

        entry = _make_entry(fn)
        result = run_workflow(entry)

        assert result == 123
        assert "on_start" in recording_hook.calls
        assert "on_complete" in recording_hook.calls
        assert recording_hook.last_result == 123
    finally:
        _hooks.clear()
        _hooks.extend(saved_hooks)


# ---------------------------------------------------------------------------
# 7. _dispatch swallows individual hook failures
# ---------------------------------------------------------------------------


def test_dispatch_swallows_individual_failures(caplog: pytest.LogCaptureFixture) -> None:
    failing = _FailingHook()
    recording = _RecordingHook()
    ctx = WorkflowContext(workflow_id="wf-dispatch", phase="test")

    with caplog.at_level(logging.WARNING, logger="workflows.runner"):
        _dispatch([failing, recording], "on_start", ctx)

    assert "on_start" in recording.calls


# ---------------------------------------------------------------------------
# 8. register_hook appends to module-level list
# ---------------------------------------------------------------------------


def test_register_hook() -> None:
    saved_hooks = _hooks.copy()
    _hooks.clear()
    try:
        hook = _RecordingHook()
        register_hook(hook)
        assert hook in _hooks
    finally:
        _hooks.clear()
        _hooks.extend(saved_hooks)


# ---------------------------------------------------------------------------
# 9. run_workflow with card metadata populates context
# ---------------------------------------------------------------------------


def test_run_workflow_with_card_populates_context() -> None:
    saved_hooks = _hooks.copy()
    _hooks.clear()
    try:
        captured: dict[str, Any] = {}

        def fn_with_ctx(*, ctx: WorkflowContext | None = None) -> int:
            captured["ctx"] = ctx
            return 0

        card = MagicMock()
        card.name = "My Test Workflow"
        card.type = "inference"

        entry = WorkflowEntry(
            workflow_id="wf-card-ctx",
            phase="inference",
            func=fn_with_ctx,
            module="test",
            card=card,
        )
        run_workflow(entry)

        ctx = captured["ctx"]
        assert ctx.workflow_name == "My Test Workflow"
        assert ctx.workflow_type == "inference"
    finally:
        _hooks.clear()
        _hooks.extend(saved_hooks)


# ---------------------------------------------------------------------------
# 10. run_workflow without card uses workflow_id as fallback
# ---------------------------------------------------------------------------


def test_run_workflow_without_card_uses_fallback() -> None:
    saved_hooks = _hooks.copy()
    _hooks.clear()
    try:
        captured: dict[str, Any] = {}

        def fn_with_ctx(*, ctx: WorkflowContext | None = None) -> int:
            captured["ctx"] = ctx
            return 0

        entry = _make_entry(fn_with_ctx, workflow_id="wf-fallback")
        run_workflow(entry)

        ctx = captured["ctx"]
        assert ctx.workflow_name == "wf-fallback"
        assert ctx.workflow_type == ""
    finally:
        _hooks.clear()
        _hooks.extend(saved_hooks)


# ---------------------------------------------------------------------------
# 11. WorkflowSkippedError dispatches on_skip (not on_error)
# ---------------------------------------------------------------------------


def test_skipped_error_dispatches_on_skip() -> None:
    """WorkflowSkippedError should trigger on_skip, not on_error."""
    from workflows.exceptions import WorkflowSkippedError

    saved_hooks = _hooks.copy()
    _hooks.clear()
    try:
        recording_hook = _RecordingHook()
        _hooks.append(recording_hook)

        def skip_pipeline() -> None:
            raise WorkflowSkippedError("all matches processed")

        entry = WorkflowEntry(
            workflow_id="wf-skip-test",
            phase="test",
            func=skip_pipeline,
        )
        result = run_workflow(entry)

        assert result is None
        # _RecordingHook.calls is list[str]
        assert "on_skip" in recording_hook.calls
        assert "on_error" not in recording_hook.calls
    finally:
        _hooks.clear()
        _hooks.extend(saved_hooks)


def test_skipped_error_does_not_reraise() -> None:
    """WorkflowSkippedError should NOT propagate — pipeline exits 0."""
    from workflows.exceptions import WorkflowSkippedError

    saved_hooks = _hooks.copy()
    _hooks.clear()
    try:
        _hooks.append(_RecordingHook())

        def skip_pipeline() -> None:
            raise WorkflowSkippedError("nothing to do")

        entry = WorkflowEntry(
            workflow_id="wf-skip-noraise",
            phase="test",
            func=skip_pipeline,
        )
        # Should NOT raise
        result = run_workflow(entry)
        assert result is None
    finally:
        _hooks.clear()
        _hooks.extend(saved_hooks)
