"""Tests for WorkflowContext."""

from __future__ import annotations

from workflows.context import WorkflowContext


def test_context_creation_with_defaults() -> None:
    ctx = WorkflowContext(workflow_id="wf-xg-v1", phase="inference")
    assert ctx.workflow_id == "wf-xg-v1"
    assert ctx.phase == "inference"
    assert ctx.run_id  # auto-generated UUID, non-empty
    assert ctx.started_at  # auto-generated timestamp


def test_context_is_frozen() -> None:
    ctx = WorkflowContext(workflow_id="wf-xg-v1", phase="inference")
    try:
        ctx.workflow_id = "wf-other"  # type: ignore[misc]
        raise AssertionError("Should have raised FrozenInstanceError")
    except AttributeError:
        pass  # Expected — frozen dataclass


def test_log_extra_contains_correlation_fields() -> None:
    ctx = WorkflowContext(
        workflow_id="wf-vaep",
        phase="training",
        workflow_name="VAEP Action Valuation",
    )
    extra = ctx.log_extra()
    assert extra["workflow_id"] == "wf-vaep"
    assert extra["workflow_phase"] == "training"
    assert extra["run_id"] == ctx.run_id
    assert extra["workflow_name"] == "VAEP Action Valuation"
    assert "started_at" in extra


def test_log_extra_includes_optional_fields_when_set() -> None:
    ctx = WorkflowContext(
        workflow_id="wf-test",
        phase="heuristic",
        workflow_type="heuristic",
        partition_key="match_id",
    )
    extra = ctx.log_extra()
    assert extra["workflow_type"] == "heuristic"
    assert extra["partition_key"] == "match_id"


def test_log_extra_excludes_empty_optional_fields() -> None:
    ctx = WorkflowContext(workflow_id="wf-test", phase="inference")
    extra = ctx.log_extra()
    assert "workflow_type" not in extra
    assert "partition_key" not in extra


def test_log_extra_all_values_are_strings() -> None:
    ctx = WorkflowContext(workflow_id="wf-test", phase="heuristic")
    extra = ctx.log_extra()
    for key, value in extra.items():
        assert isinstance(value, str), f"{key} should be str, got {type(value)}"


def test_run_id_is_unique_per_instance() -> None:
    ctx1 = WorkflowContext(workflow_id="wf-xg-v1", phase="inference")
    ctx2 = WorkflowContext(workflow_id="wf-xg-v1", phase="inference")
    assert ctx1.run_id != ctx2.run_id
