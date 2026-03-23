"""Tests for WorkflowRegistry and @workflow decorator."""

from __future__ import annotations

import textwrap

from workflows.card import WorkflowCard
from workflows.registry import WorkflowEntry, WorkflowRegistry, workflow

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

MINIMAL_CARD_TEMPLATE = textwrap.dedent("""\
    ---
    name: {name}
    id: {wf_id}
    version: "1.0.0"
    status: production
    type: training-and-inference
    domain: test
    owners:
      - test
    depends_on: {depends_on}
    ---
""")


def _make_card(wf_id: str, name: str, depends_on: list[str] | None = None) -> WorkflowCard:
    deps_yaml = "\n".join(f"      - {d}" for d in (depends_on or []))
    if deps_yaml:
        deps_yaml = "\n" + deps_yaml
    else:
        deps_yaml = " []"
    yaml_text = MINIMAL_CARD_TEMPLATE.format(name=name, wf_id=wf_id, depends_on=deps_yaml)
    return WorkflowCard.from_yaml_string(yaml_text)


# ---------------------------------------------------------------------------
# 1. Singleton behaviour
# ---------------------------------------------------------------------------


def test_registry_is_singleton() -> None:
    r1 = WorkflowRegistry()
    r2 = WorkflowRegistry()
    assert r1 is r2


# ---------------------------------------------------------------------------
# 2. Register and retrieve via decorator
# ---------------------------------------------------------------------------


def test_decorator_registers_entry() -> None:
    registry = WorkflowRegistry()
    registry.clear()

    @workflow("wf-test", phase="inference")
    def my_pipeline() -> int:
        return 42

    try:
        entries = registry.get("wf-test")
        assert len(entries) == 1
        assert entries[0].workflow_id == "wf-test"
        assert entries[0].phase == "inference"
    finally:
        registry.clear()


# ---------------------------------------------------------------------------
# 3. get() returns entry list
# ---------------------------------------------------------------------------


def test_get_returns_entries() -> None:
    registry = WorkflowRegistry()
    registry.clear()

    entry = WorkflowEntry(
        workflow_id="wf-get-test",
        phase="training",
        func=lambda: None,
        module="test_registry",
    )
    registry.register(entry)

    try:
        result = registry.get("wf-get-test")
        assert len(result) == 1
        assert result[0] is entry
    finally:
        registry.clear()


def test_get_unknown_id_returns_empty() -> None:
    registry = WorkflowRegistry()
    registry.clear()
    assert registry.get("wf-nonexistent") == []


# ---------------------------------------------------------------------------
# 4. get_phase() returns correct entry
# ---------------------------------------------------------------------------


def test_get_phase_returns_correct_entry() -> None:
    registry = WorkflowRegistry()
    registry.clear()

    entry_train = WorkflowEntry(workflow_id="wf-phase", phase="training", func=lambda: 1, module="test")
    entry_infer = WorkflowEntry(workflow_id="wf-phase", phase="inference", func=lambda: 2, module="test")
    registry.register(entry_train)
    registry.register(entry_infer)

    try:
        result = registry.get_phase("wf-phase", "inference")
        assert result is entry_infer

        result_train = registry.get_phase("wf-phase", "training")
        assert result_train is entry_train
    finally:
        registry.clear()


def test_get_phase_returns_none_for_missing() -> None:
    registry = WorkflowRegistry()
    registry.clear()

    entry = WorkflowEntry(workflow_id="wf-miss", phase="training", func=lambda: None, module="test")
    registry.register(entry)

    try:
        assert registry.get_phase("wf-miss", "inference") is None
        assert registry.get_phase("wf-unknown", "training") is None
    finally:
        registry.clear()


# ---------------------------------------------------------------------------
# 5. Register two phases for same workflow ID
# ---------------------------------------------------------------------------


def test_two_phases_same_workflow() -> None:
    registry = WorkflowRegistry()
    registry.clear()

    @workflow("wf-dual", phase="training")
    def train_fn() -> int:
        return 1

    @workflow("wf-dual", phase="inference")
    def infer_fn() -> int:
        return 2

    try:
        entries = registry.get("wf-dual")
        assert len(entries) == 2
        phases = {e.phase for e in entries}
        assert phases == {"training", "inference"}
    finally:
        registry.clear()


# ---------------------------------------------------------------------------
# 6. all_workflows() returns all entries
# ---------------------------------------------------------------------------


def test_all_workflows() -> None:
    registry = WorkflowRegistry()
    registry.clear()

    @workflow("wf-a", phase="training")
    def fn_a() -> int:
        return 1

    @workflow("wf-b", phase="inference")
    def fn_b() -> int:
        return 2

    try:
        all_wf = registry.all_workflows()
        assert "wf-a" in all_wf
        assert "wf-b" in all_wf
        assert len(all_wf["wf-a"]) == 1
        assert len(all_wf["wf-b"]) == 1
    finally:
        registry.clear()


# ---------------------------------------------------------------------------
# 7. functools.wraps preserves function metadata
# ---------------------------------------------------------------------------


def test_decorator_preserves_function_metadata() -> None:
    registry = WorkflowRegistry()
    registry.clear()

    @workflow("wf-meta", phase="inference")
    def my_special_function() -> int:
        """My docstring."""
        return 99

    try:
        assert my_special_function.__name__ == "my_special_function"
        assert my_special_function.__qualname__.endswith("my_special_function")
        assert my_special_function.__module__ == __name__
    finally:
        registry.clear()


# ---------------------------------------------------------------------------
# 8. Decorated function return value passes through
# ---------------------------------------------------------------------------


def test_decorated_function_return_value() -> None:
    registry = WorkflowRegistry()
    registry.clear()

    @workflow("wf-ret", phase="inference")
    def pipeline_fn() -> int:
        return 42

    try:
        result = pipeline_fn()
        assert result == 42
    finally:
        registry.clear()


# ---------------------------------------------------------------------------
# 9. _workflow_entry attribute set on wrapper
# ---------------------------------------------------------------------------


def test_workflow_entry_attribute_on_wrapper() -> None:
    registry = WorkflowRegistry()
    registry.clear()

    @workflow("wf-attr", phase="training", tags=("ml", "xg"))
    def tagged_fn() -> int:
        return 1

    try:
        assert hasattr(tagged_fn, "_workflow_entry")
        entry = tagged_fn._workflow_entry  # type: ignore[attr-defined]
        assert isinstance(entry, WorkflowEntry)
        assert entry.workflow_id == "wf-attr"
        assert entry.phase == "training"
        assert entry.tags == ("ml", "xg")
    finally:
        registry.clear()


# ---------------------------------------------------------------------------
# 10. downstream_of() graph inversion
# ---------------------------------------------------------------------------


def test_downstream_of_graph_inversion() -> None:
    """A depends_on B → downstream_of(B) returns [A]."""
    registry = WorkflowRegistry()
    registry.clear()

    # Register entries with cards that have depends_on
    card_a = _make_card("wf-a", "Workflow A", depends_on=["wf-b", "wf-c"])
    card_d = _make_card("wf-d", "Workflow D", depends_on=["wf-b"])
    card_b = _make_card("wf-b", "Workflow B")

    entry_a = WorkflowEntry(workflow_id="wf-a", phase="inference", func=lambda: None, module="test", card=card_a)
    entry_d = WorkflowEntry(workflow_id="wf-d", phase="inference", func=lambda: None, module="test", card=card_d)
    entry_b = WorkflowEntry(workflow_id="wf-b", phase="inference", func=lambda: None, module="test", card=card_b)

    registry.register(entry_a)
    registry.register(entry_d)
    registry.register(entry_b)

    try:
        # wf-b is upstream of both wf-a and wf-d
        downstream = registry.downstream_of("wf-b")
        assert sorted(downstream) == ["wf-a", "wf-d"]

        # wf-c is upstream of wf-a only
        downstream_c = registry.downstream_of("wf-c")
        assert downstream_c == ["wf-a"]

        # wf-a has no downstream dependents
        downstream_a = registry.downstream_of("wf-a")
        assert downstream_a == []
    finally:
        registry.clear()


def test_downstream_of_skips_entries_without_cards() -> None:
    registry = WorkflowRegistry()
    registry.clear()

    entry_no_card = WorkflowEntry(workflow_id="wf-nocard", phase="inference", func=lambda: None, module="test")
    card_with_dep = _make_card("wf-with", "With Card", depends_on=["wf-upstream"])
    entry_with_card = WorkflowEntry(
        workflow_id="wf-with", phase="inference", func=lambda: None, module="test", card=card_with_dep
    )

    registry.register(entry_no_card)
    registry.register(entry_with_card)

    try:
        downstream = registry.downstream_of("wf-upstream")
        assert downstream == ["wf-with"]
    finally:
        registry.clear()


# ---------------------------------------------------------------------------
# 11. clear() resets the registry
# ---------------------------------------------------------------------------


def test_clear_resets_registry() -> None:
    registry = WorkflowRegistry()
    registry.clear()

    entry = WorkflowEntry(workflow_id="wf-clear", phase="training", func=lambda: None, module="test")
    registry.register(entry)
    assert len(registry.get("wf-clear")) == 1

    registry.clear()
    assert registry.get("wf-clear") == []
    assert registry.all_workflows() == {}


# ---------------------------------------------------------------------------
# 12. Decorator stores original function, not wrapper
# ---------------------------------------------------------------------------


def test_decorator_stores_original_function() -> None:
    registry = WorkflowRegistry()
    registry.clear()

    def original_fn() -> int:
        return 7

    workflow("wf-orig", phase="inference")(original_fn)

    try:
        entries = registry.get("wf-orig")
        assert len(entries) == 1
        # entry.func should be the ORIGINAL function, not the wrapper
        assert entries[0].func is original_fn
    finally:
        registry.clear()
