"""Workflow registry — singleton registry and ``@workflow`` decorator.

The registry maps ``workflow_id`` to a list of :class:`WorkflowEntry`
instances (one per phase).  The ``@workflow`` decorator registers a
function and wraps it so that invocation goes through the lifecycle
runner.

Circular-dependency strategy:
    ``registry.py`` does NOT import ``runner.py`` at module level.
    The decorator wrapper uses a lazy import inside the function body
    so that Python resolves the import only at call time — after both
    modules are fully loaded.
"""

from __future__ import annotations

import functools
import threading
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any

from workflows.card import WorkflowCard

# ---------------------------------------------------------------------------
# Data model
# ---------------------------------------------------------------------------


@dataclass
class WorkflowEntry:
    """A single registered workflow phase."""

    workflow_id: str
    phase: str  # training | inference | grid_computation | heuristic | validation
    func: Callable[..., Any]
    card: WorkflowCard | None = None  # Populated later by loader
    module: str = ""
    tags: tuple[str, ...] = ()


# ---------------------------------------------------------------------------
# Singleton registry
# ---------------------------------------------------------------------------


class WorkflowRegistry:
    """Singleton. Maps ``workflow_id`` to ``list[WorkflowEntry]``."""

    _instance: WorkflowRegistry | None = None
    _lock: threading.Lock = threading.Lock()
    _entries: dict[str, list[WorkflowEntry]]

    def __new__(cls) -> WorkflowRegistry:
        with cls._lock:
            if cls._instance is None:
                cls._instance = super().__new__(cls)
                cls._instance._entries = {}
        return cls._instance

    # -- Mutation -----------------------------------------------------------

    def register(self, entry: WorkflowEntry) -> None:
        """Add a workflow entry to the registry."""
        self._entries.setdefault(entry.workflow_id, []).append(entry)

    def clear(self) -> None:
        """Reset the registry — intended for test isolation only."""
        self._entries.clear()

    # -- Queries ------------------------------------------------------------

    def get(self, workflow_id: str) -> list[WorkflowEntry]:
        """Return all entries for *workflow_id*, or an empty list."""
        return self._entries.get(workflow_id, [])

    def get_phase(self, workflow_id: str, phase: str) -> WorkflowEntry | None:
        """Return the entry for a specific phase, or ``None``."""
        for entry in self._entries.get(workflow_id, []):
            if entry.phase == phase:
                return entry
        return None

    def all_workflows(self) -> dict[str, list[WorkflowEntry]]:
        """Return a shallow copy of all registered workflows."""
        return dict(self._entries)

    def load_cards(self, cards_dir: str) -> None:
        """Load workflow cards from *cards_dir* and attach to matching entries."""
        from workflows.loader import load_cards

        cards = load_cards(cards_dir)
        for wf_id, entries in self._entries.items():
            card = cards.get(wf_id)
            if card is not None:
                for entry in entries:
                    entry.card = card

    def downstream_of(self, workflow_id: str) -> list[str]:
        """Return workflow IDs that depend on *workflow_id*.

        Inverts the ``depends_on`` graph: if A's card lists ``workflow_id``
        in ``depends_on``, then A is downstream of ``workflow_id``.
        Entries without cards are skipped.
        """
        seen: set[str] = set()
        downstream: list[str] = []
        for wf_id, entries in self._entries.items():
            for entry in entries:
                if entry.card is None:
                    continue
                if workflow_id in entry.card.depends_on and wf_id not in seen:
                    seen.add(wf_id)
                    downstream.append(wf_id)
        return downstream


# ---------------------------------------------------------------------------
# Decorator
# ---------------------------------------------------------------------------


def workflow(
    workflow_id: str,
    phase: str,
    tags: tuple[str, ...] = (),
) -> Callable[[Callable[..., Any]], Callable[..., Any]]:
    """Register a function as a workflow phase.

    The decorator stores the **original** (unwrapped) function in
    ``WorkflowEntry.func`` and routes all calls through
    :func:`workflows.runner.run_workflow`.
    """

    def decorator(func: Callable[..., Any]) -> Callable[..., Any]:
        entry = WorkflowEntry(
            workflow_id=workflow_id,
            phase=phase,
            func=func,  # Store the ORIGINAL unwrapped function
            module=func.__module__,
            tags=tags,
        )
        WorkflowRegistry().register(entry)

        @functools.wraps(func)
        def wrapper(*args: Any, **kwargs: Any) -> Any:
            # Lazy import to break circular dependency
            from workflows.runner import run_workflow

            return run_workflow(entry, *args, **kwargs)

        wrapper._workflow_entry = entry  # type: ignore[attr-defined]
        return wrapper

    return decorator
