"""The typed workflow-card model (spec D9; plan Task 8).

A `WorkflowCard` is one node of the calc-layer DAG the AI/ML Workflows page renders (Task 9): a layer-0 source, a
layer-1/2 compute, a gold view, the mart, or the (planned) publish sink. `CardIO` lists a card's input/output tables
and views and deliberately has **no** HuggingFace field — nothing in v2 depends on a published HF artifact (spec D5).
"""

from __future__ import annotations

from typing import Literal

from pydantic import BaseModel, ConfigDict, model_validator

__all__ = ["NOTE_REQUIRED", "CardIO", "CardStatus", "CardType", "WorkflowCard"]

#: A card's kind, used by the page to colour the node.
CardType = Literal["source", "compute", "view", "mart", "sink"]
#: The state of the card's unit (calc-layer spec D10):
#: - `active`: it runs in the default calc run and its output is complete;
#: - `incomplete`: it runs, and its output is known to be partial (the `note` says what is missing);
#: - `excluded`: it is built and registered, but it is not in the default calc run (the `note` says why). This is
#:   not a deferred match (`ComputeOutcome.deferred_matches`), which is a match a run did not reach;
#: - `planned`: it is defined but not built (for example the publish sink).
CardStatus = Literal["active", "incomplete", "excluded", "planned"]

#: The statuses whose card must explain itself in `note` (a partial or missing output is never silent; spec D6/D10).
NOTE_REQUIRED: frozenset[CardStatus] = frozenset({"incomplete", "excluded"})


class CardIO(BaseModel):
    """The tables and views a card reads or writes. No HF field by construction (spec D5)."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    tables: tuple[str, ...] = ()
    views: tuple[str, ...] = ()


class WorkflowCard(BaseModel):
    """One node of the calc-layer DAG: `depends_on` holds the ids of the cards it consumes (the DAG edges)."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    id: str
    name: str
    type: CardType
    status: CardStatus = "active"
    #: Why the card is not plainly `active` and what is missing; shown on the AI/ML Workflows page.
    note: str = ""
    depends_on: tuple[str, ...] = ()
    inputs: CardIO = CardIO()
    outputs: CardIO = CardIO()

    @model_validator(mode="after")
    def _explain_a_partial_or_missing_output(self) -> WorkflowCard:
        if self.status in NOTE_REQUIRED and not self.note.strip():
            raise ValueError(f"card {self.id!r} has status {self.status!r} and needs a note that says why")
        return self
