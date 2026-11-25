"""The HF multi-dataset publish sink seam (spec D5; plan Task 10): typed now, built in a later cycle.

HF publishing is OPTIONAL and independent of the run: nothing in v2 depends on a published HF artifact, so this module
is NOT called by `lakehouse.runs.run` and imports no HF client. A `PublishPlan` carries N `PublishTarget`s so one lake
can publish a public and a private dataset in a single plan (spec D5). `publish` raises until a later cycle fills in
the body; the typed shape lets the AI/ML Workflows page (Task 9) render the (planned) publish node now.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Literal

from pydantic import BaseModel, ConfigDict

if TYPE_CHECKING:
    from lakehouse.catalog.working import WorkingCatalog

__all__ = ["PublishPlan", "PublishTarget", "publish"]


class PublishTarget(BaseModel):
    """One HF dataset repo to publish a set of marts to, at a single visibility."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    repo: str
    visibility: Literal["public", "private"]
    marts: tuple[str, ...]


class PublishPlan(BaseModel):
    """A set of publish targets fulfilled together — e.g. a public and a private dataset from one lake (spec D5)."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    targets: tuple[PublishTarget, ...]


def publish(plan: PublishPlan, *, cat: WorkingCatalog) -> None:
    """Publish the plan's marts to its HF targets. BUILD DEFERRED (spec D5): raises `NotImplementedError` this cycle."""
    raise NotImplementedError("HF publish build deferred — spec D5")
