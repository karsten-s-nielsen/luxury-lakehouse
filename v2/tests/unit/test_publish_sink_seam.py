"""The HF multi-dataset publish sink seam (spec D5; plan Task 10): DEFINED this cycle, BUILD DEFERRED.

The seam is typed now so the AI/ML Workflows page (Task 9) can show the publish node, and so a later cycle fills in the
body. It must support N targets (e.g. a public and a private dataset from one lake), must not be wired into the run
(nothing in v2 depends on HF publishing — spec D5), and must not import a HuggingFace client.
"""

from __future__ import annotations

import inspect
from typing import TYPE_CHECKING, cast

import pytest

import lakehouse.publish.sink as sink_module
import lakehouse.runs.run as run_module
from lakehouse.publish import PublishPlan, PublishTarget, publish

if TYPE_CHECKING:
    from lakehouse.catalog.working import WorkingCatalog


def test_publish_plan_supports_multiple_targets() -> None:
    marts = ("fct_tracking_metrics",)
    plan = PublishPlan(
        targets=(
            PublishTarget(repo="ll/metrics-public", visibility="public", marts=marts),
            PublishTarget(repo="ll/metrics-private", visibility="private", marts=marts),
        )
    )
    assert len(plan.targets) == 2
    assert {t.visibility for t in plan.targets} == {"public", "private"}


def test_publish_build_is_deferred() -> None:
    plan = PublishPlan(targets=())
    with pytest.raises(NotImplementedError):
        publish(plan, cat=cast("WorkingCatalog", None))


def test_publish_is_not_wired_into_the_run() -> None:
    # Nothing in the run imports or calls the publish sink — HF publishing is optional and independent of the run (spec
    # D5). (The run does a *catalog* publish via checkpoint; this guards against importing `lakehouse.publish`.)
    run_src = inspect.getsource(run_module)
    assert "lakehouse.publish" not in run_src


def test_publish_module_imports_no_hf_client() -> None:
    src = inspect.getsource(sink_module)
    assert "huggingface" not in src.lower()
    assert "HfApi" not in src
