"""The tracking-metrics mart + the registry layering (spec D3/D9; plan Task 7).

`fct_tracking_metrics` is a contracted VIEW at per-player grain built over the three tracking-metric staging views (its
contract + view materialization are also checked in `test_dbt_materialisation`). It must NOT reach into the SPADL
actions layer (`fct_actions` / `stg__spadl_actions`) — that is action grain, not per-player. The registry must place
the SPADL + pitch_control + shape_graphs steps at layer 1 and line_breaking at layer 2, which is what makes the run's
build/compute alternation correct (spec D9).
"""

from __future__ import annotations

from typing import Any

import pytest

from lakehouse.compute.base import ComputeStepGroup
from lakehouse.compute.registry import REGISTERED_COMPUTE_STEPS
from lakehouse.config.model import load_defaults
from lakehouse.runs.dbt import parse_manifest

CAPS = load_defaults("laptop").caps


@pytest.fixture(scope="module")
def manifest(tmp_path_factory: pytest.TempPathFactory) -> dict[str, Any]:
    return parse_manifest(tmp_path_factory.mktemp("dbt-parse-mart"), caps=CAPS)


def _mart(manifest: dict[str, Any]) -> dict[str, Any]:
    return next(node for node in manifest["nodes"].values() if node["name"] == "fct_tracking_metrics")


def _ref_names(manifest: dict[str, Any], node: dict[str, Any]) -> set[str]:
    return {manifest["nodes"][uid]["name"] for uid in node["depends_on"]["nodes"] if uid in manifest["nodes"]}


def test_mart_is_a_contracted_view_at_match_player_grain(manifest: dict[str, Any]) -> None:
    mart = _mart(manifest)
    assert mart["config"]["materialized"] == "view"
    assert mart["config"]["contract"]["enforced"] is True
    columns = list(mart["columns"])
    assert columns[:2] == ["canonical_match_key", "canonical_player_key"]  # the per-player grain keys lead
    assert {"mean_controlled_fraction", "modal_position", "time_in_position", "line_breaking_passes"} <= set(columns)


def test_mart_refs_the_tracking_metric_staging_not_the_actions_layer(manifest: dict[str, Any]) -> None:
    refs = _ref_names(manifest, _mart(manifest))
    assert {"stg__pitch_control_values", "stg__shape_graph_labels", "stg__line_breaking_results"} <= refs
    # the per-player mart must NOT depend on the action-grain SPADL layer (that feeds line_breaking, not the mart)
    assert "fct_actions" not in refs
    assert "stg__spadl_actions" not in refs


def test_registry_steps_are_layered() -> None:
    # A step GROUP (perf spec §7.3: pitch_control + shape_graphs share one frame build) has one layer for all its names.
    layer = {
        name: step.layer
        for step in REGISTERED_COMPUTE_STEPS
        for name in (step.names if isinstance(step, ComputeStepGroup) else (step.name,))
    }
    for name in (
        "spadl_actions:statsbomb",
        "spadl_actions:wyscout",
        "spadl_actions:gradientsports",
        "spadl_actions:idsse",
        "spadl_actions:skillcorner",
        "pitch_control",
        "shape_graphs",
    ):
        assert layer[name] == 1, name  # layer 1 reads only layer-0 input views
    assert layer["line_breaking"] == 2  # layer 2 reads fct_actions, a layer-1 output view
