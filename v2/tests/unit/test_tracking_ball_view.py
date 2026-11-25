"""fct_tracking_ball — the layer-0 ball + possession input view (spec D7; plan Task 3b).

A contracted gold view over the per-provider `*_tracking_frames` bronze: per `(canonical_match_key, period, frame)`
the ball position, and a `possession_team_key` (canonical) derived from the provider's own possession signal — idsse
`ball_possession` 1/2 → home/guest, skillcorner `possession_group` → home/away, gradientsports has none → null. Never
invented: null where the provider gives no possession (spec D7 anti-goal).
"""

from __future__ import annotations

from typing import Any

import pytest

from lakehouse.config.model import load_defaults
from lakehouse.runs.dbt import parse_manifest

CAPS = load_defaults("laptop").caps


@pytest.fixture(scope="module")
def manifest(tmp_path_factory: pytest.TempPathFactory) -> dict[str, Any]:
    return parse_manifest(tmp_path_factory.mktemp("dbt-parse-ball"), caps=CAPS)


def _node(manifest: dict[str, Any]) -> dict[str, Any]:
    return next(
        node
        for node in manifest["nodes"].values()
        if node["resource_type"] == "model" and node["name"] == "fct_tracking_ball"
    )


def test_is_a_contracted_view_with_possession(manifest: dict[str, Any]) -> None:
    node = _node(manifest)
    assert node["config"]["materialized"] == "view"
    assert node["config"]["contract"]["enforced"] is True
    assert set(node["columns"]) == {
        "canonical_match_key",
        "match_key",  # the provider record key; the key reads push it down (perf spec §7.4a)
        "period",
        "frame",
        "ball_x_mm",
        "ball_y_mm",
        "ball_detected",
        "time_seconds",
        "possession_team_key",
    }
    assert all(column.get("data_type") for column in node["columns"].values())


def test_tagged_compute_input(manifest: dict[str, Any]) -> None:
    assert "compute_input" in _node(manifest)["config"]["tags"]
