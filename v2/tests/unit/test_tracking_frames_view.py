"""fct_tracking_frames — the layer-0 tracking input view (spec D3; plan Task 3).

A contracted gold view over tracking_cross_provider, carrying canonical keys and positions ONLY: velocity and
goalkeeper resolution are a compute-pass concern (silly-kicks), never SQL, so the view must not carry them.
"""

from __future__ import annotations

from typing import Any

import pytest

from lakehouse.config.model import load_defaults
from lakehouse.runs.dbt import parse_manifest

CAPS = load_defaults("laptop").caps


@pytest.fixture(scope="module")
def manifest(tmp_path_factory: pytest.TempPathFactory) -> dict[str, Any]:
    return parse_manifest(tmp_path_factory.mktemp("dbt-parse-frames"), caps=CAPS)


def _node(manifest: dict[str, Any]) -> dict[str, Any]:
    return next(
        node
        for node in manifest["nodes"].values()
        if node["resource_type"] == "model" and node["name"] == "fct_tracking_frames"
    )


def test_is_a_contracted_view(manifest: dict[str, Any]) -> None:
    node = _node(manifest)
    assert node["config"]["materialized"] == "view"
    assert node["config"]["contract"]["enforced"] is True
    assert all(column.get("data_type") for column in node["columns"].values())


def test_positions_team_provider_no_velocity_or_goalkeeper(manifest: dict[str, Any]) -> None:
    """Positions + team membership (D7) + provider (D8/R6), but velocity and goalkeeper stay a compute-pass concern."""
    columns = set(_node(manifest)["columns"])
    assert columns == {
        "canonical_match_key",
        "match_key",  # the provider record key; the key reads push it down (perf spec §7.4a)
        "canonical_player_key",
        "team_key",
        "canonical_team_key",
        "provider",
        "period",
        "frame",
        "x_mm",
        "y_mm",
        "is_visible",
        "_visibility",
    }
    assert not any("velocity" in column or column in {"vx", "vy", "is_goalkeeper"} for column in columns)


def test_carries_provider(manifest: dict[str, Any]) -> None:
    """The frame builder reads `provider` to pick the per-match pitch size + roster source (spec D8 / R6)."""
    provider = _node(manifest)["columns"]["provider"]
    assert provider["data_type"] == "varchar"


def test_tagged_compute_input(manifest: dict[str, Any]) -> None:
    assert "compute_input" in _node(manifest)["config"]["tags"]
