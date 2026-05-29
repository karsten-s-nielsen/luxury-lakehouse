from __future__ import annotations

import json
from pathlib import Path

import numpy as np
import pandas as pd

from analytics.action_context.local.parquet_sources import (
    ParquetActionsSource,
    ParquetFrameSource,
    ParquetMatchMetadataSource,
    ParquetResultSink,
    ParquetXtSource,
)
from analytics.action_context.work_unit import WorkUnit


def _seed_unit(root: Path, *, provider: str, match: str) -> Path:
    d = root / provider / match
    d.mkdir(parents=True)
    pd.DataFrame({"frame": [0, 250], "x": [1.0, 2.0]}).to_parquet(d / "frames.parquet")
    pd.DataFrame({"action_id": [1, 2], "period_id": [1, 1]}).to_parquet(d / "actions.parquet")
    pd.DataFrame(
        {
            "zone_x": [0, 1, 0, 1],
            "zone_y": [0, 0, 1, 1],
            "xt_value": [0.1, 0.2, 0.3, 0.4],
        }
    ).to_parquet(root / "xt_grid.parquet")
    pd.DataFrame({"home_team_id": ["TEAM_A"], "home_start_left": [True]}).to_parquet(d / "meta.parquet")
    return d


def test_tracking_frame_source_returns_frames(tmp_path: Path) -> None:
    _seed_unit(tmp_path, provider="idsse", match="J03WMX")
    wu = WorkUnit(provider="idsse", match_id="J03WMX")
    bundle = ParquetFrameSource(tmp_path).frames(wu)
    assert bundle.tier == "tracking"
    assert len(bundle.frames) == 2


def test_event_only_frame_source_is_empty(tmp_path: Path) -> None:
    (tmp_path / "wyscout" / "M1").mkdir(parents=True)
    pd.DataFrame({"action_id": [1]}).to_parquet(tmp_path / "wyscout" / "M1" / "actions.parquet")
    wu = WorkUnit(provider="wyscout", match_id="M1")
    bundle = ParquetFrameSource(tmp_path).frames(wu)
    assert bundle.tier == "event_only"
    assert bundle.frames.empty


def test_statsbomb_sb360_tier_when_freeze_frames_present(tmp_path: Path) -> None:
    d = tmp_path / "statsbomb" / "M2"
    d.mkdir(parents=True)
    pd.DataFrame({"action_id": [1]}).to_parquet(d / "sb360.parquet")
    wu = WorkUnit(provider="statsbomb", match_id="M2")
    bundle = ParquetFrameSource(tmp_path).frames(wu)
    assert bundle.tier == "sb360"


def test_actions_source_roundtrip(tmp_path: Path) -> None:
    _seed_unit(tmp_path, provider="idsse", match="J03WMX")
    wu = WorkUnit(provider="idsse", match_id="J03WMX")
    actions = ParquetActionsSource(tmp_path).actions(wu)
    assert list(actions["action_id"]) == [1, 2]


def test_xt_source_reconstructs_grid(tmp_path: Path) -> None:
    _seed_unit(tmp_path, provider="idsse", match="J03WMX")
    grid, n_x, n_y = ParquetXtSource(tmp_path).grid()
    assert (n_x, n_y) == (2, 2)
    arr = np.array(grid)
    assert arr.shape == (2, 2)
    assert arr[1, 0] == 0.3  # zone_y=1, zone_x=0


def test_metadata_source_plain(tmp_path: Path) -> None:
    _seed_unit(tmp_path, provider="idsse", match="J03WMX")
    wu = WorkUnit(provider="idsse", match_id="J03WMX")
    meta = ParquetMatchMetadataSource(tmp_path).metadata(wu)
    assert meta.home_team_id == "TEAM_A"
    assert meta.home_start_left is True
    assert meta.gs_team_side_to_id is None


def test_metadata_source_gradientsports_json(tmp_path: Path) -> None:
    d = tmp_path / "gradientsports" / "G1"
    d.mkdir(parents=True)
    pd.DataFrame(
        {
            "home_team_id": ["HOME"],
            "home_start_left": [False],
            "gs_team_side_to_id_json": [json.dumps({"home": "HOME", "away": "AWAY"})],
            "gs_jersey_to_player_id_json": [json.dumps({"home\t7": "P7", "away\t9": "P9"})],
            "gs_gk_player_ids_json": [json.dumps(["P1", "P2"])],
        }
    ).to_parquet(d / "meta.parquet")
    wu = WorkUnit(provider="gradientsports", match_id="G1")
    meta = ParquetMatchMetadataSource(tmp_path).metadata(wu)
    assert meta.gs_team_side_to_id == {"home": "HOME", "away": "AWAY"}
    assert meta.gs_jersey_to_player_id == {("home", "7"): "P7", ("away", "9"): "P9"}
    assert meta.gs_gk_player_ids == ["P1", "P2"]


def test_result_sink_writes_and_counts(tmp_path: Path) -> None:
    wu = WorkUnit(provider="idsse", match_id="J03WMX")
    sink = ParquetResultSink(tmp_path)
    n = sink.write(wu, pd.DataFrame({"action_id": [1, 2, 3]}))
    assert n == 3
    written = pd.read_parquet(tmp_path / "idsse" / "J03WMX" / "result.parquet")
    assert len(written) == 3


def test_period_scoped_unit_dir(tmp_path: Path) -> None:
    d = tmp_path / "idsse" / "J03WMX_p1"
    d.mkdir(parents=True)
    pd.DataFrame({"frame": [0]}).to_parquet(d / "frames.parquet")
    wu = WorkUnit(provider="idsse", match_id="J03WMX", period=1)
    bundle = ParquetFrameSource(tmp_path).frames(wu)
    assert len(bundle.frames) == 1
