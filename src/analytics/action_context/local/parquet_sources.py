"""Parquet-fixture adapters implementing the action-context ports.

Fixture layout::

    <root>/<provider>/<match>[_p<period>]/
        frames.parquet      # bronze tracking rows (raw cols incl. frame/frame_num)
        actions.parquet     # bronze.spadl_actions rows for the match
        xt_grid.parquet     # zone_x, zone_y, xt_value (global grid)
        meta.parquet        # 1 row: home_team_id, home_start_left [, gs_*_json]
        sb360.parquet       # (statsbomb only) pre-built snapshot frame -> sb360 tier. (ADR-058 Task 7
                            #   will switch this to RAW bronze.statsbomb_360 rows + in-core snapshots.)

Tier resolution mirrors production (frames-required; ADR-057): tracking providers ->
``tracking`` (via resolve_frame_tier); statsbomb -> ``sb360`` (sb360.parquet present).
A statsbomb dir WITHOUT sb360.parquet — and any non-AC provider — is OUT OF SCOPE and
raises (discovery never enqueues it).
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import TYPE_CHECKING, Any

import numpy as np
import pandas as pd

from analytics.action_context.work_unit import FrameBundle, MatchMeta, provider_tier, resolve_frame_tier

if TYPE_CHECKING:
    from analytics.action_context.work_unit import WorkUnit


def _unit_dir(root: Path, wu: WorkUnit) -> Path:
    name = wu.match_id if wu.period is None else f"{wu.match_id}_p{wu.period}"
    return root / wu.provider / name


class ParquetFrameSource:
    def __init__(self, root: str | Path) -> None:
        self._root = Path(root)

    def frames(self, wu: WorkUnit) -> FrameBundle:
        d = _unit_dir(self._root, wu)
        # provider_tier raises for non-AC providers (frames-required; ADR-057); resolve_frame_tier
        # is THE single static->runtime mapping (tracking -> tracking, statsbomb -> sb360).
        frame_tier = resolve_frame_tier(provider_tier(wu))
        if frame_tier == "tracking":
            return FrameBundle(tier="tracking", frames=pd.read_parquet(d / "frames.parquet"))
        # statsbomb -> sb360: freeze-frames are required (a no-360 dir is out of scope, never enqueued).
        sb360 = d / "sb360.parquet"
        if not sb360.exists():
            raise FileNotFoundError(
                f"statsbomb unit {wu.match_id} has no sb360.parquet — out of action-context scope "
                f"(frames-required; ADR-057). Discovery should not have enqueued it."
            )
        return FrameBundle(tier="sb360", frames=pd.read_parquet(sb360))


class ParquetActionsSource:
    def __init__(self, root: str | Path) -> None:
        self._root = Path(root)

    def actions(self, wu: WorkUnit) -> pd.DataFrame:
        return pd.read_parquet(_unit_dir(self._root, wu) / "actions.parquet")


class ParquetXtSource:
    def __init__(self, root: str | Path) -> None:
        self._root = Path(root)

    def grid(self) -> tuple[list[list[float]], int, int]:
        # xt_grid.parquet is shared across units; read from any one fixture dir or a root copy.
        path = self._root / "xt_grid.parquet"
        if not path.exists():
            # fall back to the first per-unit copy found
            matches = list(self._root.glob("*/*/xt_grid.parquet"))
            if not matches:
                msg = f"No xt_grid.parquet under {self._root}"
                raise FileNotFoundError(msg)
            path = matches[0]
        rows = pd.read_parquet(path)
        zone_x = rows["zone_x"].to_numpy(dtype=int)
        zone_y = rows["zone_y"].to_numpy(dtype=int)
        values = rows["xt_value"].to_numpy(dtype=float)
        n_x = int(zone_x.max()) + 1
        n_y = int(zone_y.max()) + 1
        grid = np.zeros((n_y, n_x))
        grid[zone_y, zone_x] = values
        return grid.tolist(), n_x, n_y


class ParquetMatchMetadataSource:
    def __init__(self, root: str | Path) -> None:
        self._root = Path(root)

    def metadata(self, wu: WorkUnit) -> MatchMeta:
        row = pd.read_parquet(_unit_dir(self._root, wu) / "meta.parquet").iloc[0]

        def _json_or_none(col: str) -> Any:
            if col not in row.index or pd.isna(row[col]):
                return None
            return json.loads(row[col])

        gs_j2p_raw = _json_or_none("gs_jersey_to_player_id_json")
        gs_j2p = {tuple(k.split("\t")): v for k, v in gs_j2p_raw.items()} if gs_j2p_raw else None

        # silly-kicks 4.0+ ET flag — tolerate missing column (older fixtures predate it).
        et_flag: bool | None = None
        if "home_team_start_left_extratime" in row.index and pd.notna(row["home_team_start_left_extratime"]):
            et_flag = bool(row["home_team_start_left_extratime"])

        return MatchMeta(
            home_team_id=str(row["home_team_id"]),
            home_start_left=bool(row["home_start_left"]),
            home_team_start_left_extratime=et_flag,
            gs_team_side_to_id=_json_or_none("gs_team_side_to_id_json"),
            gs_jersey_to_player_id=gs_j2p,
            gs_gk_player_ids=_json_or_none("gs_gk_player_ids_json"),
        )


class ParquetResultSink:
    def __init__(self, root: str | Path) -> None:
        self._root = Path(root)

    def write(self, wu: WorkUnit, result_df: pd.DataFrame) -> int:
        d = _unit_dir(self._root, wu)
        d.mkdir(parents=True, exist_ok=True)
        result_df.to_parquet(d / "result.parquet", index=False)
        return len(result_df)
