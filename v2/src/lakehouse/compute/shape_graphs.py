"""shape_graphs compute (spec D3/D8; plan Task 6): a layer-1 tracking compute.

Per match the step builds the canonical silly-kicks frame (`frames.build_match_frame`, already oriented left-to-right)
and, per team per frame, computes the Delaunay shape graph over the team's OUTFIELD players and infers each player's
tactical position label (the 5x5 vertical x horizontal decomposition). The per-player label is written to the
`shape_graph_labels` bronze, keyed per `(canonical_match_key, period, frame, canonical_player_key)`. The layer-2 mart
aggregates it to each player's modal position + time-in-position (spec D-open-1).

`shape_graph_values()` is the PURE core (frame -> arrow rows), unit-tested on a fixture frame. This module holds only
output logic: it is digested into the compute input key (`input_key.TRACKING_CODE_MODULES`). The per-match driver that
reads the catalog and writes bronze is `tracking_steps.TRACKING_FRAME_METRICS` (perf spec §7.3).

`compute_shape_graph`/`infer_positions` are a PRIVATE silly-kicks path (`silly_kicks.tracking._shape_graph`): the
public `add_shape_graph` emits team-level metrics over ACTIONS, not the per-player labels the mart needs. Pin recorded
here; a public per-player seam would remove the private import.
"""

# pandas/silly_kicks are stub-less; relax the strict "unknown"-family rules (see src/lakehouse/compute/frames.py).
# pyright: reportUnknownMemberType=false, reportUnknownVariableType=false, reportUnknownArgumentType=false
from __future__ import annotations

from typing import TYPE_CHECKING, Any

import pandas as pd  # pyright: ignore[reportMissingTypeStubs]
import pyarrow as pa
from silly_kicks.tracking._shape_graph import (  # pyright: ignore[reportMissingTypeStubs]
    compute_shape_graph,
    infer_positions,
)

from lakehouse.compute.control import ComputeAbandoned

if TYPE_CHECKING:
    from collections.abc import Callable

__all__ = ["SHAPE_GRAPH_SCHEMA", "shape_graph_values"]

#: The LTR frame (build_match_frame -> convert_to_frames output_convention="ltr") makes every team attack +x.
_ATTACKING_DIRECTION = 1.0
#: Delaunay needs at least a triangle; fewer outfield players in a frame yields no shape graph.
_MIN_OUTFIELD = 3

SHAPE_GRAPH_SCHEMA: pa.Schema = pa.schema(
    [
        pa.field("canonical_match_key", pa.int64()),
        pa.field("period", pa.int32()),
        pa.field("frame", pa.int32()),
        pa.field("canonical_player_key", pa.int64()),
        pa.field("position_label", pa.string()),
    ]
)


def shape_graph_values(
    frame: pd.DataFrame,
    *,
    should_stop: Callable[[], bool] | None = None,
    check_every: int = 500,
) -> pa.Table:
    """Infer each outfield player's tactical position label per frame (PURE; no catalog, no bronze write).

    `frame` is one match's canonical `TRACKING_FRAMES_COLUMNS` (from `build_match_frame`, already LTR). Per team per
    frame the team's outfield (non-ball, non-goalkeeper) positions seed `compute_shape_graph`; `infer_positions` then
    labels each player (e.g. "RCB"). Goalkeepers and frames with fewer than three outfield players get no row.

    `should_stop` (perf spec §7.5) is called every `check_every` team-frame groups; when it returns True the scorer
    raises `ComputeAbandoned` and returns nothing. It never changes the output of a finished call.
    """
    match_key = int(frame["game_id"].iloc[0])
    outfield: Any = frame[
        (~frame["is_ball"].astype("boolean").fillna(False))
        & (~frame["is_goalkeeper"].astype("boolean").fillna(False))
        & frame["x"].notna()
        & frame["y"].notna()
    ]
    periods: list[int] = []
    frames: list[int] = []
    players: list[int] = []
    labels: list[str] = []
    grouped = outfield.groupby(["period_id", "frame_id", "team_id"], sort=True, observed=True)
    for index, ((period_id, frame_id, _team), grp) in enumerate(grouped):
        if should_stop is not None and index % check_every == 0 and should_stop():
            raise ComputeAbandoned(f"shape_graphs: match {match_key} stopped at group {index}")
        if len(grp) < _MIN_OUTFIELD:
            continue
        positions = grp[["x", "y"]].to_numpy(dtype="float64")
        player_ids = [int(p) for p in grp["player_id"].tolist()]
        graph = compute_shape_graph(positions)
        inferred = list(infer_positions(graph, positions, _ATTACKING_DIRECTION))
        # A degenerate frame (collinear/near-collinear players -> Delaunay fails) yields fewer labels than players; its
        # shape graph is unreliable, so skip the whole frame rather than mislabel (spec D3: never invent a position).
        if len(inferred) != len(player_ids):
            continue
        for player_id, label in zip(player_ids, inferred, strict=True):
            periods.append(int(period_id))
            frames.append(int(frame_id))
            players.append(player_id)
            labels.append(str(label.label))
    return pa.table(
        {
            "canonical_match_key": pa.array([match_key] * len(periods), pa.int64()),
            "period": pa.array(periods, pa.int32()),
            "frame": pa.array(frames, pa.int32()),
            "canonical_player_key": pa.array(players, pa.int64()),
            "position_label": pa.array(labels, pa.string()),
        },
        schema=SHAPE_GRAPH_SCHEMA,
    )
