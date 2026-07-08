"""StatsBomb-360 -> canonical-SPADL freeze-frame builder — the SB-360 twin of ``build_tracking_snapshots``.

Part of the Canonical-SPADL Pre-Shot xG v3 delivery (Task 2.1). For each ``shot`` action on a StatsBomb
match, this emits per-player rows from the shot's 360 freeze-frame in canonical SPADL 105x68, conforming
to ``analytics.action_context.tracking_snapshots._SHOT_FF_COLUMNS`` (the same 12-column persisted schema
the tracking-provider path produces).

WHY NO ORIENTATION STEP (verified against LIVE data):
    StatsBomb event + 360 data is ALREADY shooter-normalized — the attacking team always shoots toward
    high-x. Live: 99.9% of statsbomb shots have ``start_x >= 52.5`` in BOTH periods, and for two real
    shots the actor's raw 360 ``location`` converted via ``_convert_locations`` lands EXACTLY (0.0000 m)
    on the shot's ``fct_action_values.start_x/y``. So the freeze-frame builder needs only coordinate
    conversion — the SAME ``silly_kicks`` transform the shot action's SPADL conversion used, keeping
    frame and action byte-consistent — and stamps ``shooter_attacks_high_x = True`` as a constant. A low
    ``start_x`` is a long-range shot, not a reversed orientation (reviewer-confirmed), so True-constant is
    MORE correct than a ``start_x`` heuristic.

Pure pandas/numpy (NO pyspark). ``silly_kicks`` is lazy-imported (heavyweight).
"""

from __future__ import annotations

import numpy as np
import pandas as pd

from analytics.action_context.sb360_snapshots import build_sb360_snapshots
from analytics.action_context.tracking_snapshots import _SHOT_FF_COLUMNS

# SB-360 is public StatsBomb data (ADR-064) and StatsBomb attack-normalizes to high-x, so both are
# per-provider constants for every SB-360 freeze-frame row.
_ACCESS_TIER = "public"
_TEAM_ATTACKING_DIRECTION = "ltr"
_DATA_SOURCE = "statsbomb"


def convert_statsbomb_locations_to_spadl(locations: pd.Series, shot_fidelity_version: int) -> np.ndarray:
    """Convert raw StatsBomb 120x80 ``[x, y]`` locations to canonical SPADL 105x68 coordinates.

    Thin verbatim wrapper over ``silly_kicks.spadl.statsbomb._convert_locations`` — the y-flip, cell-
    center offset, and fidelity handling all live there; we do NOT hand-roll a scale. This is the SAME
    transform the shot action's SPADL conversion applied, so a freeze-frame position and its shot action
    are byte-consistent (an actor row converts exactly onto the shot's ``start_x/y``).

    Parameters
    ----------
    locations : pd.Series
        Each element is a ``[x, y]`` list in raw StatsBomb 120x80 coordinates.
    shot_fidelity_version : int
        StatsBomb XY fidelity version (2 = high-fidelity 0.1-cell grid; else 1.0-cell).

    Returns
    -------
    np.ndarray
        ``(N, 2)`` array of SPADL 105x68 coordinates, row-aligned to ``locations``.
    """
    from silly_kicks.spadl.statsbomb import _convert_locations  # lazy: silly_kicks is heavyweight

    return _convert_locations(locations, shot_fidelity_version)


def build_sb360_freeze_frames(
    actions_df: pd.DataFrame, sb360_raw_df: pd.DataFrame, shot_fidelity_version: int
) -> pd.DataFrame:
    """Build per-player pre-shot freeze-frame rows for StatsBomb-360 shots.

    Parameters
    ----------
    actions_df : pd.DataFrame
        Canonical-SPADL shot actions for the match. Required columns: ``original_event_id`` (maps to the
        360 ``id``), ``action_id``, ``team_id`` (the shooter's native team id), ``match_key`` (driver-
        stamped, same as the tracking path).
    sb360_raw_df : pd.DataFrame
        Raw ``bronze.statsbomb_360`` rows: ``id`` (event uuid), ``actor``, ``teammate``, ``keeper``, and
        ``location`` (a JSON-ish ``"[x, y]"`` string in raw 120x80).
    shot_fidelity_version : int
        StatsBomb XY fidelity version for this match's shots.

    Returns
    -------
    pd.DataFrame
        One row per (shot, freeze-frame player) with columns ``_SHOT_FF_COLUMNS``. Coordinates are
        canonical SPADL 105x68; ``is_keeper`` / ``is_teammate`` are 0/1 ints; ``set_cardinality`` is the
        player count in that shot's frame; ``player_id`` is a synthetic, globally-unique id; the
        orientation / access columns are per-provider constants (see module docstring).
    """
    empty = pd.DataFrame(columns=list(_SHOT_FF_COLUMNS))

    # 1. Raw 120x80 snapshots: [action_id, team_id (resolved acting/opponent native), is_goalkeeper, x, y].
    snaps = build_sb360_snapshots(actions_df, sb360_raw_df)
    if snaps.empty:
        return empty
    snaps = snaps.reset_index(drop=True)

    # 2. Convert coords in-place (raw StatsBomb 120x80 -> canonical SPADL 105x68).
    locations = pd.Series(snaps[["x", "y"]].astype(float).to_numpy().tolist(), index=snaps.index)
    spadl = convert_statsbomb_locations_to_spadl(locations, shot_fidelity_version)
    snaps["x"] = spadl[:, 0]
    snaps["y"] = spadl[:, 1]

    # 3. Derive the remaining _SHOT_FF_COLUMNS.
    action_to_match_key = dict(zip(actions_df["action_id"], actions_df["match_key"], strict=False))
    action_to_team = dict(zip(actions_df["action_id"], actions_df["team_id"].astype(str), strict=False))

    match_key = snaps["action_id"].map(action_to_match_key)
    acting_team = snaps["action_id"].map(action_to_team)
    # build_sb360_snapshots already resolved teammate rows to the acting team, so equality recovers it.
    is_teammate = (snaps["team_id"].astype(str) == acting_team).astype("int64")
    is_keeper = snaps["is_goalkeeper"].astype(bool).astype("int64")
    set_cardinality = snaps.groupby("action_id")["team_id"].transform("size").astype("int64")

    # Synthetic globally-unique id — ``action_id`` is per-match (not global), so ``match_key`` MUST be in
    # the id or ids collide across matches (§5 invariant). SB-360 frames are anonymous; this is
    # positional-encoder training data, so the clearly-synthetic ``sb360_`` prefix is intended.
    within_frame_idx = snaps.groupby("action_id").cumcount()
    player_id = (
        "sb360_" + match_key.astype(str) + "_" + snaps["action_id"].astype(str) + "_" + within_frame_idx.astype(str)
    )

    out = pd.DataFrame(
        {
            "action_id": snaps["action_id"].to_numpy(),
            "match_key": match_key.to_numpy(),
            "data_source": _DATA_SOURCE,
            "player_id": player_id.to_numpy(),
            "x": snaps["x"].to_numpy(dtype="float64"),
            "y": snaps["y"].to_numpy(dtype="float64"),
            "is_keeper": is_keeper.to_numpy(),
            "is_teammate": is_teammate.to_numpy(),
            "set_cardinality": set_cardinality.to_numpy(),
            # StatsBomb attack-normalizes so the shooter always attacks high-x (nullable boolean True).
            "shooter_attacks_high_x": True,
            "team_attacking_direction": _TEAM_ATTACKING_DIRECTION,
            "access_tier": _ACCESS_TIER,
        }
    )
    out["shooter_attacks_high_x"] = out["shooter_attacks_high_x"].astype("boolean")
    return out[list(_SHOT_FF_COLUMNS)].reset_index(drop=True)
