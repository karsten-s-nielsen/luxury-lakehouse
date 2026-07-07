"""Pre-shot tracking snapshot builder — the tracking-provider twin of ``build_sb360_snapshots``.

Part of the Canonical-SPADL Pre-Shot xG Unification (Task 0.4). For each ``shot`` action on a
tracking match (gradientsports / skillcorner — any provider whose linked frame carries the full
player set), this emits per-player rows from the shot's linked frame:

    ``action_id``, ``match_key``, ``data_source``, ``player_id``, ``x``, ``y``,
    ``is_keeper``, ``is_teammate``, ``set_cardinality``,
    ``shooter_attacks_high_x``, ``team_attacking_direction``

Coordinates are canonical SPADL 105x68, home-LTR (the frames come out of the silly-kicks builders /
``sk_frame_adapters`` already oriented — see ADR-053/ADR-034). We DO NOT normalize here: the C2 port
``analytics.xg_freeze_frame.normalize_freeze_frame`` applies the shooter-orientation reflection later
at feature-build time. To let it do so, we carry the per-shot orientation as ``shooter_attacks_high_x``
(derived from ``team_attacking_direction``) plus the raw ``team_attacking_direction`` string.

Actor-inclusion convention (M3): ``build_sb360_snapshots`` performs NO actor-specific filtering — it
includes every freeze-frame row (StatsBomb 360 freeze-frames carry the acting player). We match that:
the full player set is kept (only the ball row is dropped), so the shooter is included.

Pure pandas/numpy (NO pyspark): both the Spark cogroup path and the local hexagon import the core, so
there is a single impl and local mirrors production. ``build_tracking_snapshots_spark`` is the thin
per-match integration wrapper that runs the silly-kicks action<->frame linkage before the core.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

import pandas as pd

if TYPE_CHECKING:
    from pyspark.sql.types import StructType

logger = logging.getLogger(__name__)

_SNAPSHOT_COLUMNS = [
    "action_id",
    "match_key",
    "data_source",
    "player_id",
    "x",
    "y",
    "is_keeper",
    "is_teammate",
    "set_cardinality",
    "shooter_attacks_high_x",
    "team_attacking_direction",
]

# ---------------------------------------------------------------------------
# Persisted bronze schema — ``bronze.shot_freeze_frames`` (Task 0.5, ADR-002 §4)
# ---------------------------------------------------------------------------
# One row per (shot action, player). This is the writer-side mirror of the
# canonical CREATE TABLE DDL in
# ``scripts/migrations/2026-07-05-shot-freeze-frames-ddl.sql``.
# ``src/tests/action_context/test_shot_freeze_frames_writer.py`` parses that DDL
# and asserts column-list + order equality against ``_SHOT_FF_COLUMNS`` — the two
# sources of truth cannot drift without a failing test.
#
# ``_ingested_at`` is appended by ``write_delta_table`` (NOT emitted by the
# builder), so it is DELIBERATELY absent from ``_SHOT_FF_COLUMNS`` and the
# StructType below — mirroring ``xg_model_v2._XG_V2_BRONZE_COLS``. The column
# order matches ``_SNAPSHOT_COLUMNS`` (the builder's output), so a snapshot frame
# maps positionally into the persisted table.
_TABLE_NAME = "shot_freeze_frames"

_SHOT_FF_COLUMNS: tuple[str, ...] = (
    "action_id",
    "match_key",
    "data_source",
    "player_id",
    "x",
    "y",
    "is_keeper",
    "is_teammate",
    "set_cardinality",
    "shooter_attacks_high_x",
    "team_attacking_direction",
)

# Column -> Spark SQL type category, consumed by ``_shot_ff_struct_type``. Kept in
# lockstep with the DDL types (BIGINT->long, INT->int, DOUBLE->double, BOOLEAN, STRING).
_SHOT_FF_TYPES: dict[str, str] = {
    "action_id": "long",
    "match_key": "long",
    "data_source": "string",
    "player_id": "string",
    "x": "double",
    "y": "double",
    "is_keeper": "int",
    "is_teammate": "int",
    "set_cardinality": "int",
    "shooter_attacks_high_x": "boolean",
    "team_attacking_direction": "string",
}

# team_attacking_direction -> does the shooting team attack the HIGH-x goal in the canonical
# home-LTR frame. "ltr" = attacks toward x=105 (high), "rtl" = toward x=0 (low). Anything else
# (missing / unexpected) leaves the orientation undecided (NA) for the feature-build step to handle.
_HIGH_X_DIRECTION = "ltr"
_LOW_X_DIRECTION = "rtl"


def _derive_shooter_attacks_high_x(direction: pd.Series) -> pd.Series:
    """Map a per-shot ``team_attacking_direction`` string to a nullable-boolean high-x flag."""
    norm = direction.astype("string").str.lower()
    out = pd.Series(pd.NA, index=direction.index, dtype="boolean")
    out[norm == _HIGH_X_DIRECTION] = True
    out[norm == _LOW_X_DIRECTION] = False
    return out


def build_tracking_snapshots(shot_actions: pd.DataFrame, frames: pd.DataFrame) -> pd.DataFrame:
    """Build per-player pre-shot snapshot rows for tracking-provider shots.

    Parameters
    ----------
    shot_actions : pd.DataFrame
        Shot actions in canonical SPADL. Required columns: ``action_id``, ``match_key``,
        ``team_id`` (the shooter's team), ``data_source``. Optional: ``type_name`` (used to
        defensively filter to shots when present) and ``team_attacking_direction`` (per-shot
        shooter orientation, used to derive ``shooter_attacks_high_x``).
    frames : pd.DataFrame
        Player rows ALREADY linked to their shot's ``action_id`` (the Spark path performs the
        silly-kicks ``link_actions_to_frames`` linkage upstream; the unit tests supply pre-linked
        frames directly). Required columns: ``action_id``, ``player_id``, ``team_id``,
        ``is_goalkeeper``, ``x``, ``y``. A ``is_ball`` column, if present, is used to drop the ball
        row (full player set only — NO visibility filter). Coordinates are canonical home-LTR
        SPADL 105x68 and are passed through unchanged.

    Returns
    -------
    pd.DataFrame
        One row per (shot, player) with columns ``_SNAPSHOT_COLUMNS``. ``is_keeper`` /
        ``is_teammate`` are 0/1 ints; ``set_cardinality`` is the player count in that shot's frame;
        ``shooter_attacks_high_x`` is a nullable boolean.
    """
    if shot_actions.empty or frames.empty:
        return pd.DataFrame(columns=_SNAPSHOT_COLUMNS)

    shots = shot_actions
    if "type_name" in shots.columns:
        shots = shots[shots["type_name"].astype("string").str.lower() == "shot"]
    if shots.empty:
        return pd.DataFrame(columns=_SNAPSHOT_COLUMNS)

    # Per-shot metadata carried onto every player row. ``team_id`` is the SHOOTER's team; rename it
    # so it does not collide with the frame's per-player ``team_id`` in the merge.
    direction = (
        shots["team_attacking_direction"]
        if "team_attacking_direction" in shots.columns
        else pd.Series(pd.NA, index=shots.index, dtype="string")
    )
    meta = pd.DataFrame(
        {
            "action_id": shots["action_id"].to_numpy(),
            "match_key": shots["match_key"].to_numpy(),
            "data_source": shots["data_source"].to_numpy(),
            "shooter_team_id": shots["team_id"].astype("string").to_numpy(),
            "team_attacking_direction": direction.astype("string").to_numpy(),
            "shooter_attacks_high_x": _derive_shooter_attacks_high_x(direction).to_numpy(),
        }
    ).drop_duplicates("action_id", keep="last")

    fr = frames.copy()
    if "is_ball" in fr.columns:
        fr = fr[~fr["is_ball"].fillna(False).astype(bool)]
    fr = fr.merge(meta, on="action_id", how="inner")  # drops unlinked frames / non-shot actions
    if fr.empty:
        return pd.DataFrame(columns=_SNAPSHOT_COLUMNS)

    is_teammate = (fr["team_id"].astype("string") == fr["shooter_team_id"]).astype("int64")
    is_keeper = fr["is_goalkeeper"].astype(bool).astype("int64")
    set_cardinality = fr.groupby("action_id")["player_id"].transform("size").astype("int64")

    out = pd.DataFrame(
        {
            "action_id": fr["action_id"].to_numpy(),
            "match_key": fr["match_key"].to_numpy(),
            "data_source": fr["data_source"].to_numpy(),
            "player_id": fr["player_id"].to_numpy(),
            "x": fr["x"].to_numpy(dtype="float64"),
            "y": fr["y"].to_numpy(dtype="float64"),
            "is_keeper": is_keeper.to_numpy(),
            "is_teammate": is_teammate.to_numpy(),
            "set_cardinality": set_cardinality.to_numpy(),
            "shooter_attacks_high_x": fr["shooter_attacks_high_x"].to_numpy(),
            "team_attacking_direction": fr["team_attacking_direction"].to_numpy(),
        }
    )
    out["shooter_attacks_high_x"] = out["shooter_attacks_high_x"].astype("boolean")
    return out.reset_index(drop=True)


def build_tracking_snapshots_spark(
    actions_df: pd.DataFrame,
    tracking_df: pd.DataFrame,
    *,
    shot_type_name: str = "shot",
) -> pd.DataFrame:
    """Per-match integration wrapper: link shot actions to frames, then run the pandas core.

    This is the entry the Spark cogroup UDF (Task 0.5's writer) calls once per ``(match, period)``
    work unit. It is NOT run in the local unit tests (no Spark / live data); its correctness is
    covered by the pipeline e2e. It stays thin: linkage -> attach ``action_id`` to the frame player
    rows -> delegate to :func:`build_tracking_snapshots`.

    ``actions_df`` is the canonical-SPADL action frame for the match (must carry ``action_id``,
    ``match_key``, ``team_id``, ``data_source``, ``type_name``, ``period_id`` and, ideally,
    ``team_attacking_direction``). ``tracking_df`` is the home-LTR AC result-frame set for the match
    (``sk_frame_adapters`` / silly-kicks builder output: ``frame_id``, ``period_id``, ``player_id``,
    ``team_id``, ``is_goalkeeper``, ``is_ball``, ``x``, ``y``, ...).

    Mirrors ``enrich.py``'s Step 1: ``link_actions_to_frames(out, tracking_df, on_low_coverage=
    "ignore")`` returns ``links`` with columns ``(action_id, frame_id, ...)`` (no ``period_id`` — we
    pull it from the action, exactly as ``_fill_possession_from_set_piece_actions`` does).
    """
    from silly_kicks.tracking import link_actions_to_frames

    shots = actions_df[actions_df["type_name"].astype("string").str.lower() == shot_type_name.lower()]
    if shots.empty or tracking_df.empty:
        return pd.DataFrame(columns=_SNAPSHOT_COLUMNS)

    links, _report = link_actions_to_frames(shots, tracking_df, on_low_coverage="ignore")
    if links is None or links.empty:
        return pd.DataFrame(columns=_SNAPSHOT_COLUMNS)

    # links: (action_id, frame_id, ...). Pull period_id from the action (links carries none) and
    # join to the per-player frame rows on the full (period_id, frame_id) key — frame_id can repeat
    # across periods, so period_id disambiguates (matches enrich.py's set-piece merge key).
    keyed = links[["action_id", "frame_id"]].merge(shots[["action_id", "period_id"]], on="action_id", how="inner")
    linked_frames = keyed.merge(tracking_df, on=["frame_id", "period_id"], how="inner")
    if linked_frames.empty:
        return pd.DataFrame(columns=_SNAPSHOT_COLUMNS)

    return build_tracking_snapshots(shots, linked_frames)


def _shot_ff_struct_type() -> StructType:
    """Build the Spark ``StructType`` for ``bronze.shot_freeze_frames``.

    Lazy pyspark import so this module imports without Spark (pyspark is only present in
    the Databricks runtime, not in local CI for the pure-pandas tests). Reads from
    ``_SHOT_FF_COLUMNS`` / ``_SHOT_FF_TYPES`` so the column list stays the single source of
    truth. This is the ``applyInPandas`` output schema for the per-(match, period) cogroup
    that runs :func:`build_tracking_snapshots_spark`, and the explicit schema the writer uses
    when materializing the collected snapshot set.

    ``_ingested_at`` is intentionally omitted — ``write_delta_table`` appends it.
    """
    from pyspark.sql.types import (
        BooleanType,
        DoubleType,
        IntegerType,
        LongType,
        StringType,
        StructField,
        StructType,
    )

    type_map = {
        "long": LongType(),
        "int": IntegerType(),
        "double": DoubleType(),
        "boolean": BooleanType(),
        "string": StringType(),
    }
    return StructType([StructField(name, type_map[_SHOT_FF_TYPES[name]], True) for name in _SHOT_FF_COLUMNS])
