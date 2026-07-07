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

# shooter_attacks_high_x -> does the shooting team attack the HIGH-x goal in the canonical
# home-LTR frame. In home-LTR the HOME team attacks toward x=105 (high); the away team toward
# x=0 (low). We DERIVE the flag from (shooter_team_id == home_team_id) — NOT from any
# ``team_attacking_direction`` input column (that column does not exist on bronze.spadl_actions;
# reading it silently produced an all-NA orientation, 2026-07-07 live finding). The output
# ``team_attacking_direction`` string is then derived FROM the flag for provenance.
_HIGH_X_DIRECTION = "ltr"  # home team (attacks high x) in the home-LTR frame
_LOW_X_DIRECTION = "rtl"  # away team (attacks low x)


def _shot_type_id() -> int:
    """The canonical SPADL ``shot`` ``type_id`` from silly-kicks' authoritative ``actiontypes`` list.

    Drift-safe (mirrors ``enrich._set_piece_restart_type_ids``): the NAME ``"shot"`` is the source of
    truth; the id follows the canonical list. Lazy import — silly-kicks is heavyweight. Used to filter
    shot actions by ``type_id`` (bronze.spadl_actions has ``type_id``, NOT ``type_name``).
    """
    from silly_kicks.spadl.config import actiontypes

    return list(actiontypes).index("shot")


def _derive_shooter_attacks_high_x(shooter_team_id: pd.Series, home_team_id: str | None) -> pd.Series:
    """Nullable-boolean high-x flag: ``True`` iff the shooter's team is the HOME team.

    In the canonical home-LTR frame the home team attacks the high-x goal. ``shooter_team_id`` and
    ``home_team_id`` MUST both be in the frame-compatible (native) id space — the caller applies the
    ``_resolve_enrichment_identity`` mutate contract before calling. Returns ``NA`` for rows whose
    ``shooter_team_id`` is missing, and all-``NA`` when ``home_team_id`` is unknown (never guesses).
    """
    out = pd.Series(pd.NA, index=shooter_team_id.index, dtype="boolean")
    if home_team_id is None:
        return out
    home = str(home_team_id)
    known = shooter_team_id.notna()
    out[known] = shooter_team_id[known].astype("string") == home
    return out


def build_tracking_snapshots(
    shot_actions: pd.DataFrame, frames: pd.DataFrame, *, home_team_id: str | None = None
) -> pd.DataFrame:
    """Build per-player pre-shot snapshot rows for tracking-provider shots.

    Parameters
    ----------
    shot_actions : pd.DataFrame
        Shot actions in canonical SPADL. Required columns: ``action_id``, ``match_key``,
        ``team_id`` (the shooter's team, in the FRAME-COMPATIBLE native id space), ``data_source``.
        Optional: ``type_id`` (canonical SPADL int; used to defensively filter to ``shot`` actions
        when present — bronze.spadl_actions has ``type_id``, NOT ``type_name``).

        IDENTITY CONTRACT: ``team_id`` MUST be in the same id space as the frames' ``team_id`` — the
        caller applies the AC pipeline's ``_resolve_enrichment_identity`` mutate contract first
        (native strings for idsse/skillcorner/gradientsports, ``"Home"``/``"Away"`` for metrica).
        A hashed BIGINT ``team_id`` (the raw bronze value) will NOT match the native frame ids and
        makes ``is_teammate`` resolve all-zero (2026-07-07 live finding).
    frames : pd.DataFrame
        Player rows ALREADY linked to their shot's ``action_id`` (the Spark path performs the
        silly-kicks ``link_actions_to_frames`` linkage upstream; the unit tests supply pre-linked
        frames directly). Required columns: ``action_id``, ``player_id``, ``team_id``,
        ``is_goalkeeper``, ``x``, ``y``. A ``is_ball`` column, if present, is used to drop the ball
        row (full player set only — NO visibility filter). Coordinates are canonical home-LTR
        SPADL 105x68 and are passed through unchanged.
    home_team_id : str | None
        The HOME team's id in the frame-compatible space (from ``MatchMeta.home_team_id`` /
        ``convert_to_frames``). ``shooter_attacks_high_x`` is derived as
        ``(shooter_team_id == home_team_id)`` — home attacks the high-x goal in the home-LTR frame.
        ``None`` leaves ``shooter_attacks_high_x`` NA (never guesses).

    Returns
    -------
    pd.DataFrame
        One row per (shot, player) with columns ``_SNAPSHOT_COLUMNS``. ``is_keeper`` /
        ``is_teammate`` are 0/1 ints; ``set_cardinality`` is the player count in that shot's frame;
        ``shooter_attacks_high_x`` is a nullable boolean and ``team_attacking_direction`` is its
        ``"ltr"``/``"rtl"`` provenance string (derived from the flag, NA when the flag is NA).
    """
    if shot_actions.empty or frames.empty:
        return pd.DataFrame(columns=_SNAPSHOT_COLUMNS)

    shots = shot_actions
    if "type_id" in shots.columns:
        shots = shots[shots["type_id"] == _shot_type_id()]
    if shots.empty:
        return pd.DataFrame(columns=_SNAPSHOT_COLUMNS)

    # Per-shot metadata carried onto every player row. ``team_id`` is the SHOOTER's team (in the
    # frame-compatible id space per the IDENTITY CONTRACT above); rename it so it does not collide
    # with the frame's per-player ``team_id`` in the merge. Orientation is derived from home/away,
    # and the provenance ``team_attacking_direction`` string follows the derived flag.
    shooter_team = shots["team_id"].astype("string")
    shooter_attacks_high_x = _derive_shooter_attacks_high_x(shooter_team, home_team_id)
    direction = shooter_attacks_high_x.map({True: _HIGH_X_DIRECTION, False: _LOW_X_DIRECTION}).astype("string")
    meta = pd.DataFrame(
        {
            "action_id": shots["action_id"].to_numpy(),
            "match_key": shots["match_key"].to_numpy(),
            "data_source": shots["data_source"].to_numpy(),
            "shooter_team_id": shooter_team.to_numpy(),
            "team_attacking_direction": direction.to_numpy(),
            "shooter_attacks_high_x": shooter_attacks_high_x.to_numpy(),
        }
    ).drop_duplicates("action_id", keep="last")

    fr = frames.copy()
    if "is_ball" in fr.columns:
        fr = fr[~fr["is_ball"].fillna(False).astype(bool)]
    # The AC-converted frame carries its OWN copies of some meta-owned columns — notably a per-frame
    # ``team_attacking_direction`` emitted by the silly-kicks / sk_frame_adapters builder. The
    # freeze-frame builder OWNS the derived/meta values, so drop any frame column that ``meta`` also
    # provides (except the ``action_id`` join key) BEFORE the merge. Otherwise pandas suffixes the
    # collision (``team_attacking_direction_x``/``_y``) and ``fr["team_attacking_direction"]`` no
    # longer resolves → KeyError (live GS/SC 2026-07-07). Derived FROM ``meta.columns`` so the guard
    # can never drift as ``meta`` gains keys, and covers ALL such collisions, not just today's.
    _meta_owned = [c for c in meta.columns if c != "action_id" and c in fr.columns]
    if _meta_owned:
        fr = fr.drop(columns=_meta_owned)
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
    home_team_id: str | None = None,
) -> pd.DataFrame:
    """Per-match integration wrapper: link shot actions to frames, then run the pandas core.

    This is the entry the driver (Task 0.5's writer) calls once per ``(match, period)`` work unit.
    It is NOT run in the local unit tests (no Spark / live data); its correctness is covered by the
    pipeline e2e. It stays thin: linkage -> attach ``action_id`` to the frame player rows ->
    delegate to :func:`build_tracking_snapshots`.

    ``actions_df`` is the canonical-SPADL action frame for the match (must carry ``action_id``,
    ``match_key``, ``team_id``, ``data_source``, ``type_id``, ``period_id``). Its ``team_id`` MUST
    already be in the frame-compatible native id space — the caller applies
    ``_resolve_enrichment_identity`` before calling (see :func:`build_tracking_snapshots`' IDENTITY
    CONTRACT). ``tracking_df`` is the home-LTR AC result-frame set for the match
    (``sk_frame_adapters`` / silly-kicks builder output: ``frame_id``, ``period_id``, ``player_id``,
    ``team_id``, ``is_goalkeeper``, ``is_ball``, ``x``, ``y``, ...). ``home_team_id`` (frame-compatible)
    drives ``shooter_attacks_high_x`` and is threaded to the pandas core.

    Mirrors ``enrich.py``'s Step 1: ``link_actions_to_frames(out, tracking_df, on_low_coverage=
    "ignore")`` returns ``links`` with columns ``(action_id, frame_id, ...)`` (no ``period_id`` — we
    pull it from the action, exactly as ``_fill_possession_from_set_piece_actions`` does).
    """
    from silly_kicks.tracking import link_actions_to_frames

    # bronze.spadl_actions carries ``type_id`` (canonical SPADL int), NOT ``type_name``.
    shots = actions_df[actions_df["type_id"] == _shot_type_id()] if "type_id" in actions_df.columns else actions_df
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

    return build_tracking_snapshots(shots, linked_frames, home_team_id=home_team_id)


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
