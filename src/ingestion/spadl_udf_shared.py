"""Shared post-conversion helpers for the 4 SPADL UDF closures.

Extracted in S5 (PR-LL3) to DRY the ~80 lines of duplicated logic across
_make_sb_spadl_udf, _make_ws_spadl_udf, _make_idsse_spadl_udf,
_make_metrica_spadl_udf.

These helpers run INSIDE applyInPandas UDF closures on Spark executors.
They import only pandas (+ stdlib). No Spark/dbt imports.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    import pandas as pd


def apply_player_id_native(
    actions: pd.DataFrame,
    *,
    source: str,
) -> pd.DataFrame:
    """Derive ``player_id_native`` from ``player_id``.

    MUST be called BEFORE the legacy BIGINT NULL-fill overwrites
    ``player_id`` with pd.NA.

    StatsBomb/Wyscout: float64-with-NaN -> Int64 -> string (avoids "3009.0").
    IDSSE/Metrica: already string-shaped -> direct string cast, with empty
    strings normalised to pd.NA so dim_players JOINs produce NULL (not a
    failed match on ``''``).
    """
    import pandas as _pd

    if source in ("statsbomb", "wyscout"):
        actions["player_id_native"] = actions["player_id"].astype("Int64").astype("string")
    else:
        col = actions["player_id"].astype("string")
        col = col.where(col != "", other=_pd.NA)  # type: ignore[arg-type]  # pd.NA is a valid `other` at runtime; pandas-stubs over-narrows it
        actions["player_id_native"] = col
    return actions


def apply_match_level_natives(
    actions: pd.DataFrame,
    *,
    home_team_id_native: str,
    competition_native_id: str,
    season_native_id: str,
    match_id_native: str,
) -> pd.DataFrame:
    """Populate match-level constant native identifier columns."""
    actions["home_team_id_native"] = home_team_id_native
    actions["competition_native_id"] = competition_native_id
    actions["season_native_id"] = season_native_id
    actions["match_id_native"] = match_id_native
    return actions


def null_fill_statsbomb_columns(
    actions: pd.DataFrame,
    *,
    n: int,
) -> pd.DataFrame:
    """NULL-fill the 4 statsbomb_* namespace columns for non-SB sources."""
    import pandas as _pd

    actions["statsbomb_possession_id"] = _pd.array([_pd.NA] * n, dtype="Int64")
    actions["statsbomb_possession_team_id"] = _pd.array([_pd.NA] * n, dtype="Int64")
    actions["statsbomb_play_pattern"] = _pd.array([_pd.NA] * n, dtype="object")
    actions["statsbomb_under_pressure"] = _pd.array([_pd.NA] * n, dtype="boolean")
    return actions


def cast_enrichment_dtypes(actions: pd.DataFrame) -> pd.DataFrame:
    """Cast post-enrichment columns to nullable dtypes for PyArrow."""
    actions["action_id"] = actions["action_id"].astype("Int64")
    actions["possession_id_heuristic"] = actions["possession_id_heuristic"].astype("Int64")
    actions["gk_role"] = actions["gk_role"].astype("object")
    actions["gk_was_distributing"] = actions["gk_was_distributing"].astype("boolean")
    actions["gk_was_engaged"] = actions["gk_was_engaged"].astype("boolean")
    actions["gk_actions_in_possession"] = actions["gk_actions_in_possession"].astype("Int64")
    actions["defending_gk_player_id"] = actions["defending_gk_player_id"].astype("Int64")
    return actions


def null_fill_tackle_qualifiers(
    actions: pd.DataFrame,
    *,
    n: int,
) -> pd.DataFrame:
    """NULL-fill the 8 tackle qualifier columns for non-IDSSE sources."""
    import pandas as _pd

    for _native_col in (
        "tackle_winner_player_id_native",
        "tackle_winner_team_id_native",
        "tackle_loser_player_id_native",
        "tackle_loser_team_id_native",
    ):
        actions[_native_col] = _pd.array([_pd.NA] * n, dtype="string")
    for _key_col in (
        "tackle_winner_player_key",
        "tackle_winner_team_key",
        "tackle_loser_player_key",
        "tackle_loser_team_key",
    ):
        actions[_key_col] = _pd.array([_pd.NA] * n, dtype="Int64")
    return actions
