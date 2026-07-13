"""Provider bronze->frames converters (pure pandas/numpy/scipy).

COPIED VERBATIM (M4) from ``ingestion.tracking_context`` (idsse/metrica/skillcorner) and
``ingestion.action_context`` (gradientsports). The legacy copies are left UNTOUCHED so they remain
the differential oracle; ``src/tests/action_context/test_convert_drift.py`` asserts the remaining
copies stay identical. De-duplicate only after the legacy pipelines retire.

``_derive_velocities_savgol`` is GONE from both copies (ADR-067, delete-and-depend): it
re-implemented silly-kicks' velocity derivation and had dropped upstream's ``len(x_vals) <= 1``
guard, crashing on 1-frame tracks and zeroing a whole work unit. Velocity now comes from the
silly-kicks ``preprocess=`` seam. ``test_convert_drift`` asserts it does not re-grow.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

import numpy as np

if TYPE_CHECKING:
    import pandas as pd

_GS_FRAME_RATE = 30


def _bronze_gradientsports_to_converter_input(
    trk_pdf: pd.DataFrame,
    *,
    team_side_to_id: dict[str, str],
    jersey_to_player_id: dict[tuple[str, str], str],
    gk_player_ids: frozenset[str],
) -> pd.DataFrame:
    """Map bronze ``gradientsports_tracking`` columns to silly-kicks converter input.

    Args:
        trk_pdf: Bronze tracking rows (columns per _GRADIENTSPORTS_TRACKING_SELECT_COLS).
        team_side_to_id: Maps team_side ("home"/"away") -> native team_id string.
        jersey_to_player_id: Maps (team_side, jersey_num) -> native player_id string.
        gk_player_ids: Set of player_id strings who are goalkeepers.

    Returns:
        DataFrame with columns matching silly_kicks.tracking.gradientsports.EXPECTED_INPUT_COLUMNS.
    """
    import pandas as _pd

    result = _pd.DataFrame()
    result["game_id"] = trk_pdf["match_id"]
    result["period_id"] = trk_pdf["period"].astype("Int64")
    result["frame_id"] = trk_pdf["frame_num"].astype("Int64")
    result["time_seconds"] = trk_pdf["period_elapsed_time"].astype("float64")
    result["frame_rate"] = _GS_FRAME_RATE
    result["is_ball"] = trk_pdf["is_ball"].fillna(False)
    result["x_centered"] = trk_pdf["x"].astype("float64")
    result["y_centered"] = trk_pdf["y"].astype("float64")
    result["z"] = trk_pdf["z"].astype("float64")
    result["speed_native"] = np.nan  # Derived by converter/post-processing
    result["ball_state"] = "alive"  # GS does not provide per-frame ball state

    # Map team_side -> team_id; ball rows get NaN team_id
    result["team_id"] = trk_pdf["team_side"].map(team_side_to_id)

    # Map (team_side, jersey_num) -> player_id; ball rows get NaN
    _side = trk_pdf["team_side"].fillna("")
    _jersey = trk_pdf["jersey_num"].fillna("")
    result["player_id"] = [jersey_to_player_id.get((s, j)) for s, j in zip(_side, _jersey, strict=False)]

    # is_goalkeeper from roster
    result["is_goalkeeper"] = result["player_id"].isin(gk_player_ids)
    # Ball rows: explicit False for is_goalkeeper
    result.loc[result["is_ball"] == True, "is_goalkeeper"] = False  # noqa: E712

    return result.sort_values(["frame_id", "is_ball"]).reset_index(drop=True)


def _coerce_gradientsports_frame_ids_to_native_str(frames: pd.DataFrame) -> pd.DataFrame:
    """Coerce GS frame ``player_id``/``team_id`` from Int64 back to native string in place.

    silly-kicks' ``GRADIENTSPORTS_TRACKING_FRAMES_COLUMNS`` schema forces
    ``player_id``/``team_id`` to ``Int64`` inside ``convert_to_frames``. But every
    downstream consumer compares frame ids to the NATIVE-STRING action ids —
    ``_resolve_action_frame_context`` does ``player_id_frame == player_id_action`` /
    ``team_id_frame != team_id_action`` (actions carry ``player_id_native`` /
    ``team_id_native`` after ``_resolve_enrichment_identity``), and the silly-kicks
    kernels compare ``frames["team_id"] == home_team_id`` where ``home_team_id`` is a
    ``str``. ``Int64(366) == "366"`` is ``False`` → GS carrier/possession/actor/opponent/
    defensive-line resolution silently breaks. SkillCorner (``.astype(str)``) and IDSSE
    (sportec ``object`` schema) already emit native-string frame ids; GS is the schema
    outlier, so we realign it here. NA (ball rows) → ``None``. See project memory
    ``project_gradientsports_player_id_space_bug``.
    """
    for col in ("player_id", "team_id"):
        if col in frames.columns:
            # Int64 -> StringDtype ("366"/<NA>, no ".0") -> object (native str + NA for ball).
            frames[col] = frames[col].astype("string").astype(object)
    return frames
