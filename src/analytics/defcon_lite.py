"""DEFCON-lite defensive valuation analytics module.

Tier 3 tabular approximation of the DEFCON framework (Kim et al. 2025).
Decomposes defensive contributions into four credit categories using
heuristic rules (Stage 1) and XGBoost value estimation (Stage 2).

Stage 1: Credit Assignment — heuristic rules on 360/tracking spatial data.
Stage 2: Value Estimation — XGBoost regressors trained on VAEP ground truth.

Coordinate system: SPADL 105x68 meters throughout.
"""

from __future__ import annotations

import enum
import math
from dataclasses import dataclass

import numpy as np
import pandas as pd
from xgboost import XGBRegressor

from analytics.array_utils import _col_f64


class CreditType(enum.Enum):
    """Defensive credit categories from the DEFCON framework."""

    INTERCEPT = "intercept"
    CONCEDE = "concede"
    DISTURB = "disturb"
    DETER = "deter"


@dataclass(frozen=True)
class DefconLiteParams:
    """Parameters for DEFCON-lite computation."""

    disturb_radius_m: float = 5.0
    deter_cone_angle_deg: float = 15.0
    pitch_length: float = 105.0  # SPADL meters
    pitch_width: float = 68.0  # SPADL meters


# Actions that qualify for Intercept credit
_INTERCEPT_ACTIONS = frozenset({"tackle", "interception", "clearance"})

# High-xT target for Deter cone: center of goal at x=105, y=34
_GOAL_CENTER_X = 105.0
_GOAL_CENTER_Y = 34.0

# SPADL action type mapping for feature encoding
_ACTION_TYPE_IDS: dict[str, int] = {
    "pass": 0,
    "cross": 1,
    "throw_in": 2,
    "freekick_crossed": 3,
    "freekick_short": 4,
    "corner_crossed": 5,
    "corner_short": 6,
    "take_on": 7,
    "foul": 8,
    "tackle": 9,
    "interception": 10,
    "shot": 11,
    "shot_penalty": 12,
    "shot_freekick": 13,
    "keeper_save": 14,
    "keeper_claim": 15,
    "keeper_punch": 16,
    "keeper_pick_up": 17,
    "clearance": 18,
    "bad_touch": 19,
    "non_action": 20,
    "dribble": 21,
    "goalkick": 22,
}


def _euclidean_dist(x1: float, y1: float, x2: float, y2: float) -> float:
    """Euclidean distance between two points."""
    return math.sqrt((x1 - x2) ** 2 + (y1 - y2) ** 2)


def _is_in_cone(
    defender_x: float,
    defender_y: float,
    ball_x: float,
    ball_y: float,
    target_x: float,
    target_y: float,
    cone_angle_deg: float,
) -> bool:
    """Check if a defender is within a cone from ball to target zone.

    The cone is centered on the vector from ball to target. Returns True
    if the angle from ball-to-target to ball-to-defender is within
    cone_angle_deg degrees.
    """
    vt_x = target_x - ball_x
    vt_y = target_y - ball_y
    vd_x = defender_x - ball_x
    vd_y = defender_y - ball_y

    mag_t = math.sqrt(vt_x**2 + vt_y**2)
    mag_d = math.sqrt(vd_x**2 + vd_y**2)

    if mag_t < 1e-9 or mag_d < 1e-9:
        return False

    cos_angle = (vt_x * vd_x + vt_y * vd_y) / (mag_t * mag_d)
    cos_angle = max(-1.0, min(1.0, cos_angle))
    angle_deg = math.degrees(math.acos(cos_angle))

    return angle_deg <= cone_angle_deg


def assign_defensive_credits(
    action: dict[str, object],
    defenders: pd.DataFrame,
    params: DefconLiteParams,
    pitch_control_fn: object | None = None,
) -> list[dict[str, object]]:
    """Assign defensive credits to nearby defenders for a single action.

    Priority: Intercept > Concede > Disturb > Deter.
    Each defender receives at most one credit per action.

    Args:
        action: Dict with keys event_id, match_id, competition_id, season_id,
            action_player_id, action_type, action_x, action_y, offensive_value.
        defenders: DataFrame with columns [player_id, team_id, x, y,
            velocity_x, velocity_y].
        params: DEFCON-lite parameters.
        pitch_control_fn: Optional callable(defenders_df, x, y) -> float.

    Returns:
        List of credit dicts, one per credited defender.
    """
    if defenders.empty:
        return []

    action_x = float(action["action_x"])  # type: ignore[arg-type]
    action_y = float(action["action_y"])  # type: ignore[arg-type]
    action_type = str(action["action_type"])
    offensive_value = float(action.get("offensive_value", 0.0))  # type: ignore[arg-type]

    credited: set[int] = set()
    results: list[dict[str, object]] = []

    def _base_credit(idx: int, credit_type: CreditType, confidence: str) -> dict[str, object]:
        row = defenders.iloc[idx]
        dx = float(row["x"])
        dy = float(row["y"])
        dist = _euclidean_dist(dx, dy, action_x, action_y)

        pc_at_action = 0.5
        if pitch_control_fn is not None:
            try:
                pc_at_action = float(pitch_control_fn(defenders, action_x, action_y))  # type: ignore[operator]
            except Exception:
                pc_at_action = 0.5

        return {
            "event_id": action["event_id"],
            "match_id": action["match_id"],
            "competition_id": action.get("competition_id"),
            "season_id": action.get("season_id"),
            "defender_player_id": int(row["player_id"]),
            "defender_team_id": int(row["team_id"]),
            "defender_x": dx,
            "defender_y": dy,
            "action_player_id": action.get("action_player_id"),
            "action_type": action_type,
            "action_x": action_x,
            "action_y": action_y,
            "credit_type": credit_type.value,
            "confidence": confidence,
            "dist_to_ball": dist,
            "pitch_control_at_action": pc_at_action,
        }

    # --- Intercept: defender performed a successful defensive action ---
    if action_type in _INTERCEPT_ACTIONS:
        action_player_id = int(action.get("action_player_id", -1))  # type: ignore[arg-type]
        for idx in range(len(defenders)):
            pid = int(defenders.iloc[idx]["player_id"])
            if pid == action_player_id and pid not in credited:
                results.append(_base_credit(idx, CreditType.INTERCEPT, "high"))
                credited.add(pid)
                break

    # --- Concede: nearest defender when offensive value > 0 ---
    if offensive_value > 0:
        dists: list[tuple[int, float]] = []
        for idx in range(len(defenders)):
            pid = int(defenders.iloc[idx]["player_id"])
            if pid in credited:
                continue
            dx = float(defenders.iloc[idx]["x"])
            dy = float(defenders.iloc[idx]["y"])
            dists.append((idx, _euclidean_dist(dx, dy, action_x, action_y)))
        if dists:
            nearest_idx = min(dists, key=lambda t: t[1])[0]
            pid = int(defenders.iloc[nearest_idx]["player_id"])
            results.append(_base_credit(nearest_idx, CreditType.CONCEDE, "high"))
            credited.add(pid)

    # --- Disturb: defenders within disturb_radius ---
    for idx in range(len(defenders)):
        pid = int(defenders.iloc[idx]["player_id"])
        if pid in credited:
            continue
        dx = float(defenders.iloc[idx]["x"])
        dy = float(defenders.iloc[idx]["y"])
        dist = _euclidean_dist(dx, dy, action_x, action_y)
        if dist <= params.disturb_radius_m:
            results.append(_base_credit(idx, CreditType.DISTURB, "approximate"))
            credited.add(pid)

    # --- Deter: defenders in cone from ball to goal, offensive_value < 0 ---
    if offensive_value < 0:
        for idx in range(len(defenders)):
            pid = int(defenders.iloc[idx]["player_id"])
            if pid in credited:
                continue
            dx = float(defenders.iloc[idx]["x"])
            dy = float(defenders.iloc[idx]["y"])
            if _is_in_cone(dx, dy, action_x, action_y, _GOAL_CENTER_X, _GOAL_CENTER_Y, params.deter_cone_angle_deg):
                results.append(_base_credit(idx, CreditType.DETER, "approximate"))
                credited.add(pid)

    return results


def extract_features(credits_df: pd.DataFrame, params: DefconLiteParams) -> pd.DataFrame:
    """Extract spatial features for XGBoost value estimation.

    Args:
        credits_df: DataFrame from assign_defensive_credits output.
        params: DEFCON-lite parameters.

    Returns:
        DataFrame with feature columns, same length as input.
    """
    dx = _col_f64(credits_df, "defender_x")
    dy = _col_f64(credits_df, "defender_y")
    ax = _col_f64(credits_df, "action_x")
    ay = _col_f64(credits_df, "action_y")

    goal_x = params.pitch_length
    goal_y = params.pitch_width / 2

    dist_to_goal = np.sqrt((dx - goal_x) ** 2 + (dy - goal_y) ** 2)
    angle_to_ball = np.arctan2(ay - dy, ax - dx)
    is_between = ((dx > ax) & (dx < goal_x)) | ((dx < ax) & (dx > goal_x))

    return pd.DataFrame(
        {
            "dist_to_ball": _col_f64(credits_df, "dist_to_ball"),
            "dist_to_goal": dist_to_goal,
            "angle_to_ball": angle_to_ball,
            "pitch_control_at_action": _col_f64(credits_df, "pitch_control_at_action"),
            "action_type_id": credits_df["action_type"].map(_ACTION_TYPE_IDS).fillna(20).astype(int),  # type: ignore[arg-type]
            "action_start_x": ax,
            "action_start_y": ay,
            "offensive_value": _col_f64(credits_df, "offensive_value"),
            "defender_x": dx,
            "defender_y": dy,
            "is_between_ball_and_goal": is_between.astype(int),
        }
    )


def estimate_defcon_values(
    credits_df: pd.DataFrame,
    params: DefconLiteParams,
) -> pd.DataFrame:
    """Estimate defensive credit values using XGBoost.

    Trains on Intercept+Concede rows (where VAEP provides ground truth),
    then scores all four credit categories.

    Args:
        credits_df: DataFrame with credit assignments and a ``vaep_target``
            column (absolute VAEP value for training rows).
        params: DEFCON-lite parameters.

    Returns:
        Input DataFrame with ``defcon_value`` column added.
    """
    features = extract_features(credits_df, params)
    feature_cols = list(features.columns)

    train_mask = credits_df["credit_type"].isin([CreditType.INTERCEPT.value, CreditType.CONCEDE.value])
    x_train = features.loc[train_mask, feature_cols]
    y_train = credits_df.loc[train_mask, "vaep_target"].astype(float)

    result = credits_df.copy()

    if len(x_train) < 10:
        result["defcon_value"] = 1.0 / (1.0 + features["dist_to_ball"])
        return result

    model = XGBRegressor(n_estimators=100, max_depth=4, learning_rate=0.1, random_state=42)
    model.fit(x_train, y_train)

    x_all = features[feature_cols]
    result["defcon_value"] = model.predict(x_all)

    return result


# Column schema for empty DEFCON DataFrames
_CREDITS_COLS = pd.Index(
    [
        "event_id",
        "match_id",
        "competition_id",
        "season_id",
        "defender_player_id",
        "defender_team_id",
        "defender_x",
        "defender_y",
        "action_player_id",
        "action_type",
        "action_x",
        "action_y",
        "credit_type",
        "confidence",
        "dist_to_ball",
        "pitch_control_at_action",
        "offensive_value",
        "vaep_target",
    ]
)

_OUTPUT_COLS = pd.Index(
    [
        "event_id",
        "match_id",
        "competition_id",
        "season_id",
        "defender_player_id",
        "defender_team_id",
        "defender_x",
        "defender_y",
        "action_player_id",
        "action_type",
        "action_x",
        "action_y",
        "credit_type",
        "confidence",
        "defcon_value",
        "dist_to_ball",
        "pitch_control_at_action",
        "data_source",
    ]
)


def assign_credits_for_period(
    actions_df: pd.DataFrame,
    freeze_frames_df: pd.DataFrame,
    params: DefconLiteParams | None = None,
) -> pd.DataFrame:
    """Assign defensive credits for a batch of actions (Stage 1).

    This function is the parallelizable portion of the DEFCON pipeline.
    It processes whatever actions it receives — the caller is responsible
    for grouping by period (or any other partitioning scheme).

    The returned DataFrame includes ``offensive_value`` and ``vaep_target``
    columns needed by :func:`estimate_values_for_match`.

    Args:
        actions_df: VAEP action values with columns [event_id, match_id,
            competition_id, season_id, player_id, team_id, action_type,
            start_x, start_y, offensive_value].
        freeze_frames_df: Opponent positions per action with columns
            [event_id, player_id, team_id, teammate, x, y, velocity_x,
            velocity_y].
        params: DEFCON-lite parameters.

    Returns:
        DataFrame of credit assignments with ``offensive_value`` and
        ``vaep_target`` columns, one row per credited defender.
    """
    if params is None:
        params = DefconLiteParams()

    if actions_df.empty:
        return pd.DataFrame(columns=_CREDITS_COLS)

    all_credits: list[dict[str, object]] = []

    # Pre-build grouped lookup for O(1) opponent retrieval per action
    opponent_groups = freeze_frames_df[~freeze_frames_df["teammate"]].groupby("event_id")

    for _, action_row in actions_df.iterrows():
        event_id = str(action_row["event_id"])
        action: dict[str, object] = {
            "event_id": event_id,
            "match_id": str(action_row["match_id"]),
            "competition_id": action_row.get("competition_id"),
            "season_id": action_row.get("season_id"),
            "action_player_id": action_row.get("player_id"),
            "action_type": str(action_row["action_type"]),
            "action_x": float(action_row["start_x"]),
            "action_y": float(action_row["start_y"]),
            "offensive_value": float(action_row.get("offensive_value", 0.0) or 0.0),
        }

        try:
            opponents = opponent_groups.get_group(event_id)
        except KeyError:
            continue

        vel_x = np.asarray(opponents["velocity_x"]) if "velocity_x" in opponents.columns else np.zeros(len(opponents))
        vel_y = np.asarray(opponents["velocity_y"]) if "velocity_y" in opponents.columns else np.zeros(len(opponents))

        defenders = pd.DataFrame(
            {
                "player_id": np.asarray(opponents["player_id"]),
                "team_id": np.asarray(opponents["team_id"]),
                "x": np.asarray(opponents["x"]),
                "y": np.asarray(opponents["y"]),
                "velocity_x": vel_x,
                "velocity_y": vel_y,
            }
        )

        credits = assign_defensive_credits(action, defenders, params)
        all_credits.extend(credits)

    if not all_credits:
        return pd.DataFrame(columns=_CREDITS_COLS)

    credits_df = pd.DataFrame(all_credits)

    action_values = actions_df.set_index("event_id")["offensive_value"].to_dict()
    credits_df["offensive_value"] = credits_df["event_id"].map(action_values).fillna(0.0)  # type: ignore[arg-type]
    credits_df["vaep_target"] = credits_df["offensive_value"].abs()

    return credits_df


def estimate_values_for_match(
    credits_df: pd.DataFrame,
    params: DefconLiteParams | None = None,
) -> pd.DataFrame:
    """Estimate DEFCON values for all credits in a match (Stage 2).

    Wraps :func:`estimate_defcon_values` and drops intermediate columns
    (``offensive_value``, ``vaep_target``).  The caller is responsible
    for tagging ``data_source``.

    Args:
        credits_df: DataFrame from :func:`assign_credits_for_period` with
            ``offensive_value`` and ``vaep_target`` columns.
        params: DEFCON-lite parameters.

    Returns:
        DataFrame with ``defcon_value`` column added and intermediate
        columns removed.
    """
    if params is None:
        params = DefconLiteParams()

    if credits_df.empty:
        result = pd.DataFrame(columns=_OUTPUT_COLS)
        return result.drop(columns=["data_source"])

    valued = estimate_defcon_values(credits_df, params)
    valued = valued.drop(columns=["offensive_value", "vaep_target"], errors="ignore")

    return valued


def compute_defcon_match(
    actions_df: pd.DataFrame,
    freeze_frames_df: pd.DataFrame,
    params: DefconLiteParams | None = None,
    data_source: str = "statsbomb_360",
) -> pd.DataFrame:
    """Compute DEFCON-lite credits for all actions in a single match.

    Convenience wrapper that calls :func:`assign_credits_for_period`
    (Stage 1) followed by :func:`estimate_values_for_match` (Stage 2),
    then tags the ``data_source`` column.

    Args:
        actions_df: VAEP action values with columns [event_id, match_id,
            competition_id, season_id, player_id, team_id, action_type,
            start_x, start_y, offensive_value].
        freeze_frames_df: Opponent positions per action with columns
            [event_id, player_id, team_id, teammate, x, y, velocity_x,
            velocity_y].
        params: DEFCON-lite parameters.
        data_source: Provenance tag (statsbomb_360, metrica_tracking).

    Returns:
        DataFrame with bronze schema columns, one row per credit.
    """
    if params is None:
        params = DefconLiteParams()

    credits_df = assign_credits_for_period(actions_df, freeze_frames_df, params)

    if credits_df.empty:
        return pd.DataFrame(columns=_OUTPUT_COLS)

    result = estimate_values_for_match(credits_df, params)
    result["data_source"] = data_source

    return result
