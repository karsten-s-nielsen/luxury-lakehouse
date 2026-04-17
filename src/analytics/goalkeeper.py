"""Goalkeeper analytics — distribution xT, collection stats, PSxG, sweeper metrics.

Four-pillar GK evaluation taxonomy:
  1. Shot stopping (D39: PSxG)
  2. Distribution (D38: xT delta on GK passes)
  3. Cross collection (D38: claim/punch rates)
  4. Defensive activity (D39: sweeper positioning)

References:
  - Butcher et al. (2025), "An Expected Goals On Target (xGOT) Model" (MDPI)
  - Lamberts (2025), Goalkeeper Value Model
  - Yam, "A Data-Driven GK Evaluation Framework" (MIT Sloan)
"""

from __future__ import annotations

from dataclasses import dataclass

import numpy as np
import pandas as pd


@dataclass(frozen=True)
class PSxGModel:
    """Fitted PSxG logistic regression parameters.

    Stores raw numpy arrays (not the sklearn estimator) for JSON-serializable
    model publishing to HF Hub (zero pickle surface).

    Fields:
        coefficients: Logistic regression weight vector, shape ``(n_features,)``.
        intercept: Logistic regression bias term.
        scaler_mean: Per-feature mean from StandardScaler fitting.
        scaler_scale: Per-feature std from StandardScaler fitting.
    """

    coefficients: np.ndarray
    intercept: float
    scaler_mean: np.ndarray
    scaler_scale: np.ndarray


# Penalty area x-extent from own goal line (metres, SPADL 105x68m coordinates).
_PENALTY_AREA_X = 16.5

# SPADL pitch dimensions (105 x 68 metres).
_PITCH_LENGTH = 105.0
_PITCH_WIDTH = 68.0
_N_ZONES_X = 12
_N_ZONES_Y = 8

# Pass length thresholds (metres).
_SHORT_THRESHOLD = 32.0
_LONG_THRESHOLD = 60.0


def compute_gk_distribution_xt(
    passes_df: pd.DataFrame,
    xt_grid: np.ndarray,
) -> pd.DataFrame:
    """Compute xT delta for GK-initiated passes.

    Args:
        passes_df: GK passes with columns ``player_id, match_id, start_x,
            start_y, end_x, end_y, action_result``. SPADL 105x68 coordinates.
        xt_grid: Expected Threat grid, shape ``(12, 8)``.

    Returns:
        DataFrame with one row per ``(player_id, match_id)``. Columns:
        ``player_id, match_id, total_xt_added, xt_per_pass, pass_count,
        short_pct, medium_pct, long_pct, launch_rate``.
    """
    result_columns = pd.Index(
        [
            "player_id",
            "match_id",
            "total_xt_added",
            "xt_per_pass",
            "pass_count",
            "short_pct",
            "medium_pct",
            "long_pct",
            "launch_rate",
        ]
    )

    if passes_df.empty:
        return pd.DataFrame(columns=result_columns)

    # Filter to successful passes for xT computation.
    # Failed passes have misleading end coordinates (interception point, not intended target).
    df: pd.DataFrame = passes_df.loc[passes_df["action_result"] == "success"].copy()
    if df.empty:
        return pd.DataFrame(columns=result_columns)

    # Vectorised xT lookup via numpy index arrays.
    start_x = df["start_x"].to_numpy(dtype=np.float64)
    start_y = df["start_y"].to_numpy(dtype=np.float64)
    end_x = df["end_x"].to_numpy(dtype=np.float64)
    end_y = df["end_y"].to_numpy(dtype=np.float64)

    def _zone_indices(coords: np.ndarray, pitch_dim: float, n_zones: int) -> np.ndarray:
        return np.clip((coords / (pitch_dim / n_zones)).astype(int), 0, n_zones - 1)

    sz_x = _zone_indices(start_x, _PITCH_LENGTH, _N_ZONES_X)
    sz_y = _zone_indices(start_y, _PITCH_WIDTH, _N_ZONES_Y)
    ez_x = _zone_indices(end_x, _PITCH_LENGTH, _N_ZONES_X)
    ez_y = _zone_indices(end_y, _PITCH_WIDTH, _N_ZONES_Y)

    df["xt_delta"] = xt_grid[ez_x, ez_y] - xt_grid[sz_x, sz_y]

    # Pass length classification.
    dx = df["end_x"].to_numpy() - df["start_x"].to_numpy()
    dy = df["end_y"].to_numpy() - df["start_y"].to_numpy()
    df["distance"] = np.sqrt(dx**2 + dy**2)
    df["length_class"] = np.where(
        df["distance"] < _SHORT_THRESHOLD,
        "short",
        np.where(df["distance"] > _LONG_THRESHOLD, "long", "medium"),
    )

    # Aggregate per player-match.
    rows: list[dict[str, object]] = []
    for key, group in df.groupby(["player_id", "match_id"]):
        pid, mid = key  # type: ignore[misc]
        n = len(group)
        total_xt = float(group["xt_delta"].sum())
        length_counts = group["length_class"].value_counts()
        short_n = int(length_counts.get("short", 0))  # type: ignore[arg-type]
        medium_n = int(length_counts.get("medium", 0))  # type: ignore[arg-type]
        long_n = int(length_counts.get("long", 0))  # type: ignore[arg-type]
        rows.append(
            {
                "player_id": pid,
                "match_id": mid,
                "total_xt_added": total_xt,
                "xt_per_pass": total_xt / n if n else 0.0,
                "pass_count": n,
                "short_pct": short_n / n if n else 0.0,
                "medium_pct": medium_n / n if n else 0.0,
                "long_pct": long_n / n if n else 0.0,
                "launch_rate": long_n / n if n else 0.0,
            }
        )
    return pd.DataFrame(rows)


def compute_gk_collection_stats(actions_df: pd.DataFrame) -> pd.DataFrame:
    """Compute GK cross-collection stats (claims and punches).

    Args:
        actions_df: GK actions with columns ``player_id, match_id,
            action_type, action_result``.

    Returns:
        DataFrame with one row per ``(player_id, match_id)``. Columns:
        ``player_id, match_id, claims, claim_success_rate, punches``.
    """
    result_columns = pd.Index(["player_id", "match_id", "claims", "claim_success_rate", "punches"])

    if actions_df.empty:
        return pd.DataFrame(columns=result_columns)

    collection = actions_df.loc[actions_df["action_type"].isin(["keeper_claim", "keeper_punch"])].copy()
    if collection.empty:
        return pd.DataFrame(columns=result_columns)

    rows: list[dict[str, object]] = []
    for key, group in collection.groupby(["player_id", "match_id"]):
        pid, mid = key  # type: ignore[misc]
        claims_mask = group["action_type"] == "keeper_claim"
        claims_total = int(claims_mask.sum())
        claims_success = int(((claims_mask) & (group["action_result"] == "success")).sum())
        punches_total = int((group["action_type"] == "keeper_punch").sum())
        rows.append(
            {
                "player_id": pid,
                "match_id": mid,
                "claims": claims_total,
                "claim_success_rate": claims_success / claims_total if claims_total else 0.0,
                "punches": punches_total,
            }
        )
    return pd.DataFrame(rows)


def compute_gk_action_summary(
    passes_df: pd.DataFrame,
    actions_df: pd.DataFrame,
    xt_grid: np.ndarray,
) -> pd.DataFrame:
    """Combine distribution xT, collection stats, and save/pick-up counts.

    This is the main aggregator that feeds the ``fct_goalkeeper_stats`` dbt model.
    It merges distribution xT + cross-collection + saves into one summary row
    per GK per match.

    Args:
        passes_df: GK passes (same schema as ``compute_gk_distribution_xt``).
        actions_df: All GK actions with ``action_type`` and ``action_result``.
        xt_grid: Expected Threat grid, shape ``(12, 8)``.

    Returns:
        DataFrame with one row per ``(player_id, match_id)``. Columns include
        all distribution columns plus ``claims, claim_success_rate, punches,
        saves, save_pct, keeper_pick_ups``.
    """
    # Distribution xT.
    dist_df = compute_gk_distribution_xt(passes_df, xt_grid)

    # Collection stats (claims, punches).
    coll_df = compute_gk_collection_stats(actions_df)

    # Save and pick-up counts per player-match.
    save_pickup_rows: list[dict[str, object]] = []
    if not actions_df.empty:
        save_pickup_actions = actions_df.loc[actions_df["action_type"].isin(["keeper_save", "keeper_pick_up"])]
        if not save_pickup_actions.empty:
            for key, group in save_pickup_actions.groupby(["player_id", "match_id"]):
                pid, mid = key  # type: ignore[misc]
                saves_mask = group["action_type"] == "keeper_save"
                saves_total = int(saves_mask.sum())
                saves_success = int((saves_mask & (group["action_result"] == "success")).sum())
                pick_ups = int((group["action_type"] == "keeper_pick_up").sum())
                save_pickup_rows.append(
                    {
                        "player_id": pid,
                        "match_id": mid,
                        "saves": saves_total,
                        "save_pct": saves_success / saves_total if saves_total else 0.0,
                        "keeper_pick_ups": pick_ups,
                    }
                )

    save_df = (
        pd.DataFrame(save_pickup_rows)
        if save_pickup_rows
        else pd.DataFrame(columns=pd.Index(["player_id", "match_id", "saves", "save_pct", "keeper_pick_ups"]))
    )

    # Merge all three on (player_id, match_id).
    merge_keys = ["player_id", "match_id"]
    result = dist_df
    if not coll_df.empty:
        result = result.merge(coll_df, on=merge_keys, how="outer")
    if not save_df.empty:
        result = result.merge(save_df, on=merge_keys, how="outer")

    # Fill NaN for players who have some but not all stat categories.
    fill_defaults: dict[str, float] = {
        "total_xt_added": 0.0,
        "xt_per_pass": 0.0,
        "pass_count": 0,
        "short_pct": 0.0,
        "medium_pct": 0.0,
        "long_pct": 0.0,
        "launch_rate": 0.0,
        "claims": 0,
        "claim_success_rate": 0.0,
        "punches": 0,
        "saves": 0,
        "save_pct": 0.0,
        "keeper_pick_ups": 0,
    }
    for col, default in fill_defaults.items():
        if col in result.columns:
            result[col] = result[col].fillna(default)

    return result


# ---------------------------------------------------------------------------
# PSxG — Post-Shot Expected Goals (Butcher et al. 2025)
# ---------------------------------------------------------------------------

# StatsBomb goalmouth coordinate frame.
_GOAL_Y_MIN = 36.0
_GOAL_Y_MAX = 44.0
_GOAL_Z_MAX = 8.0


def _normalise_goalmouth(y: np.ndarray, z: np.ndarray) -> np.ndarray:
    """Normalise StatsBomb goalmouth coordinates to [0, 1].

    StatsBomb goal frame: y in [36, 44] (8-yard goal centred at y=40),
    z in [0, 8] (crossbar height).

    Returns:
        Column-stacked array of shape ``(n, 2)`` with ``[y_norm, z_norm]``.
    """
    y_norm = (y - _GOAL_Y_MIN) / (_GOAL_Y_MAX - _GOAL_Y_MIN)
    z_norm = z / _GOAL_Z_MAX
    return np.column_stack([y_norm, z_norm])


def train_psxg_model(shots_df: pd.DataFrame) -> PSxGModel:
    """Train a logistic regression PSxG model on on-target shots.

    Features: normalised ``end_location_y`` and ``end_location_z`` (StatsBomb
    goalmouth coordinates).  Uses ``StandardScaler`` for feature scaling.

    The returned ``PSxGModel`` stores the fitted numpy arrays — not the sklearn
    estimator — enabling JSON serialisation for HF Hub publishing.

    Args:
        shots_df: On-target shots with columns ``end_location_y``,
            ``end_location_z``, ``is_goal``.

    Returns:
        Frozen ``PSxGModel`` with logistic regression coefficients and
        scaler parameters.
    """
    from sklearn.linear_model import LogisticRegression
    from sklearn.preprocessing import StandardScaler

    y_raw = shots_df["end_location_y"].to_numpy(dtype=np.float64)
    z_raw = shots_df["end_location_z"].to_numpy(dtype=np.float64)
    features = _normalise_goalmouth(y_raw, z_raw)

    scaler = StandardScaler()
    x_scaled: np.ndarray = scaler.fit_transform(features)

    target = shots_df["is_goal"].to_numpy(dtype=np.int64)
    clf = LogisticRegression(max_iter=1000, solver="lbfgs")
    clf.fit(x_scaled, target)

    coef: np.ndarray = np.asarray(clf.coef_[0]).copy()
    intercept_val = float(np.asarray(clf.intercept_).flat[0])
    mean_arr: np.ndarray = np.asarray(scaler.mean_).copy()
    scale_arr: np.ndarray = np.asarray(scaler.scale_).copy()

    return PSxGModel(
        coefficients=coef,
        intercept=intercept_val,
        scaler_mean=mean_arr,
        scaler_scale=scale_arr,
    )


def predict_psxg(model: PSxGModel, shots_df: pd.DataFrame) -> pd.DataFrame:
    """Predict PSxG probabilities using stored model parameters.

    Manual logistic sigmoid — no sklearn dependency at inference time.
    Off-target shots (``end_location_z`` is NaN) receive ``psxg = NaN``.

    Args:
        model: Fitted ``PSxGModel`` from ``train_psxg_model``.
        shots_df: Shots with ``end_location_y`` and ``end_location_z``.

    Returns:
        Copy of input DataFrame with ``psxg`` column added.
    """
    result = shots_df.copy()
    on_target_mask = result["end_location_z"].notna()

    # Default to NaN; fill only on-target rows.
    result["psxg"] = np.nan

    if bool(on_target_mask.any()):
        y_raw = result.loc[on_target_mask, "end_location_y"].to_numpy(dtype=np.float64)
        z_raw = result.loc[on_target_mask, "end_location_z"].to_numpy(dtype=np.float64)
        features = _normalise_goalmouth(y_raw, z_raw)

        # Manual StandardScaler transform.
        x_scaled = (features - model.scaler_mean) / model.scaler_scale

        # Manual logistic sigmoid: p = 1 / (1 + exp(-logits)).
        logits: np.ndarray = x_scaled @ model.coefficients + model.intercept
        probabilities = 1.0 / (1.0 + np.exp(-logits))

        result.loc[on_target_mask, "psxg"] = probabilities

    return result


def compute_goals_prevented(gk_df: pd.DataFrame) -> pd.DataFrame:
    """Compute goals prevented: ``psxg_faced - goals_conceded``.

    Args:
        gk_df: GK summary with ``psxg_faced`` and ``goals_conceded`` columns.

    Returns:
        Copy of input DataFrame with ``goals_prevented`` column added.
    """
    result = gk_df.copy()
    result["goals_prevented"] = result["psxg_faced"] - result["goals_conceded"]
    return result


# ---------------------------------------------------------------------------
# Sweeper-keeper metrics (Defensive Activity pillar)
# ---------------------------------------------------------------------------


def compute_sweeper_metrics(events_df: pd.DataFrame) -> pd.DataFrame:
    """Compute sweeper-keeper positioning metrics per GK per match.

    Measures how aggressively a goalkeeper operates outside the penalty area,
    quantifying the "sweeper" dimension of modern goalkeeping.

    Args:
        events_df: GK events with columns ``player_id, match_id, start_x,
            start_y, action_type, minutes_played``. SPADL coordinates
            (mixed orientation: own goal at x=0 or x=105 depending on match).

    Returns:
        DataFrame with one row per ``(player_id, match_id)``. Columns:
        ``player_id, match_id, avg_defensive_action_distance,
        actions_outside_box_per_90``.
    """
    result_columns = pd.Index(["player_id", "match_id", "avg_defensive_action_distance", "actions_outside_box_per_90"])

    if events_df.empty:
        return pd.DataFrame(columns=result_columns)

    rows: list[dict[str, object]] = []
    for key, group in events_df.groupby(["player_id", "match_id"]):
        pid, mid = key  # type: ignore[misc]
        # SPADL has mixed orientation — LEAST(x, 105-x) gives distance from nearest goal
        distance_from_goal = np.minimum(group["start_x"], _PITCH_LENGTH - group["start_x"])
        avg_distance = float(distance_from_goal.mean())
        outside_box_count = int((distance_from_goal > _PENALTY_AREA_X).sum())
        minutes = float(group["minutes_played"].max())
        per_90 = outside_box_count * (90.0 / minutes) if minutes > 0 else 0.0
        rows.append(
            {
                "player_id": pid,
                "match_id": mid,
                "avg_defensive_action_distance": avg_distance,
                "actions_outside_box_per_90": per_90,
            }
        )

    return pd.DataFrame(rows)
