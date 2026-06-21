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


# Canonical PSxG feature order — the single source consumed by training, the serialized
# envelope's ``feature_names`` (ADR-012 §2), and inference. BOTH modalities build this same
# 4-vector: StatsBomb via goal-line trajectory projection, tracking via the measured ball
# crossing (TF-48 shot_crossing_*). See 2026-06-21-psxg-end-location-feature-inadequacy-finding.
PSXG_FEATURE_NAMES: tuple[str, ...] = (
    "goalmouth_dist_from_centre",  # min(|y_norm - 0.5|, 0.5) — the symmetric near-post arch
    "goalmouth_z",  # crossing height as fraction of goal width
    "distance_to_goal_m",  # shot origin -> goal centre, metres (yard/metre-harmonised)
    "shot_angle",  # angle subtended at the posts, radians (modality-invariant)
)

# StatsBomb goal-line x (120x80 pitch) for trajectory projection; yards->metres for the
# distance feature so StatsBomb (yards) and tracking (metres) land on one scale.
_SB_GOAL_LINE_X = 120.0
_YARD_TO_M = 0.9144


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

# StatsBomb goalmouth coordinate frame (the PSxG model's training/input space).
_GOAL_Y_MIN = 36.0
_GOAL_Y_MAX = 44.0
_GOAL_Z_MAX = 8.0
# The model's [0,1] denominator: the goal-mouth span in StatsBomb units.
_SB_GOAL_SPAN = _GOAL_Y_MAX - _GOAL_Y_MIN  # 8.0

# SPADL metric goalmouth (the tracking-feature space: shot_crossing_y/z in metres).
# Goal centred at y=34 m, mouth 7.32 m wide (posts at 30.34 / 37.66), crossbar 2.44 m.
# The 7.32 m mouth IS the model's `_SB_GOAL_SPAN` expressed in metres, so dividing a
# metric crossing by it yields the SAME normalised fraction-of-goal-width the model
# was trained on — there is ONE normalization concept, two unit systems. (Spec D-E.)
_SPADL_GOAL_CENTRE_M = 34.0
_SPADL_GOAL_WIDTH_M = 7.32
_SPADL_LEFT_POST_M = _SPADL_GOAL_CENTRE_M - _SPADL_GOAL_WIDTH_M / 2.0  # 30.34


def _normalise_goalmouth(y: np.ndarray, z: np.ndarray) -> np.ndarray:
    """Normalise StatsBomb goalmouth coordinates to [0, 1].

    StatsBomb goal frame: y in [36, 44] (8-unit goal centred at y=40),
    z in [0, 8] (StatsBomb units; NOT metres — the 2.44 m crossbar ≈ 2.67 units).

    Returns:
        Column-stacked array of shape ``(n, 2)`` with ``[y_norm, z_norm]``.
    """
    y_norm = (y - _GOAL_Y_MIN) / _SB_GOAL_SPAN
    z_norm = z / _GOAL_Z_MAX
    return np.column_stack([y_norm, z_norm])


def normalise_tracking_goalmouth(crossing_y_m: np.ndarray, crossing_z_m: np.ndarray) -> np.ndarray:
    """Map SPADL-metre goalmouth crossings into the StatsBomb-trained [0, 1] space.

    The single normalization port for the *tracking* modality (TF-48
    ``shot_crossing_y`` / ``shot_crossing_z``, in SPADL metres) — the StatsBomb
    modality goes through :func:`_normalise_goalmouth`. Both produce the same
    fraction-of-goal-width semantics the model's ``StandardScaler`` was fit on,
    so the stored scaler + coefficients apply unchanged:

      - ``y_norm = (crossing_y_m - left_post) / goal_width`` — posts map to 0/1,
        goal centre (34 m) to 0.5, exactly as StatsBomb y=36/44/40 do.
      - ``z_norm = crossing_z_m / goal_width`` — the crossbar (2.44 m) maps to
        ``2.44/7.32 = 0.333``, matching StatsBomb's ``2.67/8 = 0.334``.

    No clamping (mirrors :func:`_normalise_goalmouth`); on-target gating keeps
    inputs inside the mouth. Handedness (which post is 0) is regression-pinned
    by the golden test — low-y maps to 0, consistent with the LTR-normalised
    SPADL/StatsBomb orientation.

    Args:
        crossing_y_m: Ball y at the goal-line crossing, SPADL metres.
        crossing_z_m: Ball height at the crossing, SPADL metres.

    Returns:
        Column-stacked array of shape ``(n, 2)`` with ``[y_norm, z_norm]``.
    """
    y_norm = (crossing_y_m - _SPADL_LEFT_POST_M) / _SPADL_GOAL_WIDTH_M
    z_norm = crossing_z_m / _SPADL_GOAL_WIDTH_M
    return np.column_stack([y_norm, z_norm])


def assemble_psxg_features(
    y_norm: np.ndarray,
    z_norm: np.ndarray,
    distance_to_goal_m: np.ndarray,
    shot_angle: np.ndarray,
) -> np.ndarray:
    """Assemble the canonical 4-feature PSxG matrix from modality-normalised inputs.

    The single feature port both modalities funnel through, so one model scores both.
    ``y_norm`` / ``z_norm`` are goalmouth-crossing fractions (0..1 across the goal, posts at
    0/1) from :func:`_normalise_goalmouth` (StatsBomb) or :func:`normalise_tracking_goalmouth`
    (tracking). The horizontal feature is **distance from the goal centre** —
    ``min(|y_norm - 0.5|, 0.5)`` (clipped at the post): the goal-vs-save signal is a symmetric
    arch (near-post hardest to save, dead-centre saveable, wide = misses), which a logistic on
    a linear y cannot represent.

    Args:
        y_norm: Goalmouth crossing y, fraction across the goal.
        z_norm: Crossing height, fraction of goal width.
        distance_to_goal_m: Shot origin -> goal centre, METRES (unit-harmonised).
        shot_angle: Angle subtended at the posts, radians.

    Returns:
        ``(n, 4)`` array in :data:`PSXG_FEATURE_NAMES` order.
    """
    dist_from_centre = np.minimum(np.abs(np.asarray(y_norm, dtype=np.float64) - 0.5), 0.5)
    return np.column_stack(
        [
            dist_from_centre,
            np.asarray(z_norm, dtype=np.float64),
            np.asarray(distance_to_goal_m, dtype=np.float64),
            np.asarray(shot_angle, dtype=np.float64),
        ]
    )


def project_sb_shot_to_goal_line(
    location_x: np.ndarray,
    location_y: np.ndarray,
    end_location_x: np.ndarray,
    end_location_y: np.ndarray,
    end_location_z: np.ndarray,
) -> tuple[np.ndarray, np.ndarray]:
    """Project a StatsBomb shot trajectory onto the goal line (x=120).

    ``end_location`` is the goal-line crossing only for goals (``end_location_x``=120); for
    saves it is the save point (~x=118), so the raw y/z are NOT the goalmouth target. Linear
    extrapolation of (location -> end_location) to x=120 recovers the intended crossing.
    Falls back to the raw end point for shots with no forward x-extent.

    Returns:
        ``(y_goal, z_goal)`` — the projected goal-line crossing y and height.
    """
    lx = np.asarray(location_x, dtype=np.float64)
    ly = np.asarray(location_y, dtype=np.float64)
    ex = np.asarray(end_location_x, dtype=np.float64)
    ey = np.asarray(end_location_y, dtype=np.float64)
    ez = np.asarray(end_location_z, dtype=np.float64)
    denom = ex - lx
    safe = denom > 0.1
    frac = np.where(safe, (_SB_GOAL_LINE_X - lx) / np.where(safe, denom, 1.0), 1.0)
    y_goal = np.where(safe, ly + (ey - ly) * frac, ey)
    z_goal = np.where(safe, ez * frac, ez)
    return y_goal, z_goal


def build_psxg_features_statsbomb(shots_df: pd.DataFrame) -> np.ndarray:
    """Build the 4-feature PSxG matrix for StatsBomb shots (project -> normalise -> assemble).

    Requires ``location_x`` / ``location_y`` / ``end_location_x`` / ``end_location_y`` /
    ``end_location_z`` (StatsBomb 120x80 yards) + ``distance_to_goal`` (yards) + ``shot_angle``
    (radians).
    """
    y_goal, z_goal = project_sb_shot_to_goal_line(
        shots_df["location_x"].to_numpy(dtype=np.float64),
        shots_df["location_y"].to_numpy(dtype=np.float64),
        shots_df["end_location_x"].to_numpy(dtype=np.float64),
        shots_df["end_location_y"].to_numpy(dtype=np.float64),
        shots_df["end_location_z"].to_numpy(dtype=np.float64),
    )
    normalised = _normalise_goalmouth(y_goal, z_goal)
    distance_m = shots_df["distance_to_goal"].to_numpy(dtype=np.float64) * _YARD_TO_M
    shot_angle = shots_df["shot_angle"].to_numpy(dtype=np.float64)
    return assemble_psxg_features(normalised[:, 0], normalised[:, 1], distance_m, shot_angle)


def spadl_shot_geometry(start_x: np.ndarray, start_y: np.ndarray) -> tuple[np.ndarray, np.ndarray]:
    """Distance-to-goal (metres) + subtended shot angle (radians) for SPADL-metre origins.

    Mirrors the StatsBomb dbt macros ``distance_to_goal`` / ``shot_angle`` (Soccermatics
    Ch.2) in SPADL coordinates (105x68 m, goal centre ``(105, 34)``, goal width 7.32 m), so
    the tracking modality's distance/angle land on the SAME definition as the StatsBomb
    model was trained on. The angle is scale-invariant (numerator and denominator both scale
    as length²); distance is metres for both modalities via the yard→metre harmonisation.
    """
    sx = np.asarray(start_x, dtype=np.float64)
    sy = np.asarray(start_y, dtype=np.float64)
    dx = _PITCH_LENGTH - sx
    dy = _SPADL_GOAL_CENTRE_M - sy
    distance_m = np.sqrt(dx**2 + dy**2)
    half_w = _SPADL_GOAL_WIDTH_M / 2.0
    angle = np.abs(np.arctan2(_SPADL_GOAL_WIDTH_M * dx, dx**2 + dy**2 - half_w**2))
    return distance_m, angle


def build_psxg_features_tracking(shots_df: pd.DataFrame) -> np.ndarray:
    """Build the 4-feature PSxG matrix for tracking shots (measured crossing + start geometry).

    Requires ``shot_crossing_y`` / ``shot_crossing_z`` (the measured goal-line crossing, TF-48,
    SPADL metres) + ``start_x`` / ``start_y`` (shot origin, SPADL metres). The crossing is the
    true goal-line crossing (no projection needed, unlike StatsBomb); distance/angle come from
    :func:`spadl_shot_geometry` so the feature semantics match :func:`build_psxg_features_statsbomb`.
    """
    normalised = normalise_tracking_goalmouth(
        shots_df["shot_crossing_y"].to_numpy(dtype=np.float64),
        shots_df["shot_crossing_z"].to_numpy(dtype=np.float64),
    )
    distance_m, shot_angle = spadl_shot_geometry(
        shots_df["start_x"].to_numpy(dtype=np.float64),
        shots_df["start_y"].to_numpy(dtype=np.float64),
    )
    return assemble_psxg_features(normalised[:, 0], normalised[:, 1], distance_m, shot_angle)


def load_psxg_model(path: str) -> PSxGModel:
    """Load a :class:`PSxGModel` from its JSON envelope.

    The envelope (published by ``scripts/train_psxg_hf.py``) carries
    ``coefficients`` / ``intercept`` / ``scaler_mean`` / ``scaler_scale`` —
    zero pickle surface. Used by the tracking-PSxG writer to score tracking
    shots with the same model the StatsBomb path was delivered.
    """
    import json

    with open(path, encoding="utf-8") as fh:
        envelope = json.load(fh)
    intercept = np.asarray(envelope["intercept"], dtype=np.float64).ravel()
    return PSxGModel(
        coefficients=np.asarray(envelope["coefficients"], dtype=np.float64).ravel(),
        intercept=float(intercept[0]),
        scaler_mean=np.asarray(envelope["scaler_mean"], dtype=np.float64),
        scaler_scale=np.asarray(envelope["scaler_scale"], dtype=np.float64),
    )


def train_psxg_model(shots_df: pd.DataFrame) -> PSxGModel:
    """Train a logistic regression PSxG model on on-target shots.

    Features: the 4-vector from :func:`build_psxg_features_statsbomb` —
    projected-goalmouth distance-from-centre, crossing height, distance-to-goal
    (metres), and shot angle (:data:`PSXG_FEATURE_NAMES`). ``StandardScaler`` scales them.

    The returned ``PSxGModel`` stores the fitted numpy arrays — not the sklearn
    estimator — enabling JSON serialisation for HF Hub publishing.

    Args:
        shots_df: On-target shots with ``location_x`` / ``location_y`` /
            ``end_location_x`` / ``end_location_y`` / ``end_location_z`` /
            ``distance_to_goal`` / ``shot_angle`` / ``is_goal``.

    Returns:
        Frozen ``PSxGModel`` with logistic regression coefficients and
        scaler parameters.
    """
    from sklearn.linear_model import LogisticRegression
    from sklearn.preprocessing import StandardScaler

    features = build_psxg_features_statsbomb(shots_df)

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

    Manual logistic sigmoid — no sklearn dependency at inference time. Off-target shots
    (``end_location_z`` is NaN) receive ``psxg = NaN``. On-target rows are scored with the
    same 4-feature vector the model was trained on (:func:`build_psxg_features_statsbomb`).

    Args:
        model: Fitted ``PSxGModel`` from ``train_psxg_model``.
        shots_df: Shots with the StatsBomb feature columns (see
            :func:`build_psxg_features_statsbomb`).

    Returns:
        Copy of input DataFrame with ``psxg`` column added.
    """
    result = shots_df.copy()
    on_target_mask = result["end_location_z"].notna()

    # Default to NaN; fill only on-target rows.
    result["psxg"] = np.nan

    if bool(on_target_mask.any()):
        features = build_psxg_features_statsbomb(result.loc[on_target_mask])

        # Manual StandardScaler transform + logistic sigmoid: p = 1 / (1 + exp(-logits)).
        x_scaled = (features - model.scaler_mean) / model.scaler_scale
        logits: np.ndarray = x_scaled @ model.coefficients + model.intercept
        probabilities = 1.0 / (1.0 + np.exp(-logits))

        result.loc[on_target_mask, "psxg"] = probabilities

    return result


def cross_validate_psxg_by_match(shots_df: pd.DataFrame, *, n_splits: int = 5) -> dict[str, float | int]:
    """Out-of-sample PSxG metrics via GroupKFold grouped by match (no same-match leakage).

    Plain k-fold leaks: shots from the same match share match-level structure (same GK,
    pitch, conditions), so a random split places correlated rows in both train and test
    and inflates the score — the xT-3 cross-game leak class. Grouping by ``match_key``
    guarantees every shot of a match is held out together, so the reported Brier/AUC are
    genuinely out-of-sample.

    Each fold trains a fresh :func:`train_psxg_model` on the train matches and scores the
    held-out matches via :func:`predict_psxg`; metrics are pooled over the out-of-sample
    predictions. Mirrors the GroupKFold discipline of
    :func:`analytics.psxg_tracking.fit_platt_calibration`.

    Args:
        shots_df: On-target shots with ``end_location_y`` / ``end_location_z`` / ``is_goal``
            and a ``match_key`` (preferred) or ``match_id`` grouping column.
        n_splits: Maximum folds; capped at the number of distinct match groups.

    Returns:
        Dict with ``brier_score``, ``log_loss``, ``roc_auc``, ``n_oos_shots``, ``n_groups``.
        Metrics are NaN with ``n_oos_shots=0`` when fewer than two match groups are present
        (a single group cannot be held out) or no fold yields a two-class train split.
    """
    from sklearn.metrics import brier_score_loss, log_loss, roc_auc_score
    from sklearn.model_selection import GroupKFold

    group_col = "match_key" if "match_key" in shots_df.columns else "match_id"
    if group_col not in shots_df.columns:
        raise KeyError("cross_validate_psxg_by_match requires a 'match_key' or 'match_id' column")

    df = shots_df.reset_index(drop=True)
    groups = df[group_col].to_numpy()
    y = df["is_goal"].to_numpy(dtype=np.int64)
    n = len(df)
    n_groups = int(pd.unique(groups).size)

    degenerate: dict[str, float | int] = {
        "brier_score": float("nan"),
        "log_loss": float("nan"),
        "roc_auc": float("nan"),
        "n_oos_shots": 0,
        "n_groups": n_groups,
    }
    splits = min(n_splits, n_groups)
    if splits < 2 or np.unique(y).size < 2:
        return degenerate

    oos = np.full(n, np.nan, dtype=np.float64)
    gkf = GroupKFold(n_splits=splits)
    for train_idx, test_idx in gkf.split(df, y, groups):
        if np.unique(y[train_idx]).size < 2:
            continue  # single-class train fold — cannot fit logistic; leave NaN, excluded below
        fold_model = train_psxg_model(df.iloc[train_idx])
        oos[test_idx] = predict_psxg(fold_model, df.iloc[test_idx])["psxg"].to_numpy(dtype=np.float64)

    scored = ~np.isnan(oos)
    if scored.sum() == 0 or np.unique(y[scored]).size < 2:
        return degenerate

    y_scored = y[scored]
    p_scored = np.clip(oos[scored], 1e-7, 1.0 - 1e-7)
    return {
        "brier_score": float(brier_score_loss(y_scored, p_scored)),
        "log_loss": float(log_loss(y_scored, p_scored)),
        "roc_auc": float(roc_auc_score(y_scored, p_scored)),
        "n_oos_shots": int(scored.sum()),
        "n_groups": n_groups,
    }


def serialize_psxg_model(
    model: PSxGModel,
    *,
    metrics: dict[str, float | int],
    model_version: str,
    feature_names: tuple[str, ...] = PSXG_FEATURE_NAMES,
) -> bytes:
    """Serialize a :class:`PSxGModel` to its canonical JSON envelope (zero pickle surface).

    Single source for the bytes written to HF Hub, logged as the MLflow artifact, and
    uploaded to the UC Volume — keeping all three byte-identical. ADR-012 §2: the envelope
    carries a top-level ``feature_names`` (the logistic envelope has no native feature
    names) so inference reindexes tabular input deterministically; ``model_version`` is
    embedded so consumers and the GK-page recalibration caption can reference it.

    Args:
        model: Fitted ``PSxGModel``.
        metrics: Evaluation metrics to embed (provenance only; not read at inference).
        model_version: Human-meaningful version string (e.g. ``"v2-ontarget"``).
        feature_names: Feature order; defaults to :data:`PSXG_FEATURE_NAMES`.

    Returns:
        UTF-8 JSON bytes carrying ``model_version`` / ``feature_names`` / ``coefficients`` /
        ``intercept`` / ``scaler_mean`` / ``scaler_scale`` / ``metrics``.
    """
    import json

    envelope: dict[str, object] = {
        "model_version": model_version,
        "feature_names": list(feature_names),
        "coefficients": model.coefficients.tolist(),
        "intercept": float(model.intercept),
        "scaler_mean": model.scaler_mean.tolist(),
        "scaler_scale": model.scaler_scale.tolist(),
        "metrics": metrics,
    }
    return json.dumps(envelope, indent=2).encode("utf-8")


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
