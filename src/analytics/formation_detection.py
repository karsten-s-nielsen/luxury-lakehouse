"""EFPI (Elastic Formation and Position Identification) formation detection.

Identifies team formations from player tracking positions by matching observed
outfield player (x, y) coordinates against a library of formation templates
from mplsoccer.  Uses the Hungarian algorithm (scipy linear_sum_assignment) to
find the optimal player-to-position assignment, and selects the template with
minimum total Euclidean assignment cost.

Algorithm:
1. Load all 68 formation templates from mplsoccer (StatsBomb 120x80 coordinates).
2. Group templates by outfield player count (8/9/10).
3. For detection: take observed outfield player (x, y) as an (n, 2) array.
4. Elastically scale ALL candidate templates jointly to the observed bounding box.
5. Solve assignment via linear_sum_assignment for each template.
6. Best formation = minimum total cost.

References:
- Bekkers, J. & Dabadghao, S. (2025). "EFPI: Elastic Formation and Position
  Identification." arXiv:2506.23843. (Primary reference — template matching method)
- Shaw, L. & Glickman, M. (2019). "Dynamic analysis of team strategy in
  professional football." (Elastic formation matching concept)

Note: unravelsports (MPL 2.0) by Joris Bekkers provides EFPI natively but
requires Python 3.11+. This project is locked to Python 3.10 (Databricks
serverless constraint), so the algorithm is reimplemented directly using
scipy + mplsoccer. Full credit to Bekkers & Dabadghao for the methodology.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass

import numpy as np
import pandas as pd
from pydantic import BaseModel
from scipy.optimize import linear_sum_assignment  # type: ignore[import-untyped]

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Data classes
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class FormationTemplate:
    """A single formation template with outfield player positions.

    Attributes:
        coords: (n, 2) array of (x, y) template positions (StatsBomb 120x80).
        labels: (n,) string array of position role labels (e.g., "RB", "LCM").
    """

    coords: np.ndarray
    labels: np.ndarray


@dataclass(frozen=True)
class FormationResult:
    """Result of formation detection for one time window.

    Attributes:
        name: Formation label (e.g., "442", "433").
        cost: Total assignment cost (sum of Euclidean distances).
        labels: Ordered position labels matching the input player order.
    """

    name: str
    cost: float
    labels: tuple[str, ...]


class FormationParams(BaseModel):
    """Configuration for formation detection pipeline."""

    window_seconds: int = 300  # 5-minute windows
    min_outfield_players: int = 8  # Minimum to attempt detection


# ---------------------------------------------------------------------------
# Template loading — lazy singleton
# ---------------------------------------------------------------------------

_TEMPLATES: dict[int, dict[str, FormationTemplate]] | None = None


def build_formation_templates() -> dict[int, dict[str, FormationTemplate]]:
    """Load all formation templates from mplsoccer, grouped by outfield player count.

    Returns a dict: {n_outfield: {formation_name: FormationTemplate}}.
    Templates use StatsBomb 120x80 coordinates (matching fct_tracking_frames).

    The result is cached at module level so templates are loaded once per
    Python process.

    WARNING: This imports ``mplsoccer`` + ``matplotlib`` (~200-400 MB). On Databricks
    serverless, call this on the **driver only** — never inside a UDF executor.
    Use ``templates_to_serializable`` / ``templates_from_serializable`` to pass
    templates into UDF closures.
    """
    global _TEMPLATES
    if _TEMPLATES is not None:
        return _TEMPLATES

    from mplsoccer import Pitch

    pitch = Pitch(pitch_type="statsbomb")
    templates: dict[int, dict[str, FormationTemplate]] = {}

    for name in pitch.formations:
        positions = pitch.get_formation(name)
        # Exclude GK (always first position)
        outfield = [p for p in positions if p.name != "GK"]
        n = len(outfield)

        coords = np.array([[p.x, p.y] for p in outfield], dtype=np.float64)
        labels = np.array([p.name for p in outfield])

        if n not in templates:
            templates[n] = {}
        templates[n][name] = FormationTemplate(coords=coords, labels=labels)

    _TEMPLATES = templates
    logger.info(
        "Loaded %d formation templates: %s",
        sum(len(v) for v in templates.values()),
        {k: len(v) for k, v in sorted(templates.items())},
    )
    return _TEMPLATES


def _get_templates() -> dict[int, dict[str, FormationTemplate]]:
    """Return cached templates, building on first call."""
    return build_formation_templates()


# ---------------------------------------------------------------------------
# Serialization for Spark UDF closures (pickle-safe, no mplsoccer import)
# ---------------------------------------------------------------------------


SerializedTemplates = dict[int, dict[str, dict[str, object]]]
"""Type alias: {n_players: {name: {"coords": ndarray, "labels": list[str]}}}"""


def templates_to_serializable(
    templates: dict[int, dict[str, FormationTemplate]],
) -> SerializedTemplates:
    """Convert FormationTemplate objects to plain dicts with numpy arrays and lists.

    The returned structure is pickle-safe for Spark closure capture and contains
    NO dataclasses, Pydantic models, or mplsoccer objects.

    Parameters
    ----------
    templates : Templates from ``build_formation_templates()``.

    Returns
    -------
    ``{n_players: {name: {"coords": np.ndarray, "labels": list[str]}}}``
    """
    serialized: SerializedTemplates = {}
    for n_players, group in templates.items():
        serialized[n_players] = {}
        for name, tmpl in group.items():
            serialized[n_players][name] = {
                "coords": tmpl.coords.copy(),
                "labels": tmpl.labels.tolist(),
            }
    return serialized


def templates_from_serializable(
    data: SerializedTemplates,
) -> dict[int, dict[str, FormationTemplate]]:
    """Reconstruct FormationTemplate objects from serialized plain dicts.

    This is the inverse of ``templates_to_serializable`` and does NOT import
    mplsoccer — safe to call inside UDF executors.

    Parameters
    ----------
    data : Serialized templates from ``templates_to_serializable()``.

    Returns
    -------
    Templates in the standard ``{n_players: {name: FormationTemplate}}`` format.
    """
    templates: dict[int, dict[str, FormationTemplate]] = {}
    for n_players, group in data.items():
        templates[n_players] = {}
        for name, entry in group.items():
            templates[n_players][name] = FormationTemplate(
                coords=np.asarray(entry["coords"], dtype=np.float64),
                labels=np.asarray(entry["labels"]),
            )
    return templates


# ---------------------------------------------------------------------------
# EFPI core algorithm
# ---------------------------------------------------------------------------


def _elastic_scale_templates(
    template_coords_list: list[np.ndarray],
    obs_min: np.ndarray,
    obs_max: np.ndarray,
) -> list[np.ndarray]:
    """Scale templates jointly to observed bounding box.

    All templates are pooled to compute a single global min/max, then each is
    linearly mapped to [obs_min, obs_max] per axis.

    Parameters
    ----------
    template_coords_list : List of (n, 2) arrays (one per template).
    obs_min : (2,) array [min_x, min_y] of observed positions.
    obs_max : (2,) array [max_x, max_y] of observed positions.

    Returns
    -------
    List of scaled (n, 2) arrays in the same order.
    """
    if not template_coords_list:
        return []

    # Pool all template coordinates to find global extents
    all_coords = np.concatenate(template_coords_list, axis=0)
    global_min = all_coords.min(axis=0)
    global_max = all_coords.max(axis=0)
    global_range = global_max - global_min
    obs_range = obs_max - obs_min

    # Zero-range guard per axis — use safe division to avoid RuntimeWarning
    safe_range = np.where(global_range > 0, global_range, 1.0)
    scale = np.where(global_range > 0, obs_range / safe_range, 1.0)

    scaled: list[np.ndarray] = []
    for coords in template_coords_list:
        s = (coords - global_min) * scale + obs_min
        scaled.append(s)

    return scaled


def detect_formation(
    outfield_xy: np.ndarray,
    templates: dict[int, dict[str, FormationTemplate]],
    params: FormationParams | None = None,
) -> FormationResult | None:
    """Detect the best-matching formation for a set of outfield player positions.

    Parameters
    ----------
    outfield_xy : (n, 2) array of outfield player (x, y) positions in
        StatsBomb 120x80 coordinates.
    templates : Formation templates grouped by player count. Required — callers
        must explicitly provide templates (built on the driver and serialized
        for UDF closures, or from the module-level cache for non-Spark usage).
    params : FormationParams for minimum player threshold.

    Returns
    -------
    FormationResult with the best-matching formation name, assignment cost,
    and position labels ordered to match the input player order. Returns None
    if fewer than min_outfield_players are provided or no matching templates
    exist for the given player count.
    """
    if params is None:
        params = FormationParams()

    n_players = len(outfield_xy)
    if n_players < params.min_outfield_players:
        return None

    # Get templates matching this player count
    candidates = templates.get(n_players)
    if not candidates:
        return None

    outfield_xy = np.asarray(outfield_xy, dtype=np.float64)

    # Observed bounding box
    obs_min = outfield_xy.min(axis=0)
    obs_max = outfield_xy.max(axis=0)

    # Prepare all template coords for joint scaling
    template_names = list(candidates.keys())
    template_coords_list = [candidates[name].coords for name in template_names]
    template_labels_list = [candidates[name].labels for name in template_names]

    # Elastic scaling — all templates scaled jointly
    scaled_coords_list = _elastic_scale_templates(template_coords_list, obs_min, obs_max)

    best_name: str | None = None
    best_cost = float("inf")
    best_labels: tuple[str, ...] = ()

    for name, scaled_coords, labels in zip(template_names, scaled_coords_list, template_labels_list, strict=True):
        # Cost matrix: Euclidean distance from each observed player to each template position
        diff = outfield_xy[:, np.newaxis, :] - scaled_coords[np.newaxis, :, :]
        cost_matrix = np.sqrt(np.sum(diff**2, axis=2))

        # Hungarian algorithm — optimal assignment
        row_ind, col_ind = linear_sum_assignment(cost_matrix)
        total_cost = float(cost_matrix[row_ind, col_ind].sum())

        if total_cost < best_cost:
            best_cost = total_cost
            best_name = name
            # Map labels back to input player order
            assigned_labels = [""] * n_players
            for r, c in zip(row_ind, col_ind, strict=True):
                assigned_labels[r] = str(labels[c])
            best_labels = tuple(assigned_labels)

    if best_name is None:
        return None

    return FormationResult(name=best_name, cost=best_cost, labels=best_labels)


# ---------------------------------------------------------------------------
# Batch processing helper for applyInPandas
# ---------------------------------------------------------------------------


def process_group_formations(
    tracking_df: pd.DataFrame,
    match_id: str,
    period: int,
    team: str,
    templates: dict[int, dict[str, FormationTemplate]],
    params: FormationParams | None = None,
) -> pd.DataFrame:
    """Detect formations for a single team in a single period.

    Divides the tracking data into time windows of ``params.window_seconds``,
    computes mean (x, y) per player per window, and runs EFPI detection.

    This is the UDF-level entry point — each ``applyInPandas`` group is one
    (match_id, period, team) combination (~7K rows), keeping executor memory
    well under the 1 GB serverless limit.

    Parameters
    ----------
    tracking_df : DataFrame with columns: player_id, timestamp_seconds, x, y.
        Must contain only outfield players for ONE team in ONE period.
        Callers are responsible for excluding the goalkeeper before calling.
    match_id : Match identifier for output.
    period : Period number (1, 2, etc.).
    team : Team identifier.
    templates : Formation templates (required — must be pre-built).
    params : FormationParams.

    Returns
    -------
    DataFrame with columns: match_id, period, team, window_start_s,
    window_end_s, formation_label, cost.
    """
    if params is None:
        params = FormationParams()

    empty_df = pd.DataFrame(
        columns=pd.Index(["match_id", "period", "team", "window_start_s", "window_end_s", "formation_label", "cost"])
    )

    if tracking_df.empty:
        return empty_df

    results: list[dict[str, object]] = []

    ts = tracking_df["timestamp_seconds"].to_numpy(dtype=np.float64)
    ts_min = float(ts.min())
    ts_max = float(ts.max())

    # Divide into windows
    window_start = ts_min
    while window_start < ts_max:
        window_end = window_start + params.window_seconds

        # Filter to this window
        mask = (ts >= window_start) & (ts < window_end)
        window_df = tracking_df[mask]

        if len(window_df) == 0:
            window_start = window_end
            continue

        # Compute mean position per player within window
        player_means = window_df.groupby("player_id")[["x", "y"]].mean()

        outfield_xy = player_means[["x", "y"]].values.astype(np.float64)

        detection = detect_formation(outfield_xy, templates, params)

        if detection is not None:
            results.append(
                {
                    "match_id": match_id,
                    "period": int(period),
                    "team": str(team),
                    "window_start_s": window_start,
                    "window_end_s": min(window_end, ts_max),
                    "formation_label": detection.name,
                    "cost": detection.cost,
                }
            )

        window_start = window_end

    if not results:
        return empty_df

    return pd.DataFrame(results)


def process_match_formations(
    tracking_df: pd.DataFrame,
    match_id: str,
    params: FormationParams | None = None,
    templates: dict[int, dict[str, FormationTemplate]] | None = None,
) -> pd.DataFrame:
    """Detect formations for all teams/periods in a single match.

    Groups tracking data by (period, team) and delegates each group to
    ``process_group_formations``.

    Parameters
    ----------
    tracking_df : DataFrame with columns: period, team, player_id,
        timestamp_seconds, x, y. Should contain only outfield players.
    match_id : Match identifier for output.
    params : FormationParams.
    templates : Formation templates (uses cached if None).

    Returns
    -------
    DataFrame with columns: match_id, period, team, window_start_s,
    window_end_s, formation_label, cost.
    """
    if params is None:
        params = FormationParams()

    if templates is None:
        templates = _get_templates()

    results: list[pd.DataFrame] = []

    # Pre-build group indexes to avoid O(n*m) boolean mask filtering
    group_index: dict[tuple[int, str], pd.DataFrame] = dict(
        iter(tracking_df.groupby(["period", "team"]))  # type: ignore[arg-type]
    )

    for (period, team), group_df in group_index.items():
        group_result = process_group_formations(group_df, match_id, int(period), str(team), templates, params)
        if len(group_result) > 0:
            results.append(group_result)

    if not results:
        return pd.DataFrame(
            columns=pd.Index(
                ["match_id", "period", "team", "window_start_s", "window_end_s", "formation_label", "cost"]
            )
        )

    return pd.concat(results, ignore_index=True)
