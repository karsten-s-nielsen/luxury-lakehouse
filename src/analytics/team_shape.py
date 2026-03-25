"""Team shape spatial metrics — centroid, convex hull, stretch index, defensive lines.

Computes per-team per-frame spatial metrics from player positions:
1. Team centroid (mean x, y)
2. Convex hull area (scipy.spatial.ConvexHull)
3. Team length and width (max spread along attacking/lateral axes)
4. Stretch index — mean distance from centroid (Clemente et al. 2013)
5. Defensive line height — mean position of deepest Ward cluster
6. Inter-line gaps — distances between cluster centroids along attacking axis (x)

Reference: Clemente, F.M. et al. (2013). "Collective tactical behaviour
in association football: A systematic review."
"""

from __future__ import annotations

import math
from dataclasses import dataclass

import numpy as np
import pandas as pd
from scipy.cluster.hierarchy import fcluster, linkage  # type: ignore[import-untyped]
from scipy.spatial import ConvexHull, QhullError  # type: ignore[import-untyped]


@dataclass(frozen=True)
class TeamShapeParams:
    """Tunable parameters for team shape computation."""

    n_defensive_lines: int = 3  # Ward clustering cluster count
    min_players: int = 3  # Minimum players for convex hull
    pitch_length: float = 120.0  # StatsBomb x-axis extent
    pitch_width: float = 80.0  # StatsBomb y-axis extent


@dataclass(frozen=True)
class TeamShapeResult:
    """Per-team per-frame spatial shape metrics."""

    centroid_x: float  # Team centroid x coordinate
    centroid_y: float  # Team centroid y coordinate
    convex_hull_area: float  # Convex hull area in coordinate units²
    team_length: float  # Max spread along attacking axis (x)
    team_width: float  # Max spread along lateral axis (y)
    stretch_index: float  # Mean distance from centroid
    defensive_line_height: float  # Mean x-position of deepest cluster
    inter_line_gaps: tuple[float, ...]  # Gaps between cluster centroids


def _nan_result() -> TeamShapeResult:
    """Return a result with all NaN values for insufficient data."""
    return TeamShapeResult(
        centroid_x=math.nan,
        centroid_y=math.nan,
        convex_hull_area=math.nan,
        team_length=math.nan,
        team_width=math.nan,
        stretch_index=math.nan,
        defensive_line_height=math.nan,
        inter_line_gaps=(),
    )


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def compute_team_shape(
    players_x: np.ndarray,
    players_y: np.ndarray,
    params: TeamShapeParams | None = None,
) -> TeamShapeResult:
    """Compute spatial shape metrics for a single team in a single frame.

    Parameters
    ----------
    players_x : 1-D array of player x-coordinates (StatsBomb 120x80).
    players_y : 1-D array of player y-coordinates.
    params : Optional tuning parameters. Defaults to ``TeamShapeParams()``.

    Returns
    -------
    TeamShapeResult with centroid, hull area, length, width, stretch index,
    defensive line height, and inter-line gaps.
    """
    if params is None:
        params = TeamShapeParams()

    n = len(players_x)
    if n < params.min_players:
        return _nan_result()

    px = np.asarray(players_x, dtype=np.float64)
    py = np.asarray(players_y, dtype=np.float64)

    # Centroid
    cx = float(np.mean(px))
    cy = float(np.mean(py))

    # Team length (x-spread) and width (y-spread)
    team_length = float(np.ptp(px))
    team_width = float(np.ptp(py))

    # Stretch index: mean Euclidean distance from centroid
    dists = np.sqrt((px - cx) ** 2 + (py - cy) ** 2)
    stretch = float(np.mean(dists))

    # Convex hull area
    try:
        points = np.column_stack((px, py))
        hull = ConvexHull(points)
        hull_area = float(hull.volume)  # 2-D: volume = area
    except QhullError:
        hull_area = 0.0

    # Ward clustering along the attacking axis (x) for defensive lines
    n_clusters = min(params.n_defensive_lines, n)
    if n_clusters < 2:
        defensive_line_height = float(np.min(px))
        gaps: tuple[float, ...] = ()
    else:
        x_col = px.reshape(-1, 1)
        z = linkage(x_col, method="ward")
        labels = fcluster(z, t=n_clusters, criterion="maxclust")

        # Compute cluster centroids along x, sorted ascending
        cluster_x_means = np.array([float(np.mean(px[labels == c])) for c in range(1, n_clusters + 1)])
        cluster_x_means.sort()

        defensive_line_height = float(cluster_x_means[0])
        gaps = tuple(float(cluster_x_means[i + 1] - cluster_x_means[i]) for i in range(len(cluster_x_means) - 1))

    return TeamShapeResult(
        centroid_x=cx,
        centroid_y=cy,
        convex_hull_area=hull_area,
        team_length=team_length,
        team_width=team_width,
        stretch_index=stretch,
        defensive_line_height=defensive_line_height,
        inter_line_gaps=gaps,
    )


def compute_team_shape_frame(
    players_df: pd.DataFrame,
    params: TeamShapeParams | None = None,
) -> dict[str, TeamShapeResult]:
    """Compute shape metrics for both teams in a single frame.

    Parameters
    ----------
    players_df : DataFrame with columns ``x``, ``y``, ``team``.
        Only rows with ``team in ("home", "away")`` are processed.
    params : Optional tuning parameters.

    Returns
    -------
    Dict mapping team name to TeamShapeResult.
    """
    if params is None:
        params = TeamShapeParams()

    results: dict[str, TeamShapeResult] = {}
    for team in ("home", "away"):
        mask = players_df["team"] == team
        team_df = players_df.loc[mask]
        if team_df.empty:
            continue
        results[team] = compute_team_shape(
            team_df["x"].to_numpy(),
            team_df["y"].to_numpy(),
            params,
        )
    return results
