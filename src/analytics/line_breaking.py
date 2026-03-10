"""Line-breaking pass detection using Ward hierarchical clustering.

Identifies passes that break through opponent defensive lines by:
1. Clustering opponent positions into 3 lines (attack/midfield/defense)
   via Ward hierarchical clustering.
2. Connecting adjacent players within each cluster + sideline extensions
   to form line segments.
3. Testing whether the pass trajectory intersects any line segment using
   a vectorized cross-product straddle test.

Reference algorithm: parmacalcio1913/line-breaking-passes (Apache 2.0).
Rewritten from PyTorch to NumPy, adapted for StatsBomb 120x80 coordinates.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass

import numpy as np
import pandas as pd
from scipy.cluster.hierarchy import fcluster, linkage  # type: ignore[import-untyped]

logger = logging.getLogger(__name__)


def _col_f64(df: pd.DataFrame, col: str) -> np.ndarray:
    """Extract a DataFrame column as a float64 numpy array (pyright-safe)."""
    return np.asarray(df[col], dtype=np.float64)


@dataclass(frozen=True)
class LineBreakingParams:
    """Parameters for line-breaking pass detection."""

    min_opponents: int = 3  # Ward clustering needs >= 3 points
    n_clusters: int = 3  # attack / midfield / defense lines
    min_pass_length: float = 3.0  # yards (120x80 system) — skip short layoffs
    min_x_spread: float = 5.0  # yards — skip compressed formations
    pitch_y_min: float = 0.0
    pitch_y_max: float = 80.0


@dataclass(frozen=True)
class LineBreakingResult:
    """Result of line-breaking detection for a single pass."""

    is_line_breaking: bool
    lines_broken: int  # 0, 1, 2, or 3
    line_breaking_type: str | None  # 'through', 'around', or None


# ---------------------------------------------------------------------------
# Private helpers
# ---------------------------------------------------------------------------

_EMPTY_RESULT = LineBreakingResult(is_line_breaking=False, lines_broken=0, line_breaking_type=None)


def _cluster_opponents(
    positions: np.ndarray,
    params: LineBreakingParams,
) -> list[np.ndarray]:
    """Cluster opponent positions into defensive lines via Ward linkage.

    Parameters
    ----------
    positions : (N, 2) array of opponent (x, y) positions.
    params : LineBreakingParams.

    Returns
    -------
    List of 3 arrays, each (K_i, 2), sorted by ascending mean-x.
    Within each cluster, players are sorted by ascending y.
    Returns empty list if fewer than min_opponents.
    """
    n = len(positions)
    if n < params.min_opponents:
        return []

    n_clusters = min(params.n_clusters, n)

    # Ward linkage on (x, y) positions
    z = linkage(positions, method="ward")
    labels = fcluster(z, t=n_clusters, criterion="maxclust")

    clusters: list[np.ndarray] = []
    for cluster_id in sorted(set(labels)):
        mask = labels == cluster_id
        cluster_pts = positions[mask]
        # Sort by y within cluster
        order = np.argsort(cluster_pts[:, 1])
        clusters.append(cluster_pts[order])

    # Sort clusters by ascending mean-x (furthest from goal = attack line first)
    clusters.sort(key=lambda c: float(np.mean(c[:, 0])))

    return clusters


def _build_line_segments(
    clusters: list[np.ndarray],
    params: LineBreakingParams,
) -> np.ndarray:
    """Connect adjacent players within each cluster + sideline extensions.

    Parameters
    ----------
    clusters : List of (K_i, 2) arrays, each sorted by y.
    params : LineBreakingParams.

    Returns
    -------
    (M, 2, 2) array of line segments, where each segment is
    [[x1, y1], [x2, y2]].
    """
    segments: list[np.ndarray] = []

    for cluster in clusters:
        if len(cluster) == 0:
            continue

        # Extend to sidelines: add virtual points at y_min and y_max
        # using the x-coordinate of the nearest player
        bottom_ext = np.array([[cluster[0, 0], params.pitch_y_min]])
        top_ext = np.array([[cluster[-1, 0], params.pitch_y_max]])
        extended = np.concatenate([bottom_ext, cluster, top_ext], axis=0)

        # Connect adjacent points
        for i in range(len(extended) - 1):
            seg = np.array([extended[i], extended[i + 1]])
            segments.append(seg)

    if not segments:
        return np.empty((0, 2, 2), dtype=np.float64)

    return np.array(segments, dtype=np.float64)


def _segments_intersect(
    pass_start: np.ndarray,
    pass_end: np.ndarray,
    segments: np.ndarray,
) -> np.ndarray:
    """Vectorized cross-product straddle test for segment intersection.

    Parameters
    ----------
    pass_start : (2,) array [x, y] of pass origin.
    pass_end : (2,) array [x, y] of pass destination.
    segments : (M, 2, 2) array of line segments.

    Returns
    -------
    (M,) boolean array — True where the pass crosses the segment.
    """
    if len(segments) == 0:
        return np.array([], dtype=bool)

    # Pass vector: A->B
    a = pass_start  # (2,)
    b = pass_end  # (2,)
    ab = b - a  # (2,)

    # Segment vectors: C->D for each segment
    c = segments[:, 0, :]  # (M, 2)
    d = segments[:, 1, :]  # (M, 2)
    cd = d - c  # (M, 2)

    # Cross product helper: 2D cross of u and v = u_x*v_y - u_y*v_x
    def _cross2d(u: np.ndarray, v: np.ndarray) -> np.ndarray:
        return u[..., 0] * v[..., 1] - u[..., 1] * v[..., 0]

    # Test 1: Does segment CD straddle line AB?
    # sign(AB x AC) != sign(AB x AD)
    ac = c - a  # (M, 2)
    ad = d - a  # (M, 2)
    cross_ac = _cross2d(ab, ac)  # (M,)
    cross_ad = _cross2d(ab, ad)  # (M,)
    straddle_1 = cross_ac * cross_ad < 0  # strict inequality

    # Test 2: Does pass AB straddle line CD?
    # sign(CD x CA) != sign(CD x CB)
    ca = a - c  # (M, 2)
    cb = b - c  # (M, 2)
    cross_ca = _cross2d(cd, ca)  # (M,)
    cross_cb = _cross2d(cd, cb)  # (M,)
    straddle_2 = cross_ca * cross_cb < 0

    return straddle_1 & straddle_2


def _classify_intersection(
    pass_start: np.ndarray,
    pass_end: np.ndarray,
    clusters: list[np.ndarray],
    segments: np.ndarray,
    intersect_mask: np.ndarray,
    params: LineBreakingParams | None = None,
) -> str:
    """Classify line-breaking type: 'through' or 'around'.

    'through' — pass goes between two defenders in a cluster.
    'around' — pass goes outside the outermost defender (via sideline extension).
    """
    if params is None:
        params = LineBreakingParams()

    if not intersect_mask.any():
        return "through"  # fallback

    # Check if any intersected segment is a sideline extension
    # Sideline extensions have y == pitch_y_min or y == pitch_y_max at one end
    intersected_segs = segments[intersect_mask]
    y_vals = intersected_segs[:, :, 1].ravel()

    # If any intersection point is near the sideline extensions, it's 'around'
    eps = 0.5
    near_sideline = np.any((y_vals < params.pitch_y_min + eps) | (y_vals > params.pitch_y_max - eps))

    return "around" if near_sideline else "through"


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def detect_line_breaking(
    pass_start_x: float,
    pass_start_y: float,
    pass_end_x: float,
    pass_end_y: float,
    opponents: pd.DataFrame,
    params: LineBreakingParams | None = None,
) -> LineBreakingResult:
    """Detect whether a pass breaks through opponent defensive lines.

    Parameters
    ----------
    pass_start_x, pass_start_y : Pass origin in 120x80 coordinates.
    pass_end_x, pass_end_y : Pass destination in 120x80 coordinates.
    opponents : DataFrame with columns ``x``, ``y`` for opponent positions.
        Should exclude goalkeeper and teammates.
    params : LineBreakingParams (uses defaults if None).

    Returns
    -------
    LineBreakingResult with is_line_breaking, lines_broken, line_breaking_type.
    """
    if params is None:
        params = LineBreakingParams()

    # Edge case: backward pass
    if pass_end_x <= pass_start_x:
        return _EMPTY_RESULT

    # Edge case: short pass
    dx = pass_end_x - pass_start_x
    dy = pass_end_y - pass_start_y
    length = np.sqrt(dx * dx + dy * dy)
    if length < params.min_pass_length:
        return _EMPTY_RESULT

    # Drop NaN positions
    valid_opponents = opponents.dropna(subset=["x", "y"])
    if len(valid_opponents) < params.min_opponents:
        return _EMPTY_RESULT

    positions = np.column_stack([_col_f64(valid_opponents, "x"), _col_f64(valid_opponents, "y")])

    # Edge case: compressed formation
    x_spread = float(positions[:, 0].max() - positions[:, 0].min())
    if x_spread < params.min_x_spread:
        return _EMPTY_RESULT

    # Cluster into lines
    clusters = _cluster_opponents(positions, params)
    if not clusters:
        return _EMPTY_RESULT

    # Build line segments
    segments = _build_line_segments(clusters, params)
    if len(segments) == 0:
        return _EMPTY_RESULT

    # Test intersection
    pass_start = np.array([pass_start_x, pass_start_y])
    pass_end = np.array([pass_end_x, pass_end_y])
    intersect_mask = _segments_intersect(pass_start, pass_end, segments)

    if not intersect_mask.any():
        return _EMPTY_RESULT

    # Count lines broken: count distinct clusters that have at least one intersected segment
    # Map each segment back to its cluster
    seg_idx = 0
    cluster_broken = set[int]()
    for cluster_i, cluster in enumerate(clusters):
        n_segs = len(cluster) + 1  # players + 2 extensions - 1 = len(cluster) + 1
        cluster_mask = intersect_mask[seg_idx : seg_idx + n_segs]
        if cluster_mask.any():
            cluster_broken.add(cluster_i)
        seg_idx += n_segs

    lines_broken = len(cluster_broken)
    lb_type = _classify_intersection(pass_start, pass_end, clusters, segments, intersect_mask, params)

    return LineBreakingResult(
        is_line_breaking=True,
        lines_broken=lines_broken,
        line_breaking_type=lb_type,
    )


def _empty_row(event_id: str) -> dict[str, object]:
    """Return a non-line-breaking result dict for a single event."""
    return {
        "event_id": event_id,
        "is_line_breaking": False,
        "lines_broken": 0,
        "line_breaking_type": None,
    }


def detect_line_breaking_batch(
    passes_df: pd.DataFrame,
    opponents_by_event: dict[str, pd.DataFrame],
    params: LineBreakingParams | None = None,
) -> pd.DataFrame:
    """Detect line-breaking passes for a batch of passes.

    Caches Ward clustering results per unique opponent position snapshot so that
    multiple passes sharing the same freeze-frame opponents only cluster once.

    Parameters
    ----------
    passes_df : DataFrame with columns ``event_id``, ``start_x``, ``start_y``,
        ``end_x``, ``end_y``.
    opponents_by_event : Mapping from event_id to opponent positions DataFrame
        (columns: ``x``, ``y``).
    params : LineBreakingParams (uses defaults if None).

    Returns
    -------
    DataFrame with columns: event_id, is_line_breaking, lines_broken,
    line_breaking_type.
    """
    if params is None:
        params = LineBreakingParams()

    # Cache Ward clusters + line segments keyed by opponent position bytes.
    # Within a single frame, many passes share identical opponent positions;
    # Ward linkage is O(n^2) so avoiding redundant calls is worthwhile.
    cluster_cache: dict[bytes, tuple[list[np.ndarray], np.ndarray]] = {}

    results: list[dict[str, object]] = []

    for _, row in passes_df.iterrows():
        event_id = str(row["event_id"])
        pass_start_x = float(row["start_x"])
        pass_start_y = float(row["start_y"])
        pass_end_x = float(row["end_x"])
        pass_end_y = float(row["end_y"])

        # --- Early-exit guards (same as detect_line_breaking) ---
        if pass_end_x <= pass_start_x:
            results.append(_empty_row(event_id))
            continue

        dx = pass_end_x - pass_start_x
        dy = pass_end_y - pass_start_y
        length = np.sqrt(dx * dx + dy * dy)
        if length < params.min_pass_length:
            results.append(_empty_row(event_id))
            continue

        opponents = opponents_by_event.get(event_id, pd.DataFrame(columns=pd.Index(["x", "y"])))
        valid_opponents = opponents.dropna(subset=["x", "y"])
        if len(valid_opponents) < params.min_opponents:
            results.append(_empty_row(event_id))
            continue

        positions = np.column_stack([_col_f64(valid_opponents, "x"), _col_f64(valid_opponents, "y")])

        x_spread = float(positions[:, 0].max() - positions[:, 0].min())
        if x_spread < params.min_x_spread:
            results.append(_empty_row(event_id))
            continue

        # --- Cached clustering ---
        cache_key = positions.tobytes()
        if cache_key not in cluster_cache:
            clusters = _cluster_opponents(positions, params)
            segments = _build_line_segments(clusters, params) if clusters else np.empty((0, 2, 2), dtype=np.float64)
            cluster_cache[cache_key] = (clusters, segments)

        clusters, segments = cluster_cache[cache_key]

        if not clusters or len(segments) == 0:
            results.append(_empty_row(event_id))
            continue

        # --- Intersection test ---
        pass_start = np.array([pass_start_x, pass_start_y])
        pass_end = np.array([pass_end_x, pass_end_y])
        intersect_mask = _segments_intersect(pass_start, pass_end, segments)

        if not intersect_mask.any():
            results.append(_empty_row(event_id))
            continue

        # Count lines broken
        seg_idx = 0
        cluster_broken = set[int]()
        for cluster_i, cluster in enumerate(clusters):
            n_segs = len(cluster) + 1
            cluster_mask = intersect_mask[seg_idx : seg_idx + n_segs]
            if cluster_mask.any():
                cluster_broken.add(cluster_i)
            seg_idx += n_segs

        lines_broken = len(cluster_broken)
        lb_type = _classify_intersection(pass_start, pass_end, clusters, segments, intersect_mask, params)

        results.append(
            {
                "event_id": event_id,
                "is_line_breaking": True,
                "lines_broken": lines_broken,
                "line_breaking_type": lb_type,
            }
        )

    if not results:
        return pd.DataFrame(columns=pd.Index(["event_id", "is_line_breaking", "lines_broken", "line_breaking_type"]))

    return pd.DataFrame(results)
