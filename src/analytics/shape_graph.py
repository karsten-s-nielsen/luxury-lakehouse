"""Shape graph algorithm for formation detection (Sotudeh 2026).

Implements Algorithm 1 from:
  Sotudeh, H. (2026). Identification of Team Tactical Formations and Player
  Positions in Association Football. PhD thesis, ETH Zurich (DISS. ETH NO. 31732).
  Published: npj Complexity, DOI: 10.1038/s44260-025-00047-x.

The shape graph is a sparse, stable subgraph of the Delaunay triangulation.
Unstable edges (low angular stability) are iteratively removed and their
incident faces merged, producing a clean proximity graph that filters the
"flicker" noise inherent in raw Delaunay triangulations of player positions.

Position inference decomposes player coordinates into a 5x5 grid of tactical
roles (5 vertical levels: B/DM/M/AM/F, 5 horizontal levels: L/LC/C/RC/R)
using recursive face-center decomposition along each axis (thesis Chapter 4).
"""

from __future__ import annotations

import logging
from collections.abc import Mapping
from dataclasses import dataclass

import numpy as np
from scipy.spatial import Delaunay, QhullError  # type: ignore[import-untyped]

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Data types
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class PositionLabel:
    """A tactical position label from the 5x5 decomposition.

    Attributes:
        vertical: Vertical level — one of B, DM, M, AM, F.
        horizontal: Horizontal level — one of L, LC, C, RC, R.
        label: Combined label using thesis notation, e.g. "RCB" (not "B-RC").
    """

    vertical: str  # B | DM | M | AM | F
    horizontal: str  # L | LC | C | RC | R
    label: str  # e.g. "RCB"


@dataclass(frozen=True)
class ShapeGraph:
    """Result of the shape graph algorithm.

    Attributes:
        edges: (m, 2) array of player index pairs forming the stable subgraph.
        faces: List of frozensets, each containing player indices in one merged face.
        stabilities: (m,) array of angular stability values (degrees) per edge.
        points: (n, 2) original player positions.
    """

    edges: np.ndarray
    faces: list[frozenset[int]]
    stabilities: np.ndarray
    points: np.ndarray


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

_STABILITY_THRESHOLD: float = 45.0

# Vertical level labels (from back to front)
_VERTICAL_LEVELS: tuple[str, ...] = ("B", "DM", "M", "AM", "F")
# Horizontal level labels (from left to right)
_HORIZONTAL_LEVELS: tuple[str, ...] = ("L", "LC", "C", "RC", "R")

# 5x5 position label matrix (thesis Figure 4.5b)
# Maps (vertical, horizontal) → compact position label.
POSITION_LABEL_MATRIX: Mapping[str, Mapping[str, str]] = {
    "B": {"L": "LB", "LC": "LCB", "C": "CB", "RC": "RCB", "R": "RB"},
    "DM": {"L": "LWB", "LC": "LDM", "C": "CDM", "RC": "RDM", "R": "RWB"},
    "M": {"L": "LM", "LC": "LCM", "C": "CM", "RC": "RCM", "R": "RM"},
    "AM": {"L": "LWF", "LC": "LAM", "C": "CAM", "RC": "RAM", "R": "RWF"},
    "F": {"L": "LF", "LC": "LCF", "C": "CF", "RC": "RCF", "R": "RF"},
}


# Steep bridging edge threshold (thesis §4.2): 90° ± 22.5°
_STEEP_ANGLE_LOW: float = 67.5  # degrees
_STEEP_ANGLE_HIGH: float = 112.5  # degrees

# Adaptive fallback threshold (Option D): if >60% of players in one level
_DEGENERATE_FRACTION: float = 0.6


# ---------------------------------------------------------------------------
# Angular stability (thesis §3.3, equation 3.2)
# ---------------------------------------------------------------------------


def _angle_at_vertex(
    vertex: np.ndarray,
    arm1: np.ndarray,
    arm2: np.ndarray,
) -> float:
    """Compute the angle at *vertex* formed by rays to *arm1* and *arm2*.

    Returns angle in degrees in [0, 180].
    """
    v1 = arm1 - vertex
    v2 = arm2 - vertex
    dot = float(np.dot(v1, v2))
    mag1 = float(np.linalg.norm(v1))
    mag2 = float(np.linalg.norm(v2))
    if mag1 < 1e-12 or mag2 < 1e-12:
        return 0.0
    cos_val = np.clip(dot / (mag1 * mag2), -1.0, 1.0)
    return float(np.degrees(np.arccos(cos_val)))


def _compute_edge_stability(
    p_idx: int,
    q_idx: int,
    simplices: np.ndarray,
    points: np.ndarray,
) -> float:
    """Compute angular stability of edge (p, q) per Sotudeh equation 3.2.

    For an edge pq shared by two triangles with opposite vertices p' and q':
      gamma = angle at p' in triangle 1
      beta  = angle at q' in triangle 2
      alpha = 180 - (gamma + beta)

    Boundary edges (one incident triangle) have beta = 0 (the missing triangle
    contributes no opposite angle), so alpha = 180 - gamma.

    Args:
        p_idx: Index of first endpoint in *points*.
        q_idx: Index of second endpoint in *points*.
        simplices: (k, 3) array of triangle vertex indices.
        points: (n, 2) array of all point coordinates.

    Returns:
        Stability angle in degrees, range [0, 180].
    """
    edge_set = {p_idx, q_idx}

    # Find triangles incident to this edge and their opposite vertices
    opposite_angles: list[float] = []
    for simplex in simplices:
        simplex_verts = {int(simplex[0]), int(simplex[1]), int(simplex[2])}
        if edge_set.issubset(simplex_verts):
            # The opposite vertex is the one NOT in the edge
            opposite_idx = (simplex_verts - edge_set).pop()
            angle = _angle_at_vertex(points[opposite_idx], points[p_idx], points[q_idx])
            opposite_angles.append(angle)

    if not opposite_angles:
        return 180.0  # No incident triangles — treat as maximally stable

    # Sum of opposite angles (one or two triangles)
    angle_sum = sum(opposite_angles)
    return max(180.0 - angle_sum, 0.0)


def _compute_edge_stability_from_faces(
    p_idx: int,
    q_idx: int,
    faces: list[frozenset[int]],
    points: np.ndarray,
) -> float:
    """Compute stability of edge (p,q) using merged faces (UpdateStabilities).

    After face merging, the opposite angle from the merged-face side becomes the
    **minimum** angle formed by (p, r, q) over all vertices r in that face
    (excluding p and q). This prevents over-pruning (thesis p.35-36, Figure 3.8).

    For each side of the edge, find the incident face and compute:
      - If the side is the external face: opposite angle = 0°
      - Else: opposite angle = min over r in face \\ {p,q} of angle(p, r, q)

    alpha = 180 - (angle_side1 + angle_side2)
    """
    edge_set = frozenset({p_idx, q_idx})
    side_angles: list[float] = []

    for face in faces:
        if p_idx in face and q_idx in face:
            # This face is incident to the edge
            other_verts = face - edge_set
            if not other_verts:
                # Degenerate face (just the edge) — contributes 0°
                side_angles.append(0.0)
                continue
            # Minimum angle at any opposite vertex in this face
            min_angle = min(_angle_at_vertex(points[r], points[p_idx], points[q_idx]) for r in other_verts)
            side_angles.append(min_angle)

    # Boundary: missing side contributes 0°
    while len(side_angles) < 2:
        side_angles.append(0.0)

    # Use the two incident face contributions
    return max(180.0 - side_angles[0] - side_angles[1], 0.0)


# ---------------------------------------------------------------------------
# Edge extraction helpers
# ---------------------------------------------------------------------------


def _extract_edges(simplices: np.ndarray) -> set[tuple[int, int]]:
    """Extract unique edges from Delaunay simplices.

    Args:
        simplices: (k, 3) array of triangle vertex indices.

    Returns:
        Set of (min_idx, max_idx) pairs — canonical edge representation.
    """
    edges: set[tuple[int, int]] = set()
    for simplex in simplices:
        for i in range(3):
            for j in range(i + 1, 3):
                a, b = int(simplex[i]), int(simplex[j])
                edges.add((min(a, b), max(a, b)))
    return edges


# ---------------------------------------------------------------------------
# Shape graph construction (Algorithm 1, thesis p.38)
# ---------------------------------------------------------------------------


def _empty_shape_graph(positions: np.ndarray) -> ShapeGraph:
    """Return an empty shape graph for degenerate inputs."""
    return ShapeGraph(
        edges=np.empty((0, 2), dtype=int),
        faces=[],
        stabilities=np.empty(0),
        points=positions,
    )


def compute_shape_graph(
    positions: np.ndarray,
    stability_threshold: float = _STABILITY_THRESHOLD,
) -> ShapeGraph:
    """Compute the shape graph of player positions (Sotudeh Algorithm 1).

    1. Compute Delaunay triangulation of outfield player (x, y) positions.
    2. Calculate angular stability for each edge.
    3. Find edges with minimal stability; if below threshold, remove them
       (with tie-breaking to prevent over-pruning).
    4. Recompute stabilities on affected edges via UpdateStabilities.
    5. Repeat until all remaining edges have stability >= threshold.

    Args:
        positions: (n, 2) array of outfield player (x, y) coordinates.
        stability_threshold: Minimum angular stability in degrees (default 45.0).

    Returns:
        ShapeGraph with stable edges, merged faces, and stability values.
    """
    n = len(positions)
    if n < 3:
        return _empty_shape_graph(positions)

    try:
        tri = Delaunay(positions)
    except QhullError:
        logger.warning("Delaunay triangulation failed (likely collinear points)")
        return _empty_shape_graph(positions)

    # Initialize edges and faces
    active_edges: set[tuple[int, int]] = _extract_edges(tri.simplices)

    # Initial faces: one per simplex
    faces: list[frozenset[int]] = [frozenset(int(v) for v in s) for s in tri.simplices]

    # Compute initial stabilities using face-based method (UpdateStabilities).
    # Initially, faces are 1:1 with simplices, so this is equivalent to the
    # simplex-based method. Using faces from the start ensures consistency
    # after merges.
    edge_stability: dict[tuple[int, int], float] = {}
    for edge in active_edges:
        edge_stability[edge] = _compute_edge_stability_from_faces(edge[0], edge[1], faces, positions)

    # Iterative removal loop (Algorithm 1)
    # Note: edge_stability acts as the priority queue Q from the thesis.
    # Edges may be in active_edges but not in edge_stability (removed from Q
    # by tie-breaking but kept in the graph).
    while edge_stability:
        # Find minimum stability among edges still in the queue
        min_stability = min(edge_stability.values())
        if min_stability >= stability_threshold:
            break  # All queued edges are stable

        # Collect all edges with minimal stability (ties)
        min_edges = [e for e, s in edge_stability.items() if abs(s - min_stability) < 1e-10]

        if len(min_edges) > 1:
            # Tie-breaking (thesis Algorithm 1, lines 10-17)
            # Pick arbitrary e0, simulate its removal, check if any other
            # tied edge would become stable (>= threshold) after removal.
            e0 = min_edges[0]
            do_not_remove = False

            # Simulate removal of e0: merge faces and recompute stabilities
            sim_faces = list(faces)
            sim_edges = set(active_edges)
            sim_edges.discard(e0)
            sim_faces = _merge_faces_for_edge(e0, sim_faces)

            # Recompute stabilities on edges adjacent to the merged face
            sim_stability = dict(edge_stability)
            del sim_stability[e0]
            _update_stabilities_for_merged_face(e0, sim_faces, sim_edges, sim_stability, positions)

            for e in min_edges:
                if e == e0:
                    continue
                if e in sim_stability and sim_stability[e] >= stability_threshold:
                    do_not_remove = True
                    break

            if do_not_remove:
                # Thesis footnote 2, Algorithm 1: if removing one tied edge would
                # stabilize another, keep ALL tied edges in the graph. Remove them
                # from the priority queue (so they're never reconsidered as minimum-
                # stability candidates) but leave them in active_edges (the graph).
                for e in min_edges:
                    edge_stability.pop(e, None)
                continue

        # Remove all tied edges and merge faces
        for e in min_edges:
            active_edges.discard(e)
            edge_stability.pop(e, None)
            faces = _merge_faces_for_edge(e, faces)
            # Update stabilities for edges adjacent to the newly merged face
            _update_stabilities_for_merged_face(e, faces, active_edges, edge_stability, positions)

    # Build output — edges kept by tie-breaking may not be in edge_stability,
    # so recompute their stability from the final face state.
    remaining_edges = sorted(active_edges)
    if remaining_edges:
        stabilities = np.array(
            [
                edge_stability.get(e, _compute_edge_stability_from_faces(e[0], e[1], faces, positions))
                for e in remaining_edges
            ]
        )
        edges_arr = np.array(remaining_edges, dtype=int).reshape(-1, 2)
    else:
        stabilities = np.empty(0)
        edges_arr = np.empty((0, 2), dtype=int)

    return ShapeGraph(
        edges=edges_arr,
        faces=faces,
        stabilities=stabilities,
        points=positions,
    )


def _merge_faces_for_edge(
    edge: tuple[int, int],
    faces: list[frozenset[int]],
) -> list[frozenset[int]]:
    """Merge faces incident to *edge* into a single face.

    Args:
        edge: (p, q) edge being removed.
        faces: Current list of faces.

    Returns:
        Updated face list with incident faces merged.
    """
    incident_indices: list[int] = []
    for fi, face in enumerate(faces):
        if edge[0] in face and edge[1] in face:
            incident_indices.append(fi)

    if len(incident_indices) < 2:
        return faces  # Boundary edge — no merge needed

    merged: frozenset[int] = frozenset[int]().union(*[faces[fi] for fi in incident_indices])
    new_faces = [f for fi, f in enumerate(faces) if fi not in set(incident_indices)]
    new_faces.append(merged)
    return new_faces


def _update_stabilities_for_merged_face(
    removed_edge: tuple[int, int],
    faces: list[frozenset[int]],
    active_edges: set[tuple[int, int]],
    edge_stability: dict[tuple[int, int], float],
    points: np.ndarray,
) -> None:
    """Recompute stabilities for edges adjacent to a just-merged face.

    After removing *removed_edge* and merging its incident faces, find the
    merged face and recompute stability for all of its boundary edges that
    are still active.

    Mutates *edge_stability* in place.
    """
    # Find the merged face (the one containing both vertices of the removed edge)
    merged_face: frozenset[int] | None = None
    for face in faces:
        if removed_edge[0] in face and removed_edge[1] in face:
            merged_face = face
            break

    if merged_face is None:
        return

    # Recompute stability for each active edge on this face
    for edge in list(active_edges):
        if edge[0] in merged_face and edge[1] in merged_face:
            edge_stability[edge] = _compute_edge_stability_from_faces(edge[0], edge[1], faces, points)


# ---------------------------------------------------------------------------
# Position inference (thesis Chapter 4, §4.1-4.3)
# ---------------------------------------------------------------------------


def _face_centroids(
    shape_graph: ShapeGraph,
    axis: int | None = None,
    min_spread_frac: float = 0.1,
) -> np.ndarray:
    """Compute centroids of inter-line faces of the shape graph.

    Internal faces have >= 3 vertices (external/unbounded face excluded).

    When *axis* is specified (0 for x, 1 for y), faces whose vertex spread
    along that axis is less than *min_spread_frac* of the total player spread
    are excluded.  These "within-line" faces have centroids AT a positional
    line rather than BETWEEN lines, which skews the max/min face center
    boundaries used for level decomposition (Option B fix).

    Args:
        shape_graph: The computed shape graph.
        axis: If given, filter out faces with low spread along this axis.
        min_spread_frac: Minimum fraction of total player spread for a face
            to be considered inter-line (default 0.1 = 10%).

    Returns:
        (f, 2) array of face centroid coordinates, or empty (0, 2) array.
    """
    # Compute total player spread along the filter axis
    total_spread = 0.0
    if axis is not None:
        axis_values = shape_graph.points[:, axis]
        total_spread = float(np.ptp(axis_values))  # max - min

    centroids: list[np.ndarray] = []
    for face in shape_graph.faces:
        if len(face) < 3:
            continue
        pts = shape_graph.points[list(face)]

        # Option B: filter out within-line faces (low spread along decomposition axis)
        if axis is not None and total_spread > 1e-10:
            face_spread = float(np.ptp(pts[:, axis]))
            if face_spread < min_spread_frac * total_spread:
                continue

        centroids.append(pts.mean(axis=0))

    if not centroids:
        return np.empty((0, 2))
    return np.array(centroids)


def _bridging_edge_midpoints(
    middle_sg: ShapeGraph,
    axis: int,
) -> np.ndarray:
    """Get midpoints of steep bridging edges along *axis* (thesis §4.2).

    A bridging edge is one whose removal would disconnect the graph.  "Steep"
    means its slope is within 90° ± 22.5° relative to the decomposition axis
    (vertical axis=0 → steep means roughly vertical edges; horizontal axis=1 →
    roughly horizontal edges).

    These midpoints act as additional face centers when the middle shape graph
    lacks internal faces (tree-like structure).

    Returns:
        (k, 2) array of midpoint coordinates, or empty (0, 2).
    """
    if len(middle_sg.edges) == 0:
        return np.empty((0, 2))

    pts = middle_sg.points
    midpoints: list[np.ndarray] = []

    # Build adjacency for bridge detection
    adj: dict[int, set[int]] = {}
    for e in middle_sg.edges:
        a, b = int(e[0]), int(e[1])
        adj.setdefault(a, set()).add(b)
        adj.setdefault(b, set()).add(a)

    edge_set = {(int(e[0]), int(e[1])) for e in middle_sg.edges}

    for e in middle_sg.edges:
        a, b = int(e[0]), int(e[1])

        # Bridge detection: remove edge, check if still connected via BFS
        remaining = edge_set - {(a, b), (b, a)}
        adj_minus: dict[int, set[int]] = {}
        for ea, eb in remaining:
            adj_minus.setdefault(ea, set()).add(eb)
            adj_minus.setdefault(eb, set()).add(ea)

        all_nodes = set(adj.keys())
        if not all_nodes:
            continue
        visited: set[int] = set()
        stack = [next(iter(all_nodes))]
        while stack:
            node = stack.pop()
            if node in visited:
                continue
            visited.add(node)
            stack.extend(adj_minus.get(node, set()) - visited)

        is_bridge = len(visited) < len(all_nodes)
        if not is_bridge:
            continue

        # Check if edge is "steep" along the decomposition axis
        dx = float(pts[b][0] - pts[a][0])
        dy = float(pts[b][1] - pts[a][1])
        if abs(dx) < 1e-10 and abs(dy) < 1e-10:
            continue

        angle_deg = float(np.degrees(np.arctan2(abs(dy), abs(dx))))
        # For vertical decomposition (axis=0): steep = edge is roughly vertical (angle ~90°)
        # For horizontal decomposition (axis=1): steep = edge is roughly horizontal (angle ~0° or ~180°)
        if axis == 0:
            is_steep = _STEEP_ANGLE_LOW <= angle_deg <= _STEEP_ANGLE_HIGH
        else:
            is_steep = angle_deg <= (90.0 - _STEEP_ANGLE_LOW) or angle_deg >= (90.0 + (180.0 - _STEEP_ANGLE_HIGH))

        if is_steep:
            midpoints.append((pts[a] + pts[b]) / 2.0)

    if not midpoints:
        return np.empty((0, 2))
    return np.array(midpoints)


def _is_tree(sg: ShapeGraph) -> bool:
    """Check if the shape graph is a tree (no internal faces with >= 3 vertices)."""
    return all(len(face) < 3 for face in sg.faces)


def _decompose_middle(
    middle_indices: list[int],
    values: np.ndarray,
    levels: list[str],
    positions: np.ndarray | None,
    axis: int,
    flip: float,
    label_above: str,
    label_below: str,
    label_center: str,
    pitch_length: float = 105.0,
    pitch_width: float = 68.0,
) -> None:
    """Decompose middle players into 3 sub-levels using §4.1 + §4.2 special cases.

    Modifies *levels* in place.

    Args:
        middle_indices: Indices into *values* for middle players.
        values: Full (n,) array of coordinate values.
        levels: Full (n,) list of level labels — modified in place.
        positions: (n, 2) player positions (for building middle shape graph).
        axis: 0 for vertical (x), 1 for horizontal (y).
        flip: Direction multiplier (+1.0 or -1.0).
        label_above: Label for players above max middle face center (AM or RC).
        label_below: Label for players below min middle face center (DM or LC).
        label_center: Label for remaining middle players (M or C).
        pitch_length: Pitch length in meters (for team centroid fallback).
        pitch_width: Pitch width in meters (for team centroid fallback).
    """
    if len(middle_indices) < 2 or positions is None:
        return

    if len(middle_indices) == 2:
        # Two middle players — split by coordinate
        mid_vals = [(values[idx], idx) for idx in middle_indices]
        mid_vals.sort()
        levels[mid_vals[0][1]] = label_below
        levels[mid_vals[1][1]] = label_above
        return

    # Option C: build a NEW shape graph from middle players (thesis §4.1)
    middle_positions = positions[middle_indices]
    middle_sg = compute_shape_graph(middle_positions)

    if len(middle_sg.edges) > 0:
        mid_centroids = _face_centroids(middle_sg, axis=axis)

        # §4.2 — steep bridging edges: add their midpoints as face centers
        # when the middle shape graph is tree-like (few or no internal faces)
        bridge_mids = _bridging_edge_midpoints(middle_sg, axis=axis)
        if len(bridge_mids) > 0:
            if len(mid_centroids) > 0:
                mid_centroids = np.vstack([mid_centroids, bridge_mids])
            else:
                mid_centroids = bridge_mids

        if len(mid_centroids) > 0:
            mid_fc = mid_centroids[:, axis].copy()
            if flip < 0:
                mid_fc = -mid_fc

            mid_fc_max = float(np.max(mid_fc))
            mid_fc_min = float(np.min(mid_fc))

            if mid_fc_max - mid_fc_min > 1e-10:
                # §4.2 — diamond: if all face centers and >= 1 player are in the
                # middle third between the highest and lowest middle players, use
                # middle thirds instead of face centers.
                mid_values = values[middle_indices]
                mid_player_min = float(np.min(mid_values))
                mid_player_max = float(np.max(mid_values))
                mid_range = mid_player_max - mid_player_min
                if mid_range > 1e-10:
                    third_low = mid_player_min + mid_range / 3.0
                    third_high = mid_player_min + 2.0 * mid_range / 3.0
                    all_fc_in_middle = all(third_low <= fc <= third_high for fc in mid_fc)
                    any_player_in_middle = any(third_low <= values[idx] <= third_high for idx in middle_indices)
                    if all_fc_in_middle and any_player_in_middle:
                        # Diamond case — use thirds instead of face centers
                        mid_fc_max = third_high
                        mid_fc_min = third_low

                # §4.2 — asymmetric middle groups: if one sub-group has >= 2 more
                # players than the other, the larger group stays center, the smaller
                # gets above/below label based on position relative to face centers.
                above = [idx for idx in middle_indices if values[idx] > mid_fc_max]
                below = [idx for idx in middle_indices if values[idx] < mid_fc_min]
                center = [idx for idx in middle_indices if mid_fc_min <= values[idx] <= mid_fc_max]

                if len(above) > 0 and len(below) > 0 and abs(len(above) - len(below)) >= 2:
                    # Asymmetric: larger group → center label, smaller → directional
                    if len(above) > len(below):
                        for idx in above:
                            levels[idx] = label_center
                        for idx in below:
                            levels[idx] = label_below
                    else:
                        for idx in below:
                            levels[idx] = label_center
                        for idx in above:
                            levels[idx] = label_above
                    for idx in center:
                        levels[idx] = label_center
                else:
                    # Standard split
                    for idx in above:
                        levels[idx] = label_above
                    for idx in below:
                        levels[idx] = label_below
                    for idx in center:
                        levels[idx] = label_center
                return

        # §4.2 — tree shapes: when middle shape graph is a tree (no internal faces),
        # use team centroid relative to pitch thirds.
        if _is_tree(middle_sg):
            pitch_dim = pitch_length if axis == 0 else pitch_width
            centroid_val = float(np.mean(values[middle_indices]))
            if flip < 0:
                centroid_val = -centroid_val
            third = pitch_dim / 3.0
            if centroid_val < third:
                for idx in middle_indices:
                    levels[idx] = label_below
            elif centroid_val > 2.0 * third:
                for idx in middle_indices:
                    levels[idx] = label_above
            # else: stays center
            return

    # Fallback: use thirds of the middle player range
    mid_values = values[middle_indices]
    mid_min_val = float(np.min(mid_values))
    mid_max_val = float(np.max(mid_values))
    mid_range = mid_max_val - mid_min_val
    if mid_range > 1e-10:
        third_low = mid_min_val + mid_range / 3.0
        third_high = mid_min_val + 2.0 * mid_range / 3.0
        for idx in middle_indices:
            if values[idx] < third_low:
                levels[idx] = label_below
            elif values[idx] > third_high:
                levels[idx] = label_above


def _assign_levels_vertical(
    values: np.ndarray,
    face_centers: np.ndarray,
    shape_graph: ShapeGraph | None = None,
    positions: np.ndarray | None = None,
    attacking_direction: float = 1.0,
) -> list[str]:
    """Assign vertical levels (B/DM/M/AM/F) using face-center decomposition.

    Implements thesis §4.1 with Options B, C, D and all §4.2 special cases.

    Args:
        values: (n,) array of coordinate values along the attacking axis.
        face_centers: (f,) array of (inter-line) face center values along the same axis.
        shape_graph: Original shape graph.
        positions: (n, 2) original player positions.
        attacking_direction: +1.0 or -1.0.

    Returns:
        List of level labels, one per value.
    """
    n = len(values)
    if n == 0:
        return []

    if len(face_centers) == 0:
        return _equal_frequency_assign(values, _VERTICAL_LEVELS)

    levels = ["M"] * n

    fc_max = float(np.max(face_centers))
    fc_min = float(np.min(face_centers))

    # Initial B/M/F split
    front_mask = values > fc_max
    back_mask = values < fc_min
    middle_mask = ~front_mask & ~back_mask

    for i in range(n):
        if front_mask[i]:
            levels[i] = "F"
        elif back_mask[i]:
            levels[i] = "B"

    # Option D: adaptive fallback — if >60% in M, tighten bounds using percentiles.
    m_count = sum(1 for lv in levels if lv == "M")
    if n >= 5 and m_count > _DEGENERATE_FRACTION * n and len(face_centers) >= 4:
        fc_p25 = float(np.percentile(face_centers, 25))
        fc_p75 = float(np.percentile(face_centers, 75))
        if fc_p75 - fc_p25 > 1e-10 and fc_p75 < fc_max and fc_p25 > fc_min:
            levels = ["M"] * n
            fc_max = fc_p75
            fc_min = fc_p25
            front_mask = values > fc_max
            back_mask = values < fc_min
            middle_mask = ~front_mask & ~back_mask
            for i in range(n):
                if front_mask[i]:
                    levels[i] = "F"
                elif back_mask[i]:
                    levels[i] = "B"

    # Middle decomposition: Option C + all §4.2 special cases
    middle_indices = [i for i in range(n) if middle_mask[i]]
    _decompose_middle(
        middle_indices,
        values,
        levels,
        positions,
        axis=0,
        flip=attacking_direction,
        label_above="AM",
        label_below="DM",
        label_center="M",
    )

    return levels


def _assign_levels_horizontal(
    values: np.ndarray,
    face_centers: np.ndarray,
    shape_graph: ShapeGraph | None = None,
    positions: np.ndarray | None = None,
) -> list[str]:
    """Assign horizontal levels (L/LC/C/RC/R) using face-center decomposition.

    Same algorithm as vertical (§4.1 + §4.2) but with horizontal labels.

    Args:
        values: (n,) array of coordinate values along the lateral axis.
        face_centers: (f,) array of face center values along the same axis.
        shape_graph: Original shape graph.
        positions: (n, 2) original player positions.

    Returns:
        List of level labels, one per value (original order preserved).
    """
    n = len(values)
    if n == 0:
        return []

    if len(face_centers) == 0:
        return _equal_frequency_assign(values, _HORIZONTAL_LEVELS)

    levels = ["C"] * n

    fc_max = float(np.max(face_centers))
    fc_min = float(np.min(face_centers))

    right_mask = values > fc_max
    left_mask = values < fc_min
    middle_mask = ~right_mask & ~left_mask

    for i in range(n):
        if right_mask[i]:
            levels[i] = "R"
        elif left_mask[i]:
            levels[i] = "L"

    # Option D: adaptive fallback for horizontal
    c_count = sum(1 for lv in levels if lv == "C")
    if n >= 5 and c_count > _DEGENERATE_FRACTION * n and len(face_centers) >= 4:
        fc_p25 = float(np.percentile(face_centers, 25))
        fc_p75 = float(np.percentile(face_centers, 75))
        if fc_p75 - fc_p25 > 1e-10 and fc_p75 < fc_max and fc_p25 > fc_min:
            levels = ["C"] * n
            fc_max = fc_p75
            fc_min = fc_p25
            right_mask = values > fc_max
            left_mask = values < fc_min
            middle_mask = ~right_mask & ~left_mask
            for i in range(n):
                if right_mask[i]:
                    levels[i] = "R"
                elif left_mask[i]:
                    levels[i] = "L"

    # Middle decomposition: Option C + all §4.2 special cases
    middle_indices = [i for i in range(n) if middle_mask[i]]
    _decompose_middle(
        middle_indices,
        values,
        levels,
        positions,
        axis=1,
        flip=1.0,  # horizontal is not flipped by attacking direction
        label_above="RC",
        label_below="LC",
        label_center="C",
    )

    return levels


def _equal_frequency_assign(values: np.ndarray, levels: tuple[str, ...]) -> list[str]:
    """Fallback: assign level labels using equal-frequency binning.

    Used when no face centers are available (degenerate shape graph).

    Args:
        values: (n,) array of coordinate values.
        levels: Ordered level labels.

    Returns:
        List of level labels, one per value (original order preserved).
    """
    n = len(values)
    if n == 0:
        return []

    n_levels = len(levels)
    sorted_indices = np.argsort(values)
    assignments = [""] * n

    for rank, idx in enumerate(sorted_indices):
        bin_idx = min(rank * n_levels // n, n_levels - 1)
        assignments[int(idx)] = levels[bin_idx]

    return assignments


def infer_positions(
    shape_graph: ShapeGraph,
    positions: np.ndarray,
    attacking_direction: float,
) -> list[PositionLabel]:
    """Infer tactical positions from player positions via face-center decomposition.

    Vertical decomposition uses the x-coordinate (attacking axis) with face
    centroids as split boundaries. Horizontal decomposition uses the y-coordinate
    (lateral axis) similarly. This follows thesis Chapter 4, §4.1-4.3.

    If ``attacking_direction < 0``, the x-axis is flipped before level assignment,
    so that "B" (back) always refers to the defensive end.

    Args:
        shape_graph: Computed shape graph (its faces provide decomposition boundaries).
        positions: (n, 2) array of player positions.
        attacking_direction: +1.0 if attacking toward higher x, -1.0 if lower.

    Returns:
        List of PositionLabel, one per player. Empty if shape graph is empty.
    """
    if len(shape_graph.edges) == 0:
        return []

    n = len(positions)
    x = positions[:, 0].copy()
    y = positions[:, 1].copy()

    # Flip x if attacking direction is reversed
    if attacking_direction < 0:
        x = -x

    # Compute face centroids for decomposition boundaries.
    # Option B: filter out within-line faces per axis to get inter-line centroids.
    centroids_v = _face_centroids(shape_graph, axis=0)  # filter on x-spread for vertical
    centroids_h = _face_centroids(shape_graph, axis=1)  # filter on y-spread for horizontal

    face_centers_x = centroids_v[:, 0].copy() if len(centroids_v) > 0 else np.empty(0)
    face_centers_y = centroids_h[:, 1].copy() if len(centroids_h) > 0 else np.empty(0)

    # Flip face centers too if attacking direction is reversed
    if attacking_direction < 0 and len(face_centers_x) > 0:
        face_centers_x = -face_centers_x

    # Assign vertical and horizontal levels
    vertical = _assign_levels_vertical(x, face_centers_x, shape_graph, positions, attacking_direction)
    horizontal = _assign_levels_horizontal(y, face_centers_y, shape_graph, positions)

    return [
        PositionLabel(
            vertical=vertical[i],
            horizontal=horizontal[i],
            label=POSITION_LABEL_MATRIX[vertical[i]][horizontal[i]],
        )
        for i in range(n)
    ]
