"""Shape graph construction — Algorithm 1 from Sotudeh (2026).

Implements the iterative edge-removal algorithm from:
  Sotudeh, H. (2026). Identification of Team Tactical Formations and Player
  Positions in Association Football. PhD thesis, ETH Zurich (DISS. ETH NO. 31732).
  Published: npj Complexity, DOI: 10.1038/s44260-025-00047-x.

The shape graph is a sparse, stable subgraph of the Delaunay triangulation.
Unstable edges (low angular stability) are iteratively removed and their
incident faces merged, producing a clean proximity graph that filters the
"flicker" noise inherent in raw Delaunay triangulations of player positions.
"""

from __future__ import annotations

import logging
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
