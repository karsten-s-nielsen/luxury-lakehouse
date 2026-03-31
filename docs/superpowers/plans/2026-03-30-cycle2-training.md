# Cycle 2 — "Training" Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Implement shape graph formation detection (D36+D37) and transformer player embeddings with adversarial team debiasing (D18+D30).

**Architecture:** Two independent tracks on a single feature branch. Track B (shape graphs) builds first — pure numpy/scipy geometry with no GPU dependency. Track A (transformer embeddings) follows — HF Jobs GPU training with a two-stage script (MLM → adversarial). Formation table refactored to support dual detectors.

**Tech Stack:** numpy, scipy.spatial.Delaunay, scipy.cluster.hierarchy, torch (transformer), HF Hub, MLflow, dbt (Spark SQL), pgvector, Taipy

**Spec:** `docs/superpowers/specs/2026-03-30-cycle2-training-design.md`

---

## File Map

### Track B — New Files
| File | Responsibility |
|------|---------------|
| `src/analytics/shape_graph.py` | Shape graph algorithm + position inference |
| `src/tests/test_shape_graph.py` | Unit tests + benchmarks |
| `workflow-cards/wf-shape-graphs.yaml` | Workflow card with Sotudeh citations |
| `dbt_project/models/staging/shape_graphs/_shape_graphs__sources.yml` | dbt source for `player_positions` bronze table |
| `dbt_project/models/staging/shape_graphs/stg_shape_graphs__positions.sql` | Staging model for frame-level positions |
| `dbt_project/models/marts/fct_player_positions.sql` | Frame-level position labels mart |
| `dbt_project/models/marts/fct_position_maps.sql` | Aggregated 5×5 position maps mart |

### Track B — Modified Files
| File | Change |
|------|--------|
| `src/ingestion/formations.py` | Add shape graph detection + `detector` column + frame-level positions output |
| `dbt_project/models/staging/formations/_formations__sources.yml` | Add `detector` column to source |
| `dbt_project/models/staging/formations/stg_formations__labels.sql` | Pass through `detector` column |
| `dbt_project/models/marts/fct_formation_labels.sql` | Add `detector` to surrogate key + select |
| `dbt_project/models/marts/_marts__models.yml` | Add `detector` to contract, add `fct_player_positions` + `fct_position_maps` contracts |
| `workflow-cards/wf-formations.yaml` | Update description to reference both detectors |
| `scripts/refresh_synced_tables.py` | Add `fct_player_positions_synced`, `fct_position_maps_synced` |
| `hf_taipy_app/src/state/team_shape.py` | No file change — already reads `fct_formation_labels_synced` (table name unchanged) |
| `hf_taipy_app/src/template.py` | Add "Shape Graph" glossary term |

### Track A — New Files
| File | Responsibility |
|------|---------------|
| `src/analytics/football2vec_transformer.py` | Transformer encoder + spatial MLP + gradient reversal layer |
| `src/tests/test_football2vec_transformer.py` | Model construction, forward pass shapes, tokenization |
| `scripts/train_football2vec_v2.py` | PEP 723 HF Jobs script (Stage 1 MLM + Stage 2 adversarial) |
| `src/ingestion/export_embeddings_training_data.py` | Export SPADL sequences from Delta to HF dataset |
| `workflow-cards/wf-football2vec-v2.yaml` | Replaces wf-football2vec for the transformer model |

### Track A — Modified Files
| File | Change |
|------|--------|
| `src/ingestion/player_embeddings.py` | Import 128d embeddings from HF dataset → Delta (replaces Doc2Vec training) |
| `dbt_project/models/marts/fct_player_embeddings_season.sql` | `sequence(0, 31)` → `sequence(0, 127)` |
| `dbt_project/models/marts/fct_player_embeddings_career.sql` | `sequence(0, 31)` → `sequence(0, 127)` |
| `dbt_project/models/staging/embeddings/_embeddings__sources.yml` | Update behavioral_vector description (128d) |
| `dbt_project/models/marts/_marts__models.yml` | Update embedding contracts (array size unchanged — type is `array<double>`) |
| `scripts/create_indexes.py` | HNSW dims 32 → 128 |
| `hf_taipy_app/src/state/player_similarity.py` | `_get_vector_dimension` returns 128 |
| `pyproject.toml` | Add `export_embeddings_training_data` entry point |

---

## Track B — Shape Graphs (D36 + D37)

### Task 1: Shape Graph Core Data Types and Angular Stability

**Files:**
- Create: `src/analytics/shape_graph.py`
- Create: `src/tests/test_shape_graph.py`

- [ ] **Step 1: Write the failing test for angular stability**

```python
# src/tests/test_shape_graph.py
"""Tests for shape graph algorithm (Sotudeh 2026)."""
from __future__ import annotations

import numpy as np
import pytest

from analytics.shape_graph import angular_stability, ShapeGraph, PositionLabel


class TestAngularStability:
    """Test the angular stability metric for Delaunay edges."""

    def test_equilateral_triangle_pair_high_stability(self) -> None:
        """Two equilateral triangles sharing an edge have high angular stability.

        The circumcenters of equilateral triangles are at their centroids.
        For a symmetric diamond (two equilateral triangles), the angle between
        circumcenters measured from the shared edge should be close to 180°.
        """
        # Diamond: two equilateral triangles sharing edge (1,0)-(0,1)
        # Triangle 1: (0,0), (1,0), (0,1)  — upper-left
        # Triangle 2: (1,0), (1,1), (0,1)  — lower-right (approx equilateral)
        points = np.array([[0.0, 0.0], [2.0, 0.0], [1.0, np.sqrt(3)], [1.0, -np.sqrt(3)]])
        # Triangles: (0,1,2) and (0,1,3) share edge (0,1)
        # Circumcenters both at (1, 0) for equilateral — stability = 180°
        stability = angular_stability(
            edge=(0, 1),
            simplices=np.array([[0, 1, 2], [0, 1, 3]]),
            points=points,
        )
        assert stability > 90.0, f"Expected high stability for symmetric diamond, got {stability}"

    def test_degenerate_narrow_triangle_low_stability(self) -> None:
        """A near-degenerate configuration should have low angular stability."""
        # Two triangles sharing an edge where one is very flat
        points = np.array([[0.0, 0.0], [10.0, 0.0], [5.0, 0.01], [5.0, -5.0]])
        stability = angular_stability(
            edge=(0, 1),
            simplices=np.array([[0, 1, 2], [0, 1, 3]]),
            points=points,
        )
        assert stability < 45.0, f"Expected low stability for flat triangle, got {stability}"

    def test_boundary_edge_returns_180(self) -> None:
        """An edge on the convex hull (one incident triangle) should be maximally stable."""
        points = np.array([[0.0, 0.0], [1.0, 0.0], [0.5, 1.0]])
        stability = angular_stability(
            edge=(0, 1),
            simplices=np.array([[0, 1, 2]]),
            points=points,
        )
        assert stability == 180.0
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest src/tests/test_shape_graph.py::TestAngularStability -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'analytics.shape_graph'`

- [ ] **Step 3: Implement data types and angular stability**

```python
# src/analytics/shape_graph.py
"""Shape graph algorithm for formation detection (Sotudeh 2026).

Implements Algorithm 1 from:
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

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class PositionLabel:
    """A tactical position label from the 5×5 decomposition."""

    vertical: str    # B | DM | M | AM | F
    horizontal: str  # L | LC | C | RC | R
    label: str       # e.g. "DM-RC"


@dataclass(frozen=True)
class ShapeGraph:
    """Result of the shape graph algorithm.

    Attributes:
        edges: (m, 2) array of player index pairs forming the stable subgraph.
        faces: List of sets, each containing the player indices in one merged face.
        stabilities: (m,) array of angular stability values for each remaining edge.
        points: (n, 2) original player positions.
    """

    edges: np.ndarray
    faces: list[frozenset[int]]
    stabilities: np.ndarray
    points: np.ndarray


def _circumcenter(a: np.ndarray, b: np.ndarray, c: np.ndarray) -> np.ndarray:
    """Compute circumcenter of triangle (a, b, c).

    Uses the perpendicular bisector intersection formula.
    Returns (2,) array of (x, y) coordinates.
    """
    ax, ay = float(a[0]), float(a[1])
    bx, by = float(b[0]), float(b[1])
    cx, cy = float(c[0]), float(c[1])

    d = 2.0 * (ax * (by - cy) + bx * (cy - ay) + cx * (ay - by))
    if abs(d) < 1e-12:
        # Degenerate (collinear) — return centroid as fallback
        return np.array([(ax + bx + cx) / 3.0, (ay + by + cy) / 3.0])

    ux = ((ax * ax + ay * ay) * (by - cy) + (bx * bx + by * by) * (cy - ay)
          + (cx * cx + cy * cy) * (ay - by)) / d
    uy = ((ax * ax + ay * ay) * (cx - bx) + (bx * bx + by * by) * (ax - cx)
          + (cx * cx + cy * cy) * (bx - ax)) / d
    return np.array([ux, uy])


def angular_stability(
    edge: tuple[int, int],
    simplices: np.ndarray,
    points: np.ndarray,
) -> float:
    """Compute angular stability of an edge in a triangulation.

    The stability is the angle (in degrees) between the circumcenters of the
    two triangles incident to this edge, as seen from the edge midpoint.
    Boundary edges (one incident triangle) return 180.0 (maximally stable).

    Args:
        edge: (i, j) indices of the edge endpoints in ``points``.
        simplices: (k, 3) array of triangle vertex indices that are incident
            to this edge. Typically k=2 (interior edge) or k=1 (boundary).
        points: (n, 2) array of all point coordinates.

    Returns:
        Stability angle in degrees, range [0, 180].
    """
    edge_set = {edge[0], edge[1]}

    # Find triangles incident to this edge
    incident: list[np.ndarray] = []
    for simplex in simplices:
        simplex_set = set(int(s) for s in simplex)
        if edge_set.issubset(simplex_set):
            incident.append(simplex)

    if len(incident) < 2:
        return 180.0  # Boundary edge — maximally stable

    # Compute circumcenters
    cc0 = _circumcenter(points[incident[0][0]], points[incident[0][1]], points[incident[0][2]])
    cc1 = _circumcenter(points[incident[1][0]], points[incident[1][1]], points[incident[1][2]])

    # Angle between circumcenters as seen from edge midpoint
    midpoint = (points[edge[0]] + points[edge[1]]) / 2.0
    v0 = cc0 - midpoint
    v1 = cc1 - midpoint

    dot = float(np.dot(v0, v1))
    mag0 = float(np.linalg.norm(v0))
    mag1 = float(np.linalg.norm(v1))

    if mag0 < 1e-12 or mag1 < 1e-12:
        return 180.0  # Coincident circumcenters — maximally stable

    cos_angle = np.clip(dot / (mag0 * mag1), -1.0, 1.0)
    return float(np.degrees(np.arccos(cos_angle)))
```

- [ ] **Step 4: Run test to verify it passes**

Run: `uv run pytest src/tests/test_shape_graph.py::TestAngularStability -v`
Expected: PASS (3 tests)

---

### Task 2: Shape Graph Construction (Algorithm 1)

**Files:**
- Modify: `src/analytics/shape_graph.py`
- Modify: `src/tests/test_shape_graph.py`

- [ ] **Step 1: Write the failing test for compute_shape_graph**

Add to `src/tests/test_shape_graph.py`:

```python
from analytics.shape_graph import compute_shape_graph


class TestComputeShapeGraph:
    """Test the full shape graph construction (Sotudeh Algorithm 1)."""

    def test_classic_442_returns_stable_graph(self) -> None:
        """A canonical 4-4-2 arrangement should produce a connected shape graph."""
        # 10 outfield players in a 4-4-2 on a 105x68 pitch
        positions = np.array([
            # Defenders (4)
            [20.0, 10.0], [20.0, 25.0], [20.0, 43.0], [20.0, 58.0],
            # Midfielders (4)
            [40.0, 10.0], [40.0, 25.0], [40.0, 43.0], [40.0, 58.0],
            # Forwards (2)
            [60.0, 25.0], [60.0, 43.0],
        ])
        sg = compute_shape_graph(positions)

        assert isinstance(sg, ShapeGraph)
        assert sg.edges.shape[1] == 2  # Each edge is a pair
        assert len(sg.edges) > 0  # At least some edges remain
        assert all(s >= 45.0 for s in sg.stabilities)  # All edges stable
        assert len(sg.faces) >= 1  # At least one face
        # All player indices should appear in at least one face
        all_players = set()
        for face in sg.faces:
            all_players.update(face)
        assert all_players == set(range(10))

    def test_minimum_players_three(self) -> None:
        """With exactly 3 players, Delaunay gives one triangle — no edges to remove."""
        positions = np.array([[0.0, 0.0], [10.0, 0.0], [5.0, 8.0]])
        sg = compute_shape_graph(positions)
        assert len(sg.edges) >= 2  # Triangle has 3 edges, some may be boundary

    def test_fewer_than_three_returns_empty(self) -> None:
        """Fewer than 3 players cannot form a triangulation."""
        positions = np.array([[0.0, 0.0], [10.0, 0.0]])
        sg = compute_shape_graph(positions)
        assert len(sg.edges) == 0
        assert len(sg.faces) == 0

    def test_collinear_players(self) -> None:
        """Collinear points cannot form a Delaunay triangulation — degenerate case."""
        positions = np.array([[0.0, 0.0], [5.0, 0.0], [10.0, 0.0], [15.0, 0.0]])
        sg = compute_shape_graph(positions)
        # scipy.spatial.Delaunay raises QhullError for collinear points
        # We should get an empty shape graph gracefully
        assert isinstance(sg, ShapeGraph)
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest src/tests/test_shape_graph.py::TestComputeShapeGraph -v`
Expected: FAIL — `ImportError: cannot import name 'compute_shape_graph'`

- [ ] **Step 3: Implement compute_shape_graph**

Add to `src/analytics/shape_graph.py`:

```python
from scipy.spatial import Delaunay


# Stability threshold in degrees (Sotudeh, thesis p.26)
_STABILITY_THRESHOLD: float = 45.0


def _extract_edges(simplices: np.ndarray) -> set[tuple[int, int]]:
    """Extract unique edges from Delaunay simplices."""
    edges: set[tuple[int, int]] = set()
    for simplex in simplices:
        for i in range(3):
            for j in range(i + 1, 3):
                a, b = int(simplex[i]), int(simplex[j])
                edges.add((min(a, b), max(a, b)))
    return edges


def _find_incident_simplices(
    edge: tuple[int, int], simplices: np.ndarray,
) -> list[np.ndarray]:
    """Find all simplices incident to a given edge."""
    edge_set = {edge[0], edge[1]}
    return [s for s in simplices if edge_set.issubset({int(s[0]), int(s[1]), int(s[2])})]


def compute_shape_graph(
    positions: np.ndarray,
    stability_threshold: float = _STABILITY_THRESHOLD,
) -> ShapeGraph:
    """Compute the shape graph of player positions (Sotudeh Algorithm 1).

    1. Compute Delaunay triangulation
    2. Calculate angular stability for each edge
    3. Find least stable edge; if stability < threshold, remove it and merge faces
    4. Recompute stabilities on affected edges
    5. Repeat until all edges are stable

    Args:
        positions: (n, 2) array of outfield player (x, y) coordinates.
        stability_threshold: Minimum angular stability in degrees (default 45.0).

    Returns:
        ShapeGraph with stable edges, merged faces, and stability values.
    """
    n = len(positions)
    if n < 3:
        return ShapeGraph(
            edges=np.empty((0, 2), dtype=int),
            faces=[],
            stabilities=np.empty(0),
            points=positions,
        )

    try:
        tri = Delaunay(positions)
    except Exception:
        logger.warning("Delaunay triangulation failed (likely collinear points)")
        return ShapeGraph(
            edges=np.empty((0, 2), dtype=int),
            faces=[],
            stabilities=np.empty(0),
            points=positions,
        )

    # Initialize: each simplex is a face (set of vertex indices)
    active_simplices = [np.array(s) for s in tri.simplices]
    active_edges = _extract_edges(tri.simplices)

    # Build face tracking: each face is a frozenset of player indices
    # Initially, one face per simplex
    faces: list[frozenset[int]] = [frozenset(int(v) for v in s) for s in tri.simplices]

    # Map each edge to the face indices it borders
    def _edge_to_face_indices(edge: tuple[int, int]) -> list[int]:
        result = []
        for fi, face in enumerate(faces):
            if edge[0] in face and edge[1] in face:
                result.append(fi)
        return result

    # Iteratively remove least stable edge
    while active_edges:
        # Compute stabilities for all active edges
        edge_stabilities: dict[tuple[int, int], float] = {}
        for edge in active_edges:
            incident = _find_incident_simplices(edge, np.array(active_simplices))
            edge_stabilities[edge] = angular_stability(edge, np.array(active_simplices), positions)

        # Find least stable edge
        min_edge = min(edge_stabilities, key=edge_stabilities.get)  # type: ignore[arg-type]
        min_stability = edge_stabilities[min_edge]

        if min_stability >= stability_threshold:
            break  # All edges are stable — done

        # Remove the least stable edge and merge its incident faces
        active_edges.discard(min_edge)

        face_indices = _edge_to_face_indices(min_edge)
        if len(face_indices) >= 2:
            # Merge faces
            merged = frozenset[int]().union(*[faces[fi] for fi in face_indices])
            # Remove old faces (in reverse order to preserve indices)
            for fi in sorted(face_indices, reverse=True):
                faces.pop(fi)
            faces.append(merged)

        # Remove any simplices that contained the removed edge from active list
        new_simplices = []
        edge_set = {min_edge[0], min_edge[1]}
        for s in active_simplices:
            s_set = {int(s[0]), int(s[1]), int(s[2])}
            if not edge_set.issubset(s_set):
                new_simplices.append(s)
        active_simplices = new_simplices

    # Build output
    remaining_edges = sorted(active_edges)
    stabilities = np.array([
        angular_stability(e, np.array(active_simplices) if active_simplices else np.empty((0, 3), dtype=int), positions)
        for e in remaining_edges
    ]) if remaining_edges else np.empty(0)

    return ShapeGraph(
        edges=np.array(remaining_edges, dtype=int).reshape(-1, 2) if remaining_edges else np.empty((0, 2), dtype=int),
        faces=faces,
        stabilities=stabilities,
        points=positions,
    )
```

- [ ] **Step 4: Run test to verify it passes**

Run: `uv run pytest src/tests/test_shape_graph.py::TestComputeShapeGraph -v`
Expected: PASS (4 tests)

- [ ] **Step 5: Run all shape graph tests together**

Run: `uv run pytest src/tests/test_shape_graph.py -v`
Expected: PASS (7 tests)

---

### Task 3: Position Inference (5×5 Level Decomposition)

**Files:**
- Modify: `src/analytics/shape_graph.py`
- Modify: `src/tests/test_shape_graph.py`

- [ ] **Step 1: Write the failing test for infer_positions**

Add to `src/tests/test_shape_graph.py`:

```python
from analytics.shape_graph import infer_positions


class TestInferPositions:
    """Test position inference via 5×5 level decomposition."""

    def test_442_vertical_levels(self) -> None:
        """A 4-4-2 should decompose into 3 vertical levels (B, M, F)."""
        positions = np.array([
            # Defenders (4) — leftmost x
            [20.0, 10.0], [20.0, 25.0], [20.0, 43.0], [20.0, 58.0],
            # Midfielders (4) — middle x
            [40.0, 10.0], [40.0, 25.0], [40.0, 43.0], [40.0, 58.0],
            # Forwards (2) — rightmost x
            [60.0, 25.0], [60.0, 43.0],
        ])
        sg = compute_shape_graph(positions)
        labels = infer_positions(sg, positions, attacking_direction=1.0)

        assert len(labels) == 10
        # Defenders should be in the B (back) vertical level
        for i in range(4):
            assert labels[i].vertical == "B", f"Player {i} expected B, got {labels[i].vertical}"
        # Midfielders should be M
        for i in range(4, 8):
            assert labels[i].vertical == "M", f"Player {i} expected M, got {labels[i].vertical}"
        # Forwards should be F
        for i in range(8, 10):
            assert labels[i].vertical == "F", f"Player {i} expected F, got {labels[i].vertical}"

    def test_442_horizontal_levels(self) -> None:
        """A 4-4-2 line of 4 should span L, LC, RC, R horizontally."""
        positions = np.array([
            [20.0, 10.0], [20.0, 25.0], [20.0, 43.0], [20.0, 58.0],
            [40.0, 10.0], [40.0, 25.0], [40.0, 43.0], [40.0, 58.0],
            [60.0, 25.0], [60.0, 43.0],
        ])
        sg = compute_shape_graph(positions)
        labels = infer_positions(sg, positions, attacking_direction=1.0)

        # Defender line horizontal: 10, 25, 43, 58 → L, LC, RC, R
        def_horizontals = [labels[i].horizontal for i in range(4)]
        assert def_horizontals == ["L", "LC", "RC", "R"]

    def test_reversed_attacking_direction(self) -> None:
        """Reversing attacking direction should flip vertical labels."""
        positions = np.array([
            [20.0, 10.0], [20.0, 25.0], [20.0, 43.0], [20.0, 58.0],
            [40.0, 10.0], [40.0, 25.0], [40.0, 43.0], [40.0, 58.0],
            [60.0, 25.0], [60.0, 43.0],
        ])
        sg = compute_shape_graph(positions)
        labels_fwd = infer_positions(sg, positions, attacking_direction=1.0)
        labels_rev = infer_positions(sg, positions, attacking_direction=-1.0)

        # Defenders in forward direction → forwards in reverse
        for i in range(4):
            assert labels_fwd[i].vertical == "B"
            assert labels_rev[i].vertical == "F"

    def test_label_format(self) -> None:
        """Each label should be 'VERTICAL-HORIZONTAL'."""
        positions = np.array([
            [20.0, 10.0], [20.0, 25.0], [20.0, 43.0], [20.0, 58.0],
            [40.0, 10.0], [40.0, 25.0], [40.0, 43.0], [40.0, 58.0],
            [60.0, 25.0], [60.0, 43.0],
        ])
        sg = compute_shape_graph(positions)
        labels = infer_positions(sg, positions, attacking_direction=1.0)
        for lbl in labels:
            assert lbl.label == f"{lbl.vertical}-{lbl.horizontal}"
            assert lbl.vertical in {"B", "DM", "M", "AM", "F"}
            assert lbl.horizontal in {"L", "LC", "C", "RC", "R"}

    def test_fewer_than_three_returns_empty(self) -> None:
        """If shape graph is empty, infer_positions returns empty list."""
        positions = np.array([[0.0, 0.0], [10.0, 0.0]])
        sg = compute_shape_graph(positions)
        labels = infer_positions(sg, positions, attacking_direction=1.0)
        assert labels == []
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest src/tests/test_shape_graph.py::TestInferPositions -v`
Expected: FAIL — `ImportError: cannot import name 'infer_positions'`

- [ ] **Step 3: Implement infer_positions**

Add to `src/analytics/shape_graph.py`:

```python
# Vertical level labels (from back to front)
_VERTICAL_LEVELS: tuple[str, ...] = ("B", "DM", "M", "AM", "F")
# Horizontal level labels (from left to right)
_HORIZONTAL_LEVELS: tuple[str, ...] = ("L", "LC", "C", "RC", "R")


def _assign_levels(values: np.ndarray, levels: tuple[str, ...]) -> list[str]:
    """Assign level labels to 1D values using equal-frequency binning.

    Sorts values, divides into len(levels) bins, assigns labels.
    Ties go to the lower bin.
    """
    n = len(values)
    if n == 0:
        return []

    n_levels = len(levels)
    sorted_indices = np.argsort(values)
    assignments = [""] * n

    # Equal-frequency: each bin gets ceil(n / n_levels) or floor
    for rank, idx in enumerate(sorted_indices):
        bin_idx = min(rank * n_levels // n, n_levels - 1)
        assignments[idx] = levels[bin_idx]

    return assignments


def infer_positions(
    shape_graph: ShapeGraph,
    positions: np.ndarray,
    attacking_direction: float,
) -> list[PositionLabel]:
    """Infer tactical positions from the shape graph via level decomposition.

    Vertical decomposition uses the x-coordinate (attacking axis).
    Horizontal decomposition uses the y-coordinate (lateral axis).

    If attacking_direction < 0, the x-axis is flipped before level assignment.

    Args:
        shape_graph: Computed shape graph.
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

    # Assign vertical levels based on x (attacking axis)
    vertical = _assign_levels(x, _VERTICAL_LEVELS)
    # Assign horizontal levels based on y (lateral axis)
    horizontal = _assign_levels(y, _HORIZONTAL_LEVELS)

    return [
        PositionLabel(
            vertical=vertical[i],
            horizontal=horizontal[i],
            label=f"{vertical[i]}-{horizontal[i]}",
        )
        for i in range(n)
    ]
```

- [ ] **Step 4: Run test to verify it passes**

Run: `uv run pytest src/tests/test_shape_graph.py::TestInferPositions -v`
Expected: PASS (5 tests)

- [ ] **Step 5: Run full test suite**

Run: `uv run pytest src/tests/test_shape_graph.py -v`
Expected: PASS (12 tests)

---

### Task 4: Shape Graph Benchmarks and Lint

**Files:**
- Modify: `src/tests/test_shape_graph.py`

- [ ] **Step 1: Add pytest-benchmark tests**

Add to `src/tests/test_shape_graph.py`:

```python
class TestShapeGraphBenchmark:
    """Performance benchmarks for shape graph computation."""

    def test_bench_compute_shape_graph_10_players(self, benchmark) -> None:  # type: ignore[no-untyped-def]
        """Shape graph for 10 outfield players — target sub-millisecond."""
        positions = np.array([
            [20.0, 10.0], [20.0, 25.0], [20.0, 43.0], [20.0, 58.0],
            [40.0, 10.0], [40.0, 25.0], [40.0, 43.0], [40.0, 58.0],
            [60.0, 25.0], [60.0, 43.0],
        ])
        result = benchmark(compute_shape_graph, positions)
        assert len(result.edges) > 0

    def test_bench_infer_positions_10_players(self, benchmark) -> None:  # type: ignore[no-untyped-def]
        """Position inference for 10 outfield players."""
        positions = np.array([
            [20.0, 10.0], [20.0, 25.0], [20.0, 43.0], [20.0, 58.0],
            [40.0, 10.0], [40.0, 25.0], [40.0, 43.0], [40.0, 58.0],
            [60.0, 25.0], [60.0, 43.0],
        ])
        sg = compute_shape_graph(positions)
        result = benchmark(infer_positions, sg, positions, 1.0)
        assert len(result) == 10
```

- [ ] **Step 2: Run benchmarks**

Run: `uv run pytest src/tests/test_shape_graph.py::TestShapeGraphBenchmark -v --benchmark-only`
Expected: PASS with sub-millisecond times

- [ ] **Step 3: Run ruff and pyright**

Run: `uv run ruff check src/analytics/shape_graph.py src/tests/test_shape_graph.py && uv run ruff format --check src/analytics/shape_graph.py src/tests/test_shape_graph.py && uv run pyright src/analytics/shape_graph.py`
Expected: Clean (0 errors)

---

### Task 5: Formation Table Migration — dbt

**Files:**
- Modify: `dbt_project/models/staging/formations/_formations__sources.yml`
- Modify: `dbt_project/models/staging/formations/stg_formations__labels.sql`
- Modify: `dbt_project/models/marts/fct_formation_labels.sql`
- Modify: `dbt_project/models/marts/_marts__models.yml` (lines 2010-2077)

- [ ] **Step 1: Add `detector` column to source YAML**

In `_formations__sources.yml`, add after the `cost` column entry:

```yaml
          - name: detector
            description: "Detection algorithm identifier (efpi or shape_graph)"
```

- [ ] **Step 2: Add `detector` to staging model**

In `stg_formations__labels.sql`, add to the `cleaned` select list after `cast(cost as double) as cost,`:

```sql
        coalesce(cast(detector as string), 'efpi') as detector,
```

The `coalesce` handles backfill — existing rows without a `detector` column get `'efpi'`.

- [ ] **Step 3: Update mart model**

In `fct_formation_labels.sql`, add `detector` to the surrogate key and select:

```sql
-- Update the surrogate key to include detector
{{ dbt_utils.generate_surrogate_key([
    'formation_labels.match_id',
    'formation_labels.period',
    'formation_labels.team',
    'formation_labels.window_start_s',
    'formation_labels.detector'
]) }}                                       as formation_label_id,

-- Add detector to the select
formation_labels.detector,
```

Update the comment at the top to mention both EFPI and shape graph detectors.

- [ ] **Step 4: Update contract in `_marts__models.yml`**

Add to the `fct_formation_labels` model contract (after the `cost` column, before `_ingested_at`):

```yaml
      - name: detector
        data_type: string
        description: "Detection algorithm identifier: efpi (Bekkers & Dabadghao 2025) or shape_graph (Sotudeh 2026)"
        data_tests:
          - not_null
          - accepted_values:
              arguments:
                values: ['efpi', 'shape_graph']
```

Update the model description to mention both detectors.

- [ ] **Step 5: Validate dbt parse**

Run: `cd dbt_project && uv run dbt parse --profiles-dir .`
Expected: Clean parse, no errors

---

### Task 6: Shape Graph Pipeline Integration

**Files:**
- Modify: `src/ingestion/formations.py`

- [ ] **Step 1: Add shape graph imports and constants**

At the top of `formations.py` (after existing imports), add:

```python
_POSITIONS_TABLE_NAME = "player_positions"

_POSITIONS_COLUMNS = [
    "match_id", "frame_id", "player_id", "team",
    "position_label", "vertical_level", "horizontal_level",
    "detector",
]
```

- [ ] **Step 2: Update `_RESULT_COLUMNS` to include detector**

```python
_RESULT_COLUMNS = [
    "match_id", "period", "team",
    "window_start_s", "window_end_s",
    "formation_label", "cost",
    "detector",
]
```

- [ ] **Step 3: Create a shape graph UDF factory**

Add a `_make_shape_graph_udf` function (parallel to `_make_formation_udf`) that:
1. Imports `shape_graph` module lazily inside the closure
2. For each (match_id, period, team) group:
   - Filters to outfield players (`is_goalkeeper == False`)
   - Computes shape graph per time window (same 300s windows as EFPI)
   - Computes per-player mean positions in window
   - Calls `compute_shape_graph` + `infer_positions`
   - Derives formation label from position counts per vertical level
   - Returns both window-level formation rows (with `detector='shape_graph'`) and frame-level position rows

- [ ] **Step 4: Update `_process_matches` to run both detectors**

After the existing EFPI `applyInPandas` call, add a second `applyInPandas` call for shape graphs. Both write to `fct_formation_labels` (with different `detector` values). Shape graph additionally writes frame-level results to `player_positions`.

- [ ] **Step 5: Update existing EFPI output to include `detector='efpi'`**

In the existing `_make_formation_udf`, add `detector='efpi'` to each output row.

- [ ] **Step 6: Update the write schema**

Add `detector` (StringType, not nullable) to the StructType schema for formation labels. Add a new schema for position results.

- [ ] **Step 7: Run existing tests**

Run: `uv run pytest src/tests/test_formation_detection.py src/tests/test_shape_graph.py -v`
Expected: PASS

---

### Task 7: D37 — Position Maps dbt Models

**Files:**
- Create: `dbt_project/models/staging/shape_graphs/_shape_graphs__sources.yml`
- Create: `dbt_project/models/staging/shape_graphs/stg_shape_graphs__positions.sql`
- Create: `dbt_project/models/marts/fct_player_positions.sql`
- Create: `dbt_project/models/marts/fct_position_maps.sql`
- Modify: `dbt_project/models/marts/_marts__models.yml`

- [ ] **Step 1: Create source YAML**

```yaml
# dbt_project/models/staging/shape_graphs/_shape_graphs__sources.yml
version: 2

sources:
  - name: shape_graphs
    schema: bronze
    database: "{{ var('catalog', 'soccer_analytics') }}"
    description: "Frame-level player position labels from shape graph algorithm (Sotudeh 2026)"
    tables:
      - name: player_positions
        description: >
          Per-frame, per-player tactical position labels inferred by the shape graph
          algorithm's 5×5 level decomposition. Written by the formations ingestion pipeline.
        freshness:
          warn_after: {count: 48, period: hour}
          error_after: {count: 168, period: hour}
        loaded_at_field: _ingested_at
        columns:
          - name: match_id
            description: "Match identifier"
          - name: frame_id
            description: "Frame identifier within the match"
          - name: player_id
            description: "Player identifier"
          - name: team
            description: "Team side (home or away)"
          - name: position_label
            description: "5×5 tactical position label (e.g., B-L, M-C, F-RC)"
          - name: vertical_level
            description: "Vertical level: B, DM, M, AM, or F"
          - name: horizontal_level
            description: "Horizontal level: L, LC, C, RC, or R"
          - name: detector
            description: "Always 'shape_graph' for this table"
          - name: _ingested_at
            description: "UTC timestamp when the row was written"
```

- [ ] **Step 2: Create staging model**

```sql
-- dbt_project/models/staging/shape_graphs/stg_shape_graphs__positions.sql
-- Deduplicate frame-level position labels from the shape graph algorithm.
--
-- Dedup: ROW_NUMBER partitioned by (match_id, frame_id, player_id),
-- latest _ingested_at wins.

with source as (

    select * from {{ source('shape_graphs', 'player_positions') }}

),

deduplicated as (

    select
        *,
        row_number() over (
            partition by match_id, frame_id, player_id
            order by _ingested_at desc
        ) as _row_num
    from source

),

cleaned as (

    select
        cast(match_id as string)          as match_id,
        cast(frame_id as bigint)          as frame_id,
        cast(player_id as string)         as player_id,
        cast(team as string)              as team,
        cast(position_label as string)    as position_label,
        cast(vertical_level as string)    as vertical_level,
        cast(horizontal_level as string)  as horizontal_level,
        cast(detector as string)          as detector,
        cast(_ingested_at as timestamp)   as _ingested_at

    from deduplicated
    where _row_num = 1

)

select * from cleaned
```

- [ ] **Step 3: Create `fct_player_positions` mart**

```sql
-- dbt_project/models/marts/fct_player_positions.sql
-- Frame-level tactical position labels from the shape graph algorithm.
--
-- Grain: one row per (match_id, frame_id, player_id).
-- Source: Sotudeh (2026), ETH Zurich DISS. 31732.

{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='position_id',
    on_schema_change='fail',
    liquid_clustered_by=['match_id']
) }}

with

{% if is_incremental() %}
existing_matches as (
    select distinct match_id from {{ this }}
),
{% endif %}

positions as (

    select * from {{ ref('stg_shape_graphs__positions') }}
    {% if is_incremental() %}
    where match_id not in (select match_id from existing_matches)
    {% endif %}

),

final as (

    select
        {{ dbt_utils.generate_surrogate_key([
            'positions.match_id',
            'positions.frame_id',
            'positions.player_id'
        ]) }}                                as position_id,

        positions.match_id,
        positions.frame_id,
        positions.player_id,
        positions.team,
        positions.position_label,
        positions.vertical_level,
        positions.horizontal_level,
        positions.detector,
        positions._ingested_at

    from positions

)

select * from final
```

- [ ] **Step 4: Create `fct_position_maps` mart**

```sql
-- dbt_project/models/marts/fct_position_maps.sql
-- Aggregated 5×5 time-in-position maps per player per match.
--
-- Three phase variants: all, in_possession, out_of_possession.
-- Grain: one row per (player_id, match_id, position_label, phase).
-- Source: Sotudeh (2026), §4.4 Position Maps.

{{ config(
    materialized='table',
    liquid_clustered_by=['match_id', 'player_id']
) }}

-- Phase 1: "all" — count frames per position regardless of possession
with position_counts as (

    select
        player_id,
        match_id,
        team,
        position_label,
        vertical_level,
        horizontal_level,
        count(*) as frame_count
    from {{ ref('fct_player_positions') }}
    group by player_id, match_id, team, position_label, vertical_level, horizontal_level

),

player_totals as (

    select
        player_id,
        match_id,
        sum(frame_count) as total_frames
    from position_counts
    group by player_id, match_id

),

final as (

    select
        {{ dbt_utils.generate_surrogate_key([
            'pc.player_id',
            'pc.match_id',
            'pc.position_label',
            "'all'"
        ]) }}                                           as position_map_id,
        pc.player_id,
        pc.match_id,
        pc.team,
        pc.position_label,
        pc.vertical_level,
        pc.horizontal_level,
        round(100.0 * pc.frame_count / pt.total_frames, 2)  as pct_time,
        'all'                                           as phase

    from position_counts pc
    inner join player_totals pt
        on pc.player_id = pt.player_id
        and pc.match_id = pt.match_id

)

select * from final
```

Note: The `in_possession` and `out_of_possession` phases require a possession state column in `fct_player_positions` that comes from tracking data. The initial implementation computes only the `all` phase. Possession-phase variants will be added when the possession state column is available in the tracking frames pipeline.

- [ ] **Step 5: Add contracts to `_marts__models.yml`**

Add after the `fct_formation_labels` contract block:

```yaml
  - name: fct_player_positions
    config:
      contract:
        enforced: true
      meta:
        data_sensitivity: public
        contains_pii: false
    description: >
      Frame-level tactical position labels from the shape graph algorithm
      (Sotudeh 2026). Each row assigns one player to one of 25 tactical
      positions (5 vertical × 5 horizontal levels) for a single tracking frame.
    columns:
      - name: position_id
        data_type: string
        description: Surrogate key (match_id + frame_id + player_id)
        data_tests:
          - unique
          - not_null
      - name: match_id
        data_type: string
        description: Match identifier
        data_tests:
          - not_null
      - name: frame_id
        data_type: bigint
        description: Tracking frame identifier
        data_tests:
          - not_null
      - name: player_id
        data_type: string
        description: Player identifier
        data_tests:
          - not_null
      - name: team
        data_type: string
        description: Team side (home or away)
        data_tests:
          - not_null
          - accepted_values:
              arguments:
                values: ['home', 'away']
      - name: position_label
        data_type: string
        description: "5×5 tactical position label (e.g., B-L, M-C, F-RC)"
        data_tests:
          - not_null
      - name: vertical_level
        data_type: string
        description: "Vertical level: B, DM, M, AM, or F"
        data_tests:
          - not_null
          - accepted_values:
              arguments:
                values: ['B', 'DM', 'M', 'AM', 'F']
      - name: horizontal_level
        data_type: string
        description: "Horizontal level: L, LC, C, RC, or R"
        data_tests:
          - not_null
          - accepted_values:
              arguments:
                values: ['L', 'LC', 'C', 'RC', 'R']
      - name: detector
        data_type: string
        description: "Always 'shape_graph'"
        data_tests:
          - not_null
      - name: _ingested_at
        data_type: timestamp
        description: UTC timestamp when the row was written

  - name: fct_position_maps
    config:
      contract:
        enforced: true
      meta:
        data_sensitivity: public
        contains_pii: false
    description: >
      Aggregated 5×5 time-in-position maps per player per match (Sotudeh 2026,
      §4.4). Shows the percentage of in-play time each player spent at each of
      the 25 tactical positions. Grain: one row per player-match-position-phase.
    columns:
      - name: position_map_id
        data_type: string
        description: Surrogate key (player_id + match_id + position_label + phase)
        data_tests:
          - unique
          - not_null
      - name: player_id
        data_type: string
        description: Player identifier
        data_tests:
          - not_null
      - name: match_id
        data_type: string
        description: Match identifier
        data_tests:
          - not_null
      - name: team
        data_type: string
        description: Team side (home or away)
        data_tests:
          - not_null
      - name: position_label
        data_type: string
        description: "5×5 tactical position label"
        data_tests:
          - not_null
      - name: vertical_level
        data_type: string
        description: "Vertical level: B, DM, M, AM, or F"
        data_tests:
          - not_null
      - name: horizontal_level
        data_type: string
        description: "Horizontal level: L, LC, C, RC, or R"
        data_tests:
          - not_null
      - name: pct_time
        data_type: double
        description: "Percentage of in-play time at this position (0-100)"
        data_tests:
          - not_null
      - name: phase
        data_type: string
        description: "Game phase: all, in_possession, or out_of_possession"
        data_tests:
          - not_null
          - accepted_values:
              arguments:
                values: ['all', 'in_possession', 'out_of_possession']
```

- [ ] **Step 6: Validate dbt parse**

Run: `cd dbt_project && uv run dbt parse --profiles-dir .`
Expected: Clean parse

---

### Task 8: Track B Peripherals — Workflow Cards, Synced Tables, Glossary

**Files:**
- Create: `workflow-cards/wf-shape-graphs.yaml`
- Modify: `workflow-cards/wf-formations.yaml`
- Modify: `scripts/refresh_synced_tables.py` (lines 37-62)
- Modify: `hf_taipy_app/src/template.py`

- [ ] **Step 1: Create shape graphs workflow card**

```yaml
# workflow-cards/wf-shape-graphs.yaml
---
name: Shape Graph Formation Detection
id: wf-shape-graphs
version: "1.0"
status: production
type: heuristic
domain: formation-detection
owners:
  - karsten
tags:
  - formation
  - team-shape
  - tracking
  - shape-graph
  - position-inference

references:
  - citation: "Sotudeh, H. (2026). Identification of Team Tactical Formations and Player Positions in Association Football. PhD thesis, ETH Zurich (DISS. ETH NO. 31732)."
    role: methodology
  - citation: "Sotudeh et al. (2025). Shape Graphs for Formation Detection. npj Complexity. DOI: 10.1038/s44260-025-00047-x."
    role: algorithm

inputs:
  datasets:
    - id: "{catalog}.gold.fct_tracking_frames"
      source: delta-table
      description: "Tracking frame data with player positions and is_goalkeeper metadata for all 20 tracked matches"

outputs:
  tables:
    - id: "{catalog}.gold.fct_formation_labels"
      destination: delta-table
      synced: fct_formation_labels_synced
      description: "Window-level formation labels with detector='shape_graph'"
    - id: "{catalog}.gold.fct_player_positions"
      destination: delta-table
      synced: fct_player_positions_synced
      description: "Frame-level 5x5 tactical position labels"
    - id: "{catalog}.gold.fct_position_maps"
      destination: delta-table
      synced: fct_position_maps_synced
      description: "Aggregated time-in-position percentages"

execution:
  inference:
    trigger: scheduled
    runtime: databricks-workflow
    entry_point: compute_formations
    module: ingestion.formations
    distribution: applyInPandas
    partition_key: match_id
    schedule: "daily 06:00 UTC"
    timeout: "600s"
    environment: analytics

depends_on:
  - wf-pitch-control

idempotency:
  strategy: skip-guard
  key: match_id
  description: "Checks existing formation labels by match_id before processing."

performance:
  inference_timeout: "600s"
  memory_ceiling: "16 GB driver, 1 GB UDF executor"

cost:
  inference:
    runtime: databricks
    sku: "jobs_serverless_compute_run_dbus"
    typical_dbu: 5
    typical_cost_usd: 0.35

monitoring:
  freshness_sla_hours: 168

links:
  source_code:
    - "src/ingestion/formations.py"
    - "src/analytics/shape_graph.py"
---

## Overview

Shape Graph Formation Detection classifies team formations and infers per-player
tactical positions from tracking data using Delaunay-based shape graphs (Sotudeh 2026).
Unlike EFPI's top-down template matching, shape graphs are bottom-up: positions emerge
from the geometric structure of the Delaunay triangulation after unstable edges are
iteratively removed. No formation template library is needed.

## Algorithm

1. Compute Delaunay triangulation of outfield player (x, y) positions
2. Calculate angular stability for each edge (angle between circumcenters)
3. Iteratively remove least stable edge (threshold < 45°), merging incident faces
4. Repeat until all remaining edges are stable
5. Infer positions via 5×5 level decomposition (vertical: B/DM/M/AM/F, horizontal: L/LC/C/RC/R)

## Position Maps

Per-player time-in-position maps (§4.4) aggregate frame-level assignments into
a 5×5 matrix showing percentage of in-play time at each tactical position.
Three phase variants: all, in_possession, out_of_possession.
```

- [ ] **Step 2: Update wf-formations.yaml**

Update the overview section to mention both detectors. Add shape graph references. Update output description.

- [ ] **Step 3: Add synced tables to refresh script**

In `scripts/refresh_synced_tables.py`, add to the `SYNCED_TABLES` list:

```python
    "fct_player_positions_synced",
    "fct_position_maps_synced",
```

- [ ] **Step 4: Add glossary term to template.py**

In `hf_taipy_app/src/template.py`, add to `GLOSSARY` dict:

```python
    "Shape Graph": "Delaunay-based geometric formation detection — builds a stable proximity graph from player positions without formation templates (Sotudeh 2026).",
```

And add `"Shape Graph"` to the `PAGE_TERMS["Team-Shape"]` list.

- [ ] **Step 5: Run ruff on all modified files**

Run: `uv run ruff check src/ingestion/formations.py src/analytics/shape_graph.py scripts/refresh_synced_tables.py && uv run ruff format --check src/ingestion/formations.py src/analytics/shape_graph.py scripts/refresh_synced_tables.py`
Expected: Clean

---

## Track A — Transformer Embeddings (D18 + D30)

### Task 9: Transformer Model Architecture

**Files:**
- Create: `src/analytics/football2vec_transformer.py`
- Create: `src/tests/test_football2vec_transformer.py`

- [ ] **Step 1: Write the failing test for model construction**

```python
# src/tests/test_football2vec_transformer.py
"""Tests for Football2vec v2 transformer encoder."""
from __future__ import annotations

import pytest

torch = pytest.importorskip("torch")

from analytics.football2vec_transformer import (
    Football2VecConfig,
    Football2VecEncoder,
    GradientReversalLayer,
    TeamClassifierHead,
)


class TestFootball2VecConfig:
    """Test configuration dataclass."""

    def test_default_config(self) -> None:
        cfg = Football2VecConfig()
        assert cfg.vocab_size == 23
        assert cfg.hidden_dim == 128
        assert cfg.num_layers == 4
        assert cfg.num_heads == 4
        assert cfg.mask_prob == 0.15

    def test_custom_config(self) -> None:
        cfg = Football2VecConfig(hidden_dim=256, num_layers=6)
        assert cfg.hidden_dim == 256
        assert cfg.num_layers == 6


class TestFootball2VecEncoder:
    """Test transformer encoder construction and forward pass."""

    def test_forward_pass_shape(self) -> None:
        """Encoder output should be (batch, hidden_dim)."""
        cfg = Football2VecConfig()
        model = Football2VecEncoder(cfg)

        batch_size = 4
        seq_len = 50
        action_ids = torch.randint(0, cfg.vocab_size, (batch_size, seq_len))
        x_coords = torch.rand(batch_size, seq_len)  # Normalized [0, 1]
        y_coords = torch.rand(batch_size, seq_len)
        mask = torch.ones(batch_size, seq_len, dtype=torch.bool)

        embeddings = model(action_ids, x_coords, y_coords, mask)
        assert embeddings.shape == (batch_size, cfg.hidden_dim)

    def test_mlm_head_shape(self) -> None:
        """MLM head should output (batch, seq_len, vocab_size)."""
        cfg = Football2VecConfig()
        model = Football2VecEncoder(cfg)

        batch_size = 4
        seq_len = 50
        action_ids = torch.randint(0, cfg.vocab_size, (batch_size, seq_len))
        x_coords = torch.rand(batch_size, seq_len)
        y_coords = torch.rand(batch_size, seq_len)
        mask = torch.ones(batch_size, seq_len, dtype=torch.bool)

        logits = model.mlm_forward(action_ids, x_coords, y_coords, mask)
        assert logits.shape == (batch_size, seq_len, cfg.vocab_size)

    def test_spatial_encoding_contributes(self) -> None:
        """Different spatial positions should produce different embeddings."""
        cfg = Football2VecConfig()
        model = Football2VecEncoder(cfg)
        model.eval()

        action_ids = torch.tensor([[0, 1, 2]])
        mask = torch.ones(1, 3, dtype=torch.bool)

        # Same actions, different positions
        emb1 = model(action_ids, torch.tensor([[0.1, 0.2, 0.3]]), torch.tensor([[0.1, 0.2, 0.3]]), mask)
        emb2 = model(action_ids, torch.tensor([[0.9, 0.8, 0.7]]), torch.tensor([[0.9, 0.8, 0.7]]), mask)

        assert not torch.allclose(emb1, emb2, atol=1e-4)


class TestGradientReversalLayer:
    """Test gradient reversal layer."""

    def test_forward_identity(self) -> None:
        """Forward pass should be identity."""
        grl = GradientReversalLayer(lambda_val=0.2)
        x = torch.tensor([1.0, 2.0, 3.0], requires_grad=True)
        y = grl(x)
        assert torch.allclose(y, x)

    def test_backward_negated(self) -> None:
        """Backward pass should negate and scale gradients."""
        grl = GradientReversalLayer(lambda_val=1.0)
        x = torch.tensor([1.0, 2.0, 3.0], requires_grad=True)
        y = grl(x)
        loss = y.sum()
        loss.backward()
        # Gradient should be -1.0 * lambda * ones
        assert x.grad is not None
        assert torch.allclose(x.grad, torch.tensor([-1.0, -1.0, -1.0]))


class TestTeamClassifierHead:
    """Test adversarial team classifier."""

    def test_output_shape(self) -> None:
        """Should output (batch, num_teams) logits."""
        head = TeamClassifierHead(hidden_dim=128, num_teams=50, lambda_val=0.2)
        x = torch.randn(4, 128)
        logits = head(x)
        assert logits.shape == (4, 50)
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest src/tests/test_football2vec_transformer.py -v`
Expected: FAIL — `ModuleNotFoundError`

- [ ] **Step 3: Implement the transformer module**

```python
# src/analytics/football2vec_transformer.py
"""Football2vec v2 — Transformer encoder with spatial encoding.

Replaces Doc2Vec (gensim) with a small transformer trained on tokenized
SPADL action sequences. Training objective: masked action prediction (MLM).
Stage 2 adds adversarial team debiasing via gradient reversal (Ganin et al. 2016).

Architecture:
  - Token embedding: 23-type SPADL vocabulary → hidden_dim
  - Spatial encoding: MLP(x) + MLP(y) summed with token embedding
  - 4-layer transformer encoder, 4 attention heads
  - Output: mean pooling → hidden_dim player-match embedding

References:
  - Ganin et al. (2016). Domain-Adversarial Training of Neural Networks. JMLR.
  - Danesi (2025). The Imposter on the Pitch. HPI/Hudl.
"""
from __future__ import annotations

import math
from dataclasses import dataclass

import torch
import torch.nn as nn
from torch.autograd import Function


@dataclass(frozen=True)
class Football2VecConfig:
    """Model configuration."""

    vocab_size: int = 23       # 23-type SPADL vocabulary
    hidden_dim: int = 128      # Embedding and transformer hidden dimension
    num_layers: int = 4        # Transformer encoder layers
    num_heads: int = 4         # Attention heads
    dropout: float = 0.1       # Dropout rate
    max_seq_len: int = 512     # Maximum sequence length
    mask_prob: float = 0.15    # MLM mask probability
    spatial_mlp_dim: int = 64  # Intermediate dim for spatial MLPs


class _SpatialMLP(nn.Module):
    """Projects a scalar coordinate (x or y) to hidden_dim."""

    def __init__(self, hidden_dim: int, intermediate_dim: int) -> None:
        super().__init__()
        self.net = nn.Sequential(
            nn.Linear(1, intermediate_dim),
            nn.GELU(),
            nn.Linear(intermediate_dim, hidden_dim),
        )

    def forward(self, coords: torch.Tensor) -> torch.Tensor:
        """coords: (batch, seq_len) → (batch, seq_len, hidden_dim)."""
        return self.net(coords.unsqueeze(-1))


class _GradientReversalFunction(Function):
    """Autograd function for gradient reversal."""

    @staticmethod
    def forward(ctx, x: torch.Tensor, lambda_val: float) -> torch.Tensor:  # type: ignore[override]
        ctx.lambda_val = lambda_val
        return x.clone()

    @staticmethod
    def backward(ctx, grad_output: torch.Tensor) -> tuple[torch.Tensor, None]:  # type: ignore[override]
        return -ctx.lambda_val * grad_output, None


class GradientReversalLayer(nn.Module):
    """Identity forward, negated+scaled gradient backward."""

    def __init__(self, lambda_val: float = 0.2) -> None:
        super().__init__()
        self.lambda_val = lambda_val

    def forward(self, x: torch.Tensor) -> torch.Tensor:
        return _GradientReversalFunction.apply(x, self.lambda_val)


class TeamClassifierHead(nn.Module):
    """Adversarial team classifier with gradient reversal."""

    def __init__(self, hidden_dim: int, num_teams: int, lambda_val: float = 0.2) -> None:
        super().__init__()
        self.grl = GradientReversalLayer(lambda_val)
        self.classifier = nn.Linear(hidden_dim, num_teams)

    def forward(self, x: torch.Tensor) -> torch.Tensor:
        """x: (batch, hidden_dim) → (batch, num_teams) logits."""
        return self.classifier(self.grl(x))


class Football2VecEncoder(nn.Module):
    """Tiny transformer encoder for SPADL action sequences."""

    def __init__(self, config: Football2VecConfig) -> None:
        super().__init__()
        self.config = config

        # Token embedding
        self.token_embedding = nn.Embedding(config.vocab_size, config.hidden_dim)

        # Spatial encodings
        self.x_spatial = _SpatialMLP(config.hidden_dim, config.spatial_mlp_dim)
        self.y_spatial = _SpatialMLP(config.hidden_dim, config.spatial_mlp_dim)

        # Positional encoding (learnable)
        self.position_embedding = nn.Embedding(config.max_seq_len, config.hidden_dim)

        # Layer norm and dropout
        self.embed_norm = nn.LayerNorm(config.hidden_dim)
        self.embed_dropout = nn.Dropout(config.dropout)

        # Transformer encoder
        encoder_layer = nn.TransformerEncoderLayer(
            d_model=config.hidden_dim,
            nhead=config.num_heads,
            dim_feedforward=config.hidden_dim * 4,
            dropout=config.dropout,
            activation="gelu",
            batch_first=True,
        )
        self.encoder = nn.TransformerEncoder(encoder_layer, num_layers=config.num_layers)

        # MLM head
        self.mlm_head = nn.Sequential(
            nn.Linear(config.hidden_dim, config.hidden_dim),
            nn.GELU(),
            nn.LayerNorm(config.hidden_dim),
            nn.Linear(config.hidden_dim, config.vocab_size),
        )

        self._init_weights()

    def _init_weights(self) -> None:
        """Xavier uniform initialization."""
        for p in self.parameters():
            if p.dim() > 1:
                nn.init.xavier_uniform_(p)

    def _encode(
        self,
        action_ids: torch.Tensor,
        x_coords: torch.Tensor,
        y_coords: torch.Tensor,
        attention_mask: torch.Tensor,
    ) -> torch.Tensor:
        """Encode input to sequence of hidden states.

        Args:
            action_ids: (batch, seq_len) int64 action type indices.
            x_coords: (batch, seq_len) float normalized x in [0, 1].
            y_coords: (batch, seq_len) float normalized y in [0, 1].
            attention_mask: (batch, seq_len) bool — True for valid tokens.

        Returns:
            (batch, seq_len, hidden_dim) hidden states.
        """
        seq_len = action_ids.size(1)
        positions = torch.arange(seq_len, device=action_ids.device).unsqueeze(0)

        # Combine token, spatial, and positional embeddings
        h = (
            self.token_embedding(action_ids)
            + self.x_spatial(x_coords)
            + self.y_spatial(y_coords)
            + self.position_embedding(positions)
        )
        h = self.embed_dropout(self.embed_norm(h))

        # Transformer: src_key_padding_mask expects True for PADDED positions
        padding_mask = ~attention_mask
        h = self.encoder(h, src_key_padding_mask=padding_mask)
        return h

    def forward(
        self,
        action_ids: torch.Tensor,
        x_coords: torch.Tensor,
        y_coords: torch.Tensor,
        attention_mask: torch.Tensor,
    ) -> torch.Tensor:
        """Compute player-match embedding via mean pooling.

        Returns:
            (batch, hidden_dim) embedding vector.
        """
        h = self._encode(action_ids, x_coords, y_coords, attention_mask)

        # Mean pooling over valid tokens
        mask_expanded = attention_mask.unsqueeze(-1).float()
        summed = (h * mask_expanded).sum(dim=1)
        count = mask_expanded.sum(dim=1).clamp(min=1)
        return summed / count

    def mlm_forward(
        self,
        action_ids: torch.Tensor,
        x_coords: torch.Tensor,
        y_coords: torch.Tensor,
        attention_mask: torch.Tensor,
    ) -> torch.Tensor:
        """Forward pass for masked language modeling.

        Returns:
            (batch, seq_len, vocab_size) logits.
        """
        h = self._encode(action_ids, x_coords, y_coords, attention_mask)
        return self.mlm_head(h)
```

- [ ] **Step 4: Run test to verify it passes**

Run: `uv run pytest src/tests/test_football2vec_transformer.py -v`
Expected: PASS (8 tests)

- [ ] **Step 5: Run ruff and pyright**

Run: `uv run ruff check src/analytics/football2vec_transformer.py src/tests/test_football2vec_transformer.py && uv run pyright src/analytics/football2vec_transformer.py`
Expected: Clean

---

### Task 10: Training Data Export Pipeline

**Files:**
- Create: `src/ingestion/export_embeddings_training_data.py`
- Modify: `pyproject.toml` (lines 77-101)

- [ ] **Step 1: Implement training data export**

Create `src/ingestion/export_embeddings_training_data.py` — a Databricks pipeline task that:

1. Reads SPADL action sequences from `{catalog}.dev_gold.fct_action_values`
2. Groups by `(canonical_player_id, match_id)` to create per-player-match documents
3. Each document: ordered list of `(action_type, x_norm, y_norm, result)` tuples
4. Includes `position_group` from D28 for stratified evaluation
5. Writes Parquet to UC Volume: `/Volumes/{catalog}/dev_gold/training_data/football2vec_v2/`
6. Publishes to HF Hub as `luxury-lakehouse/football2vec-training-data`

Pattern: follows existing `publish_xg_shots_hf.py` / `publish_freeze_frame_hf.py` for HF upload.

Uses `@workflow("wf-football2vec-v2", phase="training")` decorator.

- [ ] **Step 2: Add entry point to pyproject.toml**

Add to `[project.scripts]`:

```toml
export_embeddings_training_data = "ingestion.export_embeddings_training_data:main"
```

- [ ] **Step 3: Validate entry point resolves**

Run: `uv run python -c "from ingestion.export_embeddings_training_data import main; print('OK')"`
Expected: `OK`

---

### Task 11: HF Jobs Training Script — Stage 1 (MLM)

**Files:**
- Create: `scripts/train_football2vec_v2.py`

- [ ] **Step 1: Create the PEP 723 training script**

Create `scripts/train_football2vec_v2.py` with:

```python
# /// script
# requires-python = ">=3.10"
# dependencies = [
#     "luxury-lakehouse @ https://huggingface.co/luxury-lakehouse/build-artifacts/resolve/main/luxury_lakehouse-0.1.0-py3-none-any.whl",
#     "numpy>=1.24",
#     "pandas>=2.0",
#     "pyarrow>=14.0",
#     "torch>=2.0",
#     "huggingface-hub>=1.5.0",
#     "mlflow>=2.17.0",
# ]
# ///
```

The script implements:

1. **CLI**: `--stage 1|2` flag, `--epochs`, `--batch-size`, `--lr`
2. **Data loading**: `HfApi` → download Parquet from `luxury-lakehouse/football2vec-training-data`
3. **Dataset**: Custom `torch.utils.data.Dataset` for SPADL sequences — returns `(action_ids, x_coords, y_coords, mask)` tensors, handles variable-length sequences via padding
4. **Train/val/test split**: stratified by `competition_id`
5. **Stage 1 training loop**:
   - MLM: randomly mask 15% of tokens, predict masked action type
   - Loss: `CrossEntropyLoss` on masked positions only
   - Optimizer: AdamW, lr=1e-4, cosine schedule with warmup
   - Early stopping on val loss (patience=5)
6. **Checkpoint**: save to HF Hub repo `luxury-lakehouse/football2vec-v2` tagged `stage1`
7. **Inference**: after training, run inference on all 87K documents → 128d embeddings
8. **Publish embeddings**: upload Parquet of (player_id, match_id, behavioral_vector) to HF Hub
9. **MLflow logging**: params, val_loss, val_accuracy per epoch, final model
10. **Cost recording**: `HFJobsCostRecorder` with `HF_RATE_A10G_LARGE`

Follow existing patterns from `scripts/train_xg_v2_hf.py` for:
- Device selection (`torch.device("cuda" if ...)`)
- MLflow guarded import (`if tracking_uri:`)
- HF Hub upload pattern (`api.create_repo`, `api.upload_file`)
- Cost sidecar (`recorder.complete(metrics_payload, row_count=...)`)
- Dataset commit hash logging for reproducibility

- [ ] **Step 2: Verify script parses**

Run: `uv run python -c "import ast; ast.parse(open('scripts/train_football2vec_v2.py').read()); print('OK')"`
Expected: `OK`

---

### Task 12: HF Jobs Training Script — Stage 2 (Adversarial)

**Files:**
- Modify: `scripts/train_football2vec_v2.py`

- [ ] **Step 1: Add Stage 2 to the training script**

Extend the `--stage 2` branch:

1. **Load Stage 1 checkpoint** from HF Hub (`luxury-lakehouse/football2vec-v2`, tag `stage1`)
2. **Add team classifier head**: `TeamClassifierHead(128, num_teams, lambda_val=0.2)`
3. **Lambda warmup**: linearly ramp `lambda_val` from 0.0 → 0.2 over first 5 epochs
4. **Combined loss**: `L_total = L_mlm - lambda * L_team_ce`
5. **Hard negative mining**: within each batch, ensure pairs from same `position_group` + same `team_id`
6. **Validation metrics**: track both MLM loss (should stay stable) and team accuracy (should decrease)
7. **Early stopping**: monitor combined loss, stop when team accuracy plateaus near chance level
8. **Checkpoint**: save to HF Hub tagged `stage2` (this becomes the released model)
9. **Re-run inference**: generate debiased 128d embeddings, overwrite the Stage 1 Parquet

- [ ] **Step 2: Verify script still parses**

Run: `uv run python -c "import ast; ast.parse(open('scripts/train_football2vec_v2.py').read()); print('OK')"`
Expected: `OK`

---

### Task 13: Embedding Pipeline Update for 128d

**Files:**
- Modify: `src/ingestion/player_embeddings.py`
- Modify: `dbt_project/models/marts/fct_player_embeddings_season.sql`
- Modify: `dbt_project/models/marts/fct_player_embeddings_career.sql`
- Modify: `dbt_project/models/staging/embeddings/_embeddings__sources.yml`

- [ ] **Step 1: Update dbt season model — dimension change**

In `fct_player_embeddings_season.sql`, change:
- Line 55: `sequence(0, 31)` → `sequence(0, 127)`

The element-wise mean logic is dimension-agnostic — only the index range changes.

- [ ] **Step 2: Update dbt career model — dimension change**

In `fct_player_embeddings_career.sql`, change:
- Line 33: `sequence(0, 31)` → `sequence(0, 127)`

- [ ] **Step 3: Update source YAML description**

In `_embeddings__sources.yml`, update the `behavioral_vector` column description from "32-dim Doc2Vec" to "128-dim transformer encoder (Football2vec v2)".

- [ ] **Step 4: Update player_embeddings.py import path**

Modify `src/ingestion/player_embeddings.py` to support importing pre-computed 128d embeddings from the HF dataset (published by the training script). The pipeline should:

1. Download the embeddings Parquet from `luxury-lakehouse/football2vec-statsbomb-wyscout` (v2 version)
2. Write to `player_embeddings_raw` in Delta with `replaceWhere` per data_source
3. Update docstring references from "32-dim" to "128-dim"

Note: The existing Doc2Vec training code path can remain as a fallback (gated by a flag or model version check) but the default path reads pre-trained embeddings from HF Hub.

- [ ] **Step 5: Validate dbt parse**

Run: `cd dbt_project && uv run dbt parse --profiles-dir .`
Expected: Clean

---

### Task 14: pgvector and Taipy Updates

**Files:**
- Modify: `scripts/create_indexes.py` (lines 149-174)
- Modify: `hf_taipy_app/src/state/player_similarity.py` (lines 141-145)

- [ ] **Step 1: Update HNSW index dimensions**

In `scripts/create_indexes.py`, update all 4 HNSW indexes:

```python
# Line 154: vector(32) → vector(128)
"USING hnsw ((behavioral_vector::text::vector(128)) vector_cosine_ops)",
# Line 160: vector(13) stays unchanged (stat vector)
"USING hnsw ((stat_vector::text::vector(13)) vector_cosine_ops)",
# Line 166: vector(32) → vector(128)
"USING hnsw ((behavioral_vector::text::vector(128)) vector_cosine_ops)",
# Line 172: vector(13) stays unchanged
"USING hnsw ((stat_vector::text::vector(13)) vector_cosine_ops)",
```

- [ ] **Step 2: Update Taipy vector dimension constant**

In `hf_taipy_app/src/state/player_similarity.py`, change:

```python
# Line 144: return 32 → return 128
def _get_vector_dimension(search_mode: str) -> int:
    """Return the vector dimension based on search mode."""
    if search_mode == "Playing style":
        return 128
    return 13
```

Update the module docstring (line 6) from "32-d" to "128-d".

- [ ] **Step 3: Run ruff on modified files**

Run: `uv run ruff check scripts/create_indexes.py hf_taipy_app/src/state/player_similarity.py`
Expected: Clean

---

### Task 15: Track A Peripherals — Workflow Card, Entry Point

**Files:**
- Create: `workflow-cards/wf-football2vec-v2.yaml`
- Modify: `workflow-cards/wf-football2vec.yaml` (mark as superseded)

- [ ] **Step 1: Create new workflow card**

Create `workflow-cards/wf-football2vec-v2.yaml` following the pattern of `wf-football2vec.yaml` but updated for:
- Type: `training-and-inference`
- Training runtime: `hf-jobs`, flavor: `a10g-large`
- Two stages: MLM (Stage 1) + Adversarial (Stage 2)
- Architecture: transformer encoder (4 layers, 128d, 4 heads)
- Input: `luxury-lakehouse/football2vec-training-data` HF dataset
- Output: `luxury-lakehouse/football2vec-v2` HF model + Delta tables
- References: Ganin et al. (2016), Danesi (2025), Le & Mikolov (2014) as predecessor
- Cost: A10G large rate, estimated training time

- [ ] **Step 2: Update old workflow card**

In `wf-football2vec.yaml`, update status from `production` to `superseded` and add a note pointing to `wf-football2vec-v2`.

- [ ] **Step 3: Validate workflow cards**

Run: `uv run validate_workflow_cards`
Expected: All cards valid

---

### Task 16: Full Test Suite and Lint

**Files:** All modified files

- [ ] **Step 1: Run full test suite**

Run: `uv run pytest src/tests/ -v --timeout=120`
Expected: All existing tests pass + new shape graph and transformer tests pass

- [ ] **Step 2: Run ruff check on all source**

Run: `uv run ruff check src/ scripts/`
Expected: Clean

- [ ] **Step 3: Run ruff format check**

Run: `uv run ruff format --check src/ scripts/`
Expected: Clean

- [ ] **Step 4: Run pyright**

Run: `uv run pyright src/`
Expected: 0 errors on source files

- [ ] **Step 5: Validate dbt parse**

Run: `cd dbt_project && uv run dbt parse --profiles-dir .`
Expected: Clean

---

### Note: Terraform

The spec mentions Terraform tasks for training data export and shape graph compute. However, `compute_formations` is not in the Terraform workflow definition (it runs via manual entry point invocation). The training data export follows the same pattern — it's an HF Jobs script, not a Databricks workflow task. No Terraform changes needed for this cycle.

### Note: Existing Embedding Tests

After Track A is complete, check for existing tests in `src/tests/` that hardcode `32` as the behavioral vector dimension. Update any assertions or fixture data to expect `128`. Key files to check: `src/tests/test_player_embeddings.py` (if it exists), any test fixtures with mock embedding vectors.

---

## Execution Checklist

After all tasks are code-complete and tests pass locally:

1. **Databricks pipeline run** — execute `compute_formations` (both EFPI + shape graphs)
2. **dbt build** — full refresh of formation + embedding models
3. **dbt test** — all data tests pass
4. **Index recreation** — `scripts/create_indexes.py` (includes new HNSW dims)
5. **Synced table recreation** — user creates `fct_player_positions_synced`, `fct_position_maps_synced`, recreates `fct_formation_labels_synced` (schema changed), recreates `embeddings_season`, `embeddings_career` (dim changed)
6. **HF Jobs Stage 1** — `hf jobs uv run scripts/train_football2vec_v2.py --stage 1 --flavor a10g-large --timeout 120m --secrets HF_TOKEN=$HF_TOKEN --env MLFLOW_TRACKING_URI=$MLFLOW_TRACKING_URI --env DATABRICKS_HOST=$DATABRICKS_HOST --env DATABRICKS_TOKEN=$DATABRICKS_TOKEN`
7. **HF Jobs Stage 2** — same script with `--stage 2`
8. **Import embeddings** — run export/import pipeline to write 128d vectors to Delta
9. **dbt rebuild embeddings** — full refresh of embedding aggregation models
10. **E2E validation** — query position maps from Lakebase (sums to 100%), verify embedding similarity search works at 128d
11. **HF Hub publish** — model card, dataset updates, org card artifact list
12. **Commit** — pending user approval
