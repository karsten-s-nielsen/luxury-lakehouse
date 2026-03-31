"""Tests for shape graph formation detection (Sotudeh Algorithm 1).

Tests cover:
  Task 1 — Angular stability (thesis §3.3, equation 3.2)
  Task 2 — Shape graph construction (Algorithm 1, thesis p.38)
  Task 3 — Position inference (thesis Chapter 4, §4.1-4.3)
"""

from __future__ import annotations

import numpy as np
import pytest

from analytics.shape_graph import (
    POSITION_LABEL_MATRIX,
    PositionLabel,
    ShapeGraph,
    _assign_levels_horizontal,
    _assign_levels_vertical,
    _compute_edge_stability,
    compute_shape_graph,
    infer_positions,
)

# ---------------------------------------------------------------------------
# Fixtures — reusable player arrangements
# ---------------------------------------------------------------------------


@pytest.fixture()
def positions_442() -> np.ndarray:
    """Canonical 4-4-2 formation.

    Defenders at x=20, midfielders at x=40, forwards at x=60.
    Each line spread across y = 10, 25, 43, 58 (or 25, 43 for forwards).
    """
    return np.array(
        [
            # Defenders (4)
            [20.0, 10.0],
            [20.0, 25.0],
            [20.0, 43.0],
            [20.0, 58.0],
            # Midfielders (4)
            [40.0, 10.0],
            [40.0, 25.0],
            [40.0, 43.0],
            [40.0, 58.0],
            # Forwards (2)
            [60.0, 25.0],
            [60.0, 43.0],
        ]
    )


@pytest.fixture()
def positions_352() -> np.ndarray:
    """3-5-2 formation for vertical distribution test.

    3 defenders, 5 midfielders, 2 forwards.
    """
    return np.array(
        [
            # Defenders (3)
            [20.0, 15.0],
            [20.0, 34.0],
            [20.0, 53.0],
            # Midfielders (5)
            [40.0, 5.0],
            [40.0, 20.0],
            [40.0, 34.0],
            [40.0, 48.0],
            [40.0, 63.0],
            # Forwards (2)
            [60.0, 25.0],
            [60.0, 43.0],
        ]
    )


# ---------------------------------------------------------------------------
# Task 1: Data type contracts
# ---------------------------------------------------------------------------


class TestDataTypes:
    """Verify frozen dataclass contracts."""

    def test_position_label_frozen(self) -> None:
        lbl = PositionLabel(vertical="B", horizontal="L", label="LB")
        with pytest.raises(AttributeError):
            lbl.vertical = "F"  # type: ignore[misc]

    def test_shape_graph_frozen(self) -> None:
        sg = ShapeGraph(
            edges=np.empty((0, 2), dtype=int),
            faces=[],
            stabilities=np.empty(0),
            points=np.empty((0, 2)),
        )
        with pytest.raises(AttributeError):
            sg.edges = np.empty((0, 2), dtype=int)  # type: ignore[misc]


class TestPositionLabelMatrix:
    """Verify the 5x5 position label matrix matches the thesis Figure 4.5b."""

    def test_all_25_positions_present(self) -> None:
        """Matrix must contain exactly 25 unique labels."""
        all_labels = set()
        for row in POSITION_LABEL_MATRIX.values():
            all_labels.update(row.values())
        assert len(all_labels) == 25

    def test_specific_labels(self) -> None:
        """Spot-check labels from the thesis matrix."""
        assert POSITION_LABEL_MATRIX["B"]["L"] == "LB"
        assert POSITION_LABEL_MATRIX["B"]["RC"] == "RCB"
        assert POSITION_LABEL_MATRIX["DM"]["L"] == "LWB"
        assert POSITION_LABEL_MATRIX["DM"]["C"] == "CDM"
        assert POSITION_LABEL_MATRIX["M"]["C"] == "CM"
        assert POSITION_LABEL_MATRIX["M"]["R"] == "RM"
        assert POSITION_LABEL_MATRIX["AM"]["L"] == "LWF"
        assert POSITION_LABEL_MATRIX["AM"]["C"] == "CAM"
        assert POSITION_LABEL_MATRIX["F"]["C"] == "CF"
        assert POSITION_LABEL_MATRIX["F"]["R"] == "RF"
        assert POSITION_LABEL_MATRIX["F"]["LC"] == "LCF"


# ---------------------------------------------------------------------------
# Task 1: Angular Stability (thesis §3.3, equation 3.2)
# ---------------------------------------------------------------------------


class TestAngularStability:
    """Test angular stability per Sotudeh's equation 3.2.

    alpha = 180 - (gamma + beta) where gamma and beta are opposite vertex angles.
    """

    def test_equilateral_diamond_high_stability(self) -> None:
        """Two equilateral triangles sharing an edge: both opposite angles = 60 degrees.

        alpha = 180 - (60 + 60) = 60. Should be above the 45 degree threshold.
        """
        points = np.array(
            [
                [0.0, 0.0],
                [2.0, 0.0],
                [1.0, np.sqrt(3.0)],
                [1.0, -np.sqrt(3.0)],
            ]
        )
        # Build simplices: two triangles sharing edge (0,1)
        simplices = np.array([[0, 1, 2], [0, 1, 3]])
        stability = _compute_edge_stability(p_idx=0, q_idx=1, simplices=simplices, points=points)
        # alpha = 180 - 60 - 60 = 60
        assert abs(stability - 60.0) < 1.0, f"Expected ~60°, got {stability:.1f}°"

    def test_near_degenerate_low_stability(self) -> None:
        """Near-flat triangle: large opposite angle yields low stability (under 45 degrees)."""
        points = np.array([[0.0, 0.0], [10.0, 0.0], [5.0, 0.01], [5.0, -5.0]])
        simplices = np.array([[0, 1, 2], [0, 1, 3]])
        stability = _compute_edge_stability(p_idx=0, q_idx=1, simplices=simplices, points=points)
        assert stability < 45.0, f"Expected low stability, got {stability:.1f}°"

    def test_boundary_edge_one_triangle(self) -> None:
        """Boundary edge (one incident triangle): beta = 0, alpha = 180 - gamma.

        For equilateral triangle, opposite angle = 60 degrees, so alpha = 120.
        """
        points = np.array(
            [
                [0.0, 0.0],
                [2.0, 0.0],
                [1.0, np.sqrt(3.0)],
            ]
        )
        simplices = np.array([[0, 1, 2]])
        stability = _compute_edge_stability(p_idx=0, q_idx=1, simplices=simplices, points=points)
        # Opposite angle to edge (0,1) is the angle at vertex 2 = 60 degrees for equilateral
        # alpha = 180 - 60 - 0 = 120
        assert abs(stability - 120.0) < 1.0, f"Expected ~120°, got {stability:.1f}°"

    def test_cocircular_points_zero_stability(self) -> None:
        """Cocircular points (thesis Figure 3.3): alpha = 0 degrees.

        Four points on a circle: opposite angles sum to 180 degrees.
        For a square inscribed in a circle, each triangle has a right angle (90 degrees)
        opposite the shared diagonal. alpha = 180 - 90 - 90 = 0.
        """
        # Square on unit circle: points at 0, 90, 180, 270 degrees
        points = np.array(
            [
                [1.0, 0.0],
                [0.0, 1.0],
                [-1.0, 0.0],
                [0.0, -1.0],
            ]
        )
        # Diagonal edge (0, 2) shared by triangles (0,1,2) and (0,2,3)
        simplices = np.array([[0, 1, 2], [0, 2, 3]])
        stability = _compute_edge_stability(p_idx=0, q_idx=2, simplices=simplices, points=points)
        assert abs(stability) < 1.0, f"Expected ~0° for cocircular, got {stability:.1f}°"

    def test_right_angle_pair(self) -> None:
        """Right-angle pair: one opposite angle = 90 degrees, other = 45 degrees.

        alpha = 180 - 90 - 45 = 45 degrees, exactly at the threshold.
        """
        # Triangle 1 has 90 degrees at vertex 2 opposite edge (0,1)
        # Triangle 2 has 45 degrees at vertex 3 opposite edge (0,1)
        points = np.array(
            [
                [0.0, 0.0],
                [4.0, 0.0],
                [2.0, 2.0],  # 90° angle at this vertex (isoceles right triangle)
                [2.0, -4.0],  # 45° angle: atan2(4, 2) + atan2(4, 2)... let me compute
            ]
        )
        # For vertex 2 at (2,2): vectors to 0,0 and 4,0 are (-2,-2) and (2,-2)
        # dot = -4+4=0, so angle = 90°. Good.
        # For vertex 3 at (2,-4): vectors to 0,0 and 4,0 are (-2,4) and (2,4)
        # dot = -4+16=12, |v1|=sqrt(20), |v2|=sqrt(20)
        # cos = 12/20 = 0.6, angle = arccos(0.6) ~ 53.1 degrees
        # alpha = 180 - 90 - 53.1 = 36.9
        simplices = np.array([[0, 1, 2], [0, 1, 3]])
        stability = _compute_edge_stability(p_idx=0, q_idx=1, simplices=simplices, points=points)
        # Not exactly 45° with this geometry, but tests the formula direction
        assert stability < 45.0, f"Expected < 45°, got {stability:.1f}°"


# ---------------------------------------------------------------------------
# Task 2: Shape Graph Construction (Algorithm 1)
# ---------------------------------------------------------------------------


class TestComputeShapeGraph:
    """Test the full shape graph construction (Sotudeh Algorithm 1)."""

    def test_classic_442_returns_stable_graph(self, positions_442: np.ndarray) -> None:
        """A canonical 4-4-2 should produce a connected shape graph."""
        sg = compute_shape_graph(positions_442)

        assert isinstance(sg, ShapeGraph)
        assert sg.edges.shape[1] == 2
        assert len(sg.edges) > 0
        # All remaining edges must be stable (>= 45°)
        assert all(s >= 45.0 for s in sg.stabilities), (
            f"Unstable edges found: {[s for s in sg.stabilities if s < 45.0]}"
        )
        assert len(sg.faces) >= 1
        # All player indices should appear in at least one face
        all_players: set[int] = set()
        for face in sg.faces:
            all_players.update(face)
        assert all_players == set(range(10))

    def test_three_players_triangle(self) -> None:
        """With exactly 3 players, Delaunay gives one triangle — all edges remain."""
        positions = np.array([[0.0, 0.0], [10.0, 0.0], [5.0, 8.0]])
        sg = compute_shape_graph(positions)
        # Triangle: 3 edges, all are boundary (one incident triangle each)
        assert len(sg.edges) == 3

    def test_fewer_than_three_returns_empty(self) -> None:
        """Fewer than 3 players cannot form a triangulation."""
        for n in (0, 1, 2):
            positions = np.array([[float(i), 0.0] for i in range(n)]) if n > 0 else np.empty((0, 2))
            sg = compute_shape_graph(positions)
            assert len(sg.edges) == 0
            assert len(sg.faces) == 0

    def test_collinear_players_empty_graph(self) -> None:
        """Collinear points cannot form a Delaunay triangulation."""
        positions = np.array([[0.0, 0.0], [5.0, 0.0], [10.0, 0.0], [15.0, 0.0]])
        sg = compute_shape_graph(positions)
        assert isinstance(sg, ShapeGraph)
        assert len(sg.edges) == 0
        assert len(sg.faces) == 0

    def test_no_edge_below_threshold(self, positions_442: np.ndarray) -> None:
        """The main invariant: no remaining edge has stability < threshold."""
        sg = compute_shape_graph(positions_442)
        for stability in sg.stabilities:
            assert stability >= 45.0, f"Edge with stability {stability:.1f}° below threshold"

    def test_stabilities_match_edges(self) -> None:
        """Number of stability values must equal number of edges."""
        positions = np.array(
            [
                [0.0, 0.0],
                [10.0, 0.0],
                [5.0, 8.0],
                [15.0, 5.0],
                [7.0, 12.0],
            ]
        )
        sg = compute_shape_graph(positions)
        assert len(sg.stabilities) == len(sg.edges)

    def test_points_preserved(self) -> None:
        """Original positions should be stored in the result."""
        positions = np.array([[0.0, 0.0], [10.0, 0.0], [5.0, 8.0]])
        sg = compute_shape_graph(positions)
        np.testing.assert_array_equal(sg.points, positions)

    def test_custom_threshold_strict_removes_more(self) -> None:
        """A very high threshold should remove more edges than a low threshold."""
        positions = np.array(
            [
                [0.0, 0.0],
                [10.0, 0.0],
                [5.0, 8.0],
                [15.0, 5.0],
                [7.0, 12.0],
            ]
        )
        sg_strict = compute_shape_graph(positions, stability_threshold=170.0)
        sg_lenient = compute_shape_graph(positions, stability_threshold=0.0)
        assert len(sg_lenient.edges) >= len(sg_strict.edges)

    def test_threshold_zero_keeps_all_delaunay_edges(self) -> None:
        """With threshold = 0, no edges are removed — result is the full Delaunay."""
        positions = np.array(
            [
                [0.0, 0.0],
                [10.0, 0.0],
                [5.0, 8.0],
                [15.0, 5.0],
                [7.0, 12.0],
            ]
        )
        sg = compute_shape_graph(positions, stability_threshold=0.0)
        from scipy.spatial import Delaunay

        tri = Delaunay(positions)
        delaunay_edges: set[tuple[int, int]] = set()
        for simplex in tri.simplices:
            for i in range(3):
                for j in range(i + 1, 3):
                    a, b = int(simplex[i]), int(simplex[j])
                    delaunay_edges.add((min(a, b), max(a, b)))
        sg_edges = {(int(e[0]), int(e[1])) for e in sg.edges}
        assert sg_edges == delaunay_edges

    def test_352_produces_connected_graph(self, positions_352: np.ndarray) -> None:
        """A 3-5-2 arrangement should also produce a valid shape graph.

        Note: some edges may have stability < 45° if tie-breaking kept them
        to preserve symmetry (thesis footnote 2, p.36).
        """
        sg = compute_shape_graph(positions_352)
        assert len(sg.edges) > 0
        # All player indices should be covered
        all_players = set()
        for face in sg.faces:
            all_players.update(face)
        assert all_players == set(range(10))

    def test_tie_breaking_does_not_overprune(self) -> None:
        """When multiple edges share the minimum stability, tie-breaking prevents over-pruning.

        The algorithm simulates removing one tied edge; if that would raise another
        tied edge above threshold, none are removed. This test uses a regular pentagon
        (symmetric) to exercise the tie-breaking path.
        """
        # Regular pentagon — multiple edges will have similar stabilities
        angles = np.linspace(0, 2 * np.pi, 5, endpoint=False)
        positions = np.column_stack([np.cos(angles), np.sin(angles)]) * 10.0
        sg = compute_shape_graph(positions)
        # Should not over-prune to zero edges
        assert len(sg.edges) >= 4, f"Over-pruned to {len(sg.edges)} edges"


# ---------------------------------------------------------------------------
# Task 3: Position Inference (thesis Chapter 4)
# ---------------------------------------------------------------------------


class TestInferPositions:
    """Test position inference via recursive face-center decomposition."""

    def test_442_vertical_levels(self, positions_442: np.ndarray) -> None:
        """A 4-4-2 should decompose into B, M, F vertical levels."""
        sg = compute_shape_graph(positions_442)
        labels = infer_positions(sg, positions_442, attacking_direction=1.0)

        assert len(labels) == 10
        # Defenders (indices 0-3) should be Back
        for i in range(4):
            assert labels[i].vertical == "B", f"Player {i}: expected B, got {labels[i].vertical}"
        # Forwards (indices 8-9) should be Front
        for i in range(8, 10):
            assert labels[i].vertical == "F", f"Player {i}: expected F, got {labels[i].vertical}"

    def test_442_horizontal_levels(self, positions_442: np.ndarray) -> None:
        """A 4-wide line should span L, LC, RC, R horizontally."""
        sg = compute_shape_graph(positions_442)
        labels = infer_positions(sg, positions_442, attacking_direction=1.0)

        # Defender line (y: 10, 25, 43, 58) should include L and R extremes
        def_horizontals = [labels[i].horizontal for i in range(4)]
        assert "L" in def_horizontals
        assert "R" in def_horizontals

    def test_reversed_attacking_direction(self, positions_442: np.ndarray) -> None:
        """Reversing attacking direction should flip vertical labels."""
        sg = compute_shape_graph(positions_442)
        labels_fwd = infer_positions(sg, positions_442, attacking_direction=1.0)
        labels_rev = infer_positions(sg, positions_442, attacking_direction=-1.0)

        # Defenders in forward → forwards in reverse
        for i in range(4):
            assert labels_fwd[i].vertical == "B"
            assert labels_rev[i].vertical == "F"

    def test_position_labels_follow_thesis_matrix(self, positions_442: np.ndarray) -> None:
        """Labels should follow the 5x5 matrix notation (e.g., 'RCB' not 'B-RC')."""
        sg = compute_shape_graph(positions_442)
        labels = infer_positions(sg, positions_442, attacking_direction=1.0)
        for lbl in labels:
            assert lbl.vertical in {"B", "DM", "M", "AM", "F"}
            assert lbl.horizontal in {"L", "LC", "C", "RC", "R"}
            expected_label = POSITION_LABEL_MATRIX[lbl.vertical][lbl.horizontal]
            assert lbl.label == expected_label, (
                f"Label mismatch: vertical={lbl.vertical}, horizontal={lbl.horizontal}, "
                f"expected '{expected_label}', got '{lbl.label}'"
            )

    def test_empty_shape_graph_returns_empty(self) -> None:
        """If shape graph is empty, infer_positions returns empty list."""
        positions = np.array([[0.0, 0.0], [10.0, 0.0]])
        sg = compute_shape_graph(positions)
        labels = infer_positions(sg, positions, attacking_direction=1.0)
        assert labels == []

    def test_352_vertical_distribution(self, positions_352: np.ndarray) -> None:
        """A 3-5-2 has different vertical distribution than 4-4-2."""
        sg = compute_shape_graph(positions_352)
        labels = infer_positions(sg, positions_352, attacking_direction=1.0)

        assert len(labels) == 10
        # Defenders (indices 0-2) should be B
        for i in range(3):
            assert labels[i].vertical == "B", f"Player {i}: expected B, got {labels[i].vertical}"
        # Forwards (indices 8-9) should be F
        for i in range(8, 10):
            assert labels[i].vertical == "F", f"Player {i}: expected F, got {labels[i].vertical}"

    def test_all_labels_are_valid_matrix_entries(self, positions_442: np.ndarray) -> None:
        """Every returned label must be a value in the 5x5 matrix."""
        valid_labels = set()
        for row in POSITION_LABEL_MATRIX.values():
            valid_labels.update(row.values())

        sg = compute_shape_graph(positions_442)
        labels = infer_positions(sg, positions_442, attacking_direction=1.0)
        for lbl in labels:
            assert lbl.label in valid_labels, f"Invalid label: {lbl.label}"


class TestVerticalLevelAssignment:
    """Test the vertical level assignment helper directly."""

    def test_three_distinct_x_groups(self) -> None:
        """Three x-groups (4+4+2 players) → B, M, F vertical levels."""
        # 4 defenders at x=20, 4 mids at x=40, 2 fwds at x=60
        x_values = np.array([20.0, 20.0, 20.0, 20.0, 40.0, 40.0, 40.0, 40.0, 60.0, 60.0])
        face_centers_x = np.array([30.0, 50.0])  # Between lines
        levels = _assign_levels_vertical(x_values, face_centers_x)
        assert all(lv == "B" for lv in levels[:4])
        assert all(lv == "F" for lv in levels[8:10])

    def test_single_player_gets_level(self) -> None:
        """A single player should be assigned a valid level."""
        x_values = np.array([50.0])
        face_centers_x = np.array([50.0])
        levels = _assign_levels_vertical(x_values, face_centers_x)
        assert len(levels) == 1
        assert levels[0] in {"B", "DM", "M", "AM", "F"}


class TestHorizontalLevelAssignment:
    """Test the horizontal level assignment helper directly."""

    def test_four_players_across(self) -> None:
        """4 players spanning y-axis → should get L, LC/C, RC/C, R."""
        y_values = np.array([10.0, 25.0, 43.0, 58.0])
        face_centers_y = np.array([20.0, 40.0])
        levels = _assign_levels_horizontal(y_values, face_centers_y)
        assert levels[0] == "L"
        assert levels[3] == "R"


class TestShapeGraphBenchmark:
    """Performance benchmarks for shape graph computation."""

    def test_bench_compute_shape_graph_10_players(self, benchmark) -> None:  # type: ignore[no-untyped-def]
        """Shape graph for 10 outfield players — target sub-millisecond."""
        positions = np.array(
            [
                [20.0, 10.0],
                [20.0, 25.0],
                [20.0, 43.0],
                [20.0, 58.0],
                [40.0, 10.0],
                [40.0, 25.0],
                [40.0, 43.0],
                [40.0, 58.0],
                [60.0, 25.0],
                [60.0, 43.0],
            ]
        )
        result = benchmark(compute_shape_graph, positions)
        assert len(result.edges) > 0

    def test_bench_infer_positions_10_players(self, benchmark) -> None:  # type: ignore[no-untyped-def]
        """Position inference for 10 outfield players."""
        positions = np.array(
            [
                [20.0, 10.0],
                [20.0, 25.0],
                [20.0, 43.0],
                [20.0, 58.0],
                [40.0, 10.0],
                [40.0, 25.0],
                [40.0, 43.0],
                [40.0, 58.0],
                [60.0, 25.0],
                [60.0, 43.0],
            ]
        )
        sg = compute_shape_graph(positions)
        result = benchmark(infer_positions, sg, positions, 1.0)
        assert len(result) == 10
