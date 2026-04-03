"""Shape graph formation detection — Sotudeh (2026).

Public API re-exports. All existing import paths continue to work.
"""

from analytics.shape_graph_construction import (
    PositionLabel,
    ShapeGraph,
    _compute_edge_stability,
    compute_shape_graph,
)
from analytics.shape_graph_inference import (
    POSITION_LABEL_MATRIX,
    _assign_levels_horizontal,
    _assign_levels_vertical,
    infer_positions,
)

__all__ = [
    "POSITION_LABEL_MATRIX",
    "PositionLabel",
    "ShapeGraph",
    "_assign_levels_horizontal",
    "_assign_levels_vertical",
    "_compute_edge_stability",
    "compute_shape_graph",
    "infer_positions",
]
