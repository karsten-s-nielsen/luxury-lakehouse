"""Tests for canonical-SPADL xG feature builder additions.

Covers the Task 0.3 additions to ``analytics.xg_model``:
1. ``set_cardinality`` numeric feature (freeze-frame player count) so the
   prediction MLP can disentangle player *count* from the sum-aggregated
   context *magnitude*.
2. ``spadl_shot_geometry`` — distance-to-goal + subtended shot angle in
   canonical SPADL 105x68 metres (goal at (105, 34), width 7.32 m).
"""

from __future__ import annotations

import pandas as pd

from analytics.xg_model import XGModelConfig, build_features, spadl_shot_geometry


def test_set_cardinality_is_a_feature_column() -> None:
    df = pd.DataFrame({"distance_to_goal": [10.0], "shot_angle": [0.5], "set_cardinality": [22], "is_goal": [0]})
    x, _ = build_features(df, XGModelConfig())
    assert "set_cardinality" in x.columns
    assert float(x.iloc[0]["set_cardinality"]) == 22.0


def test_spadl_shot_geometry_uses_105x34_goal_and_732_width() -> None:
    # penalty spot ~ (94, 34): distance 11m; subtended angle = 2*atan(3.66/11) ≈ 0.6424 rad
    # (VERIFIED numerically — the true value is ~0.64, NOT >0.9; do not "fix" a failing
    # test by corrupting geometry).
    dist, ang = spadl_shot_geometry(94.0, 34.0)
    assert abs(dist - 11.0) < 0.5
    assert 0.60 < ang < 0.70  # penalty-spot subtended angle ≈ 0.64 rad
    # acute corner shot (105, 0) -> angle ≈ 0 rad
    _, ang_corner = spadl_shot_geometry(105.0, 0.0)
    assert ang_corner < 0.05
    assert ang_corner < ang
