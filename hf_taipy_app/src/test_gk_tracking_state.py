"""GK tracking state — pure-helper unit tests (Taipy state objects are not unit-testable)."""

import numpy as np
import pandas as pd
import pytest

# state.gk_tracking imports plotly at module scope; the core CI env (bare `uv run pytest`)
# has no plotly — the app suite runs with the taipy-app extras installed. Same pattern as
# the torch/gensim importorskip guards in src/tests/.
pytest.importorskip("plotly")

from state.gk_tracking import (
    GKT_SUB_VIEW_LOV,
    PRESET_COLUMN,
    _format_metric,
    _line_height_terciles,
    _preset_rank_frame,
)


def test_sub_views_and_presets():
    assert GKT_SUB_VIEW_LOV == ["Distribution Value", "Defensive Positioning", "Shot Stopping"]
    assert PRESET_COLUMN["Counter"] == "dist_xt_gk_counter_mean"
    assert len(PRESET_COLUMN) == 6


def test_format_metric_nan_is_em_dash():
    assert _format_metric(float("nan"), "{:.3f}") == "—"
    assert _format_metric(None, "{:.3f}") == "—"
    assert _format_metric(0.1234, "{:.3f}") == "0.123"


def test_preset_rank_frame_ranks_within_preset():
    df = pd.DataFrame(
        {
            "player_display_name": ["A", "B"],
            "dist_xt_gk_counter_mean": [0.02, 0.01],
            "dist_xt_gk_possession_mean": [0.01, 0.03],
        }
    )
    ranks = _preset_rank_frame(df, ["Counter", "Possession"])
    assert ranks.loc["A", "Counter"] == 1 and ranks.loc["A", "Possession"] == 2
    assert ranks.loc["B", "Counter"] == 2 and ranks.loc["B", "Possession"] == 1


def test_line_height_terciles_labels_carry_n():
    # deviations kept within the plausibility gate (<8 m) — the helper filters above it
    df = pd.DataFrame({"line_height_m": np.arange(9.0), "ghost_deviation_m": np.arange(9.0) * 0.5})
    cats, means = _line_height_terciles(df)
    assert len(cats) == 3 and all("n=3" in c for c in cats)
    assert means[0] < means[1] < means[2]


def test_line_height_terciles_filters_implausible_deviation():
    df = pd.DataFrame({"line_height_m": [5.0, 10.0, 15.0], "ghost_deviation_m": [2.0, 99.0, 3.0]})
    cats, _ = _line_height_terciles(df)
    total_n = sum(int(c.split("n=")[1]) for c in cats)
    assert total_n == 2  # the 99 m sentinel-ish row is excluded
