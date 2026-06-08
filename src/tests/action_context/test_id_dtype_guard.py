"""ADR-019 dtype-contract guard: validate_id_dtypes fails loud on an actions/frames id-dtype
mismatch. Wired at the tracking work-unit entry (pipeline.enrich_batch) after identity resolve +
frame conversion, so a future id-dtype drift (the silent-miss class) raises instead of silently
mis-resolving carrier / possession / opponent. The GS Int64->native-str coercion in convert.py is
KEPT (its drop is unproven locally — the GS enrich fixture's absolute-clock time-base guard blocks
an end-to-end seam-coverage test), so this guard passes on the real pipeline; it is the additive
loud half of the 4.15.0 handshake.
"""

from __future__ import annotations

import pandas as pd
import pytest


def test_validate_id_dtypes_raises_on_mismatch() -> None:
    from silly_kicks.tracking import validate_id_dtypes

    actions = pd.DataFrame({"action_id": [1], "team_id": [366], "player_id": [11], "game_id": ["g"]})
    frames = pd.DataFrame(
        {
            "game_id": ["g"], "period_id": [1], "frame_id": [1],
            "team_id": ["366"], "player_id": ["11"],
            "x": [0.0], "y": [0.0], "is_ball": [False],
        }
    )  # fmt: skip
    with pytest.raises(ValueError, match="validate_id_dtypes"):
        validate_id_dtypes(actions, frames, home_team_id="366", on_mismatch="raise")


def test_validate_id_dtypes_passes_on_matched_string_ids() -> None:
    """The real-pipeline shape: both sides object/string (post identity-resolve + GS coercion)."""
    from silly_kicks.tracking import validate_id_dtypes

    actions = pd.DataFrame({"action_id": [1], "team_id": ["366"], "player_id": ["11"], "game_id": ["g"]})
    frames = pd.DataFrame(
        {
            "game_id": ["g"], "period_id": [1], "frame_id": [1],
            "team_id": ["366"], "player_id": ["11"],
            "x": [0.0], "y": [0.0], "is_ball": [False],
        }
    )  # fmt: skip
    diag = validate_id_dtypes(actions, frames, home_team_id="366", on_mismatch="raise")
    assert not diag.has_mismatch
