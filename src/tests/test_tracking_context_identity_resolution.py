"""Tests for tracking context identity resolution (Bug 2 fix).

Verifies that _resolve_enrichment_identity produces non-null team_id/player_id
matching the tracking frame format, and that _restore_native_identity restores
native IDs for dim table joins.
"""

from __future__ import annotations


def test_idsse_team_id_uses_native() -> None:
    """For IDSSE, team_id passed to enrichments must be team_id_native (DFL CLU string)."""
    import pandas as pd

    actions = pd.DataFrame(
        {
            "game_id": [1, 1],
            "action_id": [0, 1],
            "period_id": [1, 1],
            "time_seconds": [10.0, 25.0],
            "team_id": pd.array([pd.NA, pd.NA], dtype="Int64"),
            "player_id": pd.array([pd.NA, pd.NA], dtype="Int64"),
            "team_id_native": ["DFL-CLU-000005", "DFL-CLU-000008"],
            "player_id_native": ["DFL-OBJ-0001LJ", "DFL-OBJ-0002HE"],
            "type_id": [0, 1],
            "result_id": [1, 0],
            "bodypart_id": [0, 0],
            "start_x": [50.0, 30.0],
            "start_y": [34.0, 20.0],
            "end_x": [60.0, 40.0],
            "end_y": [34.0, 25.0],
        }
    )

    from ingestion.tracking_context import _resolve_enrichment_identity

    resolved = _resolve_enrichment_identity(actions.copy(), provider="idsse", match_id_native="test")
    assert resolved["team_id"].iloc[0] == "DFL-CLU-000005"
    assert resolved["team_id"].iloc[1] == "DFL-CLU-000008"
    assert resolved["player_id"].iloc[0] == "DFL-OBJ-0001LJ"
    assert resolved["player_id"].iloc[1] == "DFL-OBJ-0002HE"


def test_metrica_team_id_maps_to_home_away() -> None:
    """For Metrica, team_id must be 'Home'/'Away' (matching frames and home_team_id)."""
    import pandas as pd

    actions = pd.DataFrame(
        {
            "game_id": [1, 1],
            "action_id": [0, 1],
            "period_id": [1, 1],
            "time_seconds": [10.0, 25.0],
            "team_id": pd.array([pd.NA, pd.NA], dtype="Int64"),
            "player_id": pd.array([pd.NA, pd.NA], dtype="Int64"),
            "team_id_native": [
                "metrica_Sample_Game_1_home",
                "metrica_Sample_Game_1_away",
            ],
            "player_id_native": ["Player11", "Player25"],
            "type_id": [0, 1],
            "result_id": [1, 0],
            "bodypart_id": [0, 0],
            "start_x": [50.0, 30.0],
            "start_y": [34.0, 20.0],
            "end_x": [60.0, 40.0],
            "end_y": [34.0, 25.0],
        }
    )

    from ingestion.tracking_context import _resolve_enrichment_identity

    resolved = _resolve_enrichment_identity(actions.copy(), provider="metrica", match_id_native="Sample_Game_1")
    assert resolved["team_id"].iloc[0] == "Home"
    assert resolved["team_id"].iloc[1] == "Away"
    # player_id_native is "PlayerN" (kloppy format) — matches frames after Task 4
    assert resolved["player_id"].iloc[0] == "Player11"
    assert resolved["player_id"].iloc[1] == "Player25"


def test_skillcorner_identity_resolution() -> None:
    """SkillCorner: team_id/player_id set to stringified native IDs (matching frames)."""
    import pandas as pd

    actions = pd.DataFrame(
        {
            "team_id": pd.array([pd.NA, pd.NA], dtype="Int64"),
            "player_id": pd.array([pd.NA, pd.NA], dtype="Int64"),
            "team_id_native": ["31", "42"],
            "player_id_native": ["101", "202"],
        }
    )

    from ingestion.tracking_context import _resolve_enrichment_identity

    resolved = _resolve_enrichment_identity(actions.copy(), provider="skillcorner", match_id_native="1886347")

    # team_id must be the stringified native team ID (matching frame format)
    assert resolved["team_id"].iloc[0] == "31"
    assert resolved["team_id"].iloc[1] == "42"
    # player_id must be the stringified native player ID
    assert resolved["player_id"].iloc[0] == "101"
    assert resolved["player_id"].iloc[1] == "202"


def test_output_uses_native_ids() -> None:
    """Output team_id/player_id must be native IDs (for dim table joins via staging)."""
    import pandas as pd

    actions = pd.DataFrame(
        {
            "game_id": [1],
            "action_id": [0],
            "period_id": [1],
            "time_seconds": [10.0],
            "team_id": ["DFL-CLU-000005"],  # enrichment-resolved value
            "player_id": ["DFL-OBJ-0001LJ"],  # enrichment-resolved value
            "team_id_native": ["DFL-CLU-000005"],
            "player_id_native": ["DFL-OBJ-0001LJ"],
            "type_id": [0],
            "result_id": [1],
            "bodypart_id": [0],
            "start_x": [50.0],
            "start_y": [34.0],
            "end_x": [60.0],
            "end_y": [34.0],
        }
    )

    from ingestion.tracking_context import _restore_native_identity

    restored = _restore_native_identity(actions.copy())
    assert restored["team_id"].iloc[0] == "DFL-CLU-000005"
    assert restored["player_id"].iloc[0] == "DFL-OBJ-0001LJ"


def test_resolve_rejects_all_null_native() -> None:
    """If team_id_native is ALL null, resolution must raise (data quality gate)."""
    import pandas as pd
    import pytest

    actions = pd.DataFrame(
        {
            "team_id": pd.array([pd.NA], dtype="Int64"),
            "player_id": pd.array([pd.NA], dtype="Int64"),
            "team_id_native": pd.array([pd.NA], dtype="string"),
            "player_id_native": pd.array([pd.NA], dtype="string"),
        }
    )

    from ingestion.tracking_context import _resolve_enrichment_identity

    with pytest.raises(ValueError, match="team_id_native"):
        _resolve_enrichment_identity(actions, provider="idsse", match_id_native="test")


def test_mixed_null_team_native_resolves_non_null_only() -> None:
    """Fix C: batch with BOTH null and non-null rows resolves only the non-null ones.

    J03WN1/J03WOY have a single freekick_short with NULL team_id_native.
    When mixed with non-null rows, the non-null rows must be resolved
    while null rows retain NaN team_id/player_id.
    """
    import numpy as np
    import pandas as pd

    from ingestion.tracking_context import _resolve_enrichment_identity

    actions = pd.DataFrame(
        {
            "team_id": pd.array([pd.NA, pd.NA, pd.NA], dtype="object"),
            "player_id": pd.array([pd.NA, pd.NA, pd.NA], dtype="object"),
            "team_id_native": pd.array(["DFL-CLU-000005", pd.NA, "DFL-CLU-000008"], dtype="string"),
            "player_id_native": pd.array(["DFL-OBJ-0001LJ", pd.NA, "DFL-OBJ-0002HE"], dtype="string"),
        }
    )

    resolved = _resolve_enrichment_identity(actions, provider="idsse", match_id_native="J03WN1")

    # Non-null rows get resolved
    assert resolved["team_id"].iloc[0] == "DFL-CLU-000005"
    assert resolved["player_id"].iloc[0] == "DFL-OBJ-0001LJ"
    assert resolved["team_id"].iloc[2] == "DFL-CLU-000008"
    assert resolved["player_id"].iloc[2] == "DFL-OBJ-0002HE"

    # Null row: team_id and player_id must still be NA (not resolved)
    assert pd.isna(resolved["team_id"].iloc[1])
    assert pd.isna(resolved["player_id"].iloc[1])
    # Verify it's actually the original NA, not a string "nan" or similar
    assert (
        resolved["team_id"].iloc[1] is pd.NA
        or resolved["team_id"].iloc[1] is None
        or (isinstance(resolved["team_id"].iloc[1], float) and np.isnan(resolved["team_id"].iloc[1]))
    )
