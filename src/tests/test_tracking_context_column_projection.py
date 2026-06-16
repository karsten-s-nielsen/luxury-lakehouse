"""Parity tests — projection constants ⊇ consumed constants.

Non-tautological: each consumed constant is used by its converter function
to filter trk_pdf at entry (runtime assertion). If someone adds a column
reference without updating consumed, the converter crashes at runtime.
The test then catches if projection didn't grow to match.
"""

from __future__ import annotations

# NOTE: the IDSSE projection-vs-consumed parity test was removed under
# delete-and-depend (ADR-031 T3 / Gate B): the lakehouse IDSSE tracking
# converter (`_bronze_idsse_to_sportec_input`) and its `_IDSSE_CONSUMED_COLS`
# constant are deleted — the IDSSE tracking path now calls the silly-kicks
# port `shape_tracking_to_native`, which owns its own input contract. The
# IDSSE projection is still exercised by `test_all_projections_include_match_id`.


def test_metrica_projection_covers_consumed() -> None:
    """_METRICA_TRACKING_SELECT_COLS ⊇ _METRICA_CONSUMED_COLS."""
    from ingestion.tracking_context import (
        _METRICA_CONSUMED_COLS,
        _METRICA_TRACKING_SELECT_COLS,
    )

    missing = _METRICA_CONSUMED_COLS - set(_METRICA_TRACKING_SELECT_COLS)
    assert not missing, (
        f"_METRICA_TRACKING_SELECT_COLS missing columns from _METRICA_CONSUMED_COLS: "
        f"{sorted(missing)}. Update the projection constant."
    )


def test_skillcorner_projection_plus_join_covers_consumed() -> None:
    """_SKILLCORNER_TRACKING_SELECT_COLS + join-added cols ⊇ _SKILLCORNER_CONSUMED_COLS.

    SkillCorner bronze tracking doesn't contain team/is_goalkeeper. These are
    added via Spark join with bronze.skillcorner_matches at compute time.
    The projection constant contains only bronze-native columns.
    """
    from ingestion.tracking_context import (
        _SKILLCORNER_CONSUMED_COLS,
        _SKILLCORNER_TRACKING_SELECT_COLS,
    )

    # Columns added by the Spark join with skillcorner_matches (see main() driver)
    join_added_cols = {"team", "is_goalkeeper"}

    missing = _SKILLCORNER_CONSUMED_COLS - set(_SKILLCORNER_TRACKING_SELECT_COLS) - join_added_cols
    assert not missing, (
        f"_SKILLCORNER_TRACKING_SELECT_COLS + join missing columns from "
        f"_SKILLCORNER_CONSUMED_COLS: {sorted(missing)}. Update the projection "
        f"constant or the matches join."
    )


def test_all_projections_include_match_id() -> None:
    """groupBy('match_id', ...) requires match_id in every provider's projection."""
    from ingestion.tracking_context import (
        _IDSSE_TRACKING_SELECT_COLS,
        _METRICA_TRACKING_SELECT_COLS,
        _SKILLCORNER_TRACKING_SELECT_COLS,
    )

    for name, cols in [
        ("IDSSE", _IDSSE_TRACKING_SELECT_COLS),
        ("Metrica", _METRICA_TRACKING_SELECT_COLS),
        ("SkillCorner", _SKILLCORNER_TRACKING_SELECT_COLS),
    ]:
        assert "match_id" in cols, (
            f"{name} projection missing 'match_id' — "
            f"groupBy('match_id', 'period', 'frame_batch_id') will fail with UNRESOLVED_COLUMN"
        )


def test_projection_constants_are_tuples() -> None:
    """Projection constants must be tuples (immutable), consumed must be frozensets."""
    from ingestion.tracking_context import (
        _IDSSE_TRACKING_SELECT_COLS,
        _METRICA_CONSUMED_COLS,
        _METRICA_TRACKING_SELECT_COLS,
        _SKILLCORNER_CONSUMED_COLS,
        _SKILLCORNER_TRACKING_SELECT_COLS,
    )

    assert isinstance(_IDSSE_TRACKING_SELECT_COLS, tuple)
    assert isinstance(_METRICA_TRACKING_SELECT_COLS, tuple)
    assert isinstance(_SKILLCORNER_TRACKING_SELECT_COLS, tuple)
    assert isinstance(_METRICA_CONSUMED_COLS, frozenset)
    assert isinstance(_SKILLCORNER_CONSUMED_COLS, frozenset)


def test_projection_is_not_wasteful() -> None:
    """Projection should not include columns not consumed by converter or _process_*."""
    from ingestion.tracking_context import (
        _METRICA_CONSUMED_COLS,
        _METRICA_TRACKING_SELECT_COLS,
        _SKILLCORNER_CONSUMED_COLS,
        _SKILLCORNER_TRACKING_SELECT_COLS,
    )

    # match_id is needed by groupBy(), not by the converters.
    groupby_extra = {"match_id"}

    for name, proj, consumed, process_extra in [
        ("Metrica", _METRICA_TRACKING_SELECT_COLS, _METRICA_CONSUMED_COLS, groupby_extra),
        (
            "SkillCorner",
            _SKILLCORNER_TRACKING_SELECT_COLS,
            # SkillCorner consumed cols include team + is_goalkeeper (join-added,
            # not in projection). Only check bronze-native consumed cols.
            _SKILLCORNER_CONSUMED_COLS - {"team", "is_goalkeeper"},
            groupby_extra,
        ),
    ]:
        extra = set(proj) - consumed - process_extra
        assert not extra, (
            f"{name} projection has unexplained columns: {sorted(extra)}. "
            f"Remove from projection, add to consumed, or add to process_extra."
        )
