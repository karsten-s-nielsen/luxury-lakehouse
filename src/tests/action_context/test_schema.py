from __future__ import annotations

import pandas as pd

from analytics.action_context.schema import (
    ACTION_CONTEXT_DDL,
    RESULT_COLUMNS,
    _ddl_string_columns,
    build_output,
)


def test_ddl_matches_result_columns() -> None:
    ddl_cols = [tok.strip().split()[0] for tok in ACTION_CONTEXT_DDL.split(",")]
    assert ddl_cols == RESULT_COLUMNS  # DDL includes _ingested_at, same order


def test_build_output_fills_missing_and_selects() -> None:
    raw = pd.DataFrame({"action_id": [1], "start_x": [50.0]})
    out = build_output(raw, match_id_native="M", data_source="wyscout")
    expected = [c for c in RESULT_COLUMNS if c != "_ingested_at"]
    assert list(out.columns) == expected
    assert out["match_id"].iloc[0] == "M"
    assert out["data_source"].iloc[0] == "wyscout"


def test_string_columns_are_object_none_when_absent() -> None:
    """Regression (ADR-033 §amend): an event-only match resolving NO defending GK leaves
    ``defending_gk_player_id_native`` (a STRING column) ABSENT. build_output must fill it as
    object/None, NOT ``np.nan`` (float64) — else ``spark.createDataFrame(schema=...)``'s Arrow
    serializer raises ``ArrowTypeError: Expected a string ... got float64`` (the statsbomb
    match-15978 failure). Mirrors the GradientSports id-coercion guard (ADR-034)."""
    raw = pd.DataFrame({"action_id": [1, 2], "start_x": [50.0, 60.0]})
    out = build_output(raw, match_id_native="M", data_source="statsbomb")
    for col in _ddl_string_columns(ACTION_CONTEXT_DDL):
        if col in ("match_id", "data_source"):
            continue  # set to concrete scalars above
        assert out[col].dtype == object, f"{col} must be object dtype (got {out[col].dtype})"
        assert out[col].isna().all() and all(v is None for v in out[col]), f"{col} all-absent must be None"


def test_string_columns_coerced_when_present_but_all_null() -> None:
    """A STRING column that IS present but entirely NULL (silly-kicks may emit float64) must be
    coerced back to object/None — the present-but-all-null half of the same Arrow gap."""
    raw = pd.DataFrame(
        {"action_id": [1, 2], "start_x": [50.0, 60.0], "defending_gk_player_id": [float("nan"), float("nan")]}
    )
    out = build_output(raw, match_id_native="M", data_source="statsbomb")
    assert out["defending_gk_player_id_native"].dtype == object
    assert all(v is None for v in out["defending_gk_player_id_native"])


def test_string_column_values_preserved() -> None:
    """Coercion must not damage real string values (mixed value/NULL -> value/None, object)."""
    raw = pd.DataFrame({"action_id": [1, 2], "start_x": [1.0, 2.0], "defending_gk_player_id": ["gk_7", float("nan")]})
    out = build_output(raw, match_id_native="M", data_source="idsse")
    col = out["defending_gk_player_id_native"]
    assert col.dtype == object
    assert col.iloc[0] == "gk_7"
    assert col.iloc[1] is None


def test_ingestion_reexports_match_domain() -> None:
    # The ingestion adapter still imports DDL + build_output from the domain (used
    # locally by the guard / _process_* paths), so they remain importable there.
    # RESULT_COLUMNS now lives canonically in the domain schema module only.
    from ingestion.action_context import _ACTION_CONTEXT_DDL, _build_output

    assert _ACTION_CONTEXT_DDL is ACTION_CONTEXT_DDL
    assert _build_output is build_output
