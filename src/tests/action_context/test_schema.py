from __future__ import annotations

import pandas as pd

from analytics.action_context.schema import ACTION_CONTEXT_DDL, RESULT_COLUMNS, build_output


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


def test_ingestion_reexports_match_domain() -> None:
    # The ingestion adapter still imports DDL + build_output from the domain (used
    # locally by the guard / _process_* paths), so they remain importable there.
    # RESULT_COLUMNS now lives canonically in the domain schema module only.
    from ingestion.action_context import _ACTION_CONTEXT_DDL, _build_output

    assert _ACTION_CONTEXT_DDL is ACTION_CONTEXT_DDL
    assert _build_output is build_output
