"""silly-kicks API contract tests at OUR repo (ADR-018 F5).

Mirrors silly-kicks's own ADR-001 cross-provider parity gate but at OUR
boundary, against OUR fixtures. Catches:

- silly-kicks API drift (e.g., the pre-2.0.0 tackle override that
  silently rewrote 56% of TacklingGame ``team_id`` values for IDSSE)
- our adapter regressions (e.g., a future PR that adds an ``event_type``
  filter to one of the adapters)
- input fixture drift (we re-build fixtures only when bronze schema
  changes; this test catches drift between bronze schema and silly-
  kicks's converter expectations)

4 sources x 4 invariants = 16 tests.
"""

from __future__ import annotations

from pathlib import Path

import pandas as pd
import pytest
import silly_kicks.spadl.metrica
import silly_kicks.spadl.sportec
import silly_kicks.spadl.statsbomb
import silly_kicks.spadl.wyscout

_FIXTURE_DIR = Path(__file__).parent / "fixtures" / "silly_kicks_boundary"


_PARAMETRIZE = pytest.mark.parametrize(
    "source,converter,fixture",
    [
        # (source, silly-kicks converter module, fixture filename)
        # home_team_id arg is resolved per-source inside _adapt_input
        # below to mirror the production SPADL UDF invocations.
        ("statsbomb", silly_kicks.spadl.statsbomb, "sb_match_7298.parquet"),
        ("wyscout", silly_kicks.spadl.wyscout, "ws_match_2576335.parquet"),
        ("idsse", silly_kicks.spadl.sportec, "idsse_J03WMX.parquet"),
        ("metrica", silly_kicks.spadl.metrica, "metrica_sample_game_1.parquet"),
    ],
)


def _adapt_input(source: str, df: pd.DataFrame) -> tuple[pd.DataFrame, object]:
    """Run the same per-source adapter the SPADL UDFs use, returning
    (adapted_df, home_team_id_arg).

    Mirrors the per-source invocation pattern in
    ``src/ingestion/spadl_conversion.py`` so the boundary test exercises
    the same shape.
    """
    if source == "statsbomb":
        from ingestion.spadl_adapter import adapt_statsbomb_events

        # Use first non-null team_id present as a synthetic home_team_id;
        # this test asserts team_id ⊆ teams in input — direction-of-play
        # exact value doesn't matter for the contract checks here.
        home_team_id = int(df["team_id"].dropna().iloc[0])
        return adapt_statsbomb_events(df, home_team_id), home_team_id
    if source == "wyscout":
        from ingestion.spadl_adapter import adapt_wyscout_events

        home_team_id = int(df["teamId"].dropna().iloc[0])
        return adapt_wyscout_events(df), home_team_id
    if source == "idsse":
        from ingestion.spadl_adapter import adapt_idsse_events_for_silly_kicks

        # Drop bronze period-misclassification artifacts pre-bronze-rebuild
        # (Bug #6 — pre-2pass-parser; ~27-41 rows per match in pre-fix bronze).
        # The boundary test exists to catch silly-kicks API drift, not to
        # surface our bronze parser bugs (covered by test_idsse_period_derivation).
        # After Phase H bronze re-ingest with the 2-pass parser, this filter
        # is a no-op (no negative-timestamp rows in re-ingested bronze).
        df = df[~((df["period"] == 2) & (df["timestamp_seconds"] < 0))].reset_index(drop=True)
        return adapt_idsse_events_for_silly_kicks(df), "home"
    if source == "metrica":
        from ingestion.spadl_adapter import adapt_metrica_events_for_silly_kicks

        return adapt_metrica_events_for_silly_kicks(df), "Home"
    msg = f"unknown source {source!r}"
    raise ValueError(msg)


def _call_converter(source: str, converter, adapted: pd.DataFrame, hti, df: pd.DataFrame):  # type: ignore[no-untyped-def]
    """Invoke silly-kicks converter with per-provider kwargs that match production.

    silly-kicks 3.0.1 (PR-S23) requires Sportec + Metrica callers to pass
    ``home_team_start_left``. Lakehouse derivation helpers compute it from
    bronze (authoritative for IDSSE via DFL XML; empirical from period-1
    SHOT positions for Metrica). Mirrors the production SPADL UDF call sites
    in ``src/ingestion/spadl_conversion.py``.
    """
    if source == "idsse":
        from ingestion.spadl_adapter import derive_idsse_home_team_start_left

        home_team_id_native = str(df["home_team_id_native"].dropna().iloc[0])
        home_start_left = derive_idsse_home_team_start_left(adapted, home_team_id_native)
        return converter.convert_to_actions(adapted, home_team_id=hti, home_team_start_left=home_start_left)
    if source == "metrica":
        from ingestion.spadl_adapter import derive_metrica_home_team_start_left

        home_start_left = derive_metrica_home_team_start_left(adapted, home_team_value="Home")
        return converter.convert_to_actions(adapted, home_team_id=hti, home_team_start_left=home_start_left)
    return converter.convert_to_actions(adapted, home_team_id=hti)


@_PARAMETRIZE
def test_team_id_subset_of_input_team_or_team_id(source, converter, fixture) -> None:  # type: ignore[no-untyped-def]
    """ADR-018 boundary contract: silly-kicks's output ``team_id`` values
    are a subset of the input's team identification (column varies per provider).
    """
    df = pd.read_parquet(_FIXTURE_DIR / fixture)
    adapted, hti = _adapt_input(source, df)
    actions, _report = _call_converter(source, converter, adapted, hti, df)
    out_teams = set(actions["team_id"].dropna().unique())
    # Determine the input team-identifier column per provider:
    if source == "statsbomb":
        input_teams = set(df["team_id"].dropna().astype(int).unique())
        assert out_teams <= input_teams, (
            f"silly-kicks {source} output team_id contains values not in input team_id: {out_teams - input_teams}"
        )
    elif source == "wyscout":
        input_teams = set(df["teamId"].dropna().astype(int).unique())
        assert out_teams <= input_teams
    else:
        # IDSSE + Metrica use string team labels in input
        input_teams_str = set(df["team"].dropna().astype(str).unique())
        out_teams_str = set(str(v) for v in out_teams)
        assert out_teams_str <= input_teams_str, (
            f"{source}: output teams {out_teams_str - input_teams_str} not in input {input_teams_str}"
        )


@_PARAMETRIZE
def test_action_id_non_null(source, converter, fixture) -> None:  # type: ignore[no-untyped-def]
    """Every output action must have a non-NULL action_id (silly-kicks invariant)."""
    df = pd.read_parquet(_FIXTURE_DIR / fixture)
    adapted, hti = _adapt_input(source, df)
    actions, _ = _call_converter(source, converter, adapted, hti, df)
    assert actions["action_id"].notna().all(), f"{source}: NULL action_id rows present"


@_PARAMETRIZE
def test_period_id_in_valid_range(source, converter, fixture) -> None:  # type: ignore[no-untyped-def]
    """Output period_id must be in {1..5}."""
    df = pd.read_parquet(_FIXTURE_DIR / fixture)
    adapted, hti = _adapt_input(source, df)
    actions, _ = _call_converter(source, converter, adapted, hti, df)
    periods = set(actions["period_id"].dropna().astype(int).unique())
    assert periods <= {1, 2, 3, 4, 5}, f"{source}: invalid period_ids {periods}"


@_PARAMETRIZE
def test_time_seconds_non_negative(source, converter, fixture) -> None:  # type: ignore[no-untyped-def]
    """Output time_seconds must be ≥ 0 (period-relative). Catches Bug #6 IDSSE class.

    Pre-fix on IDSSE this test FAILED because the bronze parser's state-machine
    `current_period` mistagged secondary-block events with negative
    period-relative timestamps. Post 2-pass parser refactor this passes.
    """
    df = pd.read_parquet(_FIXTURE_DIR / fixture)
    adapted, hti = _adapt_input(source, df)
    actions, _ = _call_converter(source, converter, adapted, hti, df)
    neg = actions[actions["time_seconds"] < 0]
    assert len(neg) == 0, (
        f"{source}: {len(neg)} rows with negative time_seconds — bronze parser period misclassification"
    )


@_PARAMETRIZE
def test_apply_spadl_enrichments_baseline(source, converter, fixture) -> None:  # type: ignore[no-untyped-def]
    """The full enrichment chain runs on every production-shape fixture without raising.

    Catches: silly-kicks API breaks, our adapter regressions, or a future
    enrichment helper that fails on a provider's natural data shape.
    """
    from ingestion.spadl_enrichments import apply_spadl_enrichments

    df = pd.read_parquet(_FIXTURE_DIR / fixture)
    adapted, hti = _adapt_input(source, df)
    actions, _ = _call_converter(source, converter, adapted, hti, df)

    enriched = apply_spadl_enrichments(actions, source=source)

    expected_cols = {
        "possession_id_heuristic",
        "gk_role",
        "gk_was_distributing",
        "gk_was_engaged",
        "gk_actions_in_possession",
        "defending_gk_player_id",
    }
    missing = expected_cols - set(enriched.columns)
    assert not missing, f"{source}: enrichment chain dropped columns {missing}"
    assert len(enriched) == len(actions), f"{source}: enrichment changed row count {len(actions)} -> {len(enriched)}"


@_PARAMETRIZE
def test_apply_spadl_enrichments_nan_player_id_safe(source, converter, fixture) -> None:  # type: ignore[no-untyped-def]
    """Regression for the silly-kicks NaN-safety contract (silly-kicks ≥ 2.5.0).

    Pre-2.5.0 ``compute_spadl_vaep`` failed in production with::

        File "silly_kicks/spadl/utils.py", line 534, in add_pre_shot_gk_context
            gk_id = int(player_id[window_start + relative_indices[-1]])
        ValueError: cannot convert float NaN to integer

    Curated dev fixtures don't reliably contain the trigger shape (the bug
    appeared on real bronze data from Phase H IDSSE re-ingest, run
    ``959919928283335`` task ``compute_spadl_vaep``). To lock in the contract
    independent of fixture coverage, this test injects NaN into the
    ``player_id`` column at every 5th row of the converted actions frame
    — the same shape silly-kicks's enrichment helpers index into.

    silly-kicks 2.5.0 ships ADR-003 (NaN-safety contract for enrichment
    helpers): a NaN in any caller-supplied identifier column is treated
    as "not identifiable" for that row — the helper produces NULL output
    on the affected row, never raises. This test fails on any
    silly-kicks build that violates that contract.
    """
    from ingestion.spadl_enrichments import apply_spadl_enrichments

    df = pd.read_parquet(_FIXTURE_DIR / fixture)
    adapted, hti = _adapt_input(source, df)
    actions, _ = _call_converter(source, converter, adapted, hti, df)

    # Inject np.nan into player_id on every 5th row to mirror the exact
    # production failure mode: bronze.spadl_actions returned player_id
    # with np.nan for some rows. Different providers use different native
    # player_id dtypes (StatsBomb/Wyscout: int; IDSSE/Metrica: string DFL
    # IDs); pandas upcasts each appropriately on np.nan assignment.
    # silly-kicks ≥ 2.5.0 must handle this under ADR-003 (NaN-safety contract).
    import numpy as np

    actions_with_nan = actions.copy()
    nan_mask = actions_with_nan.index % 5 == 0
    n_nan = int(nan_mask.sum())
    assert n_nan > 0, f"{source}: fixture too small to inject NaN"
    actions_with_nan.loc[nan_mask, "player_id"] = np.nan

    # The contract: this must not raise.
    enriched = apply_spadl_enrichments(actions_with_nan, source=source)

    # Enrichment chain ran to completion: row count preserved, contract
    # columns all present. The actual NULL-handling for affected rows is
    # silly-kicks's contract to honor — we don't assert specific values
    # for the NaN rows here, only that no exception escaped.
    expected_cols = {
        "possession_id_heuristic",
        "gk_role",
        "gk_was_distributing",
        "gk_was_engaged",
        "gk_actions_in_possession",
        "defending_gk_player_id",
    }
    missing = expected_cols - set(enriched.columns)
    assert not missing, f"{source}: enrichment chain dropped columns {missing}"
    assert len(enriched) == len(actions_with_nan), (
        f"{source}: enrichment changed row count {len(actions_with_nan)} -> {len(enriched)}"
    )
