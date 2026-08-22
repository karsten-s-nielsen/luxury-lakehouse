"""Fast CI golden gate: RECOMPUTE a tiny real-pipeline slice and compare to a frozen mini-golden.

This is the always-on counterpart to ``test_e2e.py``. ``test_e2e`` runs the full 97-action
IDSSE anchor (~5 min, DAS-dominated) and is gated behind ``AC1_E2E=1`` -> it never runs in CI.
``test_differential`` DOES run in CI but only *reads* the committed golden vs legacy oracles; it
never recomputes the pipeline, and DAS is a ``known_divergence`` it does not assert. So a value
shift in DAS / ghost-GK / any enrichment could ride ``main`` uncaught -- which is exactly what
happened: silly-kicks 4.2.0's DAS carrier-forwarding change landed in #328 and was only caught
when the gated e2e was finally run during the 4.4.0 adoption (ADR-036).

This test closes that gap. It recomputes the REAL ``run_work_unit`` -> ``enrich_batch`` on a
3-action / 2-batch slice of the IDSSE J03WMX_p1 fixture (``idsse/J03WMXmini_p1/``, all 3 actions
carry non-NaN DAS + ghost-GK) and asserts ALL 164 output columns reproduce the frozen mini-golden
(``RESULT_COLUMNS`` = 165 incl. ``_ingested_at``, which ``build_output`` drops before the write ->
164 in the golden parquet; silly-kicks 4.87.0 surface). Because the mini-golden is frozen from the
same slice, it is self-consistent: a library/algorithm change diverges the RECOMPUTE from the
frozen golden -> the assertion fails in CI.

Runtime ~30s local (per-action ghost-GK brute-force KDE dominates); it will drop to a few seconds
once silly-kicks ships the FFT-KDE ghost-GK backend. NOT gated -- runs in the default suite.

Regenerate the mini-golden (after an INTENTIONAL, signed-off value change) with:
    uv run python scripts/build_ac1_mini_golden.py
and commit the updated ``idsse/J03WMXmini_p1/golden.parquet`` in the same PR.
"""

from __future__ import annotations

import numpy as np
import pandas as pd
import yaml

_ROOT = "src/tests/fixtures/action_context"
_MINI_DIR = f"{_ROOT}/idsse/J03WMXmini_p1"
_MARTS_YML = "dbt_project/models/marts/_marts__models.yml"
_FLOAT_ATOL = 1e-6
_EXACT_COLS = {"data_source", "match_id", "action_id", "period_id", "type_name"}


def _recompute() -> pd.DataFrame:
    from analytics.action_context.local.parquet_sources import (
        ParquetActionsSource,
        ParquetFrameSource,
        ParquetMatchMetadataSource,
        ParquetXtSource,
    )
    from analytics.action_context.pipeline import run_work_unit
    from analytics.action_context.work_unit import WorkUnit

    class _Collect:
        df: pd.DataFrame | None = None

        def write(self, wu: WorkUnit, result_df: pd.DataFrame) -> int:
            self.df = result_df
            return len(result_df)

    sink = _Collect()
    run_work_unit(
        WorkUnit(provider="idsse", match_id="J03WMXmini", period=1),
        frames=ParquetFrameSource(_ROOT),
        actions=ParquetActionsSource(_ROOT),
        xt=ParquetXtSource(_ROOT),
        meta=ParquetMatchMetadataSource(_ROOT),
        sink=sink,
        is_slice=True,  # ADR-067: fixture = windowed frames + whole-match actions
    )
    assert sink.df is not None
    return sink.df


def test_mini_golden_recompute_matches_and_exercises_das_ghost() -> None:
    golden = pd.read_parquet(f"{_MINI_DIR}/golden.parquet")
    result = _recompute()

    # Same shape + columns as the frozen mini-golden.
    assert list(result.columns) == list(golden.columns), "result columns drifted from mini-golden"
    assert len(result) == len(golden), f"row count {len(result)} != mini-golden {len(golden)}"

    # The gate is only meaningful if the heaviest enrichments are actually exercised on this slice.
    assert golden["das_diff"].notna().all(), "mini-golden has NaN DAS -- a DAS shift would not be caught"
    assert golden["ghost_gk_x"].notna().all(), "mini-golden has NaN ghost-GK -- a ghost-GK shift would not be caught"

    # M13: boundary-dup-free.
    dupes = result.groupby(["match_id", "action_id", "period_id"]).size()
    assert dupes[dupes > 1].empty, f"duplicate action rows: {dupes[dupes > 1].to_dict()}"

    r = result.sort_values(["period_id", "action_id"]).reset_index(drop=True)
    g = golden.sort_values(["period_id", "action_id"]).reset_index(drop=True)

    mismatches: list[str] = []
    for col in g.columns:
        if col in _EXACT_COLS:
            if not r[col].astype(str).equals(g[col].astype(str)):
                mismatches.append(f"{col}: exact mismatch")
            continue
        rv = pd.to_numeric(r[col], errors="coerce").to_numpy(dtype=float)
        gv = pd.to_numeric(g[col], errors="coerce").to_numpy(dtype=float)
        both = ~(np.isnan(rv) | np.isnan(gv))
        if not np.array_equal(np.isnan(rv), np.isnan(gv)):
            mismatches.append(f"{col}: NaN pattern differs")
        elif both.any() and not np.allclose(rv[both], gv[both], atol=_FLOAT_ATOL, rtol=1e-4):
            mismatches.append(f"{col}: maxd={np.abs(rv[both] - gv[both]).max():.4g}")

    detail = "\n  ".join(mismatches)
    assert not mismatches, f"mini-golden recompute diverged (intentional value change? regen mini-golden):\n  {detail}"


_NEW_PLAYER_INFLUENCE = [
    "actor_reachable_area_m2", "off_ball_xt_team", "off_ball_xt_opponent",
    "off_ball_xt_diff", "reachable_area_team", "reachable_area_opponent", "reachable_area_diff",
]  # fmt: skip
_NEW_STRUCTURAL = ["structural_lbs", "structural_sgm", "structural_sdi"]
_PASS_OR_CROSS = {"pass", "cross"}


def test_new_ac_fields_emit_and_nan_contracts() -> None:
    """Golden-independent guards for the 11 new columns (spec §9).

    - Emit-drift: xcross_attempt + the 7 player-influence columns populate on the possessing-team
      tracking slice, so an upstream emit rename (column drops out of the enrich output and
      build_output fills it all-NaN) fails this RED without needing the frozen golden.
    - NaN contract: structural_* is NaN on every non-pass/cross action (silly-kicks contract).
    """
    result = _recompute()

    for col in ["xcross_attempt", *_NEW_PLAYER_INFLUENCE, *_NEW_STRUCTURAL]:
        assert col in result.columns, f"{col} missing from enrich output"

    # Emit-drift guard: these populate for the possessing team on the IDSSE mini slice.
    for col in ["xcross_attempt", *_NEW_PLAYER_INFLUENCE]:
        assert result[col].notna().any(), f"{col} all-NaN — aggregator not wired or emit renamed upstream"

    # Structural NaN contract: non-NaN only on pass/cross rows.
    non_pass = ~result["type_name"].isin(_PASS_OR_CROSS)
    for col in _NEW_STRUCTURAL:
        assert result.loc[non_pass, col].isna().all(), f"{col} must be NaN on non-pass/cross actions"
    # If the slice has any pass/cross, structural must populate on at least one (emit-drift guard).
    if result["type_name"].isin(_PASS_OR_CROSS).any():
        for col in _NEW_STRUCTURAL:
            assert result.loc[~non_pass, col].notna().any(), f"{col} all-NaN on pass/cross — emit drift?"


# silly-kicks 4.87.0 / spec §7.4: the 16 v1 xt_gk METRIC columns are RETIRED from the drain; xt_gk_v2
# replaces them as a MART-JOIN set (schema-level retirement guarded by test_xtgk_v2_replaces_v1.py).
# What the DRAIN still emits are the resolved-coordinate geometry bridge + gk_completion — the KEPT set
# (identical to test_xtgk_v2_replaces_v1._KEPT_COLUMNS).
_KEPT_XT_GK_COLS = ["xt_gk_origin_x", "xt_gk_origin_y", "xt_gk_dest_x", "xt_gk_dest_y", "gk_completion"]
_RETIRED_XT_GK_V1_COLS = [
    "xt_gk", "xt_gk_possession", "xt_gk_counter", "xt_gk_direct", "xt_gk_high_press",
    "xt_gk_low_block", "xt_gk_base", "xt_gk_pev", "xt_gk_rav", "xt_gk_dzv", "xt_gk_pressure",
    "xt_gk_origin_source", "xt_gk_dest_source", "xt_gk_origin_confidence",
    "xt_gk_completion_variant", "xt_gk_completion_source",
]  # fmt: skip


def test_xt_gk_fields_present_and_scope_contract() -> None:
    """4.36.0 resolved coords + gk_completion KEPT; 16 v1 metric columns RETIRED (spec §7.4).

    The mini slice (3 IDSSE open-play actions) contains NO GK-distribution action, so the KEPT
    columns are all-NaN/None — a non-null value on an open-play row would mean the upstream in-scope
    mask drifted. The retired v1 metric columns must be ABSENT from the drain output entirely (this
    recompute-level check complements the schema-level test_xtgk_v2_replaces_v1). EMIT coverage of
    the KEPT columns lives in test_full_golden_xt_gk_emits (the full anchor has a goalkick).
    """
    result = _recompute()
    for col in _KEPT_XT_GK_COLS:
        assert col in result.columns, f"{col} missing from enrich output"
        assert result[col].isna().all(), f"{col} non-null on an open-play-only slice — xT-GK scope drift"
    for col in _RETIRED_XT_GK_V1_COLS:
        assert col not in result.columns, f"retired v1 xt_gk column still emitted by the drain: {col}"


def test_full_golden_xt_gk_emits() -> None:
    """Emit-drift lock through the FROZEN full anchor (golden-reading, CI-cheap).

    The J03WMX_p1 anchor owns a goalkick; the frozen golden must carry non-null KEPT xT-GK values on
    it (the 4 resolved-coordinate columns + gk_completion). An upstream emit-rename would surface at
    golden-regen time as this going all-NaN — this makes that loud instead of silently freezing an
    empty feature family. (The row count is NOT pinned — only non-empty.)
    """
    golden = pd.read_parquet(f"{_ROOT}/idsse/J03WMX_p1/golden.parquet")
    gk_rows = golden[golden["type_name"] == "goalkick"]
    assert len(gk_rows) > 0, "full anchor lost its goalkick rows — re-extract before trusting this gate"
    for col in _KEPT_XT_GK_COLS:
        assert golden[col].notna().any(), f"{col} all-NaN in the full golden — emit drift at regen"


def test_ghost_gk_populated_after_direction_rekey() -> None:
    """silly-kicks 4.87.0 direction re-key guard (spec §6.1, KEEP case).

    ``add_ghost_gk`` still READS ``home_team_id`` (the ADR-055 exception — one of only two
    aggregators that keep the kwarg). A wrongly-dropped kwarg NaNs the ghost-GK columns, and the
    rebaselined golden would be blind to that. This is an independent, non-vacuous guard — it also
    proves the whole re-keyed chain RUNS on 4.87.0 without ``TypeError``, and that ``add_obso`` gets
    its mandatory ``xt=`` (omitting it would fire a non-fatal ``SyntheticEPVWarning`` and fall back to
    synthetic EPV — the warning is not escalated to an error; the obso_epv_source value test guards it).
    """
    result = _recompute()
    assert result["ghost_gk_x"].notna().any(), "ghost_gk_* all-NaN — home_team_id wrongly dropped from add_ghost_gk"


def test_keep_kwargs_present_in_enrich_source() -> None:
    """Structural guard for the two KEEP-``home_team_id`` cases (spec §6.1).

    ``add_xcross_attempt``'s ``home_team_id`` feeds the ``score_differential`` FEATURE (silently NaN
    on drop), not a persisted column — so guard both KEEP cases at the source level, so the kwarg
    cannot be silently removed by a future edit.
    """
    import re

    with open("src/analytics/action_context/enrich.py", encoding="utf-8") as fh:
        src = fh.read()
    for fn in ("add_ghost_gk", "add_xcross_attempt"):
        call = re.search(fn + r"\((.*?)\n    \)", src, re.S)
        assert call and "home_team_id=home_team_id" in call.group(1), (
            f"{fn} must KEEP home_team_id=home_team_id (silent-failure case)"
        )


# silly-kicks 4.87.0 DRAIN-NATIVE columns (Task 8 new aggregator calls + Task 17a team_shape gaps).
# obso_epv_source (Task 6) is guarded separately by its value test; here we assert PRESENCE of the
# 16 Task-8 columns + the 6 free-ride team_shape gap columns.
_NEW_SK4861_COLS = [
    "run_value_target", "run_value_disruptive_sum", "run_value_enabled_pass",
    "n_disruptive_runs", "n_valued_disruptive_runs",
    "press_commitment", "press_commitment_closing_speed", "press_commitment_source",
    "packing_made", "packing_goal_threat", "packing_net", "packing_receiver_player_id", "packing_secured",
    "das_source", "ghost_gk_source", "max_single_defender_player_id",
    "team_shape_defensive_line_height_attacking", "team_shape_defensive_line_height_defending",
    "team_shape_inter_line_gap_1_attacking", "team_shape_inter_line_gap_1_defending",
    "team_shape_inter_line_gap_2_attacking", "team_shape_inter_line_gap_2_defending",
]  # fmt: skip


def test_new_sk4861_columns_present() -> None:
    """PRESENCE guard for the silly-kicks 4.87.0 drain-native columns (spec §7.1; Task 8 + 17a).

    build_output filters _recompute() output to RESULT_COLUMNS, so a column survives into the output
    ONLY once schema.py registers it. Population is event-conditional (many are legitimately NaN on the
    3-action open-play mini slice), so PRESENCE — not notna — is the correct emit-drift guard here (same
    shape as test_xt_gk_fields_present_and_scope_contract).
    """
    result = _recompute()
    missing = [c for c in _NEW_SK4861_COLS if c not in result.columns]
    assert not missing, f"missing new columns from work-unit output: {missing}"


# ---------------------------------------------------------------------------------------------
# Task 12 Step 3 (review-3 H-1): golden <-> fct_action_context CONTRACT binding.
# ---------------------------------------------------------------------------------------------
# The mart is a SUPERSET of the drain. Two contract-only families are legitimately absent from the
# drain golden and MUST be exempted from the golden⊆contract direction:
#   * writer-join set — xt_gk_v2_* + gk_geometry_source + xt_gk_match_contaminated are scored by
#     ingestion.xt_gk_v2_writer into bronze.xt_gk_v2_predictions and LEFT-JOINed per action (ADR-013,
#     spec §7.4). The AC drain never produces them.
#   * Kimball surrogate keys — action_context_id / *_key resolve in the staging layer from the
#     golden's native ids; they are not drain output.
# A NEW mart-only column MUST be added to the matching set below, else Direction A fails (fail-closed).
_CONTRACT_WRITER_JOIN_COLS = frozenset(
    {
        "xt_gk_v2_position",
        "xt_gk_v2_pev",
        "xt_gk_v2_retention_loss",
        "xt_gk_v2_dzv",
        "xt_gk_v2",
        "gk_geometry_source",
        "xt_gk_match_contaminated",
    }
)
_CONTRACT_SURROGATE_KEY_COLS = frozenset(
    {"action_context_id", "match_key", "team_key", "player_key", "defending_gk_player_key"}
)
# Native ids the golden carries that staging RESOLVES into the surrogate keys above — golden-only,
# so they are exempted from the (golden ⊆ contract) direction.
_GOLDEN_NATIVE_ID_COLS = frozenset({"match_id", "team_id", "player_id"})


def _fct_action_context_contract() -> dict[str, str]:
    with open(_MARTS_YML, encoding="utf-8") as fh:
        doc = yaml.safe_load(fh)
    model = next(m for m in doc["models"] if m["name"] == "fct_action_context")
    return {c["name"]: c["data_type"] for c in model["columns"]}


def test_golden_columns_and_types_bound_to_fct_action_context_contract() -> None:
    """Bind the drain golden to the ``fct_action_context`` dbt CONTRACT (Task 12 Step 3 / spec §8).

    ``test_action_context_schema_parity`` pins ``RESULT_COLUMNS`` ↔ ``ACTION_CONTEXT_DDL``; this
    binds the golden (the drain output) ↔ the ``_marts__models.yml`` contract — an independent
    declaration of the same schema — so a mis-typed or unregistered new column cannot pass the
    rebaselined-golden equality while violating its contract.

    The mart is a SUPERSET of the drain (review-3 H-1), so this is ``golden ⊆ contract`` — NOT ``==``.
    It is non-vacuous in BOTH directions:
      * Direction A (load-bearing): every DRAIN-NATIVE contract column (contract minus the writer-join
        + surrogate-key sets) must be present in the golden. A contract-declared drain column the drain
        does not emit fails here.
      * Direction B: every golden column (minus the native ids staging resolves to surrogates) must be
        declared in the contract. A drain column the mart forgot to contract fails here.
      * Type leg: STRING-ness must agree on every shared drain-native column — ``build_output``
        stringifies every DDL STRING column to object/None and fills the rest numeric/bool, so a
        contract type that disagrees on string-vs-non-string is real DDL↔contract drift (the
        Arrow-serialization / dbt-contract boundary).
    """
    contract = _fct_action_context_contract()
    contract_cols = set(contract)
    golden = pd.read_parquet(f"{_MINI_DIR}/golden.parquet")
    golden_cols = set(golden.columns)

    # Non-vacuity: the parse actually found the (long) contract block.
    assert len(contract_cols) > 100, f"parsed only {len(contract_cols)} contract cols — wrong block/shape?"

    # Direction A — drain-native contract columns must all be in the golden.
    drain_native_contract = contract_cols - _CONTRACT_WRITER_JOIN_COLS - _CONTRACT_SURROGATE_KEY_COLS
    missing_from_golden = sorted(drain_native_contract - golden_cols)
    assert not missing_from_golden, (
        f"drain-native contract column(s) absent from the golden: {missing_from_golden} — the "
        "fct_action_context contract declares a column the AC drain does not emit (or it is a NEW "
        "mart-only column that must be added to _CONTRACT_WRITER_JOIN_COLS / _CONTRACT_SURROGATE_KEY_COLS)"
    )

    # Direction B — every golden column (minus resolved native ids) must be contracted.
    golden_uncontracted = sorted(golden_cols - _GOLDEN_NATIVE_ID_COLS - contract_cols)
    assert not golden_uncontracted, (
        f"golden column(s) not declared in the fct_action_context contract: {golden_uncontracted}"
    )

    # Type leg — STRING-ness agreement on the shared drain-native columns.
    type_mismatches: list[str] = []
    for col in sorted(drain_native_contract & golden_cols):
        contract_is_string = contract[col] == "string"
        gd = golden[col].dtype
        golden_is_string = pd.api.types.is_object_dtype(gd) or isinstance(gd, pd.StringDtype)
        if contract_is_string != golden_is_string:
            type_mismatches.append(f"{col}: contract={contract[col]} golden_dtype={gd}")
    assert not type_mismatches, "golden↔contract STRING-ness mismatch:\n  " + "\n  ".join(type_mismatches)


# ---------------------------------------------------------------------------------------------
# Task 14: new-column range + closed-vocabulary checks (spec §8).
# ---------------------------------------------------------------------------------------------
# Vocabs transcribed from the INSTALLED silly-kicks 4.87.0 source (re-confirmed 2026-08-20):
#   das_source            <- tracking/_das.py::DAS_SOURCE_VALUES
#   ghost_gk_source       <- tracking/_ghost_gk.py::GHOST_GK_SOURCE_VALUES
#   press_commitment_source <- tracking/_press_commitment.py::PRESS_COMMITMENT_SOURCE_VALUES
#   obso_epv_source       <- tracking/features.py::_resolve_epv_grid returns {"xt","injected","synthetic"};
#                            the xt= tracking path is "xt" (never "synthetic"), so the vocab here is
#                            {"xt","injected"}.
# A value outside a set is an UPSTREAM vocab change to fold in (update the constant + re-confirm the
# source), NOT a test to loosen.
_OBSO_SRC_VOCAB = {"xt", "injected"}
_DAS_SRC_VOCAB = {"computed", "unlinked", "unscoreable_frame", "team_unresolved", "unscoreable_call"}
_GHOST_SRC_VOCAB = {
    "computed",
    "velocity_unavailable",
    "no_keeper",
    "unlinked",
    "goal_end_unresolved",
    "direction_unresolved",
}
_PRESS_SRC_VOCAB = {
    "computed",
    "no_pressing_defender",
    "velocity_unavailable",
    "window_too_short",
    "degenerate_axis",
    "unlinked",
}
# Physical bound on press_commitment_closing_speed. It is a SIGNED single-player velocity projected
# onto the defender→actor axis (silly-kicks tracking/_press_commitment.py:177,184 —
# `v_close = vx*axis[0] + vy*axis[1]`, taken at the frame nearest the action), so a retreating
# (containing) defender is legitimately NEGATIVE. The physical invariant is therefore a MAGNITUDE
# bound at a single player's max speed (~11 m/s) with headroom — NOT `>= 0` (which the signed
# projection contradicts). |cs| <= 15 m/s.
_CLOSING_SPEED_ABS_MAX = 15.0


def test_new_column_ranges_and_vocab() -> None:
    """Range + closed-vocab guards for the silly-kicks 4.87.0 drain-native columns (Task 14, spec §8).

    Independent of the frozen golden (Task 12 rebaselines it from the same code, so the golden cannot
    validate correctness of a value baked in at the same time). This recomputes the slice and asserts
    provenance columns stay inside their upstream closed vocabularies and the physical/finite ranges
    hold. Runs only after schema.py registers the columns (build_output filters _recompute() output to
    RESULT_COLUMNS).
    """
    result = _recompute()

    def _nn(col: str) -> pd.Series:
        return result[col][result[col].notna()]

    assert set(_nn("obso_epv_source").unique()) <= _OBSO_SRC_VOCAB, set(_nn("obso_epv_source").unique())
    assert set(_nn("das_source").unique()) <= _DAS_SRC_VOCAB, set(_nn("das_source").unique())
    assert set(_nn("ghost_gk_source").unique()) <= _GHOST_SRC_VOCAB, set(_nn("ghost_gk_source").unique())
    assert set(_nn("press_commitment_source").unique()) <= _PRESS_SRC_VOCAB, set(
        _nn("press_commitment_source").unique()
    )

    cs = _nn("press_commitment_closing_speed").astype(float)
    assert bool((cs.abs() <= _CLOSING_SPEED_ABS_MAX).all()), (
        f"press_commitment_closing_speed out of physical m/s range: {cs.tolist()}"
    )

    assert bool((_nn("n_disruptive_runs").astype(float) >= 0).all()), "n_disruptive_runs < 0"
    assert bool(np.isfinite(_nn("packing_net").astype(float)).all()), "packing_net non-finite"
