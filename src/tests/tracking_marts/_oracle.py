"""Shared transport-neutrality oracle for the tracking-marts executor refactor (rev3).

The executor refactor (two-stage build-once -> UC-Volume-Parquet spill -> six per-mart scoring
passes) must NOT change any scorer's output vs the pre-refactor driver path. This module is the
reference: it builds a unit's oriented ``(actions, frames, xt)`` from the committed AC fixtures via the
SAME ``build_unit_inputs`` seam the driver uses, then runs each of the six pure scorers directly
(the "baseline"). The executor-path tests (``test_build_spill`` / ``test_transport_oracle``) assert the
refactored path reproduces these baseline outputs byte-for-byte on silly-kicks 4.123 (transport
neutrality isolated from the 4.128 DAS shift).

Pure pandas, no Spark — the scorers are pure cores (``build_unit_inputs`` is pure-pandas); the Spark
``mapInPandas`` dispatch is covered by the ``_make_streaming_group_mapper`` carry/flush test
(``action_context/test_adr045_perf.py``) + live Part-B, matching the repo's testing posture.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

import numpy as np
import pandas as pd

from analytics.action_context.local.parquet_sources import (
    ParquetActionsSource,
    ParquetFrameSource,
    ParquetMatchMetadataSource,
    ParquetXtSource,
)
from analytics.action_context.unit_inputs import UnitInputs, build_unit_inputs
from analytics.action_context.work_unit import WorkUnit

if TYPE_CHECKING:  # pragma: no cover - typing only
    from analytics.action_context.work_unit import MatchMeta

FIXTURE_ROOT = "src/tests/fixtures/action_context"

#: One small committed fixture unit per tracking provider (all have frames + actions + meta).
#: gradientsports 10502_p1 is actions-only (no frames) and is deliberately excluded.
UNITS: dict[str, tuple[str, int]] = {
    "skillcorner": ("1886347", 2),
    "idsse": ("J03WMXmini", 1),
    "gradientsports": ("10517", 3),
    "metrica": ("Sample_Game_1", 2),
}

#: The six tracking-marts output keys (aligned with ``tracking_marts_dispatch.make_mart_scorers``).
MART_KEYS: tuple[str, ...] = (
    "off_ball_runs",
    "action_defensive",  # defensive_credit aggregate
    "defensive_credit_attributions",  # defensive_credit long
    "gk_decision",
    "rest_defense",
    "gkdv_observations",
)


def work_unit(provider: str) -> WorkUnit:
    match_id, period = UNITS[provider]
    return WorkUnit(provider=provider, match_id=match_id, period=period)


def build_inputs(provider: str) -> UnitInputs:
    """Build oriented ``(actions, frames, xt)`` for a provider's fixture unit (the driver's seam)."""
    wu = work_unit(provider)
    grid, xt_l, xt_w = ParquetXtSource(FIXTURE_ROOT).grid()
    return build_unit_inputs(
        wu,
        frame_bundle=ParquetFrameSource(FIXTURE_ROOT).frames(wu),
        actions_df=ParquetActionsSource(FIXTURE_ROOT).actions(wu),
        meta=metadata(provider),
        xt_grid_data=grid,
        xt_l=xt_l,
        xt_w=xt_w,
    )


def metadata(provider: str) -> MatchMeta:
    return ParquetMatchMetadataSource(FIXTURE_ROOT).metadata(work_unit(provider))


def synthetic_xg_preds(inp: UnitInputs) -> pd.DataFrame:
    """The synthetic per-shot xG predictions (fct_shot_xg is absent in fixtures). Shared by the baseline and
    the dispatch-closure test so both ``attach_xg`` the identical preds."""
    import silly_kicks.spadl.config as cfg

    shot_id = cfg.actiontype_id["shot"]
    shots = inp.actions[inp.actions["type_id"] == shot_id]
    return pd.DataFrame(
        {
            "data_source": shots["data_source"].to_numpy(),
            "match_id_native": shots["match_id_native"].to_numpy(),
            "action_id": shots["action_id"].to_numpy(),
            "xg": np.full(len(shots), 0.12),
        }
    )


def actions_with_synthetic_xg(inp: UnitInputs) -> pd.DataFrame:
    """Per-shot synthetic xG via the production ``attach_xg`` LEFT-JOIN (mirrors the defensive-credit test)."""
    from ingestion.defensive_credit_writer import attach_xg

    return attach_xg(inp.actions, synthetic_xg_preds(inp))


def _access_tier(inp: UnitInputs) -> str | None:
    _at = inp.actions["access_tier"].iloc[0] if "access_tier" in inp.actions.columns else None
    return None if _at is None or (isinstance(_at, float) and _at != _at) else str(_at)


def score_all_marts(
    provider: str,
    inp: UnitInputs,
    frames: pd.DataFrame,
    *,
    include_gkdv: bool = True,
) -> dict[str, pd.DataFrame]:
    """Run the six pure scorers on a GIVEN ``frames`` (one signature source — no drift).

    The transport-neutrality capstone calls this twice — once with the in-memory built frames, once with
    the spilled+restored frames — and asserts equality per mart. ``pure_mart_outputs`` is the built-frame
    case (the baseline). Identity columns are stamped exactly as ``TrackingMartsProcessor.process`` stamps.
    """
    from ingestion.defensive_credit_writer import compute_action_defensive_credit, compute_defensive_credit_long
    from ingestion.gk_decision_writer import _bundled_completion_model, score_gk_decision_unit
    from ingestion.gkdv_writer import score_gkdv_unit
    from ingestion.off_ball_runs_writer import compute_off_ball_runs
    from ingestion.restdefense_writer import compute_rest_defense_samples

    meta = metadata(provider)
    match_id, _period = UNITS[provider]
    actions_xg = actions_with_synthetic_xg(inp)
    access_tier = _access_tier(inp)

    out: dict[str, pd.DataFrame] = {
        "off_ball_runs": compute_off_ball_runs(inp.actions, frames, inp.xt),
        "action_defensive": compute_action_defensive_credit(actions_xg, frames, inp.xt),
        "defensive_credit_attributions": compute_defensive_credit_long(actions_xg, frames, inp.xt),
        "gk_decision": score_gk_decision_unit(
            inp.actions,
            frames,
            _bundled_completion_model(),
            data_source=provider,
            match_id=match_id,
            access_tier=access_tier,
        ),
        "rest_defense": compute_rest_defense_samples(inp.actions, frames, inp.xt, access_tier=access_tier),
    }
    if include_gkdv:
        obs = score_gkdv_unit(
            frames,
            meta.home_team_id,
            inp.xt,
            data_source=provider,
            match_id=match_id,
            competition_id=None,
            season_id=None,
        )
        obs = obs.copy()
        obs["match_id"] = match_id  # the processor stamps this for the per-unit replaceWhere
        out["gkdv_observations"] = obs
    return out


def pure_mart_outputs(provider: str, *, include_gkdv: bool = True) -> dict[str, pd.DataFrame]:
    """The six pure scorers on a provider's fixture unit (built frames) — the neutrality BASELINE."""
    inp = build_inputs(provider)
    return score_all_marts(provider, inp, inp.frames, include_gkdv=include_gkdv)


def sorted_for_compare(df: pd.DataFrame) -> pd.DataFrame:
    """Deterministic KEY-column sort for ``assert_frame_equal`` (does NOT re-sort frame order away).

    Sorts only by identity/key columns present, so a genuine intra-unit frame-order regression in the
    VALUE columns still fails the compare (TM-PLAN-12 / TM-PLAN-03).
    """
    key_cols = [
        c for c in ("action_id", "player_id", "frame_id", "decision_id", "sample_id", "rule") if c in df.columns
    ]
    if not key_cols:
        return df.reset_index(drop=True)
    return df.sort_values(key_cols, kind="mergesort").reset_index(drop=True)


def assert_mart_equal(got: pd.DataFrame, expected: pd.DataFrame, mart: str) -> None:
    """Order-insensitive-on-keys mart-output VALUE comparison.

    ``check_dtype=False``: the pure-fixture baseline carries pandas NULLABLE extension dtypes (``string`` /
    ``boolean`` / ``Int64``) from the pyarrow-backed fixture read, while the Spark round-trip yields their
    numpy equivalents (``object`` / ``bool`` / ``int64``) — same values, and the FINAL Delta dtype is
    governed identically in both paths by each mart's explicit ``StructType`` (the writers' ``_struct_type``,
    pinned by the DDL-parity tests), not by the pandas intermediate. Transport neutrality is a VALUE claim;
    frame-dtype fidelity (float32 / category) is checked strictly in the spill round-trip test instead.
    """
    from pandas.testing import assert_frame_equal

    def _canon(df: pd.DataFrame) -> pd.DataFrame:
        # Object-cast + unify EVERY null (NaN / None / <NA>) to None. Dissolves the pure-vs-Spark artifacts
        # (object/bool/int64 + None from the Spark round-trip vs string/boolean/Int64 + <NA> from the pyarrow
        # fixture read, incl. all-null columns convert_dtypes cannot canonicalize). Transport neutrality is a
        # VALUE claim; final Delta dtype is governed by each mart's StructType (see the dtype note above).
        # np.where (not DataFrame.where(..., None)) — the pandas stub's ``other`` union excludes None,
        # though None is valid at runtime. Fill the False branch from an explicit object array of None
        # (np.where's ``y`` wants ArrayLike, not a bare None) so the result is a pure-object None-unified frame.
        obj_arr = df.astype(object).to_numpy()
        none_arr = np.full(obj_arr.shape, None, dtype=object)
        arr = np.where(df.notna().to_numpy(), obj_arr, none_arr)
        return pd.DataFrame(arr, columns=df.columns).reset_index(drop=True)

    g = sorted_for_compare(got)
    e = sorted_for_compare(expected)
    # Align column order (production write selects an explicit column list; order is not semantic here).
    assert set(g.columns) == set(e.columns), f"{mart}: column set differs: {set(g.columns) ^ set(e.columns)}"
    g = g[list(e.columns)]
    assert_frame_equal(_canon(g), _canon(e), check_dtype=False, check_like=False, obj=mart)


__all__: list[str] = [
    "FIXTURE_ROOT",
    "MART_KEYS",
    "UNITS",
    "actions_with_synthetic_xg",
    "assert_mart_equal",
    "build_inputs",
    "metadata",
    "pure_mart_outputs",
    "score_all_marts",
    "sorted_for_compare",
    "synthetic_xg_preds",
    "work_unit",
]
