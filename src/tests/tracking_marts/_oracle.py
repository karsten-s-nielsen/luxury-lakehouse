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
from analytics.action_context.unit_inputs import UnitInputs
from analytics.action_context.work_unit import WorkUnit

# SINGLE SOURCE of the build+score CODE (ADR-087 amendment 3): the mechanics live in the wheel module
# ``ingestion.tracking_marts_canary`` (so the preflight's bundled-fixture Probe C and this test oracle
# share one builder — no drift). This oracle supplies its OWN ``FIXTURE_ROOT`` + per-provider ``UNITS``
# (its multi-provider transport fixtures) and the test-only compare helpers below.
from ingestion import tracking_marts_canary as _canary

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

#: The six tracking-marts output keys — single-sourced from the lifted canary module.
MART_KEYS: tuple[str, ...] = _canary.MART_KEYS

#: The build+score CODE is single-sourced from ``ingestion.tracking_marts_canary`` (no drift). These thin
#: wrappers bind this oracle's ``FIXTURE_ROOT`` + per-provider ``UNITS`` / ``metadata`` to that one builder.
synthetic_xg_preds = _canary.synthetic_xg_preds
actions_with_synthetic_xg = _canary.actions_with_synthetic_xg
_access_tier = _canary._access_tier


def work_unit(provider: str) -> WorkUnit:
    match_id, period = UNITS[provider]
    return WorkUnit(provider=provider, match_id=match_id, period=period)


def build_inputs(provider: str) -> UnitInputs:
    """Build oriented ``(actions, frames, xt)`` for a provider's fixture unit (the driver's seam)."""
    return _canary.build_inputs(FIXTURE_ROOT, work_unit(provider))


def metadata(provider: str) -> MatchMeta:
    return ParquetMatchMetadataSource(FIXTURE_ROOT).metadata(work_unit(provider))


def score_all_marts(
    provider: str,
    inp: UnitInputs,
    frames: pd.DataFrame,
    *,
    include_gkdv: bool = True,
) -> dict[str, pd.DataFrame]:
    """Run the six pure scorers on a GIVEN ``frames`` (single-sourced — no drift).

    The transport-neutrality capstone calls this twice — once with the in-memory built frames, once with
    the spilled+restored frames — and asserts equality per mart. ``pure_mart_outputs`` is the built-frame
    case (the baseline). Delegates to the lifted ``tracking_marts_canary.score_all_marts``, binding this
    oracle's per-provider ``match_id`` (``UNITS``) + ``metadata`` so the identity stamps are unchanged.
    """
    match_id, _period = UNITS[provider]
    return _canary.score_all_marts(
        provider, inp, frames, match_id=match_id, meta=metadata(provider), include_gkdv=include_gkdv
    )


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
    # Parquet-adapter re-exports: ``test_frame_windows`` builds units through ``_oracle.Parquet*Source``.
    "ParquetActionsSource",
    "ParquetFrameSource",
    "ParquetMatchMetadataSource",
    "ParquetXtSource",
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
