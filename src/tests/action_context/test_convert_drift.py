"""Drift guard (L4) — the remaining COPIED domain converters must not diverge from legacy.

TF-23 (ADR-034/ADR-035) retired the metrica + skillcorner copies in
``analytics.action_context.convert``: the AC tracking path now calls the silly-kicks
``tracking.{skillcorner,metrica}.convert_to_frames`` builders via
``analytics.action_context.sk_frame_adapters``, and the SkillCorner period offset is
single-sourced from ``silly_kicks.spadl.skillcorner._PERIOD_START_SECONDS``. The metrica/SC
drift guards (and the documented-SC-divergence guard) are therefore gone, replaced by a
STRUCTURAL guard that the AC builders are not re-introduced. The GradientSports converter and
the shared Savitzky-Golay velocity helper are still copied (GS uses the lakehouse converter
input; velocity stays lakehouse-owned), so their drift guards remain.

Comparison is by AST (parsed structure), robust to formatting but catching real logic change.
"""

from __future__ import annotations

import ast
import inspect
import textwrap

from analytics.action_context import convert as new
from ingestion import action_context as ac_legacy
from ingestion import tracking_context as tc_legacy


def _ast_of(fn) -> str:
    return ast.dump(ast.parse(textwrap.dedent(inspect.getsource(fn))))


def test_ac_metrica_skillcorner_builders_deleted() -> None:
    """Structural guard (TF-23): the AC metrica/SC builders + their constants are gone — the
    dispatch goes through sk_frame_adapters. Re-introducing a lakehouse copy (the duplicated-truth
    this PR removed) fails here."""
    for sym in (
        "_bronze_metrica_to_frames",
        "_bronze_skillcorner_to_frames",
        "_METRICA_CONSUMED_COLS",
        "_SKILLCORNER_CONSUMED_COLS",
        "_SKILLCORNER_PERIOD_START_SECONDS",
    ):
        assert not hasattr(new, sym), f"analytics.action_context.convert re-grew {sym} (delete-and-depend regression)"


def test_velocity_helper_no_drift() -> None:
    assert _ast_of(new._derive_velocities_savgol) == _ast_of(tc_legacy._derive_velocities_savgol)


def test_gradientsports_converter_no_drift() -> None:
    assert _ast_of(new._bronze_gradientsports_to_converter_input) == _ast_of(
        ac_legacy._bronze_gradientsports_to_converter_input
    )


def test_remaining_shared_constants_match() -> None:
    # metrica/SC consumed-cols + SC period-start were deleted from the AC copy (TF-23); only the
    # GradientSports frame-rate constant remains shared between the AC and legacy converters.
    assert new._GS_FRAME_RATE == ac_legacy._GS_FRAME_RATE
