"""Drift guard (L4) — the remaining COPIED domain converters must not diverge from legacy.

TF-23 (ADR-034/ADR-035) retired the metrica + skillcorner copies in
``analytics.action_context.convert``: the AC tracking path now calls the silly-kicks
``tracking.{skillcorner,metrica}.convert_to_frames`` builders via
``analytics.action_context.sk_frame_adapters``, and the SkillCorner period offset is
single-sourced from ``silly_kicks.spadl.skillcorner._PERIOD_START_SECONDS``. The metrica/SC
drift guards (and the documented-SC-divergence guard) are therefore gone, replaced by a
STRUCTURAL guard that the AC builders are not re-introduced.

ADR-067 extends that: the Savitzky-Golay velocity helper is DELETED from both copies too (it had
dropped silly-kicks' ``len(x_vals) <= 1`` guard and crashed on 1-frame tracks). Velocity now comes
from the silly-kicks ``preprocess=`` seam, so its drift guard became a delete-and-depend guard.
Only the GradientSports converter is still copied (GS uses the lakehouse converter input), so its
AST drift guard remains.

PR-1 (TC-1 retirement): ``ingestion.tracking_context`` is now DELETED entirely with the TC-1
pipeline, so only the ``analytics.action_context.convert`` copy remains to guard against the
velocity helper re-appearing.

Comparison is by AST (parsed structure), robust to formatting but catching real logic change.
"""

from __future__ import annotations

import ast
import inspect
import textwrap

from analytics.action_context import convert as new
from ingestion import action_context as ac_legacy


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


def test_velocity_helper_is_deleted_and_depended() -> None:
    """ADR-067: BOTH lakehouse copies of the velocity derivation are DELETED; silly-kicks owns it.

    The port re-implemented silly-kicks' short-group fallback but dropped upstream's
    ``len(x_vals) <= 1`` guard, so a 1-frame player track hit ``np.gradient`` (needs >= 2 points)
    and raised. One raising batch failed the whole ``applyInPandas`` write, so the unit emitted 0 of
    550 actions -- and the drain swallowed it, reporting SUCCESS (2026-07-11, skillcorner:1552423:2).

    This replaces the former AST-equality drift guard: with no second copy to compare against, the
    only contract worth asserting is that neither copy comes BACK. A comment claiming a copy
    "matches silly-kicks" is not a contract -- deletion is.
    """
    assert not hasattr(new, "_derive_velocities_savgol"), (
        "analytics.action_context.convert re-grew _derive_velocities_savgol (delete-and-depend regression)"
    )
    # The second copy (ingestion.tracking_context) is DELETED entirely with the TC-1 pipeline
    # (PR-1), so its half of the guard is subsumed — there is no module left to re-grow the helper.


def test_gradientsports_converter_no_drift() -> None:
    assert _ast_of(new._bronze_gradientsports_to_converter_input) == _ast_of(
        ac_legacy._bronze_gradientsports_to_converter_input
    )


def test_remaining_shared_constants_match() -> None:
    # metrica/SC consumed-cols + SC period-start were deleted from the AC copy (TF-23); only the
    # GradientSports frame-rate constant remains shared between the AC and legacy converters.
    assert new._GS_FRAME_RATE == ac_legacy._GS_FRAME_RATE
