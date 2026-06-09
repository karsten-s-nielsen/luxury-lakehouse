"""Drift guard (L4) — the COPIED domain converters must not diverge from legacy.

Phase A copies the pure bronze->frames converters into
``analytics.action_context.convert`` while leaving the legacy copies in
``ingestion.tracking_context`` / ``ingestion.action_context`` untouched (M4: the
legacy modules remain the differential oracle). Two copies of behavior-critical
coordinate logic can silently diverge and corrupt the oracle relationship the
copy was meant to protect. This test fails the moment they do.

Comparison is by AST (parsed structure), so it is robust to formatting
differences (ruff may wrap a line differently in one file) but catches any
real logic change. Constants are compared by value.
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


def test_idsse_converter_no_drift() -> None:
    assert _ast_of(new._bronze_idsse_to_sportec_input) == _ast_of(tc_legacy._bronze_idsse_to_sportec_input)


def test_metrica_converter_no_drift() -> None:
    assert _ast_of(new._bronze_metrica_to_frames) == _ast_of(tc_legacy._bronze_metrica_to_frames)


def test_skillcorner_converter_no_drift() -> None:
    assert _ast_of(new._bronze_skillcorner_to_frames) == _ast_of(tc_legacy._bronze_skillcorner_to_frames)


def test_velocity_helper_no_drift() -> None:
    assert _ast_of(new._derive_velocities_savgol) == _ast_of(tc_legacy._derive_velocities_savgol)


def test_gradientsports_converter_no_drift() -> None:
    assert _ast_of(new._bronze_gradientsports_to_converter_input) == _ast_of(
        ac_legacy._bronze_gradientsports_to_converter_input
    )


def test_consumed_cols_constants_match() -> None:
    assert new._IDSSE_CONSUMED_COLS == tc_legacy._IDSSE_CONSUMED_COLS
    assert new._METRICA_CONSUMED_COLS == tc_legacy._METRICA_CONSUMED_COLS
    assert new._SKILLCORNER_CONSUMED_COLS == tc_legacy._SKILLCORNER_CONSUMED_COLS
    assert new._SKILLCORNER_PERIOD_START_SECONDS == tc_legacy._SKILLCORNER_PERIOD_START_SECONDS
    assert new._GS_FRAME_RATE == ac_legacy._GS_FRAME_RATE
