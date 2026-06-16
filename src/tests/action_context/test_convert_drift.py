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


# NOTE: test_idsse_converter_no_drift was removed under delete-and-depend
# (ADR-031 T3 / Gate B): both copies of `_bronze_idsse_to_sportec_input`
# (AC-1 + legacy tracking_context) are deleted — the IDSSE tracking path now
# calls the silly-kicks port `shape_tracking_to_native`, so there is no longer
# a lakehouse copy to drift-guard. The metrica/skillcorner/GS copies remain.


def test_metrica_converter_no_drift() -> None:
    assert _ast_of(new._bronze_metrica_to_frames) == _ast_of(tc_legacy._bronze_metrica_to_frames)


def _strip_sc_rebase_statements(tree: ast.AST) -> ast.AST:
    """Drop any statement referencing _SKILLCORNER_PERIOD_START_SECONDS (the documented divergence)."""

    class _Strip(ast.NodeTransformer):
        def visit_FunctionDef(self, node: ast.FunctionDef) -> ast.FunctionDef:
            node.body = [
                stmt
                for stmt in node.body
                if not any(
                    isinstance(n, ast.Name) and n.id == "_SKILLCORNER_PERIOD_START_SECONDS" for n in ast.walk(stmt)
                )
            ]
            return node

    return ast.fix_missing_locations(_Strip().visit(tree))


def test_skillcorner_converter_documented_divergence_only() -> None:
    """The SC copies deliberately diverge by EXACTLY one statement (ADR-040 amendment).

    AC-1 moved the SkillCorner period re-base to the DISPATCH layer (the per-batch action
    window + M13 ownership consume the same clock), so its converter is pass-through; the
    LEGACY tracking_context pipeline has no dispatch re-base and keeps the subtraction in
    its converter (one layer subtracts in each pipeline — different layers). This test
    pins the divergence to precisely that statement: re-growing a subtraction in the AC-1
    copy (the double-subtraction class), or ANY other drift between the copies, fails here.
    """
    new_tree = ast.parse(textwrap.dedent(inspect.getsource(new._bronze_skillcorner_to_frames)))
    legacy_tree = ast.parse(textwrap.dedent(inspect.getsource(tc_legacy._bronze_skillcorner_to_frames)))

    # The AC-1 copy must NOT subtract (the dispatcher owns the re-base)...
    assert not any(
        isinstance(n, ast.Name) and n.id == "_SKILLCORNER_PERIOD_START_SECONDS" for n in ast.walk(new_tree)
    ), "AC-1 SC converter re-grew the offset subtraction (double-subtraction class)"
    # ...the legacy copy MUST (its pipeline has no dispatch re-base)...
    assert any(
        isinstance(n, ast.Name) and n.id == "_SKILLCORNER_PERIOD_START_SECONDS" for n in ast.walk(legacy_tree)
    ), "legacy SC converter lost its re-base — if tracking_context retired, restore verbatim equality"
    # ...and with that one statement stripped, the copies are otherwise identical.
    assert ast.dump(_strip_sc_rebase_statements(legacy_tree)) == ast.dump(new_tree), (
        "SC copies diverged beyond the documented re-base statement"
    )


def test_velocity_helper_no_drift() -> None:
    assert _ast_of(new._derive_velocities_savgol) == _ast_of(tc_legacy._derive_velocities_savgol)


def test_gradientsports_converter_no_drift() -> None:
    assert _ast_of(new._bronze_gradientsports_to_converter_input) == _ast_of(
        ac_legacy._bronze_gradientsports_to_converter_input
    )


def test_consumed_cols_constants_match() -> None:
    # _IDSSE_CONSUMED_COLS deleted from both copies (delete-and-depend, ADR-031 T3).
    assert new._METRICA_CONSUMED_COLS == tc_legacy._METRICA_CONSUMED_COLS
    assert new._SKILLCORNER_CONSUMED_COLS == tc_legacy._SKILLCORNER_CONSUMED_COLS
    assert new._SKILLCORNER_PERIOD_START_SECONDS == tc_legacy._SKILLCORNER_PERIOD_START_SECONDS
    assert new._GS_FRAME_RATE == ac_legacy._GS_FRAME_RATE
