"""AC-1 — every driver-side createDataFrame to the action-context table must pass an explicit schema.

Regression guard for the DELTA_FAILED_TO_MERGE_FIELDS bug found by the first
serverless wyscout run (2026-05-31): ``_process_event_only_match`` and
``_process_statsbomb_match`` called ``spark.createDataFrame(out_pdf)`` with NO
schema. Event-only / sb360 output leaves the ~80 tracking columns entirely
absent, so ``build_output`` fills them via ``out[col] = np.nan`` → all-NULL
float64 pandas columns. Spark infers those as DoubleType, which then collides
with the table's BIGINT columns (e.g. ``frame_id``) under write_delta_table's
``mergeSchema`` → ``[DELTA_FAILED_TO_MERGE_FIELDS] Failed to merge fields
'frame_id' and 'frame_id'``. The applyInPandas tracking path was always safe
because it passes ``schema=_get_result_schema()``.

This test is AST-based (no Spark needed — pyspark is not installed locally) so
it runs in offline CI. It asserts that EVERY ``spark.createDataFrame(...)`` call
inside ``src/ingestion/action_context.py`` passes a ``schema=`` keyword
argument. See ADR-033.
"""

from __future__ import annotations

import ast
from pathlib import Path

_MODULE_PATH = Path(__file__).resolve().parents[1] / "ingestion" / "action_context.py"


def _createdataframe_calls(tree: ast.Module) -> list[ast.Call]:
    """Return every ``<something>.createDataFrame(...)`` Call node in the module."""
    calls: list[ast.Call] = []
    for node in ast.walk(tree):
        if isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute) and node.func.attr == "createDataFrame":
            calls.append(node)
    return calls


def test_module_has_createdataframe_calls() -> None:
    """Guardrail: the AST walk actually finds the driver-side writers.

    If this drops to zero, the writers were renamed/removed and the schema
    assertion below would vacuously pass — fail loudly instead.
    """
    tree = ast.parse(_MODULE_PATH.read_text(encoding="utf-8"))
    calls = _createdataframe_calls(tree)
    assert len(calls) >= 2, (
        f"Expected >=2 createDataFrame calls in {_MODULE_PATH.name} "
        f"(_process_event_only_match + _process_statsbomb_match), found {len(calls)}"
    )


def test_every_createdataframe_passes_explicit_schema() -> None:
    """Every createDataFrame in action_context.py must pass schema=<...>.

    Without an explicit schema, Spark infers all-NULL tracking columns as
    DoubleType and the write fails to merge into the BIGINT table columns.
    """
    tree = ast.parse(_MODULE_PATH.read_text(encoding="utf-8"))
    offenders: list[int] = []
    for call in _createdataframe_calls(tree):
        kwargs = {kw.arg for kw in call.keywords if kw.arg is not None}
        # Accept either the schema= kwarg or a 2nd positional arg (schema).
        has_schema = "schema" in kwargs or len(call.args) >= 2
        if not has_schema:
            offenders.append(call.lineno)
    assert not offenders, (
        "createDataFrame() without an explicit schema in action_context.py at "
        f"line(s) {offenders}. Pass schema=_get_result_schema() — see ADR-033 "
        "(all-NULL tracking columns infer as DoubleType and fail to merge into "
        "the BIGINT table columns, raising DELTA_FAILED_TO_MERGE_FIELDS)."
    )
