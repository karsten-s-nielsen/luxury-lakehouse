"""AC-1 — every distributed UDF dispatch to the action-context table must pass an explicit schema.

Regression guard for the DELTA_FAILED_TO_MERGE_FIELDS / Arrow-cast class: the sb360 output leaves the
~80 tracking columns absent, so ``build_output`` fills them via ``out[col] = np.nan`` → all-NULL
float64 pandas columns. Without an explicit schema, Spark/Arrow infers those as DoubleType, which
collides with the table's BIGINT columns (e.g. ``frame_id``).

Originally this guarded the driver-side ``spark.createDataFrame(out_pdf)`` in
``_process_statsbomb_match`` (the 2026-05-31 wyscout bug). ADR-058 removed that per-match driver path:
statsbomb now writes via ``cogroup.applyInPandas`` and tracking via ``mapInPandas`` — both must pass
``schema=_get_result_schema()`` (cogroup's Arrow conversion has NO intervening createDataFrame
coercion, so the explicit schema is the only protection — see the live check in test_sb360_cogroup +
ADR-058 Task 8). This test now guards the surviving distributed writers.

AST-based (no Spark — pyspark is not installed locally), so it runs in offline CI. It asserts EVERY
``.applyInPandas(...)`` / ``.mapInPandas(...)`` in ``src/ingestion/action_context.py`` passes a
``schema=`` keyword. See ADR-033 + ADR-058.
"""

from __future__ import annotations

import ast
from pathlib import Path

_MODULE_PATH = Path(__file__).resolve().parents[1] / "ingestion" / "action_context.py"
_DISPATCH_METHODS = {"applyInPandas", "mapInPandas"}


def _dispatch_calls(tree: ast.Module) -> list[ast.Call]:
    """Return every ``<something>.applyInPandas(...)`` / ``.mapInPandas(...)`` Call node."""
    return [
        node
        for node in ast.walk(tree)
        if isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute) and node.func.attr in _DISPATCH_METHODS
    ]


def test_module_has_distributed_writers() -> None:
    """Guardrail: the AST walk actually finds the distributed writers (cogroup + tracking mapInPandas).

    If this drops to zero, the writers were renamed/removed and the schema assertion below would
    vacuously pass — fail loudly instead.
    """
    tree = ast.parse(_MODULE_PATH.read_text(encoding="utf-8"))
    calls = _dispatch_calls(tree)
    assert len(calls) >= 1, (
        f"Expected >=1 applyInPandas/mapInPandas call in {_MODULE_PATH.name} "
        f"(cogroup statsbomb + tracking mapInPandas; driver createDataFrame removed per ADR-058), "
        f"found {len(calls)}"
    )


def test_every_distributed_writer_passes_explicit_schema() -> None:
    """Every applyInPandas/mapInPandas in action_context.py must pass schema=<...>.

    cogroup.applyInPandas Arrow-converts the returned frame to the declared schema directly (no
    createDataFrame coercion), so the explicit schema is the ONLY guard against all-NULL float64
    tracking columns failing to cast into the BIGINT table columns (ADR-033/ADR-058).
    """
    tree = ast.parse(_MODULE_PATH.read_text(encoding="utf-8"))
    offenders: list[int] = []
    for call in _dispatch_calls(tree):
        kwargs = {kw.arg for kw in call.keywords if kw.arg is not None}
        # Accept the schema= kwarg or a 2nd positional arg (func, schema).
        has_schema = "schema" in kwargs or len(call.args) >= 2
        if not has_schema:
            offenders.append(call.lineno)
    assert not offenders, (
        "applyInPandas/mapInPandas without an explicit schema in action_context.py at "
        f"line(s) {offenders}. Pass schema=_get_result_schema() — see ADR-033/ADR-058 (all-NULL "
        "tracking columns infer as DoubleType and fail to cast into the BIGINT table columns)."
    )


def test_new_gk_and_xshot_columns_present() -> None:
    """xShotOccurrence + gk_influence near/far zones + pitch_control_method are in the
    DDL single source (and thus the derived StructType) and RESULT_COLUMNS."""
    from analytics.action_context.schema import ACTION_CONTEXT_DDL, RESULT_COLUMNS

    new_cols = [
        "gk_closing_time_mean_s__near_post",
        "gk_closing_time_min_s__near_post",
        "gk_closing_time_mean_s__far_post",
        "gk_closing_time_min_s__far_post",
        "xshot_occurrence",
        "pitch_control_method",
        "ghost_gk_method",
    ]
    for c in new_cols:
        assert c in RESULT_COLUMNS, f"{c} missing from RESULT_COLUMNS"
        assert c in ACTION_CONTEXT_DDL, f"{c} missing from ACTION_CONTEXT_DDL"
    assert "pitch_control_method STRING" in ACTION_CONTEXT_DDL
    assert "xshot_occurrence DOUBLE" in ACTION_CONTEXT_DDL
    assert "ghost_gk_method STRING" in ACTION_CONTEXT_DDL
