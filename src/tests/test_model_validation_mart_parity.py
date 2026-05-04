"""Static parity tests between `ingestion.model_validation` Spark column
references and the dbt mart contract YAML.

Motivation: PR-LL2 close-out (job 887419551716059) was blocked by two
distinct column drifts in `model_validation.py`:

  1. `fct_xg_predictions.xg_prediction` → `xg_gradient_boosted` (PR #225)
     — masked by silent-swallow `except Exception` until PR #122 narrowed
     suppression to `tolerate_missing_table()`.
  2. `fct_passes.match_id` → `match_key` (Kimball-PR7 / ADR-011 rename)
     — surfaced AFTER (1) was fixed and the validator continued past xg
     to line-breaking.

This test pins the contract: for every `_validate_*` function that reads
`{catalog}.dev_gold.<mart>` via `spark.table(...)`, the columns it
selects MUST exist in the mart's contract block in
`dbt_project/models/marts/_marts__models.yml`. Catches drift at lint
time, not deploy time.

Three layers of defense:

- `test_validator_columns_exist_in_mart_contract` (parametrized) —
  every parity-table column appears in the YAML contract block for its
  mart. Catches: column renamed in dbt mart, validator unaware.

- `test_model_validation_references_match_parity_table` — the parity
  table's columns appear as literals in `model_validation.py` source.
  Catches: validator updated to a new column name, parity table left
  stale (silent green).

- `test_parity_table_covers_every_spark_table_call` (AST-based) —
  every `spark.table(<f-string-with-mart>)` call in `model_validation.py`
  has a matching key in `_VALIDATOR_MART_COLUMNS`. Catches: a NEW
  validator added for a NEW mart with no parity entry (the gap that
  let `fct_passes` slip through PR #225).

If you add or rename a column in a `_validate_*` reader, update the
parity table below — and the AST check will tell you immediately if
you've forgotten to register a new mart.
"""

from __future__ import annotations

import ast
from pathlib import Path

import pytest

# Mart -> required columns referenced by the matching `_validate_*` function.
# Mapping is checked against `_marts__models.yml`'s `columns:` block for that
# mart. Source of truth is `src/ingestion/model_validation.py`; this table
# mirrors it. Update both together.
_VALIDATOR_MART_COLUMNS: dict[str, tuple[str, ...]] = {
    # fct_xg_predictions (v1) retired SK3-MIG-B 2026-05-03 per ADR-023.
    "fct_action_values": ("vaep_value",),
    "fct_passes": ("match_key", "is_line_breaking"),
    "fct_physical_stats": ("max_speed_ms",),
    "fct_pausa_values": ("temporal_judgment", "spatial_selection"),
}

_MARTS_YML = Path("dbt_project/models/marts/_marts__models.yml")
_MODEL_VALIDATION_PY = Path("src/ingestion/model_validation.py")


def _extract_mart_block(yml_text: str, mart_name: str) -> str:
    """Return the substring of `_marts__models.yml` covering `mart_name`'s
    block (from `- name: <mart>` up to the next top-level `- name:` entry).
    """
    needle = f"  - name: {mart_name}\n"
    start = yml_text.find(needle)
    if start == -1:
        raise AssertionError(f"_marts__models.yml has no entry for {mart_name!r}")
    # Next top-level mart entry begins with two spaces + "- name: " at column 2.
    next_idx = yml_text.find("\n  - name: ", start + len(needle))
    return yml_text[start : next_idx if next_idx != -1 else len(yml_text)]


def _extract_marts_referenced_in_source(src: str) -> set[str]:
    """Walk the AST of `model_validation.py` and return every mart name
    appearing in a `spark.table(<f-string>)` call within `_validate_*`
    functions.

    The expected pattern in source is:

        table = f"{catalog}.{DEFAULT_GOLD_SCHEMA}.<mart>"
        ...
        spark.table(table)

    or directly:

        spark.table(f"{catalog}.{DEFAULT_GOLD_SCHEMA}.<mart>")

    We resolve both shapes by tracking f-string assignments to local
    `table` variables and by inspecting f-strings passed directly to
    `spark.table(...)`. The mart name is the trailing
    dotted-identifier segment of the f-string.
    """
    tree = ast.parse(src)
    marts: set[str] = set()

    def _mart_from_fstring(node: ast.AST) -> str | None:
        # An f-string is parsed as ast.JoinedStr whose .values mix
        # ast.Constant (literal) and ast.FormattedValue (interpolation).
        # We need the trailing identifier after the last "." in literal
        # constant segments.
        if not isinstance(node, ast.JoinedStr):
            return None
        # Concatenate literal text segments only (drop FormattedValues).
        literal = "".join(v.value for v in node.values if isinstance(v, ast.Constant) and isinstance(v.value, str))
        if "." not in literal:
            return None
        # The mart name is the trailing dotted segment, e.g.
        # ".dev_gold.fct_passes" or ".{DEFAULT_GOLD_SCHEMA}.fct_passes" both
        # leave the mart as the substring after the final ".".
        candidate = literal.rsplit(".", 1)[-1].strip()
        # Mart names are snake_case identifiers — reject if it doesn't look
        # like one (defensive for unusual patterns).
        if candidate and candidate.replace("_", "").isalnum():
            return candidate
        return None

    for func_node in ast.walk(tree):
        if not isinstance(func_node, ast.FunctionDef) or not func_node.name.startswith("_validate_"):
            continue

        # Track local `table = f"..."` assignments inside this validator.
        local_table_marts: dict[str, str] = {}
        for node in ast.walk(func_node):
            if isinstance(node, ast.Assign):
                for target in node.targets:
                    if isinstance(target, ast.Name):
                        mart = _mart_from_fstring(node.value)
                        if mart:
                            local_table_marts[target.id] = mart

        # Find every `spark.table(...)` call.
        for node in ast.walk(func_node):
            if not isinstance(node, ast.Call):
                continue
            func = node.func
            if not (
                isinstance(func, ast.Attribute)
                and func.attr == "table"
                and isinstance(func.value, ast.Name)
                and func.value.id == "spark"
            ):
                continue
            if not node.args:
                continue
            arg = node.args[0]
            # Direct f-string: spark.table(f"{cat}.{schema}.fct_x")
            mart = _mart_from_fstring(arg)
            if mart:
                marts.add(mart)
                continue
            # Variable reference: spark.table(table)
            if isinstance(arg, ast.Name) and arg.id in local_table_marts:
                marts.add(local_table_marts[arg.id])

    return marts


@pytest.mark.parametrize(
    ("mart", "expected_columns"),
    [(m, cols) for m, cols in _VALIDATOR_MART_COLUMNS.items()],
)
def test_validator_columns_exist_in_mart_contract(mart: str, expected_columns: tuple[str, ...]) -> None:
    """Every column read by `_validate_*` must appear in the mart's contract block."""
    yml = _MARTS_YML.read_text(encoding="utf-8")
    block = _extract_mart_block(yml, mart)
    for col in expected_columns:
        needle = f"      - name: {col}\n"
        assert needle in block, (
            f"\n[model_validation parity] dbt mart {mart!r} contract block is missing "
            f"column {col!r}, but `ingestion.model_validation` reads it. Either:\n"
            f"  (a) restore the column in dbt_project/models/marts/{mart}.sql + _marts__models.yml, or\n"
            f"  (b) update the column reference in src/ingestion/model_validation.py and "
            f"this parity table."
        )


def test_model_validation_references_match_parity_table() -> None:
    """The columns this test pins MUST match the actual references in
    `model_validation.py` source. Prevents silent table-skew where a
    validator reads column X but the parity table only checks Y (which
    would let X-drift sneak through under a green test).
    """
    src = _MODEL_VALIDATION_PY.read_text(encoding="utf-8")
    for mart, cols in _VALIDATOR_MART_COLUMNS.items():
        # The mart name must appear in an f-string somewhere; both shapes
        # `f"{catalog}.{DEFAULT_GOLD_SCHEMA}.<mart>"` and `f"...{schema}.<mart>"`
        # leave the literal `.<mart>"` substring in source.
        marker = f".{mart}"
        assert marker in src, (
            f"model_validation.py no longer references mart {mart!r}. "
            f"Either remove {mart!r} from _VALIDATOR_MART_COLUMNS or restore the validator."
        )
        for col in cols:
            quoted = f'"{col}"'
            assert quoted in src, (
                f"model_validation.py does not reference column {col!r} (expected for mart "
                f"{mart!r}). The parity table is out of sync with the source."
            )


def test_parity_table_covers_every_spark_table_call() -> None:
    """Every `spark.table(...)` call in `_validate_*` functions must read a
    mart that is registered in `_VALIDATOR_MART_COLUMNS`. Closes the
    "validator added without parity entry" gap that let the
    `fct_passes.match_id` → `match_key` drift through PR #225.
    """
    src = _MODEL_VALIDATION_PY.read_text(encoding="utf-8")
    referenced_marts = _extract_marts_referenced_in_source(src)
    registered_marts = set(_VALIDATOR_MART_COLUMNS.keys())

    missing_from_table = referenced_marts - registered_marts
    assert not missing_from_table, (
        f"\n[model_validation parity] AST scan found `spark.table(...)` calls reading "
        f"marts not registered in _VALIDATOR_MART_COLUMNS: {sorted(missing_from_table)}.\n"
        f"Every validator MUST register its mart + selected columns so future column "
        f"drift is caught at lint time. Add an entry to _VALIDATOR_MART_COLUMNS in "
        f"this file."
    )

    stale_in_table = registered_marts - referenced_marts
    assert not stale_in_table, (
        f"\n[model_validation parity] _VALIDATOR_MART_COLUMNS has entries that do NOT "
        f"correspond to any `spark.table(...)` call in model_validation.py: "
        f"{sorted(stale_in_table)}.\n"
        f"Either restore the validator or remove the stale parity entry."
    )
