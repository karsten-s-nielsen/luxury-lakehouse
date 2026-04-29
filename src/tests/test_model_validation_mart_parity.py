"""Static parity tests between `ingestion.model_validation` Spark column
references and the dbt mart contract YAML.

Motivation: a single-character column drift between `fct_xg_predictions.xg_logistic`
(dbt mart) and `model_validation.py:select("xg_prediction")` was masked by
silent-swallow exception handling for an unknown duration; surfaced cleanly
only after PR #122 narrowed exception suppression to `tolerate_missing_table()`.
The bug then cascaded into PR-LL2 close-out (job 887419551716059, dbt_build
SKIPPED via UPSTREAM_FAILED).

This test pins the contract: for every `_validate_*` function that reads
`{catalog}.dev_gold.<mart>` via `spark.table(...).select(<col>...)`, the
selected columns MUST exist in the mart's contract block in
`dbt_project/models/marts/_marts__models.yml`. Catches drift at lint time,
not deploy time.

If you add or rename a column in a `_validate_*` reader, update the parity
table below.
"""

from __future__ import annotations

from pathlib import Path

import pytest

# Mart -> required columns referenced by the matching `_validate_*` function.
# Mapping is checked against `_marts__models.yml`'s `columns:` block for that
# mart. Source of truth is `src/ingestion/model_validation.py`; this table
# mirrors it. Update both together.
_VALIDATOR_MART_COLUMNS: dict[str, tuple[str, ...]] = {
    "fct_xg_predictions": ("xg_gradient_boosted",),
    "fct_action_values": ("vaep_value",),
    "fct_physical_stats": ("max_speed_ms",),
    "fct_pausa_values": ("temporal_judgment", "spatial_selection"),
}

_MARTS_YML = Path("dbt_project/models/marts/_marts__models.yml")


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
            f"column {col!r}, but `ingestion.model_validation` reads it via "
            f"`spark.table(...).select({col!r})`. Either:\n"
            f"  (a) restore the column in dbt_project/models/marts/{mart}.sql + _marts__models.yml, or\n"
            f"  (b) update the column reference in src/ingestion/model_validation.py and "
            f"this parity table."
        )


def test_model_validation_references_match_parity_table() -> None:
    """The columns this test pins MUST match the actual `select(...)` calls in
    `model_validation.py`. Prevents silent table-skew where a validator reads
    column X but the parity table only checks Y (which would let X-drift sneak
    through under a green test)."""
    src = Path("src/ingestion/model_validation.py").read_text(encoding="utf-8")
    for mart, cols in _VALIDATOR_MART_COLUMNS.items():
        # Locate the `table = f"{catalog}.{DEFAULT_GOLD_SCHEMA}.<mart>"` line and
        # the `.select(...)` call that follows. We grep the literal mart name in
        # an f-string and the literal column names — keeps the test simple and
        # readable without an AST walk.
        marker = f'.{mart}"'
        assert marker in src, (
            f"model_validation.py no longer references mart {mart!r} via f-string. "
            f"Either remove {mart!r} from _VALIDATOR_MART_COLUMNS or restore the validator."
        )
        for col in cols:
            quoted = f'"{col}"'
            assert quoted in src, (
                f"model_validation.py does not reference column {col!r} (expected for mart "
                f"{mart!r}). The parity table is out of sync with the source."
            )
