"""Watermark card inputs must match actual dbt source lineage.

Rules:
  1. Every bronze table in a dbt-build card's inputs.datasets must have a
     corresponding ``source()`` call in a dbt staging SQL file.
  2. No table may appear as both input AND output in the same card (circular).
  3. Every gold-table input must be in the ref-graph ancestry of the card's
     tagged models.

Prevents the phantom-table class of watermark crash (PR #261 incident).
"""

from __future__ import annotations

import re
from pathlib import Path

import pytest
import yaml

_REPO = Path(__file__).resolve().parents[2]
_CARDS_DIR = _REPO / "workflow-cards"
_DBT_MODELS_DIR = _REPO / "dbt_project" / "models"
_DBT_SEEDS_DIR = _REPO / "dbt_project" / "seeds"

_FRONTMATTER = re.compile(r"^---\s*\n(.*?\n)---\s*\n", re.DOTALL)
# Matches {{ source('schema', 'table_name') }}
_SOURCE_RE = re.compile(r"\{\{\s*source\(\s*'(\w+)'\s*,\s*'(\w+)'\s*\)")
# Matches {{ ref('model_name') }}
_REF_RE = re.compile(r"\{\{\s*ref\(\s*'(\w+)'\s*\)")
# Matches tags=['marts', 'output_mart'] in config blocks
_TAGS_RE = re.compile(r"tags\s*=\s*\[([^\]]+)\]")

_DBT_BUILD_CARDS = [
    "wf-dbt-build-input-marts",
    "wf-dbt-build-intermediate-marts",
    "wf-dbt-build-output-marts",
]

# Card ID -> set of dbt tags that select models for this stage
# Mirrors _SELECTOR_TO_CARD in dbt_runner.py:59-63
_CARD_TAG_SETS: dict[str, set[str]] = {
    "wf-dbt-build-input-marts": {"input_mart", "dimension"},
    "wf-dbt-build-intermediate-marts": {"intermediate_mart"},
    "wf-dbt-build-output-marts": {"output_mart"},
}


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _load_card(card_id: str) -> dict:
    path = _CARDS_DIR / f"{card_id}.yaml"
    text = path.read_text(encoding="utf-8")
    m = _FRONTMATTER.match(text)
    if not m:
        pytest.fail(f"Card {card_id} has no YAML frontmatter")
    return yaml.safe_load(m.group(1))


def _card_bronze_tables(card: dict) -> set[str]:
    """Extract bronze table names from card inputs (strip catalog.bronze. prefix)."""
    tables: set[str] = set()
    inputs = card.get("inputs", {})
    for section in ("tables", "datasets"):
        for entry in inputs.get(section, []):
            if entry.get("source") != "delta-table":
                continue
            table_id: str = entry["id"]
            # Only check bronze tables -- gold refs (dim_*, fct_*) are cross-stage
            if ".bronze." in table_id:
                tables.add(table_id.rsplit(".", 1)[-1])
    return tables


def _card_gold_tables(card: dict) -> set[str]:
    """Extract gold/mart table names from card inputs (those using {schema} placeholder)."""
    tables: set[str] = set()
    inputs = card.get("inputs", {})
    for section in ("tables", "datasets"):
        for entry in inputs.get(section, []):
            if entry.get("source") != "delta-table":
                continue
            table_id: str = entry["id"]
            # Gold tables use {schema} placeholder, bronze uses .bronze.
            if "{schema}" in table_id and ".bronze." not in table_id:
                tables.add(table_id.rsplit(".", 1)[-1])
    return tables


def _card_output_tables(card: dict) -> set[str]:
    """Extract output table names from card outputs."""
    tables: set[str] = set()
    outputs = card.get("outputs", {})
    for entry in outputs.get("tables", []):
        table_id: str = entry["id"]
        tables.add(table_id.rsplit(".", 1)[-1])
    return tables


def _all_dbt_source_tables() -> set[str]:
    """Scan all dbt model SQL for source() calls, return set of table names.

    Scans all models (staging, intermediate, marts) because some models
    have direct source() calls outside staging (e.g. int_player_xref reads
    player_xref_raw, fct_goalkeeper_stats reads expected_threat_grids).
    """
    tables: set[str] = set()
    for sql_file in _DBT_MODELS_DIR.rglob("*.sql"):
        content = sql_file.read_text(encoding="utf-8")
        for _schema, table in _SOURCE_RE.findall(content):
            tables.add(table)
    return tables


def _build_ref_graph() -> dict[str, set[str]]:
    """Build upstream ref graph from dbt SQL files.

    Returns:
        ref_parents[model] = set of model names this model ref()'s
    """
    ref_parents: dict[str, set[str]] = {}

    for sql_file in _DBT_MODELS_DIR.rglob("*.sql"):
        model = sql_file.stem
        content = sql_file.read_text(encoding="utf-8")
        ref_parents[model] = set(_REF_RE.findall(content))

    # Seeds are valid ref targets but have no upstream dependencies
    for csv_file in _DBT_SEEDS_DIR.glob("*.csv"):
        ref_parents.setdefault(csv_file.stem, set())

    return ref_parents


def _models_with_tags(tags: set[str]) -> set[str]:
    """Find all mart models that have any of the given tags."""
    tagged: set[str] = set()
    marts_dir = _DBT_MODELS_DIR / "marts"
    for sql_file in marts_dir.glob("*.sql"):
        content = sql_file.read_text(encoding="utf-8")
        m = _TAGS_RE.search(content)
        if m:
            model_tags = {t.strip().strip("'\"") for t in m.group(1).split(",")}
            if model_tags & tags:
                tagged.add(sql_file.stem)
    return tagged


def _walk_upstream(
    models: set[str],
    ref_parents: dict[str, set[str]],
) -> set[str]:
    """Recursively walk upstream through ref graph, return all ancestor models."""
    visited: set[str] = set()
    stack = list(models)
    while stack:
        node = stack.pop()
        if node in visited:
            continue
        visited.add(node)
        for parent in ref_parents.get(node, set()):
            if parent not in visited:
                stack.append(parent)
    return visited


# ---------------------------------------------------------------------------
# Test classes
# ---------------------------------------------------------------------------


class TestWatermarkCardBronzeInputsExist:
    """Every bronze table in a dbt-build card must have a dbt source() consumer."""

    @pytest.mark.parametrize("card_id", _DBT_BUILD_CARDS)
    def test_bronze_inputs_have_dbt_source(self, card_id: str) -> None:
        card = _load_card(card_id)
        bronze_in_card = _card_bronze_tables(card)
        dbt_sources = _all_dbt_source_tables()

        missing = bronze_in_card - dbt_sources
        assert not missing, (
            f"Card {card_id} lists bronze tables with no dbt source() call: {sorted(missing)}. "
            f"These have no dbt source() consumer and should not be in the watermark card."
        )


class TestWatermarkCardNoCircularRefs:
    """No table may appear as both input AND output in the same card."""

    @pytest.mark.parametrize("card_id", _DBT_BUILD_CARDS)
    def test_no_circular_input_output(self, card_id: str) -> None:
        card = _load_card(card_id)
        inputs = card.get("inputs", {})
        input_ids: set[str] = set()
        for section in ("tables", "datasets"):
            for entry in inputs.get(section, []):
                if entry.get("source") == "delta-table":
                    input_ids.add(entry["id"].rsplit(".", 1)[-1])

        output_ids = _card_output_tables(card)
        circular = input_ids & output_ids
        assert not circular, (
            f"Card {card_id} has tables in both inputs AND outputs: {sorted(circular)}. "
            f"This defeats the watermark skip guard (table always appears 'changed')."
        )


class TestWatermarkCardGoldInputLineage:
    """Gold-table inputs in dbt-build cards must be in the tag set's ref-graph ancestry.

    Builds the full ref() graph from dbt SQL, walks upstream from tagged models,
    and verifies every gold-table card input appears in the ancestor set.
    Catches extras like dim_competitions in the intermediate-marts card
    (which only builds fct_action_values, which does not ref dim_competitions).
    """

    @pytest.mark.parametrize("card_id", _DBT_BUILD_CARDS)
    def test_gold_inputs_in_tag_lineage(self, card_id: str) -> None:
        tags = _CARD_TAG_SETS.get(card_id)
        if tags is None:
            pytest.skip(f"No tag set defined for {card_id}")

        card = _load_card(card_id)
        gold_in_card = _card_gold_tables(card)
        if not gold_in_card:
            pytest.skip(f"Card {card_id} has no gold-table inputs")

        ref_parents = _build_ref_graph()
        tagged_models = _models_with_tags(tags)
        assert tagged_models, f"No models found with tags {tags}"

        ancestors = _walk_upstream(tagged_models, ref_parents)

        not_in_lineage = gold_in_card - ancestors
        assert not not_in_lineage, (
            f"Card {card_id} lists gold tables not in the ref-graph ancestry of "
            f"models tagged {tags}: {sorted(not_in_lineage)}. "
            f"These cause unnecessary watermark rebuilds."
        )
