# Watermark Card Input Accuracy Fix

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Fix phantom, circular, and extra entries in the three dbt-build workflow cards that cause watermark guard crashes and unnecessary rebuilds.

**Architecture:** The three `wf-dbt-build-*-marts.yaml` workflow cards list upstream Delta tables for the watermark skip-guard (`check_upstream_freshness` / `record_watermarks` in `src/ingestion/guards.py`). PR #261 expanded these lists but introduced phantom table names, a circular self-reference, and several extras. `record_watermarks` crashes on phantom tables because `DESCRIBE HISTORY` throws `AnalysisException` for non-existent tables, and unlike `check_upstream_freshness`, it has no try/except. The dbt build succeeds but the task is reported as FAILED. This plan fixes the card data only (Option B: keep loud failures, fix the data).

**Tech Stack:** YAML workflow cards, pytest, `bump_wheel.py`

---

## Context: How the watermark system works

1. `dbt_runner.py:main()` maps the `--select` tags to a card ID via `_SELECTOR_TO_CARD` (line 59-63)
2. Pre-build: `check_upstream_freshness()` (line 441 in guards.py) reads the card, does `DESCRIBE HISTORY` on each listed table, compares to stored watermarks. Has try/except (line 464) -- fails open on errors.
3. Post-build: `record_watermarks()` (line 484) does the same `DESCRIBE HISTORY` calls but has NO try/except. A phantom table name crashes the entire task even though dbt succeeded.
4. `resolve_upstream_tables_from_card()` (line 552) reads `inputs.datasets` entries where `source == "delta-table"`, substitutes `{catalog}` and `{schema}` placeholders, returns FQN list.

## Context: Three-stage dbt build topology (ADR-019)

| Stage | Card | Selector | Builds |
|-------|------|----------|--------|
| 1 | `wf-dbt-build-input-marts` | `+tag:input_mart +tag:dimension` | 4 dims + 3 input_marts + ancestors |
| 2 | `wf-dbt-build-intermediate-marts` | `+tag:intermediate_mart` | 1 intermediate_mart + ancestors |
| 3 | `wf-dbt-build-output-marts` | `tag:output_mart` | 32 output_marts (no ancestors) |

The `+` prefix means "include upstream ancestors." Stage 3 omits `+` because stages 1+2 already built all ancestors.

## Root cause evidence

The `dbt_build_output_marts` task fails with:
```
TABLE_OR_VIEW_NOT_FOUND: soccer_analytics.bronze.formations_efpi_results
```
This error comes from `record_watermarks` → `_get_latest_data_version` → `DESCRIBE HISTORY` (guards.py:495 → 428). The dbt build itself succeeded -- all 32 output marts were written.

---

## File Structure

| File | Action | Purpose |
|------|--------|---------|
| `workflow-cards/wf-dbt-build-output-marts.yaml` | Modify | Remove 3 wrong entries, add 4 missing entries |
| `workflow-cards/wf-dbt-build-intermediate-marts.yaml` | Modify | Remove 4 wrong entries, keep 4 correct entries |
| `workflow-cards/wf-dbt-build-input-marts.yaml` | Modify | Remove 4 extras, add 1 missing entry |
| `src/tests/test_watermark_card_lineage.py` | Create | Regression test: card inputs match dbt source lineage |

---

**Execution order:** Task 4 (write tests) → Task 4 Step 2 (RED verification) → Task 5 (apply card fixes from Tasks 1-3, GREEN verification) → Task 6 (version bump). Tasks 1-3 define WHAT to change with evidence tables; Task 5 applies them.

---

### Task 1: Fix stage 3 card (output-marts) -- the crash

This is the highest priority -- it's the one causing the runtime crash.

**Files:**
- Modify: `workflow-cards/wf-dbt-build-output-marts.yaml:26-61`

**Verified findings (every entry traced to dbt source/staging SQL):**

| Current entry | Verdict | Evidence |
|---------------|---------|----------|
| `line_breaking_results` | CORRECT | `stg_line_breaking__results.sql` source |
| `pitch_control_values` | CORRECT | `stg_pitch_control__values.sql` source |
| `off_ball_xt_results` | CORRECT | `stg_off_ball_xt__results.sql` source |
| `defcon_results` | CORRECT | `stg_defcon__results.sql` source |
| `expected_threat_grids` | CORRECT | `stg_expected_threat__grids.sql` + `fct_goalkeeper_stats.sql:78` direct source |
| `formations_efpi_results` | **PHANTOM** | No bronze table, no dbt source. Actual table: `formation_labels` (`stg_formations__labels.sql:12`) |
| `formations_shape_graph_results` | **PHANTOM** | No bronze table, no dbt source. Actual table: `player_positions` (`stg_shape_graphs__positions.sql:11`) |
| `elastic_sync_results` | **EXTRA** | `stg_idsse__elastic_sync.sql` exists but 0 ref() consumers in any mart |
| `pausa_values` | CORRECT | `stg_pausa__values.sql` source |
| `player_embeddings_raw` | CORRECT | `stg_embeddings__raw.sql` source |
| `xg_predictions_v2` | CORRECT | `stg_xg__predictions_v2.sql` source |
| *(missing)* `formation_labels` | **ADD** | `stg_formations__labels.sql:12` → `fct_formation_labels` |
| *(missing)* `player_positions` | **ADD** | `stg_shape_graphs__positions.sql:11` → `fct_position_maps`, `fct_tracking_shape_timeline` |
| *(missing)* `space_creation_values` | **ADD** | `stg_space_creation__values.sql:4` → `fct_space_creation` |
| *(missing)* `psxg_predictions` | **ADD** | `stg_psxg__predictions.sql:9` → `fct_goalkeeper_stats:359` |

- [ ] **Step 1: Edit the inputs.datasets section**

Replace lines 43-51 (the three wrong entries) and add the four missing entries. The final `inputs.datasets` section should be:

```yaml
inputs:
  datasets:
    - id: "{catalog}.bronze.line_breaking_results"
      source: delta-table
      description: "Line-breaking detection results"
    - id: "{catalog}.bronze.pitch_control_values"
      source: delta-table
      description: "Pitch control computation results"
    - id: "{catalog}.bronze.off_ball_xt_results"
      source: delta-table
      description: "Off-ball expected threat results"
    - id: "{catalog}.bronze.defcon_results"
      source: delta-table
      description: "DEFCON defensive contribution results"
    - id: "{catalog}.bronze.expected_threat_grids"
      source: delta-table
      description: "Expected threat grid values"
    - id: "{catalog}.bronze.formation_labels"
      source: delta-table
      description: "Formation labels from formations pipeline"
    - id: "{catalog}.bronze.player_positions"
      source: delta-table
      description: "Player positions from shape graph pipeline"
    - id: "{catalog}.bronze.space_creation_values"
      source: delta-table
      description: "Space creation values from elastic sync pipeline"
    - id: "{catalog}.bronze.psxg_predictions"
      source: delta-table
      description: "Post-shot xG predictions"
    - id: "{catalog}.bronze.pausa_values"
      source: delta-table
      description: "PAUSA values from OBSO computation"
    - id: "{catalog}.bronze.player_embeddings_raw"
      source: delta-table
      description: "Player embeddings (v1 + v2)"
    - id: "{catalog}.bronze.xg_predictions_v2"
      source: delta-table
      description: "xG v2 deep-sets predictions"
```

Net change: 12 entries (was 11, removed 3, added 4).

---

### Task 2: Fix stage 2 card (intermediate-marts)

**Files:**
- Modify: `workflow-cards/wf-dbt-build-intermediate-marts.yaml:24-51`

**Verified findings (traced via `fct_action_values.sql` refs):**

`fct_action_values.sql` is the ONLY intermediate_mart. Its actual refs:
- `ref('stg_spadl__action_values')` (which reads `source('spadl', 'vaep_action_values')`)
- `ref('int_running_score')` (intermediate, feeds from staging)
- `ref('dim_matches')` (line 67)
- `ref('dim_teams')` (lines 214, 226)
- `ref('dim_players')` (line 219)

It does NOT ref: `fct_shots`, `dim_competitions`, `spadl_actions`.

| Current entry | Verdict | Evidence |
|---------------|---------|----------|
| `dim_competitions` | **EXTRA** | Not referenced by `fct_action_values.sql` |
| `dim_matches` | CORRECT | `fct_action_values.sql:67` |
| `dim_players` | CORRECT | `fct_action_values.sql:219` |
| `dim_teams` | CORRECT | `fct_action_values.sql:214,226` |
| `fct_shots` | **EXTRA** | Not referenced by `fct_action_values.sql` |
| `fct_action_values` | **CIRCULAR** | Listed as BOTH input AND output -- defeats skip guard (always sees "changed") |
| `spadl_actions` | **EXTRA** | Bronze table exists but is not in `fct_action_values` lineage. No `source('spadl', 'spadl_actions')` call in any staging SQL. The actual source consumed is `vaep_action_values` via `source('spadl', 'vaep_action_values')` in `stg_spadl__action_values.sql`. Won't crash (table exists) but tracks an irrelevant table. |
| `vaep_action_values` | CORRECT | `stg_spadl__action_values.sql` reads `source('spadl', 'vaep_action_values')` |

- [ ] **Step 1: Update the stale header comment**

Line 19 currently says:
```
# bronze.{spadl_actions, vaep_action_values} written by compute_spadl_vaep,
```

Replace with:
```
# bronze.vaep_action_values written by compute_spadl_vaep,
```

- [ ] **Step 2: Replace the inputs.datasets section**

The final `inputs.datasets` section should be:

```yaml
inputs:
  datasets:
    # Stage 1 gold outputs consumed by fct_action_values
    - id: "{catalog}.{schema}.dim_matches"
      source: delta-table
      description: "Match dimension (fct_action_values.sql:67)"
    - id: "{catalog}.{schema}.dim_players"
      source: delta-table
      description: "Player dimension (fct_action_values.sql:219)"
    - id: "{catalog}.{schema}.dim_teams"
      source: delta-table
      description: "Team dimension (fct_action_values.sql:214,226)"
    # Compute bronze tables consumed by intermediate staging models
    - id: "{catalog}.bronze.vaep_action_values"
      source: delta-table
      description: "VAEP action values from compute_spadl_vaep"
```

Net change: 4 entries (was 8, removed 4).

---

### Task 3: Fix stage 1 card (input-marts)

**Files:**
- Modify: `workflow-cards/wf-dbt-build-input-marts.yaml:23-72`

**Verified findings (traced via `source()` calls in staging SQL consumed by dim/input_mart models):**

| Current entry | Verdict | Evidence |
|---------------|---------|----------|
| `statsbomb_events` | CORRECT | `stg_statsbomb__events.sql` → multiple dim/input_mart ancestors |
| `statsbomb_360` | **EXTRA** | `stg_statsbomb__360.sql:17` reads it, but 0 models `ref()` that staging model. Dead-end. |
| `statsbomb_lineups` | CORRECT | `stg_statsbomb__lineups.sql` → dim_players lineage |
| `statsbomb_competitions` | **EXTRA** | Source defined in `_statsbomb__sources.yml:50` but 0 `source('statsbomb', 'statsbomb_competitions')` calls anywhere. No staging model exists (`stg_statsbomb__competitions.sql` does not exist). `dim_competitions.sql:38` uses `ref('stg_statsbomb__matches')`, not this source. |
| `statsbomb_matches` | CORRECT | `stg_statsbomb__matches.sql` → dim_matches, dim_competitions |
| `metrica_tracking` | CORRECT | `stg_metrica__tracking.sql` → fct_tracking_frames (input_mart) |
| `metrica_events` | **EXTRA** | Consumed by `stg_metrica__passes.sql:31` → `int_unified_passes` → `fct_passes` (output_mart) and `fct_line_breaking_results` (output_mart). NOT in lineage of any input_mart or dimension model. `stg_metrica__events.sql` has 0 ref() consumers (dead-end). |
| `idsse_tracking` | CORRECT | `stg_idsse__tracking.sql` → fct_tracking_frames (input_mart) |
| `idsse_events` | **EXTRA** | Consumed by `stg_idsse__passes.sql:52,146` → `int_unified_passes` → `fct_passes` (output_mart). `stg_idsse__events.sql` → `stg_idsse__elastic_sync` which has 0 ref() consumers (dead-end). NOT in lineage of any input_mart or dimension model. |
| `skillcorner_tracking` | CORRECT | `stg_skillcorner__tracking.sql` → fct_tracking_frames (input_mart) |
| `wyscout_events` | CORRECT | `stg_wyscout__events.sql` → dim/input_mart lineage |
| `wyscout_matches` | CORRECT | `stg_wyscout__matches.sql` → dim_matches |
| `wyscout_players` | CORRECT | `stg_wyscout__players.sql` → dim_players |
| `wyscout_teams` | CORRECT | `stg_wyscout__teams.sql` → dim_teams |
| `player_xref_raw` | CORRECT | `int_player_xref.sql` → dim_players |
| `tracking_player_metadata` | CORRECT | staging → fct_tracking_frames lineage |
| *(missing)* `team_xref_raw` | **ADD** | `int_team_xref.sql:23` → `dim_teams.sql:199,209` |

**Note for reviewer:** `metrica_events` and `idsse_events` feed output_mart models (`fct_passes`, `fct_line_breaking_results`) via `int_unified_passes`. Removing them from stage 1 is correct (they don't affect input_mart/dimension models). Whether they should be ADDED to the stage 3 card is a separate enhancement -- currently stage 3 only lists compute-output bronze tables, not provider-source bronze tables. If `metrica_events` is the ONLY table that changed, stage 3's watermark check would not detect it. This is a pre-existing design gap in the watermark system, not introduced by this fix.

- [ ] **Step 1: Replace the inputs.datasets section**

The final `inputs.datasets` section should be:

```yaml
inputs:
  datasets:
    - id: "{catalog}.bronze.statsbomb_events"
      source: delta-table
      description: "StatsBomb events"
    - id: "{catalog}.bronze.statsbomb_lineups"
      source: delta-table
      description: "StatsBomb lineups"
    - id: "{catalog}.bronze.statsbomb_matches"
      source: delta-table
      description: "StatsBomb matches"
    - id: "{catalog}.bronze.metrica_tracking"
      source: delta-table
      description: "Metrica tracking data"
    - id: "{catalog}.bronze.idsse_tracking"
      source: delta-table
      description: "IDSSE (DFL) tracking data"
    - id: "{catalog}.bronze.skillcorner_tracking"
      source: delta-table
      description: "SkillCorner tracking data"
    - id: "{catalog}.bronze.wyscout_events"
      source: delta-table
      description: "Wyscout events"
    - id: "{catalog}.bronze.wyscout_matches"
      source: delta-table
      description: "Wyscout matches"
    - id: "{catalog}.bronze.wyscout_players"
      source: delta-table
      description: "Wyscout players"
    - id: "{catalog}.bronze.wyscout_teams"
      source: delta-table
      description: "Wyscout teams"
    - id: "{catalog}.bronze.player_xref_raw"
      source: delta-table
      description: "Player cross-reference (entity resolution input)"
    - id: "{catalog}.bronze.team_xref_raw"
      source: delta-table
      description: "Team cross-reference (entity resolution input)"
    - id: "{catalog}.bronze.tracking_player_metadata"
      source: delta-table
      description: "Tracking player metadata"
```

Net change: 13 entries (was 16, removed 4, added 1).

---

### Task 4: Write ALL regression tests (before fixing cards)

Write the complete test file with all three test classes BEFORE applying any card fixes. This enables proper RED-GREEN verification: run tests against broken cards (expect failures), then fix cards, re-run (expect passes).

**Files:**
- Create: `src/tests/test_watermark_card_lineage.py`

- [ ] **Step 1: Write the complete test file**

```python
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
_DBT_STAGING_DIR = _DBT_MODELS_DIR / "staging"
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
    """Scan all staging SQL for source() calls, return set of table names."""
    tables: set[str] = set()
    for sql_file in _DBT_STAGING_DIR.rglob("*.sql"):
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
```

- [ ] **Step 2: Run ALL tests against CURRENT (broken) cards — verify RED**

Run: `uv run pytest src/tests/test_watermark_card_lineage.py -v`
Expected: At least 4 FAILures:
- `test_bronze_inputs_have_dbt_source[wf-dbt-build-output-marts]` — `formations_efpi_results`, `formations_shape_graph_results` (phantom tables)
- `test_bronze_inputs_have_dbt_source[wf-dbt-build-intermediate-marts]` — `spadl_actions` (exists as a bronze table but has no `source()` call in staging SQL)
- `test_no_circular_input_output[wf-dbt-build-intermediate-marts]` — `fct_action_values`
- `test_gold_inputs_in_tag_lineage[wf-dbt-build-intermediate-marts]` — `dim_competitions`, `fct_shots` not in `fct_action_values` ancestry

---

### Task 5: Apply all card fixes

Now apply the YAML fixes from Tasks 1-3 and verify GREEN.

**Files:**
- Modify: `workflow-cards/wf-dbt-build-output-marts.yaml` (Task 1)
- Modify: `workflow-cards/wf-dbt-build-intermediate-marts.yaml` (Task 2)
- Modify: `workflow-cards/wf-dbt-build-input-marts.yaml` (Task 3)

- [ ] **Step 1: Apply output-marts card fix (Task 1)**
- [ ] **Step 2: Apply intermediate-marts card fix (Task 2, including header comment)**
- [ ] **Step 3: Apply input-marts card fix (Task 3)**
- [ ] **Step 4: Run ALL tests — verify GREEN**

Run: `uv run pytest src/tests/test_watermark_card_lineage.py -v`
Expected: ALL PASS (all three test classes: bronze existence, circular refs, gold lineage).

- [ ] **Step 5: Verify existing card tests still pass**

Run: `uv run pytest src/tests/test_guard_conformance.py::TestWatermarkGuardHasCardInputs -v && uv run pytest src/tests/test_card_dbt_model_field.py -v`
Expected: ALL PASS

---

### Task 6: Version bump + full test suite

**Files:**
- Modify (via script): `pyproject.toml` + 25+ downstream version files

Workflow cards are bundled in the wheel (`pyproject.toml:309: "workflow-cards" = "workflow_cards"`). The card fixes must be deployed via a new wheel version.

- [ ] **Step 1: Bump version**

Run: `uv run python scripts/bump_wheel.py`
Expected: Version incremented, all 25+ files updated.

- [ ] **Step 2: Run full test suite**

Run: `uv run pytest src/tests/ -v --timeout=60`
Expected: All tests pass (excluding the 10 known pre-existing failures documented in `project_known_pretest_failures_on_main_2026_05_04.md` -- these are live-warehouse data state issues, not code regressions).

- [ ] **Step 3: Run ruff + pyright**

Run: `uv run ruff check src/ scripts/ && uv run ruff format --check src/ scripts/ && uv run pyright src/`
Expected: Zero violations.

---

## Commit scope

Single commit with all changes:
- 3 workflow card YAML fixes
- 1 new test file (with 3 test classes: bronze existence, circular refs, gold lineage)
- Version bump files

Suggested message: `fix(watermark-cards): remove phantom/circular/extra inputs from dbt-build cards`

---

## Open items (not in this plan)

1. **`metrica_events` / `idsse_events` in stage 3 card**: These bronze tables feed output_mart models (`fct_passes`, `fct_line_breaking_results`) via `int_unified_passes`. They're correctly removed from stage 1 (not in input_mart/dimension lineage), but they're also not in stage 3's card. If they're the ONLY tables that change, stage 3's watermark check won't detect it. This is a pre-existing design gap -- stage 3's card only lists compute-output bronze tables, not provider-source bronze tables. Consider adding them in a follow-up.

2. **`elastic_sync_results` dead-end**: `stg_idsse__elastic_sync.sql` reads `stg_idsse__events` but has 0 ref() consumers. This staging model may be vestigial. Consider removing it in a separate cleanup.

3. **`statsbomb_360` dead-end**: `stg_statsbomb__360.sql` has a source definition and staging SQL but 0 ref() consumers. It was likely created for a future mart that hasn't been built yet.
