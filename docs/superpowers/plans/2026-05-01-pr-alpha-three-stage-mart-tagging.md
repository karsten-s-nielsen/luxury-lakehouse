# PR-α: Three-Stage Mart Tagging + Conformance Test + Career Fix Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Tag every dbt mart with one of `dimension` / `input_mart` / `intermediate_mart` / `output_mart`; add a classification conformance test; fix the career mart's mixed-dim bug deferred from PR #242; introduce ADR-019; amend ADR-017; commit the spec doc; update MEMORY.md.

**Architecture:** PR-α is **behaviour-neutral** — daily-job runs unchanged because the single `dbt_build` Databricks task is unchanged. Tags are pure metadata at this stage; PR-β will exploit them via dbt selectors.

**Tech Stack:** dbt-core (jinja `{{ config(...) }}`), pytest (AST-based dbt manifest parsing), Python 3.10.

---

## File Structure

| File | Responsibility |
|---|---|
| `docs/superpowers/specs/2026-05-01-option-b-three-stage-dbt-build-design.md` (existing untracked) | The spec itself — committed as part of PR-α |
| `dbt_project/models/marts/dim_*.sql` (4 files) | Add `tags=['marts', 'dimension']` to `{{ config(...) }}` |
| `dbt_project/models/marts/fct_*.sql` (34 files) | Add `tags=['marts', '<input/intermediate/output>_mart']` per the classification table below |
| `dbt_project/models/marts/fct_player_embeddings_career.sql` (additional edit) | Career v1 filter in `player_best_dim` CTE |
| `dbt_project/models/marts/fct_player_embeddings_season.sql` (additional edit) | Season v1 filter in `player_best_dim` CTE |
| `src/tests/test_dbt_mart_classification.py` (new) | Conformance test |
| `docs/superpowers/adrs/ADR-019-three-stage-dbt-build.md` (new) | New ADR |
| `docs/superpowers/adrs/ADR-002-silent-exception-swallow-elimination.md` (existing — already amended in #242) | No change in PR-α |
| `docs/superpowers/adrs/ADR-017-model-validation-as-signal-not-gate.md` (existing) | Amendment header marking the carve-out as supplanted by ADR-019 |
| `MEMORY.md` (`memory/` directory under user's claude config) | New index entry for ADR-019 |

## Mart Classification Table (locked — Task 4 applies these)

| Tag | Marts | Count |
|---|---|---|
| `dimension` | `dim_competitions`, `dim_matches`, `dim_players`, `dim_teams` | 4 |
| `input_mart` | `fct_tracking_frames`, `fct_shots`, `fct_passes`, `fct_match_summary`, `fct_physical_stats`, `fct_discipline_events` | 6 |
| `intermediate_mart` | `fct_action_values` | 1 |
| `output_mart` | `fct_player_stats`, `fct_player_percentiles`, `fct_xg_predictions`, `fct_xg_predictions_v2`, `fct_off_ball_xt`, `fct_formation_labels`, `fct_player_positions`, `fct_position_maps`, `fct_player_embeddings`, `fct_player_embeddings_career`, `fct_player_embeddings_career_360`, `fct_player_embeddings_season`, `fct_player_embeddings_season_360`, `fct_line_breaking_results`, `fct_pausa_values`, `fct_pausa_rankings`, `fct_pass_timing`, `fct_defcon_actions`, `fct_defcon_pressure`, `fct_defensive_values`, `fct_goalkeeper_stats`, `fct_funnel_stages_agg`, `fct_heatmap_agg`, `fct_vaep_breakdown_agg`, `fct_gk_actions_detail`, `fct_space_creation`, `fct_tracking_avg_positions`, `fct_tracking_shape_timeline`, `fct_workflow_costs` | 29 |
| **Total** | | **40** |

(40 marts total per `ls dbt_project/models/marts/*.sql`. The earlier "37" count was approximate.)

---

## Task 1: Reconcile spec input_mart definition with its examples

**Files:**
- Modify: `docs/superpowers/specs/2026-05-01-option-b-three-stage-dbt-build-design.md` (§3 input_mart row)

The spec's strict definition ("Consumed by at least one compute task; no compute task writes to its lineage") contradicts its example list (`fct_passes`, `fct_match_summary`, `fct_physical_stats` are not consumed by compute tasks under ADR-019's new model_validation positioning). Relax the definition to match the examples: input_mart = "built only from ingest output, no compute task writes to its lineage" — drop the "consumed by compute" requirement.

- [ ] **Step 1: Edit the spec's §3 input_mart row**

Find the row in the §3 taxonomy table where the `input_mart` definition appears. Replace:

```markdown
| `input_mart` | Consumed by at least one compute task; **no compute task writes to its lineage** (built only from ingest output) | `fct_tracking_frames`, `fct_shots`, `fct_passes`, `fct_match_summary`, `fct_physical_stats` |
```

with:

```markdown
| `input_mart` | Built **only** from ingest output (no compute task writes to its lineage). May or may not be consumed by a compute task. | `fct_tracking_frames`, `fct_shots`, `fct_passes`, `fct_match_summary`, `fct_physical_stats`, `fct_discipline_events` |
```

(Also adds `fct_discipline_events` to the example set per the locked classification.)

- [ ] **Step 2: No commit yet — accumulating PR-α changes; final commit at Task 18**

---

## Task 2: Write the classification conformance test (TDD red phase)

**Files:**
- Create: `src/tests/test_dbt_mart_classification.py`

The test parses the dbt project's `marts/*.sql` files via regex (no dbt manifest dependency — keeps the test fast and offline). Asserts every mart has exactly one classification tag from the 4-tag taxonomy.

- [ ] **Step 1: Create the test file with the basic "every mart has exactly one tag" assertion**

```python
"""Meta-test: every dbt mart in marts/*.sql declares exactly one of the four
classification tags from ADR-019: ``dimension``, ``input_mart``,
``intermediate_mart``, or ``output_mart``.

Background: PR-Cycle-C (2026-05-01) introduces three-stage dbt_build
(input → compute → intermediate → compute → output → refresh). Each
mart needs to declare which stage it belongs to. The tag is read by
the dbt selector in ``terraform/modules/workflows/main.tf`` (PR-β).

This test runs at PR-CI time. Pure regex scan of marts/*.sql; no dbt
manifest dependency, no warehouse dependency.

References:
- ADR-019 — Three-Stage dbt_build
- docs/superpowers/specs/2026-05-01-option-b-three-stage-dbt-build-design.md §3
"""

from __future__ import annotations

import re
from pathlib import Path

_REPO = Path(__file__).resolve().parents[2]
_MARTS_DIR = _REPO / "dbt_project" / "models" / "marts"

# The 4-tag taxonomy. Every mart declares exactly one.
_CLASSIFICATION_TAGS: frozenset[str] = frozenset(
    {"dimension", "input_mart", "intermediate_mart", "output_mart"}
)

# Matches `tags=[...]` inside a `{{ config(...) }}` block at the top of a
# mart .sql file. Captures the contents of the list. Tolerant of whitespace
# and single-or-double-quoted tag strings.
_CONFIG_BLOCK_RE = re.compile(
    r"\{\{\s*config\s*\([^}]*?\btags\s*=\s*\[([^\]]*)\][^}]*?\)\s*\}\}",
    re.DOTALL,
)
_TAG_LITERAL_RE = re.compile(r"['\"]([^'\"]+)['\"]")


def _extract_tags(sql_path: Path) -> set[str]:
    """Return the set of tags declared in the mart's `{{ config(tags=[...]) }}` block.

    Returns an empty set if no tags= argument is found (which the test
    treats as a missing classification — the file fails the assertion).
    """
    text = sql_path.read_text(encoding="utf-8")
    m = _CONFIG_BLOCK_RE.search(text)
    if not m:
        return set()
    return set(_TAG_LITERAL_RE.findall(m.group(1)))


def _all_mart_files() -> list[Path]:
    """List every .sql file under dbt_project/models/marts/ (sorted, deterministic)."""
    return sorted(_MARTS_DIR.glob("*.sql"))


def test_every_mart_has_classification_tag() -> None:
    """Every marts/*.sql file must declare exactly one of the four
    classification tags in its ``{{ config(tags=[...]) }}`` block.

    Extra tags (e.g., ``marts``) are allowed; only the count of
    classification tags is checked.
    """
    errors: list[str] = []
    for sql_path in _all_mart_files():
        tags = _extract_tags(sql_path)
        classification_tags = tags & _CLASSIFICATION_TAGS
        if len(classification_tags) == 0:
            errors.append(
                f"{sql_path.name}: no classification tag in `{{{{ config(tags=[...]) }}}}` block. "
                f"Add ONE of {sorted(_CLASSIFICATION_TAGS)} per ADR-019."
            )
        elif len(classification_tags) > 1:
            errors.append(
                f"{sql_path.name}: declares {len(classification_tags)} classification tags "
                f"({sorted(classification_tags)}); ADR-019 requires exactly one."
            )
    assert not errors, "\n".join(errors)
```

- [ ] **Step 2: Run the test — expect it to FAIL on every mart (none are tagged yet)**

Run: `uv run pytest src/tests/test_dbt_mart_classification.py -v`
Expected: FAIL with 40 errors, one per mart, all "no classification tag in `{{ config(tags=[...]) }}` block".

---

## Task 3: Tag the 4 dimensions

**Files:**
- Modify: `dbt_project/models/marts/dim_competitions.sql`
- Modify: `dbt_project/models/marts/dim_matches.sql`
- Modify: `dbt_project/models/marts/dim_players.sql`
- Modify: `dbt_project/models/marts/dim_teams.sql`

For each file, find the existing `{{ config(...) }}` block (or absence of one — some marts inherit from `dbt_project.yml` only) and add `tags=['marts', 'dimension']` to it. The existing `marts` tag from `dbt_project.yml` is the global `+tags: ["marts"]` and stays applied; declaring it explicitly here just keeps the tag list complete inline.

- [ ] **Step 1: Edit `dim_competitions.sql`**

Read the file. Find the `{{ config(...) }}` block. If one exists, append `tags=['marts', 'dimension']` (or merge with existing tags). If none exists, add one at the top:

```jinja
{{ config(materialized='table', tags=['marts', 'dimension']) }}
```

- [ ] **Step 2: Edit `dim_matches.sql`** — same pattern, `tags=['marts', 'dimension']`

- [ ] **Step 3: Edit `dim_players.sql`** — same pattern, `tags=['marts', 'dimension']`

- [ ] **Step 4: Edit `dim_teams.sql`** — same pattern, `tags=['marts', 'dimension']`

- [ ] **Step 5: Run the conformance test to verify the 4 dimensions now pass**

Run: `uv run pytest src/tests/test_dbt_mart_classification.py -v`
Expected: 36 errors remaining (all `fct_*.sql` files); the 4 `dim_*.sql` no longer in the error list.

---

## Task 4: Tag the 6 input_marts

**Files:**
- Modify: `dbt_project/models/marts/fct_tracking_frames.sql`
- Modify: `dbt_project/models/marts/fct_shots.sql`
- Modify: `dbt_project/models/marts/fct_passes.sql`
- Modify: `dbt_project/models/marts/fct_match_summary.sql`
- Modify: `dbt_project/models/marts/fct_physical_stats.sql`
- Modify: `dbt_project/models/marts/fct_discipline_events.sql`

For each file: add `tags=['marts', 'input_mart']` to its `{{ config(...) }}` block. Apply the same edit pattern as Task 3.

- [ ] **Step 1: Edit `fct_tracking_frames.sql`** — add `tags=['marts', 'input_mart']`

- [ ] **Step 2: Edit `fct_shots.sql`** — same

- [ ] **Step 3: Edit `fct_passes.sql`** — same

- [ ] **Step 4: Edit `fct_match_summary.sql`** — same

- [ ] **Step 5: Edit `fct_physical_stats.sql`** — same

- [ ] **Step 6: Edit `fct_discipline_events.sql`** — same

- [ ] **Step 7: Run the conformance test**

Run: `uv run pytest src/tests/test_dbt_mart_classification.py -v`
Expected: 30 errors remaining (the 29 output_mart candidates + 1 intermediate_mart not yet tagged).

---

## Task 5: Tag the 1 intermediate_mart

**Files:**
- Modify: `dbt_project/models/marts/fct_action_values.sql`

`fct_action_values` is built from `bronze.{spadl_actions, vaep_action_values}` written by `compute_spadl_vaep`, AND consumed by `compute_embeddings_v2` for canonical_player_id derivation. It's the only mart in the codebase that satisfies both sides of the intermediate definition.

- [ ] **Step 1: Edit `fct_action_values.sql`** — add `tags=['marts', 'intermediate_mart']`

- [ ] **Step 2: Run the conformance test**

Run: `uv run pytest src/tests/test_dbt_mart_classification.py -v`
Expected: 29 errors remaining (the output_marts).

---

## Task 6: Tag the 29 output_marts

**Files** (29 in total — one edit each):
- `fct_player_stats.sql`, `fct_player_percentiles.sql`
- `fct_xg_predictions.sql`, `fct_xg_predictions_v2.sql`
- `fct_off_ball_xt.sql`
- `fct_formation_labels.sql`, `fct_player_positions.sql`, `fct_position_maps.sql`
- `fct_player_embeddings.sql`, `fct_player_embeddings_career.sql`, `fct_player_embeddings_career_360.sql`, `fct_player_embeddings_season.sql`, `fct_player_embeddings_season_360.sql`
- `fct_line_breaking_results.sql`
- `fct_pausa_values.sql`, `fct_pausa_rankings.sql`, `fct_pass_timing.sql`
- `fct_defcon_actions.sql`, `fct_defcon_pressure.sql`, `fct_defensive_values.sql`
- `fct_goalkeeper_stats.sql`
- `fct_funnel_stages_agg.sql`, `fct_heatmap_agg.sql`, `fct_vaep_breakdown_agg.sql`, `fct_gk_actions_detail.sql`
- `fct_space_creation.sql`
- `fct_tracking_avg_positions.sql`, `fct_tracking_shape_timeline.sql`
- `fct_workflow_costs.sql`

For each file: add `tags=['marts', 'output_mart']` to its `{{ config(...) }}` block.

- [ ] **Step 1: Edit each of the 29 files** (one tag per file; same pattern as Tasks 3-5)

Recommended ordering: alphabetical within `marts/` to make the edit set easier to verify.

- [ ] **Step 2: Run the conformance test — should now pass on all 40 marts**

Run: `uv run pytest src/tests/test_dbt_mart_classification.py -v`
Expected: PASS (1 test, 0 errors).

---

## Task 7: Add semantic-correctness assertions to the test

**Files:**
- Modify: `src/tests/test_dbt_mart_classification.py`

Now extend the test with the second-tier assertions: confirm the classifications match the lineage. Three new assertions added to the same test file:

1. `output_mart` and `intermediate_mart` lineage MUST contain at least one bronze table written by a `compute_*` Databricks task (curated set, mirroring `_BRONZE_READ_REQUIREMENTS` from `test_workflow_dag_bronze_reads.py`).
2. `input_mart` and `dimension` lineage MUST NOT contain any compute-output bronze table.
3. The single `intermediate_mart` (`fct_action_values`) must additionally be referenced by a known compute consumer (`compute_embeddings_v2`).

- [ ] **Step 1: Append the semantic-correctness assertions to the test file**

```python
# ──────────────────────────────────────────────────────────────────────────────
# Tier 2: semantic correctness — the tag must match the model's lineage.
# ──────────────────────────────────────────────────────────────────────────────

# Curated set of bronze tables written by compute_* Databricks tasks. A mart
# whose lineage contains any of these is built from compute output, NOT from
# ingest output. Mirrors the semantic of `_BRONZE_READ_REQUIREMENTS` in
# src/tests/test_workflow_dag_bronze_reads.py — when adding a new compute
# writer, update both lists.
_COMPUTE_OUTPUT_BRONZE_TABLES: frozenset[str] = frozenset(
    {
        "spadl_actions",
        "vaep_action_values",
        "xg_predictions",
        "xg_predictions_v2",
        "defcon_results",
        "defcon_actions",
        "defcon_pressure",
        "defensive_values",
        "expected_threat_grids",
        "off_ball_xt_results",
        "pitch_control_values",
        "formation_labels",
        "player_positions",
        "position_maps",
        "line_breaking_results",
        "pausa_values",
        "pausa_raw_scores",
        "obso_surfaces",
        "elastic_event_match",
        "player_embeddings_raw",
        "space_creation",
    }
)

# Marts that compute tasks read directly. Used to assert intermediate_mart
# placement is justified.
_COMPUTE_READ_MARTS: dict[str, str] = {
    "fct_action_values": "compute_embeddings_v2",
    # Future intermediate_mart cases register here.
}


def _bronze_table_names(model_path: Path) -> set[str]:
    """Heuristic: find bare bronze table names referenced from a mart.

    Walks `{{ source('<schema>', '<table>') }}` and `{{ ref('stg_<provider>__<entity>') }}`
    references in the mart .sql file and any dbt staging models it depends
    on. Returns the union of bronze table names that flow into this mart.

    Implementation: regex-scan the .sql file plus all transitively-ref'd
    staging models. dbt manifest would be more accurate but adds a build
    dependency we don't want in unit tests.
    """
    bronze_tables: set[str] = set()
    seen_files: set[Path] = set()
    queue: list[Path] = [model_path]
    while queue:
        f = queue.pop()
        if f in seen_files:
            continue
        seen_files.add(f)
        text = f.read_text(encoding="utf-8")
        # source('bronze', 'table_name') OR source('<provider>', 'table_name')
        for m in re.finditer(r"source\s*\(\s*['\"][^'\"]+['\"]\s*,\s*['\"]([^'\"]+)['\"]", text):
            bronze_tables.add(m.group(1))
        # ref('other_model') — recurse into staging/intermediate
        for m in re.finditer(r"ref\s*\(\s*['\"]([^'\"]+)['\"]", text):
            ref_name = m.group(1)
            # Find the .sql file for this ref.
            candidates = [
                _REPO / "dbt_project" / "models" / "staging" / "**" / f"{ref_name}.sql",
                _REPO / "dbt_project" / "models" / "intermediate" / "**" / f"{ref_name}.sql",
                _MARTS_DIR / f"{ref_name}.sql",
            ]
            for pat in candidates:
                for candidate in _REPO.glob(str(pat.relative_to(_REPO)).replace("\\", "/")):
                    if candidate not in seen_files:
                        queue.append(candidate)
    return bronze_tables


def _classification_of(sql_path: Path) -> str | None:
    """Return the single classification tag for a mart, or None if not exactly one."""
    tags = _extract_tags(sql_path)
    classification_tags = tags & _CLASSIFICATION_TAGS
    if len(classification_tags) != 1:
        return None
    return next(iter(classification_tags))


def test_input_mart_and_dimension_have_no_compute_output_lineage() -> None:
    """`input_mart` and `dimension` marts must not have any compute-output
    bronze table in their lineage. If they do, they should be reclassified
    as ``intermediate_mart`` (compute-consumed) or ``output_mart`` (not
    compute-consumed) per ADR-019.
    """
    errors: list[str] = []
    for sql_path in _all_mart_files():
        tag = _classification_of(sql_path)
        if tag not in ("input_mart", "dimension"):
            continue
        bronze = _bronze_table_names(sql_path)
        offenders = bronze & _COMPUTE_OUTPUT_BRONZE_TABLES
        if offenders:
            errors.append(
                f"{sql_path.name} (tagged {tag}): lineage contains compute-output bronze "
                f"table(s) {sorted(offenders)}. Reclassify as 'intermediate_mart' (if a "
                f"compute task reads it) or 'output_mart' (if not)."
            )
    assert not errors, "\n".join(errors)


def test_output_mart_has_compute_output_lineage_or_pure_aggregation() -> None:
    """`output_mart` marts SHOULD have at least one compute-output bronze
    table in their lineage — they exist to expose compute outputs to apps/
    dashboards/HF/validation. The exception: pure aggregation/projection
    marts (``fct_*_agg``, ``fct_gk_actions_detail``, ``fct_workflow_costs``,
    ``fct_tracking_avg_positions``, ``fct_tracking_shape_timeline``) which
    aggregate gold marts that themselves contain compute output upstream.

    For these "derived from gold" marts we relax the assertion to: lineage
    must contain compute-output bronze table TRANSITIVELY (via ref'd marts).
    Already covered by the recursive lineage scan in ``_bronze_table_names``.
    """
    errors: list[str] = []
    for sql_path in _all_mart_files():
        tag = _classification_of(sql_path)
        if tag != "output_mart":
            continue
        bronze = _bronze_table_names(sql_path)
        if not (bronze & _COMPUTE_OUTPUT_BRONZE_TABLES):
            errors.append(
                f"{sql_path.name} (tagged output_mart): lineage has no compute-output "
                f"bronze table. Either reclassify or add the compute writer to "
                f"_COMPUTE_OUTPUT_BRONZE_TABLES if it's a new writer."
            )
    assert not errors, "\n".join(errors)


def test_intermediate_mart_has_known_compute_consumer() -> None:
    """Every ``intermediate_mart`` must be in the ``_COMPUTE_READ_MARTS``
    registry — that's the single source of truth for which marts compute
    tasks read directly. New intermediate_mart entries land in this dict
    in the same PR that tags the mart.
    """
    errors: list[str] = []
    for sql_path in _all_mart_files():
        tag = _classification_of(sql_path)
        if tag != "intermediate_mart":
            continue
        if sql_path.stem not in _COMPUTE_READ_MARTS:
            errors.append(
                f"{sql_path.name} (tagged intermediate_mart): not in _COMPUTE_READ_MARTS "
                f"registry. Add it with the consuming compute task."
            )
    assert not errors, "\n".join(errors)
```

- [ ] **Step 2: Run the test — semantic assertions should pass given the locked classification**

Run: `uv run pytest src/tests/test_dbt_mart_classification.py -v`
Expected: 4 tests pass (1 from Task 2 + 3 new). If any fail, the classification table or `_COMPUTE_OUTPUT_BRONZE_TABLES` set is wrong; fix and re-run.

---

## Task 8: Career mart v1 filter (deferred fix from PR #242)

**Files:**
- Modify: `dbt_project/models/marts/fct_player_embeddings_career.sql`
- Modify: `dbt_project/models/marts/fct_player_embeddings_season.sql`

The career mart's `player_best_dim` CTE uses `MAX(size(behavioral_vector))` per player to pick the highest-dim embedding. Players with only v1 (32d Doc2Vec) inference produce 32d career rows; players with v2 (192d transformer) produce 192d rows. Mixed-dim across rows breaks HNSW build. Since v1 is documented as deprecated (per `terraform/modules/workflows/main.tf:22-24` "Retained for comparison; superseded by v2"), exclude v1 from the career and season aggregations.

- [ ] **Step 1: Edit `fct_player_embeddings_career.sql` — add v1 filter to `player_best_dim` CTE**

Find the existing CTE (around line 18-29):

```sql
with player_best_dim as (
    -- For players with mixed-dimension vectors (32d v1 + 128d v2),
    -- keep only the highest-dimension embeddings per player.
    -- D62 2026-04-15: explicitly exclude 360-enriched rows (144d) so they
    -- do not promote over v2's 128d embeddings. The 360 aggregates live
    -- in fct_player_embeddings_season_360 / _career_360 with their own
    -- dimensionally-homogeneous aggregation.
    select canonical_player_id, max(size(behavioral_vector)) as best_dim
    from {{ ref('fct_player_embeddings') }}
    where data_source != 'football2vec_360'
    group by canonical_player_id
),
```

Replace the `where` clause with:

```sql
    where data_source != 'football2vec_360'
      and data_source != 'football2vec_v1'   -- PR-Cycle-C 2026-05-01: exclude 32d v1 Doc2Vec.
                                             -- v1 is "Retained for comparison; superseded by v2"
                                             -- per terraform/modules/workflows/main.tf:22-24.
                                             -- Mixed-dim career rows broke HNSW build at vector(192).
```

- [ ] **Step 2: Edit `fct_player_embeddings_season.sql` — same fix**

The season mart has the identical CTE structure; apply the same `and data_source != 'football2vec_v1'` clause with the same comment.

- [ ] **Step 3: Verify the v1 filter does not change tag-level test outcomes**

Run: `uv run pytest src/tests/test_dbt_mart_classification.py -v`
Expected: still 4 passing tests (career + season are tagged `output_mart` per Task 6; the v1 filter doesn't affect classification).

---

## Task 9: Write ADR-019

**Files:**
- Create: `docs/superpowers/adrs/ADR-019-three-stage-dbt-build.md`

Use the existing ADRs in `docs/superpowers/adrs/` as the template (Michael Nygard format: Context / Decision / Alternatives considered / Consequences / CLAUDE.md Amendment / Related). The spec already has all the content drafted — translate it to ADR form.

- [ ] **Step 1: Create the ADR file**

```markdown
# ADR-019: Three-Stage `dbt_build` for Same-Day Gold-Reader Compute

| Field | Value |
|---|---|
| **Date** | 2026-05-01 |
| **Status** | Accepted |
| **Deciders** | Karsten S. Nielsen |

## Context

PR #242 (PR-Cycle-B, 2026-05-01) closed 4 overnight CI failures + 6 session-69 hardening gaps + an 11-file SDK module-level-import architectural fix. While doing the DAG audit for #242, we identified an undocumented architectural drift: most gold-reader compute tasks (`compute_pitch_control`, `compute_off_ball_xt`, `compute_xg_model[_v2]`, `compute_formations_efpi/shape_graph`, `compute_embeddings_v2`) read **yesterday's** gold marts because `dbt_build` runs at the END of the daily-job DAG (after compute). New matches/data therefore appear in those compute outputs **one day late**.

ADR-017 (2026-04-29) documents one carve-out for `run_model_validation` reading yesterday's gold by design. That carve-out was a workaround for the single-stage `dbt_build` architecture: any `run_model_validation → dbt_build` edge would let validation regressions block today's mart refresh. Three-stage architecture removes the workaround's need by topology — see §5.

## Decision

Replace the single `dbt_build` Databricks task with **three** sequential dbt invocations, governed by a per-mart classification tag in each model's `{{ config(...) }}` block:

```
ingest_*  →  dbt_build_input_marts  →  compute_phase_1  →  dbt_build_intermediate_marts
                                                                                       ↘
                                            compute_phase_2  →  dbt_build_output_marts  →  refresh_synced_tables
                                                                                       ↘
                                                                                          run_model_validation
```

Where:
- `dbt_build_input_marts`: builds dimensions + marts built only from ingest output (e.g., `gold.fct_tracking_frames` from bronze tracking)
- `dbt_build_intermediate_marts`: builds marts that compute reads but that themselves depend on **other** compute output (e.g., `gold.fct_action_values` from `bronze.{spadl_actions, vaep_action_values}` written by `compute_spadl_vaep`)
- `dbt_build_output_marts`: builds remaining marts (built from compute outputs and consumed only by apps/dashboards/HF/`run_model_validation`)

## Mart taxonomy

Every mart gets exactly one of four tags (in addition to the inherited `marts` tag from `dbt_project.yml`):

| Tag | Definition | Stage |
|---|---|---|
| `dimension` | Pure conformed dimensions; no compute task in lineage | 1 |
| `input_mart` | Built only from ingest output (no compute task in lineage); may or may not be compute-consumed | 1 |
| `intermediate_mart` | Has compute output in lineage AND consumed by at least one compute task | 2 |
| `output_mart` | Has compute output in lineage; not consumed by any compute task | 3 |

Enforcement: `src/tests/test_dbt_mart_classification.py` asserts at PR-CI time that every mart has exactly one tag and that the tag matches the lineage (input/dimension marts have no compute-output bronze; output marts do; intermediate marts must be in the `_COMPUTE_READ_MARTS` registry).

## "Compute reads today's gold" principle

Any Databricks task that reads a `gold.fct_*` table reads **today's** gold (built earlier in the same daily-job run). **No exceptions in the new architecture.** ADR-017's pre-three-stage carve-out for `run_model_validation` is supplanted by the new topology — validation depends on `dbt_build_output_marts` (so reads today's gold) and runs as a sibling of `refresh_synced_tables` (so a validation regression cannot block synced-table refresh). The "signal not gate" guarantee is preserved by **structure**, not by stale reads.

## ADR-017 supersession

ADR-017's yesterday-gold carve-out for `run_model_validation` was a workaround for the single-stage `dbt_build` architecture. Three-stage replaces it with topology: validation is a sibling of `refresh_synced_tables` (both children of `dbt_build_output_marts`), so a validation regression cannot transitively block synced-table refresh. The "signal not gate" principle is preserved by **structure**, not by stale reads.

ADR-017 receives an "Amended" header line referencing ADR-019; the original narrative remains intact for historical context (PR-LL2 close-out forcing function).

## Migration sequence

- **PR-α** (this cycle's first PR) — adds tags to all marts + classification conformance test + career mart v1 filter (deferred from PR #242) + ADR-019 itself + ADR-017 amendment + spec doc commit. Behaviour-neutral: TF still has the single `dbt_build` task. Tags are pure metadata until PR-β.
- **PR-β** (this cycle's second PR) — TF restructure into three dbt tasks; reorders compute task `depends_on`; removes 13 stale gold-reader edges (per PR #242's audit); adds `run_model_validation → dbt_build_output_marts` edge; adds `src/tests/test_workflow_dag_gold_reads.py` peer to the bronze-read conformance test from PR #242.

## Alternatives considered

| Option | Pros | Cons | Why rejected |
|---|---|---|---|
| **A.** Bronze-direct refactor of compute tasks (read bronze instead of gold) | Same-day freshness without dbt restructure | ~7 compute tasks need rewriting; loses dbt's schema-enforced inputs; duplicates ID-normalization | Larger refactor surface than B; bronze-direct compute loses the gold-mart benefits from ADR-013 |
| **B.** Two-stage strict, accept 1-day-lag for `embeddings_v2 → fct_action_values` | Simpler than three-stage | Forecloses on more `intermediate_mart` cases as ML pipelines compose (e.g. an OBSO-derived feature flowing to a downstream embedding model) | User direction (per spec brainstorm): "very likely to not be the only task long term" |
| **C.** Two-stage strict, accept the lag for ALL gold-reader compute | Zero structural change | Defeats the cycle goal (lag was the whole motivation) | Rejected by user |
| **D.** Three-stage **(chosen)** | Same-day freshness for all gold-reader compute; forward-compatible with future intermediate-mart cases | 3 dbt invocations/day (~5 min added wall-clock); mart classification adds discipline | — |
| **E.** Keep ADR-017's yesterday-gold carve-out for `run_model_validation` unchanged | No ADR-017 amendment needed | Three-stage makes the carve-out strictly worse than topology-based "signal not gate"; would leave stale reads on the table for no architectural reason | — |

## Consequences

### Positive

- Same-day freshness for **all** gold-reader compute tasks, including `run_model_validation`. New matches in today's ingest produce today's xG predictions, today's pitch control, today's formations, today's embeddings inference, today's validation signals.
- Mart classification taxonomy provides a single audit-friendly place to ask "which stage builds this mart". Adding a new mart is a 1-line `tags=[...]` decision; the conformance test enforces it.
- ADR-017's "signal not gate" principle is preserved by topology (sibling positioning) rather than by stale reads — a structural improvement.
- New `intermediate_mart` cases (future ML pipeline composition) just register one entry in `_COMPUTE_READ_MARTS` and inherit the existing 3-stage flow.

### Negative

- 3 dbt invocations per day instead of 1. Each invocation has a fixed warehouse warmup + parse cost (~1-2 min); combined ~5 min added wall-clock. Daily-job has a 4-hour budget; well within.
- Mart classification adds discipline: every new mart requires a tag + a justification. The conformance test catches missing tags at PR-CI time.
- ADR-017's narrative is now partially historical (the yesterday-gold workaround it documented is supplanted). The "Amended" header line preserves the original context.

### Neutral

- Wheel-resident library code is unchanged. PR-α is dbt models + tests + docs; PR-β is TF + tests.
- Daily-job behaviour during PR-α is identical to today. PR-α is purely metadata + documentation.

## CLAUDE.md Amendment

None. The classification taxonomy is enforced by `test_dbt_mart_classification.py` and documented in this ADR; CLAUDE.md doesn't need a new bullet.

## Related

- **Predecessor**: PR #242 (PR-Cycle-B) — surfaced the 1-day-lag class; deferred career mart fix
- **Spec**: `docs/superpowers/specs/2026-05-01-option-b-three-stage-dbt-build-design.md`
- **Plans**: `docs/superpowers/plans/2026-05-01-pr-alpha-three-stage-mart-tagging.md` (PR-α); PR-β plan TBD after PR-α merges
- **Conformance tests**: `src/tests/test_dbt_mart_classification.py` (PR-α); `src/tests/test_workflow_dag_gold_reads.py` (PR-β)
- **Sibling ADRs**:
  - ADR-017 — Model validation as signal not gate (amended by this cycle; the yesterday-gold carve-out is supplanted)
  - ADR-013 — ML inference outputs in dbt mart (governs the bronze→gold flow that this cycle restructures)
  - ADR-002 §6 — overwrite-writer schema drift guard (precedent for declarative metadata + conformance test)
  - ADR-018 — cross-table format contract testing (same enforcement pattern)

## Notes

The user explicitly chose three-stage over two-stage on the rationale that more `intermediate_mart` cases will likely emerge as ML pipelines compose (e.g. an OBSO-derived feature flowing to a downstream embedding model). Locking in the three-stage pattern now avoids a future cycle that would otherwise re-introduce the migration cost.
```

- [ ] **Step 2: No commit yet** — accumulating PR-α changes

---

## Task 10: Amend ADR-017

**Files:**
- Modify: `docs/superpowers/adrs/ADR-017-model-validation-as-signal-not-gate.md`

Add a header line to ADR-017 indicating the carve-out has been supplanted by ADR-019. Preserve the rest of ADR-017 intact.

- [ ] **Step 1: Add the "Amended" header line to ADR-017**

Find the table at the top:

```markdown
| Field | Value |
|---|---|
| **Date** | 2026-04-29 |
| **Status** | Accepted |
| **Deciders** | Karsten S. Nielsen |
```

Add an "Amended" row immediately after "Status":

```markdown
| Field | Value |
|---|---|
| **Date** | 2026-04-29 |
| **Status** | Accepted |
| **Amended** | 2026-05-01 — yesterday-gold carve-out supplanted by ADR-019's three-stage `dbt_build` topology. The "signal not gate" principle is now preserved by sibling-of-`refresh_synced_tables` positioning under `dbt_build_output_marts`, not by stale reads. The original narrative below remains intact for historical context. |
| **Deciders** | Karsten S. Nielsen |
```

- [ ] **Step 2: No commit yet**

---

## Task 11: Update MEMORY.md index

**Files:**
- Modify: `C:\Users\Karsten\.claude\projects\D--Development-karstenskyt--luxury-lakehouse\memory\MEMORY.md`

Add a one-line entry indexing ADR-019 in the project memory (so future sessions discover the cycle).

- [ ] **Step 1: Append to the "Cycle Completion" section of MEMORY.md**

Find a logical location in the user's MEMORY.md (likely near the end of the cycle-completion or memory-entries section) and add:

```markdown
- **PR-Cycle-C (2026-05-01)** — Option B three-stage `dbt_build` for same-day gold freshness. PR-α (this commit): mart classification tagging + conformance test + career mart v1 filter (deferred from PR #242) + ADR-019 + ADR-017 amendment + spec commit. Behaviour-neutral. PR-β (next): TF restructure with 3 dbt tasks + 13 stale-edge cleanup + gold-read conformance test. See [ADR-019](docs/superpowers/adrs/ADR-019-three-stage-dbt-build.md).
```

- [ ] **Step 2: No commit yet**

---

## Task 12: Run full quality gate

**Files:**
- All edits from Tasks 1-11

Before committing, run the full quality gate to catch any regressions.

- [ ] **Step 1: Ruff lint**

Run: `uv run ruff check src/ scripts/`
Expected: `All checks passed!`

If failures: fix any issues in the new test file (likely import order or line length).

- [ ] **Step 2: Ruff format check**

Run: `uv run ruff format --check src/ scripts/`
Expected: `<N> files already formatted`

If failures: `uv run ruff format src/ scripts/`.

- [ ] **Step 3: Pyright (background — takes ~60s)**

Run in background: `uv run pyright src/`
Expected: `0 errors`

- [ ] **Step 4: New conformance test passes**

Run: `uv run pytest src/tests/test_dbt_mart_classification.py -v`
Expected: 4 tests pass.

- [ ] **Step 5: Full pytest suite (background — takes ~2 min)**

Run in background: `uv run pytest src/tests/ --ignore=src/tests/integration -q`
Expected: 2334+ tests pass (the +4 from this PR).

If any pre-existing test breaks: investigate. The PR is behaviour-neutral so no test should regress.

---

## Task 13: Stage + final review checkpoint

**Files**: all modifications + new files staged.

- [ ] **Step 1: Verify file count**

Run: `git status --short | wc -l`
Expected: 45 modifications + new files:
- 40 mart `.sql` files modified (tags added; 2 of those have additional v1 filter edits)
- 1 new test file (`src/tests/test_dbt_mart_classification.py`)
- 1 new ADR (`ADR-019-three-stage-dbt-build.md`)
- 1 modified ADR (`ADR-017-model-validation-as-signal-not-gate.md`)
- 1 spec file (`docs/superpowers/specs/2026-05-01-option-b-three-stage-dbt-build-design.md`)
- 1 MEMORY.md update (in user's `~/.claude/projects/.../memory/`)

That's 45 file changes. The MEMORY.md is in the user's home (not the repo); it's saved separately and doesn't go into the repo commit.

- [ ] **Step 2: Stage everything in the repo**

Run:
```bash
git add dbt_project/models/marts/
git add src/tests/test_dbt_mart_classification.py
git add docs/superpowers/adrs/ADR-019-three-stage-dbt-build.md
git add docs/superpowers/adrs/ADR-017-model-validation-as-signal-not-gate.md
git add docs/superpowers/specs/2026-05-01-option-b-three-stage-dbt-build-design.md
```

- [ ] **Step 3: Diff review**

Run: `git diff --cached --stat`
Expected: ~44 files in the repo commit (40 marts + 1 test + 1 spec + 1 new ADR + 1 amended ADR).

Spot-check the diff with `git diff --cached docs/superpowers/specs/2026-05-01-option-b-three-stage-dbt-build-design.md` — should show the spec being added in full + the §3 input_mart definition fix from Task 1.

---

## Task 14: Pause for commit approval

The commit is sentinel-gated per `reference_git_commit_sentinel.md`. Pause and ask the user to touch `~/.claude-git-approval` before committing.

- [ ] **Step 1: Tell the user the PR is ready and what's in the commit**

Wait for the user to touch the sentinel.

- [ ] **Step 2: Commit (after sentinel touched)**

Run:
```bash
git commit -m "$(cat <<'EOF'
feat(dbt+adrs): tag marts for three-stage dbt_build (PR-α of PR-Cycle-C)

Behaviour-neutral metadata + conformance test prep for the upcoming
TF restructure (PR-β). Daily-job runs unchanged.

Mart classification (40 marts):
- dimension (4): dim_competitions, dim_matches, dim_players, dim_teams
- input_mart (6): fct_tracking_frames, fct_shots, fct_passes,
  fct_match_summary, fct_physical_stats, fct_discipline_events
- intermediate_mart (1): fct_action_values (consumed by compute_embeddings_v2;
  built from compute_spadl_vaep output)
- output_mart (29): everything else

Conformance test (src/tests/test_dbt_mart_classification.py): asserts
every mart has exactly one classification tag AND that the tag matches
the model's lineage. Catches misclassification + tag drift at PR-CI
time. Same enforcement pattern as ADR-002 §4 / ADR-018 / PR #242's
test_workflow_dag_bronze_reads.py.

Career mart v1 filter (deferred from PR #242): excludes 32d v1 Doc2Vec
embeddings from fct_player_embeddings_career + _season aggregations.
Resolves the HNSW index build failure (mixed-dim 32d + 192d rows
broke vector(192) cast). v1 is "Retained for comparison; superseded
by v2" per terraform/modules/workflows/main.tf:22-24.

Docs:
- ADR-019 — Three-stage dbt_build (new). Codifies the taxonomy + the
  "compute reads today's gold" principle.
- ADR-017 — Amended. The yesterday-gold carve-out for run_model_validation
  is supplanted by ADR-019's topology (validation becomes a sibling of
  refresh_synced_tables under dbt_build_output_marts). Original
  narrative preserved.
- Spec: docs/superpowers/specs/2026-05-01-option-b-three-stage-dbt-build-design.md
  committed as the design-locked source for both PR-α and PR-β.

Next cycle: PR-β implements the TF restructure (replace dbt_build with
3 stages, reorder compute deps, remove 13 stale edges, add gold-read
conformance test).

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
```

- [ ] **Step 3: Confirm commit landed**

Run: `git log --oneline -2`
Expected: shows the new PR-α commit at HEAD.

---

## Task 15: Pause for push + PR-create approval

- [ ] **Step 1: Ask user for chat-level approval to push + create PR**

Routine push and PR-create are NOT sentinel-gated per `reference_git_commit_sentinel.md`. Chat approval suffices.

- [ ] **Step 2: After approval — push + create PR**

```bash
git push -u origin feat/three-stage-dbt-mart-tagging

gh pr create --title "feat(dbt+adrs): tag marts for three-stage dbt_build (PR-α of PR-Cycle-C)" --body "$(cat <<'EOF'
## Summary
- Tags every dbt mart with one of `dimension` / `input_mart` / `intermediate_mart` / `output_mart` per ADR-019.
- New `src/tests/test_dbt_mart_classification.py` conformance test (4 tests: tag presence + 3 semantic-correctness assertions).
- Career mart v1 filter (deferred fix from PR #242) — resolves HNSW `vector(192)` build failure on `fct_player_embeddings_career_synced`.
- ADR-019 (new) + ADR-017 amendment + spec commit.
- **Behaviour-neutral**: daily-job runs unchanged. PR-β (next) does the TF restructure.

## Test plan
- [ ] Python CI green (incl. new mart classification test)
- [ ] dbt CI green (mart tags don't change build behaviour without `--select`)
- [ ] Terraform Plan green (no TF changes in PR-α)
- [ ] dbt live CI green
- [ ] Post-merge: drop + recreate `idx_embeddings_career_behavioral_hnsw` via `scripts/create_indexes.py` — should succeed at `vector(192)` with v1 rows now excluded
- [ ] Post-merge: Lakebase Maintenance scheduled run — green for the first time since PR #242

## Related
- PR #242 (PR-Cycle-B) — predecessor; surfaced the 1-day-lag class and the deferred career fix
- ADR-019 — full design rationale
- Spec: `docs/superpowers/specs/2026-05-01-option-b-three-stage-dbt-build-design.md`

🤖 Generated with [Claude Code](https://claude.com/claude-code)
EOF
)"
```

- [ ] **Step 3: Report PR URL to user; await CI green**

---

## Self-Review

**Spec coverage check** (each requirement → task):
- ✅ Tag 37+ marts: Tasks 3-6
- ✅ `test_dbt_mart_classification.py`: Tasks 2 + 7
- ✅ Career mart v1 filter: Task 8
- ✅ ADR-019 (new): Task 9
- ✅ ADR-017 amendment: Task 10
- ✅ MEMORY.md index entry: Task 11
- ✅ Spec file commit: Task 13's stage step (existing untracked file)
- ✅ Behaviour-neutral: no TF/wheel changes anywhere
- ✅ Single commit: Task 14

**Placeholder scan**: All code blocks have actual content. No "TBD"/"TODO".

**Type consistency**: Test function names + tag values + classification table consistent throughout the plan.

**Spec definition fix in Task 1** addresses the inconsistency I noted at plan start.

---

## Risks & Mitigations

| Risk | Mitigation |
|---|---|
| Dbt build fails because tag syntax changes parsing | Tags are valid jinja in `{{ config(...) }}`; Task 12 runs `dbt build` step is omitted (no warehouse access in unit tests) but local-machine `dbt parse` will catch syntax errors before commit |
| Career mart v1 filter changes downstream embedding aggregation | Validated post-merge: HNSW index builds at vector(192); fct_player_embeddings_career row count drops by N (players with only v1 inference) — documented in ADR-019 and the deferred-fix memory |
| Conformance test misclassifies a mart | Three-tier semantic assertions (Task 7) catch mismatch at PR-CI; if classification is wrong, fix the tag (not the test) |
| PR-α merge happens between daily-job runs and breaks something | Behaviour-neutral by design — TF unchanged, wheel unchanged, dbt build invocation unchanged |
| Pyright errors on new test file | Task 12 Step 3 catches; the test uses pure stdlib + pathlib + re, no external deps |
