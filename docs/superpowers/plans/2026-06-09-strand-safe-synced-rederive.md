# Strand-safe Re-derive for TRIGGERED Synced Tables — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make operator re-derives of TRIGGERED-synced gold marts strand-safe by construction — D marts re-derive via a CDF-preserving MERGE, all other TRIGGERED marts via a full rebuild that deletes+recreates the synced table, and a runtime tripwire blocks every other overwrite path.

**Architecture:** Two dbt macros add a per-match reprocess capability to the 7 D marts (pre_hook DELETE + parenthesized OR-include predicate). A dbt `on-run-start` tripwire macro aborts any `--full-refresh` build that selects a TRIGGERED source (the live-verified strand vector), keyed off a committed `triggered_synced_marts` registry var. A pure planner (`rederive_planner.py`, zero IO) classifies selected marts into D (MERGE-reprocess), T (plain rebuild of the 2 `table` marts, zero downtime), or B (delete→full-refresh→recreate for merge-all incremental) steps; a thin executor CLI (`scripts/rederive_synced_marts.py`) runs them by composing existing scripts (dbt + delete/create/maintain). Static guard tests enforce an exhaustive D/T/B partition, registry parity, and CDF coverage; the live e2e gains a positive no-strand proof of the D mechanism.

**Tech Stack:** dbt-databricks (Jinja macros, incremental merge), Python 3.10 (dataclasses, argparse, subprocess), databricks-sdk (`[sdk]` extra — synced-table + jobs APIs), pytest (offline unit + gated serverless e2e), ruff + pyright.

**Spec:** `docs/superpowers/specs/2026-06-09-strand-safe-synced-rederive-design.md` (**rev 4** — unified `--full-refresh` tripwire after live evidence; no enable-var injection; `--rebuild` escape; P2 documented).

**Single PR / single commit** per user instruction (overrides the skill's frequent-commit default). Tasks below are TDD-structured for build order; the commit is the final task and requires explicit user approval.

---

## File Structure

| File | Responsibility | Create/Modify |
|------|----------------|---------------|
| `dbt_project/macros/reprocess_match_ids.sql` | D macros: `reprocess_match_ids_list`, `reprocess_predicate`, `reprocess_delete_hook` | Create |
| `dbt_project/macros/assert_no_triggered_full_refresh.sql` | Tripwire: `assert_no_triggered_full_refresh` (unified `--full-refresh` rule) | Create |
| `dbt_project/dbt_project.yml` | `on-run-start` wiring + `triggered_synced_marts` registry var (flat model-name list) | Modify |
| `dbt_project/models/marts/fct_action_values.sql` + 6 other D marts | Add `pre_hook` + parenthesized `reprocess_predicate` at each `not in` site | Modify (×7) |
| `src/ingestion/rederive_planner.py` | Pure planner: `PlanStep`, `D_REPROCESS_MODELS`, `_TABLE_MARTS`, `plan_rederive(…, rebuild=False)` (no IO) | Create |
| `scripts/rederive_synced_marts.py` | Thin executor CLI (dbt + SDK + script composition); `--rebuild`, `--dry-run`, `--force`, job-state guard | Create |
| `src/tests/test_rederive_planner.py` | Planner unit tests (pure) | Create |
| `src/tests/test_strand_safe_rederive.py` | Static guards: exhaustive D/B partition, registry parity, CDF coverage, no-bare-full-refresh scan (incl. `terraform/`), tripwire wiring | Create |
| `src/tests/test_synced_table_heal_e2e.py` | Add positive no-strand D-mechanism e2e | Modify |
| `docs/superpowers/adrs/ADR-043-strand-safe-synced-rederive.md` | ADR | Create |
| `CLAUDE.md` | One-line pointer under Lakebase Ops | Modify |

---

## Task 1: D macros (`reprocess_match_ids.sql`)

**Files:**
- Create: `dbt_project/macros/reprocess_match_ids.sql`

- [ ] **Step 1: Write the macro file**

```jinja
{#-
  Strand-safe per-match re-derive macros (ADR-043).

  Applied to the 7 TRIGGERED + incremental + match_id-filtered "D" marts so an
  operator re-derive MERGEs changed matches (CDF partial-update, synced table
  keeps its streaming checkpoint) instead of needing a strand-inducing
  --full-refresh. Daily builds are unchanged: every macro is a no-op unless
  var('reprocess_match_ids') is set.

  See docs/superpowers/specs/2026-06-09-strand-safe-synced-rederive-design.md.
-#}

{%- macro reprocess_match_ids_list() -%}
  {#- var('reprocess_match_ids') coerced to a list[int] (injection-safe). [] if unset.
      m2: tolerate an operator passing a scalar (--vars '{reprocess_match_ids: 5}') by wrapping it. -#}
  {%- set raw = var('reprocess_match_ids', none) -%}
  {%- if raw is none -%}
    {{ return([]) }}
  {%- endif -%}
  {%- if raw is string or raw is number -%}
    {{ return([raw | int]) }}
  {%- endif -%}
  {{ return(raw | map('int') | list) }}
{%- endmacro -%}


{%- macro reprocess_predicate(match_col='match_id') -%}
  {#-
    OR-include clause that re-admits reprocessed matches into an incremental SELECT.

    MUST be placed INSIDE parentheses that wrap the existing `not in (...)` filter,
    so the OR scopes ONLY to the match-exclusion and cannot defeat sibling AND
    predicates (e.g. `where player_id is not null and (<not in> <predicate>)`):

        where (match_id not in (select ...) {{ reprocess_predicate('match_id') }})

    Safety net (ADR-043 / review N1): if the reprocess_delete_hook DELETE is not yet
    visible to this SELECT (commit-ordering), this OR still re-includes the match, so
    a deleted match is never left un-reinserted. Renders empty when no reprocess ids.
  -#}
  {%- set ids = reprocess_match_ids_list() -%}
  {%- if ids | length > 0 -%}
    or {{ match_col }} in ({{ ids | join(', ') }})
  {%- endif -%}
{%- endmacro -%}


{%- macro reprocess_delete_hook(match_col='match_id') -%}
  {#-
    Model pre_hook. Deletes the reprocessed matches up-front so a re-derive that
    DROPS rows cannot orphan them (a pure MERGE never deletes), and a surrogate-key
    shift (e.g. time_seconds change) cannot strand the old key. reprocess_predicate
    re-inserts fresh rows in the same run. No-op unless incremental AND ids are set
    (so first-build, full-refresh, and daily runs never hit it — {{ this }} may not
    exist on first build, which is why is_incremental() guards it).
  -#}
  {%- set ids = reprocess_match_ids_list() -%}
  {%- if execute and is_incremental() and ids | length > 0 -%}
    delete from {{ this }} where {{ match_col }} in ({{ ids | join(', ') }})
  {%- endif -%}
{%- endmacro -%}
```

- [ ] **Step 2: Verify it parses (no live warehouse needed)**

Run: `cd dbt_project && uvx --from "dbt-core>=1.10.0,<1.12.0" --with dbt-databricks dbt parse --profiles-dir . 2>&1 | tail -5`
Expected: `Wrote manifest` / no Jinja parse error referencing `reprocess_match_ids.sql`. (Macro render correctness is asserted in Task 2's compile + Task 6's static checks.)

> NOTE: if `dbt parse` requires warehouse auth in this environment and none is configured, this step may print a connection warning AFTER successful parsing — a parse-level Jinja error is the only failure that matters here.

---

## Task 2: Apply D macros to the 7 marts

Each mart gets (a) `pre_hook="{{ reprocess_delete_hook('match_id') }}",` added to its `config()`, and (b) every existing incremental `match_id not in (...)` filter wrapped in `( ... {{ reprocess_predicate('match_id') }})`. The parenthesization is mandatory (precedence — see macro doc).

**Files:**
- Modify: `dbt_project/models/marts/fct_action_values.sql`
- Modify: `dbt_project/models/marts/fct_defcon_actions.sql`
- Modify: `dbt_project/models/marts/fct_defcon_pressure.sql`
- Modify: `dbt_project/models/marts/fct_defensive_values.sql`
- Modify: `dbt_project/models/marts/fct_off_ball_xt.sql`
- Modify: `dbt_project/models/marts/fct_tracking_frames.sql`
- Modify: `dbt_project/models/marts/fct_tracking_shape_timeline.sql`

- [ ] **Step 1: `fct_action_values.sql` — config pre_hook**

Replace:
```
    on_schema_change='append_new_columns',
    tags=['marts', 'intermediate_mart'],
```
With:
```
    on_schema_change='append_new_columns',
    pre_hook="{{ reprocess_delete_hook('match_id') }}",
    tags=['marts', 'intermediate_mart'],
```

- [ ] **Step 2: `fct_action_values.sql` — predicate at the one filter site**

Replace:
```
    {% if is_incremental() %}
    where match_id not in (select distinct match_id from {{ this }} where match_id is not null)
    {% endif %}
```
With:
```
    {% if is_incremental() %}
    where (match_id not in (select distinct match_id from {{ this }} where match_id is not null) {{ reprocess_predicate('match_id') }})
    {% endif %}
```

- [ ] **Step 3: `fct_defcon_actions.sql`**

Add to `config()` after `on_schema_change='append_new_columns',`:
```
    pre_hook="{{ reprocess_delete_hook('match_id') }}",
```
Replace the filter:
```
    {% if is_incremental() %}
    where match_id not in (select distinct match_id from {{ this }})
    {% endif %}
```
With:
```
    {% if is_incremental() %}
    where (match_id not in (select distinct match_id from {{ this }}) {{ reprocess_predicate('match_id') }})
    {% endif %}
```

- [ ] **Step 4: `fct_defcon_pressure.sql`**

Add to `config()` after `on_schema_change='append_new_columns',`:
```
    pre_hook="{{ reprocess_delete_hook('match_id') }}",
```
Replace the filter (note this site uses `and`, and has `where action_player_id is not null` above it — parenthesization is what keeps the OR from defeating that):
```
    {% if is_incremental() %}
    and match_id not in (select distinct match_id from {{ this }})
    {% endif %}
```
With:
```
    {% if is_incremental() %}
    and (match_id not in (select distinct match_id from {{ this }}) {{ reprocess_predicate('match_id') }})
    {% endif %}
```

- [ ] **Step 5: `fct_defensive_values.sql`**

Add to `config()` after `on_schema_change='append_new_columns',`:
```
    pre_hook="{{ reprocess_delete_hook('match_id') }}",
```
Replace:
```
    {% if is_incremental() %}
    where match_id not in (select distinct match_id from {{ this }})
    {% endif %}
```
With:
```
    {% if is_incremental() %}
    where (match_id not in (select distinct match_id from {{ this }}) {{ reprocess_predicate('match_id') }})
    {% endif %}
```

- [ ] **Step 6: `fct_off_ball_xt.sql` (existing_matches CTE idiom)**

Add to `config()` after `on_schema_change='append_new_columns',`:
```
    pre_hook="{{ reprocess_delete_hook('match_id') }}",
```
Replace:
```
    {% if is_incremental() %}
    where match_id not in (select match_id from existing_matches)
    {% endif %}
```
With:
```
    {% if is_incremental() %}
    where (match_id not in (select match_id from existing_matches) {{ reprocess_predicate('match_id') }})
    {% endif %}
```

- [ ] **Step 7: `fct_tracking_frames.sql` (3 union arms — replace ALL three)**

Add to `config()` after `on_schema_change='append_new_columns',`:
```
    pre_hook="{{ reprocess_delete_hook('match_id') }}",
```
There are THREE identical filter blocks (metrica / idsse / skillcorner arms). Replace each occurrence of:
```
    {% if is_incremental() %}
    where match_id not in (select match_id from existing_matches)
    {% endif %}
```
With:
```
    {% if is_incremental() %}
    where (match_id not in (select match_id from existing_matches) {{ reprocess_predicate('match_id') }})
    {% endif %}
```
Use `replace_all` (all three arms are byte-identical). Verify 3 sites changed.

> B1 note: `fct_tracking_frames` selects `tracking_id` from staging (the surrogate key is generated in `stg_*__tracking`, not in this mart). The whole-match DELETE pre_hook makes the re-derive count-safe regardless of where the key is minted. The §Task-7 e2e spike covers this empirically.

- [ ] **Step 8: `fct_tracking_shape_timeline.sql`**

Add to `config()` after `on_schema_change='append_new_columns',`:
```
    pre_hook="{{ reprocess_delete_hook('match_id') }}",
```
Replace:
```
    {% if is_incremental() %}
    and match_id not in (select match_id from existing_matches)
    {% endif %}
```
With:
```
    {% if is_incremental() %}
    and (match_id not in (select match_id from existing_matches) {{ reprocess_predicate('match_id') }})
    {% endif %}
```

- [ ] **Step 9: Verify all 7 parse**

Run: `cd dbt_project && uvx --from "dbt-core>=1.10.0,<1.12.0" --with dbt-databricks dbt parse --profiles-dir . 2>&1 | tail -5`
Expected: no Jinja error. (Static macro-presence assertion is Task 6.)

---

## Task 3: Registry var + tripwire macro + on-run-start wiring

**Files:**
- Create: `dbt_project/macros/assert_no_triggered_full_refresh.sql`
- Modify: `dbt_project/dbt_project.yml`

- [ ] **Step 1: Write the tripwire macro**

```jinja
{%- macro assert_no_triggered_full_refresh() -%}
  {#-
    on-run-start tripwire (ADR-043). UNIFIED RULE (rev 4, evidence-driven — supersedes the
    rev-3 materialization split): abort iff a --full-refresh build selects a TRIGGERED synced
    source. Live evidence (strand ledger + DESCRIBE HISTORY, 2026-06-09): routine builds
    (incremental MERGE; table CREATE OR REPLACE) do NOT strand — only --full-refresh does, and
    the two `table` marts are DAILY output_marts, so a "abort any table build" rule would abort
    the production stage-3 build. Escaped only by --vars '{allow_triggered_full_refresh: true}',
    which ONLY scripts/rederive_synced_marts.py's B path passes.
  -#}
  {%- if not execute or not flags.FULL_REFRESH -%}{{ return('') }}{%- endif -%}
  {%- if var('allow_triggered_full_refresh', false) == true -%}{{ return('') }}{%- endif -%}
  {%- set triggered = var('triggered_synced_marts', []) -%}   {#- flat list of dbt model names -#}
  {%- set hit = [] -%}
  {%- for uid in selected_resources -%}                        {#- N-a: on-run-start exposes unique_ids (dbt >=1.5) -#}
    {%- set node = graph.nodes.get(uid) -%}
    {%- set name = node.name if node else uid.split('.')[-1] -%}
    {%- if name in triggered -%}{%- do hit.append(name) -%}{%- endif -%}
  {%- endfor -%}
  {%- if hit | length > 0 -%}
    {%- do exceptions.raise_compiler_error(
        "Refusing --full-refresh of TRIGGERED synced source(s) " ~ (hit | join(', ')) ~
        " — it strands the Lakebase synced table. Use "
        "`uv run --extra sdk python scripts/rederive_synced_marts.py --select <selector>` (strand-safe). "
        "Tool-only override: --vars '{allow_triggered_full_refresh: true}'.") -%}
  {%- endif -%}
{%- endmacro -%}
```

- [ ] **Step 2: Add the registry var to `dbt_project.yml`**

In the `vars:` block (after `minutes_per_match: 90`), add the 13 TRIGGERED marts as a **flat model-name list**
(the unified tripwire only needs names; materialization is read from the mart SQL by the partition test):
```yaml
  # ── Strand-safe re-derive registry (ADR-043) ───────────────────────────
  # TRIGGERED synced-table source marts (dbt MODEL names, not the _synced names).
  # The on-run-start tripwire (assert_no_triggered_full_refresh) refuses a
  # --full-refresh that selects any of these unless scripts/rederive_synced_marts.py
  # passes allow_triggered_full_refresh. Parity with SYNCED_TABLES is enforced by
  # test_strand_safe_rederive.py — add a TRIGGERED entry to BOTH places or CI fails.
  triggered_synced_marts:
    - fct_action_values
    - fct_passes
    - fct_player_embeddings
    - fct_tracking_frames
    - fct_defensive_values
    - fct_defcon_actions
    - fct_defcon_pressure
    - fct_line_breaking_results
    - fct_off_ball_xt
    - fct_tracking_shape_timeline
    - fct_action_context
    - fct_space_creation
    - fct_pausa_values
```

- [ ] **Step 3: Wire the tripwire via on-run-start**

In `dbt_project.yml`, after the `profile: databricks` line (top-level key, sibling of `models:`), add:
```yaml
on-run-start:
  - "{{ assert_no_triggered_full_refresh() }}"
```

- [ ] **Step 4: Verify parse**

Run: `cd dbt_project && uvx --from "dbt-core>=1.10.0,<1.12.0" --with dbt-databricks dbt parse --profiles-dir . 2>&1 | tail -5`
Expected: no error. (The raise-behavior is verified live in Task 9 Step 3.)

---

## Task 4: Pure planner (`rederive_planner.py`)

**Files:**
- Create: `src/ingestion/rederive_planner.py`
- Test: `src/tests/test_rederive_planner.py`

- [ ] **Step 1: Write the failing planner tests**

```python
"""Unit tests for the pure re-derive planner (zero IO — no warehouse, no SDK)."""

from __future__ import annotations

from ingestion.rederive_planner import (
    D_REPROCESS_MODELS,
    PlanStep,
    plan_rederive,
)
from ingestion.refresh_synced_tables import SYNCED_TABLES


def _triggered_models() -> set[str]:
    return {c.source_table for c in SYNCED_TABLES if c.scheduling_policy == "TRIGGERED"}


def test_d_mart_plans_merge_reprocess_no_full_refresh() -> None:
    steps = plan_rederive({"fct_action_values"}, [10, 20])
    assert len(steps) == 1
    step = steps[0]
    assert step.action == "D"
    assert step.full_refresh is False
    assert step.synced_table == "fct_action_values_synced"
    assert step.dbt_vars == {"reprocess_match_ids": [10, 20]}  # no enable vars injected (rev 4)


def test_table_mart_plans_plain_rebuild_T() -> None:
    # rev 5: the 2 `table` marts use the T (plain rebuild) action — zero downtime, no synced
    # delete, no --full-refresh (matches the strand-free daily plain build). No vars (no
    # enable-var injection; dbt_project.yml defaults match production — space_creation stays 0-row).
    for mart in ("fct_pausa_values", "fct_space_creation"):
        steps = plan_rederive({mart}, [])
        assert len(steps) == 1, mart
        step = steps[0]
        assert step.action == "T", mart
        assert step.full_refresh is False, mart
        assert step.dbt_vars == {}, mart


def test_merge_all_incremental_mart_plans_b() -> None:
    steps = plan_rederive({"fct_passes"}, [])
    assert len(steps) == 1
    step = steps[0]
    assert step.action == "B"
    assert step.full_refresh is True
    assert step.dbt_vars == {"allow_triggered_full_refresh": True}


def test_rebuild_routes_a_d_mart_through_b() -> None:
    # --rebuild: full-rebuild a D mart (schema/contract change) via the B path.
    steps = plan_rederive({"fct_action_values"}, [10], rebuild=True)
    assert len(steps) == 1 and steps[0].action == "B" and steps[0].full_refresh is True
    assert steps[0].dbt_vars == {"allow_triggered_full_refresh": True}


def test_rebuild_routes_a_table_mart_through_b() -> None:
    # --rebuild of a table mart forces the heavy delete→recreate (e.g. to refresh synced schema).
    steps = plan_rederive({"fct_pausa_values"}, [], rebuild=True)
    assert len(steps) == 1 and steps[0].action == "B"


def test_snapshot_mart_is_skipped() -> None:
    # fct_shots is SNAPSHOT (immune) — must produce no step.
    assert plan_rederive({"fct_shots"}, [1]) == []


def test_non_synced_model_is_skipped() -> None:
    assert plan_rederive({"int_running_score"}, [1]) == []


def test_d_then_t_then_b_ordering() -> None:
    steps = plan_rederive({"fct_passes", "fct_pausa_values", "fct_action_values"}, [5])
    assert [s.action for s in steps] == ["D", "T", "B"]


def test_every_d_model_is_triggered() -> None:
    assert D_REPROCESS_MODELS <= _triggered_models()
```

- [ ] **Step 2: Run — verify it fails**

Run: `uv run pytest src/tests/test_rederive_planner.py -q`
Expected: FAIL with `ModuleNotFoundError: No module named 'ingestion.rederive_planner'`.

- [ ] **Step 3: Implement the planner**

```python
"""Pure planner for strand-safe synced-table re-derives (ADR-043).

Zero IO: classifies a set of selected dbt model names into ordered re-derive
PlanSteps using only SYNCED_TABLES + the declared D registry. The thin executor
(scripts/rederive_synced_marts.py) resolves the selection + match ids and runs the
plan. This split makes all classification logic unit-testable offline.

Three actions (rev 5): D (MERGE-reprocess, incremental+match-filter), T (plain rebuild, the 2
`table` marts — zero downtime), B (delete→full-refresh→recreate, merge-all incremental). No
enable-var injection: dbt_project.yml already enables every gated mart that should be enabled
(pausa_enabled / embeddings_enabled / defcon_enabled = true) and intentionally leaves
fct_space_creation 0-row (no node-level enabled=; only a body gate) — so the re-derive reproduces
the daily build's state by passing NO enable vars.
"""

from __future__ import annotations

from collections.abc import Iterable, Sequence
from dataclasses import dataclass
from typing import Literal

from ingestion.refresh_synced_tables import SYNCED_TABLES, SyncedTableConfig

# The 7 TRIGGERED + incremental + match_id-filtered marts re-derived via the
# CDF-preserving D path (MERGE + reprocess macros). Exhaustive D/T/B partition over
# the TRIGGERED set is enforced by src/tests/test_strand_safe_rederive.py.
D_REPROCESS_MODELS: frozenset[str] = frozenset(
    {
        "fct_action_values",
        "fct_defcon_actions",
        "fct_defcon_pressure",
        "fct_defensive_values",
        "fct_off_ball_xt",
        "fct_tracking_frames",
        "fct_tracking_shape_timeline",
    }
)

# The TRIGGERED `table`-materialized marts. Re-derived via the T (plain rebuild) action:
# a plain `dbt build` is an atomic create-or-replace (count-safe, same id, strand-free —
# it is exactly what the daily stage-3 does) so no synced delete/recreate and no
# --full-refresh is needed. Zero downtime. Verified table-materialized by the partition test.
_TABLE_MARTS: frozenset[str] = frozenset({"fct_pausa_values", "fct_space_creation"})


@dataclass(frozen=True)
class PlanStep:
    """One mart's re-derive instruction. ``dbt_vars`` is passed verbatim to ``dbt build --vars``."""

    model: str
    synced_table: str
    action: Literal["D", "T", "B"]
    full_refresh: bool
    dbt_vars: dict[str, object]


def _triggered_configs() -> dict[str, SyncedTableConfig]:
    return {c.source_table: c for c in SYNCED_TABLES if c.scheduling_policy == "TRIGGERED"}


def plan_rederive(
    selected_models: Iterable[str], match_ids: Sequence[int], *, rebuild: bool = False
) -> list[PlanStep]:
    """Classify selected models into ordered (D, then T, then B) re-derive steps.

    SNAPSHOT / non-synced models are skipped (immune). D = MERGE-reprocess (``reprocess_match_ids``,
    no overwrite). T = plain rebuild of a `table` mart (atomic create-or-replace, zero downtime).
    B = delete synced → ``--full-refresh`` (``allow_triggered_full_refresh``) → recreate, for
    merge-all incremental marts. ``rebuild=True`` forces EVERY selected mart through B — the
    sanctioned full-rebuild for a D mart's schema/contract change, or to refresh a T mart's synced
    schema (the tripwire blocks a bare ``dbt --full-refresh``).
    """
    triggered = _triggered_configs()
    steps: list[PlanStep] = []
    for model in set(selected_models):
        cfg = triggered.get(model)
        if cfg is None:
            continue
        if rebuild:
            steps.append(PlanStep(model, cfg.name, "B", True, {"allow_triggered_full_refresh": True}))
        elif model in D_REPROCESS_MODELS:
            steps.append(PlanStep(model, cfg.name, "D", False, {"reprocess_match_ids": list(match_ids)}))
        elif model in _TABLE_MARTS:
            steps.append(PlanStep(model, cfg.name, "T", False, {}))
        else:
            steps.append(PlanStep(model, cfg.name, "B", True, {"allow_triggered_full_refresh": True}))
    _order = {"D": 0, "T": 1, "B": 2}
    return sorted(steps, key=lambda s: (_order[s.action], s.model))
```

- [ ] **Step 4: Run — verify pass**

Run: `uv run pytest src/tests/test_rederive_planner.py -q`
Expected: PASS (9 tests).

---

## Task 5: Executor CLI (`scripts/rederive_synced_marts.py`)

Thin orchestrator: resolve selection (`dbt ls`), compute match ids (warehouse query or `--match-ids`), build the plan, guard against the daily job, then execute each step by composing existing scripts (`dbt build`, `scripts/delete_synced_table.py`, `scripts/create_synced_table.py`, `scripts/maintain_synced_tables.py`, `ingestion.refresh_synced_tables`).

**Files:**
- Create: `scripts/rederive_synced_marts.py`
- Test: `src/tests/test_rederive_synced_marts_cli.py`

- [ ] **Step 1: Write the failing CLI smoke tests (pure — no live calls)**

```python
"""Smoke tests for the rederive executor's pure helpers (no live dbt/SDK)."""

from __future__ import annotations

import importlib.util
from pathlib import Path

import pytest

_SPEC = importlib.util.spec_from_file_location(
    "rederive_synced_marts", Path(__file__).resolve().parents[2] / "scripts" / "rederive_synced_marts.py"
)
mod = importlib.util.module_from_spec(_SPEC)
assert _SPEC and _SPEC.loader
_SPEC.loader.exec_module(mod)


def test_parse_model_names_strips_dbt_ls_noise() -> None:
    raw = "fct_action_values\nfct_pausa_values\n\nSome log line that is not a model\n"
    # Only names that match a known model identifier pattern survive.
    names = mod._parse_model_names(raw)
    assert "fct_action_values" in names
    assert "fct_pausa_values" in names


def test_downtime_estimate_flags_large_b_marts() -> None:
    assert "window" in mod._downtime_estimate("fct_action_context", "B").lower()
    assert mod._downtime_estimate("fct_action_values", "D") == "none (in-place MERGE)"
    assert "none" in mod._downtime_estimate("fct_pausa_values", "T").lower()


def test_requires_match_ids_when_d_step_present() -> None:
    from ingestion.rederive_planner import plan_rederive

    steps = plan_rederive({"fct_action_values"}, [])
    with pytest.raises(SystemExit):
        mod._validate_match_ids(steps, match_ids=[])
```

- [ ] **Step 2: Run — verify fail**

Run: `uv run pytest src/tests/test_rederive_synced_marts_cli.py -q`
Expected: FAIL (`FileNotFoundError` / module load error — script not created yet).

- [ ] **Step 3: Implement the executor CLI**

```python
#!/usr/bin/env python3
"""Strand-safe re-derive of TRIGGERED-synced gold marts (ADR-043).

The ONLY operator entry point for re-deriving a mart whose Lakebase synced table is
scheduling_policy=TRIGGERED. Classifies each selected mart (pure planner) into:
  D  — incremental + match_id-filtered: `dbt build` with reprocess_match_ids (MERGE,
       CDF partial-update, no strand), then trigger+wait the synced table.
  T  — `table` mart: plain `dbt build` (atomic create-or-replace, count-safe, strand-free —
       the daily stage-3 path), then trigger+wait. Zero downtime, no synced delete.
  B  — merge-all incremental: delete synced -> `dbt build --full-refresh`
       (allow_triggered_full_refresh) -> recreate synced -> grants+indexes.

SNAPSHOT marts are skipped (immune). Composes existing scripts; this file is the thin
executor adapter (the planning logic lives in ingestion.rederive_planner).

Usage:
    uv run --extra sdk python scripts/rederive_synced_marts.py --select fct_action_values --provider idsse
    uv run --extra sdk python scripts/rederive_synced_marts.py --select tag:marts --match-ids 12,34 --dry-run
"""

from __future__ import annotations

import argparse
import json
import re
import subprocess
import sys
from pathlib import Path

from ingestion.rederive_planner import PlanStep, plan_rederive
from shared.constants import IDENTIFIER_RE

_REPO_ROOT = Path(__file__).resolve().parent.parent
_DBT_PROJECT = _REPO_ROOT / "dbt_project"

_CATALOG = "soccer_analytics"
_GOLD_SCHEMA = "dev_gold"
_BRONZE_SCHEMA = "bronze"  # live-confirmed: soccer_analytics.bronze.spadl_actions (9.7M rows). NOT dev_bronze.
_DAILY_JOB_ID = 302697362345215  # soccer-analytics-ingestion-dev (mega-job)
_MODEL_NAME_RE = re.compile(r"^[a-z][a-z0-9_]*$")

# Static per-mart downtime hints for --dry-run (B path re-snapshot is the cost, T5).
_LARGE_B_MARTS = frozenset({"fct_action_context", "fct_player_embeddings"})


def _parse_model_names(dbt_ls_stdout: str) -> set[str]:
    """Extract model names from `dbt ls --output name` output (ignore log noise)."""
    return {line.strip() for line in dbt_ls_stdout.splitlines() if _MODEL_NAME_RE.match(line.strip())}


def _downtime_estimate(model: str, action: str) -> str:
    if action == "D":
        return "none (in-place MERGE)"
    if action == "T":
        return "none (atomic create-or-replace + CDF refresh)"
    if model in _LARGE_B_MARTS:
        return "MINUTES — size a maintenance window (synced re-snapshot of a multi-million-row table)"
    return "seconds-to-minutes (small synced re-snapshot)"


def _validate_match_ids(steps: list[PlanStep], *, match_ids: list[int]) -> None:
    """A D step with no match ids is a no-op re-derive — fail loud rather than silently do nothing."""
    if any(s.action == "D" for s in steps) and not match_ids:
        print(
            "ERROR: selection includes D (per-match) marts but no --provider/--match-ids given. "
            "A D re-derive with no match ids changes nothing. Supply --provider <p> or --match-ids a,b.",
            file=sys.stderr,
        )
        raise SystemExit(2)


def _resolve_selected_models(selector: str) -> set[str]:
    res = subprocess.run(  # noqa: S603
        # --quiet (m3): suppress dbt log lines so a stray lowercase token can't be parsed as a model.
        ["dbt", "ls", "--quiet", "--resource-type", "model", "--select", selector, "--output", "name"],  # noqa: S607
        cwd=str(_DBT_PROJECT),
        capture_output=True,
        text=True,
        check=False,
    )
    if res.returncode != 0:
        print(f"ERROR: `dbt ls` failed:\n{res.stderr}", file=sys.stderr)
        raise SystemExit(1)
    return _parse_model_names(res.stdout)


def _match_ids_for_provider(provider: str) -> list[int]:
    from databricks.sdk import WorkspaceClient
    from databricks.sdk.service.sql import StatementParameterListItem, StatementState

    if not IDENTIFIER_RE.match(provider):
        print(f"ERROR: invalid --provider {provider!r}", file=sys.stderr)
        raise SystemExit(2)
    ws = WorkspaceClient()
    warehouse_id = _warehouse_id()
    stmt = (
        f"select distinct match_id from {_CATALOG}.{_BRONZE_SCHEMA}.spadl_actions "
        "where data_source = :provider and match_id is not null"
    )
    resp = ws.statement_execution.execute_statement(
        warehouse_id=warehouse_id,
        statement=stmt,
        parameters=[StatementParameterListItem(name="provider", value=provider)],
        wait_timeout="50s",
    )
    if not resp.status or resp.status.state != StatementState.SUCCEEDED:
        print(f"ERROR: match-id query did not succeed: {resp.status}", file=sys.stderr)
        raise SystemExit(1)
    rows = (resp.result.data_array if resp.result else None) or []
    return sorted(int(r[0]) for r in rows if r and r[0] is not None)


def _warehouse_id() -> str:
    import os

    m = re.search(r"/warehouses/([a-f0-9]+)$", os.environ.get("DATABRICKS_HTTP_PATH", ""))
    if not m:
        print("ERROR: cannot resolve warehouse id from DATABRICKS_HTTP_PATH", file=sys.stderr)
        raise SystemExit(2)
    return m.group(1)


def _assert_daily_job_idle(force: bool) -> None:
    """Refuse to run while the daily ingestion job is active (real job state, not the clock — T4)."""
    if force:
        return
    from databricks.sdk import WorkspaceClient

    ws = WorkspaceClient()
    active = list(ws.jobs.list_runs(job_id=_DAILY_JOB_ID, active_only=True))
    if active:
        ids = ", ".join(str(r.run_id) for r in active)
        print(
            f"ERROR: daily ingestion job {_DAILY_JOB_ID} has active run(s) [{ids}]. "
            "A concurrent D MERGE/B rebuild can conflict with the daily MERGE. "
            "Re-run after it finishes, or pass --force to override.",
            file=sys.stderr,
        )
        raise SystemExit(3)


def _run(cmd: list[str], *, cwd: str | None = None) -> None:
    print(f"  $ {' '.join(cmd)}", flush=True)
    res = subprocess.run(cmd, cwd=cwd, check=False)  # noqa: S603
    if res.returncode != 0:
        print(f"ERROR: step failed (exit {res.returncode}): {' '.join(cmd)}", file=sys.stderr)
        raise SystemExit(res.returncode)


def _execute_d(step: PlanStep) -> None:
    print(f"[D] {step.model} — MERGE reprocess (no downtime)")
    _run(["dbt", "build", "--select", step.model, "--vars", json.dumps(step.dbt_vars)], cwd=str(_DBT_PROJECT))
    _run([sys.executable, "-m", "ingestion.refresh_synced_tables", "--tables", step.synced_table, "--wait"])


def _execute_t(step: PlanStep) -> None:
    # Plain rebuild of a `table` mart: atomic create-or-replace (count-safe, same id, strand-free —
    # the daily stage-3 does exactly this). No synced delete, no --full-refresh. Then pull CDF.
    print(f"[T] {step.model} — plain rebuild (atomic create-or-replace, no downtime)")
    _run(["dbt", "build", "--select", step.model], cwd=str(_DBT_PROJECT))
    _run([sys.executable, "-m", "ingestion.refresh_synced_tables", "--tables", step.synced_table, "--wait"])


def _execute_b(step: PlanStep) -> None:
    print(f"[B] {step.model} — delete synced -> full-refresh -> recreate ({_downtime_estimate(step.model, 'B')})")
    _run([sys.executable, "scripts/delete_synced_table.py", step.synced_table], cwd=str(_REPO_ROOT))
    _run(
        ["dbt", "build", "--select", step.model, "--full-refresh", "--vars", json.dumps(step.dbt_vars)],
        cwd=str(_DBT_PROJECT),
    )
    _run(["uv", "run", "--extra", "sdk", "python", "scripts/create_synced_table.py", step.synced_table], cwd=str(_REPO_ROOT))


def main() -> int:
    parser = argparse.ArgumentParser(description="Strand-safe re-derive of TRIGGERED-synced gold marts.")
    parser.add_argument("--select", required=True, help="dbt selector (e.g. fct_action_values, tag:marts)")
    parser.add_argument("--provider", default="", help="Re-derive all matches of this data_source (D marts)")
    parser.add_argument("--match-ids", default="", help="Comma-separated match_ids (D marts)")
    parser.add_argument(
        "--rebuild",
        action="store_true",
        help="Full-rebuild selected marts via the B path (delete->full-refresh->recreate). "
        "Use for a D mart's schema/contract change — the tripwire blocks a bare dbt --full-refresh.",
    )
    parser.add_argument("--dry-run", action="store_true", help="Print the resolved plan and exit")
    parser.add_argument("--force", action="store_true", help="Run even if the daily ingestion job is active")
    args = parser.parse_args()

    if args.provider and args.match_ids:
        print("ERROR: pass --provider OR --match-ids, not both", file=sys.stderr)
        return 2

    selected = _resolve_selected_models(args.select)
    if not selected:
        print(f"No models matched selector {args.select!r}", file=sys.stderr)
        return 1

    if args.match_ids:
        match_ids = sorted(int(x) for x in args.match_ids.split(",") if x.strip())
    elif args.provider:
        match_ids = _match_ids_for_provider(args.provider)
    else:
        match_ids = []

    steps = plan_rederive(selected, match_ids, rebuild=args.rebuild)
    if not steps:
        print("No TRIGGERED synced marts in selection — nothing to do (SNAPSHOT marts are strand-immune).")
        return 0

    _validate_match_ids(steps, match_ids=match_ids)

    print(f"Plan ({len(steps)} step(s); {len(match_ids)} match id(s)):")
    for s in steps:
        print(f"  [{s.action}] {s.model} -> {s.synced_table} | downtime: {_downtime_estimate(s.model, s.action)}")
        print(f"        vars: {json.dumps(s.dbt_vars)}")
    if args.dry_run:
        print("\n--dry-run: no changes made.")
        return 0

    _assert_daily_job_idle(args.force)

    ran_b = False
    for s in steps:
        if s.action == "D":
            _execute_d(s)
        elif s.action == "T":
            _execute_t(s)
        else:
            _execute_b(s)
            ran_b = True

    if ran_b:
        print("Re-applying grants + indexes after B rebuild(s)...")
        _run(
            ["uv", "run", "--extra", "sdk", "python", "scripts/maintain_synced_tables.py", "--skip-heal", "--skip-refresh"],
            cwd=str(_REPO_ROOT),
        )

    print("Done — re-derive complete, synced tables online.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
```

- [ ] **Step 4: Run — verify pass**

Run: `uv run pytest src/tests/test_rederive_synced_marts_cli.py -q`
Expected: PASS (3 tests).

---

## Task 6: Static guard tests (`test_strand_safe_rederive.py`)

Offline (filesystem-only) enforcement: exhaustive D/T/B partition, registry parity, CDF coverage, macro presence, no-bare-full-refresh scan (incl. `terraform/`), tripwire wiring.

**Files:**
- Test: `src/tests/test_strand_safe_rederive.py`

- [ ] **Step 1: Write the tests**

```python
"""Static guards for strand-safe re-derive (ADR-043). Filesystem-only — no warehouse."""

from __future__ import annotations

import re
from pathlib import Path

import yaml

from ingestion.rederive_planner import D_REPROCESS_MODELS, _TABLE_MARTS
from ingestion.refresh_synced_tables import SYNCED_TABLES

_REPO = Path(__file__).resolve().parents[2]
_MARTS = _REPO / "dbt_project" / "models" / "marts"
_DBT_PROJECT_YML = _REPO / "dbt_project" / "dbt_project.yml"
_MAT_RE = re.compile(r"materialized\s*=\s*'(\w+)'")
_CDF_RE = re.compile(r"delta\.enableChangeDataFeed['\"]\s*:\s*['\"]true['\"]")


def _triggered_source_tables() -> set[str]:
    return {c.source_table for c in SYNCED_TABLES if c.scheduling_policy == "TRIGGERED"}


def _mart_sql(model: str) -> str:
    return (_MARTS / f"{model}.sql").read_text(encoding="utf-8")


def _materialization(model: str) -> str:
    m = _MAT_RE.search(_mart_sql(model))
    assert m, f"{model}: no materialized=... in config"
    return m.group(1)


def test_dtb_exhaustively_partition_the_triggered_set() -> None:
    triggered = _triggered_source_tables()
    d_set, t_set = set(D_REPROCESS_MODELS), set(_TABLE_MARTS)
    b_set = triggered - d_set - t_set
    # Total + pairwise-disjoint partition (T3): no TRIGGERED mart may be unclassified.
    assert d_set <= triggered, f"D models not in TRIGGERED set: {d_set - triggered}"
    assert t_set <= triggered, f"T models not in TRIGGERED set: {t_set - triggered}"
    assert d_set | t_set | b_set == triggered
    assert d_set & t_set == set() and d_set & b_set == set() and t_set & b_set == set()


def test_t_marts_are_table_and_others_incremental() -> None:
    triggered = _triggered_source_tables()
    for model in _TABLE_MARTS:
        assert _materialization(model) == "table", f"{model} routed to T but is not materialized='table'"
    for model in triggered - set(_TABLE_MARTS):
        assert _materialization(model) == "incremental", f"{model} (D/B) must be incremental"


def test_every_d_mart_is_incremental_and_carries_both_macros() -> None:
    for model in D_REPROCESS_MODELS:
        sql = _mart_sql(model)
        assert _materialization(model) == "incremental", f"{model} must be incremental for the D path"
        assert "reprocess_delete_hook(" in sql, f"{model} missing reprocess_delete_hook pre_hook"
        assert "reprocess_predicate(" in sql, f"{model} missing reprocess_predicate"


def test_registry_var_matches_synced_tables() -> None:
    # rev 4: triggered_synced_marts is a FLAT list of dbt MODEL names (== source_table).
    raw = yaml.safe_load(_DBT_PROJECT_YML.read_text(encoding="utf-8"))
    registry = set(raw["vars"]["triggered_synced_marts"])
    triggered = _triggered_source_tables()
    assert registry == triggered, (
        f"dbt_project.yml triggered_synced_marts != SYNCED_TABLES TRIGGERED set; "
        f"missing={triggered - registry}, extra={registry - triggered}"
    )


def test_every_triggered_mart_declares_cdf_true() -> None:
    # C2 (live-confirmed all marts carry it): a TRIGGERED synced table requires CDF on the source.
    # m-1: assert the VALUE is 'true', not just that the key string appears (a 'false' would pass otherwise).
    for model in _triggered_source_tables():
        assert _CDF_RE.search(_mart_sql(model)), f"{model} missing `delta.enableChangeDataFeed: 'true'`"


def test_tripwire_is_wired_on_run_start() -> None:
    raw = yaml.safe_load(_DBT_PROJECT_YML.read_text(encoding="utf-8"))
    hooks = raw.get("on-run-start", [])
    assert any("assert_no_triggered_full_refresh" in h for h in hooks), "tripwire not wired in on-run-start"


def test_no_bare_full_refresh_of_triggered_source_in_committed_automation() -> None:
    # P2: include terraform/ — the dbt_full_refresh job parameter is the largest vector. A
    # parameterized `--dbt-full-refresh {{...}}` is NOT flagged (it selects no TRIGGERED mart
    # by NAME and is guarded by the runtime tripwire); only a hardcoded `--full-refresh` that
    # selects a TRIGGERED mart by name is an offender.
    #
    # m-2: this scan is NAME-BASED defense-in-depth only — it will NOT catch a `--full-refresh`
    # whose selection is a TAG (e.g. `--select tag:output_mart --full-refresh`) that happens to
    # include a TRIGGERED mart. That bypass is caught at execution by the runtime on-run-start
    # tripwire (§5), which is the real guard; this static test is the cheap committed-automation net.
    triggered = _triggered_source_tables()
    scan_dirs = [
        _REPO / ".github" / "workflows",
        _REPO / "scripts",
        _REPO / "workflow-cards",
        _REPO / "terraform",
    ]
    offenders: list[str] = []
    for d in scan_dirs:
        if not d.exists():
            continue
        for f in d.rglob("*"):
            if not f.is_file() or f.name == "rederive_synced_marts.py":
                continue
            try:
                text = f.read_text(encoding="utf-8")
            except (UnicodeDecodeError, OSError):
                continue
            if "--full-refresh" not in text:
                continue
            for model in triggered:
                if re.search(rf"--select\s+\S*{re.escape(model)}\b", text) and "--full-refresh" in text:
                    offenders.append(f"{f}: --full-refresh selecting TRIGGERED {model}")
    assert not offenders, "Bare --full-refresh of TRIGGERED source(s) outside the re-derive tool:\n" + "\n".join(offenders)
```

- [ ] **Step 2: Run**

Run: `uv run pytest src/tests/test_strand_safe_rederive.py -q`
Expected: PASS (7 tests). If `test_registry_var_matches_synced_tables` fails, the `triggered_synced_marts` list (Task 3 Step 2) drifted from `SYNCED_TABLES` — fix the data, not the test. If `test_dtb_exhaustively_partition_the_triggered_set` or `test_t_marts_are_table_and_others_incremental` fails, a TRIGGERED mart is unclassified or `_TABLE_MARTS`/`D_REPROCESS_MODELS` is wrong. If `test_every_triggered_mart_declares_cdf_true` fails, a TRIGGERED mart lacks `delta.enableChangeDataFeed: 'true'`.

---

## Task 7: Positive no-strand D-mechanism e2e

**Files:**
- Modify: `src/tests/test_synced_table_heal_e2e.py`

- [ ] **Step 1: Append the new test**

Add at the end of the file (it shares the module's `pytestmark` skip + helpers):

```python
def test_d_mechanism_delete_insert_keeps_synced_online() -> None:
    """Positive proof (ADR-043): the D re-derive mechanism — in-place DELETE + INSERT on the
    SAME source table (no DROP, no CREATE OR REPLACE -> table id unchanged) followed by one
    incremental CDF refresh — keeps the TRIGGERED synced table SYNCED_TABLE_ONLINE and converges
    row counts. This is the data-plane guarantee that the D path cannot strand.

    The negative half (a new-id source overwrite DOES strand) is locked by
    test_heal_resets_checkpoint_and_resumes_incremental_cdf above. This harness never runs dbt,
    so the on-run-start tripwire is not exercised here (it is proven in the offline dbt-compile
    path); this test proves only the data-plane mechanism.
    """
    from databricks.sdk import WorkspaceClient

    from ingestion.heal_synced_tables import _make_pg_connect, _make_sql_exec
    from ingestion.refresh_synced_tables import SyncedTableConfig
    from ingestion.synced_table_lifecycle import SdkReaderAdapter, SdkWriterAdapter

    ws = WorkspaceClient()
    sql = _make_sql_exec(ws)
    reader = SdkReaderAdapter(ws)
    writer = SdkWriterAdapter(ws)
    # m1: unique throwaway names so this test never collides with the heal test under pytest-xdist.
    src_name = "fct_heal_e2e_d_src"
    synced_name = "fct_heal_e2e_d_src_synced"
    cfg = SyncedTableConfig(synced_name, src_name, ("id",), "TRIGGERED", schema_override=_SCHEMA)
    fqn = f"{_CATALOG}.{_SCHEMA}.{synced_name}"
    src = f"{_CATALOG}.{_SCHEMA}.{src_name}"

    try:
        sql(f"CREATE SCHEMA IF NOT EXISTS {_CATALOG}.{_SCHEMA}")
        sql(f"CREATE OR REPLACE TABLE {src} (id BIGINT) TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true')")
        sql(f"INSERT INTO {src} VALUES (1), (2), (3)")
        writer.create_synced_table(cfg, _CATALOG, _SCHEMA)
        assert writer.wait_until_online(fqn, timeout_s=900) == "SYNCED_TABLE_ONLINE"

        # Commit a streaming offset (the precondition that makes an overwrite strand).
        pid = reader.get_pipeline_id(fqn)
        sql(f"INSERT INTO {src} VALUES (4)")
        writer.trigger_refresh(pid)
        assert writer.wait_until_online(fqn, timeout_s=600) == "SYNCED_TABLE_ONLINE"

        # D mechanism: in-place DELETE + re-INSERT on the SAME table (no DROP/REPLACE).
        sql(f"DELETE FROM {src} WHERE id IN (2, 4)")
        sql(f"INSERT INTO {src} VALUES (2), (4), (5)")
        writer.trigger_refresh(reader.get_pipeline_id(fqn))
        # Must stay ONLINE (no strand) — the table id never changed.
        assert writer.wait_until_online(fqn, timeout_s=600) == "SYNCED_TABLE_ONLINE"

        conn = _make_pg_connect(ws)()
        try:
            with conn.cursor() as cur:
                # B-2 fix: query synced_name (this test's table), NOT the module _SYNCED (the heal test's).
                cur.execute(f'SELECT count(*) FROM {_SCHEMA}."{synced_name}"')
                row = cur.fetchone()
                assert row is not None and row[0] == 5, "D-mechanism CDF did not converge row count (1,2,3,4,5)"
        finally:
            conn.close()
    finally:
        writer.sdk_delete(fqn)
        from ingestion.synced_table_lifecycle import PsycopgGhostAdapter

        PsycopgGhostAdapter(_make_pg_connect(ws)).drop_pg_ghost(_SCHEMA, synced_name)
        sql(f"DROP TABLE IF EXISTS {src}")


def test_t_mechanism_create_or_replace_keeps_synced_online() -> None:
    """Positive proof (ADR-043, T action): a plain `CREATE OR REPLACE TABLE … AS SELECT` (what dbt's
    `table` materialization emits for fct_pausa_values / fct_space_creation) — an atomic full replace
    that keeps the Delta table id — followed by one incremental CDF refresh keeps the TRIGGERED synced
    table SYNCED_TABLE_ONLINE and converges row counts. This regression-locks the load-bearing
    "create-or-replace is strand-free" claim the T path rests on (symmetric with the D-mechanism proof
    above; protects against a future DBR/adapter change silently re-stranding the table marts).

    Contrast with test_heal_resets_checkpoint_and_resumes_incremental_cdf, which uses DROP+CREATE (a NEW
    table id) to *reproduce* a strand — here a CREATE OR REPLACE (same id) must NOT strand.
    """
    from databricks.sdk import WorkspaceClient

    from ingestion.heal_synced_tables import _make_pg_connect, _make_sql_exec
    from ingestion.refresh_synced_tables import SyncedTableConfig
    from ingestion.synced_table_lifecycle import PsycopgGhostAdapter, SdkReaderAdapter, SdkWriterAdapter

    ws = WorkspaceClient()
    sql = _make_sql_exec(ws)
    reader = SdkReaderAdapter(ws)
    writer = SdkWriterAdapter(ws)
    src_name = "fct_heal_e2e_t_src"
    synced_name = "fct_heal_e2e_t_src_synced"
    cfg = SyncedTableConfig(synced_name, src_name, ("id",), "TRIGGERED", schema_override=_SCHEMA)
    fqn = f"{_CATALOG}.{_SCHEMA}.{synced_name}"
    src = f"{_CATALOG}.{_SCHEMA}.{src_name}"

    try:
        sql(f"CREATE SCHEMA IF NOT EXISTS {_CATALOG}.{_SCHEMA}")
        sql(f"CREATE OR REPLACE TABLE {src} (id BIGINT) TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true')")
        sql(f"INSERT INTO {src} VALUES (1), (2), (3)")
        writer.create_synced_table(cfg, _CATALOG, _SCHEMA)
        assert writer.wait_until_online(fqn, timeout_s=900) == "SYNCED_TABLE_ONLINE"

        # Commit a streaming offset (the precondition that makes a NEW-id overwrite strand).
        pid = reader.get_pipeline_id(fqn)
        sql(f"INSERT INTO {src} VALUES (4)")
        writer.trigger_refresh(pid)
        assert writer.wait_until_online(fqn, timeout_s=600) == "SYNCED_TABLE_ONLINE"

        # T mechanism: atomic CREATE OR REPLACE TABLE ... AS SELECT (same id, full replace) — exactly
        # what dbt's table materialization does for the 2 table marts. Must NOT strand.
        sql(f"CREATE OR REPLACE TABLE {src} TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true') AS SELECT * FROM VALUES (1),(2),(3),(4),(5) AS t(id)")
        writer.trigger_refresh(reader.get_pipeline_id(fqn))
        assert writer.wait_until_online(fqn, timeout_s=600) == "SYNCED_TABLE_ONLINE", "CREATE OR REPLACE stranded the synced table — the T path's strand-safe assumption no longer holds"

        conn = _make_pg_connect(ws)()
        try:
            with conn.cursor() as cur:
                cur.execute(f'SELECT count(*) FROM {_SCHEMA}."{synced_name}"')
                row = cur.fetchone()
                assert row is not None and row[0] == 5, "T-mechanism CDF did not converge row count (1,2,3,4,5)"
        finally:
            conn.close()
    finally:
        writer.sdk_delete(fqn)
        PsycopgGhostAdapter(_make_pg_connect(ws)).drop_pg_ghost(_SCHEMA, synced_name)
        sql(f"DROP TABLE IF EXISTS {src}")
```

- [ ] **Step 2: Verify offline collection (test is skipped without RUN_SERVERLESS_TESTS)**

Run: `uv run pytest src/tests/test_synced_table_heal_e2e.py -q`
Expected: `3 skipped` (heal + D-mechanism + T-mechanism e2e tests all skip without `RUN_SERVERLESS_TESTS=1`). Confirms no import/syntax error.

> **T-path 0-row safety (micro-note 1 — verified):** `fct_space_creation.sql:64–79` has an explicit `{% else %}` branch emitting a fully-typed 0-row query (`select cast(null as …) … where 1 = 0`) when `space_creation_enabled` is false, so the T path's plain `dbt build` compiles to a runnable 0-row statement (not an empty/invalid query). The daily stage-3 plain build already exercises this path green.

---

## Task 8: ADR-043 + docs + memory

**Files:**
- Create: `docs/superpowers/adrs/ADR-043-strand-safe-synced-rederive.md`
- Modify: `CLAUDE.md`

- [ ] **Step 1: Write ADR-043** (Nygard format, mirror `ADR-TEMPLATE.md`). Decision, context, and consequences must capture:
  - the TRIGGERED-`--full-refresh`→strand mechanism + the **live evidence** (strand ledger + `DESCRIBE HISTORY`, 2026-06-09: routine builds don't strand, only `--full-refresh` does);
  - the **three-action split** D (MERGE-reprocess) / **T (plain rebuild of the 2 `table` marts)** / B (delete→full-refresh→recreate for merge-all incremental), with **why merge-all marts go to B** (count-safety, B3) and **why table marts go to T not B** (C-1): a plain `dbt build` of a `table` mart is an atomic create-or-replace that the daily stage-3 already runs **strand-free** (observational evidence: fct_pausa_values `CREATE OR REPLACE` on 06-04/05/06 stranded nothing; only the 06-08 `--full-refresh` did) → T is zero-downtime and avoids `--full-refresh` on table marts entirely;
  - **the C-1 honesty caveat**: the table-mart plain-vs-`--full-refresh` builds were observed (daily vs the 06-08 incident), not A/B-tested in isolation; the inferred mechanism is that `--full-refresh` does a drop+recreate (new Delta id → strand) while a plain build does an atomic replace (same id) — T is safe under either reading because it uses the plain-build path;
  - **N1** — why `reprocess_predicate` is kept despite being redundant after the pre_hook DELETE (data-loss safety net against delete-not-yet-visible commit ordering);
  - the **unified `--full-refresh` tripwire** (and explicitly that the rev-3 "abort any table build" rule was rejected — the two table marts are daily `output_mart`s, so it would have aborted production stage-3); the registry-var mechanism (read at on-run-start, parity-tested);
  - **P2** — `dbt_full_refresh=true` on the mega-job now aborts TRIGGERED-containing stages 2 & 3 (intentional; parameter kept, error routes to the tool);
  - **micro-note 2 — a dated assumption**: as of **2026-06-09**, `fct_space_creation` is **0 rows in production** (`space_creation_enabled` is not set in `dbt_project.yml` nor passed as a job-level var; live row count = 0). The T re-derive reproduces this by passing no enable var. **If a future operator enables `space_creation` in production (e.g. a per-run job var), the re-derive tool must be updated to inject `space_creation_enabled=true`** — otherwise T's plain build would shrink the mart back to 0 rows. Record this so the coupling is discoverable;
  - and that **no scheduling-policy/materialization changed**. Reference the spec (rev 5).

- [ ] **Step 2: Add CLAUDE.md pointer** under "## Database Performance → Lakebase" bullets:
```
- **Re-deriving a TRIGGERED-synced mart**: never `dbt --full-refresh` it directly (strands the synced table; the on-run-start tripwire now aborts any `--full-refresh` selecting a TRIGGERED mart — including the mega-job `dbt_full_refresh=true` parameter on stages 2/3). Use `uv run --extra sdk python scripts/rederive_synced_marts.py --select <sel> [--provider P | --match-ids …]` — D marts MERGE-reprocess (no downtime), other TRIGGERED marts rebuild+recreate; `--rebuild` full-rebuilds a D mart for a schema/contract change. See ADR-043.
```

- [ ] **Step 3: Bump the wheel (REQUIRED — the dbt project ships IN the wheel).** `dbt_project/` (macros,
  models, `dbt_project.yml`) is bundled into the wheel via `[tool.hatch.build.targets.wheel.force-include]`
  (`luxury_lakehouse_dbt_project/`), and the daily `dbt_build` task resolves it from the installed wheel via
  `importlib.resources`. So the new tripwire macro, mart pre_hooks, and `on-run-start`/registry var only reach
  the daily Databricks job once a new wheel is built+deployed (main-branch CI "Deploy wheel" step on merge).
  Bump `pyproject.toml` `version` (0.5.23 → **0.5.24**, patch-per-PR convention), then
  `uv run python scripts/bump_wheel.py` (propagates to `src/shared/wheel.py` + PEP-723 scripts + terraform),
  verify `uv run python scripts/bump_wheel.py --check` and `uv run pytest src/tests/test_wheel_constants.py`.
  **Done 2026-06-09: 0.5.24, 28 files synced, --check + test_wheel_constants (16) green.**

- [ ] **Step 4: Write the memory file** `project_strand_safe_rederive_pr.md` + one-line MEMORY.md index entry (after the PR merges / at commit time).

---

## Task 9: Full local gate + live tripwire verification

- [ ] **Step 1: Lint + format + types**

Run:
```
uv run ruff check src/ scripts/
uv run ruff format --check src/ scripts/
uv run pyright src/ scripts/rederive_synced_marts.py
```
Expected: zero violations. (Note `scripts/` typing: match the existing scripts' annotation level.)

- [ ] **Step 2: Targeted + full unit suite**

Run: `uv run pytest src/tests/test_rederive_planner.py src/tests/test_rederive_synced_marts_cli.py src/tests/test_strand_safe_rederive.py src/tests/test_synced_table_heal_e2e.py -q`
Then a **real full-suite execution** (NOT `--collect-only`): `uv run pytest src/tests -q`.
Expected: all pass / pre-existing-only skips (e.g. the local SDK-0.77 `test_migrate_synced_tables` artifact noted in memory — `--ignore` it if it trips locally).

> **POST-MERGE-CI FIX (learned the hard way):** the first push ran `--collect-only` here, which does NOT
> execute `test_dbt_mart_classification` / `test_dbt_stage_selector_coverage` — two meta-tests that
> **text-parse every mart's `config()` block**. Their naive `\{\{ config\((.+?)\)\s*\}\}` regex closed on the
> FIRST `) }}`, and the new `pre_hook="{{ reprocess_delete_hook('match_id') }}"` is the first mart hook to put
> a macro call (ending `) }}`) inside `config()` → the regex truncated the body before `tags=[...]` → both
> failed in CI. Fix: replaced the lazy regex with a quote/paren-aware `_config_body` scan in both meta-tests
> (dbt parsed fine — only the text guards were naive). **Always run the real suite for dbt-config edits.**

- [ ] **Step 3: `_BRONZE_SCHEMA` — live-confirmed `bronze` (NOT `dev_bronze`)**

`soccer_analytics.bronze.spadl_actions` confirmed live during planning (9.7M rows; `dev_bronze` does not
exist). `_BRONZE_SCHEMA = "bronze"` is set accordingly — no further action unless a future env differs.

- [ ] **Step 4: Live tripwire verification — via `dbt run` (NOT `dbt compile`) — VERIFIED 2026-06-09**

**Important (learned during execution):** `on-run-start` hooks do NOT run in execute mode under `dbt compile`
(compile doesn't materialize, so it can't strand and the hook is inert). The tripwire fires under
`dbt run` / `dbt build --full-refresh` — and it aborts **at on-run-start, before any model builds**, so a
`dbt run --full-refresh` of a TRIGGERED mart is a **safe** verification (nothing is overwritten).
```
cd dbt_project
dbt run --full-refresh --select fct_passes        # EXPECT: Compilation Error "Refusing --full-refresh of
                                                  #   TRIGGERED synced source(s) fct_passes" — aborts BEFORE build
                                                  #   (VERIFIED 2026-06-09: full_refresh=True, selected fct_passes, raised)
dbt run --full-refresh --select fct_pausa_values  # EXPECT: same abort (table mart)
```
The "allowed" paths are NOT run destructively here: a **plain** `dbt build` (no `--full-refresh`) of these
marts is exactly what the daily stage-3 does in production and does NOT abort (production-proven); the
`allow_triggered_full_refresh` override is exercised by the tool's B path. (`dbt compile --full-refresh` does
NOT abort — on-run-start is inert under compile — so it cannot be used to verify the tripwire.)

- [ ] **Step 5: Live macro-render check (C1) — assert the reprocess macros actually render**

```
cd dbt_project
dbt compile --select fct_action_values --vars '{reprocess_match_ids: [1, 2], allow_triggered_full_refresh: true}'
# Then grep the compiled SQL:
grep -E "delete from .*where match_id in \(1, 2\)" target/compiled/soccer_analytics/models/marts/fct_action_values.sql   # pre_hook rendered
grep -E "or match_id in \(1, 2\)" target/compiled/soccer_analytics/models/marts/fct_action_values.sql                    # predicate rendered
# And the no-var case renders NEITHER:
dbt compile --select fct_action_values
grep -c "or match_id in" target/compiled/soccer_analytics/models/marts/fct_action_values.sql   # EXPECT: 0
```
> `allow_triggered_full_refresh` is passed only to satisfy the tripwire during the *compile* (compile of an incremental without `--full-refresh` won't trip it anyway; harmless). The pre_hook DELETE renders into the model's compiled SQL because `reprocess_delete_hook` is in `config(pre_hook=...)`.
>
> **C-2 (accepted limitation):** macro *rendering* is NOT a CI regression gate — `dbt compile` needs a dbt profile + a live Databricks connection (the adapter resolves `this`/relations), so it can't run in offline CI. The regression guards are: the offline string-presence test (Task 6) + this live grep. A future edit that makes a macro render empty would pass offline CI; it is caught here and by the e2e D-mechanism proof (Task 7). Documented so this gap is known, not silent.

- [ ] **Step 6: Live C-3 check — `refresh_synced_tables --tables <TRIGGERED> --wait` terminates on CDF completion**

The D/T paths call `refresh_synced_tables --tables <synced> --wait` to propagate a TRIGGERED incremental
refresh. Confirm `--wait` returns only after the TRIGGERED pipeline's latest update reaches `COMPLETED`
(not prematurely), so the tool doesn't print "done" while Lakebase is still consuming CDF. (Code path:
`main()` has no `scheduling_policy` filter — C4 — and `_poll_pipeline` keys terminal on
`latest_updates[0].state == "COMPLETED"`, which a TRIGGERED incremental update reaches.)
```
uv run python -m ingestion.refresh_synced_tables --tables fct_action_values_synced --wait
# EXPECT: "fct_action_values_synced: COMPLETE" and exit 0 only after the incremental update finishes.
```

- [ ] **Step 7: Verify the dry-run plan**

Run: `uv run --extra sdk python scripts/rederive_synced_marts.py --select "fct_action_values fct_pausa_values" --match-ids 1 --dry-run`
(NB: the selector is passed verbatim to `dbt ls` — dbt union is **space**-separated; a comma is dbt
*intersection* and yields no models.)
Expected: `[D] fct_action_values` (downtime none), `[T] fct_pausa_values` (downtime none), no changes made.
Also: `--select fct_action_values --rebuild --dry-run` → `[B] fct_action_values` (rebuild routes D→B);
`--select fct_pausa_values --rebuild --dry-run` → `[B] fct_pausa_values` (rebuild routes T→B).

> Report Steps 4–7 results to the user before any data-plane execution. Per CLAUDE.md, the actual re-derive of real marts is a separate operator-approved action, NOT part of this PR.

---

## Task 10: Commit + PR (REQUIRES EXPLICIT USER APPROVAL)

> Per CLAUDE.md + memory `feedback_no_commits_without_explicit_approval`: do NOT commit, push, or open a PR without separate explicit approval at this moment. This is one feature → **one commit** (user instruction overrides the skill's frequent-commit default).

- [ ] **Step 1: Pre-commit branch hygiene** — `git fetch origin && git status && git log --oneline origin/main..HEAD`; branch from fresh `main` if not already on a feature branch (`git checkout -b feat/strand-safe-synced-rederive`).

- [ ] **Step 2: Stage + single commit** (only after approval), message body covering the D/B design + ADR-043 + tripwire; co-author trailer per repo convention. Use the `git commit -F tmp/<msg>.txt` Bash-tool pattern (memory `feedback_git_commit_via_bash_tool`).

- [ ] **Step 3: Push + open PR** (only after approval). PR body: summary, the 13 TRIGGERED marts D/B split, tripwire, test matrix, and an explicit "no scheduling-policy/materialization changed" line.

- [ ] **Step 4: Run `mad-scientist-skills:final-review`** (C4 diagram + ADR scan) before requesting merge, per the project's pre-commit gate.

---

## Self-Review

**Spec (rev 5) coverage:**
- §2 D/T/B split → Tasks 2, 4 (`D_REPROCESS_MODELS`, `_TABLE_MARTS`), 6 (3-way partition test). B1 (tracking_frames inherited key) → Task 2 Step 7 note + Task 7 spike. No enable-var injection → Task 4 (D passes only `reprocess_match_ids`; T passes `{}`; B passes `allow_triggered_full_refresh`).
- §3 D macros (predicate + delete_hook, N1, parenthesization, m2 scalar coercion) → Task 1 + Task 2 (parenthesized at every `not in` site, incl. tracking_frames ×3 arms).
- §4 single tool + planner/executor split (N-c), **T plain-rebuild action (B-1/C-1)**, `--dry-run` (N-d), `--rebuild` (C3/m-3 — routes D *and* T → B), job-state guard (T4), per-mart downtime (T5), `--quiet` (m3) → Tasks 4 + 5.
- §5 **unified `--full-refresh` tripwire** (P1 fix), `selected_resources` (N-a), flat-list registry var keyed on model name (N-b) → Task 3.
- §6 static guards: exhaustive **D/T/B partition** (T3) + table-vs-incremental check, registry parity, **CDF=='true' coverage (C2/m-1)**, no-bare-full-refresh scan incl. **`terraform/` (P2)** with the tag-bypass caveat (m-2), tripwire wiring → Task 6.
- §8 testing: macro/planner units, **live tripwire + live macro-render grep (C1, not CI-gated — C-2 note)**, e2e positive no-strand (m1 unique names — **B-2 fix**), **C-3 live `--wait`-on-TRIGGERED check** → Tasks 1,4,5,7,9.
- §9 P2 documented breaking change → Task 8 ADR + CLAUDE.md pointer. No policy/materialization change; no GH token.
- §10 risks (N2 CDF; space_creation 0-row reproduced by T; mechanism caveat) → Task 6 CDF test + ADR.
- **C4** (refresh_synced_tables policy-agnostic) → confirmed from source; **m4** (`_warehouse_id`) → confirmed live; `_BRONZE_SCHEMA="bronze"` → live-confirmed.

**Evidence-driven changes across reviews:** rev 3→4: P1 + "does a routine table build strand?" resolved via live `DESCRIBE HISTORY` + strand ledger (only `--full-refresh` strands) → unified tripwire + enable-var injection dropped. rev 4→5: B-1 (`fct_space_creation` has **no node-level `enabled=`** — verified — so it always builds 0 rows; the delete-then-build-nothing footgun was illusory) + C-1 (table marts plain-safe but were routed to heavy B) → added the **T (plain rebuild) action**: zero-downtime, no synced delete, matches the strand-free daily build. B-2 (e2e queried the wrong table) fixed.

**Placeholder scan:** none — all code blocks complete; `_BRONZE_SCHEMA="bronze"` live-confirmed.

**Type consistency:** `PlanStep(model, synced_table, action: "D"|"T"|"B", full_refresh, dbt_vars)` identical in planner (Task 4) and executor (Task 5, which dispatches all three: `_execute_d`/`_execute_t`/`_execute_b`). `plan_rederive(selected_models, match_ids, *, rebuild=False)` matches all call sites. `_downtime_estimate(model, action)` handles D/T/B; `_validate_match_ids`, `_parse_model_names` consistent between impl and tests.
