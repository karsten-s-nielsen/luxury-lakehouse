# PR-β: Three-Stage `dbt_build` TF Restructure + CAN_RUN Auto-Heal Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Land PR-β of PR-Cycle-C as a single squash commit bundling three phases: Phase 0 fixes the career-mart v1 filter that PR-α introduced as a no-op; Phase 1 fulfills ADR-019 by replacing the single `dbt_build` Databricks task with three sequential dbt invocations driven by the mart classification tags PR-α applied; Phase 2 adds ADR-020 — a new step to `.github/workflows/lakebase-grants.yml` that re-applies CAN_RUN on synced-table backing pipelines so workspace-API ACL drift after UI recreation auto-heals.

**Architecture:**
- **Phase 0** is a 2-line SQL fix in two mart files. Replaces the wrong-column filter (`data_source != 'football2vec_v1'`) with the correct dim-based filter (`size(behavioral_vector) != 32`).
- **Phase 1** replaces one Databricks task with three (`dbt_build_input_marts` → `dbt_build_intermediate_marts` → `dbt_build_output_marts`), reorders compute task `depends_on` so gold-reader compute waits on stage 1 / stage 2, removes 13 stale gold-reader edges from PR #242's audit, and adds a new gold-read conformance test peer to the PR #242 bronze-read test. Three workflow cards replace `wf-dbt-build.yaml`.
- **Phase 2** adds one CI step to a self-healing workflow + a new ADR documenting the forcing function (today's 403s on UI-recreated synced tables).

**Tech Stack:** Terraform HCL (databricks_job resource), Python 3.10 (pytest, regex-based TF parsing — no terraform CLI dependency), dbt-core (selectors), GitHub Actions YAML.

---

## File Structure

| File | Action | Responsibility |
|---|---|---|
| `dbt_project/models/marts/fct_player_embeddings_career.sql` | Modify | Phase 0: fix `player_best_dim` CTE filter |
| `dbt_project/models/marts/fct_player_embeddings_season.sql` | Modify | Phase 0: fix `player_best_dim` CTE filter |
| `terraform/modules/workflows/main.tf` | Modify | Phase 1: replace `dbt_build` with 3 tasks; reorder 9 compute `depends_on`; update `refresh_synced_tables` + `run_model_validation` deps |
| `src/tests/test_workflow_dag_gold_reads.py` | Create | Phase 1: gold-read conformance test peer to bronze-reads |
| `src/tests/test_terraform_workflow_dbt_task.py` | Modify | Phase 1: assert on three-task topology, not single `dbt_build` |
| `src/tests/test_workflows_tf_ordering.py` | Modify | Phase 1: task count anchor 31 → 33 |
| `src/tests/test_card_parity_with_terraform.py` | Modify | Phase 1: add 3 new task → card mappings |
| `workflow-cards/wf-dbt-build.yaml` | Delete | Phase 1: split into 3 cards |
| `workflow-cards/wf-dbt-build-input-marts.yaml` | Create | Phase 1: stage 1 card |
| `workflow-cards/wf-dbt-build-intermediate-marts.yaml` | Create | Phase 1: stage 2 card |
| `workflow-cards/wf-dbt-build-output-marts.yaml` | Create | Phase 1: stage 3 card |
| `docs/superpowers/adrs/ADR-019-three-stage-dbt-build.md` | Modify | Phase 1: add "Implementation status" line referencing PR-β |
| `.github/workflows/lakebase-grants.yml` | Modify | Phase 2: add 4th self-healing step running `grant_synced_table_permissions.py` |
| `docs/superpowers/adrs/ADR-020-lakebase-canrun-autoheal.md` | Create | Phase 2: new ADR codifying the self-healing pattern |

`scripts/grant_synced_table_permissions.py` is **invoked** by Phase 2 but **not modified** — its CAN_RUN logic is already idempotent and current.

---

## Locked DAG topology (Phase 1)

Stage flow:

```
ingest_*  +  resolve_players  +  backfill_*  +  extract_tracking_metadata  →  dbt_build_input_marts
                                                                                       ↓
                                                                          [stage-1 marts: dim_*, fct_tracking_frames, fct_shots, fct_discipline_events]
                                                                                       ↓
                              compute_pitch_control / compute_off_ball_xt / compute_xg_model[_v2] /
                              compute_formations_efpi / compute_formations_shape_graph / compute_line_breaking  +  compute_spadl_vaep
                                                                                       ↓
                                                                          dbt_build_intermediate_marts
                                                                                       ↓
                                                                          [stage-2 marts: fct_action_values]
                                                                                       ↓
                              compute_embeddings_v2  +  rest of phase-2 compute (defcon, embeddings_v1, embeddings_360, pausa, elastic, expected_threat, hf_sync)
                                                                                       ↓
                                                                          dbt_build_output_marts
                                                                                       ↓
                                                                          ↗  refresh_synced_tables   (sibling)
                                                                          ↘  run_model_validation   (sibling — ADR-019 supplants ADR-017's yesterday-gold carve-out)
```

### Locked `depends_on` table — every changed task

| Task | Existing depends_on | New depends_on (PR-β) | Δ |
|---|---|---|---|
| `dbt_build_input_marts` | (new task) | `backfill_statsbomb_360`, `backfill_statsbomb_extra`, `extract_tracking_metadata`, `ingest_idsse`, `ingest_idsse_events`, `ingest_metrica`, `ingest_skillcorner`, `ingest_statsbomb`, `ingest_wyscout`, `resolve_players` | NEW |
| `dbt_build_intermediate_marts` | (new task) | `compute_spadl_vaep`, `dbt_build_input_marts` | NEW |
| `dbt_build_output_marts` | (new task) | `compute_defcon_lite`, `compute_elastic_sync`, `compute_embeddings_360`, `compute_embeddings_v1`, `compute_embeddings_v2`, `compute_expected_threat`, `compute_formations_efpi`, `compute_formations_shape_graph`, `compute_line_breaking`, `compute_off_ball_xt`, `compute_pausa`, `compute_pitch_control`, `compute_xg_model`, `compute_xg_model_v2`, `dbt_build_intermediate_marts`, `hf_sync` | NEW |
| `dbt_build` | (12 deps; see TF) | (REMOVED) | −1 task |
| `compute_pitch_control` | `ingest_idsse`, `ingest_metrica`, `ingest_skillcorner` | `dbt_build_input_marts` | −3, +1 |
| `compute_off_ball_xt` | `compute_expected_threat`, `ingest_idsse`, `ingest_metrica`, `ingest_skillcorner` | `compute_expected_threat`, `dbt_build_input_marts` | −3, +1 |
| `compute_xg_model` | `compute_spadl_vaep` | `dbt_build_input_marts` | −1, +1 |
| `compute_xg_model_v2` | `compute_spadl_vaep` | `dbt_build_input_marts` | −1, +1 |
| `compute_formations_efpi` | `compute_pitch_control` | `dbt_build_input_marts` | −1, +1 |
| `compute_formations_shape_graph` | `compute_formations_efpi` | `compute_formations_efpi`, `dbt_build_input_marts` | +1 |
| `compute_line_breaking` | `backfill_statsbomb_360`, `ingest_idsse`, `ingest_idsse_events`, `ingest_metrica`, `ingest_statsbomb` | `backfill_statsbomb_360`, `dbt_build_input_marts`, `ingest_idsse`, `ingest_idsse_events`, `ingest_metrica`, `ingest_statsbomb` | +1 |
| `compute_embeddings_v2` | `resolve_players` | `dbt_build_intermediate_marts` | −1, +1 |
| `run_model_validation` | `compute_pausa` | `dbt_build_output_marts` | −1, +1 |
| `refresh_synced_tables` | `dbt_build` | `dbt_build_output_marts` | rename |

13 stale gold-reader edges removed (per PR #242 audit + spec §5):

1. `compute_pitch_control → ingest_idsse`
2. `compute_pitch_control → ingest_metrica`
3. `compute_pitch_control → ingest_skillcorner`
4. `compute_off_ball_xt → ingest_idsse`
5. `compute_off_ball_xt → ingest_metrica`
6. `compute_off_ball_xt → ingest_skillcorner`
7. `compute_embeddings_v2 → resolve_players`
8. `compute_formations_efpi → compute_pitch_control`
9. `compute_xg_model → compute_spadl_vaep`
10. `compute_xg_model_v2 → compute_spadl_vaep`
11. `run_model_validation → compute_pausa`

Plus 2 implicit removals through replacement:
12. `refresh_synced_tables → dbt_build` (replaced by `→ dbt_build_output_marts`)
13. `dbt_build` task itself (replaced by 3 new tasks — its 12 incoming edges become deps on `dbt_build_output_marts` indirectly via the new topology)

### dbt selector strings (per spec §5)

| Stage | TF `parameters` |
|---|---|
| Stage 1 | `["--select", "+tag:input_mart", "+tag:dimension"]` |
| Stage 2 | `["--select", "+tag:intermediate_mart"]` |
| Stage 3 | `["--select", "tag:output_mart"]` |

`+tag:X` builds X and ancestors (staging views, seeds). Stage 3 uses no leading `+` because all ancestors are built by stages 1+2 already (per spec §5).

### Task count anchor

Today: **31 tasks** on `databricks_job.data_ingestion`. PR-β adds 3, removes 1 → **33 tasks**.

---

## Single commit policy

This entire plan accumulates changes locally and lands as **one commit** at the end (per repo convention `feedback_no_commits_without_explicit_approval.md`). No per-task `git commit` steps. The final commit is **sentinel-gated** — the user must run `!touch ~/.claude-git-approval` before the commit step succeeds.

---

## Phase 0 — Career mart v1 filter fix

PR-α added `where data_source != 'football2vec_v1'` to two career-mart CTEs. The filter is a no-op because `data_source` is the **provider label** (`'wyscout'`, `'statsbomb'`, `'football2vec_360'`), not the model version — v1 32d wyscout rows and v2 192d wyscout rows share `data_source='wyscout'`. The correct filter is dim-based. Verifiable post-merge: `select size(behavioral_vector), count(*) from soccer_analytics.dev_gold.fct_player_embeddings_career group by 1` → currently `(32, 147)` + `(192, 9618)`; post-fix → `(192, 9765)`.

### Task 0.1: Fix `fct_player_embeddings_career.sql` filter

**Files:**
- Modify: `dbt_project/models/marts/fct_player_embeddings_career.sql:29-32`

- [ ] **Step 0.1.1: Replace the v1 label filter with dim-based filter**

Edit `fct_player_embeddings_career.sql`. The current `player_best_dim` CTE contains:

```sql
    where data_source != 'football2vec_360'
      and data_source != 'football2vec_v1'   -- PR-Cycle-C 2026-05-01: exclude 32d v1 Doc2Vec.
                                             -- v1 is "Retained for comparison; superseded by v2"
                                             -- per terraform/modules/workflows/main.tf:22-24.
                                             -- Mixed-dim career rows broke HNSW build at vector(192).
    group by canonical_player_id
```

Replace those four lines (the second filter and its 3-line comment) with:

```sql
    where data_source != 'football2vec_360'
      and size(behavioral_vector) != 32   -- PR-Cycle-C 2026-05-02 (PR-β Phase 0): exclude 32d v1 Doc2Vec.
                                          -- data_source is provider label ('wyscout'/'statsbomb'/etc),
                                          -- NOT model version — v1 + v2 wyscout share data_source='wyscout',
                                          -- so PR-α's `data_source != 'football2vec_v1'` filter was a no-op.
                                          -- Dim-based filter is robust + version-agnostic.
                                          -- Mixed-dim career rows broke HNSW build at vector(192).
    group by canonical_player_id
```

- [ ] **Step 0.1.2: Verify the file parses**

Run: `uvx --from "dbt-core>=1.10.0,<1.12.0" --with "dbt-databricks>=1.10.0,<1.12.0" dbt parse --project-dir dbt_project --profiles-dir dbt_project --target serverless`

Expected: no parse errors. (May warn about missing creds — that's fine; we're just parsing, not running.)

If `dbt parse` requires credentials and fails locally, skip this step — `dbt parse` will run in CI on PR open.

### Task 0.2: Fix `fct_player_embeddings_season.sql` filter

**Files:**
- Modify: `dbt_project/models/marts/fct_player_embeddings_season.sql:31-34`

- [ ] **Step 0.2.1: Replace the v1 label filter with dim-based filter**

Same edit as Task 0.1 but in the season mart. Find:

```sql
    where data_source != 'football2vec_360'
      and data_source != 'football2vec_v1'   -- PR-Cycle-C 2026-05-01: exclude 32d v1 Doc2Vec.
                                             -- v1 is "Retained for comparison; superseded by v2"
                                             -- per terraform/modules/workflows/main.tf:22-24.
                                             -- Mixed-dim career rows broke HNSW build at vector(192).
    group by canonical_player_id
```

Replace with:

```sql
    where data_source != 'football2vec_360'
      and size(behavioral_vector) != 32   -- PR-Cycle-C 2026-05-02 (PR-β Phase 0): exclude 32d v1 Doc2Vec.
                                          -- data_source is provider label ('wyscout'/'statsbomb'/etc),
                                          -- NOT model version — v1 + v2 wyscout share data_source='wyscout',
                                          -- so PR-α's `data_source != 'football2vec_v1'` filter was a no-op.
                                          -- Dim-based filter is robust + version-agnostic.
                                          -- Mixed-dim career rows broke HNSW build at vector(192).
    group by canonical_player_id
```

---

## Phase 1 — TF DAG restructure (ADR-019 fulfillment)

### Task 1.1: Update `test_workflows_tf_ordering.py` task count anchor (TDD red phase first)

**Files:**
- Modify: `src/tests/test_workflows_tf_ordering.py:245`

This is the canonical task-count anchor for the daily-job. Updating it FIRST (before any TF edit) puts the test into a known-failing red state, which is the TDD discipline the writing-plans skill requires.

- [ ] **Step 1.1.1: Bump the count from 31 to 33**

Find:

```python
    # 30 → 31 in PR-Cycle-B (2026-05-01): split `import_obso_results` out of
    # hf_sync into its own scheduled task so compute_pausa can declare an
    # explicit dependency on the OBSO import.
    assert len(task_keys) == 31, f"expected 31 task blocks on data_ingestion, parser found {len(task_keys)}"
```

Replace with:

```python
    # 30 → 31 in PR-Cycle-B (2026-05-01): split `import_obso_results` out of
    # hf_sync into its own scheduled task so compute_pausa can declare an
    # explicit dependency on the OBSO import.
    # 31 → 33 in PR-Cycle-C PR-β (2026-05-02): replace single `dbt_build`
    # task with three sequential tasks (`dbt_build_input_marts`,
    # `_intermediate_marts`, `_output_marts`) per ADR-019. Net +2 tasks
    # (3 added, 1 removed).
    assert len(task_keys) == 33, f"expected 33 task blocks on data_ingestion, parser found {len(task_keys)}"
```

- [ ] **Step 1.1.2: Run the test to confirm it now fails (red phase)**

Run: `uv run pytest src/tests/test_workflows_tf_ordering.py::test_data_ingestion_parser_count_anchor -v`

Expected: FAIL with `AssertionError: expected 33 task blocks on data_ingestion, parser found 31`. This confirms the anchor is wired against the un-edited TF — Task 1.5 will turn it green.

### Task 1.2: Rewrite `test_terraform_workflow_dbt_task.py` for three-task topology (TDD red phase)

**Files:**
- Modify: `src/tests/test_terraform_workflow_dbt_task.py`

The current test asserts a single `dbt_build` task with 12 leaf-compute deps. Post-PR-β: three tasks, each with their own deps. Replace the dbt-task block (lines 64-133) with three-task assertions.

- [ ] **Step 1.2.1: Replace the dbt_build task tests**

Replace the entire region from line 64 (`def test_dbt_build_task_exists`) through line 133 (end of `test_refresh_synced_tables_depends_only_on_dbt_build`) with:

```python
# ---------------------------------------------------------------------------
# Three-stage dbt task topology (PR-Cycle-C PR-β, ADR-019)
# ---------------------------------------------------------------------------

_THREE_STAGE_TASKS = ("dbt_build_input_marts", "dbt_build_intermediate_marts", "dbt_build_output_marts")


def test_three_stage_dbt_tasks_exist() -> None:
    """ADR-019: single `dbt_build` is replaced with three sequential
    invocations driven by mart classification tags."""
    src = _read_workflows_main_tf()
    for task_key in _THREE_STAGE_TASKS:
        assert f'task_key        = "{task_key}"' in src or f'task_key = "{task_key}"' in src, (
            f"workflows module must contain a task with task_key = {task_key!r}"
        )
    # The legacy single-task name must NOT exist anymore.
    assert 'task_key        = "dbt_build"' not in src and 'task_key = "dbt_build"' not in src, (
        'Legacy single `dbt_build` task is replaced by the three-stage topology in PR-Cycle-C PR-β. '
        "Remove it."
    )


def test_three_stage_dbt_tasks_use_correct_entry_point_and_environment() -> None:
    """All three stages run the same `dbt_build` entry point; differentiation
    is by the `--select` parameter, not by entry point."""
    src = _read_workflows_main_tf()
    for task_key in _THREE_STAGE_TASKS:
        idx = src.find(f'"{task_key}"')
        assert idx != -1
        # Window ends before the next task block; 2500 is generous given
        # each stage carries up to 16 depends_on entries.
        window = src[idx : idx + 2500]
        assert "python_wheel_task" in window, f"{task_key} must use python_wheel_task"
        assert 'entry_point  = "dbt_build"' in window or 'entry_point = "dbt_build"' in window, (
            f"{task_key} entry_point must be 'dbt_build' (selector differentiates stages)"
        )
        assert 'environment_key = "dbt"' in window, f"{task_key} must use the dbt environment_key"


def test_three_stage_dbt_tasks_use_distinct_select_parameters() -> None:
    """Stage 1 selects input_mart + dimension (with ancestors), stage 2
    selects intermediate_mart (with ancestors), stage 3 selects output_mart
    (no ancestors — they were built by stages 1 + 2)."""
    src = _read_workflows_main_tf()
    expected = {
        "dbt_build_input_marts": ["+tag:input_mart", "+tag:dimension"],
        "dbt_build_intermediate_marts": ["+tag:intermediate_mart"],
        "dbt_build_output_marts": ["tag:output_mart"],
    }
    for task_key, selectors in expected.items():
        idx = src.find(f'"{task_key}"')
        assert idx != -1
        window = src[idx : idx + 2500]
        for sel in selectors:
            assert f'"{sel}"' in window, (
                f"{task_key} must pass --select with selector {sel!r}; window=\n{window[:600]}..."
            )


def test_dbt_build_input_marts_depends_on_all_ingest_helpers() -> None:
    """Stage 1 runs after all bronze-writer ingest tasks (including the
    ingest-helper `compute_*` tasks per ADR-019: `extract_tracking_metadata`,
    backfills, `resolve_players`)."""
    src = _read_workflows_main_tf()
    idx = src.find('"dbt_build_input_marts"')
    assert idx != -1
    window = src[idx : idx + 2500]
    expected_deps = [
        "backfill_statsbomb_360",
        "backfill_statsbomb_extra",
        "extract_tracking_metadata",
        "ingest_idsse",
        "ingest_idsse_events",
        "ingest_metrica",
        "ingest_skillcorner",
        "ingest_statsbomb",
        "ingest_wyscout",
        "resolve_players",
    ]
    missing = [d for d in expected_deps if f'task_key = "{d}"' not in window]
    assert not missing, f"dbt_build_input_marts task missing depends_on entries: {missing}"


def test_dbt_build_intermediate_marts_depends_on_stage1_and_spadl_vaep() -> None:
    """Stage 2 must run after stage 1 (`dbt_build_input_marts`) AND after
    `compute_spadl_vaep` which writes the SPADL/VAEP bronze that
    `fct_action_values` (the only intermediate_mart) consumes."""
    src = _read_workflows_main_tf()
    idx = src.find('"dbt_build_intermediate_marts"')
    assert idx != -1
    window = src[idx : idx + 2500]
    for dep in ("compute_spadl_vaep", "dbt_build_input_marts"):
        assert f'task_key = "{dep}"' in window, (
            f"dbt_build_intermediate_marts must depend on {dep!r}"
        )


def test_dbt_build_output_marts_depends_on_stage2_and_phase2_compute() -> None:
    """Stage 3 must run after stage 2 (`dbt_build_intermediate_marts`)
    AND after every phase-2 compute task that writes bronze read by an
    output_mart."""
    src = _read_workflows_main_tf()
    idx = src.find('"dbt_build_output_marts"')
    assert idx != -1
    window = src[idx : idx + 2500]
    expected_deps = [
        "compute_defcon_lite",
        "compute_elastic_sync",
        "compute_embeddings_360",
        "compute_embeddings_v1",
        "compute_embeddings_v2",
        "compute_expected_threat",
        "compute_formations_efpi",
        "compute_formations_shape_graph",
        "compute_line_breaking",
        "compute_off_ball_xt",
        "compute_pausa",
        "compute_pitch_control",
        "compute_xg_model",
        "compute_xg_model_v2",
        "dbt_build_intermediate_marts",
        "hf_sync",
    ]
    missing = [d for d in expected_deps if f'task_key = "{d}"' not in window]
    assert not missing, f"dbt_build_output_marts task missing depends_on entries: {missing}"


def test_refresh_synced_tables_depends_only_on_dbt_build_output_marts() -> None:
    """After PR-β, `refresh_synced_tables` waits on stage 3 (the final
    dbt invocation), NOT on the legacy single `dbt_build` task."""
    src = _read_workflows_main_tf()
    idx = src.find('"refresh_synced_tables"')
    assert idx != -1
    window = src[idx : idx + 2000]
    assert 'task_key = "dbt_build_output_marts"' in window, (
        "refresh_synced_tables must depend on dbt_build_output_marts (stage 3)"
    )
    # Verify the legacy dep on single `dbt_build` is gone.
    assert 'task_key = "dbt_build"\n' not in window, (
        "refresh_synced_tables must NOT depend on the legacy single `dbt_build` task — "
        "it now depends on `dbt_build_output_marts` (stage 3)."
    )


def test_run_model_validation_depends_on_dbt_build_output_marts() -> None:
    """ADR-019: `run_model_validation` runs as a SIBLING of
    `refresh_synced_tables` (both depend on `dbt_build_output_marts`).
    This supplants ADR-017's yesterday-gold workaround — validation reads
    today's gold but cannot block today's mart refresh."""
    src = _read_workflows_main_tf()
    idx = src.find('"run_model_validation"')
    assert idx != -1
    window = src[idx : idx + 2000]
    assert 'task_key = "dbt_build_output_marts"' in window, (
        "run_model_validation must depend on dbt_build_output_marts (sibling of refresh_synced_tables)"
    )
    # The legacy dep on `compute_pausa` is gone.
    assert 'task_key = "compute_pausa"' not in window, (
        "run_model_validation must NOT depend on compute_pausa anymore — "
        "the new topology covers this transitively via dbt_build_output_marts."
    )
```

- [ ] **Step 1.2.2: Run the new tests to confirm they all fail (red phase)**

Run: `uv run pytest src/tests/test_terraform_workflow_dbt_task.py -v`

Expected: 7 failures (the rewritten test functions all assert TF state that doesn't exist yet); 2 passes (the two `ingestion_sp` tests at the top of the file are unaffected). Task 1.5 turns them all green.

### Task 1.3: Add `dbt_build_*` mappings to `test_card_parity_with_terraform.py` (TDD red phase)

**Files:**
- Modify: `src/tests/test_card_parity_with_terraform.py:174` (the `dbt_build` entry in `_DIRECT_TASK_ENTRY_POINT_TO_CARD`)

- [ ] **Step 1.3.1: Replace the single `dbt_build` mapping with three entries**

Find the line:

```python
    "dbt_build": "wf-dbt-build",
```

Replace with:

```python
    # PR-Cycle-C PR-β (2026-05-02, ADR-019): single `dbt_build` task replaced
    # with three sequential dbt invocations driven by mart classification tags.
    # All three share the same `dbt_build` wheel entry point — differentiated
    # by the `--select` parameter passed in TF. Cards are split 1→3 to match.
    "dbt_build_input_marts": "wf-dbt-build-input-marts",
    "dbt_build_intermediate_marts": "wf-dbt-build-intermediate-marts",
    "dbt_build_output_marts": "wf-dbt-build-output-marts",
```

- [ ] **Step 1.3.2: Run parity tests to confirm they fail (red phase)**

Run: `uv run pytest src/tests/test_card_parity_with_terraform.py -v`

Expected: failures on `test_mapping_matches_tf_task_list` (mapping references 3 TF tasks that don't exist yet, AND TF still has `dbt_build` not in the new mapping) and `test_every_direct_tf_task_has_scheduled_card` (3 cards don't exist yet). These turn green after Tasks 1.5 + 1.6.

NOTE: the `test_every_direct_tf_task_has_scheduled_card` test reads card frontmatter, so cards have to exist on disk for that test to pass. Task 1.6 creates them.

### Task 1.4: Create the gold-read conformance test (TDD red phase)

**Files:**
- Create: `src/tests/test_workflow_dag_gold_reads.py`

This is the peer to `src/tests/test_workflow_dag_bronze_reads.py`. Same parser, same closure logic, different curated requirement set.

- [ ] **Step 1.4.1: Create the gold-read conformance test**

Create `src/tests/test_workflow_dag_gold_reads.py`:

```python
"""Conformance test: every today's-gold mart read by a Databricks compute
task must have a transitive ``depends_on`` path to the dbt stage that
builds it.

Peer to ``test_workflow_dag_bronze_reads.py`` (PR-Cycle-B, PR #242).
ADR-019's "compute reads today's gold" principle: any compute task that
reads ``gold.fct_*`` must wait on the dbt stage that produced that mart,
otherwise the compute output silently uses yesterday's gold.

Curated rather than auto-discovered — same rationale as the bronze-read
peer (string-literal false positives, write-vs-read fingerprinting).

Pure parse of ``terraform/modules/workflows/main.tf``. No Databricks
connection, no module imports. Reuses the parser from the bronze-read
peer.

References:
- ADR-019 — Three-Stage dbt_build for Same-Day Gold-Reader Compute
- src/tests/test_workflow_dag_bronze_reads.py — bronze-read peer
- docs/superpowers/specs/2026-05-01-option-b-three-stage-dbt-build-design.md §6.2
"""

from __future__ import annotations

from pathlib import Path

from src.tests.test_workflow_dag_bronze_reads import (  # type: ignore[import-not-found]
    _parse_task_depends_on,
    _transitive_closure,
)

_REPO = Path(__file__).resolve().parents[2]
_TF_FILE = _REPO / "terraform" / "modules" / "workflows" / "main.tf"

# ──────────────────────────────────────────────────────────────────────────────
# Curated gold-read requirements.
#
# Format: (consumer_task, gold_mart, expected_dbt_stage)
#
# Each entry asserts that ``consumer_task`` has a transitive ``depends_on``
# path to ``expected_dbt_stage``. ``gold_mart`` is documentary — it identifies
# WHICH read motivates the dependency.
#
# When a new compute task starts reading a gold mart, add an entry here.
# When a read is removed, remove the entry. The test fails loudly either way.
#
# Stage assignments per ADR-019 mart classification tags:
# - dbt_build_input_marts: dim_*, fct_tracking_frames, fct_shots, fct_discipline_events
# - dbt_build_intermediate_marts: fct_action_values
# - dbt_build_output_marts: every other mart
# ──────────────────────────────────────────────────────────────────────────────

_GOLD_READ_REQUIREMENTS: list[tuple[str, str, str]] = [
    # ── compute_pitch_control: reads input_mart fct_tracking_frames ────────
    # Spearman 2017 pitch-control surfaces over fct_tracking_frames frames.
    ("compute_pitch_control", "fct_tracking_frames", "dbt_build_input_marts"),
    # ── compute_off_ball_xt: reads input_mart fct_tracking_frames ──────────
    # Off-ball xT computed over tracking frames.
    ("compute_off_ball_xt", "fct_tracking_frames", "dbt_build_input_marts"),
    # ── compute_xg_model + compute_xg_model_v2: read input_mart fct_shots ──
    # Both v1 (XGBoost) and v2 (Deep Sets) score from fct_shots gold.
    ("compute_xg_model", "fct_shots", "dbt_build_input_marts"),
    ("compute_xg_model_v2", "fct_shots", "dbt_build_input_marts"),
    # ── compute_formations_efpi: reads input_mart fct_tracking_frames ──────
    # EFPI template matching reads tracking frames.
    ("compute_formations_efpi", "fct_tracking_frames", "dbt_build_input_marts"),
    # ── compute_formations_shape_graph: reads input_mart fct_tracking_frames
    # Sotudeh 2026 shape-graph detector reads tracking frames.
    ("compute_formations_shape_graph", "fct_tracking_frames", "dbt_build_input_marts"),
    # ── compute_line_breaking: gold-side reads stay input_mart-only ────────
    # Path A reads bronze.statsbomb_360 (covered by bronze-read peer);
    # gold-side queries against fct_tracking_frames keep the dependency
    # on stage 1.
    ("compute_line_breaking", "fct_tracking_frames", "dbt_build_input_marts"),
    # ── compute_embeddings_v2: reads intermediate_mart fct_action_values ───
    # ML inference reads SPADL/VAEP action values from gold (intermediate stage).
    ("compute_embeddings_v2", "fct_action_values", "dbt_build_intermediate_marts"),
    # ── run_model_validation: reads output_marts ──────────────────────────
    # ADR-019 supplants ADR-017's yesterday-gold carve-out: validation now
    # reads TODAY's gold, but its sibling-of-refresh_synced_tables position
    # under dbt_build_output_marts means a validation regression cannot
    # block today's mart refresh. The "signal not gate" guarantee is
    # preserved by topology, not by stale reads.
    ("run_model_validation", "fct_xg_predictions_v2", "dbt_build_output_marts"),
    ("run_model_validation", "fct_pausa_values", "dbt_build_output_marts"),
]


# ──────────────────────────────────────────────────────────────────────────────
# Tests.
# ──────────────────────────────────────────────────────────────────────────────


def test_every_gold_read_has_transitive_depends_on_path() -> None:
    """For each curated (consumer, gold_mart, expected_dbt_stage) requirement,
    the consumer task's transitive ``depends_on`` closure must contain the
    expected dbt stage. Catches the same-day-gold-reader-edge class going
    forward (peer to PR-Cycle-B's bronze-read conformance test)."""
    deps = _parse_task_depends_on(_TF_FILE.read_text(encoding="utf-8"))
    errors: list[str] = []
    for consumer, gold_mart, expected_stage in _GOLD_READ_REQUIREMENTS:
        if consumer not in deps:
            errors.append(
                f"{consumer!r} not found in TF data_ingestion job — "
                f"requirement (consumer={consumer!r}, gold_mart={gold_mart!r}, "
                f"stage={expected_stage!r}) is unsatisfiable."
            )
            continue
        closure = _transitive_closure(deps, consumer)
        if expected_stage not in closure:
            errors.append(
                f"{consumer!r} reads gold.{gold_mart} (built by {expected_stage!r}) "
                f"but has no transitive depends_on path to {expected_stage!r}. "
                f"Closure: {sorted(closure)}. "
                f"Add `depends_on {{ task_key = {expected_stage!r} }}` to {consumer!r} "
                f"in terraform/modules/workflows/main.tf."
            )
    assert not errors, "\n\n".join(errors)


def test_gold_read_consumers_present_in_tf() -> None:
    """Anchor: every consumer in ``_GOLD_READ_REQUIREMENTS`` must be parseable
    from the TF file. Guards against a parser regression silently producing
    an empty deps dict."""
    deps = _parse_task_depends_on(_TF_FILE.read_text(encoding="utf-8"))
    consumers = {c for c, _m, _s in _GOLD_READ_REQUIREMENTS}
    consumer_missing = consumers - set(deps.keys())
    assert not consumer_missing, (
        f"Consumer tasks missing from TF parse output: {sorted(consumer_missing)}. "
        f"Either the TF lost the task or the parser has a regression. "
        f"Parsed tasks: {sorted(deps.keys())}"
    )


def test_three_stage_dbt_tasks_present_in_tf() -> None:
    """Anchor: the three dbt stage tasks must exist in TF. Without these,
    every gold-read requirement is unsatisfiable."""
    deps = _parse_task_depends_on(_TF_FILE.read_text(encoding="utf-8"))
    expected_stages = {"dbt_build_input_marts", "dbt_build_intermediate_marts", "dbt_build_output_marts"}
    parsed_keys = set(deps.keys())
    # Stages may appear as deps without having their own deps entries — that's fine.
    # We ALSO accept them appearing as consumer keys. A stage missing from BOTH is a bug.
    stages_seen_as_consumer_or_dep: set[str] = set(deps.keys())
    for dep_set in deps.values():
        stages_seen_as_consumer_or_dep |= dep_set
    missing_stages = expected_stages - stages_seen_as_consumer_or_dep
    assert not missing_stages, (
        f"Three-stage dbt tasks missing from TF: {sorted(missing_stages)}. "
        f"Parsed tasks: {sorted(parsed_keys)}"
    )
```

- [ ] **Step 1.4.2: Run the new test to confirm it fails (red phase)**

Run: `uv run pytest src/tests/test_workflow_dag_gold_reads.py -v`

Expected: 3 failures (consumers exist in TF, but the 3 dbt stage tasks don't yet, AND none of the compute consumers have transitive deps to the new stages). All turn green after Task 1.5.

### Task 1.5: Restructure `terraform/modules/workflows/main.tf`

**Files:**
- Modify: `terraform/modules/workflows/main.tf` (multiple regions)

This is the heaviest task. Two strategies for safety:
1. Edit each region with surgical Edit tool calls (preferred — preserves alphabetical task ordering and HCL formatting).
2. Read the full file and `Write` a wholly-new version (avoid — too risky, terraform fmt may reformat differently).

We use strategy 1.

- [ ] **Step 1.5.1: Update `compute_pitch_control` depends_on**

Edit `terraform/modules/workflows/main.tf`. Find the `compute_pitch_control` task block (around line 443) and replace its three `depends_on` blocks:

```hcl
    depends_on {
      task_key = "ingest_idsse"
    }
    depends_on {
      task_key = "ingest_metrica"
    }
    depends_on {
      task_key = "ingest_skillcorner"
    }
```

with a single new edge:

```hcl
    # PR-Cycle-C PR-β (2026-05-02, ADR-019): reads gold.fct_tracking_frames
    # (input_mart) — wait on stage 1 to ensure today's tracking frames are
    # built before pitch-control compute. Drops the legacy ingest_* edges
    # which were pre-three-stage gold-reader workarounds.
    depends_on {
      task_key = "dbt_build_input_marts"
    }
```

- [ ] **Step 1.5.2: Update `compute_off_ball_xt` depends_on**

Find the `compute_off_ball_xt` task block (around line 373). Current:

```hcl
    depends_on {
      task_key = "compute_expected_threat"
    }
    depends_on {
      task_key = "ingest_idsse"
    }
    depends_on {
      task_key = "ingest_metrica"
    }
    depends_on {
      task_key = "ingest_skillcorner"
    }
```

Replace with:

```hcl
    # PR-Cycle-C PR-β (2026-05-02, ADR-019): reads gold.fct_tracking_frames
    # (input_mart) + bronze xT grids from compute_expected_threat. Drops the
    # legacy ingest_* edges (pre-three-stage gold-reader workarounds).
    # Order: alphabetical (test_workflows_tf_ordering enforcement).
    depends_on {
      task_key = "compute_expected_threat"
    }
    depends_on {
      task_key = "dbt_build_input_marts"
    }
```

- [ ] **Step 1.5.3: Update `compute_xg_model` depends_on**

Find the `compute_xg_model` task block (around line 512). Current:

```hcl
    depends_on {
      task_key = "compute_spadl_vaep"
    }
```

Replace with:

```hcl
    # PR-Cycle-C PR-β (2026-05-02, ADR-019): reads gold.fct_shots (input_mart)
    # — wait on stage 1. Drops the legacy compute_spadl_vaep edge which was a
    # serialization-not-data-flow remnant (xg model reads gold.fct_shots, not
    # bronze.spadl_actions).
    depends_on {
      task_key = "dbt_build_input_marts"
    }
```

- [ ] **Step 1.5.4: Update `compute_xg_model_v2` depends_on**

Find the `compute_xg_model_v2` task block (around line 534). Same edit as Task 1.5.3 — replace the single `depends_on { task_key = "compute_spadl_vaep" }` with the same `dbt_build_input_marts` block.

- [ ] **Step 1.5.5: Update `compute_formations_efpi` depends_on**

Find the `compute_formations_efpi` task block (around line 275). Current:

```hcl
    depends_on {
      task_key = "compute_pitch_control"
    }
```

Replace with:

```hcl
    # PR-Cycle-C PR-β (2026-05-02, ADR-019): reads gold.fct_tracking_frames
    # (input_mart). The legacy compute_pitch_control edge was a peer
    # serialization remnant — formations_efpi does NOT read pitch_control
    # bronze (would be in test_workflow_dag_bronze_reads if it did).
    depends_on {
      task_key = "dbt_build_input_marts"
    }
```

- [ ] **Step 1.5.6: Update `compute_formations_shape_graph` depends_on**

Find the `compute_formations_shape_graph` task block (around line 301). Current:

```hcl
    depends_on {
      task_key = "compute_formations_efpi"
    }
```

Replace with:

```hcl
    # PR-Cycle-C PR-β (2026-05-02, ADR-019): consumes the EFPI temp table
    # written by compute_formations_efpi (kept) AND reads gold.fct_tracking_frames
    # input_mart (new edge to stage 1). Order: alphabetical.
    depends_on {
      task_key = "compute_formations_efpi"
    }
    depends_on {
      task_key = "dbt_build_input_marts"
    }
```

- [ ] **Step 1.5.7: Update `compute_line_breaking` depends_on**

Find the `compute_line_breaking` task block (around line 328). Current 5 ingest edges (preserved by bronze-reads test). Add `dbt_build_input_marts` while preserving alphabetical order:

Current:
```hcl
    depends_on {
      task_key = "backfill_statsbomb_360"
    }
    depends_on {
      task_key = "ingest_idsse"
    }
    depends_on {
      task_key = "ingest_idsse_events"
    }
    depends_on {
      task_key = "ingest_metrica"
    }
    depends_on {
      task_key = "ingest_statsbomb"
    }
```

Replace with (insert `dbt_build_input_marts` block alphabetically after `backfill_statsbomb_360`):

```hcl
    depends_on {
      task_key = "backfill_statsbomb_360"
    }
    # PR-Cycle-C PR-β (2026-05-02, ADR-019): gold-side reads
    # (gold.fct_tracking_frames) wait on stage 1. Bronze-side ingest
    # edges retained per test_workflow_dag_bronze_reads.
    depends_on {
      task_key = "dbt_build_input_marts"
    }
    depends_on {
      task_key = "ingest_idsse"
    }
    depends_on {
      task_key = "ingest_idsse_events"
    }
    depends_on {
      task_key = "ingest_metrica"
    }
    depends_on {
      task_key = "ingest_statsbomb"
    }
```

- [ ] **Step 1.5.8: Update `compute_embeddings_v2` depends_on**

Find the `compute_embeddings_v2` task block (around line 226). Current:

```hcl
    depends_on {
      task_key = "resolve_players"
    }
```

Replace with:

```hcl
    # PR-Cycle-C PR-β (2026-05-02, ADR-019): reads gold.fct_action_values
    # (intermediate_mart) — wait on stage 2. The legacy resolve_players
    # edge was a misclassification — v2 inference reads HF-published
    # weights + fct_action_values gold, NOT bronze.player_xref_raw.
    depends_on {
      task_key = "dbt_build_intermediate_marts"
    }
```

- [ ] **Step 1.5.9: Update `run_model_validation` depends_on**

Find the `run_model_validation` task block (around line 918). Current:

```hcl
    depends_on {
      task_key = "compute_pausa"
    }
```

Replace with:

```hcl
    # PR-Cycle-C PR-β (2026-05-02, ADR-019): reads today's output_marts
    # (fct_xg_predictions_v2, fct_pausa_values, etc.). Sibling of
    # refresh_synced_tables (both children of dbt_build_output_marts) —
    # validation regression CANNOT block today's mart refresh, preserving
    # ADR-017's "signal not gate" intent via topology rather than via
    # stale reads. Drops the legacy compute_pausa edge (subsumed by
    # transitive path through dbt_build_output_marts).
    depends_on {
      task_key = "dbt_build_output_marts"
    }
```

- [ ] **Step 1.5.10: Update `refresh_synced_tables` depends_on**

Find the `refresh_synced_tables` task block (around line 870). Current:

```hcl
    depends_on {
      task_key = "dbt_build"
    }
```

Replace with:

```hcl
    # PR-Cycle-C PR-β (2026-05-02, ADR-019): waits on stage 3 (the final dbt
    # invocation). Pre-PR-β this depended on the single `dbt_build` task;
    # post-PR-β stage 3 is the equivalent terminal mart-build step.
    depends_on {
      task_key = "dbt_build_output_marts"
    }
```

- [ ] **Step 1.5.11: Replace the `dbt_build` task with three new tasks**

This is the largest single edit. Find the `dbt_build` task block (lines 555-594, the whole region from the `# ── Task: dbt build` comment through the closing `}`). Replace ALL of it with three new task blocks (in alphabetical order: `dbt_build_input_marts` < `dbt_build_intermediate_marts` < `dbt_build_output_marts`):

```hcl
  # ── Task: dbt build — stage 1 (input + dimension marts) ──────────────
  # PR-Cycle-C PR-β (2026-05-02, ADR-019): first of three sequential dbt
  # invocations. Builds dimensions + input_marts (marts whose lineage has
  # NO compute task in it — `fct_tracking_frames`, `fct_shots`,
  # `fct_discipline_events`) plus their staging-view ancestors and seeds.
  # Compute tasks reading these gold marts depend on this stage.
  task {
    task_key        = "dbt_build_input_marts"
    timeout_seconds = 3600

    python_wheel_task {
      package_name = "luxury_lakehouse"
      entry_point  = "dbt_build"
      parameters   = ["--select", "+tag:input_mart", "+tag:dimension"]
    }

    # All ingest tasks + ingest-helper compute tasks (extract_tracking_metadata,
    # backfills, resolve_players) per ADR-019 § "Treatment of ingest-helper
    # compute tasks". Order: alphabetical.
    depends_on { task_key = "backfill_statsbomb_360" }
    depends_on { task_key = "backfill_statsbomb_extra" }
    depends_on { task_key = "extract_tracking_metadata" }
    depends_on { task_key = "ingest_idsse" }
    depends_on { task_key = "ingest_idsse_events" }
    depends_on { task_key = "ingest_metrica" }
    depends_on { task_key = "ingest_skillcorner" }
    depends_on { task_key = "ingest_statsbomb" }
    depends_on { task_key = "ingest_wyscout" }
    depends_on { task_key = "resolve_players" }

    environment_key = "dbt"
  }

  # ── Task: dbt build — stage 2 (intermediate marts) ───────────────────
  # PR-Cycle-C PR-β (2026-05-02, ADR-019): second of three sequential dbt
  # invocations. Builds intermediate_marts (marts with compute output in
  # their lineage AND consumed by at least one compute task) — currently
  # `fct_action_values` only. `+tag:intermediate_mart` includes ancestors
  # so any staging views unique to intermediate marts are built here.
  task {
    task_key        = "dbt_build_intermediate_marts"
    timeout_seconds = 3600

    python_wheel_task {
      package_name = "luxury_lakehouse"
      entry_point  = "dbt_build"
      parameters   = ["--select", "+tag:intermediate_mart"]
    }

    # Sequential edge to stage 1 + the phase-1 compute task that writes
    # bronze.{spadl_actions, vaep_action_values} consumed by fct_action_values.
    # Order: alphabetical.
    depends_on { task_key = "compute_spadl_vaep" }
    depends_on { task_key = "dbt_build_input_marts" }

    environment_key = "dbt"
  }

  # ── Task: dbt build — stage 3 (output marts) ─────────────────────────
  # PR-Cycle-C PR-β (2026-05-02, ADR-019): third of three sequential dbt
  # invocations. Builds output_marts — every mart that is NOT consumed by
  # a compute task (consumed only by apps/dashboards/HF/run_model_validation).
  # `tag:output_mart` (no leading `+`) selects ONLY tagged models — staging
  # ancestors were built by stages 1 + 2.
  task {
    task_key        = "dbt_build_output_marts"
    timeout_seconds = 3600

    python_wheel_task {
      package_name = "luxury_lakehouse"
      entry_point  = "dbt_build"
      parameters   = ["--select", "tag:output_mart"]
    }

    # Stage 2 sequential edge + every phase-2 compute task that writes
    # bronze read by an output_mart. (compute_spadl_vaep is in stage 2;
    # extract_tracking_metadata + backfills + resolve_players are in
    # stage 1.) Order: alphabetical.
    depends_on { task_key = "compute_defcon_lite" }
    depends_on { task_key = "compute_elastic_sync" }
    depends_on { task_key = "compute_embeddings_360" }
    depends_on { task_key = "compute_embeddings_v1" }
    depends_on { task_key = "compute_embeddings_v2" }
    depends_on { task_key = "compute_expected_threat" }
    depends_on { task_key = "compute_formations_efpi" }
    depends_on { task_key = "compute_formations_shape_graph" }
    depends_on { task_key = "compute_line_breaking" }
    depends_on { task_key = "compute_off_ball_xt" }
    depends_on { task_key = "compute_pausa" }
    depends_on { task_key = "compute_pitch_control" }
    depends_on { task_key = "compute_xg_model" }
    depends_on { task_key = "compute_xg_model_v2" }
    depends_on { task_key = "dbt_build_intermediate_marts" }
    depends_on { task_key = "hf_sync" }

    environment_key = "dbt"
  }
```

Note: this replacement preserves the alphabetical task-block ordering invariant — `compute_xg_model_v2` (existing previous task) < `dbt_build_input_marts` < `dbt_build_intermediate_marts` < `dbt_build_output_marts` < `extract_tracking_metadata` (existing next task). Sanity-checked against the alphabetical anchor in `test_workflows_tf_ordering`.

- [ ] **Step 1.5.12: Run the full TF test suite to confirm Task 1.5 turns red→green**

Run: `uv run pytest src/tests/test_terraform_workflow_dbt_task.py src/tests/test_workflow_dag_gold_reads.py src/tests/test_workflow_dag_bronze_reads.py src/tests/test_workflows_tf_ordering.py -v`

Expected: ALL pass. The bronze-read peer must still pass — the existing curated entries reference tasks that still exist (none of the bronze writers were removed) and the `dbt_build` consumer entries become unsatisfiable since `dbt_build` no longer exists.

**Sub-step:** the bronze-reads test has 4 entries with `consumer_task = "dbt_build"`:
```python
("dbt_build", "pausa_values", "compute_pausa"),
("dbt_build", "elastic_event_match", "compute_elastic_sync"),
("dbt_build", "statsbomb_360", "backfill_statsbomb_360"),
("dbt_build", "player_embeddings_raw_360", "compute_embeddings_360"),
```

These will FAIL post-PR-β because `dbt_build` no longer exists. Update them to `dbt_build_output_marts` (the stage that actually reads these bronze tables via stg_*).

- [ ] **Step 1.5.13: Update `_BRONZE_READ_REQUIREMENTS` in test_workflow_dag_bronze_reads.py**

Edit `src/tests/test_workflow_dag_bronze_reads.py:73-77`. Replace:

```python
    # ── dbt_build: stg_* views read leaf compute outputs ────────────────
    # Without these edges, today's gold marts get built from yesterday's
    # bronze for the named source (1-day lag class).
    ("dbt_build", "pausa_values", "compute_pausa"),
    ("dbt_build", "elastic_event_match", "compute_elastic_sync"),
    ("dbt_build", "statsbomb_360", "backfill_statsbomb_360"),
    ("dbt_build", "player_embeddings_raw_360", "compute_embeddings_360"),
```

with:

```python
    # ── dbt_build_output_marts: stg_* views read leaf compute outputs ───
    # PR-Cycle-C PR-β (2026-05-02, ADR-019): single `dbt_build` task replaced
    # with three sequential stages. The bronze→staging→mart flow for these
    # 4 entries lives in stage 3 (output_marts) since they all feed
    # output-classified marts. Without these edges, today's gold marts get
    # built from yesterday's bronze for the named source (1-day lag class).
    ("dbt_build_output_marts", "pausa_values", "compute_pausa"),
    ("dbt_build_output_marts", "elastic_event_match", "compute_elastic_sync"),
    # statsbomb_360 (backfill_statsbomb_360 writer) — read by stg_statsbomb_360
    # which is an ancestor of input_mart fct_shots. Migration to
    # dbt_build_input_marts is correct.
    ("dbt_build_input_marts", "statsbomb_360", "backfill_statsbomb_360"),
    ("dbt_build_output_marts", "player_embeddings_raw_360", "compute_embeddings_360"),
```

Re-run: `uv run pytest src/tests/test_workflow_dag_bronze_reads.py -v` → all pass.

### Task 1.6: Split `wf-dbt-build.yaml` into three workflow cards

**Files:**
- Delete: `workflow-cards/wf-dbt-build.yaml`
- Create: `workflow-cards/wf-dbt-build-input-marts.yaml`
- Create: `workflow-cards/wf-dbt-build-intermediate-marts.yaml`
- Create: `workflow-cards/wf-dbt-build-output-marts.yaml`

Per `test_card_parity_with_terraform.py`, each TF task needs a card whose `entry_point` matches AND has `trigger=scheduled`. The 3 new TF tasks all share `entry_point="dbt_build"` (selector differentiates), but each card must declare it.

Each card's outputs section restricts to the marts that stage builds. The classification table from ADR-019 § "Mart taxonomy" is the authoritative source.

- [ ] **Step 1.6.1: Create `wf-dbt-build-input-marts.yaml`**

Create `workflow-cards/wf-dbt-build-input-marts.yaml` (4 dimensions + 3 input_marts = 7 outputs):

```yaml
---
name: dbt build — stage 1 (input + dimension marts)
id: wf-dbt-build-input-marts
version: "1.0.0"
status: production
type: data-movement
domain: soccer-analytics
owners:
  - karsten
tags:
  - dbt
  - gold
  - daily-job
  - three-stage

# Stage 1 of 3 in PR-Cycle-C PR-β (ADR-019). Builds dimensions + marts whose
# lineage contains no compute task output. Compute tasks reading these gold
# marts (pitch_control / off_ball_xt / xg_model[_v2] / formations_efpi /
# formations_shape_graph / line_breaking) wait on this stage.
# See src/ingestion/dbt_runner.py — selector arg differentiates from stages 2/3.
references: []

inputs:
  datasets:
    - id: "{catalog}.bronze.statsbomb_events"
      source: delta-table
      description: "Bronze tables produced by ingest_* + ingest-helper compute tasks (extract_tracking_metadata, backfills, resolve_players)"

outputs:
  tables:
    # Every row maps 1:1 to a marts/*.sql file with tag:dimension or tag:input_mart.
    # test_card_dbt_model_field enforces full coverage in both directions.
    - id: "{catalog}.dev_gold.dim_competitions"
      destination: delta-table
      dbt_model: dim_competitions
    - id: "{catalog}.dev_gold.dim_matches"
      destination: delta-table
      dbt_model: dim_matches
    - id: "{catalog}.dev_gold.dim_players"
      destination: delta-table
      dbt_model: dim_players
    - id: "{catalog}.dev_gold.dim_teams"
      destination: delta-table
      dbt_model: dim_teams
    - id: "{catalog}.dev_gold.fct_discipline_events"
      destination: delta-table
      dbt_model: fct_discipline_events
    - id: "{catalog}.dev_gold.fct_shots"
      destination: delta-table
      dbt_model: fct_shots
    - id: "{catalog}.dev_gold.fct_tracking_frames"
      destination: delta-table
      dbt_model: fct_tracking_frames

execution:
  ingestion:
    trigger: scheduled
    runtime: databricks-workflow
    entry_point: dbt_build
    module: ingestion.dbt_runner
    distribution: driver-bound
    schedule: "daily 06:00 UTC"
    timeout: "3600s"
    environment: dbt

depends_on:
  - wf-statsbomb
  - wf-wyscout
  - wf-metrica
  - wf-idsse
  - wf-skillcorner
  - wf-entity-resolution

idempotency:
  strategy: full-overwrite
  key: model
  description: "dbt CREATE OR REPLACE TABLE semantics — idempotent per model."

performance:
  inference_timeout: "3600s"
  memory_ceiling: "16 GB driver"

cost:
  ingestion:
    runtime: databricks
    sku: "jobs_serverless_compute_run_dbus"
    typical_dbu: 2
    typical_cost_usd: 0.14

monitoring:
  freshness_sla_hours: 24

links:
  source_code:
    - "src/ingestion/dbt_runner.py"
---

## Overview

Stage 1 of the three-stage dbt_build topology introduced by ADR-019 (PR-Cycle-C
PR-β). Builds the 4 dimensions + 3 input_marts (`fct_tracking_frames`,
`fct_shots`, `fct_discipline_events`) plus their staging-view ancestors and
seeds. Selector: `--select +tag:input_mart +tag:dimension`.

Compute tasks reading these gold marts (`compute_pitch_control`,
`compute_off_ball_xt`, `compute_xg_model`, `compute_xg_model_v2`,
`compute_formations_efpi`, `compute_formations_shape_graph`,
`compute_line_breaking`) declare `depends_on { task_key = "dbt_build_input_marts" }`
so they read TODAY's gold rather than yesterday's.

See `docs/superpowers/adrs/ADR-019-three-stage-dbt-build.md` for the full
classification taxonomy and § "Compute reads today's gold" principle.
```

- [ ] **Step 1.6.2: Create `wf-dbt-build-intermediate-marts.yaml`**

Create `workflow-cards/wf-dbt-build-intermediate-marts.yaml` (1 output):

```yaml
---
name: dbt build — stage 2 (intermediate marts)
id: wf-dbt-build-intermediate-marts
version: "1.0.0"
status: production
type: data-movement
domain: soccer-analytics
owners:
  - karsten
tags:
  - dbt
  - gold
  - daily-job
  - three-stage

# Stage 2 of 3 in PR-Cycle-C PR-β (ADR-019). Builds intermediate_marts —
# marts that have compute output in their lineage AND are consumed by at
# least one compute task. Currently only `fct_action_values` (built from
# bronze.{spadl_actions, vaep_action_values} written by compute_spadl_vaep,
# read by compute_embeddings_v2). Forward-compatible with future intermediate
# marts as ML pipelines compose.
references: []

inputs:
  datasets:
    - id: "{catalog}.bronze.spadl_actions"
      source: delta-table
      description: "SPADL actions bronze written by compute_spadl_vaep (4-source union)"

outputs:
  tables:
    - id: "{catalog}.dev_gold.fct_action_values"
      destination: delta-table
      dbt_model: fct_action_values

execution:
  ingestion:
    trigger: scheduled
    runtime: databricks-workflow
    entry_point: dbt_build
    module: ingestion.dbt_runner
    distribution: driver-bound
    schedule: "daily 06:00 UTC"
    timeout: "3600s"
    environment: dbt

depends_on:
  - wf-vaep
  - wf-dbt-build-input-marts

idempotency:
  strategy: full-overwrite
  key: model
  description: "dbt CREATE OR REPLACE TABLE semantics — idempotent per model."

performance:
  inference_timeout: "3600s"
  memory_ceiling: "16 GB driver"

cost:
  ingestion:
    runtime: databricks
    sku: "jobs_serverless_compute_run_dbus"
    typical_dbu: 1
    typical_cost_usd: 0.07

monitoring:
  freshness_sla_hours: 24

links:
  source_code:
    - "src/ingestion/dbt_runner.py"
---

## Overview

Stage 2 of the three-stage dbt_build topology introduced by ADR-019. Builds
the single intermediate_mart (`fct_action_values`) from
`bronze.{spadl_actions, vaep_action_values}` written by `compute_spadl_vaep`.
Selector: `--select +tag:intermediate_mart` — `+` includes the staging-view
ancestors unique to this mart.

`compute_embeddings_v2` declares `depends_on { task_key = "dbt_build_intermediate_marts" }`
so its inference reads TODAY's `fct_action_values`.

See `docs/superpowers/adrs/ADR-019-three-stage-dbt-build.md`.
```

- [ ] **Step 1.6.3: Create `wf-dbt-build-output-marts.yaml`**

Create `workflow-cards/wf-dbt-build-output-marts.yaml` (32 outputs — every mart NOT in stages 1 or 2). Use the existing `wf-dbt-build.yaml` outputs list and remove the 4 dim_* + 3 input_mart entries already in `wf-dbt-build-input-marts.yaml` and the 1 intermediate_mart entry already in `wf-dbt-build-intermediate-marts.yaml`:

```yaml
---
name: dbt build — stage 3 (output marts)
id: wf-dbt-build-output-marts
version: "1.0.0"
status: production
type: data-movement
domain: soccer-analytics
owners:
  - karsten
tags:
  - dbt
  - gold
  - daily-job
  - three-stage

# Stage 3 of 3 in PR-Cycle-C PR-β (ADR-019). Builds output_marts — marts that
# are NOT consumed by any compute task (consumed only by apps/dashboards/HF/
# run_model_validation). Selector: `--select tag:output_mart` (no leading `+`)
# because all ancestors are built by stages 1 + 2.
#
# refresh_synced_tables and run_model_validation are siblings depending on
# this stage — validation reads today's output marts but a validation
# regression cannot block synced-table propagation.
references: []

inputs:
  datasets:
    - id: "{catalog}.bronze.xg_predictions"
      source: delta-table
      description: "Bronze tables produced by phase-2 compute tasks (xg_model[_v2], pausa, defcon, embeddings, formations, off_ball_xt, line_breaking, etc.)"

outputs:
  tables:
    - id: "{catalog}.dev_gold.fct_defcon_actions"
      destination: delta-table
      dbt_model: fct_defcon_actions
    - id: "{catalog}.dev_gold.fct_defcon_pressure"
      destination: delta-table
      dbt_model: fct_defcon_pressure
    - id: "{catalog}.dev_gold.fct_defensive_values"
      destination: delta-table
      dbt_model: fct_defensive_values
    - id: "{catalog}.dev_gold.fct_formation_labels"
      destination: delta-table
      dbt_model: fct_formation_labels
    - id: "{catalog}.dev_gold.fct_funnel_stages_agg"
      destination: delta-table
      dbt_model: fct_funnel_stages_agg
    - id: "{catalog}.dev_gold.fct_gk_actions_detail"
      destination: delta-table
      dbt_model: fct_gk_actions_detail
    - id: "{catalog}.dev_gold.fct_goalkeeper_stats"
      destination: delta-table
      dbt_model: fct_goalkeeper_stats
    - id: "{catalog}.dev_gold.fct_heatmap_agg"
      destination: delta-table
      dbt_model: fct_heatmap_agg
    - id: "{catalog}.dev_gold.fct_line_breaking_results"
      destination: delta-table
      dbt_model: fct_line_breaking_results
    - id: "{catalog}.dev_gold.fct_match_summary"
      destination: delta-table
      dbt_model: fct_match_summary
    - id: "{catalog}.dev_gold.fct_off_ball_xt"
      destination: delta-table
      dbt_model: fct_off_ball_xt
    - id: "{catalog}.dev_gold.fct_pass_timing"
      destination: delta-table
      dbt_model: fct_pass_timing
    - id: "{catalog}.dev_gold.fct_passes"
      destination: delta-table
      dbt_model: fct_passes
    - id: "{catalog}.dev_gold.fct_pausa_rankings"
      destination: delta-table
      dbt_model: fct_pausa_rankings
    - id: "{catalog}.dev_gold.fct_pausa_values"
      destination: delta-table
      dbt_model: fct_pausa_values
    - id: "{catalog}.dev_gold.fct_physical_stats"
      destination: delta-table
      dbt_model: fct_physical_stats
    - id: "{catalog}.dev_gold.fct_player_embeddings"
      destination: delta-table
      dbt_model: fct_player_embeddings
    - id: "{catalog}.dev_gold.fct_player_embeddings_career"
      destination: delta-table
      dbt_model: fct_player_embeddings_career
    - id: "{catalog}.dev_gold.fct_player_embeddings_career_360"
      destination: delta-table
      dbt_model: fct_player_embeddings_career_360
    - id: "{catalog}.dev_gold.fct_player_embeddings_season"
      destination: delta-table
      dbt_model: fct_player_embeddings_season
    - id: "{catalog}.dev_gold.fct_player_embeddings_season_360"
      destination: delta-table
      dbt_model: fct_player_embeddings_season_360
    - id: "{catalog}.dev_gold.fct_player_percentiles"
      destination: delta-table
      dbt_model: fct_player_percentiles
    - id: "{catalog}.dev_gold.fct_player_positions"
      destination: delta-table
      dbt_model: fct_player_positions
    - id: "{catalog}.dev_gold.fct_player_stats"
      destination: delta-table
      dbt_model: fct_player_stats
    - id: "{catalog}.dev_gold.fct_position_maps"
      destination: delta-table
      dbt_model: fct_position_maps
    - id: "{catalog}.dev_gold.fct_space_creation"
      destination: delta-table
      dbt_model: fct_space_creation
    - id: "{catalog}.dev_gold.fct_tracking_avg_positions"
      destination: delta-table
      dbt_model: fct_tracking_avg_positions
    - id: "{catalog}.dev_gold.fct_tracking_shape_timeline"
      destination: delta-table
      dbt_model: fct_tracking_shape_timeline
    - id: "{catalog}.dev_gold.fct_vaep_breakdown_agg"
      destination: delta-table
      dbt_model: fct_vaep_breakdown_agg
    - id: "{catalog}.dev_gold.fct_workflow_costs"
      destination: delta-table
      dbt_model: fct_workflow_costs
    - id: "{catalog}.dev_gold.fct_xg_predictions"
      destination: delta-table
      dbt_model: fct_xg_predictions
    - id: "{catalog}.dev_gold.fct_xg_predictions_v2"
      destination: delta-table
      dbt_model: fct_xg_predictions_v2

execution:
  ingestion:
    trigger: scheduled
    runtime: databricks-workflow
    entry_point: dbt_build
    module: ingestion.dbt_runner
    distribution: driver-bound
    schedule: "daily 06:00 UTC"
    timeout: "3600s"
    environment: dbt

depends_on:
  - wf-dbt-build-intermediate-marts
  - wf-xg-v1
  - wf-xg-v2
  - wf-defcon
  - wf-off-ball-xt
  - wf-line-breaking
  - wf-formations
  - wf-football2vec
# Note: wf-model-validation deliberately omitted — validation reads from gold
# marts that this stage produces, so gating mart refresh on validation only
# checks yesterday's data. Validation is a quality signal, not a correctness
# gate. See docs/superpowers/adrs/ADR-017-model-validation-as-signal-not-gate.md
# (amended by ADR-019 — sibling-of-refresh_synced_tables now realises the
# "signal not gate" intent via topology, not via stale reads).

idempotency:
  strategy: full-overwrite
  key: model
  description: "dbt CREATE OR REPLACE TABLE semantics — idempotent per model."

performance:
  inference_timeout: "3600s"
  memory_ceiling: "16 GB driver"

cost:
  ingestion:
    runtime: databricks
    sku: "jobs_serverless_compute_run_dbus"
    typical_dbu: 3
    typical_cost_usd: 0.21

monitoring:
  freshness_sla_hours: 24

links:
  source_code:
    - "src/ingestion/dbt_runner.py"
---

## Overview

Stage 3 of the three-stage dbt_build topology introduced by ADR-019. Builds
the 32 output_marts. Selector: `--select tag:output_mart` (no leading `+`)
because all staging ancestors were built by stages 1 + 2.

`refresh_synced_tables` and `run_model_validation` are SIBLINGS depending on
this stage — both children of `dbt_build_output_marts`. A validation
regression cannot transitively block today's synced-table refresh, preserving
ADR-017's "signal not gate" intent via topology rather than via stale reads
(supplants ADR-017's pre-three-stage yesterday-gold workaround).

See `docs/superpowers/adrs/ADR-019-three-stage-dbt-build.md`.
```

- [ ] **Step 1.6.4: Delete the legacy `wf-dbt-build.yaml`**

Run: `git rm workflow-cards/wf-dbt-build.yaml`

(In Bash on Windows / PowerShell, use `git rm` rather than `rm` to ensure git tracks the deletion.)

- [ ] **Step 1.6.5: Run card parity tests to confirm green**

Run: `uv run pytest src/tests/test_card_parity_with_terraform.py src/tests/test_workflow_card_references.py -v`

Expected: all pass. Both `test_mapping_matches_tf_task_list` and `test_every_direct_tf_task_has_scheduled_card` should now succeed because the 3 new cards exist on disk and reference the 3 new TF tasks.

### Task 1.7: Add ADR-019 implementation status line

**Files:**
- Modify: `docs/superpowers/adrs/ADR-019-three-stage-dbt-build.md` (header table)

- [ ] **Step 1.7.1: Add Implementation status line under the Status row**

Find the header table:

```markdown
| Field | Value |
|---|---|
| **Date** | 2026-05-01 |
| **Status** | Accepted |
| **Deciders** | Karsten S. Nielsen |
```

Replace with:

```markdown
| Field | Value |
|---|---|
| **Date** | 2026-05-01 |
| **Status** | Accepted |
| **Implementation status** | PR-α merged 2026-05-01 (mart tags + classification conformance test); PR-β merged 2026-05-02 (TF restructure + gold-read conformance test + 3 workflow cards) |
| **Deciders** | Karsten S. Nielsen |
```

---

## Phase 2 — ADR-020 CAN_RUN auto-heal

### Task 2.1: Add the CAN_RUN auto-heal step to lakebase-grants.yml

**Files:**
- Modify: `.github/workflows/lakebase-grants.yml`

The workflow currently runs three idempotent self-healing steps (`fix_event_log_ownership.py`, `run_lakebase_grants.py`, `create_indexes.py --verify`). Add a fourth: `grant_synced_table_permissions.py`. Per the handoff's mental model, place AFTER `Apply Lakebase grants` (PG-side) and BEFORE `Apply Lakebase indexes`.

- [ ] **Step 2.1.1: Add the new step + extend the workflow comment header**

Find the header comment block (lines 1-23). Update the description list:

```yaml
# Self-healing Lakebase synced-table maintenance. Runs THREE idempotent
# passes in sequence — all three are the canonical hooks for any synced
# table added to `ingestion.refresh_synced_tables.SYNCED_TABLES`:
#
#   1. `fix_event_log_ownership.py --skip-trigger-refresh` — re-sets
#      ...
#   2. `run_lakebase_grants.py` — re-applies SELECT grants. ...
#   3. `create_indexes.py --verify` — re-applies PG indexes (IF NOT EXISTS)
#      ...
```

Replace with:

```yaml
# Self-healing Lakebase synced-table maintenance. Runs FOUR idempotent
# passes in sequence — all four are the canonical hooks for any synced
# table added to `ingestion.refresh_synced_tables.SYNCED_TABLES`:
#
#   1. `fix_event_log_ownership.py --skip-trigger-refresh` — re-sets
#      pipeline `event_log_*` table ownership to `dbt-owners-{env}` after
#      a UI recreation transfers it to whoever performed the recreate.
#      Without this, the ingestion SP cannot trigger pipeline refreshes.
#      Idempotent (already_correct entries are no-op).
#   2. `run_lakebase_grants.py` — re-applies PG SELECT grants. See
#      docs/superpowers/adrs/ADR-005-lakebase-synced-table-grants.md for why
#      auto-inherit via pg_default_acl is structurally unavailable and this
#      scheduled re-apply is the canonical compensating control.
#   3. `grant_synced_table_permissions.py` — re-applies workspace-API CAN_RUN
#      on each synced table's backing pipeline (and CAN_USE on the database
#      project). Closes the same drift class as step 2 but on the workspace
#      ACL surface, not the PG schema surface. After UI recreation,
#      pipeline_ids change AND ownership transfers — both reset workspace
#      ACLs to default. The daily Databricks job's `refresh_synced_tables`
#      task and the Taipy admin endpoint's pipeline-refresh path both
#      require CAN_RUN. Without this step, those tasks 403 silently after
#      every UI recreation. See docs/superpowers/adrs/ADR-020-lakebase-canrun-autoheal.md.
#   4. `create_indexes.py --verify` — re-applies PG indexes (IF NOT EXISTS)
#      and EXPLAIN-ANALYZE-verifies Index Scan plans. Synced-table
#      recreation drops custom indexes; a daily rebuild restores them
#      before the first dashboard query hits Seq Scan.
```

Then find the existing step `Apply Lakebase grants` (lines 120-124) and add the new step AFTER it:

```yaml
      - name: Apply Lakebase grants
        if: ${{ github.event_name != 'workflow_dispatch' || inputs.verify_only != true }}
        run: |
          uv run python scripts/run_lakebase_grants.py \
              --sp-uuid "$HF_APP_SP_APPLICATION_ID"
```

Insert immediately after that block:

```yaml
      # Step 3: Re-apply workspace-API CAN_RUN on each synced table's backing
      # pipeline (and CAN_USE on the database project). Step 2 (above) handles
      # the PG-side SELECT grants; this step handles the workspace-side ACL
      # surface that the Lakebase Refresh API checks. Both surfaces drift after
      # a UI recreation. Idempotent — granting an existing permission is a
      # no-op; the script's --status mode is the way to verify state.
      # ADR-020 (PR-Cycle-C PR-β, 2026-05-02) — empirical motivation: 2026-05-01
      # 403s on UI-recreated career + season embedding synced tables.
      - name: Apply synced-table workspace permissions
        if: ${{ github.event_name != 'workflow_dispatch' || inputs.verify_only != true }}
        run: |
          uv run python scripts/grant_synced_table_permissions.py
```

The existing `Verify grant coverage` step + the `Apply Lakebase indexes` step that follow stay unchanged (the new step slots between them).

### Task 2.2: Create `ADR-020-lakebase-canrun-autoheal.md`

**Files:**
- Create: `docs/superpowers/adrs/ADR-020-lakebase-canrun-autoheal.md`

- [ ] **Step 2.2.1: Create the ADR**

Create `docs/superpowers/adrs/ADR-020-lakebase-canrun-autoheal.md`:

```markdown
# ADR-020: Lakebase CAN_RUN Workspace-ACL Auto-Heal in `lakebase-grants.yml`

| Field | Value |
|---|---|
| **Date** | 2026-05-02 |
| **Status** | Accepted |
| **Deciders** | Karsten S. Nielsen |

## Context

`lakebase-grants.yml` (the daily 07:00 UTC self-healing workflow) ran three idempotent passes through 2026-05-01: `fix_event_log_ownership.py` → `run_lakebase_grants.py` (PG SELECT grants) → `create_indexes.py --verify`. None of those re-applied the **workspace-API ACL** surface — specifically the `CAN_RUN` permission on each synced table's backing pipeline that the Lakebase Refresh API checks.

PR-Cycle-C PR-α (2026-05-01) ran the PR-γ pilot's first synced-table UI recreation (career + season embedding tables, plus the 3 SNAPSHOT→TRIGGERED conversions). Within 24 hours the daily-job's `refresh_synced_tables` task and the Taipy admin endpoint's pipeline-refresh path both 403-ed silently on the recreated tables. Diagnosis: UI recreation creates new `pipeline_id`s AND transfers ownership to whoever performed the recreation. Both side effects reset the workspace-API ACL surface to default. The 3 self-healing steps in `lakebase-grants.yml` cover the PG schema surface and the dbt-owners surface — but not the workspace ACL surface.

`scripts/grant_synced_table_permissions.py` already exists, is idempotent, and handles all three SP CAN_RUN grants in ~17s. It runs on demand from operator workstations (and as Step 0 of `scripts/maintain_synced_tables.py`), but had no scheduled cadence.

The forcing function is empirical: today's 403 errors on 2 embedding synced tables that were UI-recreated yesterday. Manual re-running of the script restored access; without a scheduled re-run, every future UI recreation reopens the 24-hour failure window.

## Decision

Add `scripts/grant_synced_table_permissions.py` as a fourth idempotent step in `.github/workflows/lakebase-grants.yml`, slotted between `Apply Lakebase grants` (PG SELECT, step 2) and `Apply Lakebase indexes` (step 4). The new step inherits the workflow's existing triggers (daily 07:00 UTC cron + post-Terraform-Apply chained run + workflow_dispatch). No new secrets, no new permissions — the workflow already runs as an admin SP via `DATABRICKS_TOKEN`, which has `CAN_MANAGE` on the database project + `IS_OWNER` on each pipeline. The script's `--status` mode is documented as the verify pathway; this ADR does not add an explicit verify step (the script's idempotent grant-or-noop output is sufficient).

## Alternatives considered

| Option | Pros | Cons | Why rejected |
|---|---|---|---|
| **A.** Manual operator re-run after every UI recreation | Zero infrastructure change | Relies on 100% operator discipline; the 2026-05-01 incident proves this fails in practice; 24-hour failure window per gap | Rejected — same forcing-function structure as the original ADR-005 (PG grants needed daily re-apply because operator discipline failed) |
| **B.** Bake CAN_RUN into the synced-table TF declaration so a `terraform apply` re-applies | Single source of truth | TF synced-table resources have `lifecycle.ignore_changes = all` (because the Lakebase backing pipeline is auto-managed); CAN_RUN would not actually be applied through TF; would also fail on UI recreations that bypass TF | Rejected — TF doesn't own the live backing pipeline_id, so it can't apply ACLs against the right target |
| **C.** Daily auto-heal step in `lakebase-grants.yml` **(chosen)** | Same self-healing pattern as the 3 existing steps; idempotent; ~17s wall-clock; covers both UI recreations AND TF-driven pipeline_id rotation | Adds a 4th step (minor) | — |
| **D.** Move CAN_RUN application into the `refresh_synced_tables` Databricks task itself | One-time cost per refresh | The Databricks task runs as the ingestion SP, which doesn't have `CAN_MANAGE` on the database project (would require granting it). Granting `CAN_MANAGE` to the runtime SP is a privilege escalation we explicitly avoid (SEC4 cycle). The maintenance workflow runs as a more privileged SP. | Rejected — privilege boundary violation |

## Consequences

### Positive

- CAN_RUN drift after UI recreation auto-heals within 24 hours (worst case — depends on how soon the recreation happens after the 07:00 UTC cron). The `workflow_run` chained trigger from Terraform Apply also catches TF-driven recreations within minutes.
- Same self-healing pattern as ADR-005 (PG grants), `fix_event_log_ownership.py` (event-log ownership), and `create_indexes.py --verify` (PG indexes). One mental model covers all four ACL/permission surfaces.
- Operator discipline is no longer the SLA-keeper — the cron is. The script remains available for on-demand operator use.
- Adding a new synced table to `ingestion.refresh_synced_tables.SYNCED_TABLES` is automatically covered (the script reads from that single registry).

### Negative

- 17s added wall-clock to the daily 07:00 UTC workflow (negligible — total workflow is now ~3-4 min).
- A new failure mode: if `grant_synced_table_permissions.py` itself breaks (e.g. SDK API change), the workflow fails AND the next operator who tries to refresh a UI-recreated table will be hit by the 403. Mitigation: the script is exercised on every workflow run, so breakage is caught within 24 hours rather than at the next operator demand.

### Neutral

- The script already runs as Step 0 of `scripts/maintain_synced_tables.py` — that local-dev path is unchanged.
- No CLAUDE.md amendment needed — the lakebase-grants.yml workflow's design philosophy ("coverage before blame; daily re-apply because pg_default_acl is structurally unavailable") is unchanged.

## Related

- **Predecessors**:
  - ADR-005 — Lakebase Synced Table Grants (the original PG-side compensating control); this ADR extends the same pattern to the workspace-ACL surface.
  - PR-Cycle-C PR-α (PR #243, commit `fb52bdc`, 2026-05-01) — UI recreation of pilot synced tables exposed the gap.
- **Empirical motivation**: 2026-05-01 403 errors on `fct_player_embeddings_career_synced` + `fct_player_embeddings_season_synced` after UI recreation; 24-hour resolution gap pre-fix.
- **Implementation**: `.github/workflows/lakebase-grants.yml` step 3; `scripts/grant_synced_table_permissions.py` (unchanged).
- **Memory**:
  - `feedback_synced_table_deletion.md` — UI-recreation pattern that triggers the drift.
  - `project_pr_cycle_c_alpha_complete.md` § "CAN_RUN grant gap confirmed" — diagnosis log.

## Notes

The script ships **without** a `--status`-only step in this workflow because the operator-friendly idempotent-grant-or-noop output covers the verification path naturally. If a separate verify-only step becomes useful later (e.g. for incident response dashboards), it can be added under the `verify_only` workflow_dispatch input — same pattern as the existing index verify step.

Same structural mitigation applies to future ACL surfaces (Unity Catalog, Lakebase database-project SELECT escalation, etc.): if any new ACL drifts after UI / TF lifecycle events, the canonical answer is "add an idempotent re-apply step to lakebase-grants.yml" rather than relying on operator discipline.
```

### Task 2.3: Update MEMORY.md index entry for ADR-020

**Files:**
- Modify: `C:\Users\Karsten\.claude\projects\D--Development-karstenskyt--luxury-lakehouse\memory\MEMORY.md`

The `## Engineering Standards (PERMANENT)` section is the right home for ADR pointers in this codebase's MEMORY index. Per CLAUDE.md guidance, MEMORY.md entries are one-line under ~200 chars.

- [ ] **Step 2.3.1: Add the ADR-020 line**

(Detailed line content + insertion location is finalized at execution time when MEMORY.md state is read fresh — keep entries to one line per the index conventions in CLAUDE.md.)

---

## Final verification + commit

### Task FV.1: Run the full test suite

- [ ] **Step FV.1.1: Run pytest src/tests/ (background — multi-minute)**

The full suite includes ~700+ tests; expect 5-15 min on Win11. Run in background, poll for completion.

Run: `uv run pytest src/tests/ -v --tb=short` (with `run_in_background=true`).

Expected: ALL pass. Specifically watch:
- `test_workflows_tf_ordering.py::test_data_ingestion_parser_count_anchor` — green (33 tasks)
- `test_workflows_tf_ordering.py::test_all_databricks_jobs_task_blocks_alphabetical` — green (preserved alphabetical order across the 3 new dbt tasks)
- `test_terraform_workflow_dbt_task.py::*` — green (all 7 new test functions)
- `test_workflow_dag_gold_reads.py::*` — green (all 3 functions)
- `test_workflow_dag_bronze_reads.py::*` — green (updated 4 dbt_build entries)
- `test_card_parity_with_terraform.py::*` — green (3 new card mappings)
- `test_dbt_mart_classification.py::*` — green (no changes; PR-α test, sanity check)

If any test fails: investigate root cause per CLAUDE.md "Failure Investigation Protocol". Three-strikes rule applies — diagnose before retrying.

- [ ] **Step FV.1.2: Run terraform fmt against modified TF file**

Run: `(cd terraform && terraform fmt --check --recursive)` (in Bash) — flag any reformat needed.

If `terraform fmt` reports the file needs reformatting, run `(cd terraform && terraform fmt --recursive)` to apply. Re-stage the file. The pre-commit hook will run `terraform fmt` again — if it modifies anything mid-commit, the commit fails silently per the handoff's gotcha note.

- [ ] **Step FV.1.3: Run ruff + pyright on touched Python files**

Run:
- `uv run ruff check src/tests/test_workflow_dag_gold_reads.py src/tests/test_terraform_workflow_dbt_task.py src/tests/test_workflow_dag_bronze_reads.py src/tests/test_workflows_tf_ordering.py src/tests/test_card_parity_with_terraform.py`
- `uv run ruff format --check src/tests/test_workflow_dag_gold_reads.py src/tests/test_terraform_workflow_dbt_task.py src/tests/test_workflow_dag_bronze_reads.py src/tests/test_workflows_tf_ordering.py src/tests/test_card_parity_with_terraform.py`
- `uv run pyright src/tests/test_workflow_dag_gold_reads.py src/tests/test_terraform_workflow_dbt_task.py`

Expected: zero violations. If `ruff format` flags the new test file, run without `--check` to apply.

### Task FV.2: Single sentinel-gated commit

- [ ] **Step FV.2.1: Stage all changes**

Run:
```
git add dbt_project/models/marts/fct_player_embeddings_career.sql \
        dbt_project/models/marts/fct_player_embeddings_season.sql \
        terraform/modules/workflows/main.tf \
        src/tests/test_workflow_dag_gold_reads.py \
        src/tests/test_terraform_workflow_dbt_task.py \
        src/tests/test_workflows_tf_ordering.py \
        src/tests/test_card_parity_with_terraform.py \
        src/tests/test_workflow_dag_bronze_reads.py \
        workflow-cards/wf-dbt-build-input-marts.yaml \
        workflow-cards/wf-dbt-build-intermediate-marts.yaml \
        workflow-cards/wf-dbt-build-output-marts.yaml \
        docs/superpowers/adrs/ADR-019-three-stage-dbt-build.md \
        docs/superpowers/adrs/ADR-020-lakebase-canrun-autoheal.md \
        .github/workflows/lakebase-grants.yml
```

Plus `git rm workflow-cards/wf-dbt-build.yaml`.

- [ ] **Step FV.2.2: Show diff to user; request sentinel touch**

Run: `git diff --cached --stat` — display the file change summary.

Then prompt the user:

> Phase 0 + Phase 1 + Phase 2 are staged. Single squash commit message:
>
> `feat(tf+dbt+ci): three-stage dbt_build TF restructure + CAN_RUN auto-heal (PR-β of PR-Cycle-C)`
>
> Please run `!touch ~/.claude-git-approval` to unblock the sentinel-gated `git commit`.

- [ ] **Step FV.2.3: Commit (after sentinel materialized)**

Use a HEREDOC to pass the multi-line commit message:

```bash
git commit -m "$(cat <<'EOF'
feat(tf+dbt+ci): three-stage dbt_build TF restructure + CAN_RUN auto-heal (PR-β of PR-Cycle-C)

Implements ADR-019 (three-stage dbt_build) by replacing the single
`dbt_build` Databricks task with three sequential dbt invocations
driven by the mart classification tags PR-α applied. Compute tasks
reading gold marts now wait on the appropriate stage so they read
TODAY's gold rather than yesterday's, eliminating the 1-day-lag
class identified in PR #242's audit.

Phase 0 — career mart filter fix
  * Replace the no-op `data_source != 'football2vec_v1'` filter
    (data_source is provider label, not model version) with the
    correct dim-based `size(behavioral_vector) != 32` filter in
    fct_player_embeddings_career + _season player_best_dim CTEs.
    Unblocks `idx_embeddings_career_behavioral_hnsw` HNSW build at
    vector(192).

Phase 1 — TF DAG restructure (ADR-019 fulfillment)
  * Replace `dbt_build` task with `dbt_build_input_marts` + `_intermediate_marts` + `_output_marts`
    (each invokes the same `dbt_build` wheel entry point with a
    distinct `--select` parameter).
  * Reorder 9 compute task `depends_on`: pitch_control, off_ball_xt,
    xg_model[_v2], formations_efpi, formations_shape_graph, line_breaking
    now depend on `dbt_build_input_marts`; embeddings_v2 depends on
    `dbt_build_intermediate_marts`; run_model_validation + refresh_synced_tables
    depend on `dbt_build_output_marts` (siblings).
  * Remove 13 stale gold-reader edges per PR #242 audit.
  * New `src/tests/test_workflow_dag_gold_reads.py` peer to bronze-read
    conformance test from PR #242. 7 new test functions in
    `test_terraform_workflow_dbt_task.py`. Task count anchor 31 → 33.
  * Split `wf-dbt-build.yaml` into 3 cards matching the 3 new TF tasks.
  * ADR-019 receives an "Implementation status" line.

Phase 2 — ADR-020 CAN_RUN auto-heal
  * Add `grant_synced_table_permissions.py` as a 4th step in
    `lakebase-grants.yml`. Closes the 24-hour CAN_RUN drift window
    after UI synced-table recreation that caused 2026-05-01 403s on
    the 2 embedding synced tables.
  * New ADR-020 codifies the self-healing pattern. MEMORY.md indexed.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
```

- [ ] **Step FV.2.4: Verify commit succeeded**

Run: `git log --oneline -3 && git status`

Expected: top of `git log` shows the new commit on `feat/three-stage-dbt-tf-restructure`; `git status` reports clean tree. **Critical gotcha** (per handoff): if the output shows "Passed" on hooks but no `[main xxxx]` commit line, `terraform fmt` modified the file mid-commit. Recovery: re-stage the file, request a fresh sentinel touch, retry the commit. Do NOT use `git commit --amend` — per `feedback_no_commits_without_explicit_approval.md`, amends require explicit approval.

### Task FV.3: Push branch + open PR

- [ ] **Step FV.3.1: Push branch**

Run: `git push -u origin feat/three-stage-dbt-tf-restructure`

(Routine push — not sentinel-gated per `reference_git_commit_sentinel.md`.)

- [ ] **Step FV.3.2: Open PR**

```bash
gh pr create --title "feat(tf+dbt+ci): three-stage dbt_build + CAN_RUN auto-heal (PR-β of PR-Cycle-C)" --body "$(cat <<'EOF'
## Summary

PR-β of PR-Cycle-C — bundles three phases into a single squash commit:

- **Phase 0** — Career mart filter fix (2 SQL files). Replaces PR-α's no-op `data_source != 'football2vec_v1'` filter (data_source is provider label, not model version) with the correct dim-based `size(behavioral_vector) != 32` filter.
- **Phase 1** — ADR-019 fulfillment. Replaces single `dbt_build` Databricks task with three sequential dbt invocations (`dbt_build_input_marts` + `_intermediate_marts` + `_output_marts`), reorders 9 compute task `depends_on`, removes 13 stale gold-reader edges, adds gold-read conformance test peer, splits workflow card 1→3.
- **Phase 2** — ADR-020 (new). Adds `grant_synced_table_permissions.py` as a 4th step in `lakebase-grants.yml` so workspace-API CAN_RUN drift after UI recreation auto-heals (closes the 24-hour gap that caused 2026-05-01 403s on 2 embedding synced tables).

## Test plan

- [ ] `pytest src/tests/test_workflow_dag_gold_reads.py -v` — new test green
- [ ] `pytest src/tests/test_workflow_dag_bronze_reads.py -v` — updated `dbt_build` → 3-stage entries green
- [ ] `pytest src/tests/test_terraform_workflow_dbt_task.py -v` — 7 new test functions green
- [ ] `pytest src/tests/test_workflows_tf_ordering.py -v` — task count anchor 33 green
- [ ] `pytest src/tests/test_card_parity_with_terraform.py -v` — 3 new card mappings green
- [ ] `pytest src/tests/test_dbt_mart_classification.py -v` — PR-α test still green
- [ ] Full `pytest src/tests/` — no regressions
- [ ] `terraform fmt --check --recursive terraform/` — clean
- [ ] `ruff check + ruff format --check + pyright` on touched files — clean
- [ ] CI workflows green
- [ ] Post-merge: manual daily-job trigger to verify 33 tasks SUCCESS in topology order
- [ ] Post-merge: drop + recreate `idx_embeddings_career_behavioral_hnsw` via `scripts/create_indexes.py` — succeeds at vector(192) (verifies Phase 0)
- [ ] Post-merge next 07:00 UTC `lakebase-grants.yml` cron — 4 self-healing steps SUCCESS (verifies Phase 2)

🤖 Generated with [Claude Code](https://claude.com/claude-code)
EOF
)"
```

- [ ] **Step FV.3.3: Report PR URL**

Capture the URL `gh pr create` printed and report it back to the user. Wait for CI green before declaring the session complete.

---

## Risks & mitigations

| Risk | Mitigation |
|---|---|
| Stage 3 `tag:output_mart` fails because some output_marts have staging-view ancestors not built by stages 1+2 | If CI catches: switch stage 3 to `+tag:output_mart` (5min added wall-clock, still within budget). The spec author chose `tag:output_mart` deliberately; trust it through PR-β; hotfix if needed. |
| `terraform fmt` reformats the new task blocks mid-commit, causing silent commit failure | Run `terraform fmt` BEFORE staging in Step FV.1.2. If silent failure happens at FV.2.3, recovery is documented inline. |
| `dbt_build_output_marts` deps list grows fragile as new compute tasks are added | New gold-read conformance test fails at PR-CI if a new compute task adds a gold read without a matching stage edge. Forces explicit acknowledgement. |
| PR-α career mart filter fix changes downstream embeddings counts | Verified pre-merge intent: `fct_player_embeddings_career` row count goes from 9,765 (with 32d v1 rows) to 9,618 (192d only) — known number. |
| Pre-commit hook silently rejects commit (the gotcha caught today) | Step FV.2.4 explicitly checks for `[main xxxx]` line in commit output. Recovery procedure documented inline. |
| Workflow card output coverage breaks `test_card_dbt_model_field` (every dbt_model entry must map to a marts/*.sql file) | Cross-checked the 7 + 1 + 32 = 40 entries against the existing wf-dbt-build.yaml's 40 entries — exact match. |
| Local main drift since handoff | Plan starts with `git fetch origin && git status` (already done); branch off `origin/main` (already done). |

---

## Out of scope (explicitly NOT in PR-β)

- TRIGGERED+CDF migration of additional synced tables (PR-γ; observe pilot results passively this session, no action).
- ADR-021 per-mart triage decision matrix (PR-γ — written after PR-β merges + pilot validates).
- Wheel version bump (cycle convention — PR-α set the convention).
- Modifications to `scripts/grant_synced_table_permissions.py` (already idempotent + correct).
- Any changes to the 3 PR-γ pilot synced tables — they collect daily-job validation data this session.
