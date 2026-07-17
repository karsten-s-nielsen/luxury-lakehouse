# PR-2 Implementation Plan: Orphan-Mart Deletions

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development or
> superpowers:executing-plans. Steps use checkbox (`- [ ]`) syntax.

**Goal:** Delete four gold fact marts that have **zero dbt refs and zero Taipy consumers** but still pay for
Lakebase synced tables: `fct_space_creation`, `fct_off_ball_xt`, `fct_line_breaking_results`,
`fct_gk_actions_detail`.

**Architecture:** Each mart's values already exist elsewhere (on `fct_action_context`, or — for
`fct_gk_actions_detail` — it is a pure projection of `fct_action_values`). **These are MART-only deletions: the
producing TASKS stay.** `fct_passes` reads `stg_line_breaking__results` directly and `fct_physical_stats` reads
`stg_off_ball_xt__results` directly, both *bypassing* the marts — so the staging views and their compute tasks
remain live.

**Tech stack:** dbt (Databricks), Terraform, pytest, Lakebase.

**Spec:** `docs/superpowers/specs/2026-07-14-mart-consolidation-tc1-retirement-design.md` (§3 Phase 2)
**Depends on:** PR-1 merged (sequencing only — keeps diffs legible; no code dependency).

**⚠ READ BEFORE STARTING:**
- CLAUDE.md-governed. No commit without explicit user approval. One branch / one commit / one PR.
- **Line numbers are from 2026-07-14 and WILL drift — grep for the named symbol.**
- **`fct_gk_tracking_actions` is NOT in scope** (its view-ification was rejected — review B1). Do not touch it.
- **`fct_pausa_values` is NOT in scope** (values-disagreement bug — see `project_pausa_two_pipelines_disagree`).

---

## The teardown matrix (verified 2026-07-14)

| Mart | Synced policy | In `triggered_synced_marts`? | PG indexes | `rederive_planner.py` registry | `_marts__models.yml` block |
|---|---|---|---|---|---|
| `fct_space_creation` | **TRIGGERED** | yes (`dbt_project.yml:142`) | `create_indexes.py:181-184` (3) | **`_TABLE_MARTS` (:43)** | `:4080-4139` |
| `fct_off_ball_xt` | **TRIGGERED** | yes (`:137`) | none | **`D_REPROCESS_MODELS` (:33)** | `:3867-3937` |
| `fct_line_breaking_results` | **TRIGGERED** | yes (`:136`) | none | none | `:3801-3866` |
| `fct_gk_actions_detail` | SNAPSHOT | no | `create_indexes.py:195-200` (4) | none | `:4522-4642` |

All four: workflow-card output block in `workflow-cards/wf-dbt-build-output-marts.yaml`
(`fct_gk_actions_detail:82-84`, `fct_line_breaking_results:100-102`, `fct_off_ball_xt:106-108`,
`fct_space_creation:157-159`). None has an HF publisher.

**The two `rederive_planner.py` registry entries are the non-obvious trap:** deleting `fct_off_ball_xt` /
`fct_space_creation` from `SYNCED_TABLES` without removing them from those frozensets fails
`test_strand_safe_rederive.py::test_dtb_exhaustively_partition_the_triggered_set`. And
`test_rederive_planner.py:31` hard-codes `"fct_space_creation"` in a tuple.

---

## Task 0: Branch + green baseline + re-confirm zero consumers

**Files:** none (setup + verification)

- [ ] **Step 1: Sync + branch.**

```bash
cd /d/Development/karstenskyt__luxury-lakehouse
git checkout main && git pull --ff-only origin main
git checkout -b feat/orphan-mart-deletions
```

- [ ] **Step 2: Green baseline.**

```bash
uv run ruff check src/ scripts/ && uv run pyright src/ && uv run lint-imports
uv run pytest src/tests/ -q -p no:warnings   # exit 0
```

- [ ] **Step 3: RE-CONFIRM zero consumers at execution time** (the spec's "0/0" was measured on 2026-07-14; a
  new consumer may have landed since). For each mart:

```bash
for m in fct_space_creation fct_off_ball_xt fct_line_breaking_results fct_gk_actions_detail; do
  echo "=== $m ==="
  grep -rn "ref('$m')\|ref(\"$m\")" dbt_project/models dbt_project/tests
  grep -rn "$m" hf_taipy_app/src
done
```

Expected: **no output** (no `ref()`, no Taipy query). If ANY mart shows a consumer, **STOP** — that mart is no
longer a free deletion; report it and exclude it from this PR.

> Note the mart-vs-task distinction stays intact: `fct_passes` reads `stg_line_breaking__results` and
> `fct_physical_stats` reads `stg_off_ball_xt__results` — those are the *staging views*, not these marts, and
> they must still show up in the grep for the staging names. Do not delete the staging views or their tasks.

---

## Task 1: Delete `fct_gk_actions_detail` (SNAPSHOT, simplest — no registry/triggered edits)

**Files:**
- Delete: `dbt_project/models/marts/fct_gk_actions_detail.sql`
- Modify: `_marts__models.yml` (delete `- name: fct_gk_actions_detail` block)
- Modify: `src/ingestion/refresh_synced_tables.py` (delete `fct_gk_actions_detail_synced` config)
- Modify: `scripts/create_indexes.py` (delete its 4-index block)
- Modify: `workflow-cards/wf-dbt-build-output-marts.yaml` (delete its output block)

- [ ] **Step 1: Delete the mart SQL + its yml block** (grep `name: fct_gk_actions_detail`).
- [ ] **Step 2: Remove the synced config** (SNAPSHOT — NOT in `triggered_synced_marts`, so no `dbt_project.yml`
  edit).
- [ ] **Step 3: Remove the PG index block + the card output block.**
- [ ] **Step 4: Verify.**

```bash
uv run --extra sdk dbt parse
uv run pytest src/tests/test_strand_safe_rederive.py src/tests/test_card_parity_with_terraform.py -q
```

- [ ] **Step 5: Commit checkpoint.**

---

## Task 2: Delete `fct_line_breaking_results` (TRIGGERED, no registry entry)

**Files:**
- Delete: `dbt_project/models/marts/fct_line_breaking_results.sql`
- Modify: `_marts__models.yml` (delete block)
- Modify: `src/ingestion/refresh_synced_tables.py` (delete the multi-line `SyncedTableConfig`, was `:187-192`)
- Modify: `dbt_project/dbt_project.yml` (delete `- fct_line_breaking_results` from `triggered_synced_marts`)
- Modify: `workflow-cards/wf-dbt-build-output-marts.yaml` (delete output block)

- [ ] **Step 1: Delete the mart SQL + yml block.**
- [ ] **Step 2: Remove the synced config AND the `triggered_synced_marts` entry — BOTH, together.** Removing one
  without the other trips `test_strand_safe_rederive.py`. (No `rederive_planner.py` registry entry for this one
  — it falls through to the default rebuild branch.)
- [ ] **Step 3: Remove the card output block.**
- [ ] **Step 4: Verify** (`dbt parse` + `test_strand_safe_rederive` + card parity).
- [ ] **Step 5: Commit checkpoint.**

---

## Task 3: Delete `fct_off_ball_xt` (TRIGGERED + `D_REPROCESS_MODELS` registry)

**Files:**
- Delete: `dbt_project/models/marts/fct_off_ball_xt.sql`
- Modify: `_marts__models.yml` (delete block)
- Modify: `src/ingestion/refresh_synced_tables.py` (delete config, was `:199`)
- Modify: `dbt_project/dbt_project.yml` (delete `- fct_off_ball_xt` from `triggered_synced_marts`, was `:137`)
- Modify: `src/ingestion/rederive_planner.py` (delete `"fct_off_ball_xt"` from `D_REPROCESS_MODELS`, was `:33`)
- Modify: `workflow-cards/wf-dbt-build-output-marts.yaml` (delete output block)

- [ ] **Step 1: Delete the mart SQL + yml block.**
- [ ] **Step 2: Remove the synced config + `triggered_synced_marts` entry + the `D_REPROCESS_MODELS` frozenset
  entry — all three.** The registry entry is the trap: leaving it makes `test_strand_safe_rederive.py`'s
  partition assertion fail (`D_REPROCESS_MODELS ⊄ triggered` once the synced table is gone).
- [ ] **Step 3: Remove the card output block.**
- [ ] **Step 4: Verify.**

```bash
uv run --extra sdk dbt parse
uv run pytest src/tests/test_strand_safe_rederive.py src/tests/test_rederive_planner.py -q
```

- [ ] **Step 5: Commit checkpoint.**

---

## Task 4: Delete `fct_space_creation` (TRIGGERED + `_TABLE_MARTS` registry + hard-coded test)

**Files:**
- Delete: `dbt_project/models/marts/fct_space_creation.sql`
- Modify: `_marts__models.yml` (delete block)
- Modify: `src/ingestion/refresh_synced_tables.py` (delete config, was `:211`)
- Modify: `dbt_project/dbt_project.yml` (delete `- fct_space_creation` from `triggered_synced_marts`, was `:142`)
- Modify: `src/ingestion/rederive_planner.py` (delete `"fct_space_creation"` from `_TABLE_MARTS`, was `:43`)
- Modify: `src/tests/test_rederive_planner.py` (remove `"fct_space_creation"` from the tuple at `:31`)
- Modify: `scripts/create_indexes.py` (delete its 3-index block, was `:181-184`)
- Modify: `workflow-cards/wf-dbt-build-output-marts.yaml` (delete output block)

- [ ] **Step 1: Delete the mart SQL + yml block.**
- [ ] **Step 2: Remove synced config + `triggered_synced_marts` + `_TABLE_MARTS` entry + PG index block.**
- [ ] **Step 3: Fix `test_rederive_planner.py:31`** — it iterates `("fct_pausa_values", "fct_space_creation")`;
  drop `fct_space_creation` so the test stops asserting a plan for a deleted mart. (Leave `fct_pausa_values` —
  out of scope.)
- [ ] **Step 4: Remove the card output block.**
- [ ] **Step 5: Verify.**

```bash
uv run --extra sdk dbt parse
uv run pytest src/tests/test_strand_safe_rederive.py src/tests/test_rederive_planner.py src/tests/test_card_parity_with_terraform.py -q
```

- [ ] **Step 6: Commit checkpoint.**

---

## Task 5: Full verification

**Files:** none (verification only). **No wheel bump** — these are dbt/config deletions, no `src/ingestion/`
module removed. (Double-check: if `bump_wheel --check` reports drift for any reason, investigate; otherwise the
wheel is untouched.)

- [ ] **Step 1: Confirm no dangling references to any deleted mart.**

```bash
for m in fct_space_creation fct_off_ball_xt fct_line_breaking_results fct_gk_actions_detail; do
  echo "=== $m ==="; grep -rn "$m" dbt_project/ src/ scripts/ terraform/ workflow-cards/ | grep -v "\.md:"
done
```

Expected: **no output**. Any hit is a missed teardown edit.

- [ ] **Step 2: FULL verification on a SETTLED tree** (bare commands, no output redirection):

```bash
uv run ruff check src/ scripts/ ; echo "RUFF=$?"
uv run pyright src/
uv run lint-imports
uv run pytest src/tests/ -q -p no:warnings   # exit 0
uv run --extra sdk dbt parse
```

- [ ] **Step 3: STOP at the commit gate.** Commit requires separate explicit user approval.

---

## Task 6: OPERATOR-ONLY post-merge (do NOT auto-run)

- [ ] Drop the four Lakebase synced tables from Postgres (`fct_space_creation_synced`, `fct_off_ball_xt_synced`,
  `fct_line_breaking_results_synced`, `fct_gk_actions_detail_synced`) — the daily grants job will not recreate
  them once the configs are gone, but the existing synced tables must be dropped manually.
- [ ] The underlying gold tables (`dev_gold.fct_*`) become un-rebuilt once the dbt models are deleted; if
  storage cleanup is wanted, `DROP TABLE` them (destructive — operator-driven).

---

## Self-review checklist

- [ ] Zero-consumer re-confirmed live at execution time (Task 0 Step 3), not trusted from the spec.
- [ ] The 3 TRIGGERED marts removed from **both** `SYNCED_TABLES` and `triggered_synced_marts`.
- [ ] `fct_off_ball_xt` removed from `D_REPROCESS_MODELS`; `fct_space_creation` from `_TABLE_MARTS` **and**
      `test_rederive_planner.py`.
- [ ] Producing tasks + staging views for line-breaking / off-ball-xT **left intact** (mart-only deletion).
- [ ] `fct_gk_tracking_actions` and `fct_pausa_values` untouched.
- [ ] Final grep for all four mart names returns nothing outside `.md`.
