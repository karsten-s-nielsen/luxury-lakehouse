# Option B — Three-Stage `dbt_build` for Same-Day Gold-Reader Compute

| Field | Value |
|---|---|
| **Date** | 2026-05-01 |
| **Status** | Draft (design-locked, awaiting plan + implementation) |
| **Cycle** | PR-Cycle-C (follow-up to PR #242) |
| **Forcing function** | PR #242 surfaced 1-day-lag class on gold-reader compute tasks; user explicitly approved Option B as the next-cycle architectural fix |

## 1. Context

PR #242 (PR-Cycle-B, 2026-05-01) closed 4 overnight CI failures + 6 session-69 hardening gaps + 11-file SDK-import architectural fix. While doing the DAG audit for #242, we identified a previously-undocumented architectural drift: most gold-reader compute tasks (`compute_pitch_control`, `compute_off_ball_xt`, `compute_xg_model[_v2]`, `compute_formations_efpi/shape_graph`, `compute_embeddings_v2`, `run_model_validation`) read **yesterday's** gold marts because `dbt_build` runs at the END of the daily-job DAG (after compute). New matches/data therefore appear in those compute outputs **one day late**.

The user-approved fix is **Option B**: split `dbt_build` into multiple stages so input-side gold marts (the ones compute reads) are built **before** compute, and output-side marts (built from compute outputs) run after. The split eliminates the 1-day-lag class with one structural change instead of refactoring N compute tasks to read bronze directly.

ADR-017 (Model validation as signal not gate, 2026-04-29) introduced a workaround for the single-stage `dbt_build` architecture: `run_model_validation` reads **yesterday's** gold so a validation regression couldn't gate today's mart refresh. ADR-019 (this cycle) supersedes that workaround in spirit — three-stage architecture makes `run_model_validation` a **sibling** of `refresh_synced_tables` (both downstream of `dbt_build_output_marts`), so validation can read **today's** gold while preserving ADR-017's "signal not gate" intent via topology rather than via stale reads. ADR-019 amends ADR-017 to mark the original carve-out as supplanted by the new structure.

## 2. Decision summary

Replace the single `dbt_build` Databricks task with **three** sequential dbt invocations:

```
ingest_*  →  dbt_build_input_marts  →  compute_*  →  dbt_build_intermediate_marts
                                                                                 ↘
                                          compute_*  →  dbt_build_output_marts  →  refresh_synced_tables
```

Where:
- `dbt_build_input_marts`: builds dimensions + marts that compute consumes from bronze ingest output (e.g. `gold.fct_tracking_frames` from bronze tracking)
- `dbt_build_intermediate_marts`: builds marts that compute reads but that themselves depend on **other** compute output (e.g. `gold.fct_action_values` from `bronze.{spadl_actions, vaep_action_values}` written by `compute_spadl_vaep`)
- `dbt_build_output_marts`: builds remaining marts (the ones built from compute outputs and consumed only by app/dashboards/HF/`run_model_validation`)

Classification is encoded as a per-mart dbt **tag** in each model's `{{ config(...) }}` block. A new conformance test enforces correctness at PR-CI time.

## 3. Mart classification taxonomy

Every mart gets exactly **one** of four tags (in addition to the existing `marts` tag):

| Tag | Definition | Example marts |
|---|---|---|
| `dimension` | Pure conformed dimensions; no compute task writes to their lineage | `dim_matches`, `dim_players`, `dim_teams`, `dim_competitions` |
| `input_mart` | Built **only** from ingest output (no compute task writes to its lineage). May or may not be consumed by a compute task. | `fct_tracking_frames`, `fct_shots`, `fct_discipline_events` |
| `intermediate_mart` | Consumed by at least one compute task **AND** has a compute task in its lineage | `fct_action_values` (read by `compute_embeddings_v2`; built from `compute_spadl_vaep` output) |
| `output_mart` | NOT consumed by any compute task (only by apps/dashboards/HF exports/`run_model_validation`) | `fct_xg_predictions[_v2]`, `fct_pausa_values`, `fct_off_ball_xt`, `fct_formation_labels`, `fct_player_embeddings*`, `fct_*_agg`, all derived/aggregated marts |

**Why not auto-detect via dbt's DAG**: explicit tags are auditable in `git diff` per-mart, and the conformance test's error messages can name the offending mart directly. The DAG-walk approach hides the classification in test output and complicates code review.

**`run_model_validation` reads `output_mart`s** (post-PR-Cycle-C topology, today's values). It is a task, not a mart, and so does not appear in the taxonomy directly — but the `output_mart`s it reads must be tagged correctly. ADR-017's pre-three-stage yesterday-gold workaround is supplanted by the new sibling-of-refresh_synced_tables positioning (see §5 and the ADR-017 amendment in §8).

## 4. The "compute reads today's gold" principle

Any Databricks task that reads a `gold.fct_*` table reads **today's** gold (built earlier in the same daily-job run). **No exceptions in the new architecture.** ADR-017's pre-three-stage carve-out for `run_model_validation` is supplanted by the new topology — validation depends on `dbt_build_output_marts` (so reads today's gold) and runs as a sibling of `refresh_synced_tables` (so a validation regression cannot block synced-table refresh). The "signal not gate" guarantee is preserved by **structure**, not by stale reads.

The mechanism enforcing this:

- `compute_*` tasks (except `run_model_validation`) declare `depends_on { task_key = "dbt_build_input_marts" }` (or `..._intermediate_marts` for the embeddings_v2 case)
- Gold marts those compute tasks read are tagged `input_mart` or `intermediate_mart`
- Conformance test asserts: every `gold.fct_*` table read in a `compute_*` entry-point's source has its mart tagged correctly AND the corresponding TF dependency is present

This is the gold-read peer to PR #242's bronze-read conformance test (`src/tests/test_workflow_dag_bronze_reads.py`).

## 5. DAG architecture (TF restructure)

The `terraform/modules/workflows/main.tf` `data_ingestion` job replaces the single `dbt_build` task with three:

```hcl
task { task_key = "dbt_build_input_marts"
       depends_on { task_key = "<each ingest_* task>" }    # 6 ingest tasks + backfill_statsbomb_*
       parameters = ["--select", "+tag:input_mart", "+tag:dimension"]
}

task { task_key = "dbt_build_intermediate_marts"
       depends_on { task_key = "<each compute_*_phase_1>" }    # compute_spadl_vaep + others writing to intermediate-mart lineage
       parameters = ["--select", "+tag:intermediate_mart"]
}

task { task_key = "dbt_build_output_marts"
       depends_on { task_key = "<each compute_*_phase_2>" }    # all remaining compute_* tasks
       parameters = ["--select", "tag:output_mart"]
}
```

Note: `+tag:X` (with leading `+`) means "build X and all its ancestors", which pulls in staging models (views) and seeds. `tag:X` (no `+`) builds only the tagged models; their ancestors must already be current. Stage 3 uses `tag:output_mart` because all ancestors have been built by stages 1 and 2 already.

Compute task `depends_on` reorganisation (full enumeration):
- Compute tasks reading **only `input_mart`**: `compute_pitch_control`, `compute_off_ball_xt`, `compute_xg_model`, `compute_xg_model_v2`, `compute_formations_efpi`, `compute_formations_shape_graph`, `compute_line_breaking` (Path A reads `bronze.statsbomb_360`; gold-side reads remain `input_mart`-only)
  - Rule: `depends_on { task_key = "dbt_build_input_marts" }`
- Compute tasks reading **`intermediate_mart`**: `compute_embeddings_v2`
  - Rule: `depends_on { task_key = "dbt_build_intermediate_marts" }`
- Compute tasks reading **only bronze** (no gold): `compute_spadl_vaep`, `compute_pausa`, `compute_elastic_sync`, `compute_defcon_lite`, `compute_expected_threat`, `compute_embeddings_v1`, `compute_embeddings_360`, `extract_tracking_metadata`, `import_obso_results` (PR #242), `hf_sync`
  - Rule: keep existing `depends_on` on ingest/upstream-compute tasks
- `run_model_validation`: `depends_on { task_key = "dbt_build_output_marts" }` — reads today's `output_mart` values. ADR-017's "signal not gate" intent preserved by **topology**: validation is a sibling of `refresh_synced_tables` (both downstream of `dbt_build_output_marts`), so a validation regression doesn't block synced-table refresh
- `refresh_synced_tables`: `depends_on { task_key = "dbt_build_output_marts" }` (sibling of `run_model_validation`)

Stale-edge cleanup (per Q5 → A): all 13 stale gold-reader deps from PR #242's audit are removed in PR-β:
- 6× `compute_pitch_control + compute_off_ball_xt → ingest_idsse/metrica/skillcorner` (subsumed by `dbt_build_input_marts`)
- `compute_embeddings_v2 → resolve_players` (no actual bronze read)
- `compute_formations_efpi → compute_pitch_control` (peer relationship, not linear)
- `compute_xg_model + compute_xg_model_v2 → compute_spadl_vaep` (no SPADL bronze read; xg reads `gold.fct_shots` from `dbt_build_input_marts`)
- `run_model_validation → compute_pausa` removed (was Cos-3 in PR #242's audit). Replaced by `run_model_validation → dbt_build_output_marts` which gives validation today's gold reads while preserving ADR-017's "signal not gate" intent via sibling-of-refresh_synced_tables positioning

## 6. Conformance tests

### 6.1 `src/tests/test_dbt_mart_classification.py` (PR-α)

Asserts:
1. Every `marts/*.sql` file declares **exactly one** of `input_mart` / `intermediate_mart` / `output_mart` / `dimension` in `{{ config(tags=[...]) }}`
2. Semantic correctness via dbt manifest:
   - `input_mart` models have **no** ancestor that is a bronze table written by a `compute_*` Databricks task (curated set, mirroring `_BRONZE_READ_REQUIREMENTS` from `test_workflow_dag_bronze_reads.py`)
   - `intermediate_mart` models have **at least one** such compute-output ancestor AND **at least one** compute task reading them
   - `output_mart` models have **no** compute task reading them
   - `dimension` is the escape hatch — no constraints
3. The classification's curated "compute reads from gold" set matches the marts tagged as `input_mart` or `intermediate_mart` (no untagged compute-read mart)

### 6.2 `src/tests/test_workflow_dag_gold_reads.py` (PR-β)

Peer to `test_workflow_dag_bronze_reads.py`. Asserts:
- Every compute task reading a `gold.fct_*` table has a transitive `depends_on` path to `dbt_build_input_marts` (or `_intermediate_marts` for embeddings_v2)
- `run_model_validation` is the documented exception (per ADR-017); the test allows it explicitly
- Curated `_GOLD_READ_REQUIREMENTS` list updated as new compute tasks are added

## 7. Migration plan — two PRs

### PR-α (lands first; behaviour-neutral)

**Files**:
- `dbt_project/models/marts/*.sql` — add tag to each mart's `{{ config(...) }}` (~37 files, one-line edit each)
- `dbt_project/models/marts/fct_player_embeddings_career.sql` + `_season.sql` — add `where data_source != 'football2vec_v1'` to the `player_best_dim` CTE (career mart fix; fixes the HNSW dim mismatch deferred from PR #242)
- `src/tests/test_dbt_mart_classification.py` — new
- `docs/superpowers/adrs/ADR-019-three-stage-dbt-build.md` — new
- `docs/superpowers/adrs/ADR-017-model-validation-as-signal-not-gate.md` — amendment marking the yesterday-gold workaround as supplanted by ADR-019's topology (status remains "Accepted"; new "Amended" line references ADR-019)
- `docs/superpowers/specs/2026-05-01-option-b-three-stage-dbt-build-design.md` — this spec (committed as part of PR-α per the user-confirmed pattern)
- `MEMORY.md` — index entry for ADR-019

**Daily-job behaviour**: unchanged. Single `dbt_build` task remains; tags are pure metadata at this stage.

**Validation**:
- Pytest suite green (incl. new classification test)
- `dbt build` runs identically (tags don't change selector behaviour without `--select`)
- HNSW career index recreates cleanly post-merge (career mart fix verified)

**Operations post-merge**: drop + recreate `idx_embeddings_career_behavioral_hnsw` via `scripts/create_indexes.py`. Lakebase Maintenance daily run goes green.

### PR-β (lands second; topology change)

**Files**:
- `terraform/modules/workflows/main.tf` — replace `dbt_build` task with three; reorder ~10 compute task `depends_on`; remove 13 stale edges
- `src/tests/test_workflow_dag_gold_reads.py` — new
- `src/tests/test_terraform_workflow_dbt_task.py` — update for the three-task topology
- `src/tests/test_workflows_tf_ordering.py` — update task count anchor (31 → 33)
- `src/tests/test_card_parity_with_terraform.py` — add `dbt_build_input_marts` / `_intermediate_marts` / `_output_marts` to `_DIRECT_TASK_ENTRY_POINT_TO_CARD`
- `workflow-cards/wf-dbt-build-input-marts.yaml` + `_intermediate_marts.yaml` + `_output_marts.yaml` — new (split the existing `wf-dbt-build.yaml`)
- ADR-019 updated with implementation status

**Daily-job behaviour**: changes. Three dbt invocations per day (~5 min added wall-clock combined; covered by serverless warehouse pricing). Compute tasks read today's gold for the first time.

**Validation**:
- Pytest suite green (incl. new gold-read conformance test)
- Daily-job manual trigger post-merge — confirm 33 tasks SUCCESS, gold marts populated correctly across all three stages
- Verify embeddings_v2 picks up today's `fct_action_values` (test new-player onboarding case)

## 8. ADR-019 outline

Sections:
1. **Context** — PR #242 surfaced 1-day-lag; user-approved Option B fix
2. **Decision** — three-stage dbt_build with `input_mart` / `intermediate_mart` / `output_mart` / `dimension` taxonomy
3. **Mart taxonomy** — precise definitions; references `test_dbt_mart_classification.py` as the enforcement contract
4. **"Compute reads today's gold" principle** — universal topology rule with no exceptions; `run_model_validation` reads today's gold as a sibling of `refresh_synced_tables` under `dbt_build_output_marts`
5. **ADR-017 supersession** — ADR-017's yesterday-gold carve-out for `run_model_validation` was a workaround for the single-stage `dbt_build` architecture. Three-stage replaces it with topology: validation is a sibling of `refresh_synced_tables` (both children of `dbt_build_output_marts`), so a validation regression cannot transitively block synced-table refresh. The "signal not gate" principle is preserved by **structure**, not by stale reads. ADR-017 receives an "Amended" header line referencing ADR-019; the original narrative remains intact for historical context.
6. **Migration sequence** — PR-α (tags + tests + docs + career fix + ADR-017 amendment) → PR-β (TF restructure + stale-edge cleanup + gold-read conformance test + validation→dbt_build_output_marts edge)
7. **Alternatives considered** — bronze-direct refactor (rejected: 7-task rewrite); accept-the-lag (rejected: defeats cycle goal); two-stage strict (rejected: forecloses on more `intermediate_mart` cases per user direction); keep ADR-017's yesterday-gold carve-out unchanged (rejected: three-stage makes it strictly worse than topology-based "signal not gate")
8. **Consequences** — same-day freshness for **all** gold-reader compute tasks including `run_model_validation`; 3 dbt invocations/day; ~5 min added wall-clock; mart classification adds discipline; ADR-017 retains historical context but its workaround is supplanted

## 9. Career mart fix (PR-α)

Add to `fct_player_embeddings_career.sql:18-29` and `fct_player_embeddings_season.sql:20-31`:

```sql
with player_best_dim as (
    select canonical_player_id, max(size(behavioral_vector)) as best_dim
    from {{ ref('fct_player_embeddings') }}
    where data_source != 'football2vec_360'
      and data_source != 'football2vec_v1'   -- PR-α (2026-05-01): exclude 32d v1 Doc2Vec.
                                             -- v1 is "Retained for comparison; superseded by v2"
                                             -- per terraform/modules/workflows/main.tf:22-24.
                                             -- Mixed-dim career rows broke HNSW build at vector(192).
    group by canonical_player_id
)
```

## 10. Out of scope / non-goals

- **Bronze-direct refactor** of compute tasks (Option A from PR #242's earlier discussion). Not pursued — Option B delivers same-day freshness with one structural change.
- **Real-time / sub-daily freshness**. Daily-job cadence preserved.
- **Removing `bronze` schema entirely**. Marts that read both bronze (writer side) and gold (consumer side) keep their bronze raw outputs.
- **Re-tagging staging or intermediate dbt models**. Only `marts/*.sql` get the new tags; staging is `view`-materialised and intermediate is `ephemeral`.
- **Consolidating `wf-dbt-build` workflow card into one**. Split into three to match TF tasks.

## 11. Risks & mitigations

| Risk | Mitigation |
|---|---|
| PR-β breaks daily-job mid-deploy | PR-α lands first as pure metadata change; PR-β changes topology in one atomic merge. Manual trigger post-merge before relying on tomorrow's scheduled run. Revert is one PR-β revert; PR-α stays useful. |
| Mart misclassification (e.g., `input_mart` tagged on a mart with compute-output ancestor) | Conformance test catches at PR-CI time; can't merge without classification consistency |
| Three dbt invocations slower than one | Wall-clock added: ~5 min combined (each invocation has the same warehouse warmup + parse cost). Daily-job has 4-hour budget; well within. |
| Embeddings_v2 fails to find `fct_action_values` (intermediate stage failed) | dbt failures already cascade to compute failures via DAG; this just makes the dependency explicit. Existing failure handling applies. |
| New mart added without tag | Conformance test fails at PR-CI |
| New compute task added without proper `depends_on` | Gold-read conformance test fails at PR-CI |
| Career mart fix changes downstream embeddings | Validated post-merge: HNSW index builds cleanly at vector(192); fct_player_embeddings_career row count drops by N (players with only v1) — documented in ADR-019 |

## 12. Test plan

### PR-α
- [ ] `pytest src/tests/test_dbt_mart_classification.py -v` — new test green
- [ ] `pytest src/tests/` full suite green (no regression)
- [ ] `dbt build` (manual) — daily-job behaviour unchanged
- [ ] Post-merge: drop + recreate HNSW career index — succeeds at vector(192)
- [ ] Post-merge: Lakebase Maintenance scheduled run — green

### PR-β
- [ ] `pytest src/tests/test_workflow_dag_gold_reads.py -v` — new test green
- [ ] `pytest src/tests/test_terraform_workflow_dbt_task.py -v` — updated for three-task topology
- [ ] `pytest src/tests/test_workflows_tf_ordering.py -v` — task count anchor 31 → 33
- [ ] `pytest src/tests/test_card_parity_with_terraform.py -v` — three new TF tasks classified
- [ ] `pytest src/tests/` full suite green
- [ ] Manual daily-job trigger post-merge — 33 tasks SUCCESS
- [ ] Verify `gold.fct_xg_predictions` (and other output_mart) timestamps — should be later than `gold.fct_shots` (input_mart) timestamps for the same daily-run

## Related

- **Branch (PR-α)**: `feat/three-stage-dbt-mart-tagging` (planned)
- **Branch (PR-β)**: `feat/three-stage-dbt-tf-restructure` (planned)
- **Predecessor**: PR #242 (PR-Cycle-B) — closed 1-day-lag identification gap; deferred career mart fix
- **ADRs**:
  - ADR-017 — Model validation as signal not gate (existing exception to "reads today's gold" rule)
  - ADR-019 — Three-stage dbt_build (this cycle)
  - ADR-002 §6 — overwrite-writer schema drift guard (precedent for declarative metadata + conformance test)
  - ADR-018 — cross-table format contract testing (same enforcement pattern)
  - ADR-013 — ML inference outputs in dbt mart (governs the bronze→gold flow that this cycle restructures)
- **Memory**:
  - `project_career_mart_v1_v2_dim_mismatch.md` — career fix rationale
  - `feedback_pull_origin_main_before_branching.md` — git hygiene for the branch creation step

## Notes

The user explicitly chose three-stage over two-stage on the rationale that more `intermediate_mart` cases will likely emerge as ML pipelines compose (e.g. an OBSO-derived feature flowing to a downstream embedding model). Locking in the three-stage pattern now avoids a future cycle that would otherwise re-introduce the migration cost.
