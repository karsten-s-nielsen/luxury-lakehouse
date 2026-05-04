# SK3-MIG-B Orchestrator Hardening — Source-of-Truth Reconciliation + γ Trainer Rewrite

| Field | Value |
|---|---|
| **Date** | 2026-05-04 |
| **Status** | Phase 1 (revised post-reviewer-comments + Phase 0 audit + Phase 0.5 verification) — awaiting implementation approval |
| **Cycle** | SK3-MIG-B Phase 9 hardening, third hotfix in chain |
| **Predecessors** | PR #251 (PR-α), PR #252 (HF API alignment), PR #253 (str-vs-enum stage hotfix) |
| **Triggering memory** | `project_sk3_mig_b_orchestrator_hardening_complete.md`; Phase 9 first dispatch + reviewer feedback + Phase 0 audit pass |

## §0 — Why this spec is structured the way it is

PR-α shipped the orchestrator + spec without ground-truthing against existing validated artifacts (script docs, workflow-cards, terraform, the live mega-job, dbt seeds). PR #252 + #253 closed the surface bugs that surfaced empirically. **This PR's premise is that "fix the bugs we hit" is no longer enough — we have to fix the meta-pattern.** Sections §1.1–§1.10 are the audit findings; §2 is the corresponding fixes; §3 is the explicit out-of-scope; §4 is the test plan; §5 covers risks, costs, and follow-ups.

This spec was reviewed by a parallel session before authoring; reviewer comments Q1-Q15 are integrated below at the relevant sections, not as a separate "responses" appendix. Phase 0 findings N1-N7 are integrated similarly. Sources of evidence are cited inline.

## §1 — Audit findings (the inventory the PR fixes)

### §1.1 Trainer input-dataset staleness — vaep, xg_v2

**vaep** trains against HF dataset `luxury-lakehouse/spadl-vaep-action-values`. **xg_v2** trains against HF datasets `luxury-lakehouse/xg-shots` + `luxury-lakehouse/xg-freeze-frame-data`. All three are PUBLISHED by Group 3 publishers (`spadl_vaep_publish`, `xg_shots_publish`, `freeze_frame_publish`) which run AFTER Group 1. Group 1 vaep + xg_v2 retrains therefore consume the OLD pre-SK3-MIG snapshot — silently defeating the migration's purpose. Phase 9 cycle 1 actually executed against this stale data before halting on a different bug.

### §1.2 Trainer input-dataset staleness — f2v_v2, f2v_360, scoutgpt (architectural surprise)

**f2v_v2** trains against `luxury-lakehouse/football2vec-training-data`. **f2v_360** trains against `luxury-lakehouse/football2vec-360-training-data`. **scoutgpt** trains against `luxury-lakehouse/scoutgpt-training-data`.

Phase 0.5 verified these against the live mega-job (`WorkspaceClient.jobs.get(job_id=302697362345215).settings.tasks`) — the original spec's assumption that there are mega-job tasks `export_embeddings_training_data`, `prepare_360_training_data`, `export_scoutgpt_training_data` is **FALSE**:

- The seed `dbt_project/seeds/task_workflow_mapping.csv` claims they exist
- Terraform `terraform/modules/workflows/main.tf` does NOT define them
- The live mega-job has 32 task_keys; none of those three are present
- Per `workflow-cards/wf-hf-sync.yaml`, the f2v_v2 + f2v_360 exports run as **sub-operations** inside the daily `hf_sync` task — NOT as standalone tasks
- The scoutgpt export is not even in the `hf_sync` sub-operations list — **scoutgpt-training-data has no recurring refresh mechanism and is permanently stale** (operator-confirmed: not currently used by the UI)

The original spec's "Group 2 export prereqs mirroring the scoutgpt pattern" was unreachable from the start.

### §1.3 Orchestrator `flavor_map` silently downsizes validated flavors

Phase 0 cross-checked four sources of truth:

| Trainer | Script doc | Workflow-card | Spec | Orchestrator | Verdict |
|---|---|---|---|---|---|
| vaep | cpu-basic | cpu-basic | cpu-basic | cpu-basic | ALL agree on cpu-basic, but cpu-basic OOMs at full data scale (§1.6) |
| xg_v2 | l40sx1 | l40sx1 | l40sx1 | l40sx1 | ✓ all agree |
| f2v_v1 | cpu-large | (workflow-card deprecated) | gpu-medium | gpu-medium | ❌ orchestrator + spec downsized |
| f2v_v2 | l40sx1 | l40sx1 | gpu-medium | gpu-medium | ❌ orchestrator + spec downsized |
| f2v_360 | (no `--flavor` in docstring) | l40sx1 | gpu-medium | gpu-medium | ❌ orchestrator + spec downsized; trainer doc gap |
| scoutgpt | l40sx1 | l40sx1 | gpu-large | gpu-large | ❌ orchestrator + spec downsized |

**Per the operator's directive ("orchestrator is not validated, whereas the existing flavors were, do not start downsizing these"), the orchestrator + spec are wrong. Trainer docs + workflow-cards are the validated source of truth.**

The reviewer's Q2 ("why is every divergence a downsize?") is answered: the orchestrator inherited from the design spec, which itself was authored without checking trainer docs / workflow-cards. The error compounded.

### §1.4 Orchestrator `_task_key_for_item` mappings — two wrong, one unreachable

Live mega-job verification (Phase 0.5) confirmed:

| cycle_item | Orchestrator says | Live mega-job has | Status |
|---|---|---|---|
| `defcon_lite` | `compute_defcon` | `compute_defcon_lite` | ❌ wrong (rename in seed) |
| `obso` | `compute_pausa` | `compute_pausa` | ✓ |
| `pausa` | `compute_pausa` | `compute_pausa` | ✓ |
| `vaep` | `compute_spadl_vaep` | `compute_spadl_vaep` | ✓ |
| `xg_v2` | `compute_xg_model_v2` | `compute_xg_model_v2` | ✓ |
| `scoutgpt_export` | `wf_scoutgpt_export` | (absent from live mega-job) | ❌ unreachable — no such task |

The two wrong mappings would have caused `_trigger_mega_job_task` to deadlock-poll until walltime cap (no task with that key exists). Phase 9 stopped at vaep before reaching either. The scoutgpt path was unreachable from the start regardless of the typo.

### §1.5 Anti-pattern: vaep trainer's silly-kicks pin — VERIFIED silently downgrades to 1.0.2

The reviewer's Q1 demanded empirical evidence. Local repro of the trainer's PEP 723 deps:

```python
# /// script
# dependencies = [
#     "luxury-lakehouse @ https://huggingface.co/.../luxury_lakehouse-0.3.31-py3-none-any.whl",  # wheel pins silly-kicks>=3.0.1,<4
#     "silly-kicks>=1.0.0,<2.0",  # explicit conflict
# ]
# ///
import silly_kicks; print(silly_kicks.__version__)
# Output: 1.0.2
```

**uv silently picks 1.0.2.** It does NOT fail with ResolutionImpossible. The top-level pin "wins" over the wheel's transitive pin. This is anti-intuitive and worth a CLAUDE.md note. Implications:

- Phase 9 cycle 1 (canceled) was running silly-kicks 1.x — uncorrected SK3 coordinates — for the 4323 games it processed before OOM. Any mart artifacts had a Champion been promoted would have been **poisoned**. The cancel was the only thing that saved the cycle.
- The lesson: explicit pins in PEP 723 deps that conflict with wheel-transitive pins are silent footguns, not loud errors.

**Why the pin is an anti-pattern:**
1. **Self-conflicting deps + uv's silent resolution** — the trainer's `silly-kicks>=1.0.0,<2.0` plus the wheel's `silly-kicks>=3.0.1,<4` should be ResolutionImpossible. uv instead picks 1.0.2. The pin actively harms.
2. **Pin ownership** — silly-kicks is project-owned. SemVer-style upper-bound caution exists for external libraries. With a project-owned dep, breaking changes are intentional. Pinning `<2.0` doesn't shield against anything.
3. **Stale assumption** — the `<2.0` was probably written when silly-kicks 1.x was current. Library moved 1.x → 2.x → 3.0.1; pin became fossil.
4. **Inconsistency** — the other 5 trainers don't pin silly-kicks explicitly. vaep is the lone outlier.

The fix is to remove the line entirely AND add runtime version assertions to ALL trainers (defense-in-depth — see §2.5).

### §1.6 vaep cpu-basic OOM at full data scale

vaep training at 8.8M actions × 5404 matches × 12 silly-kicks features × pandas overhead OOM-killed at 4323/5404 games on cpu-basic (16 GB system RAM). Per the operator's Q1.c directive, **cpu-large is the new validated flavor for vaep**, with the trainer doc updated alongside the orchestrator.

The reviewer's Q3 ("cpu-large isn't validated either") is a fair concern. We don't have empirical evidence cpu-large fits the current data scale. Mitigation: §2.6 instruments the vaep trainer with per-match `psutil.Process().memory_info().rss` logging so the next OOM (if any) reports its high-water mark explicitly — and cycle authors can decide whether to escalate to cpu-upgrade or chunked feature extraction.

### §1.7 ext_v2 dispatch is broken — orchestrator references non-existent modules

**Phase 9 cycle 1 did NOT reach this code path.** vaep was the first dispatched item and aborted with OOMKill before the orchestrator advanced to ext_v2_p0. The bug was found by Phase 0 static audit, not runtime.



Orchestrator `_dispatch_trained_model` for ext_v2_p0 / ext_v2_p1:
```python
phase_module = "phase_0" if "p0" in cycle_item else "phase_1"
cmd = ["uv", "run", "python", "-c",
       f"from analytics.ext_v2.{phase_module} import run_phase; run_phase()"]
```

But `analytics.ext_v2` actually contains: `__init__.py, fitness.py, harness.py, holdout.py, kde.py, producer.py, transition.py, value_iteration.py`. **No `phase_0.py` or `phase_1.py`. No `run_phase` function.**

Existing entry-points (in `harness.py`):
- `run_phase0_harness(actions: pd.DataFrame, ...)`
- `run_phase1_harness(actions: pd.DataFrame, ...)`

The smoke gates `src/tests/sk3_mig_b/test_ext_v2_p{0,1}_post_retrain_smoke.py` ALSO reference the non-existent `phase_0.compute_phase_0_nll` / `phase_1.compute_phase_1_nll` and `pytest.skip` on ImportError. So today the smoke gates pass-by-skipping; the dispatch would crash.

Both need fixing: orchestrator points at `harness.run_phase{0,1}_harness`; smoke gates either point at the same or the test file is rewritten against the actual harness API.

### §1.8 f2v_v1 env-var name mismatch

Trainer (`scripts/train_football2vec.py:warehouse_id`) reads:
```python
warehouse_id = os.environ["DATABRICKS_SQL_WAREHOUSE_ID"]
```

Orchestrator (`_dispatch_trained_model.secrets`) passes:
```python
"DATABRICKS_WAREHOUSE_ID": os.environ["DATABRICKS_WAREHOUSE_ID"],
```

Different name. The trainer would KeyError at startup. Fix: orchestrator passes both names (or the trainer accepts either). Cleanest: orchestrator passes `DATABRICKS_SQL_WAREHOUSE_ID` for f2v_v1 specifically, since the f2v_v1 trainer is the only one needing SQL access today.

After §2.4 γ (rewrite f2v_v2/f2v_360/scoutgpt to read from gold marts), all four of those trainers will need `DATABRICKS_SQL_WAREHOUSE_ID` — making this env var standard for the SQL-reading trainer set. Orchestrator passes it always; non-SQL trainers ignore it.

### §1.9 Stale workflow-card and seed entries

- `workflow-cards/wf-football2vec.yaml`: `status: deprecated` + still points at `notebooks/train_football2vec.py` (HF4-deleted). Update to point at `scripts/train_football2vec.py` OR retire the workflow-card explicitly.
- `dbt_project/seeds/task_workflow_mapping.csv`: 11 entries that don't exist in the live mega-job (43 seed rows vs. 32 live tasks). Per operator's directive, fold into this PR.
  - `export_scoutgpt_training_data,wf-scoutgpt-export` — orphan
  - `export_embeddings_training_data,wf-football2vec-v2-export` — orphan (sub-operation of `wf-hf-sync`)
  - `prepare_360_training_data,wf-prepare-360-data` — orphan (sub-operation of `wf-hf-sync`)
  - `compute_xg_model,wf-xg-v1` — orphan post-XG1-RETIRE
  - …plus 7 others to identify during implementation

The seed reconciliation is mechanical: drop entries whose task_key isn't in the live mega-job, OR add a column denoting "sub_operation_of" for entries that legitimately exist as sub-operations under another task.

### §1.10 Spec contamination

`docs/superpowers/specs/2026-05-03-sk3-mig-b-retrain-and-republish-design.md` contains the same flavor downsizes in 4+ locations (§1.1.1 cycle items table, lines 125-137 ASCII art, dispatch text on lines 320 + 324). The orchestrator inherited from this spec.

**Disposition (per reviewer Q24): preserve-with-corrections.** The 2026-05-03 spec is not archived. It documents the SK3-MIG-B cycle plan that PR-α implemented; post-correction, it describes the cycle plan as it's executed (correct flavors, correct task_keys, γ amendment cross-referenced). Per CLAUDE.md "documentation describes what IS, not what WAS" — after §2.7 corrections land, the upstream spec describes current reality. §2.7 takes the in-place edit path, not an archive path.

## §2 — Scope (PR-1 + PR-2 split, ordered TDD steps within each)

### §2.0 PR-1 / PR-2 split (operator decision 2026-05-04)

Per reviewer Q16 and Q26, scope is split into two sequential PRs. Q17's verified γ scope expansion (~750 LOC of pandas reimplementation of Spark transforms across 3 trainers, plus per-trainer parity validation) makes single-squash review-impractical and bisect-hostile. PR-1 ships the orchestrator-only fixes that unblock Phase 9 retry; PR-2 ships γ + wf-hf-sync amendment + evolve pin-drift.

| Subsection | PR-1 (orchestrator) | PR-2 (γ + downstream) |
|---|---|---|
| §2.1 γ trainer rewrites | — | ✓ |
| §2.2 module-level constants | ✓ | — |
| §2.3 `VALIDATED_HF_FLAVOR` | ✓ | — |
| §2.4 Group 0 publishes (vaep + xg_v2) | ✓ | — |
| §2.4b Group 0.5 hf_sync_prereq trigger (Q31 — IN SCOPE) | ✓ | — |
| §2.4a wf-hf-sync amendment (now covers scoutgpt + 3 Group 0 datasets — Q37 IN SCOPE) | — | ✓ |
| §2.5 runtime silly-kicks assertion | ✓ | — |
| §2.6 vaep psutil instrumentation | ✓ | — |
| §2.7 spec corrections (preserve-with-corrections) | ✓ | — |
| §2.8 seed CSV reconciliation | ✓ | — |
| §2.9 orchestrator other fixes (ext_v2 dispatch, env-var, etc.) | ✓ | — |
| §2.10 CI sentinels — 4 of 5 | ✓ | — |
| §2.10 CI sentinels — `test_seed_csv_subset_of_live_mega_job` | ✓ | — |
| §2.11 CLAUDE.md amendment | ✓ | — |
| §2.12 evolve pin-drift discipline | — | ✓ |

#### What PR-1 retrains see during the first Phase 9 retry

PR-1 ships → post-merge CI clears → Phase 9 retry. During that retry:
- **vaep, xg_v2, ext_v2_p0, ext_v2_p1, defcon_lite, obso, pausa, f2v_v1** — all retrain on FRESH SK3-MIG-corrected data (Group 0 republishes vaep + xg_v2 inputs; defcon/obso/pausa/ext_v2 read from gold marts; f2v_v1 is already gold-mart-direct via SQL).
- **f2v_v2, f2v_360** — retrain on FRESH SK3-MIG-corrected data because §2.4b Group 0.5 triggers `hf_sync_prereq` (which refreshes `football2vec-training-data` and `football2vec-360-training-data` via existing `wf-football2vec-v2-export` + `wf-prepare-360-data` sub-operations of `hf_sync`).
- **scoutgpt** — continues to retrain on STALE `scoutgpt-training-data` (>1 month stale per §1.2). The wf-scoutgpt-export wiring is PR-2 work; scoutgpt cannot be made fresh in PR-1 without prematurely landing the wf-hf-sync amendment. PR description explicitly documents scoutgpt staleness as the one remaining gap closed by PR-2.

**Q31 verification (2026-05-04, reviewer-driven):** queried Databricks Jobs API `list_runs(job_id=302697362345215)` for hf_sync task `end_time > 2026-05-02T17:38Z` (SK3-MIG-A merge timestamp). **Zero post-merge successful hf_sync runs in last 20 mega-job invocations.** The daily mega-job's automatic hf_sync refresh has not fired since SK3-MIG-A. **Operator decision (2026-05-04): IN SCOPE for PR-1 — §2.4b adds `_trigger_mega_job_task("hf_sync_prereq")` as a Group 0.5 step.**

PR-2 ships → second Phase 9 retry, with γ trainers reading gold marts directly (no HF dataset round-trip), and the wf-hf-sync amendment refreshing public datasets for non-trainer consumers (evolve + 263+519+682 anonymous external downloaders).

The two-cycle approach burns one extra retry walltime (~6-9h × 2) but reduces blast-radius per merge and preserves operator's ability to ship today's fixes today.

Per reviewer Q10, each TDD step is a separate red/green pair against a specific sentinel test.

### §2.1 Architectural change: Group 2 trainers read from gold marts directly (γ)

Per the operator's choice on §1.2: **rewrite `scripts/train_football2vec_v2.py`, `scripts/train_football2vec_360.py`, `scripts/train_scoutgpt_hf.py` to read training data from `fct_action_values` (and related gold marts) via Databricks SQL, mirroring the f2v_v1 pattern.**

Each trainer:
1. Imports the existing data-prep logic from `src/ingestion/{export_embeddings_training_data,prepare_360_training_data,export_scoutgpt_training_data}.py` as a library module — OR — inlines the relevant transform if the source module has a Spark dependency that won't fly on HF Jobs.
2. Replaces `huggingface_hub.hf_hub_download(...)` / `datasets.load_dataset(...)` for the training data with a `query_databricks_sql(...)` call returning a pandas DataFrame.
3. Adds `DATABRICKS_TOKEN`, `DATABRICKS_HOST`, `DATABRICKS_SQL_WAREHOUSE_ID` to its env-var reads (same as f2v_v1 does today).
4. Verifies the source-of-truth column shapes match what the trainer expects (the HF dataset and the gold mart should have the same schema; if they don't, document the divergence).

The SQL data fetch may produce ~5-50 GB of pandas data depending on trainer. Two safeguards:
- Trainer uses chunked SQL pagination (e.g., `LIMIT/OFFSET` per match_id partition) for memory headroom on the HF Jobs container
- Per-trainer flavor (l40sx1) has 192 GB system RAM, enough for full-fact loads

**HF dataset publication is unchanged** — the existing `wf-hf-sync` sub-operations continue to publish HF datasets for downstream consumers. Trainers no longer depend on them. Stale HF datasets become a transparency / reproducibility nicety, not a correctness gate.

**scoutgpt-training-data refresh + republish (operator-mandated in scope):** see §2.4a — added to `wf-hf-sync.sub_operations`, gets refreshed and republished as part of orchestrator Step 6's daily-mega-job trigger.

### §2.2 Promote orchestrator dicts to module-level constants

Per reviewer Q5: the regex-on-dict-literal sentinel approach is fragile. Refactor:

```python
# scripts/sk3_mig_b_retrain.py — top of file, near _COST_CAP_USD
_FLAVOR_MAP: dict[str, str] = {
    "vaep": "cpu-large",
    "xg_v2": "l40sx1",
    "f2v_v1": "cpu-large",
    "f2v_v2": "l40sx1",
    "f2v_360": "l40sx1",
    "scoutgpt": "l40sx1",
}

_TASK_KEY_MAP: dict[str, str] = {
    "defcon_lite": "compute_defcon_lite",
    "obso": "compute_pausa",
    "pausa": "compute_pausa",
    "vaep": "compute_spadl_vaep",
    "xg_v2": "compute_xg_model_v2",
    # NOTE: scoutgpt_export removed — task doesn't exist; γ moved scoutgpt to gold-mart SQL.
}
```

Inner functions `_dispatch_trained_model` / `_task_key_for_item` consume these constants. Sentinel tests (§2.7) use `importlib` to load the module + introspect the constants directly — no regex.

### §2.3 Trainer-side `VALIDATED_HF_FLAVOR` constant

**Q28 note:** for `scripts/train_football2vec_360.py`, `VALIDATED_HF_FLAVOR = "l40sx1"` is the FIRST canonical declaration of that flavor — the trainer's docstring example invocation has no `--flavor` token (gap noted in §1.3 table). The constant ships in PR-1 alongside the orchestrator's `_FLAVOR_MAP` correction; sentinel §2.10.1 enforces parity going forward.


Per reviewer Q6: avoid docstring-grammar dependency. Each trainer declares:

```python
# scripts/train_<trainer>_hf.py — module-level
VALIDATED_HF_FLAVOR: str = "cpu-large"   # vaep
VALIDATED_HF_FLAVOR: str = "l40sx1"      # xg_v2, f2v_v2, f2v_360, scoutgpt
VALIDATED_HF_FLAVOR: str = "cpu-large"   # f2v_v1
```

Sentinel test imports `VALIDATED_HF_FLAVOR` from each trainer + cross-checks against orchestrator's `_FLAVOR_MAP[item]`. Single source of truth per trainer, asserted at CI time. No regex on docstrings.

Trainer docstring example invocation can reference the constant via comment or be hand-aligned; the test only cares about the constant.

### §2.4 Group 0 — input-dataset republish (vaep + xg_v2 only)

Add `_step_0a_group_0_inputs(state)` running AFTER pre-flight, BEFORE Group 1. Dispatches synchronously (subprocess.run + check returncode):
- `spadl_vaep_publish` (`scripts/publish_spadl_vaep_hf.py`)
- `xg_shots_publish` (`scripts/publish_xg_shots_hf.py`)
- `freeze_frame_publish` (`scripts/publish_freeze_frame_hf.py`)

These three are REMOVED from Group 3 (no duplication). The remaining Group 3 publishes (5 items: shots_on_target, obso_pausa_inputs, obso_trained_grids, obso_pausa_values, f2v_embeddings) are output publishes that depend on Group 1/2 retrains and stay.

**`shots_on_target_publish` placement (reviewer Q13):** verified during Phase 0 — publisher reads `FROM soccer_analytics.dev_gold.fct_shots s`. `fct_shots` includes xg prediction columns refreshed by `compute_xg_model_v2`. Therefore depends on xg_v2 retrain output → stays in Group 3.

Group 2 trainers (f2v_v2, f2v_360, scoutgpt) no longer need an export prereq because §2.1 γ has them reading from gold marts directly. **The original §2.2 export-prereq scope is dropped entirely.**

Considered alternative (reviewer Q8): pre-flight asserts HF dataset freshness and fails loud if stale, requires operator to manually trigger upstream refresh. Rejected because operator wants single-button retrain that "just works" without manual prereq orchestration; γ + Group 0 satisfies this without coupling pre-flight to external infra state.

### §2.4b Group 0.5 — hf_sync_prereq trigger (PR-1, Q31 in scope)

After the 3 Group 0 publishes complete, AND BEFORE Group 1 starts, the orchestrator triggers the daily mega-job's `hf_sync` task to refresh `football2vec-training-data` and `football2vec-360-training-data`. Mechanism: reuse existing `_trigger_mega_job_task` with a new `_TASK_KEY_MAP` entry:

```python
_TASK_KEY_MAP["hf_sync_prereq"] = "hf_sync"
```

New orchestrator step `_step_0b_hf_sync_prereq(state)`:

```python
def _step_0b_hf_sync_prereq(state: CycleState) -> None:
    """Refresh f2v_v2 + f2v_360 input datasets via daily-mega-job hf_sync task.

    Triggers the full mega-job and blocks until the `hf_sync` task reaches a
    terminal state. Other tasks in the mega-job continue running independently
    in parallel — this is the canonical mega-job-task trigger pattern (see
    `reference_mega_job_orchestrator_design.md`). Cost: ~$5-10 of mega-job
    compute attributable to the hf_sync task itself; walltime ~3 min for
    hf_sync (last 10 runs: 125-175s, well under wf-hf-sync's 1800s timeout).

    scoutgpt-training-data is NOT covered by this step — wf-scoutgpt-export is
    not yet in hf_sync.sub_operations until PR-2's amendment lands. PR-1
    Phase 9 retry leaves scoutgpt on stale input; PR-2's second Phase 9 retry
    closes that gap.
    """
    _emit_status(state, step="0b", phase="running", msg="hf_sync_prereq trigger (Q31 in scope)")
    if state.dry_run:
        _emit_status(state, step="0b", phase="running", msg="[dry-run] skip hf_sync trigger")
        return
    _trigger_mega_job_task(state, "hf_sync_prereq")
    _emit_status(state, step="0b", phase="complete", msg="hf_sync_prereq COMPLETE")
```

`main()`'s `steps_in_order` list inserts `("hf_sync_prereq", lambda: _step_0b_hf_sync_prereq(state))` between the existing `("group_0_inputs", ...)` and `("group_1", ...)` entries.

**Cost impact:** triggering the mega-job spawns up to 32 tasks running in parallel/sequence. The orchestrator only blocks until `hf_sync` task SUCCESS (3 min). Other concurrent tasks (e.g. `compute_xg_model_v2`, `compute_pausa`, `dbt_build_*`) run independently; some are no-ops by skip-guard (since data is fresh post-SK3-MIG-A); some cost real money. Per `wf-hf-sync.yaml` cost block: typical_dbu=8, typical_cost_usd=0.5 for `hf_sync` task ALONE; the mega-job-as-a-whole is more like $5-10 per full run. Adding to PR-1 retry cost: bump from $45.35-58.85 to $50.35-68.85.

### §2.4a HF dataset refresh + republish for all three Group 2 input datasets (PR-2, Q37 in scope)

**Operator-mandated in scope (2026-05-04):** the three Group 2 trainer-input datasets that γ frees the trainers from must still be refreshed and republished as part of every Phase 9 cycle. They have measured external usage (footbal2vec-training-data: 682 downloads; football2vec-360-training-data: 519; scoutgpt-training-data: 263) AND internal evolve consumers (`src/evolve/backends/remote_ssh.py`, `src/evolve/targets/football2vec/evaluator.py`, `scripts/evaluate_football2vec_l2_adversary_seeds.py`, `scripts/evaluate_scoutgpt_l2_seeds.py`). γ frees the trainers; it does NOT free evolve or external consumers. Public broken-coord data is not acceptable post-SK3-MIG.

**Q37 expansion (2026-05-04, IN SCOPE):** the same wf-hf-sync amendment also covers the 3 Group 0 input datasets (`spadl-vaep-action-values`, `xg-shots`, `xg-freeze-frame-data`) for daily refresh between orchestrator cycles. Currently those datasets are republished only when an orchestrator cycle runs (operator-triggered, infrequent). External consumers (Hub anonymous downloaders) read whatever-was-published-last between cycles. Adding to `hf_sync.sub_operations` means daily refresh in lockstep with `fct_action_values` / `fct_shots` / `fct_shot_freeze_frames` mart updates. Mechanism per below.

#### Mechanism

`wf-hf-sync` already orchestrates 6 sub-operations (per `workflow-cards/wf-hf-sync.yaml`), two of which produce two of the three target datasets:
- `wf-football2vec-v2-export` → `football2vec-training-data` ✓
- `wf-prepare-360-data` → `football2vec-360-training-data` ✓

`wf-scoutgpt-export` is NOT in `sub_operations` (the gap that explains scoutgpt-training-data's >1-month staleness).

**Verified (Phase 0 follow-up, reviewer Q19):** `src/ingestion/hf_sync.py` line 112 has a hardcoded `_SUB_OPERATIONS: list[tuple[str, Callable]]` list, NOT a workflow-card lookup. So the amendment is **two YAML lines per new sub-operation + matching Python list edits + a wheel rebuild**:

#### §2.4a.1 Add `wf-scoutgpt-export` to hf_sync (Q31's scoutgpt counterpart)

1. `workflow-cards/wf-hf-sync.yaml`: add `- wf-scoutgpt-export` to `sub_operations` (documentation-of-intent)
2. `workflow-cards/wf-scoutgpt-export.yaml`: change `trigger: manual` → `trigger: orchestrated` and add `orchestrated_by: wf-hf-sync`
3. `src/ingestion/hf_sync.py`: append `("wf-scoutgpt-export", export_scoutgpt_training_data_main)` to the `_SUB_OPERATIONS` list

#### §2.4a.2 Add Group 0 dataset publishers as Databricks-runtime sub-operations (Q37 — IN SCOPE)

The 3 existing PEP 723 publishers (`scripts/publish_spadl_vaep_hf.py`, `scripts/publish_xg_shots_hf.py`, `scripts/publish_freeze_frame_hf.py`) need Databricks-workflow counterparts so `hf_sync` can invoke them daily. Approach: extract the core publish logic from each PEP 723 script into a module function under `src/ingestion/`, then create a thin Databricks workflow card pointing at it. The PEP 723 scripts in `scripts/` continue to exist for orchestrator-cycle dispatch and import the same module function.

For each of the 3 publishers:
1. Refactor `scripts/publish_<name>_hf.py` to delegate to a new `src/ingestion/publish_<name>_hf.py:main()` (Databricks-runtime entry point reading from gold marts via Spark, writing to HF Hub).
2. New workflow card `workflow-cards/wf-publish-<name>.yaml` declaring `runtime: databricks-workflow`, `trigger: orchestrated`, `orchestrated_by: wf-hf-sync`, `entry_point: publish_<name>_hf`, `module: ingestion.publish_<name>_hf`.
3. `pyproject.toml` `[project.scripts]`: add `publish_<name>_hf = "ingestion.publish_<name>_hf:main"` entry-point.
4. `src/ingestion/hf_sync.py`: append `("wf-publish-<name>", publish_<name>_hf_main)` to `_SUB_OPERATIONS`.
5. `workflow-cards/wf-hf-sync.yaml`: append `- wf-publish-<name>` to `sub_operations`.

This is mostly mechanical (the PEP 723 publishers already do SQL fetch + HF Hub upload; just porting the SQL fetch from `query_databricks_sql` to `spark.table` / `spark.sql`). Estimated +400 LOC across 3 publisher refactors + 3 new workflow cards + the hf_sync wiring.

#### §2.4a.3 Wheel republish

Wheel republish (PR-2's TDD step) — the live mega-job's `hf_sync` task installs the wheel; the new 4 sub-operations (1 from §2.4a.1, 3 from §2.4a.2) only take effect after the next wheel republish. The wheel republish is also where `_FLAVOR_MAP` and `_TASK_KEY_MAP` constants land in the orchestrator's wheel — wait, no, those live in `scripts/` per Q34, NOT in the wheel. The PR-2 wheel republish covers `_SUB_OPERATIONS` + new module functions in `src/ingestion/publish_*_hf.py` only.

#### Trigger point in the orchestrator

The orchestrator's existing Step 6 (`_step_6_final_sweep`) already triggers the daily mega-job via `WorkspaceClient.jobs.run_now(job_id=mega_job_id)`. The mega-job's `hf_sync` task is one of its 32 tasks. After amendment, that task refreshes all three target datasets in one shot. **No new orchestrator step needed** — Step 6 covers it.

Timeout headroom (per reviewer Q20, verified empirically): wf-hf-sync currently runs in **125-175s** across the last 10 mega-job invocations (Databricks Jobs API `list_runs(job_id=302697362345215)`). wf-scoutgpt-export's standalone timeout is 900s (15 min) per its workflow-card. Total worst-case post-amendment: ~17 min, well under wf-hf-sync's 1800s timeout. **No preemptive timeout bump needed in this PR.** If wf-scoutgpt-export's actual runtime regresses past expectation, separate workflow-card amendment.

#### Why not Group 3 publish (alternative considered)

A PEP 723 `publish_scoutgpt_training_data_hf.py` Group 3 wrapper would require porting `ingestion.export_scoutgpt_training_data`'s Spark `applyInPandas` distribution to a driver-bound SQL fetch. ~250 LOC of new code. Group 3 placement would also require parallel rewrites for `wf-football2vec-v2-export` and `wf-prepare-360-data` to avoid divergent patterns. The wf-hf-sync amendment is two YAML lines vs ~750 LOC of new orchestrator code; the existing Databricks-workflow pattern is correct.

### §2.5 Runtime silly-kicks version assertion in ALL trainers

Per reviewer Q7-upgraded (defense against the verified §1.5 uv silent-downgrade behavior): every trainer MUST assert silly-kicks ≥ 3.0.1 at startup. Pattern (added to each trainer's `main()` near top):

```python
import silly_kicks
# Q38 note: minimum-only check. silly-kicks 4.0+ requires re-validation
# (the historical <2.0 upper-bound was originally a defense against exactly
# that — but a silent fossil was worse than no check; runtime assertion + pin
# bump on next major version is the correct discipline).
_REQUIRED_SK_MIN = (3, 0, 1)
_actual = tuple(int(p) for p in silly_kicks.__version__.split(".")[:3])
if _actual < _REQUIRED_SK_MIN:
    raise RuntimeError(
        f"silly-kicks {silly_kicks.__version__} < required {'.'.join(str(p) for p in _REQUIRED_SK_MIN)}. "
        f"SK3-MIG coords are wrong; refusing to train. "
        f"This is the uv silent-downgrade footgun; check trainer PEP 723 deps for explicit silly-kicks pin."
    )
```

Applies to: vaep, xg_v2, f2v_v1, f2v_v2, f2v_360, scoutgpt. Adds ~10 lines to each trainer.

### §2.6 vaep memory instrumentation

Per reviewer Q3: vaep trainer logs `psutil.Process().memory_info().rss` once per match in the feature-extraction loop. On OOM, the operator can read the high-water mark from logs and decide flavor escalation.

```python
import psutil
process = psutil.Process()
for i, (game_id, group) in enumerate(actions.groupby("game_id")):
    # ... feature extraction ...
    if i % 100 == 0:
        rss_gb = process.memory_info().rss / 1e9
        logger.info(f"feature_extraction game={i}/{n_games} rss={rss_gb:.2f}GB")
```

Add `psutil>=5.9` to vaep trainer's PEP 723 deps.

### §2.7 Spec corrections (§1.10)

Update `docs/superpowers/specs/2026-05-03-sk3-mig-b-retrain-and-republish-design.md` in 4+ locations to replace gpu-medium → l40sx1 (f2v_v2/f2v_360), gpu-large → l40sx1 (scoutgpt), cpu-large for f2v_v1. Per CLAUDE.md "documentation describes what IS, not what WAS."

### §2.8 Seed CSV reconciliation (per operator: fold into this PR)

`dbt_project/seeds/task_workflow_mapping.csv` has 11 entries that don't exist in the live mega-job. Action:
- Drop entries whose task_key is absent from the live mega-job
- Re-derive the canonical list from a one-shot script: `WorkspaceClient.jobs.get(...).settings.tasks` → CSV
- Add a sentinel test asserting seed CSV == live mega-job task list (or, if seed is documentation-of-intent, document why the divergence exists)

The 11 orphan entries identified in Phase 0 will be enumerated in the PR description; the test catches future drift.

### §2.9 Orchestrator other fixes

- §1.4 `_TASK_KEY_MAP`: `defcon_lite` value corrected to `compute_defcon_lite`. `scoutgpt_export` entry removed entirely (γ eliminated the prereq).
- §1.7 ext_v2 dispatch: rewrite to use `from analytics.ext_v2.harness import run_phase0_harness, run_phase1_harness`. Smoke gates `test_ext_v2_p{0,1}_post_retrain_smoke.py` rewritten to use the same harness functions.
- §1.8 f2v_v1 env-var: orchestrator passes `DATABRICKS_SQL_WAREHOUSE_ID` (additionally to `DATABRICKS_WAREHOUSE_ID`) in `secrets`.
- §1.9 wf-football2vec.yaml: update to point at `scripts/train_football2vec.py` PEP 723 (or fully retire the workflow card if SK3-MIG-B no longer uses it for inference).

### §2.10 CI sentinels (6 new tests, all importlib-based — Q22-confirmed CI-gated)

**Q22-confirmed (Phase 0 follow-up):** `.github/workflows/python-ci.yml` triggers on `push: branches: [main]` AND `pull_request`, with `secrets.HF_TOKEN` + `secrets.DATABRICKS_TOKEN` available to the lint-and-test job. Env-gated sentinels (§2.10.3 seed-vs-live, §2.10.6 input-dataset upstream check, §2.12.2 evolve pin-drift) run with secrets on every PR + every merge to main; drift caught at-or-near-merge. Forks / dependabot / draft PRs without secret access skip — acceptable per typical OSS posture.

Per reviewer Q5+Q15: commit to importlib introspection, no regex.

#### §2.10.1 `test_orchestrator_flavor_map_matches_trainer_constants`

```python
import importlib.util
def _load(path: str): ...  # std importlib pattern
orch = _load("scripts/sk3_mig_b_retrain.py")
TRAINER_PATHS = {
    "vaep":     "scripts/train_vaep_model_hf.py",
    "xg_v2":    "scripts/train_xg_v2_hf.py",
    "f2v_v1":   "scripts/train_football2vec.py",
    "f2v_v2":   "scripts/train_football2vec_v2.py",
    "f2v_360":  "scripts/train_football2vec_360.py",
    "scoutgpt": "scripts/train_scoutgpt_hf.py",
}
for item, path in TRAINER_PATHS.items():
    trainer = _load(path)
    assert orch._FLAVOR_MAP[item] == trainer.VALIDATED_HF_FLAVOR
```

#### §2.10.2 `test_orchestrator_task_keys_present_in_seed`

Loads `_TASK_KEY_MAP` via importlib + parses `dbt_project/seeds/task_workflow_mapping.csv` + asserts every value is in column 1 of the seed.

#### §2.10.3 `test_seed_csv_subset_of_live_mega_job` (env-gated)

Loads the seed + queries live mega-job task_keys via `WorkspaceClient.jobs.get(...)`. If `DATABRICKS_TOKEN` available, asserts seed ⊆ live tasks. Skipped otherwise. Catches seed drift in CI when secrets are wired.

#### §2.10.4 `test_no_trainer_pins_silly_kicks_explicitly`

For each PEP 723 trainer file, parses the metadata block and asserts no `silly-kicks` line. Already-functional pattern from existing tests like `test_topandas_boundedness.py`.

#### §2.10.5 `test_all_trainers_assert_silly_kicks_runtime_min`

Per reviewer Q18 (no source-grep fallback — commit to importlib introspection): each trainer exposes a module-level `_REQUIRED_SK_MIN: tuple[int, int, int] = (3, 0, 1)` constant. The sentinel imports each trainer via importlib + asserts the constant is defined AND equals `(3, 0, 1)`. The runtime check itself (the `raise RuntimeError(...)` block in `main()`) is not asserted by CI — covered by code review. Honest about what's mechanically testable.

#### §2.10.6 `test_orchestrator_input_dataset_is_upstream_of_trainer` (RESTORED per Q32 — PR-2 ships)

Static map `TRAINER_INPUT_DATASETS` curated in the test file from the audit:

```python
TRAINER_INPUT_DATASETS = {
    "vaep":     [("luxury-lakehouse/spadl-vaep-action-values",
                  "group_0", "spadl_vaep_publish")],
    "xg_v2":    [("luxury-lakehouse/xg-shots",
                  "group_0", "xg_shots_publish"),
                 ("luxury-lakehouse/xg-freeze-frame-data",
                  "group_0", "freeze_frame_publish")],
    "f2v_v2":   [("fct_action_values", "gold_sql", "γ-direct")],     # post-PR-2
    "f2v_360":  [("fct_action_values", "gold_sql", "γ-direct")],     # post-PR-2
    "scoutgpt": [("fct_action_values", "gold_sql", "γ-direct")],     # post-PR-2
    # f2v_v1 reads via SQL → fct_action_values directly; pre-flight Step 0
    # already enforces fct_action_values freshness via MAX(_loaded_at) gate.
    # ext_v2_p0 / ext_v2_p1 are local; out of scope.
}
```

For each trainer:
- If producer is `group_0`, asserts the trainer references the dataset string AND the orchestrator's source contains the producer item name in its Group 0 list (importlib of orchestrator + grep of trainer source).
- If producer is `gold_sql`, asserts the trainer source references `fct_action_values` AND uses `query_databricks_sql` (or equivalent SQL fetch helper). Catches a future trainer silently regressing back to HF dataset consumption.

This is the only sentinel that catches a future trainer drifting onto a Group 3 dataset (or onto a new HF dataset entirely). PR-2 ships it because the γ-direct entries in the map only become true post-PR-2.

### §2.11 CLAUDE.md amendment — uv silent-downgrade footgun

Add to `CLAUDE.md` under "Project Conventions" or "Engineering Standards":

```
- **uv silent-downgrade footgun in PEP 723 deps:** uv does NOT fail-fast on
  conflicting top-level vs wheel-transitive dep pins. The top-level pin wins
  silently. If a PEP 723 script declares `silly-kicks>=1.0.0,<2.0` and the
  wheel pulls `silly-kicks>=3.0.1`, uv silently installs silly-kicks 1.0.2
  (verified empirically 2026-05-04). This makes explicit pins in PEP 723
  scripts an active footgun: prefer letting the wheel's transitive pin be
  the single source of truth. If a PEP 723 script must pin a project-owned
  library, its main() MUST add a runtime version assertion (see e.g.
  scripts/train_*_hf.py's `_REQUIRED_SK_MIN` check).
```

### §2.12 Evolve pin-drift discipline (in scope per operator 2026-05-04)

The four evolve consumers of the Group 2 input datasets (`scripts/evaluate_football2vec_l2_adversary_seeds.py`, `scripts/evaluate_scoutgpt_l2_seeds.py`, `src/evolve/targets/football2vec/evaluator.py`, `src/evolve/backends/remote_ssh.py`) currently pull from HF Hub at HEAD revision. After this PR's HF dataset refresh, evolve experiments would silently shift onto fresh data mid-cycle — contaminating any in-flight architecture comparison.

**Pattern: explicit SHA pinning + drift CI sentinel + bump helper.** Standard ML research practice (frozen artifact per experiment family) plus a forcing function against silent staleness.

#### §2.12.1 Per-script pin constants

Each of the 4 evolve consumer scripts gets module-level constants:

```python
# scripts/evaluate_football2vec_l2_adversary_seeds.py — module top
PINNED_DATASET_REPO: str = "luxury-lakehouse/football2vec-training-data"
PINNED_DATASET_SHA: str = "abc1234..."  # set to post-Phase-9 HEAD by bump helper
PINNED_REASON: str = "L2 adversarial seed comparison; pin until next architecture cycle"
```

Each script updated to pass `revision=PINNED_DATASET_SHA` to `huggingface_hub.hf_hub_download(...)` / `datasets.load_dataset(...)`.

#### §2.12.2 `test_evolve_pin_drift` sentinel

```python
# src/tests/test_evolve_pin_drift.py
EVOLVE_SCRIPTS = [
    "scripts/evaluate_football2vec_l2_adversary_seeds.py",
    "scripts/evaluate_scoutgpt_l2_seeds.py",
    "src/evolve/targets/football2vec/evaluator.py",
    "src/evolve/backends/remote_ssh.py",
]
_MAX_AGE_DAYS = 90  # threshold; bump deliberately on architecture cycle boundaries

@pytest.mark.skipif(not os.environ.get("HF_TOKEN"), reason="HF Hub access required")
def test_pinned_shas_within_max_age():
    api = HfApi()
    for path in EVOLVE_SCRIPTS:
        mod = _load_module(path)
        head = api.dataset_info(repo_id=mod.PINNED_DATASET_REPO)
        if mod.PINNED_DATASET_SHA != head.sha:
            age = (datetime.now(timezone.utc) - head.last_modified).days
            assert age < _MAX_AGE_DAYS, (
                f"{path} pinned SHA is {age}d behind HEAD ({mod.PINNED_DATASET_REPO}). "
                f"Bump deliberately via `uv run python scripts/bump_evolve_pin.py {path}` "
                f"or extend _MAX_AGE_DAYS with rationale."
            )
```

The `90` threshold is a starting point; tune by experiment cadence. The test is env-gated (skips without `HF_TOKEN`); CI runs with secrets surface drift, runs without secrets silently skip — same pattern as §2.10.3.

#### §2.12.3 `scripts/bump_evolve_pin.py` operator helper

Single-shot CLI that the operator runs deliberately at the start of a new evolve experiment family:

```bash
uv run python scripts/bump_evolve_pin.py scripts/evaluate_scoutgpt_l2_seeds.py \
    --confirm-not-mid-experiment \
    --reason "starting new architecture cycle XYZ"
# Bumps PINNED_DATASET_SHA to current HF Hub HEAD.
# Refuses if --confirm-not-mid-experiment is absent.
# Updates PINNED_REASON inline.
```

**Q23 redesign:** the original draft proposed an "active evolve runs in last 7 days via bronze.workflow_costs" auto-check. Phase 0 verified evolve runs do NOT emit to `bronze.workflow_costs` (`grep -rnE "workflow_costs|cost_recorder|HFJobsCostRecorder" src/evolve/` returns zero matches; SQL query on `workflow_costs` for evolve workflow_ids in last 60 days returns 0 rows). The auto-check would have always succeeded and provided no protection. **Replaced with explicit operator confirmation flag** (`--confirm-not-mid-experiment`). The operator owns the mid-experiment-protection assertion explicitly, with a `--reason` string captured in the updated `PINNED_REASON`. ~50 LOC.

#### §2.12.4 Why `_MAX_AGE_DAYS=90`

ScoutGPT L2 seed harvest (PR #163) and football2vec L2 adversarial cycle (project_ev2_*) ran on the order of 2-4 weeks each. 90 days covers ~1 cycle plus headroom; longer pins go stale silently and the sentinel forces a deliberate bump or threshold extension.

#### §2.12.5 First-bump after this PR

After this PR ships AND Phase 9 retrains complete AND the wf-hf-sync amendment refreshes the three datasets, operator runs `bump_evolve_pin.py` for all 4 scripts to capture the post-Phase-9 HEAD as the new baseline. Documented as a §6 follow-up in PR description.

## §3 — Out of scope (explicit)

- **Full-refresh policy for export workflows.** Per `feedback_dbt_incremental_match_id_skip_silent_stale.md`. γ's gold-mart SQL reads make this concern moot for trainers; the §2.4a wf-hf-sync amendment ensures HF datasets are refreshed against current marts; what remains is whether the export workflows themselves bypass dbt incremental skip-on-match_id guards correctly. Separate follow-up.
- **f2v_v1 trainer rewrite.** It already reads from gold marts (the gold standard for γ). Already correct.
- **Daily mega-job task list audit beyond seed reconciliation.** The orchestrator's task references are the surface this PR fixes; cleaning up the broader 32-task DAG vs the workflow-cards inventory is its own audit cycle.
- **Evolve `_MAX_AGE_DAYS` tuning beyond initial 90.** Per §2.12.4 — starting threshold; future architecture cycles inform tuning.

## §4 — Test plan / TDD ordering

Per reviewer Q10, ordered red/green pairs:

| Step | Test | Code change | Section |
|---|---|---|---|
| 1 | `test_orchestrator_task_keys_present_in_seed` | Promote `_TASK_KEY_MAP`; correct `defcon_lite`; remove `scoutgpt_export` | §2.2, §2.9 |
| 2 | `test_no_trainer_pins_silly_kicks_explicitly` | Remove vaep trainer's silly-kicks line | §1.5, §2.10.4 |
| 3 | `test_all_trainers_assert_silly_kicks_runtime_min` | Add runtime assertion to all 6 trainers | §2.5 |
| 4a | (no test — refactor only, behavior-neutral) | Promote `_FLAVOR_MAP` to module-level constant in `scripts/sk3_mig_b_retrain.py`. Values stay wrong-but-current at this point. | §2.2 |
| 4b | `test_orchestrator_flavor_map_matches_trainer_constants` | Add `VALIDATED_HF_FLAVOR` to all 6 trainers AND simultaneously correct `_FLAVOR_MAP` to validated values. Both sides change in lockstep against the sentinel. | §2.3, §2.2 |
| 5 | (existing tests + manual) | ext_v2 dispatch fix + smoke gate rewrite | §1.7, §2.9 |
| 6 | (existing tests) | f2v_v1 env-var fix in orchestrator | §1.8, §2.9 |
| 7a | (manual + existing) | Add `_step_0a_group_0_inputs` step + telemetry kind `'input_publish'` | §2.4 |
| 7b | (manual + existing) | Move 3 publishes from Group 3 to Group 0 | §2.4 |
| 7c | (manual + existing) | Add `_step_0b_hf_sync_prereq` step + `hf_sync_prereq` entry to `_TASK_KEY_MAP` (Q31) | §2.4b |
| 7d | (test fixture) | Add psutil instrumentation to vaep | §2.6 |
| 8 | (test fixture per trainer) | γ trainer rewrite f2v_v2 (read fct_action_values via SQL) | §2.1 |
| 9 | (test fixture per trainer) | γ trainer rewrite f2v_360 | §2.1 |
| 10 | (test fixture per trainer) | γ trainer rewrite scoutgpt | §2.1 |
| 11 | `test_seed_csv_subset_of_live_mega_job` | Reconcile seed CSV (drop orphan entries) | §2.8 |
| 12 | (manual) | Spec corrections in 4+ locations | §1.10, §2.7 |
| 13 | (manual) | wf-football2vec.yaml cleanup | §1.9, §2.9 |
| **— PR-1 ends here. Steps 1-13 + 18 ship together. Phase 9 retry happens after PR-1 merges + CI clears. —** | | | |
| 14a | (manual + Python list + wheel) | §2.4a.1 — wf-scoutgpt-export wired into hf_sync sub_operations | §2.4a.1 |
| 14b | (manual — refactor + new workflow cards + Python list + wheel) | §2.4a.2 — port 3 Group 0 publishers to Databricks-runtime modules; add 3 new workflow cards; wire into hf_sync sub_operations (Q37 IN SCOPE) | §2.4a.2 |
| 15 | (manual) | Add `PINNED_DATASET_*` constants to 4 evolve consumers + `revision=` kwargs at HF download sites | §2.12.1 |
| 16 | `test_evolve_pin_drift` | Add evolve pin-drift sentinel | §2.12.2 |
| 17 | (manual) | Add `scripts/bump_evolve_pin.py` helper with `--confirm-not-mid-experiment` flag | §2.12.3 |
| 18 | (manual) | CLAUDE.md amendment | §2.11 — ships in PR-1 |
| 19 | Final sweep — ruff + pyright + pytest + dbt parse on seed CSV change + uv lock regen if PEP 723 deps changed (e.g. psutil added to vaep). **Q34-corrected:** wheel republish needed for PR-2 ONLY (since `_SUB_OPERATIONS` lives in `src/ingestion/hf_sync.py` which IS in the wheel per pyproject `[tool.hatch.build.targets.wheel] packages = ["src/ingestion", ...]`). PR-1 changes are entirely in `scripts/sk3_mig_b_retrain.py` (NOT in wheel — `scripts/` excluded from packages list); no wheel republish needed. PR-1 sentinels test the orchestrator script directly, not via wheel. | Final | — |

Approximate diff size: 800-1200 lines across orchestrator, 6 trainers, 4 evolve consumers, 1 new helper, telemetry module, 2 workflow-cards, spec doc, CLAUDE.md, 6 new tests. γ is the long pole; evolve pin-drift adds ~150 LOC.

## §5 — Cost recalculation, risks, and follow-ups

### §5.1 Cost cap recalculation

| Item | Pre-PR (current) | Post-PR (this design) | Notes |
|---|---|---|---|
| Group 0 publishes (3 items) | (in Group 3, $0.30) | $0.30 | move only |
| vaep | $0.50 (cpu-basic, OOM) | $2.00 (cpu-large) | flavor change + ±50% headroom |
| xg_v2 | $6.00 (l40sx1) | $6.00 | unchanged |
| ext_v2_p0/p1 | $0.05 each (local) | $0.05 each | unchanged |
| defcon/obso/pausa | $2.00 total | $2.00 | unchanged |
| f2v_v1 | $1.50 (gpu-medium) | $1.00-2.00 (cpu-large) | flavor restore; ±50% headroom — depends on per-match runtime |
| f2v_v2 | $4.00 (gpu-medium, stale data) | $5.00-7.00 (l40sx1, gold-mart fetch + train) | flavor restore + γ adds SQL fetch (5-10 GB rows; ~5 min add-on) |
| f2v_360 | $5.00 (gpu-medium, stale data) | $6.00-9.00 (l40sx1, gold-mart fetch + train) | as above |
| scoutgpt | $18.00 (gpu-large, stale data) | $20.00-26.00 (l40sx1, gold-mart fetch + train) | as above |
| Group 3 (5 publishes after Group 0 moves) | $0.50 | $0.50 | unchanged |
| §2.4a wf-hf-sync extra cost (PR-2 only) | n/a | $2.10 | wf-scoutgpt-export added; per its workflow-card cost block |
| §2.12 evolve pin-drift | n/a | $0 | static analysis; no new infra cost |

Per reviewer Q33 reconciliation: f2v_v2/f2v_360/scoutgpt have different costs in PR-1 vs PR-2. Splitting the row:

| Trainer | PR-1 retry (l40sx1, HF dataset consumption) | PR-2 retry (l40sx1, γ SQL fetch + train) | Delta from γ |
|---|---|---|---|
| f2v_v2 | $4.50-6.00 | $5.00-7.00 | +$0.50-1.00 (5-10 GB SQL fetch ~5-10 min × $2/h l40sx1) |
| f2v_360 | $5.50-8.00 | $6.00-9.00 | +$0.50-1.00 |
| scoutgpt | $19.00-25.00 | $20.00-26.00 | +$1.00-1.50 (larger fetch — sequence-format aggregation) |

| §2.4b hf_sync_prereq trigger (PR-1, Q31 in scope) | n/a | $5.00-10.00 | full mega-job spawn for hf_sync task wait |
| §2.4a.2 Group 0 datasets in sub_operations (PR-2, Q37 in scope) | n/a | (no new task cost) | reuses existing 3-publisher workflow infrastructure once converted; runs as part of hf_sync's existing 30-min budget |
| **Sum-of-rows (PR-1 first Phase 9 retry)** | ~$37 | **$50.35-68.85** | flavor restoration + hf_sync_prereq trigger; f2v_v2 + f2v_360 fresh; only scoutgpt stale |
| **Sum-of-rows (PR-2 second Phase 9 retry)** | ~$37 | **$52.95-71.45** | + γ overhead ($2.00-3.50) + wf-hf-sync amendment ($2.10) |

Both stay well under `_COST_CAP_USD = 80.0`. The cost trade is "PR-1 cheaper but partial fix (3 trainers stale-input)" vs. "PR-2 fully correct (γ + wf-hf-sync amendment)."

### §5.2 Risks (split per PR per reviewer Q35)

#### §5.2.1 PR-1 risks

- **First execution of corrected ext_v2 dispatch.** Orchestrator now invokes `from analytics.ext_v2.harness import run_phase0_harness, run_phase1_harness`; never previously run from this entry point. Mitigation: smoke gate `test_ext_v2_p0_post_retrain_smoke.py` rewritten to use the same harness API and validated locally pre-merge.
- **First execution of `_TASK_KEY_MAP['defcon_lite'] → compute_defcon_lite`.** Task exists in live mega-job (Phase 0.5 verified) but the orchestrator has never invoked it under the corrected key. First-execution risk is bounded — defcon_lite is a compute-only mega-job task; failure surfaces as orchestrator halt with the task's error message, not silent corruption.
- **First execution of `_step_0a_group_0_inputs` (3 publishes synchronously chained).** Each publisher has been individually invoked via the daily mega-job historically (per `bronze.workflow_costs` row counts); chaining them is new. Mitigation: each subprocess.run + check returncode; halt on first failure with the publisher's stderr surfaced.
- **psutil dep added to vaep trainer** — extra ~1 sec install per HF Job dispatch. Negligible.
- **Seed CSV orphan removal may break unrelated tests.** Mitigation: §2.10.2 + §2.10.3 are the new seed truth sources; surface during implementation.
- **Q31 staleness leak: f2v_v2/f2v_360/scoutgpt train on stale HF inputs unless operator manually triggers hf_sync first.** Acknowledged limitation; PR-2 closes it. PR description must explicitly document.

#### §5.2.2 PR-2 risks

- **γ trainer rewrites are the highest-risk change in this PR-2.** Q17 verified all 3 export modules are Spark-bound (`spark.table`, `applyInPandas`, `pyspark.sql.functions/types`). Reimplementing in pandas-on-driver requires bit-parity validation against the historical HF dataset rows. Mitigation: each γ rewrite is a separate TDD step (§4 #8/#9/#10) with a test fixture comparing pandas output against the historical HF dataset row for shape + value parity on a small sample.
- **wf-hf-sync amendment first-execution risk.** Q19-corrected to include the Python list edit in `src/ingestion/hf_sync.py`. The wheel republish is the gating step — until the new wheel is on the daily mega-job's Python env, the amendment has no effect. Mitigation: explicit wheel-republish step in §4 #19 plus operator-triggered one-shot mega-job run post-PR-2-merge to verify hf_sync's new sub-operation fires.
- **First execution of new export sub-operation in production.** wf-scoutgpt-export has run standalone (`trigger: manual`) but never as an orchestrated sub-operation. Q20 timeout headroom is comfortable (125-175s base + 900s cap → well under 1800s). Risk is bounded by sub-operation isolation: per `wf-hf-sync.yaml` "failures in one sub-operation are logged at ERROR and do not abort the parent task" — wf-scoutgpt-export crash leaves f2v_v2/f2v_360/sync-hf-costs untouched.
- **§2.10.6 input-dataset upstream sentinel (PR-2-shipped) requires γ to be in place.** The map's `gold_sql` entries become true only post-γ. Sentinel + γ rewrite must merge atomically.
- **Evolve pin-drift first-bump after PR-2** — per §2.12.5, operator runs `bump_evolve_pin.py --confirm-not-mid-experiment` for all 4 evolve scripts to capture post-Phase-9 HEAD. Until the bump, evolve scripts are pinned to pre-this-PR-cycle SHAs (existing behavior); after the bump, they're pinned to post-Phase-9 fresh data. No silent behavior change between merge and bump.

### §5.3 Open follow-ups (NOT in this PR)

- **Workflow-card vs live mega-job audit** — broader pattern beyond seed CSV. The 11 orphan entries in seed are the symptom; the cause is workflow-cards drifting from terraform reality. Separate audit cycle.
- **First-bump of evolve pin SHAs** — after PR-2 ships AND second Phase 9 retrains complete, operator runs `bump_evolve_pin.py --confirm-not-mid-experiment` for all 4 evolve consumer scripts to capture post-Phase-9 HEAD. Operator-runtime task, not PR work.
- **Phase 9 retries (two of them)** — PR-1's first retry validates orchestrator path with f2v_v2/f2v_360/scoutgpt on stale-input. PR-2's second retry validates γ trainer correctness end-to-end. Each ~7-9h walltime; sequential.
- **Q37 between-cycle staleness of Group 0 datasets — RESOLVED IN PR-2 SCOPE.** `spadl-vaep-action-values`, `xg-shots`, `xg-freeze-frame-data` are added to `hf_sync.sub_operations` per §2.4a.2. Between-cycle freshness via daily mega-job refresh.

### §5.4 Acknowledged-and-rejected design alternatives

- **β: terraform-deploy export task_keys + use existing prereq pattern** (rejected). Adds export tasks to mega-job as standalone tasks; deploys via terraform. Pros: independently triggerable; clean DAG. Cons: bigger blast radius; deployment ordering issues; γ achieves the same correctness goal without touching the mega-job DAG.
- **Hybrid (γ for f2v_v2/f2v_360, β-style for scoutgpt)** (rejected). Saves rewriting scoutgpt trainer. Cons: scoutgpt-training-data has measured external usage (263 downloads) + internal evolve consumers; we can't skip refreshing it; γ + §2.4a wf-hf-sync amendment is cleaner than adding a standalone task to mega-job DAG.
- **(i) Re-point evolve scripts at gold marts directly** (rejected for the 4 evolve consumers). Eliminates staleness but breaks the architecture-comparison reproducibility invariant — the L2 seed harvests + adversarial seed evaluations all require frozen data per experiment family. (ii) SHA-pinning + drift sentinel preserves reproducibility AND prevents silent staleness; better fit for research use case.
- **Pre-flight HF Hub last_modified check** (rejected, reviewer Q8). Adds a freshness gate without solving the staleness; would just halt the cycle until operator runs the daily mega-job manually. γ is single-button.

## §6 — Approval gate

### §6.1 PR-1 (orchestrator-only) — APPROVED 2026-05-04

Operator-confirmed scope for PR-1 ahead of implementation:
- §2.2 module-level constants (`_FLAVOR_MAP`, `_TASK_KEY_MAP`)
- §2.3 `VALIDATED_HF_FLAVOR` constants in all 6 trainers
- §2.4 Group 0 publishes (vaep + xg_v2 inputs only)
- §2.4b Group 0.5 hf_sync_prereq trigger (Q31 in scope)
- §2.5 runtime silly-kicks assertion in all 6 trainers (mandatory, defense vs verified §1.5 uv silent-downgrade)
- §2.6 vaep psutil instrumentation
- §2.7 spec corrections (preserve-with-corrections per Q24)
- §2.8 seed CSV reconciliation
- §2.9 orchestrator other fixes (ext_v2 dispatch + env-var + workflow-card cleanup)
- §2.10 5 of 6 importlib sentinels (§2.10.1 flavor parity, §2.10.2 task_keys-vs-seed, §2.10.3 seed-vs-live env-gated, §2.10.4 no-pin, §2.10.5 runtime-assert constant)
- §2.11 CLAUDE.md amendment
- §5.1 PR-1 cost row $45.85-58.85 against $80 cap

Implementation begins now per §4 steps 1-13 + 18. Phase 9 retry happens after PR-1 merges + post-merge CI clears.

### §6.2 PR-2 (γ + wf-hf-sync + evolve) — scope approved, implementation gated on PR-1 success

Operator-confirmed scope for PR-2 (begins after PR-1's Phase 9 retry validates the orchestrator correctness path):
- §2.1 γ trainer rewrites (Q17 verified ~750 LOC pandas reimplementation; per-trainer parity validation required)
- §2.4a wf-hf-sync amendment (Q19 verified Python-list-edit + wheel-rebuild required); §2.4a.1 covers scoutgpt; §2.4a.2 covers 3 Group 0 datasets via Databricks-workflow conversion (Q37 in scope)
- §2.10 1 of 6 importlib sentinels (§2.10.6 input-dataset upstream check, RESTORED per Q32) + §2.12.2 evolve pin-drift sentinel
- §2.12 evolve pin-drift discipline (Q23 fixed: explicit `--confirm-not-mid-experiment` flag in §2.12.3)
- §5.1 PR-2 cost row $47.95-60.95 against $80 cap

PR-2 sequencing: branches off PR-1's merge SHA after PR-1's Phase 9 retry produces correct vaep + xg_v2 + ext_v2 + defcon/obso/pausa + f2v_v1 outputs. PR-2's Phase 9 retry is the second cycle, validating γ trainer parity end-to-end.
