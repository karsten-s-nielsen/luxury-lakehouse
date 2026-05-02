# SK3-MIG — silly-kicks 3.0.0 direction-of-play migration (Group A)

| Field | Value |
|---|---|
| **Date** | 2026-05-02 |
| **Status** | Draft (brainstorm complete; awaiting plan) |
| **Cycle** | SK3-MIG (Group A — data correctness; Group B — model retrains tracked separately) |
| **silly-kicks reference** | v3.0.0 (commit `a1ebfa0`, tag `v3.0.0`, PR-S22) + CI follow-up `fd742d8` |
| **Companion ADR (silly-kicks side)** | ADR-006 (in `D:\Development\karstenskyt__silly-kicks`) |
| **Companion ADR (lakehouse side)** | **ADR-022** — to be created in this PR |
| **Triggering investigation** | OPT-1 e2e probe (2026-05-02), `project_silly_kicks_direction_of_play_bug.md`, `reference_provider_coordinate_conventions.md` |

## §0 — Context (one paragraph)

silly-kicks 3.0.0 corrects a dual-mirror direction-of-play inversion present since v0.1.0. The bug: `_fix_direction_of_play` (converter) and `play_left_to_right` (VAEP `compute_features`) both mirrored the away team's `(x, y) → (105-x, 68-y)`. For possession-perspective providers (StatsBomb, Wyscout) the two mirrors cancelled by accident, producing correct VAEP gamestates but broken raw SPADL coords. For absolute-frame providers (IDSSE/Sportec, Metrica) the converter produced correct SPADL but VAEP's second mirror inverted away-team rows. OPT-1's e2e probe of `expected_threat` against `dev_gold.fct_action_values` exposed the broken SPADL state via a U-shaped global xT grid (max=0.035 vs the 2026-03-13 grid's monotonic max=0.116). silly-kicks 3.0.0 introduces an explicit `InputConvention` enum + `to_spadl_ltr` dispatcher per converter and removes the second mirror from VAEP. **SK3-MIG Group A** is the lakehouse-side data correctness migration: bump the pin, adapt call sites, force-rebuild `bronze.spadl_actions` and all coord-dependent downstream marts under the new converter behaviour, wipe + recompute `expected_threat_grids`, and verify end-to-end with explicit provider-coverage and coord-correctness gates. Model retrains and HF dataset republishes are **deferred to Group B** as a separate single-PR follow-up cycle, captured as a TODO row at PR merge time.

## §1 — Code-side changes

### 1.1 Pin bump + wheel + TF env-spec parity

- `pyproject.toml`: `silly-kicks>=2.5.0,<3.0` → `silly-kicks>=3.0.0,<4`
- `bump_wheel.py` advance: 0.3.29 → 0.3.30
- 3 Terraform env-spec files updated for the new floor (per `src/tests/test_terraform_env_dep_parity.py` — failing test surfaces the exact files)

### 1.2 silly-kicks 3.0.0 API adaptation in `src/ingestion/spadl_vaep.py`

silly-kicks 3.0.0 deleted `_fix_direction_of_play` from `silly_kicks.spadl.base` and removed the second mirror from `vaep.compute_features`. Each of our 4 source-specific converter call sites must invoke `to_spadl_ltr(input_convention=...)` explicitly. Per-source values:

| Provider | `InputConvention` value | Source of truth |
|---|---|---|
| StatsBomb | `POSSESSION_PERSPECTIVE` | `reference_provider_coordinate_conventions.md` |
| Wyscout | `POSSESSION_PERSPECTIVE` | same |
| IDSSE / Sportec | `ABSOLUTE_FRAME_HOME_RIGHT` | same |
| Metrica | `ABSOLUTE_FRAME_HOME_RIGHT` | same |

Audit + adaptation also covers any other module that imports from `silly_kicks.spadl.base`: `src/ingestion/spadl_conversion.py` and any place referenced in `src/tests/test_silly_kicks_boundary.py`. Verify whether VAEP `compute_features`' new `frames_convention` kwarg requires an explicit value at our call site (likely yes for tracking-aware VAEP feature paths).

### 1.3 Tracking-adapter audit (preserve current behaviour by default)

silly-kicks 3.0.0 makes `silly_kicks/tracking/*.py` adapters output SPADL-LTR by default with `output_convention="absolute_frame"` opt-out + `DeprecationWarning`. The lakehouse has multiple tracking-aware ingestion modules that consume tracking frames in absolute coords today: `pitch_control_batch.py`, `line_breaking.py`, `off_ball_xt.py`, `formations_efpi.py`, `formations_shape_graph.py`, `defcon_lite.py`, `elastic_sync.py`. **Default in this PR:** pin all consumers to `output_convention="absolute_frame"` to preserve current behaviour. Any LTR migration is out of scope for SK3-MIG; if any consumer would benefit from LTR semantics, that's a follow-up PR.

### 1.4 Strict-mode env var in CI + production

`SILLY_KICKS_ASSERT_INVARIANTS=1` set in:

- `.github/workflows/python-ci.yml`
- `.github/workflows/dbt-live-ci.yml`
- Production Databricks job environment (TF env declaration, all coord-dependent jobs)

Effect: silly-kicks's `validate_input_convention` raises on mismatch instead of warn. The user explicitly chose production+CI rather than CI-only — daily-job fails loud on any future regression.

### 1.5 Remove the v2 → v1 XGBoost feature-list fallback (ADR-012 §2 grace-period closure)

Lines 240-255 of `src/ingestion/xg_model_v2.py` are deleted in this PR. Replace with a strict envelope read:

```python
v2_envelope = json.loads(v2_weights_bytes.decode("utf-8"))
v2_features = v2_envelope.get("feature_names")
if not v2_features:
    raise RuntimeError(
        "v2 weights envelope is missing 'feature_names'. "
        "ADR-012 §2 grace-period removal — refresh @Champion via "
        "scripts/train_xg_v2_hf.py before re-running."
    )
v2_tabular_dim = v2_envelope["tabular_dim"]
assert len(v2_features) == v2_tabular_dim, (
    f"v2 envelope is inconsistent: feature_names={len(v2_features)} "
    f"!= tabular_dim={v2_tabular_dim}. Envelope corrupted at training time."
)
cache["v2_features"] = list(v2_features)
```

Justification: ADR-012 §2 specified "one release window, then the fallback is removed." The v2 retrain that produces `feature_names`-bearing envelopes shipped 2026-04-22 (PR #177 `ecf2551`); we have shipped 17+ wheel versions since (0.3.12 → 0.3.29). The grace period has more than expired. The fallback alive while we add a check that flags it as a footgun is contradictory; SK3-MIG is the natural moment to close the loop because the orchestrator's pre-flight check (§6) verifies the new strict path before any inference runs.

ADR-012 §2 receives a one-line update from "for one release window, then the fallback is removed" → "the fallback was removed in PR #N (silly-kicks 3.0.0 / SK3-MIG cycle, 2026-05-02)" — the actual PR number is filled in at PR-merge time.

### 1.6 New ADR-022

`docs/superpowers/adrs/ADR-022-direction-of-play-migration.md` documents:
- The dual-mirror bug (lakehouse-side perspective)
- Why silly-kicks 3.0.0's per-converter `InputConvention` design is the right downstream fix
- The lakehouse-side consequences (Group A / Group B split; tracking-adapter `output_convention` posture)
- Cross-references to silly-kicks ADR-006

### 1.7 New TODO row — `XG1-RETIRE`

Added to `TODO.md` On Deck section at PR-prep time. Scope (verbatim from §6 below): retire `compute_xg_predictions` v1 entirely now that the fallback path is closed. Includes UI migration in Shot Map (currently consumes `xg_logistic, xg_gradient_boosted` from `fct_xg_predictions_synced` per `hf_taipy_app/src/state/shot_map.py:82`). Wicked-sized.

## §2 — Sequencing (orchestrated rebuild)

The rebuild lives in `scripts/sk3_mig_rebuild.py` — one orchestrator script that issues Databricks job triggers in dependency order with per-step verification. Steps must be idempotent; the script supports `--start-at <step>` for re-firing after a transient failure.

| Step | Action | Verification gate before next step |
|------|--------|----|
| 0 | Pre-flight: `pip show silly-kicks` reports 3.0.0+; wheel 0.3.30 active | Pin/wheel sanity |
| 1 | Capture `DESCRIBE HISTORY` versions for `bronze.spadl_actions`, `bronze.vaep_action_values`, `dev_gold.expected_threat_grids` → write to JSON sidecar `sk3_mig_rollback.json` | Sidecar written, 3 versions captured |
| 2 | DELETE `bronze.spadl_actions` (full) + DELETE `bronze.vaep_action_values` (full) | Both tables row count == 0 |
| 3 | Trigger `compute_spadl_vaep` Databricks job; wait for completion | Job state == SUCCESS |
| 4 | **E2E provider-coverage gate** (Gate A in §3) | All 4 sources present in both bronze tables, row counts within ±5% of pre-rebuild |
| 5 | Trigger 3-stage `dbt build` (input → intermediate → output marts) | All stages SUCCESS |
| 6 | DELETE `dev_gold.expected_threat_grids` (all rows) → trigger `compute_expected_threat` (`need_global=True` exercised by OPT-1 streaming refactor) | Job SUCCESS, table re-populated |
| 7 | **xG v1/v2 dimension pre-flight check** (§6 — runs against MLflow @Champion artifacts) | All 3 checks pass |
| 8 | Trigger coord-dependent inference workflows in dependency order: `compute_xg_predictions`, `compute_xg_predictions_v2`, `compute_defcon_lite`, `compute_pausa`, `import_obso_results` (+ `compute_obso` if applicable), `compute_player_embeddings_v1`, `compute_player_embeddings_v2`, `compute_player_embeddings_360`, F2V batch inference workflows | Each job SUCCESS |
| 9 | Final 3-stage `dbt build` (refreshes marts that depend on new bronze prediction rows) | All stages SUCCESS |
| 10 | Refresh Lakebase synced tables + restore custom indexes via `run_lakebase_grants.py` + `maintain_synced_tables.py` | Synced-table state ONLINE, indexes present |
| 11 | **Final coord-correctness probe** (Gate B in §3) | All 4 sources show ~50/50 high-x / low-x split per team |

The orchestrator's exit code is non-zero if any gate fails; the PR cannot be merged until a clean run completes (`scripts/sk3_mig_verify.py --full` against the live `dev_gold` returns 0).

## §3 — Validation gates

Implemented in `scripts/sk3_mig_verify.py` (separate script so it can be re-run after merge for regression checking).

### Gate A — Provider coverage (the user's strict requirement)

```sql
SELECT data_source, COUNT(*) rows, COUNT(DISTINCT match_id) matches
FROM bronze.spadl_actions
GROUP BY data_source
ORDER BY 1
```

**Pass criteria:** 4 rows, one per `data_source ∈ {statsbomb, wyscout, idsse, metrica}`. Per-source row counts must be **within ±0.5% of pre-rebuild snapshot** (orchestrator captures pre-rebuild counts in `sk3_mig_rollback.json` step 1). silly-kicks 3.0.0's change is to coordinate values, not row emission, so per-source totals should be near-exact in principle. The 0.5% bound exists to absorb minor differences from any incidental converter behaviour change (e.g., type-coercion, NaN handling); a wider deviation indicates a regression and the gate fails.

**Same probe + criteria applied to `bronze.vaep_action_values` and `dev_gold.fct_action_values`.**

### Gate B — Coord-correctness (the OPT-1 diagnostic, formalised)

```sql
SELECT data_source,
       COUNT(*)                                                   AS pairs,
       SUM(CASE WHEN avg_x > 52.5 THEN 1 ELSE 0 END)              AS high_teams,
       SUM(CASE WHEN avg_x <= 52.5 THEN 1 ELSE 0 END)             AS low_teams
FROM (
    SELECT match_id,
           team_id,
           data_source,
           AVG(start_x)  AS avg_x,
           COUNT(*)      AS n
    FROM dev_gold.fct_action_values
    WHERE action_type IN ('shot', 'shot_penalty', 'shot_freekick')
    GROUP BY match_id, team_id, data_source
    HAVING COUNT(*) >= 3
) AS per_team
GROUP BY data_source
```

Notes on the literals: SPADL pitch length is 105m, so `pitch_mid = 52.5`. Shot action types in SPADL are `('shot', 'shot_penalty', 'shot_freekick')` per `dbt_project/models/marts/fct_funnel_stages_agg.sql:121`.

**Pass criteria post-rebuild (CORRECTED 2026-05-02 mid-cycle):** for every source, `low_teams / pairs <= 0.10` — i.e., ≥90% of per-team-match pairs have avg shot start_x **above** the midline. Per silly-kicks 3.0.0+ docstring, canonical SPADL LTR means "every team's actions are oriented as if the team plays from left to right — shots cluster at high-x for both teams." Both teams at high-x (NOT a 50/50 split) is the correct post-fix state. The pre-rebuild OPT-1 diagnostic table (`reference_provider_coordinate_conventions.md`) measured RAW BRONZE x-distribution, not SPADL output — the "50/50 split" applies to absolute-frame providers' bronze events, not their SPADL output.

### Sanity probe — `expected_threat_grids` (informational, not a hard gate)

Global xT grid should be **monotonic-increasing** in x (high values near attacking goal, low values near own goal) — NOT U-shaped. Pre-rebuild current grid: `max ≈ 0.035` U-shape. Post-rebuild expectation: monotonic with `max` somewhere in (0.05, 0.15) range — exact value depends on the new 4-source data mix. Recorded in PR body for future reference.

## §4 — Error handling / rollback

- **Pin bump rollback:** revert `pyproject.toml` + `bump_wheel.py`, force `pip install --force-reinstall 'silly-kicks<3.0'`. Trivial.
- **Bronze rollback:** Delta Lake time travel via the JSON sidecar from step 1. `RESTORE TABLE bronze.spadl_actions TO VERSION AS OF <pre-delete-version>` — same for `bronze.vaep_action_values` and `dev_gold.expected_threat_grids`.
- **Lakebase synced-table rollback:** re-sync from rolled-back gold via `maintain_synced_tables.py`.
- **Mid-rebuild failure:** re-fire from the failed step via `--start-at <step>`. Per-step idempotency guarantees no partial-state cleanup needed — each step either completes or doesn't write.
- **Pre-flight check fail at step 7 (xG v2 envelope missing `feature_names`):** orchestrator halts; fix is one of (a) refresh v1 weights from HF Hub via `artifact_deploy.upload_weights_to_uc_volume(...)` if v1 is the issue, (b) trigger a v2 retrain (escalates to Group B early), or (c) accept the mismatch with `--force` flag (logs an override warning; not recommended).

## §5 — Testing

### 5.1 Unit tests (existing)

Must pass under `SILLY_KICKS_ASSERT_INVARIANTS=1`:

- `src/tests/test_spadl_vaep_writer_parity.py`
- `src/tests/test_silly_kicks_boundary.py`
- `src/tests/test_spadl_vaep.py`

Any test that implicitly relied on the old dual-mirror behaviour gets fixture-updated. Any test that exercised the v2 → v1 fallback specifically gets either deleted (if it asserted fallback behaviour) or updated (if it just used a legacy fixture — fixture is regenerated with `feature_names`).

### 5.2 New invariant test

`src/tests/test_sk3_coord_correctness.py` — synthetic 2-team fixture (one home, one away, 5 shots each at known x positions), run through `compute_spadl_vaep`'s SPADL conversion path, assert per-team avg `start_x` is split (one high, one low). This is the unit-test equivalent of Gate B; lives next to `test_silly_kicks_boundary.py`. Catches any future regression where a stray mirror is re-introduced anywhere in our call chain.

### 5.3 Live-CI integration

`.github/workflows/dbt-live-ci.yml` runs the full dbt build against `dev_gold` after the bronze re-derive — natural integration test.

### 5.4 Verification script

`scripts/sk3_mig_verify.py` — implements Gate A, Gate B, the xT sanity probe, and the §6 xG v1/v2 pre-flight. Exit code 0 = clean. Re-runnable post-merge for regression checking.

### 5.5 PR body content (mandatory)

- Probe outputs for both gates (pre-rebuild + post-rebuild snapshots side-by-side per source)
- xT grid `(max, sum, monotonicity)` summary before + after
- Drift signals expected during the Group A → Group B window (model predictions are biased — current weights against new coords; `fct_model_validation_baselines` will fire harmlessly until Group B retrains)
- Cross-references: silly-kicks ADR-006, lakehouse ADR-022, OPT-1 PR #248, `project_silly_kicks_direction_of_play_bug.md`, `reference_provider_coordinate_conventions.md`

## §6 — Explicit xG v1/v2 dimension-mismatch pre-flight

Gates step 7 of the orchestrator. Lives in `scripts/sk3_mig_verify.py`. **Three checks:**

### Check 1 — Artifact resolution + feature-list extraction

```
v1: load @Champion XGBoost from MLflow → booster.feature_names → N1 = len(...)
v2: load @Champion weights envelope from MLflow → JSON-parse →
    v2_features = envelope["feature_names"]   # MUST exist post-§1.5
    v2_tabular_dim = envelope["tabular_dim"]
```

Report N1, `len(v2_features)`, `v2_tabular_dim`, and the resolved MLflow run IDs for both into the PR body for permanent record.

### Check 2 — Envelope consistency (post-§1.5 collapsed to one assertion)

```python
if not v2_features:
    raise RuntimeError(
        "v2 weights envelope is missing 'feature_names'. "
        "ADR-012 §2 grace-period removal — refresh @Champion via "
        "scripts/train_xg_v2_hf.py before re-running."
    )
assert len(v2_features) == v2_tabular_dim, (
    f"v2 envelope inconsistent: feature_names={len(v2_features)} "
    f"!= tabular_dim={v2_tabular_dim}."
)
```

Soft warning (does not block) if v1 booster's feature list and v2 envelope's feature list diverge — they're allowed to post-ADR-012, but the diff is logged so any unintentional drift is visible.

### Check 3 — End-to-end smoke inference on synthetic input

Build a 1-shot synthetic `pd.DataFrame` matching the `fct_shots` schema (one row, all required columns populated, `shot_freeze_frame` JSON with one synthetic player). Invoke each model's actual UDF path on this input; both must return one row of predictions without raising. Catches:

- Schema drift between `fct_shots` columns and what the UDF reads
- Set-encoder shape errors that only surface during forward pass
- `parse_freeze_frame` JSON-parsing changes
- v1 booster expecting feature names that no longer exist in tabular input

Smoke-test takes <5 s. Far cheaper than discovering the failure 20 min into a real `compute_xg_predictions_v2` job that has to retry from scratch.

**On any check failure:** orchestrator halts before step 8 (any inference triggers), reports the diagnostic, waits for human resolution. See §4 for resolution paths.

## §7 — Out of scope (deferred to Group B)

Captured as a single TODO row at PR-prep time. Items:

1. Re-train ALL action-value-derived models: VAEP, xG v1+v2, xT v1 production grid, ExT v2 P0+P1 baselines (validation NLL changes likely), DEFCON-lite, OBSO, PAUSA, Football2Vec v1+v2+360, ScoutGPT (re-tokenize)
2. Re-publish all HF datasets that ride on `fct_action_values`: `spadl-vaep`, `xg-shots`, `freeze-frame`, `shots-on-target`, embedding training data
3. Re-baseline `fct_model_validation_baselines` (every drift threshold will fire simultaneously without a fresh baseline once Group B retrains shift prediction distributions)
4. Refresh `docs/performance-baselines.md` with re-build cycle timings

These are explicitly **not** in SK3-MIG Group A. The Group A → Group B window will see drift detection fire harmlessly (model predictions are biased — current weights against new coords). PR body documents this expectation; no action needed until Group B ships.

## §8 — XG1-RETIRE TODO row content (added to TODO.md at PR-prep time)

```
| XG1-RETIRE | Retire compute_xg_predictions (v1) workflow + fct_xg_predictions mart | Wicked | SK3-MIG (2026-05-02) — fallback removal made v1 dead-code from inference path | **Triggered when:** SK3-MIG ships (the v2 fallback removal eliminates v1's inference-path role; only Shot Map's display columns remain). **Scope:** (1) Delete src/ingestion/xg_model.py + the v1 entry point in pyproject.toml + the v1 workflow card wf-xg-v1.yaml + Terraform job declaration; (2) Delete dbt_project/models/marts/fct_xg_predictions.sql + dbt_project/models/staging/xg/stg_xg__predictions.sql + their YAML contract entries + _xg__sources.yml v1 references; (3) Drop the Lakebase fct_xg_predictions_synced synced table + indexes via scripts/delete_synced_table.py; (4) Wipe the v1 MLflow registered model + UC Volume v1 weights folder; (5) **UI migration in hf_taipy_app/src/state/shot_map.py**: either DROP the v1 custom xG display columns (xg_logistic, xg_gradient_boosted), OR MIGRATE to display v2's xg_set_encoder + xg_ci_lower + xg_ci_upper instead (UX decision — needs design call); (6) Delete fetch_xg_predictions() in hf_taipy_app/src/queries/shots.py; (7) Update HF model card xg-model-statsbomb-wyscout.md to reflect retirement (or delete + update org-card.md per ADR-014). **References:** ADR-012 §2 grace-period closure; SK3-MIG PR; hf_taipy_app/src/state/shot_map.py:82-235. |
```

## §9 — Open implementation questions to resolve in the plan

- Exact list of `silly_kicks.spadl.*` import sites in our codebase (only `spadl_vaep.py` + `spadl_conversion.py` known; live-grep at plan time)
- Whether `compute_features` `frames_convention` kwarg has a sensible default we can rely on, or whether each VAEP feature path needs an explicit value
- Whether any test fixture in `src/tests/_fixtures/` was generated under the old converter and now produces different SPADL output (will surface as test failures during §1 implementation; fixtures regenerated as needed)
- The exact list of TF env-spec files that fail `test_terraform_env_dep_parity.py` post-pin-bump (3 known per OPT-1; live-confirm)
- Whether the orchestrator's `compute_obso` step is a no-op (OBSO is currently `import_obso_results` only — confirm at plan time whether there's a `compute_obso` workflow or whether all OBSO data flows through the import path)

## §10 — References

- silly-kicks v3.0.0 release (commit `a1ebfa0`, tag `v3.0.0`, PR-S22) — `D:\Development\karstenskyt__silly-kicks`
- silly-kicks ADR-006: direction-of-play handling per converter
- Lakehouse ADR-012 §2: training-to-production delivery hardening (`feature_names` envelope convention + grace-period rule this PR closes)
- Lakehouse ADR-014: HF card inventory parity (`upload_hf_readme` + filename == repo basename)
- Lakehouse ADR-018: cross-table format-contract testing
- Lakehouse ADR-022 (NEW, in this PR): direction-of-play migration
- OPT-1 PR #248 (`b3c9d9e` on main) — the e2e probe that surfaced the bug
- Memory: `project_silly_kicks_direction_of_play_bug.md`
- Memory: `reference_provider_coordinate_conventions.md`
- Memory: `feedback_drop_calendar_effort_estimates.md`
- TODO.md SK3-MIG row (top of On Deck) — superseded by this spec at PR-merge time
