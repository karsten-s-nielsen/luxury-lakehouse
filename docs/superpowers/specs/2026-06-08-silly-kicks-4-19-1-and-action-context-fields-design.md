# silly-kicks 4.19.1 adoption + action-context field additions — Design (PR-1)

**Date:** 2026-06-08
**Status:** Approved-pending-review
**Author:** Karsten Nielsen (with Claude)
**ADR:** ADR-042 (new, written in this PR)

## 1. Context

The lakehouse runs silly-kicks **4.13.0** (`pyproject.toml`: `silly-kicks[das,ghost-gk]>=4.13.0,<5`).
Latest is **4.19.1** (verified on PyPI 2026-06-08). The changelog 4.14.0→4.19.1 carries new
action-context aggregators, one breaking ghost-GK column rename + value-semantics change, a
dtype-contract correctness sweep that shifts several existing AC values, and the Sportec/IDSSE
cross-label fix (the `project_idsse_pass_cross_bug` in project memory).

This is **PR-1 of a three-part program**:

- **PR-1 (this spec):** force silly-kicks 4.19.1 *everywhere*; add the new AC fields; ghost-GK
  rename; dtype-contract cleanup; regenerate both goldens; ADR-042. **No live full recompute** —
  `fct_action_context` stays at its current ~1,414 sparse rows; code + schema + goldens only.
- **PR-2 (separate cycle):** IDSSE SPADL re-conversion (4.16.1 cross fix) + VAEP champion retrain +
  re-score + HF republish. Its own spec.
- **Next cycle:** full all-provider `compute_action_context` recompute (currently on hold pending
  silly-kicks tuning lock-down; 4.19.1 is that lock-down, but the full run is explicitly deferred).

### Verified facts (against the 4.19.1 source + live backend)

- Installed 4.13.0; PyPI latest 4.19.1; pin `>=4.13.0,<5`.
- Live `soccer_analytics.dev_gold.fct_action_context` = **1,414 rows** (sparse 1-GS-match state).
- New tracking `add_*` aggregators in 4.19.1 vs the chain we run today: `add_structural_pass`
  (4.16.0), `add_xcross_attempt` (4.18.0), `add_player_influence` (pre-existing, never adopted),
  `add_off_ball_runs` (pre-existing — **redundant**, emits the 4 columns already produced by the
  `add_off_ball_context` umbrella; **skipped**).
- `infer_ball_carrier` / `derive_team_in_possession` (4.19.1 `_ball_carrier.py`) are
  dtype-internally-consistent (pre-index to numpy; merge on `[game_id, period_id, frame_id]` only)
  — they do not cross-compare id dtypes. The 4.15.0 seam coercion (`_id_compat.canonical_id`)
  covers the **registered `add_*` aggregators** (incl. `add_das`).

## 2. New field inventory

| Source aggregator | New columns | dbt / Spark type |
|---|---|---|
| `add_structural_pass` | `structural_lbs`, `structural_sgm`, `structural_sdi` | bigint, double, double |
| `add_xcross_attempt` | `xcross_attempt` | double |
| `add_player_influence` | `actor_reachable_area_m2`, `off_ball_xt_team`, `off_ball_xt_opponent`, `off_ball_xt_diff`, `reachable_area_team`, `reachable_area_opponent`, `reachable_area_diff` | double ×7 |
| ghost-GK rename | `ghost_gk_spread` → **`ghost_gk_density_spread`** | double |

Net: **+11 columns, 1 rename.** `structural_lbs` is `Int64` in silly-kicks (count, NaN-distinct) →
`bigint` in DDL/dbt. All three new aggregators use the bundled "default" model / pure geometry →
**no network, serverless-safe** (mirrors the existing bundled xS / ghost-GK defaults).

### NaN / coverage contracts (load-bearing for tests)

- `structural_*`: NaN for non-pass/non-cross actions, and for the non-possessing team's actions.
- `xcross_attempt`: the possessing team's cross propensity; NaN for a non-possessing-team action at
  the linked frame, and where the bundled model's required features are absent (e.g. SB360 frames
  with no velocity).
- `player_influence`: requires the shared pitch-control surface; honest NULL where the linked frame
  lacks the needed players.

## 3. Compute layer — `src/analytics/action_context/`

### 3.1 `enrich.py :: _enrich_tracking_match`

Insert three steps into the 21-step chain, reusing the shared `xt` + `PitchControlCache`:

```python
# Structural pass primitives (TF-45; no xt, no pitch control)
out = add_structural_pass(out, tracking_df, links=links, home_team_id=home_team_id)

# Player influence (xt positional; shared cache; spearman for velocity-aware full tracking)
out = add_player_influence(
    out, tracking_df, xt, links=links, home_team_id=home_team_id,
    method="spearman", pitch_control_cache=pc_cache,
)

# xCrossAttempt (bundled "default" model; shared cache; actions_for_context for score_diff)
out = add_xcross_attempt(
    out, tracking_df, model=None, links=links, home_team_id=home_team_id,
    actions_for_context=actions_df, pitch_control_cache=pc_cache,
)
```

Rename the ghost-GK spread handling to `ghost_gk_density_spread` (the 4.14.0 emitted name). The
served `ghost_gk_x/y` now carry the boosted-HGBR mean (≈1.07 m MAE) instead of the KDE mode — a
deliberate value change, not an API break.

Import the three new aggregators (`add_structural_pass`, `add_xcross_attempt` from
`silly_kicks.tracking`; `add_player_influence` from `silly_kicks.tracking.features`).

### 3.2 `enrich.py :: _enrich_sb360_match`

Add the same three where single-frame-supportable, using `method="voronoi"` for the
pitch-control-dependent ones (freeze-frames have no velocity; spearman returns all-NaN). xCross on
SB360 will emit NaN where the bundled model's velocity features are unavailable — honest NULL, per
ADR-039's partial-coverage discipline. Behaviour verified per-path in tests (§7).

### 3.3 `schema.py`

- Add the 11 columns to `RESULT_COLUMNS` and `ACTION_CONTEXT_DDL` (the **single source of truth**;
  the Spark `StructType` is parsed from `ACTION_CONTEXT_DDL` in `ingestion/action_context.py`, so
  there is no separate StructType to edit).
- Rename `ghost_gk_spread` → `ghost_gk_density_spread` in both `RESULT_COLUMNS` and the DDL.
- Update the header comment column count.

### 3.4 Dtype-contract cleanup (4.15.0 handshake) — `convert.py` / `pipeline.py` / work-unit entry

Two parts:

1. **Additive guard (low risk, always in PR-1):** call
   `validate_id_dtypes(actions, frames, home_team_id=..., on_mismatch="raise")` once at work-unit
   entry (the driver `_process_tracking_match` / `enrich_batch` boundary, mirroring ADR-040's
   `assert_work_unit_time_base`). Loud pre-flight on any actions↔frames id-dtype mismatch.

2. **TDD-gated removal of `_coerce_gradientsports_frame_ids_to_native_str`
   (`convert.py:513`, called at `pipeline.py:139`):** the 4.15.0 changelog invites dropping this
   workaround in favour of seam coercion, but the lakehouse's own possession-fill produces a
   mixed-dtype `team_in_possession` column, so safety cannot be proven by inspection. Gate the drop
   on a **red-first** test that runs the GS enrich path with **Int64** frame ids (coercion bypassed)
   through carrier → possession → DAS + actor/opponent resolution and asserts nonzero/correct
   values under 4.19.1.
   - **Green** → remove the coercion + its call site; keep `validate_id_dtypes` as the guard;
     update `test_gradientsports_roster_dicts.py` (which currently asserts the coercion's behaviour).
   - **Red** → **keep** the coercion (the seam does not cover the directly-called primitives for
     this path); ship only the `validate_id_dtypes` guard. Record the residual gap as a silly-kicks
     follow-up (extend canonicalization to `infer_ball_carrier`/`derive_team_in_possession`
     consumers).

   This honours the user's "drop / add validate_id_dtypes" decision while obeying Chesterton's fence
   — the drop only lands if proven safe.

## 4. dbt layer

- `models/staging/action_context/stg_action_context__values.sql`: add 11 `cast(...) as ...` lines;
  rename the ghost cast to `cast(ghost_gk_density_spread as double) as ghost_gk_density_spread`.
- `models/marts/fct_action_context.sql`: add the 11 columns to both the `action_raw` CTE and the
  `final` select; rename ghost. (Incremental + `append_new_columns` + enforced contract.)
- `models/marts/_marts__models.yml` (`fct_action_context`): add 11 `- name / data_type` entries;
  rename the ghost column entry. Enforced contract must match the SELECT exactly.

The new fields are tracking-only; event-only providers (statsbomb/wyscout) emit them as NULL —
consistent with the existing tracking-column NULL pattern (no accepted_values / not_null tests).

## 5. Migrations + gold full-refresh + synced-table rebuild (operator runbook)

> **Corrected after review (C1, C2).** Two premises in the original draft were false against the live
> repo and are fixed here:
> 1. **Bronze-migration auto-apply no longer exists.** `dbt-live-ci.yml` (verified at current HEAD)
>    is a **daily scheduled** job (`cron: 0 9 * * *`) + `workflow_dispatch`; it has **no**
>    "apply pending migrations" step and **no** `_runner.py` reference. Both new migrations are
>    **operator-applied**, and ordering before any dbt build is operator-critical. There is no
>    "auto tree vs operator tree" distinction any more — **both files live flat in
>    `scripts/migrations/`** (the repo convention; e.g. `2026-05-29-add-ghost-gk-to-action-context.sql`).
>    No `operator/` subdir.
> 2. **`fct_action_context` IS a synced Lakebase table** — `fct_action_context_synced` (TRIGGERED, PK
>    `action_context_id`, `refresh_synced_tables.py:236`) with 3 custom PG indexes
>    (`create_indexes.py:229-231`). Both `fct_action_context` and `fct_action_context_synced` confirmed
>    live in `dev_gold`. The gold full-refresh + the non-additive rename **break** the synced table and
>    require an explicit rebuild.

### 5.1 Migration files (both flat in `scripts/migrations/`, both operator-applied)

- `2026-06-08-add-ac-structural-xcross-playerinfluence.sql` — idempotent
  `ALTER TABLE soccer_analytics.bronze.spadl_action_context ADD COLUMNS (...)` for the **11 new
  columns**. (`_runner.py` makes single-leading-column ADD COLUMNS idempotent via a `DESCRIBE`
  pre-check.)
- `2026-06-08-rename-ghost-gk-spread.sql` — enable Delta column-mapping, then rename:
  ```sql
  ALTER TABLE soccer_analytics.bronze.spadl_action_context SET TBLPROPERTIES (
    'delta.columnMapping.mode' = 'name',
    'delta.minReaderVersion' = '2',
    'delta.minWriterVersion' = '5'
  );
  ALTER TABLE soccer_analytics.bronze.spadl_action_context
    RENAME COLUMN ghost_gk_spread TO ghost_gk_density_spread;
  ```
  `_runner.main()` executes arbitrary DDL via `_exec` (the `_ADD_RE` match is idempotency-only, **not**
  a dispatch filter — verified), so RENAME runs through `_runner.py`. RENAME is **not** idempotent in
  the runner, so the file header documents "run once" + a manual column-existence pre-check. Delta
  column-mapping is a **one-way protocol bump** (minReader=2/minWriter=5) on
  `bronze.spadl_action_context` — recorded as irreversible in ADR-042.

### 5.2 Operator runbook (ordering is mandatory; ~1,414 rows so all steps are cheap)

Run as a single operator sequence at merge time (NOT relying on any CI auto-apply):

> **R1 — chosen path: explicit delete + SDK create (Option B), not heal (Option A).** Heal
> (`maintain_synced_tables.py` Step -2 → `ingestion.heal_synced_tables`) only recreates a synced table
> it can *detect* as checkpoint-broken, and project memory records that synced-table `PIPELINE_FAILED`
> **lags ~13 min** behind the break (DLT retry+backoff) — so running heal synchronously right after the
> full-refresh would not yet see a broken table. Option B is deterministic and immediate, matches the
> canonical "non-additive schema change → manual delete + SDK recreate" convention, and is trivial at
> ~1,414 rows. Steps below compose (delete → recreate via the SDK create path, not a maintenance-only
> command).

1. **Apply bronze ADD COLUMNS:** `uv run --extra sdk python scripts/migrations/_runner.py scripts/migrations/2026-06-08-add-ac-structural-xcross-playerinfluence.sql`
2. **Apply bronze RENAME:** `uv run --extra sdk python scripts/migrations/_runner.py scripts/migrations/2026-06-08-rename-ghost-gk-spread.sql`
3. **Delete the synced table + its PG ghost** (the rename is non-additive — synced tables auto-evolve
   only *additive* columns): `uv run --extra sdk python scripts/delete_synced_table.py fct_action_context_synced`
   (deletes the Databricks synced table **and** drops the PG ghost table).
4. **Gold full-refresh:** `dbt run --full-refresh --select stg_action_context__values fct_action_context`
   — drops+recreates the Delta table so no stale `ghost_gk_spread` column lingers under the enforced
   contract. The mart's dbt config already sets `delta.enableChangeDataFeed = true`
   (`fct_action_context.sql`), so the recreated table carries CDF; **verify** `delta.enableChangeDataFeed`
   post-build and `ALTER TABLE … SET TBLPROPERTIES('delta.enableChangeDataFeed'='true')` only if absent
   (CDF is required for the synced source). Doing this *after* step 3 avoids leaving the existing
   synced table pointing at a dead Delta table id.
5. **Recreate the synced table via the symmetric single-table CLI** (added in PR-1, Task 11B —
   mirrors `delete_synced_table.py`; resolves the canonical config from `SYNCED_TABLES`, NOT
   `--phase 3` which creates all 41):
   `uv run --extra sdk python scripts/create_synced_table.py fct_action_context_synced`
   A freshly-created TRIGGERED synced table **auto-starts its initial sync** (verified:
   `synced_table_lifecycle.py:38-41`), and the CLI `wait_until_online`s — so no separate refresh is
   needed (step 6 uses `--skip-refresh`).
6. **Reapply grants + the 3 PG indexes:**
   `uv run --extra sdk python scripts/maintain_synced_tables.py --skip-refresh --skip-heal` (runs
   grants + `create_indexes` + verify; `--skip-refresh` because the fresh table auto-syncs,
   `--skip-heal` because the recreate is explicit). The daily `lakebase-grants` action would also
   reapply these, but the runbook does it inline so the app isn't degraded between merge and the next
   07:00 cron.

This runbook ships in the PR description and the ADR. Because `dbt-live-ci.yml` is a daily job, if the
branch merges without steps 1–2 applied, the **next daily live build breaks** (staging casts
`structural_lbs`/etc. + `ghost_gk_density_spread` from bronze columns that don't exist) — so the
runbook is executed **with** the merge, not after.

> **Follow-up (meta):** the project `CLAUDE.md` still documents the removed bronze-migration
> auto-apply contract. **Folded into this PR — see §12.**

## 6. Golden re-baseline (mandatory)

Ghost-GK (boosted mean) + 4.15.0 dtype fixes (`bekkers_pi`, `cover_shadows`, `player_influence`
internals) + the 11 new columns all change AC output, so **both** goldens are regenerated with
4.19.1 installed locally:

- `scripts/build_ac1_full_golden.py` → `J03WMX_p1` full golden.
- `scripts/build_ac1_mini_golden.py` → 3-action IDSSE mini-golden (the always-on per-PR e2e gate).
- `src/tests/action_context/oracle_map.py` → rename `ghost_gk_spread`; add the 11 columns.

Per the standing rule: after any silly-kicks bump that changes tracking-enrichment values, **both**
goldens are regenerated in the same PR.

## 7. "Force silly-kicks 4.19.1 everywhere"

| Location | Change |
|---|---|
| `pyproject.toml` | `silly-kicks[das,ghost-gk]>=4.13.0,<5` → `>=4.19.1,<5` |
| `uv.lock` | `uv lock --refresh-package silly-kicks` + `uv sync --inexact` (per `reference_uv_dep_adoption`) |
| `src/shared/wheel.py` | `WHEEL_VERSION 0.5.22 → 0.5.23` via `bump_wheel.py` (propagates to all 6 trainer wheel URLs) |
| `scripts/train_{football2vec,football2vec_360,football2vec_v2,scoutgpt_hf,vaep_model_hf,xg_v2_hf}.py` | `_REQUIRED_SK_MIN (4,13,0) → (4,19,1)` |
| `scripts/submit_ac1_oneshot.py` | PEP-723 `silly-kicks>=4.19.1,<5` |
| `scripts/sk3_mig_b_retrain.py` | PEP-723 `silly-kicks>=4.19.1,<5` + runtime assertion `< 4.19.1` |
| `terraform/modules/workflows/main.tf` | env silly-kicks pin → 4.19.1 |
| `src/tests/test_terraform_env_dep_parity.py` | expected pin string → 4.19.1 |
| `src/tests/test_sk3_mig_b_orchestrator_invariants.py` | §2.10.5 `_REQUIRED_SK_MIN == (4,19,1)`; `[spadl]` pin string → 4.19.1 |
| `.github/workflows/python-ci.yml`, `dbt-live-ci.yml` | update any pinned silly-kicks reference |

`scripts/profile_ac1_local.py` docstring examples (`silly-kicks==4.1.1/4.2.0`) are non-load-bearing
doc — left as historical examples (optional refresh).

## 8. ADR-042

New ADR documenting:
- ghost-GK boosted-mean serve + `ghost_gk_density_spread` rename. **Consequences** must call out the
  Hyrum value-semantics flip on `ghost_gk_x/y` (KDE mode ≈4.65 m → boosted-HGBR mean ≈1.07 m), not
  just the rename. **Consumer audit (M1, verified):** there are **no runtime / mart / UI consumers**
  of `ghost_gk_*` (zero hits in `hf_taipy_app` and in marts other than `fct_action_context`); the only
  references outside the AC compute code are the test fixtures/goldens and the HF dataset card this PR
  updates (§9). So the value shift is contained to `fct_action_context` + its synced mirror (both
  rebuilt in §5).
- The 4.15.0 dtype-contract value shifts + the lakehouse-side handshake decision (§3.4).
- The 11 new AC fields.
- The Delta column-mapping protocol bump on `bronze.spadl_action_context` as **irreversible**.

**Academic reference (M3, verified against silly-kicks `NOTICE`):** mirror the NOTICE spelling exactly
— **`Karakus, O., & Arkadas, H. (2026). "Structural Pass Analysis in Football…", arXiv:2603.28916`**
(ASCII, no diacritics). These authors are **not yet** in `ARCHITECTURE.md` Appendix D → add them to
Appendix D **and** the `expected_authors` list in `test_architecture_md_appendix.py` (strict CI gate;
the D56 audit root cause). Add the xCross attribution + `NOTICE` entry per the same rule.

## 9. Testing strategy (TDD where it fits)

- **Schema:** `test_schema.py` — explicit assertions for the 11 new columns + the ghost rename;
  `RESULT_COLUMNS ↔ ACTION_CONTEXT_DDL` parity auto-covers them.
- **Enrich helpers:** `test_enrich_helpers.py` / `test_action_context_enrichment.py` — assert the
  emitted **new-column set equals the declared set** (set-equality vs `RESULT_COLUMNS`, not mere
  presence — guards `add_player_influence`'s 7 names against any upstream drift that would break the
  DDL↔RESULT_COLUMNS parity + enforced contract). NaN contracts (structural NaN for non-pass; xcross
  NaN for non-possessing-team). SB360 path: present + honest-NULL where unsupported.
- **Dtype cleanup:** the red-first GS Int64-id resolution test (§3.4) decides drop-vs-keep;
  `validate_id_dtypes` guard has a fail-loud unit test.
- **Goldens:** both regenerated; `test_mini_golden.py` (real-pipeline e2e on the IDSSE slice) green.
- **Live parity / dbt:** `test_action_context_live_ddl_parity.py` green post-migration; dbt contract
  compiles; stage-3 build over staging + marts.
- **Pins:** `test_terraform_env_dep_parity.py`, `test_sk3_mig_b_orchestrator_invariants.py` green.
- **Repo-wide `ghost_gk` audit (verified)** — readers are confined to the AC pipeline/tests/dbt this
  PR already touches, plus two extra touchpoints to update for the rename + new columns:
  `src/tests/action_context/test_sb360_coverage.py` (rename) and
  `docs/huggingface/dataset-cards/spadl-action-context.md` (rename + document the 11 new columns; the
  HF dataset republish itself rides with the deferred recompute, but the in-repo card is corrected in
  PR-1 per the ADR-014 card-parity rule). No `hf_taipy_app` / other-mart consumers.
  - **`test_hf_publish_parity.py` must stay green** with the card edited but the dataset not re-pushed:
    confirm that test asserts only the filename==repo-basename + card-inventory parity (in-repo state),
    not live column-level parity against the published dataset. If it does assert live parity, note the
    expected state / skip condition rather than breaking it.
- **Shift-left gate before "done":** `uv run ruff check`, `uv run pyright src/`,
  `uv run pytest src/tests/action_context -v` + the changed-pin tests.

## 10. Non-goals (explicit)

- No IDSSE SPADL re-conversion, no VAEP retrain (PR-2).
- No full all-provider AC recompute (next cycle).
- No new UI surfacing of the new metrics (separate work; the fields land in the mart only).

**Forward-pointer for the next-cycle full recompute (not PR-1 work):** adding `add_player_influence`
(spearman + pitch control) and `add_xcross_attempt` (bundled model + pitch control) to the chain
raises per-frame cost. Irrelevant here (no recompute), but the next-cycle full run must re-check the
ADR-037 per-half worker-drain watchdog (2700 s) against the heavier chain before kicking off. Noted
in ADR-042 so it isn't a surprise.

## 11. Risks

- **Golden regeneration requires a local 4.19.1 enrich run** — the full golden (`J03WMX_p1`) needs
  IDSSE tracking + SPADL fixtures present locally. Mitigation: confirm fixtures before regen; the
  mini-golden is the CI gate regardless.
- **Dtype-cleanup drop** could re-open the GS id bug if the red-first test is mis-scoped.
  Mitigation: the test exercises carrier/possession/DAS/actor/opponent end-to-end; default to KEEP
  on any doubt.
- **Gold full-refresh under enforced contract** — verify the renamed-column contract compiles before
  the live full-refresh.
- **Synced-table breakage (C2)** — the §5 gold full-refresh + non-additive rename break
  `fct_action_context_synced` (stale streaming checkpoint + 3 dropped PG indexes) unless the §5.2
  runbook (delete synced → full-refresh → recreate + re-enable CDF → reapply indexes/grants) is
  followed in order. Mitigation: the runbook is mandatory and ships in the PR description.
- **Merge/runbook ordering (C1)** — because `dbt-live-ci.yml` is a daily job with no auto-apply, a
  merge without the operator migrations applied breaks the next daily live build. Mitigation: execute
  §5.2 with the merge, not after.

## 12. CLAUDE.md correction (folded into PR-1)

The C1 error originated in a **stale `CLAUDE.md` bullet** (line 216, "Project Conventions") that
still documents the removed auto-apply contract. Verified 2026-06-08: no `.github/workflows/*.yml`
references `scripts/migrations/_runner.py`; `dbt-ci.yml` is slim CI (parse + manifest), `dbt-live-ci.yml`
is daily-cron with no migration step. Replace the bullet so future specs don't repeat C1.

**Current (stale) — CLAUDE.md:216:**
> **Bronze migrations under `scripts/migrations/*.sql` are auto-applied at live-build CI time**
> (session 69 — `.github/workflows/dbt-live-ci.yml` "Apply pending bronze migrations" step). Any new
> file added in a PR (`git diff --diff-filter=A` against `origin/main`) is run through
> `scripts/migrations/_runner.py` BEFORE `dbt build` triggers… [etc.]

**Replacement:**
> **Bronze migrations under `scripts/migrations/*.sql` are operator-applied — there is NO CI
> auto-apply.** (The former `dbt-live-ci.yml` "Apply pending bronze migrations" step was removed;
> verified 2026-06-08 — no workflow references `scripts/migrations/_runner.py`.) Apply each new
> migration manually **with** the merge via
> `uv run --extra sdk python scripts/migrations/_runner.py scripts/migrations/<file>.sql` —
> `dbt-live-ci.yml` is a **daily scheduled** live build, so a merge whose migration is unapplied
> breaks the next daily build. `_runner.main()` executes ANY statement, but only single-leading-column
> `ALTER TABLE ADD COLUMNS` is made idempotent (DESCRIBE skip-if-exists); **every migration MUST still
> be idempotent by construction** (`UPDATE … WHERE col IS NULL`, `SET TBLPROPERTIES`,
> `CREATE TABLE IF NOT EXISTS`). A non-idempotent op (e.g. `RENAME COLUMN`) is a documented run-once.
> Destructive ops (`DROP`/`DELETE`/`TRUNCATE`) remain operator-driven. Always verify with a live
> `DESCRIBE`/`SELECT` post-apply.

The `reference_bronze_migration_autoapply_gap` project-memory entry has already been corrected to
match (done during this review).
