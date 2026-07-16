# PR-1 Implementation Plan: TC-1 Retirement + Re-home

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or
> superpowers:executing-plans. Steps use checkbox (`- [ ]`) syntax.

**Goal:** Delete the `compute_tracking_context` (TC-1) pipeline end-to-end and re-home its two real consumers
(GK identity, IDSSE minutes) onto the action-context (AC-1) pipeline, which is a proven-canonical superset.

**Architecture:** TC-1 is a redundant duplicate of AC-1 producing a strict column+provider subset on
un-oriented frames with 4,052 divergent duplicate keys. AC-1 covers every action TC-1 does (`TC-only = 0`
live) and is dedup-free by construction (M13 work-unit ownership). This PR re-points the two consumers, hardens
AC's dedup as defense-in-depth, then removes the entire TC-1 vertical.

**Tech stack:** dbt (Databricks), PySpark ingestion, Terraform (mega-job), pytest.

**Spec:** `docs/superpowers/specs/2026-07-14-mart-consolidation-tc1-retirement-design.md`
**Review:** `docs/superpowers/specs/2026-07-14-mart-consolidation-tc1-retirement-REVIEW.md` (+ REVIEW-2)

**⚠ READ BEFORE STARTING:**
- This is a **CLAUDE.md-governed repo.** Never commit without explicit user approval. One branch / one commit
  / one PR.
- **Line numbers in this plan are from 2026-07-14 and WILL drift.** Where a step says "delete the block at
  `file:NNN-MMM`", first `grep` for the named symbol to confirm the current location. Trust the *symbol names*,
  not the line numbers.
- Several steps are **live-data gated** (the parity gate, Task 1) or **operator-only destructive** (the bronze
  DROP, Task 10). Do not auto-execute those — surface them.

---

## Task 0: Branch + green baseline

**Files:** none (setup)

- [ ] **Step 1: Sync main and branch.**

```bash
cd /d/Development/karstenskyt__luxury-lakehouse
git checkout main && git pull --ff-only origin main
git checkout -b feat/tc1-retirement
```

- [ ] **Step 2: Confirm a green baseline BEFORE any change** (so later failures are attributable).

```bash
uv run ruff check src/ scripts/ && uv run ruff format --check src/ scripts/
uv run pyright src/
uv run lint-imports
uv run pytest src/tests/ -q -p no:warnings   # capture exit code; must be 0
```

Expected: all pass. If the baseline is red, STOP and report — do not build on a broken baseline.

---

## Task 1: THE PARITY GATE — live, run FIRST, STOP on failure

**This is a pre-condition, not a code change.** It compares GK identity derived from TC-1 vs from AC while
**both pipelines still exist**, and decides go/no-go. Re-homing changes two variables at once (dedup pick +
frame orientation), so a difference is only acceptable if **AC is verified correct against roster ground
truth** — not if it merely differs.

**Files:**
- Create (scratch, NOT committed): `scratchpad/pr1_parity_gate.py`

- [ ] **Step 1: Write the gate query.** For idsse/metrica/skillcorner, build the `int_tracking_goalkeepers`
  logic from **both** `stg_spadl__tracking_context` (TC-1) and `stg_action_context__values` (AC) and diff the
  `(match_key, player_key) → is_goalkeeper` sets. For every DIFFERENCE, resolve the disputed player against
  **roster ground truth** (`dim_players.position_group = 'Goalkeeper'` and/or the provider's own GK
  designation).

Use the `WorkspaceClient().statement_execution` pattern (see any `scripts/*` that runs live SQL). **Use a
query helper that polls to SUCCEEDED and RAISES on error** — a helper returning `data_array or []` masks a
failed query as an empty (clean-looking) result. This exact footgun bit the investigation.

- [ ] **Step 2: Run it and record the numbers in the PR body.**

Expected shape (from the spec's investigation):
- **idsse:** identical (oriented in both, same derivation). Any idsse difference = a genuine derivation
  change → **STOP and investigate.**
- **metrica/skillcorner:** differences are permitted **only** where AC's GK is correct vs ground truth. A
  difference where AC is *wrong* → **STOP.**

- [ ] **Step 3: GO/NO-GO.** If every difference resolves in AC's favour (or there are none), proceed. Otherwise
  STOP and report to the user with the disputed rows. **Do not proceed on "AC is newer so AC is right."**

---

## Task 2: Harden AC's dedup (defense-in-depth, independent of TC-1)

AC bronze has **0 divergent dups today** (measured: 937,324 rows = 937,324 distinct keys), so this changes no
values. It removes the *latent* ingest-order dependency and adds the regression guard that catches a future
M13-ownership break — **at the bronze layer, the only place such a break is visible.**

**Files:**
- Modify: `dbt_project/models/staging/action_context/stg_action_context__values.sql` (~:16-19)
- Create: `dbt_project/tests/assert_action_context_bronze_no_divergent_dups.sql`
- Modify: `dbt_project/models/marts/_marts__models.yml` (add grain test to `fct_action_context`)

- [ ] **Step 1: Add a deterministic tiebreaker to the staging dedup.** Confirm the current block first:

```bash
grep -n "row_number() over" dbt_project/models/staging/action_context/stg_action_context__values.sql
```

Change (symbol: the `row_number()` window in the `deduplicated` CTE):

```sql
        row_number() over (
            partition by match_id, action_id
            order by _ingested_at desc, action_id          -- deterministic tiebreaker (was: _ingested_at only)
        ) as _row_num
```

> `action_id` is already in the partition, so as a tiebreaker it is a constant within each group — it makes the
> sort *stable* without changing which row wins when `_ingested_at` ties (both rows are content-identical today
> anyway). If a stronger guarantee is wanted, use a content hash of the feature columns. Either is inert on
> today's 0-dup data; the point is determinism, not a value change.

- [ ] **Step 2 (TDD): Write the BRONZE zero-dup singular test FIRST.** This is the load-bearing guard (review-2):
  it must count dups in the **source table**, because a mart-grain test is vacuous (staging `row_number()=1`
  makes the grain unique by construction).

`dbt_project/tests/assert_action_context_bronze_no_divergent_dups.sql`:

```sql
-- ADR-030 / M13-ownership guard. bronze.spadl_action_context must have exactly one row per
-- (data_source, match_id, action_id). AC-1's work-unit ownership guarantees this; if it ever
-- breaks, the staging dedup would silently collapse the dups and NO mart-level test would see it.
-- This is the ONLY layer where an ownership regression is visible. See ADR-068 / spec review-2.
{{ config(severity='error') }}

select data_source, match_id, action_id, count(*) as n
from {{ source('action_context', 'spadl_action_context') }}
group by 1, 2, 3
having count(*) > 1
```

- [ ] **Step 3: Run it against live bronze — expect PASS (0 rows) today.**

```bash
uv run --extra sdk dbt test --select assert_action_context_bronze_no_divergent_dups   # or via dbt-live-ci path
```

Expected: PASS. (If it FAILS, bronze already has dups — STOP, that is a live incident, not a plan step.)

- [ ] **Step 4: Add the mart-grain uniqueness test** to `fct_action_context` (it currently lacks one). This
  defends **join integrity**, NOT ownership — it must not be treated as the guard from Step 2.

In `_marts__models.yml`, under `- name: fct_action_context`, add to its `data_tests:`:

```yaml
      - dbt_utils.unique_combination_of_columns:
          combination_of_columns: [match_key, action_id]
```

- [ ] **Step 5: Assert the tiebreaker text is present** (a cheap source-level pin so a future edit can't
  silently drop it). Add to an existing staging test file or a new `src/tests/` check that
  `stg_action_context__values.sql` contains `order by _ingested_at desc, action_id`.

- [ ] **Step 6: Commit checkpoint** (local only — do NOT push).

---

## Task 3: Re-home GK identity onto AC

**Files:**
- Modify: `dbt_project/models/intermediate/int_tracking_goalkeepers.sql` (:20-21)
- Create: `dbt_project/tests/assert_tracking_gk_provider_scope.sql` (pin THE TRAP)

- [ ] **Step 1 (TDD): Write the provider-scope guard FIRST.** THE TRAP: `int_tracking_goalkeepers` has no
  `data_source` filter, so swapping to AC silently admits gradientsports + statsbomb-360 rows. Pin the intended
  scope so the swap can't widen it by accident.

`dbt_project/tests/assert_tracking_gk_provider_scope.sql`:

```sql
-- int_tracking_goalkeepers must cover ONLY the tracking providers that have real tracking frames.
-- After the AC re-home, an omitted data_source filter would admit gradientsports / statsbomb rows.
{{ config(severity='error') }}

select distinct data_source
from {{ ref('int_tracking_goalkeepers') }}
where data_source not in ('idsse', 'metrica', 'skillcorner')
```

- [ ] **Step 2: DEMONSTRATE the guard fails (kill-line discipline — plan review-3).** The trap is only real in
  the window *after* the ref-swap and *before* the filter. A guard that goes green→green never proves it can
  catch anything, so open that window deliberately. Swap the ref **without** the filter first:

```sql
    from {{ ref('stg_action_context__values') }}
    where defending_gk_player_id_native is not null
    -- NO data_source filter yet — this is the trap, deliberately exposed
```

  Then run the guard and confirm it **FAILS** (AC admits gradientsports + statsbomb-360, which the filter will
  later exclude):

```bash
uv run --extra sdk dbt build --select int_tracking_goalkeepers assert_tracking_gk_provider_scope
```

  Expected: `assert_tracking_gk_provider_scope` **FAILS**, returning `gradientsports` and/or `statsbomb`. If it
  passes here, the guard is vacuous — STOP and fix it before proceeding.

- [ ] **Step 3: Add the filter — the guard must now go green.** Symbol: the `where` clause just edited:

```sql
    from {{ ref('stg_action_context__values') }}
    where defending_gk_player_id_native is not null
      and data_source in ('idsse', 'metrica', 'skillcorner')   -- THE TRAP: AC carries GS + statsbomb-360; TC-1 did not
```

- [ ] **Step 4: Verify green + parity holds.** Build the model, its guard, and downstream `fct_tracking_frames`;
  the parity gate in Task 1 already proved the identity set — this confirms the wired-up mart matches.

```bash
uv run --extra sdk dbt build --select int_tracking_goalkeepers fct_tracking_frames assert_tracking_gk_provider_scope
```

Expected: all pass; `assert_tracking_gk_provider_scope` now green **because of** the filter (it was red in
Step 2, green here — that red→green is the proof it guards the trap).

- [ ] **Step 5: Commit checkpoint.**

---

## Task 4: Re-home IDSSE minutes onto AC

**Files:**
- Modify: `dbt_project/models/intermediate/int_minutes_played_per_match.sql` (`idsse_roster` CTE, ~:162-172)

- [ ] **Step 1: Swap the source in the `idsse_roster` CTE.** It already filters `data_source = 'idsse'`
  (confirmed) so THE TRAP cannot reach it. Confirm location:

```bash
grep -n "idsse_roster\|stg_spadl__tracking_context" dbt_project/models/intermediate/int_minutes_played_per_match.sql
```

Change (symbol: inside `idsse_roster`, the `from {{ ref('stg_spadl__tracking_context') }}`):

```sql
    from {{ ref('stg_action_context__values') }}
    where data_source = 'idsse'
```

- [ ] **Step 2: Verify `fct_goalkeeper_stats` (the sole consumer of this leg) is unchanged.**

```bash
uv run --extra sdk dbt build --select int_minutes_played_per_match fct_goalkeeper_stats
```

Expected: pass. (Context: IDSSE minutes never reach `fct_player_stats` — `int_minutes_played.sql` NULLs DFL
string IDs via `try_cast(... as bigint)` and drops them — so this leg feeds `fct_goalkeeper_stats` only.)

- [ ] **Step 3: Commit checkpoint.**

---

## Task 5: Re-home / retire the 3 TC-1 assertion tests

**Files:**
- Modify: `dbt_project/tests/assert_unresolved_gk_player_ids.sql` (:10)
- Decide+modify: `dbt_project/tests/assert_idsse_minutes_roster_vs_tracking_context.sql` (:23)
- Verify only: `dbt_project/tests/assert_idsse_gk_parity.sql` (no direct TC-1 ref)

- [ ] **Step 1: `assert_unresolved_gk_player_ids.sql` — swap the ref.** Symbol:
  `from {{ ref('stg_spadl__tracking_context') }} tc` →
  `from {{ ref('stg_action_context__values') }} tc`.

- [ ] **Step 2: `assert_idsse_minutes_roster_vs_tracking_context.sql` — this test becomes CIRCULAR.** It asserts
  the minutes roster (now AC-derived, Task 4) against the tracking-context roster (TC-1). After the re-home both
  sides come from AC → the test compares AC to AC and is vacuous. **Two options — recommend to the user:**
  - **(a) Delete it** — its ground truth (TC-1) is being deleted; the guard it provided is now subsumed by the
    single AC source.
  - **(b) Re-base it** to assert the roster against a *genuine* ground truth (`bronze.idsse_tracking` distinct
    players per match), which is what it was really trying to approximate.
  Default recommendation: **(b)** — a roster guard against real tracking bronze is more valuable than deletion.
  Rename the file accordingly (`assert_idsse_minutes_roster_vs_bronze_tracking.sql`).

  > **The re-base MUST be a CONTAINMENT check, not equality (plan review-3).** The minutes roster is
  > *action-generating* players; `bronze.idsse_tracking` distinct players is *all tracked* players — a
  > **superset**. An equality assert would false-positive on every match where a tracked player generated no
  > SPADL action (a substitute who never touched the ball, etc.). Assert **minutes-roster ⊆ tracking-players**:
  > the failing (anti-join) shape is "a minutes-roster player NOT present in tracking bronze for that match",
  > which would be a genuine identity bug. Do NOT assert the reverse direction.

- [ ] **Step 3: `assert_idsse_gk_parity.sql` — no swap.** It references `int_tracking_goalkeepers` (re-homed in
  Task 3), not TC-1 directly. Just re-run it and confirm still green.

- [ ] **Step 4: Build all three.**

```bash
uv run --extra sdk dbt build --select assert_unresolved_gk_player_ids assert_idsse_gk_parity assert_idsse_minutes_roster_vs_bronze_tracking
```

- [ ] **Step 5: Commit checkpoint.**

---

## Task 6: Delete the TC-1 gold + staging layer

**Every re-home is now live. Nothing reads `stg_spadl__tracking_context` / `fct_tracking_context` any more —
confirm that before deleting:**

```bash
grep -rn "stg_spadl__tracking_context\|fct_tracking_context" dbt_project/ src/ scripts/ hf_taipy_app/ | grep -v "test_staging_coverage\|oracle\|extract_action_context_fixture\|_sources.yml\|_models.yml\|\.md:"
```

Expected: only the teardown targets below remain. If a *new* consumer appears, STOP.

**Files (delete):**
- `dbt_project/models/marts/fct_tracking_context.sql`
- `dbt_project/models/staging/tracking_context/stg_spadl__tracking_context.sql`
- `dbt_project/models/staging/tracking_context/_tracking_context__models.yml` + `_tracking_context__sources.yml`
- The `- name: fct_tracking_context` block in `_marts__models.yml` (grep for it; was `:4849-5048`)

**Files (modify):**
- `src/ingestion/refresh_synced_tables.py` — delete the `fct_tracking_context_synced` `SyncedTableConfig` (was
  `:235`). **It is SNAPSHOT (no 4th arg), so it is NOT in `triggered_synced_marts` — no `dbt_project.yml` edit
  needed here.**
- `scripts/create_indexes.py` — delete the tracking_context index block (was `:224-230`).
- `src/tests/test_staging_coverage.py` — delete the `"tracking_context": [...]` dict entry (was `:87-89`).

- [ ] **Step 1: Delete the gold + staging files.**
- [ ] **Step 2: Delete the `_marts__models.yml` block** (grep `name: fct_tracking_context`).
- [ ] **Step 3: Remove the synced config + PG indexes + staging-coverage entry.**
- [ ] **Step 4: Verify dbt still parses and `test_strand_safe_rederive` still passes** (TC-1 was SNAPSHOT, so
  this should be unaffected — confirm).

```bash
uv run --extra sdk dbt parse
uv run pytest src/tests/test_strand_safe_rederive.py src/tests/test_staging_coverage.py -q
```

- [ ] **Step 5: Commit checkpoint.**

---

## Task 7: Delete the TC-1 producer (tasks, entry points, module, cards)

**Files (delete):**
- `src/ingestion/tracking_context.py`
- `workflow-cards/wf-tracking-context.yaml` (whole file)

**Files (modify):**
- `pyproject.toml` — delete `compute_tracking_context` + `preflight_tracking_context` from `[project.scripts]`
  (was `:168-169`).
- `terraform/modules/workflows/main.tf` — delete both task blocks (`compute_tracking_context` was `:725-758`,
  `preflight_tracking_context` was `:1360-1395`) **and remove the `depends_on { task_key =
  "compute_tracking_context" }` edge in `dbt_build_output_marts`** (was `:914`).
- `dbt_project/seeds/task_workflow_mapping.csv` — delete the two `*_tracking_context,wf-tracking-context` rows
  (was `:16-17`).
- `workflow-cards/wf-dbt-build-output-marts.yaml` — delete the `fct_tracking_context` output block (was
  `:175-177`).
- `src/tests/test_workflows_tf_ordering.py` — decrement the task-count anchor **42 → 40** (was `:281`) and add
  a dated history comment in the existing style.

- [ ] **Step 1: Delete the module + card.**
- [ ] **Step 2: Remove the two pyproject entry points.**
- [ ] **Step 3: Delete the two TF task blocks + the `dbt_build_output_marts` dependency edge.** After editing,
  `terraform fmt`:

```bash
terraform -chdir=terraform/modules/workflows fmt
terraform -chdir=terraform/environments/dev fmt
```

- [ ] **Step 4: Remove the seed rows + the output-marts card block.**
- [ ] **Step 5: Decrement the count anchor 42 → 40 with a history comment.** First **confirm-then-decrement**
  (plan review-3): verify the current anchor really is 42 and that *both* deleted task_keys were counted in it,
  so the new number is derived, not guessed:

```bash
grep -n "len(task_keys) ==" src/tests/test_workflows_tf_ordering.py     # confirm current anchor == 42
git show HEAD:terraform/modules/workflows/main.tf | grep -c 'task_key.*=.*"compute_tracking_context"\|task_key.*=.*"preflight_tracking_context"'   # both top-level keys present pre-delete
```

  Expected: anchor 42; grep shows the two top-level task keys existed. Then set the anchor to **40**. (The test
  would catch a wrong number, but confirm-first saves a CI cycle.)
- [ ] **Step 6: Verify the parity + ordering tests.**

```bash
uv run pytest src/tests/test_workflows_tf_ordering.py src/tests/test_card_parity_with_terraform.py src/tests/test_pipeline_row_count.py -q
```

Expected: green. `test_card_parity_with_terraform` stays green **only if task + card + entry-point are all
deleted together** — if it fails, one of the three was missed.

- [ ] **Step 7: Commit checkpoint.**

---

## Task 8: Freeze the AC-1 cross-pipeline oracle (do NOT re-source it)

**⚠ THIS TASK WAS CORRECTED (plan review-3). The earlier version — "re-point the regen at
`fct_action_context`" — was WRONG and would have shipped a green-but-blind regression suite. Do NOT do that.**

`oracle_fct_tracking_context.parquet` is a **cross-pipeline** oracle. `oracle_map.py:1-12` is explicit: AC-1's
output is validated against the values of the **independent TC-1 pipeline**, and the "geometric features match
tightly / window-dependent features legitimately diverge" structure only means anything *because* the two
pipelines are different implementations. Re-pointing the regen at `fct_action_context` (AC-1's own output) makes
the suite **AC-1 validated against AC-1** — permanently green, catching nothing, and a future AC-1 regression
would be captured into the "oracle" on the next regen and blessed as correct. That is the same silent-vacuity
class as the circular assert (Task 5) and the vacuous mart-grain test (Task 2).

**The correct fix (which the spec §3 "Keep" already implied): freeze the oracle, kill the regen branch, do NOT
re-source.** Losing regenerability is *correct* — a cross-pipeline oracle must never be regenerated from the
pipeline it is supposed to independently check. The frozen parquet remains a valid historical baseline: AC-1's
output today matched the independent TC-1 pipeline, and that snapshot still catches AC-1 drift away from it.

**Files:**
- Modify: `scripts/extract_action_context_fixture.py` (the tracking-context block in `_pull_oracles`, was
  `:398-416`)
- Modify: `src/tests/action_context/oracle_map.py` (docstring, was `:1-27`)

- [ ] **Step 1: Remove (or hard-raise) the dead regen branch.** The TC-1 block in `_pull_oracles` reads a table
  that no longer exists. Replace the query with an explicit failure rather than leaving a branch that would
  silently break or, worse, be "fixed" later by re-sourcing from AC:

```python
    # oracle_fct_tracking_context.parquet is a CROSS-PIPELINE oracle: AC-1 validated against the
    # INDEPENDENT (now-retired) TC-1 pipeline. It is intentionally FROZEN and non-regenerable — a
    # cross-pipeline oracle must never be regenerated from the pipeline it checks (that would make
    # the golden suite AC-1-vs-AC-1, permanently green and blind). See oracle_map.py + spec §3.
    raise RuntimeError(
        "The tracking_context oracle is frozen (TC-1 retired, PR-1). It cannot be regenerated. "
        "If AC-1 legitimately changed, re-baseline manually and record why — do NOT re-source from AC."
    )
```

  (If the surrounding structure makes a `raise` awkward, delete the whole `if provider in {...}:` tracking-context
  block instead — the effect is the same: the parquet is never regenerated from live data.)

- [ ] **Step 2: Update `oracle_map.py`'s docstring** to record: the TC-1 pipeline is **retired** (PR-1); the
  `tracking_context` oracle is now a **frozen historical cross-pipeline snapshot**, non-regenerable by design.
  Keep the rest of its structure (`ORACLE_JOIN`, `OracleSpec` entries, `PROVIDERS_TRACKING_CONTEXT`) — the
  frozen parquet is still consumed by the AC-1 golden suite exactly as before.

- [ ] **Step 3: Keep the frozen parquet fixture unchanged.** Confirm the AC-1 golden suite still passes against
  it (nothing about the *comparison* changed — only the regen path was removed):

```bash
uv run pytest src/tests/action_context/ -q -p no:warnings
```

- [ ] **Step 4: Commit checkpoint.**

---

## Task 9: Wheel bump + full verification

**Files:** ~31 files via `bump_wheel.py`.

- [ ] **Step 1: Bump the wheel.** A `src/ingestion/` module was deleted, so the wheel changes.

```bash
# edit pyproject.toml version X.Y.Z -> X.Y.(Z+1)
uv run python scripts/bump_wheel.py
uv run python scripts/bump_wheel.py --check   # expect: All files consistent
```

- [ ] **Step 2: FULL verification on a SETTLED tree** (do NOT edit while pytest runs — `inspect.getsource`
  lockstep sentinels produce phantom failures). Run each gate **bare** (redirecting output can mask exit codes):

```bash
uv run ruff check src/ scripts/ ; echo "RUFF=$?"
uv run ruff format --check src/ scripts/
uv run pyright src/
uv run lint-imports
uv run pytest src/tests/ -q -p no:warnings   # exit 0
terraform -chdir=terraform/environments/dev fmt -check
```

- [ ] **Step 3: STOP at the commit gate.** Commit requires separate explicit user approval.

---

## Task 10: OPERATOR-ONLY post-merge steps (do NOT auto-run)

These are destructive / external and belong to the operator **after** merge + wheel deploy.

- [ ] **Bronze table DROP** (destructive — operator-driven per CLAUDE.md):

```sql
DROP TABLE IF EXISTS soccer_analytics.bronze.spadl_tracking_context;
```

- [ ] **Lakebase synced table teardown** — drop `fct_tracking_context_synced` from Postgres (the daily
  `lakebase-grants.yml` will not recreate it once the config is gone, but the existing synced table must be
  dropped manually).
- [ ] **HF dataset decision** (owner) — `luxury-lakehouse/spadl-tracking-context`: retire / freeze / replace
  with an AC-derived publish. Whichever, remove `scripts/publish_tracking_context_hf.py` and its HF-card entry
  in the same PR only if the decision is "retire".

---

## Self-review checklist (run before declaring the plan done)

- [ ] Every re-home (Tasks 3–5) lands **before** any deletion (Tasks 6–7).
- [ ] The parity gate (Task 1) ran and passed on live data **before** any change.
- [ ] The bronze zero-dup guard is on the **source table**, not the mart (review-2).
- [ ] Task 3's trap guard was shown **red → green** (fails before the filter, passes after) — not green→green.
- [ ] The oracle is **frozen, not re-sourced** (Task 8) — the regen branch raises/deleted; it is NOT re-pointed
      at `fct_action_context` (that would make the golden suite AC-1-vs-AC-1, blind). Review-3 blocker.
- [ ] Task 5's minutes re-base (if option b) is a **containment** check (roster ⊆ tracking-players), not equality.
- [ ] `grep -rn "tracking_context"` across `dbt_project/ src/ scripts/ terraform/ workflow-cards/` returns only
      intended survivors (the AC-1 oracle infra) after Task 8.
- [ ] Count anchor **confirmed 42** then decremented to 40; card/task/entry-point deleted as a triple.
- [ ] Wheel bumped + `--check` clean.
