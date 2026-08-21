# silly-kicks 4.87.0 Full Adoption + Live Recompute — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Adopt silly-kicks 4.43.0 → 4.87.0 across the lakehouse, materialize the in-scope new columns into the gold marts with contracts, and recalculate the tracking + downstream-model surface on live data.

**Architecture:** One big-bang code PR (Part A + Part A2) covering the version lockstep, breaking call-site adaptation, ALL new-metric materialization (wide-by-grain, incl. xtgk v2 replacing v1 as a trainer+writer), golden rebaselines + independent correctness checks, and governance — validated by the 7 CI gates. Then a driven live runbook (Part B): AC recompute → VAEP/ScoutGPT/xG-v3 retrains + xtgk-v2 fit + the Rev-6 mart writers → DAG-complete mart rebuild → HF republish → synced refresh, each gated on explicit approval. Design source: `docs/superpowers/specs/2026-08-18-silly-kicks-4-85-full-adoption-design.md` (**Rev 6**).

**Tech Stack:** Python 3.10, silly-kicks 4.87.0, PySpark/Delta (Databricks serverless), dbt, uv, Terraform, HF Jobs, ruff/pyright/pytest.

## Global Constraints

- **silly-kicks pin = `4.87.0`** everywhere in the lockstep set (pyproject floor `>=4.87.0,<5`; TF env `==4.87.0`; `uv.lock`; all `_REQUIRED_SK_MIN=(4,87,0)`; `submit_ac1_oneshot.py`). Verified released: PyPI latest + tag `v4.87.0`; the off-ball fix is in the tagged source (`_off_ball_runs.py:313`).
- **Never commit without explicit user approval.** No commit/push/PR/merge steps appear in this plan. The user commits once, at the end, on their own approval. This is CLAUDE.md's non-negotiable git rule.
- **No wheel-consuming job runs until post-merge `python-ci.yml` is green** (Part B is entirely post-merge).
- **Live acceptance gate = per-column expected-null-rate bounds, NOT blanket 0-NULL** (many new columns are legitimately nullable/event-conditional).
- **Rebaselined goldens are a regression guard, not a correctness validator** — correctness comes from the independent invariant checks + the expected-shift oracle + the pre-wipe shadow diff.
- **Mart rebuild is DAG-complete** (`dbt ls --select <root>+`), never a hand-picked list.
- **The 7 CI checks must pass locally before the PR is considered done:** `ruff check src/ scripts/`, `ruff format --check src/ scripts/`, `lint-imports`, `python scripts/bump_wheel.py --check`, `python scripts/pip_audit_ignores.py --check`, `pytest src/tests/`, `pyright src/ hf_taipy_app/src/ scripts/_tf_env_pins.py scripts/sync_tf_env_pins.py`.
- **Canonical SPADL 105×68**; medallion layer schemas are named constants (`DEFAULT_BRONZE/SILVER/GOLD_SCHEMA`).
- **Delta re-validation is CLOSED (target 4.87.0):** the only changes past the mined-4.85 tables are 4.86.0 StatsBomb `cross_blocked` NA→real (Task 7), the 4.86.1 off-ball crash-fix (Task 0), and 4.87.0 (a reported-not-gated research-validation cycle — NIL library/API/column/behaviour change). **No new retrain trigger *from the version delta*.** The xtgk-v2 fit (Task 17b/22c) is a **scope decision** (v2-replaces-v1), not a delta-driven retrain — distinct from the VAEP/ScoutGPT/xG-v3 retrains, which the delta *does* drive.

---

# Part A — Code PR (local + CI)

> **AC test harness (use this, do not invent one).** AC-output tests recompute a real work unit via the
> `_recompute()` helper in `src/tests/action_context/test_mini_golden.py`:
> `run_work_unit(WorkUnit(provider="idsse", match_id="J03WMXmini", period=1), frames=ParquetFrameSource(_ROOT), actions=ParquetActionsSource(_ROOT), xt=ParquetXtSource(_ROOT), meta=ParquetMatchMetadataSource(_ROOT), sink=<collector>, is_slice=True)`
> where `_ROOT = "src/tests/fixtures/action_context"`. The output DataFrame is captured by a sink with a
> `write(wu, result_df)` method. There is **no** `load_fixture_work_unit`/`enrich_batch(...)` call — new AC
> tests below add functions to `test_mini_golden.py` (reusing `_recompute()`) or replicate that harness.
> **Do not assert new-column correctness against the rebaselined golden** (Task 12 regenerates it from the new
> code, so it would be blind to a bug baked in at the same time) — assert independently (`notna().any()`,
> ranges, source structure).

> **Big-bang red window (review-1 H1 — read before running any intermediate suite).** Task 1 installs
> **4.87.0 while the call sites are still un-adapted** (16× `home_team_id`). At 4.87.0 the 14 self-resolving
> aggregators no longer accept `home_team_id`, so **any `_recompute()`-based test ERRORS (`TypeError`) from
> the end of Task 1 until the direction re-key (Task 3) completes**, and new-column tests stay red until
> Task 9 registers the names. This is inherent to a big-bang bump — there is no green intermediate for
> silly-kicks-calling code between old-lib and fully-adapted. So:
> - Per-task "run to green" for a `_recompute()`-based test means green **once the tree is coherent again** —
>   after **Task 3** for existing-column/behavior tests (Tasks 4, 5). Anything that observes a **new** column
>   through `_recompute()` needs **Task 9** too: `_recompute()` output is filtered to `RESULT_COLUMNS` (the
>   existing mini-golden asserts exactly that), so `build_output` drops any new column until Task 9 registers
>   it. That means the `obso_epv_source` assertion (Task 6 Step 4), the presence guard (Task 8 Step 4), and the
>   range/vocab checks (Task 14) all go green **after Task 9**, not after their own task's edit.
> - Source-text guards (regex/AST — Task 3's kwarg guard, Task 15) are green throughout; run them anytime.
> - Do not interpret a red `_recompute()` test inside the window as a regression — re-run it after Task 3/9.

### Task 0: Prerequisite verification (silly-kicks 4.87.0)

**Files:** none (verification only).

- [ ] **Step 1: Confirm 4.87.0 is installable and carries the fix**

Run:
```bash
python -c "import urllib.request,json; d=json.load(urllib.request.urlopen('https://pypi.org/pypi/silly-kicks/json')); print('latest', d['info']['version']); assert '4.87.0' in d['releases']"
cd /d/Development/karstenskyt__silly-kicks && git fetch --tags && git show v4.87.0:silly_kicks/tracking/_off_ball_runs.py | grep -n "except GoalEndUnresolvedError"
```
Expected: latest `4.87.0`; the grep prints the `except GoalEndUnresolvedError:` line inside `_line_break_kernel` (~line 313). If either fails, STOP — the prerequisite is not satisfied.

- [ ] **Step 2: xtgk-v2 bundling check (review-2 blocking — decides the biggest workstream before the big-bang).**

Run:
```bash
cd /d/Development/karstenskyt__silly-kicks && git ls-tree -r --name-only v4.87.0 -- silly_kicks/xtgk/ | grep -viE '\.py$'
```
Expected (VERIFIED 2026-08-19): only `_retention_weights/{default,skillcorner}/*` — i.e. **only `retention` is bundled**; `possession_value` (`MarkovPossessionValue`, `.fit()`/`.load()` only) and `turnover_cost` (`.fit()` only) are **NOT bundled**. This CONFIRMS xtgk-v2 is a **fit-on-corpus training sub-project** (Task 17b is a trainer + writer, not inline wiring; §11.2 has its retrain row). If — contrary to the verified result — a bundled possession/turnover surface appears, xtgk-v2 collapses back to wiring; otherwise the trainer path in Task 17b is mandatory.

---

### Task 1: Version lockstep to 4.87.0

**Files:**
- Modify: `pyproject.toml:71`, `uv.lock`, `terraform/modules/workflows/main.tf:1567`, `src/ingestion/exec_visibility.py:450`, `scripts/train_vaep_model_hf.py:73`, `scripts/train_football2vec.py:82`, `scripts/train_football2vec_360.py:76`, `scripts/train_football2vec_v2.py:78`, `scripts/train_scoutgpt_hf.py:82`, `scripts/train_xg_v3_hf.py:145`, `scripts/submit_ac1_oneshot.py:55`, `src/tests/test_sk3_mig_b_orchestrator_invariants.py:363`, `src/shared/wheel.py` (+ ~30 consumers via tool), `scripts/sk3_mig_b_retrain.py` (mark dead)
- Test: `src/tests/test_terraform_env_dep_parity.py`, `src/tests/test_executor_env_guard.py`, `src/tests/test_sk3_mig_b_orchestrator_invariants.py`

**Interfaces:**
- Produces: `silly-kicks==4.87.0` resolvable across the env; `_REQUIRED_SK_MIN=(4,87,0)` in `exec_visibility` and all 6 trainers.

- [ ] **Step 1: Bump the pyproject floor.** In `pyproject.toml:71` change `"silly-kicks[das,ghost-gk,parse-dfl]>=4.43.0,<5"` → `">=4.87.0,<5"`.

- [ ] **Step 2: Re-resolve the lock.** Run: `uv lock --refresh-package silly-kicks && uv sync --inexact`. Expected: `uv.lock` now resolves `silly-kicks 4.87.0`.

- [ ] **Step 3: Mirror the TF env pin from the lock (never hand-edit).** Run: `uv run python scripts/sync_tf_env_pins.py`. Expected: `main.tf:1567` becomes `"silly-kicks[das,ghost-gk,parse-dfl]==4.87.0"`; the CI dbt pins + `uvx --from` lines update too.

- [ ] **Step 4: Bump the guard + trainer sentinels.** Set `_REQUIRED_SK_MIN = (4, 86, 1)` in `exec_visibility.py:450` and in all 6 trainer scripts (the five sentinel-enforced ones + `train_xg_v3_hf.py:145`, which is **not** sentinel-enforced — do not skip it). Update the expected tuple in `test_sk3_mig_b_orchestrator_invariants.py:363` to `(4, 86, 1)`.

- [ ] **Step 5: Realign `submit_ac1_oneshot.py`.** Change its silly-kicks pin (`:55`) to `"silly-kicks[das,ghost-gk,parse-dfl]==4.87.0"` and fix its `numba` pin (`:59`) to match Terraform's `numba==0.66.0`.

- [ ] **Step 6: Mark `sk3_mig_b_retrain.py` dead.** Add a module-docstring note that it is a retired one-time orchestrator excluded from the silly-kicks lockstep (its internal floor 4.26 / wheel gate 0.3.34 are stale by design). Do not update its pins. Confirm it is not in `test_sk3_mig_b_orchestrator_invariants.py`'s `_TRAINER_PATHS`.

- [ ] **Step 7: Bump the wheel version across consumers.** Run: `uv run python scripts/bump_wheel.py` (bumps `src/shared/wheel.py` + ~30 consumers). Do NOT build/publish the wheel here — the build happens post-Phase-2 (Task 18).

- [ ] **Step 8: Verify `_SK_GUARD_SUBMODULES` private paths still exist at 4.87.0.** Run: `python -c "import importlib; [importlib.import_module(m) for m in ('silly_kicks.tracking._ghost_gk','silly_kicks.tracking._xt_gk','silly_kicks.tracking._gk_completion','silly_kicks.tracking._gk_geometry')]"`. Expected: no ImportError. If any moved, update `exec_visibility.py:466`.

- [ ] **Step 9: Run the lockstep guardrail tests.** Run: `uv run pytest src/tests/test_terraform_env_dep_parity.py src/tests/test_executor_env_guard.py src/tests/test_sk3_mig_b_orchestrator_invariants.py -v` and `uv run python scripts/bump_wheel.py --check`. Expected: all pass.

---

### Task 2: Repoint the `id_compat` import (4.53 breaking move)

**Files:**
- Modify: `src/tests/action_context/test_frame_orientation_golden.py` (+ any other hit)
- Test: the file collects.

- [ ] **Step 1: Find every consumer of the old private path.** Run: `rg -n "tracking\._id_compat" src/ scripts/`. Expected: at least `test_frame_orientation_golden.py`.

- [ ] **Step 2: Repoint each hit** from `from silly_kicks.tracking._id_compat import ids_match` to `from silly_kicks.id_compat import ids_match` (public path, 4.53+).

- [ ] **Step 3: Verify collection.** Run: `uv run pytest src/tests/action_context/test_frame_orientation_golden.py --collect-only -q`. Expected: collects without `ImportError`.

---

### Task 3: Direction re-key in `enrich_batch` (drop `home_team_id`, keep two)

**Files:**
- Modify: `src/analytics/action_context/enrich.py` (the tracking chain ~275–528 and the SB360 chain ~556–647)
- Test: add two functions to `src/tests/action_context/test_mini_golden.py` (reuses its `_recompute()` helper)

**Interfaces:**
- Consumes: silly-kicks 4.87.0 aggregator signatures (14 self-resolve direction; `add_ghost_gk` + `add_xcross_attempt` still take `home_team_id`).
- Produces: work-unit output unchanged in column set; ghost-GK columns still populated; the two KEEP kwargs preserved.

- [ ] **Step 1: Write the regression guards** (add to `test_mini_golden.py`, reusing `_recompute()`):

```python
def test_ghost_gk_populated_after_direction_rekey() -> None:
    # add_ghost_gk still READS home_team_id (ADR-055 exception). A wrongly-dropped kwarg NaNs it —
    # and the rebaselined golden (Task 12) would be blind to that. Independent, non-vacuous guard:
    result = _recompute()
    assert result["ghost_gk_x"].notna().any(), \
        "ghost_gk_* all-NaN — home_team_id wrongly dropped from add_ghost_gk"

def test_keep_kwargs_present_in_enrich_source() -> None:
    # add_xcross_attempt's home_team_id feeds the score_differential FEATURE (silent-NaN on drop),
    # not a persisted column — guard it structurally so the kwarg cannot be silently removed.
    import re
    src = open("src/analytics/action_context/enrich.py", encoding="utf-8").read()
    for fn in ("add_ghost_gk", "add_xcross_attempt"):
        call = re.search(fn + r"\((.*?)\n    \)", src, re.S)
        assert call and "home_team_id=home_team_id" in call.group(1), \
            f"{fn} must KEEP home_team_id=home_team_id (silent-failure case)"
```

(`ghost_gk_x` is populated on the mini slice — the existing `test_mini_golden` asserts `golden["ghost_gk_x"].notna().all()`. The regex tolerates the multi-line call formatting in `enrich.py`; adjust the closing-paren pattern if the call indentation differs.)

- [ ] **Step 2: Run the source-text guard now (green); the `_recompute()` guard is deferred to Step 6.** Run: `uv run pytest src/tests/action_context/test_mini_golden.py::test_keep_kwargs_present_in_enrich_source -v`. Expected: PASS (reads text, not silly-kicks). The `test_ghost_gk_populated_after_direction_rekey` guard **cannot** pass here — the venv is already 4.87.0 (Task 1) but the calls are un-re-keyed, so `_recompute()` raises `TypeError`. It goes green at Step 6. (See the big-bang red-window note above.)

- [ ] **Step 3: Build the shared goal map once.** At the top of `enrich_batch`'s tracking chain (after `tracking_df` is available, before Step 7), add:
```python
from silly_kicks.tracking import resolve_defended_goals
goal_map = resolve_defended_goals(tracking_df)
```

- [ ] **Step 4: Drop `home_team_id` from the 14 self-resolvers; pass `goal_map=` to the 5 that accept it.** Edit each call per this table (tracking chain):
  - `add_defensive_line` (328): remove `home_team_id=…`, add `goal_map=goal_map`.
  - `add_off_ball_context` (331): remove `home_team_id=…`, add `goal_map=goal_map`.
  - `add_line_break` (334): remove `home_team_id=…` (keep `method="ward"`).
  - `add_team_shape` (337): remove `home_team_id=…`.
  - `add_gk_influence` (387–396): remove `home_team_id=…`, add `goal_map=goal_map`.
  - `add_cover_shadows` (402–404): remove `home_team_id=…`, add `goal_map=goal_map`.
  - `add_shape_graph` (408): remove `home_team_id=…`.
  - `add_obso` (411–418): remove `home_team_id=…` (Task 8 adds `xt=`).
  - `add_pausa` (421): remove `home_team_id=…` (Task 8 adds `xt=`).
  - `add_space_creation` (424): remove `home_team_id=…` (Task 8 adds `xt=`).
  - `add_structural_pass` (447): remove `home_team_id=…`.
  - `add_player_influence` (450–458): remove `home_team_id=…`.
  - `add_xt_gk` (478): remove `home_team_id=…`.
  - `add_xshot_occurrence` (435–437): remove `home_team_id=…`.
  **KEEP** `home_team_id=home_team_id` on `add_ghost_gk` (374–382) and `add_xcross_attempt` (462–470).

- [ ] **Step 5: Apply the same drop/keep to the SB360 chain.** Run: `rg -n "home_team_id=" src/analytics/action_context/enrich.py` and resolve every remaining hit by the Step-4 rules (SB360 AC is held/empty, so this is code-correctness only).

- [ ] **Step 6: Run the KEEP-cases guards + the AC unit suite.** Run: `uv run pytest src/tests/action_context/test_mini_golden.py::test_ghost_gk_populated_after_direction_rekey src/tests/action_context/test_mini_golden.py::test_keep_kwargs_present_in_enrich_source src/tests/action_context/test_enrich_helpers.py src/tests/action_context/test_pipeline_dispatch.py -v`. Expected: PASS (no `TypeError` from a stale `home_team_id`; both guards hold). The mini-golden value comparison itself is rebaselined in Task 12 — do not rely on it here.

---

### Task 4: SB360 velocity-availability declaration (ADR-063)

**Files:**
- Modify: `src/analytics/action_context/sb360_snapshots.py` (only if a hand-rolled frame path bypasses the converter)
- Test: Create `src/tests/action_context/test_sb360_velocity_declared.py`

- [ ] **Step 1: Write the failing test** that SB360 snapshot frames declare velocity-unavailable:
```python
# src/tests/action_context/test_sb360_velocity_declared.py
from silly_kicks.tracking import SPEED_SOURCE_UNAVAILABLE
from analytics.action_context.sb360_snapshots import build_sb360_snapshots  # existing entry

def test_sb360_frames_declare_velocity_unavailable(sb360_fixture):
    frames = build_sb360_snapshots(sb360_fixture)  # adapt to actual signature
    assert (frames["speed_source"] == SPEED_SOURCE_UNAVAILABLE).all(), \
        "SB360 frames must declare velocity-unavailable or the 4.87.0 pitch-control aggregators raise"
```
(Grep `build_sb360_snapshots` for the real signature/fixture; use the existing SB360 fixture under `src/tests/fixtures/action_context/statsbomb/`.)

- [ ] **Step 2: Run it.** Run: `uv run pytest src/tests/action_context/test_sb360_velocity_declared.py -v`. If it FAILS (no `speed_source` column), proceed to Step 3; if it PASSES (the converter already stamps it), record that and skip Step 3.

- [ ] **Step 3: Stamp the marker** if needed — in `sb360_snapshots.py`, after building the snapshot frames, add `frames["speed_source"] = SPEED_SOURCE_UNAVAILABLE` (import from `silly_kicks.tracking`).

- [ ] **Step 4: Run to green.** Run: `uv run pytest src/tests/action_context/test_sb360_velocity_declared.py -v`. Expected: PASS.

---

### Task 5: Nullable-dtype + ghost-GK-removal audit

**Files:**
- Modify: any lakehouse code pinning tracking id dtype or `.astype(bool)` on silly-kicks output; remove `ghost_gk_density_spread` references
- Test: existing suite (`pytest src/tests/`)

- [ ] **Step 1: Find dtype-fragile spots.** Run: `rg -n "astype\(bool\)|astype\('bool'\)|int64.*team_id|team_id.*int64" src/`. For each, confirm it does not depend on a non-nullable int or the `pd.Series(['False']).astype(bool) is True` trap; fix any that do (compare with `id_compat` helpers or an explicit `== True`).

- [ ] **Step 2: Remove `ghost_gk_density_spread`.** Run: `rg -n "ghost_gk_density_spread" src/ dbt_project/`. Remove any schema/mart/contract reference (the column no longer exists at 4.87.0; ghost_gk_xfns is 6-col now).

- [ ] **Step 3: Verify no cached 1.2.0 ghost-GK artifact.** Note for Part B: confirm `/Volumes/soccer_analytics/dev_gold/model_weights/` holds no ghost-GK `1.2.0` artifact the loader could prefer (the 1.3.0 bundled variant ships in the wheel). Record as a Part-B pre-check.

- [ ] **Step 4: Run the AC unit suite.** Run: `uv run pytest src/tests/action_context/ -q`. Expected: PASS (or only the goldens fail, which Task 12 rebaselines).

---

### Task 6: Mandatory real-xT OBSO (`xt=`) + `obso_epv_source`

**Files:**
- Modify: `src/analytics/action_context/enrich.py` (the `add_obso`/`add_pausa`/`add_space_creation` calls at 411/421/424 and the SB360 chain 640–641)
- Test: add a function to `src/tests/action_context/test_mini_golden.py` (reuses `_recompute()`)

**Interfaces:**
- Produces: `obso_epv_source` column with value `"xt"` on the tracking path.

- [ ] **Step 1: Write the failing test** (add to `test_mini_golden.py`):
```python
def test_obso_epv_source_is_real_xt() -> None:
    # 4.87.0 fires a non-fatal SyntheticEPVWarning + falls back to synthetic EPV if xt= is omitted
    # (not escalated to an error); the value check below is what confirms real-xT provenance.
    result = _recompute()
    src = result["obso_epv_source"]
    assert (src[src.notna()] == "xt").all(), "obso_epv_source must be 'xt' on the tracking path"
```

- [ ] **Step 2: Run it — expect FAIL** (`obso_epv_source` missing or value `"synthetic"` — the warning is not escalated, so `_recompute()` does not raise). Run: `uv run pytest src/tests/action_context/test_mini_golden.py::test_obso_epv_source_is_real_xt -v`.

- [ ] **Step 3: Add `xt=xt`** to the three tracking-chain calls (`add_obso` 411, `add_pausa` 421, `add_space_creation` 424) and the two SB360-chain calls (640–641). The fitted `xt` is already the enclosing param.

- [ ] **Step 4: Run to green — AFTER Task 9.** `obso_epv_source` is a new column, so it survives into the `_recompute()` output only once Task 9 registers it in `RESULT_COLUMNS` (before that this test `KeyError`s, not green). Run after Task 9: `uv run pytest src/tests/action_context/test_mini_golden.py::test_obso_epv_source_is_real_xt -v`. Expected: PASS.

---

### Task 7: SPADL `shot_blocked` / `cross_blocked` (incl. 4.86.0 StatsBomb real mask)

**Files:**
- Modify: `src/ingestion/spadl_vaep.py` (`_SPADL_SCHEMA` `:55`, `_VAEP_SCHEMA` `:126`), the per-provider applyInPandas StructTypes in `src/ingestion/spadl_conversion.py` (the mirror blocks flagged in its comments)
- Test: `src/tests/test_spadl_vaep_writer_parity.py`

**Interfaces:**
- Produces: bronze `spadl_actions` + `vaep_action_values` carry `shot_blocked`, `cross_blocked` (`BOOLEAN`, nullable).

- [ ] **Step 1: Add the columns to the two DDL constants.** In `_SPADL_SCHEMA` and `_VAEP_SCHEMA`, add `shot_blocked BOOLEAN` and `cross_blocked BOOLEAN` (nullable) in the position matching the SPADL column order. Update each per-provider applyInPandas `StructType` to mirror (grep the `Must mirror _spadl_cols + _SPADL_SCHEMA` comment blocks in `spadl_conversion.py`).

- [ ] **Step 2: Run the parity gate.** Run: `uv run pytest src/tests/test_spadl_vaep_writer_parity.py -v`. Expected: PASS (schema ↔ StructType ↔ DDL parity).

- [ ] **Step 3: Record the 4.86.0 value change for the oracle.** Note (feeds Task 13/Part B): StatsBomb `cross_blocked` moves from all-`pd.NA` (its 4.56 state) to a real open-play-cross mask — non-null rate goes 0% → ~base-rate on open-play crosses. No retrain.

---

### Task 8: New AC aggregator calls (run-values, press-commitment, packing) + free-ride provenance

**Files:**
- Modify: `src/analytics/action_context/enrich.py` (tracking chain)
- Test: add a function + column-list constant to `src/tests/action_context/test_mini_golden.py`

**Interfaces:**
- Produces (exact names, for Task 9's schema): `run_value_target` (float), `run_value_disruptive_sum` (float), `run_value_enabled_pass` (float), `n_disruptive_runs` (Int64), `n_valued_disruptive_runs` (Int64); `press_commitment` (float), `press_commitment_closing_speed` (float), `press_commitment_source` (str); `packing_made` (Int64), `packing_goal_threat` (Int64), `packing_net` (float), `packing_receiver_player_id` (id-passthrough), `packing_secured` (boolean); plus the free-ride `das_source` (str), `ghost_gk_source` (str), `max_single_defender_player_id` (id-passthrough).

- [ ] **Step 1: Write the failing presence guard** (add to `test_mini_golden.py`):
```python
_NEW_SK4861_COLS = [
    "run_value_target", "run_value_disruptive_sum", "run_value_enabled_pass",
    "n_disruptive_runs", "n_valued_disruptive_runs",
    "press_commitment", "press_commitment_closing_speed", "press_commitment_source",
    "packing_made", "packing_goal_threat", "packing_net", "packing_receiver_player_id", "packing_secured",
    "das_source", "ghost_gk_source", "max_single_defender_player_id",
]  # fmt: skip

def test_new_sk4861_columns_present() -> None:
    # build_output wiring guard (PRESENCE). Population is event-conditional — many are legitimately
    # NaN on the 3-action open-play mini slice — so presence, not notna, is the correct emit-drift
    # guard here (same shape as test_xt_gk_fields_present_and_scope_contract).
    result = _recompute()
    missing = [c for c in _NEW_SK4861_COLS if c not in result.columns]
    assert not missing, f"missing new columns from work-unit output: {missing}"
```

- [ ] **Step 2: Run it — expect FAIL** (missing columns). Run: `uv run pytest src/tests/action_context/test_mini_golden.py::test_new_sk4861_columns_present -v`.

- [ ] **Step 3: Add the three new aggregator calls** to `enrich_batch` (tracking chain), importing from `silly_kicks.tracking`. **(review-2 L-A dual-definition trap):** `add_off_ball_run_values`/`add_press_commitment`/`add_packing` are defined in BOTH `silly_kicks/atomic/tracking/features.py` and `silly_kicks/tracking/features.py`; `silly_kicks.tracking.__init__` re-exports the **tracking** version, so `from silly_kicks.tracking import …` is correct — but pin the call signatures to `silly_kicks/tracking/features.py` (e.g. `add_off_ball_run_values` at `:2052`), not the atomic-SPADL twin, so an atomic-signature copy-paste can't creep in.
  - `out = add_off_ball_run_values(out, tracking_df, xt, links=links, pitch_control_cache=pc_cache)`
  - `out = add_press_commitment(out, tracking_df, links=links)`
  - `out = add_packing(out, tracking_df, links=links)`
  (`das_source`/`ghost_gk_source` ride the existing `add_das`/`add_ghost_gk` calls; `max_single_defender_player_id` rides the existing `add_cover_shadows(detailed=True)` — no new call needed for those three.) Note: if `build_output`/`RESULT_COLUMNS` gates emit before Task 9, this may still show columns missing until Task 9 registers them — run Step 4 after Task 9 if so.

- [ ] **Step 4: Run to green — AFTER Task 9.** These are new columns; `build_output` filters `_recompute()` output to `RESULT_COLUMNS`, so the presence guard is red until Task 9 registers all 16 names. Run after Task 9: `uv run pytest src/tests/action_context/test_mini_golden.py::test_new_sk4861_columns_present -v`. Expected: PASS. **(Task 8 is not independently green-able — it pairs with Task 9; treat 8→9 as a unit at review time.)**

---

### Task 9: Register new AC columns in `schema.py` (RESULT_COLUMNS + DDL)

**Files:**
- Modify: `src/analytics/action_context/schema.py` (`RESULT_COLUMNS` `:24`, `ACTION_CONTEXT_DDL` `:243`)
- Test: `src/tests/test_action_context_schema_parity.py`

**Interfaces:**
- Consumes: the 16 new column names from Task 8.
- Produces: `RESULT_COLUMNS` grows 151→167 output columns; DDL parity holds; the applyInPandas StructType (DDL-derived) picks them up automatically.

- [ ] **Step 1: Add all 16 new columns to `RESULT_COLUMNS`** in a stable position (append before `_ingested_at`). Add the matching lines to `ACTION_CONTEXT_DDL` with Spark types: `DOUBLE` for the 4 float columns (`run_value_target`, `run_value_disruptive_sum`, `run_value_enabled_pass`, `press_commitment`, `press_commitment_closing_speed`, `packing_net`), `BIGINT` for the 4 Int64 columns (`n_disruptive_runs`, `n_valued_disruptive_runs`, `packing_made`, `packing_goal_threat`), `STRING` for `press_commitment_source`/`das_source`/`ghost_gk_source`, `BOOLEAN` for `packing_secured`, and — for the two id-passthrough columns (`packing_receiver_player_id`, `max_single_defender_player_id`) — **the same Spark type as the existing actor player-id column** (grep `_player_id` in `ACTION_CONTEXT_DDL` and match it).

- [ ] **Step 2: Run the parity gate.** Run: `uv run pytest src/tests/test_action_context_schema_parity.py -v`. Expected: PASS (`RESULT_COLUMNS` ↔ `ACTION_CONTEXT_DDL` name+order+no-dupes parity).

---

### Task 10: Bronze migration (one idempotent ALTER)

**Files:**
- Create: `scripts/migrations/2026-08-19-add-sk4861-ac-and-spadl-columns.sql`
- Test: local idempotency check

- [ ] **Step 1: Write one idempotent migration** adding the new bronze columns to `soccer_analytics.bronze.spadl_action_context` (the 16 AC columns) and to `soccer_analytics.bronze.spadl_actions` + `vaep_action_values` (`shot_blocked`, `cross_blocked`). Use single-leading-column `ALTER TABLE ... ADD COLUMNS (...)` blocks (the runner makes leading-column ADD idempotent via DESCRIBE skip-if-exists). Match the Spark types from Task 9 / Task 7.

- [ ] **Step 2: Note operator-apply.** This migration is applied WITH the merge in Part B (Task 19), via `uv run --extra sdk python scripts/migrations/_runner.py scripts/migrations/2026-08-19-add-sk4861-ac-and-spadl-columns.sql`. It is not auto-applied by CI. Confirm idempotency by construction (ADD COLUMNS only).

---

### Task 11: dbt staging + mart + contract wiring

**Files:**
- Modify: `dbt_project/models/staging/action_context/stg_action_context__values.sql` (cast list), `dbt_project/models/marts/fct_action_context.sql`, `dbt_project/models/marts/_marts__models.yml` (the `fct_action_context` `columns:` block), and the SPADL staging/mart for `shot_blocked`/`cross_blocked` (`stg_spadl__*`, `fct_action_values` contract)
- Test: `src/tests/test_marts_models_yml_completeness.py` + dbt parse

**Interfaces:**
- Consumes: bronze columns from Task 10; RESULT_COLUMNS from Task 9.
- Produces: the new columns reach `fct_action_context` (+ `fct_action_values` for the SPADL pair) under enforced contracts.

- [ ] **Step 1: Cast the new bronze columns in staging.** In `stg_action_context__values.sql`'s `cleaned` CTE, add an explicit `cast(<col> as <type>) as <col>` for each of the 16 new AC columns (it is not a `select *` past dedup). Do the same for `shot_blocked`/`cross_blocked` in the SPADL staging model that feeds `fct_action_values`.

- [ ] **Step 2: Add the columns to the mart contracts.** In `_marts__models.yml`, under the `fct_action_context` model's `columns:` list, add each new column with its `data_type` matching the DDL (Task 9). Add `shot_blocked`/`cross_blocked` to the `fct_action_values` model's `columns:` block. (`fct_action_context.sql` uses `on_schema_change='append_new_columns'`, so the SELECT auto-appends, but the contract requires explicit column entries.)

- [ ] **Step 3: Run the completeness gate + dbt parse.** Run: `uv run pytest src/tests/test_marts_models_yml_completeness.py -v` and `uv run dbt parse --project-dir dbt_project --profiles-dir dbt_project`. Expected: PASS / no compile error.

---

### Task 12: Rebaseline goldens + docstring + golden↔contract assertion

**Files:**
- Modify: `src/tests/fixtures/action_context/idsse/J03WMXmini_p1/golden.parquet`, `src/tests/action_context/test_mini_golden.py` (docstring `103`→`167`), optionally the full `J03WMX_p1` golden
- Test: `src/tests/action_context/test_mini_golden.py`

- [ ] **Step 1: Regenerate the mini-golden.** Run: `uv run python scripts/build_ac1_mini_golden.py`. This recomputes the real work-unit through the 4.87.0 chain and rewrites `golden.parquet` with the new column set.

- [ ] **Step 2: Fix the stale docstring.** In `test_mini_golden.py`, change the "103 columns" docstring to the real count (now 167 output columns, or a generic phrasing).

- [ ] **Step 3: Add a golden↔contract type assertion — EXEMPT the ADR-013 writer-join columns (review-3 H-1).** In `test_mini_golden.py` (or a sibling), assert the golden's column set + inferred dtypes are consistent with the `fct_action_context` contract in `_marts__models.yml`. **The mart is a SUPERSET of the drain**: the xt_gk_v2 columns (`xt_gk_v2_*`, `gk_geometry_source`) are contract columns fed by a mart LEFT JOIN, NOT drain output — so assert `golden.columns ⊆ contract.columns` (not `==`) and exempt the writer-join set from the golden side. A drain-native column present in the contract but missing from the golden still fails; a writer-join column does not.

- [ ] **Step 4: Run the mini-golden gate.** Run: `uv run pytest src/tests/action_context/test_mini_golden.py -v`. Expected: PASS.

- [ ] **Step 5: Regenerate the gated full golden.** Run: `uv run python scripts/build_ac1_full_golden.py`, then `AC1_E2E=1 uv run pytest src/tests/action_context/test_e2e.py -v`. Expected: PASS. Spot-check `test_differential.py` for the orientation-cycle away-vs-home asymmetry.

---

### Task 13: Expected-shift oracle (correctness deliverable)

**Files:**
- Create: `src/tests/action_context/expected_shift_oracle.py`, `src/tests/action_context/test_expected_shift_oracle.py`
- Test: the oracle self-test

**Interfaces:**
- Produces: `expected_shift_bands(column: str) -> ShiftBand` where `ShiftBand` declares `cohort` (`"away"`/`"home_y_mirror"`/`"all"`), `min_change_rate`/`max_change_rate` (floats), and optional `direction`. Consumed by Part B §11.1b.

- [ ] **Step 1: Write the oracle module** encoding, per value-shifting column, the expected shift from the changelog's measured deltas, **with deliberately generous band width** (the deltas are point estimates on different corpora — see the Task 20 calibration note; err wide, because a too-tight band false-halts a 5.5h recompute). Include at minimum: `space_creation` created/denied (away cohort, ~0.47 GS / ~0.60 IDSSE change rate, direction = columns exchanged pre-fix); the OBSO synthetic→xT columns (all rows, distributional not per-row); `cross_blocked` for StatsBomb (0%→~base-rate non-null). Represent as a dict of `ShiftBand` dataclasses.

```python
# src/tests/action_context/expected_shift_oracle.py
from dataclasses import dataclass

@dataclass(frozen=True)
class ShiftBand:
    cohort: str            # "away" | "home_y_mirror" | "all"
    min_change_rate: float
    max_change_rate: float
    direction: str | None = None

EXPECTED: dict[str, dict[str, ShiftBand]] = {
    # column -> provider -> band  (provider "*" = all)
    "space_creation_created": {"gradientsports": ShiftBand("away", 0.35, 0.60, "exchanged"),
                               "idsse": ShiftBand("away", 0.45, 0.70, "exchanged")},
    "cross_blocked":          {"statsbomb": ShiftBand("all", 0.005, 0.05, "na_to_real")},
    # ... one entry per value-shifting column, values from the changelog deltas
}

def band_for(column: str, provider: str) -> ShiftBand | None:
    per = EXPECTED.get(column)
    if per is None:
        return None
    return per.get(provider) or per.get("*")
```

- [ ] **Step 2: Write the oracle self-test** asserting the bands are well-formed (rates in [0,1], min≤max, cohorts valid) and cover the columns Task 12's differential flagged as moved.

- [ ] **Step 3: Run it.** Run: `uv run pytest src/tests/action_context/test_expected_shift_oracle.py -v`. Expected: PASS. (The oracle is consumed against LIVE data in Part B Task 20.)

---

### Task 14: New-column range/invariant checks

**Files:**
- Test: add a function to `src/tests/action_context/test_mini_golden.py` (reuses `_recompute()`)

- [ ] **Step 1: Write range/vocab checks** (add to `test_mini_golden.py`):
```python
_OBSO_SRC_VOCAB = {"xt", "injected"}  # never "synthetic" on the tracking path
_DAS_SRC_VOCAB = {"computed", "unlinked", "unscoreable_frame", "team_unresolved", "unscoreable_call"}
_GHOST_SRC_VOCAB = {"computed", "velocity_unavailable", "no_keeper", "unlinked", "goal_end_unresolved", "direction_unresolved"}
_PRESS_SRC_VOCAB = {"computed", "no_pressing_defender", "velocity_unavailable", "window_too_short", "degenerate_axis", "unlinked"}

def test_new_column_ranges_and_vocab() -> None:
    result = _recompute()
    def _nn(col):  # non-null values
        return result[col][result[col].notna()]
    assert set(_nn("obso_epv_source").unique()) <= _OBSO_SRC_VOCAB
    assert set(_nn("das_source").unique()) <= _DAS_SRC_VOCAB
    assert set(_nn("ghost_gk_source").unique()) <= _GHOST_SRC_VOCAB
    assert set(_nn("press_commitment_source").unique()) <= _PRESS_SRC_VOCAB
    cs = _nn("press_commitment_closing_speed").astype(float)
    assert ((cs >= 0) & (cs <= 15)).all(), "closing speed out of physical m/s range"
    assert (_nn("n_disruptive_runs").astype(float) >= 0).all()
    assert np.isfinite(_nn("packing_net").astype(float)).all()
```
(The vocab sets are transcribed from the silly-kicks 4.87.0 changelog; re-confirm against the source — a value outside a set means an upstream vocab change to fold in, not a test to loosen.)

- [ ] **Step 2: Run it — after Task 9** (needs the columns registered to survive into `_recompute()` output). Run: `uv run pytest src/tests/action_context/test_mini_golden.py::test_new_column_ranges_and_vocab -v`. Expected: PASS.

---

### Task 15: New-private-import lint

**Files:**
- Create: `src/tests/test_no_new_private_sk_imports.py`
- Test: itself

- [ ] **Step 1: Write an AST lint** that scans `src/` + `scripts/` for `silly_kicks._…` / `silly_kicks.<pkg>._…` imports and asserts every one is on a documented-intentional allowlist (the 4 `_SK_GUARD_SUBMODULES` private paths + the known `_id_compat`→now-public, `_xt_gk`, `_PERIOD_START_SECONDS`, `_convert_locations` uses). A new private import not on the allowlist fails.

- [ ] **Step 2: Run it.** Run: `uv run pytest src/tests/test_no_new_private_sk_imports.py -v`. Expected: PASS (seed the allowlist from the current known-intentional set so it starts green).

---

### Task 16: Governance — cards, AI_GOVERNANCE.md, Appendix D

**Files:**
- Modify: `AI_GOVERNANCE.md` (§5), `ARCHITECTURE.md` (§8 Appendix D), `src/tests/test_architecture_md_appendix.py` (`expected_authors`), the affected `workflow-cards/*.yaml` + `docs/huggingface/model-cards/*`
- Create: any new per-family workflow card + model card (if a family needs its own)
- Test: `src/tests/test_ai_governance_md.py`, `src/tests/test_architecture_md_appendix.py`

- [ ] **Step 1: Determine the card mapping.** For each new evaluative family, decide extend-existing vs new card: run-values → likely extends `wf-off-ball-xt`; `obso_epv_source` → `wf-obso-pausa`; packing → new `wf-packing`; press-commitment → new `wf-press-commitment`. For every new card, add it to `PER_PLAYER_EVALUATIVE_CARDS` **and** `WORKFLOW_TO_MODEL_CARD` in `test_ai_governance_md.py`, create the workflow card `.yaml` with a `governance:` block referencing `AI_GOVERNANCE.md`, create the matching model card with the `EU AI Act — Intended Use and Non-Use` stanza + `SEC-AUDIT-v1.12.0 REG-01` tag, and add the card ID to `AI_GOVERNANCE.md` §5.

- [ ] **Step 2: Add academic references.** Add authors to `ARCHITECTURE.md` §8 Appendix D (packing = Impect; press-commitment; run-values = TF-35) and extend `expected_authors` in `test_architecture_md_appendix.py`.

- [ ] **Step 3: Bump the `Next review` date** in `AI_GOVERNANCE.md` frontmatter if within the 30-day grace window.

- [ ] **Step 4: Run the governance gates.** Run: `uv run pytest src/tests/test_ai_governance_md.py src/tests/test_architecture_md_appendix.py -v`. Expected: PASS.

---

### Task 17: ADR

**Files:**
- Create: `docs/superpowers/adrs/ADR-0XX-silly-kicks-4-86-1-full-adoption.md` (next free ADR number — grep `docs/superpowers/adrs/` for the highest)

- [ ] **Step 1: Write the ADR** in the Nygard format (`ADR-TEMPLATE.md`): context (4.43→4.87.0 adoption + the new column/mart contracts + the corpus-wide recompute/retrain), decision (**full scope, nothing deferred**; the wide-by-grain mart map; **xtgk v2 replaces v1**; the DAG-complete rebuild; the expected-shift oracle), consequences. Reference the spec (Rev 6). **Include** one sentence that `cross_blocked` is now consumed by `compute_bravery` (→ `fct_match_summary`) and defensive-credit — no VAEP feature reads it, so "no retrain" holds. Note the xtgk-v2 not-construct-validated caveat as an explicit accepted consequence.

- [ ] **Step 2: No test** — ADRs are prose. Confirm the file renders and links resolve.

---

## Part A2 — Rev 6 families & marts (full scope, wide-by-grain)

> These run interleaved with Part A: the AC-column tasks (17a/17b/17c) must land **before** the golden
> rebaseline (Task 12) and schema registration (Task 9); the mart tasks (17d–17h) can follow. All AC-observable
> tests obey the big-bang red-window + Task-9 registration rule from Part A's preamble.

### Task 17a: team_shape gap columns (free-ride)

**Files:** Modify `src/analytics/action_context/schema.py` (add 6 cols to `RESULT_COLUMNS`+DDL), the bronze migration (Task 10), `stg_action_context__values.sql`, `_marts__models.yml`. Test: `test_action_context_schema_parity.py` + a presence guard in `test_mini_golden.py`.

- [ ] **Step 1:** Add `team_shape_defensive_line_height_{attacking,defending}`, `team_shape_inter_line_gap_1_{attacking,defending}`, `team_shape_inter_line_gap_2_{attacking,defending}` (6 × DOUBLE) to `RESULT_COLUMNS`+`ACTION_CONTEXT_DDL`. `add_team_shape` already emits them (it produces 20, we carried 14) — no enrich change, pure registration.
- [ ] **Step 2:** Add to `_NEW_SK4861_COLS` presence guard; run `test_action_context_schema_parity.py` + the presence guard (after Task 9). Expected: PASS.

### Task 17b: xtgk v2 replaces v1 (**training sub-project**: fit + ADR-013 writer)

> **Corrected from inline-wiring (review-2 blocking).** Only `retention` is bundled (Task 0 Step 2); `possession_value`
> + `turnover_cost` must be **fit** on the gold action marts. So v2 is a trainer + writer — NOT inline in `enrich.py`
> (enrich runs only bundled models; fitted v2 weights are delivered via UC Volume per ADR-012, then scored by an
> ADR-013 writer). This is the largest single workstream.

**Files:** Create `scripts/train_xt_gk_v2_hf.py` (ADR-012 trainer), `src/ingestion/xt_gk_v2_writer.py` (ADR-013 writer). Modify `src/analytics/action_context/enrich.py` (**retire** steps 25/25b `add_xt_gk` + the preset loop — do NOT add v2 there), `schema.py` (retire v1 cols, add 6 v2 cols sourced from the writer), migration, staging, mart contract. Test: `scripts/tests/test_train_xt_gk_v2.py` (smoke) + `test_xtgk_v2_replaces_v1.py`.

- [ ] **Step 1: Trainer** `scripts/train_xt_gk_v2_hf.py` — `MarkovPossessionValue.fit(actions, xg_column=…, pressure_column=…, pressure_levels=…)` + `EmpiricalTurnoverValue.fit(actions, xg_column=…, pressure_column=…)` (retention via `GkRetentionModel.from_variant` — bundled).
  - **Fit corpus (review-4 A2/A5 — precise, or `.fit()` RAISES):** NOT "the gold action marts" — it is **AC-enriched actions ⋈ `fct_shot_xg`** carrying, on every row: non-null `game_id` (turnover `.fit` hard-raises on null — ADR-017/019), `possession_id` (else `add_possessions` runs), `start_x/start_y`, a **`pressure` column** (AC-layer — in `fct_action_context`/`bronze.spadl_action_context`, NOT `fct_action_values`), and the `xg_column` (left-joined from `fct_shot_xg`). Fitting on `fct_action_values` (no pressure) raises. `MarkovPossessionValue.fit` runs `validate_possession_value_input` and hard-raises on a missing column.
  - **Acyclicity invariant (review-4 A1 — load-bearing):** the trainer AND the writer read this **v2-free** corpus (bronze AC + `fct_shot_xg`), **never** the post-join `fct_action_context` mart (which contains the writer's own v2 output). Reading the mart the writer lands into is a data/dbt cycle. Nothing `compute_xt_gk_v2` or the fit consumes is downstream of `xt_gk_v2`.
  - **Artifact packaging (review-4 A4):** `MarkovPossessionValue.save` writes a **directory** (surfaces `.npz` + support + metadata), not a single file — confirm `upload_weights_to_uc_volume` (ADR-012) and the writer's `.load(directory)` both handle a directory artifact. `pressure_levels` round-trips in metadata (`.load()` restores `.pressure_levels`, satisfying v2's "never refit / terciles must match" guard).
  - ADR-012 delivery: `require_mlflow_env`, `set_and_verify_mlflow_champion`, `upload_weights_to_uc_volume`; module-level `_REQUIRED_SK_MIN=(4,87,0)`; `--secrets` on HF Jobs.
- [ ] **Step 2: v1→v2 reconciliation.** Retire the 16 v1 `xt_gk_*` + the 5 presets `xt_gk_{possession,counter,direct,high_press,low_block}`; **keep `gk_completion`** (distinct `add_gk_completion`). Grep consumers (`rg xt_gk src/ dbt_project/ hf_taipy_app/`) and re-home onto `xt_gk_v2`. **⚠ Capability loss (M-A): the 5 philosophy presets have NO v2 successor** — any Taipy/HF/mart view on them loses it; this is a REGRESSION of v2-replaces-v1, flag it for the user, do not pretend it's a re-home.
- [ ] **Step 3: Writer** `src/ingestion/xt_gk_v2_writer.py` (ADR-013), reading the fitted models from UC Volume + bundled retention:
```python
gk = actions[actions["is_gk_distribution"]].copy()    # A3: MANDATORY pre-filter — compute_xt_gk_v2 scores
                                                      # EVERY finite row in a per-action Python LOOP; passing the
                                                      # full stream scores off-domain rows AND OOM/timeouts on 3M+
resolved = apply_resolved_gk_geometry(gk)             # adds gk_geometry_source, overrides coords in-domain
rf = extract_retention_features(resolved)
v2 = compute_xt_gk_v2(resolved, possession_value=pv_from_volume, retention=GkRetentionModel.from_variant(variant),
                      turnover_cost=tc_from_volume, pressure_levels=pl, retention_features=rf)
# → bronze xt_gk_v2_{position,pev,retention_loss,dzv} + xt_gk_v2 + gk_geometry_source, GK-distribution rows only
```
Ordering enforced by v2's internal `_check_coordinate_coherence` (resolve → features → score). **The writer reads
the v2-free corpus (A1), NOT `fct_action_context`.** Off-domain (non-GK-distribution) actions get NULL v2 in the
mart LEFT JOIN — correct.
- [ ] **Step 4: TWO-TIER schema split (review-3 H-1) — v2 is a mart-join, NOT a drain column.** The AC drain (`enrich.py`/`_recompute()`) no longer produces xt_gk_v2 (it's writer-scored, ADR-013), so:
  - **Retire** the 21 v1 columns (16 `xt_gk_*` + 5 presets) from `RESULT_COLUMNS` + `ACTION_CONTEXT_DDL` — the drain no longer emits them. `gk_completion` stays (drain-native).
  - **Do NOT add** the 6 v2 columns to `RESULT_COLUMNS`/`ACTION_CONTEXT_DDL`/the golden — the drain never produces them, so the mini-golden (`_recompute() == RESULT_COLUMNS`) and `test_action_context_schema_parity` would fail. This is the xG/PSxG pattern (ADR-013): writer emits predictions → **not** in the AC schema → resolved by a mart join.
  - Register the 6 v2 cols **only in the `fct_action_context` mart contract** (`_marts__models.yml`), fed by the writer's bronze (Step 3) → a new `stg_xt_gk_v2` staging model → a per-action **LEFT JOIN** in `fct_action_context.sql`. The **bronze migration goes on the writer's OWN table**, not an `ALTER` on `bronze.spadl_action_context`.
  - Test: `test_xtgk_v2_replaces_v1.py` asserts the v1 cols are ABSENT from `RESULT_COLUMNS`, `gk_completion` retained, and the 6 v2 cols present in the **mart contract** (not the drain golden).

### Task 17c: visibility parser + 8 columns

**Files:** Create `src/analytics/action_context/visible_area.py` (parser); modify `enrich.py` (pass `visible_area=` to `add_action_context` + call `add_visible_area_coverage`), `schema.py`, migration, staging, mart contract. Test: `test_visible_area_parser.py`.

- [ ] **Step 1:** Write a parser building an `action_id`→`polygon` `(N,2)` frame from `bronze.statsbomb_360.visible_area` (JSON vertices), mirroring `providers.statsbomb.shape_snapshots`; keyed on `canonical_id(action_id)` (ADR-019 — dtype-mismatch silently → all `no_polygon`).
- [ ] **Step 2:** In the SB360 chain of `enrich.py`, build the frame and pass `visible_area=` to `add_action_context` (adds the 6 `*_observed_*` companions) and call `add_visible_area_coverage(out, visible_area=va)` (adds `visible_area_fraction/source`). Register all 8 in `schema.py`.
- [ ] **Step 3:** Test the parser on the SB360 fixture; the 8 columns are SB360-only (empty for other providers / until SB360 AC enabled). Expected: parser PASS; columns present in schema.

### Task 17d: `fct_action_defensive` mart (per-action defensive-credit, post-xG)

**Files:** Create a writer `src/ingestion/defensive_credit_writer.py` (reads bronze AC frames + `fct_shot_xg` predictions, calls `add_defensive_credit(xg_column=…, xt=…, blocked_column="shot_blocked")`) → bronze → `stg_*` → `dbt_project/models/marts/fct_action_defensive.sql` + `_marts__models.yml`. Test: writer unit test + dbt contract.

- [ ] **Step 1:** Writer produces `defensive_credit_net/_plus/_minus` (DOUBLE), `n_defensive_credits` (BIGINT), per action, following ADR-013. **Must be downstream of `fct_shot_xg`** (not `fct_action_values` — that is upstream-joined by xG; a dbt cycle otherwise). **(review-3 L, verified):** `add_defensive_credit` takes `blocked_column="shot_blocked"` only — no `cross_blocked` param, and `rule_failed_cross_block` (`_rules.py:405`) reads `ctx.blocked_column` (shot_blocked) on the *resulting shot*, not `cross_blocked`. So passing shot_blocked is complete; nothing cross-block is silently dropped (only bravery/Task 17g consumes `cross_blocked`). The asymmetry is by design.
- [ ] **Step 1b: input columns + xG merge key (review-4 B3).** `add_defensive_credit` also reads `on_target_column="shot_on_target_derived"` — **confirm that SPADL-enrichment column is on the lakehouse actions corpus** (silly-kicks emits it in `apply_spadl_enrichments`; verify it lands in `bronze.spadl_actions`/the AC actions frame). And **pin the xG merge:** `xg_column` must be present on `actions`, so the writer LEFT-JOINs `fct_shot_xg` onto the actions frame on the per-shot action identity — `fct_shot_xg` resolves via `shot_id` per ADR-013, so pin the `shot_id ↔ action_id` key; non-shot actions get NaN xg (fine — credit rules only fire on shot/cross-resulting-in-shot rows).
- [ ] **Step 2:** dbt model `fct_action_defensive` `ref('fct_shot_xg')` + the AC identity fact; `contract: enforced`. Test: `dbt parse` (no cycle) + `test_marts_models_yml_completeness.py`.

### Task 17e: `fct_off_ball_runs` mart (per (action, runner))

**Files:** Writer `src/ingestion/off_ball_runs_writer.py` (`detect_off_ball_runs` + `value_off_ball_runs(runs, actions, frames, xt)`) → bronze → staging → `fct_off_ball_runs.sql` + contract. Test: writer unit + contract.

- [ ] **Step 1:** Emit the 14 detect cols (incl. `peak_speed_source`, `toward_goal` BOOLEAN) + 4 value cols (`role`,`is_receiver`,`run_value`,`enabled_pass_credit`). Grain: one row per (action, runner) (confirmed review-4 B5). `value_off_ball_runs` needs a fitted `xt`. **Null-rate bounds (B5):** `value_off_ball_runs` values only completed passes/crosses with a resolved receiver — everything else is legitimately `NaN` `run_value` / `<NA>` `role`, so **most rows are off-domain NaN**; size the mart's per-column null-rate bounds for that large, correct NaN share (do not gate on low-null).
- [ ] **Step 2:** dbt model + contract; run completeness test.

### Task 17f: long-form defensive-credit mart (per (action, player, rule))

**Files:** Extend the Task-17d writer (or a sibling) to also emit `compute_defensive_credits` long-form → bronze → staging → `fct_defensive_credit_attributions.sql` (grain-named) + contract.

- [ ] **Step 1:** Emit the 11 cols (`game_id,period_id,action_id,player_id,team_id,rule,signed_value,anchor_type,frame_id,sizing,resolution`) with the exact vocabs. Grain: (action, player, rule).
- [ ] **Step 2:** dbt model + contract; completeness test.

### Task 17g: bravery → extend `fct_match_summary`

**Files:** Modify `dbt_project/models/marts/fct_match_summary.sql` (join bravery, keyed on `(match_key, defending team_id)`), its `_marts__models.yml` contract; a writer `src/ingestion/bravery_writer.py` (`compute_bravery`) → bronze if not computable in-dbt.

- [ ] **Step 1:** Compute `compute_bravery(actions, shot_blocked_column="shot_blocked", cross_blocked_column="cross_blocked")` → 10 cols (grain = **defending** team per match, confirmed review-4 B4). **Resolve native ids → surrogates (review-4 B2):** `compute_bravery` emits native `(game_id, team_id)`; `fct_match_summary` is keyed on `match_key`/`team_id` — resolve `game_id → match_key` (via `dim_matches` on `(provider, native_match_id)`) and align `team_id` to the mart's team key before landing, or the join is silently all-NULL. Land in bronze, join into `fct_match_summary` at its `(match_key, team_id)` grain.
- [ ] **Step 2:** Extend the contract; run completeness + `dbt parse`.

### Task 17h: gkdv → extend `fct_gk_shot_stopping_pooled`

**Files:** Writer `src/ingestion/gkdv_writer.py` (`build_ghost_frames(home_team_id)` → per-frame `delta_das`/`delta_threat_suppression` → `aggregate_by_keeper(value_col, min_nonzero=20, min_games=2)` partitioned by `(competition, season)`) → bronze → staging → extend `fct_gk_shot_stopping_pooled.sql` + contract. Requires the `[das]` extra.

- [ ] **Step 1: EXCLUDE dropped frames before scoring (review-4 B1 — HIGH, silent null-bias).** `build_ghost_frames` returns `(counterfactual_frames, provenance, report)`; a dropped frame (missing/NaN GK, off-domain) is **byte-identical** across the actual/ghost legs → `delta_das`/`delta_threat` = **0** on it, biasing every keeper aggregate toward null. The API does NOT enforce this (only an upstream sk test does). So restrict to `provenance["drop_reason"].isna()` (or use `provenance_to_targets`) **before** differencing/aggregating. Then per value_col emit `mean, median, n, n_nonzero, n_games, gate_eligible` as `gkdv_delta_das_*` / `gkdv_delta_threat_*`.
- [ ] **Step 2: Resolve native ids → Kimball surrogates (review-4 B2).** `aggregate_by_keeper` keys on **native `player_id`** (the library deliberately avoids a gold join). The mart is keyed on `player_key` / `competition_key` / `season_id`. So the writer must resolve `player_id → player_key` (via `dim_players` on `(provider, native_player_id)`) and land per `(player_key, competition_key, season_id)` — a wrong/missing resolution makes the mart join silently all-NULL. Run `aggregate_by_keeper` partitioned by `(competition, season)` to match the grain.
- [ ] **Step 3:** Join into `fct_gk_shot_stopping_pooled`; extend contract. Caveat comment: gkdv API evolving upstream — pinned to 4.87.0.

### Task 17i: governance for the new evaluative families (per-family budget — review-2 M-B)

**This is ~7× the Task-16 governance surface — not "as Task 16, extended."** Each of the seven evaluative
families below is a **full non-negotiable chain**, all parity-enforced by `test_ai_governance_md.py`: a workflow
card + a HF model card (with the `EU AI Act — Intended Use and Non-Use` stanza + `SEC-AUDIT-v1.12.0 REG-01`
tag) + a `governance:` YAML block + an entry in BOTH `PER_PLAYER_EVALUATIVE_CARDS` and `WORKFLOW_TO_MODEL_CARD`
+ an `ARCHITECTURE.md` §8 Appendix-D author. Budget one sub-task per family; it half-completes and reds the
gate at the very end if rushed.

- [ ] **Per family** (extend-existing vs new card): **run-values** → likely extend `wf-off-ball-xt`; **packing**
  → new `wf-packing`; **press-commitment** → new `wf-press-commitment`; **defensive-credit** → new
  `wf-defensive-credit`; **bravery** → new `wf-bravery`; **gkdv** → new `wf-gkdv`; **xtgk-v2** → extend/replace
  the xt-gk card (v1→v2). For each: create/extend the card + model card + governance block, register in both
  dicts, add the Appendix-D author.
- [ ] **Run the gates once all seven land:** `uv run pytest src/tests/test_ai_governance_md.py src/tests/test_architecture_md_appendix.py -v`. Expected: PASS (the parity tests fail loudly on any half-done family).

---

### Task 18: Full local gate + wheel build

**Files:** none new (verification + wheel build)

- [ ] **Step 1: Run all 7 CI checks locally.** Run each and capture exit codes (never `| tail`):
```bash
uv run ruff check src/ scripts/
uv run ruff format --check src/ scripts/
uv run lint-imports
uv run python scripts/bump_wheel.py --check
uv run python scripts/pip_audit_ignores.py --check
uv run pytest src/tests/
uv run pyright src/ hf_taipy_app/src/ scripts/_tf_env_pins.py scripts/sync_tf_env_pins.py
```
Expected: all pass with exit 0.

- [ ] **Step 2: Build/publish the wheel from the final tree (post-Phase-2).** The wheel force-includes `dbt_project/`; build it now, after all dbt edits (Task 11), so it ships the updated project. (A same-version wheel will not overwrite the UC Volume copy — the version bump in Task 1 Step 7 ensures a new version.) Follow the repo's wheel publish path.

- [ ] **Step 3: STOP for the merge gate.** Part A is complete. The user reviews, commits, and merges (with `--admin`); post-merge `python-ci.yml` must be green before any Part B step runs. Do not proceed to Part B until CI is green.

---

# Part B — Live recalculation runbook (post-merge, operator-driven, per-step approval)

> Each Part-B task runs against LIVE Databricks and requires explicit user approval before it runs. Use `run_in_background` + poll for anything > 30s. Capture per-provider counts BEFORE any destructive step.

### Task 19: Apply the bronze migration + pre-checks

- [ ] **Step 1:** Confirm post-merge CI is green.
- [ ] **Step 2:** Apply the migration: `uv run --extra sdk python scripts/migrations/2026-08-19-add-sk4861-ac-and-spadl-columns.sql` via `scripts/migrations/_runner.py`. Verify with a live `DESCRIBE` that the new columns exist.
- [ ] **Step 3:** Ghost-GK artifact pre-check (Task 5 Step 3): confirm no `1.2.0` ghost-GK artifact in the UC Volume.

### Task 20: Pre-wipe shadow validation (§11.1b)

- [ ] **Step 1:** Recompute a small **sample** of matches (≥1 per provider) into a shadow schema (NOT the live bronze). Capture pre-existing live values for the same matches.
- [ ] **Step 2:** Diff old-vs-new distributions across the four surfaces (AC columns; `spadl_actions` incl. `cross_blocked`; `shot_freeze_frames`; `vaep_action_values`) and assert each value-shifting column falls inside its `expected_shift_oracle` band (Task 13). A shift outside the bands HALTS the runbook. Capture the diff report before proceeding.
- [ ] **Step 2 (calibration — review-1 M2):** the bands are hand-transcribed estimates, so the **first** Task-20 run is partly a calibration of the oracle, not only a verdict on the data. For each out-of-band column: **investigate before adjusting.** If the observed shift is explained by a known 4.87.0 mechanism (an entry in the changelog's measured deltas / the orientation cohort model) and merely sits outside a too-tight point-estimate band, **widen the band and record the mechanism in the oracle** as the justification, then re-check. If the shift is **unexplained** — wrong cohort, wrong sign, a column the changelog says should not move — it is a real halt: stop and investigate the recompute, do NOT widen. Never widen a band solely to make Step 2 pass; each widening carries a one-line mechanism citation.

### Task 21: AC recompute (destructive)

- [ ] **Step 1:** Capture per-provider pre-counts of `bronze.spadl_action_context` (capture-before-cleanup).
- [ ] **Step 2:** Wipe: `DELETE FROM soccer_analytics.bronze.spadl_action_context` for the 4 tracking providers.
- [ ] **Step 3:** Run `w.jobs.run_now(job_id=302697362345215, only=["preflight_action_context","compute_action_context"])`. Monitor bronze row count climbing (~5.5h). Re-run SPADL/VAEP bronze + `shot_freeze_frames` (SPADL surface changed).
- [ ] **Step 4:** Verify per-column null-rate bounds (NOT 0-NULL) and row-count == pre-count + additive. Read `observability.action_context_unit_events` / the `verify_action_context_drain` gate.

### Task 22: Retrain VAEP → ScoutGPT → xG v3

- [ ] **Step 1:** Retrain VAEP (`hf jobs uv run` per ADR-012, `--secrets`). Verify `_REQUIRED_SK_MIN==(4,87,0)` from the shipped wheel before dispatch. Rebuild `fct_action_values`.
- [ ] **Step 2:** Retrain ScoutGPT (consumes the rebuilt `vaep_value`).
- [ ] **Step 3:** Retrain xG v3 (consumes recomputed `shot_freeze_frames`; may run parallel to Steps 1–2). Rebuild `fct_shot_xg`.
- [ ] **Step 4:** Confirm football2vec + PSxG are NOT retrained (verdict NOT-NEEDED).

### Task 22b: Run the Rev-6 mart writers (post-recompute, post-xG)

- [ ] **Step 1:** After the AC recompute (Task 21) and the xG-v3 retrain + `fct_shot_xg` rebuild (Task 22), run the new-mart bronze writers: `off_ball_runs_writer` (needs recomputed AC frames + `xt`), `bravery_writer`, `gkdv_writer` (build-ghost-frames→score→aggregate; `[das]` extra), and `defensive_credit_writer` — the last **must run after `fct_shot_xg`** (reads its predictions). Capture per-provider row counts.
- [ ] **Step 2:** Verify each writer's bronze lands with per-column null-rate bounds (event-conditional columns are legitimately null — packing/press/run-values/credit); these feed Task 23's DAG rebuild of the new/extended marts.

### Task 22c: Fit + score xtgk-v2 (review-3 H-2 — v1 retired but v2 never runs otherwise)

- [ ] **Step 1: Fit.** After Task 22 Step 3 (`fct_shot_xg` rebuilt — the Markov `.fit(actions, xg_column=…)` + turnover `.fit(…, xg_column=…)` need an xG column), dispatch `train_xt_gk_v2_hf.py` (per §11.2 order); champion-verify + UC-Volume upload per ADR-012.
- [ ] **Step 2: Score.** Run `xt_gk_v2_writer` (reads the fitted models from UC Volume + bundled retention) → populate its bronze (`xt_gk_v2_*` + `gk_geometry_source`), per-action. Without this, `xt_gk_v2` is all-NULL and `gk_geometry_source` absent after the rebuild — silently, since the mart LEFT JOIN yields NULL with no error.
- [ ] **Step 3:** Verify the writer bronze row count matches the AC action count (within domain) before Task 23 picks it up.

### Task 23: DAG-complete mart rebuild

- [ ] **Step 0 (guard — review-1 M1): `dbt ls --select +` only sees `ref()` edges.** A mart that reads a root by a hardcoded `{catalog}.dev_gold.<table>` string is invisible to the selector and would silently NOT rebuild. Before trusting the selector, run `rg -n "fct_action_values|spadl_actions|spadl_action_context|shot_freeze_frames" dbt_project/models` and confirm **every** hit is inside a `{{ ref(...) }}` (or a declared `source()`), not a raw string. Any raw-string reader must be added to the rebuild set by hand (and ideally converted to `ref()`).
- [ ] **Step 1:** Compute the downstream set: `dbt ls --select stg_spadl__actions+ stg_action_context__values+ stg_spadl__action_values+ <freeze-frame staging>+ fct_action_values+ --resource-type model`. Union the results. **(review-2 R1-reintroduced): the Rev-6 marts fed by NEW bronze — `fct_off_ball_runs` and the long-form defensive-credit mart — are NOT descendants of any of these five roots (their lineage starts at the Task-22b new-bronze tables), so the selector will MISS them.** Add each new-bronze staging model as an explicit additional root (`stg_off_ball_runs+ stg_defensive_credit_long+ stg_gkdv+ stg_bravery+ stg_xt_gk_v2+` — `stg_xt_gk_v2` is UPSTREAM of `fct_action_context`, so it too must be a root or the mart re-runs its v2 LEFT JOIN against stale/absent staging) **or** select the new/extended marts by name (`fct_off_ball_runs fct_action_defensive fct_match_summary fct_gk_shot_stopping_pooled <long-form>`). Do not assume the five-root selector reaches them — verify each Rev-6 mart is in the union before building.
- [ ] **Step 2:** Partition by TRIGGERED-synced membership. Rebuild staging views first, then: TRIGGERED marts via `rederive_synced_marts.py --select <them>` (`--rebuild` for the schema-changed ones); others via `dbt build --select <them>`. NEVER `dbt --full-refresh` a TRIGGERED mart.
- [ ] **Step 3:** Verify each rebuilt mart's row counts + that the new columns are populated within their null-rate bounds.

### Task 24: HF republish

- [ ] **Step 1:** Republish every changed dataset through the ADR-072 guarded seam (`prepare_public_upload` / `upload_guarded`) — never direct `HfApi`. Update HF cards (`build_provider_configs`).

### Task 25: Refresh synced tables

- [ ] **Step 1:** Refresh SNAPSHOT-synced marts in the rebuilt set via `refresh_synced_tables`. (TRIGGERED ones were handled by `rederive_synced_marts` in Task 23.)

### Task 26: End-to-end verification

- [ ] **Step 1:** Final sweep: per-column null-rate bounds hold bronze→staging→gold on every touched table; row-count parity; the Task-20 shadow-diff shape matched expectation.
- [ ] **Step 2:** Verify Taipy pages load and render the new columns with scale/direction labels (UX standard).
- [ ] **Step 3:** Report the diff/verification summary to the user.

---

## Self-Review notes

- **Spec coverage (Rev 6 — nothing deferred):** Phase 0 → Tasks 0–1; Phase 1 (§6.1–6.5) → Tasks 2–6; Phase 2 (§7, ALL families wide-by-grain) → Tasks 6–11 (AC columns) **+ Part A2 Tasks 17a–17i** (team_shape, **xtgk-v2 trainer+writer**, visibility, `fct_action_defensive`, `fct_off_ball_runs`, long-form defensive-credit, bravery→`fct_match_summary`, gkdv→`fct_gk_shot_stopping_pooled`, governance); Phase 3 (§8) → Tasks 12–14; Phase 4 (§9) → Tasks 15–17 + 17i; Phase 5 (§10) → Task 18 Step 3; Phase 6 (§11) → Tasks 19–26 + 22b. Delta-revalidation → Task 0.
- **Nothing is deferred (H-C fix — the previous "Deferred (no task)" line was stale from Rev ≤5).** `gk_geometry_source`/xtgk-v2 → Task 17b; visibility → Task 17c; defensive-credit (per-action + long-form) → Tasks 17d/17f; bravery → 17g; gkdv → 17h. These ARE the bulk of Part A2.
- **Type consistency:** the 16 new AC column names + dtypes are defined once in Task 8's Interfaces and reused verbatim in Tasks 9/10/11/13/14. The two id-passthrough columns defer their Spark type to the existing `_player_id` DDL column (Task 9 Step 1).
- **Open determinations left to the implementer (not placeholders — repo-state lookups):** the exact fixture-loader API in the AC tests; the exact id-passthrough Spark type (grep the DDL); the per-family card extend-vs-new decision (Task 16 Step 1); the next free ADR number (Task 17).
