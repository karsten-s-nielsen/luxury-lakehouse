# silly-kicks 4.19.1 + action-context fields (PR-1) — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Force silly-kicks `4.19.1` everywhere, add 11 new action-context columns (structural-pass ×3, xCross ×1, player-influence ×7), rename the ghost-GK spread column, add the dtype guard, and regenerate both AC goldens — all as code+schema+goldens (no live full recompute).

**Architecture:** silly-kicks is the single enrichment engine; the AC pipeline (`src/analytics/action_context/`) runs its `add_*` aggregators inside an `applyInPandas` UDF, projects to `RESULT_COLUMNS` (DDL is the single source of truth → parsed Spark StructType), persists to `bronze.spadl_action_context`, and surfaces via dbt `stg_action_context__values` → `fct_action_context` (enforced contract) → Lakebase `fct_action_context_synced`.

**Tech Stack:** Python 3.10, silly-kicks 4.19.1, pandas/numpy, PySpark (serverless), dbt (Databricks), Delta, Lakebase (PostgreSQL synced tables), uv, ruff, pyright, pytest.

**Spec:** `docs/superpowers/specs/2026-06-08-silly-kicks-4-19-1-and-action-context-fields-design.md`

---

## As-built deviations (recorded during execution 2026-06-08)

The authoritative as-built record is **ADR-042**. Deviations from this plan as written:

1. **Version is 4.19.2, not 4.19.1.** silly-kicks 4.19.2 published the same day (CI/test-infra only —
   byte-identical library runtime) and is the latest; per the user's "most recent" intent the floor is
   `>=4.19.2,<5` and all strings/`_REQUIRED_SK_MIN`/wheel-0.5.23 reflect 4.19.2. (Plan version-string
   `4.19.1` → `4.19.2` everywhere.)
2. **Task 7 resolved to KEEP the GS coercion (permanent), guard added.** The red-first drop test was
   blocked: the only GS enrich fixture (`gradientsports/10517_p3`) trips the ADR-040 absolute-clock
   time-base guard before reaching the seam, so the drop could not be proven safe → KEEP per
   Chesterton's fence (user-confirmed "leave the helper"). The additive `validate_id_dtypes` guard was
   added (exercised on the IDSSE path by the mini-golden recompute). `test_gs_int64_seam_coverage.py`
   was NOT created (the drop is not happening); `test_gradientsports_roster_dicts.py` is unchanged.
3. **`add_xcross_attempt` is NOT on the SB360 path.** Its `extract_xcross_features` hard-requires ball
   velocity (`vx`), which freeze-frames lack (raises `KeyError`, not honest-NaN). xcross is therefore
   velocity-dependent (tracking-only) and stays NULL on SB360 via `build_output`. `structural_pass` +
   `player_influence` (voronoi) do run on SB360.
4. **New academic authors added:** Karakus & Arkadas (2026, structural pass) AND Cao et al. (2025,
   xCross inspired-by) → ARCHITECTURE.md Appendix D + `expected_authors` + NOTICE.

## Commit policy (READ FIRST)

Per project CLAUDE.md + user rule: **no `git commit` / push / PR without separate explicit approval at the moment.** This PR uses a **single commit** at the end (Task 14), gated on approval. Individual tasks do **not** commit. The "Commit" step appears only once, at the end.

## Canonical new-column order (use this exact order EVERYWHERE)

Insert the 11 new columns as one block, and rename ghost. The block goes **immediately after** the ghost-GK block and **before** `xshot_occurrence` in `RESULT_COLUMNS`, `ACTION_CONTEXT_DDL`, `stg_action_context__values.sql`, `fct_action_context.sql` (both CTEs), and `_marts__models.yml`:

```
ghost_gk_x, ghost_gk_y, ghost_gk_density_spread,            # (ghost block; spread RENAMED)
structural_lbs, structural_sgm, structural_sdi,             # add_structural_pass
actor_reachable_area_m2, off_ball_xt_team, off_ball_xt_opponent, off_ball_xt_diff,
reachable_area_team, reachable_area_opponent, reachable_area_diff,   # add_player_influence
xcross_attempt,                                             # add_xcross_attempt
xshot_occurrence, pitch_control_method, ghost_gk_method     # (unchanged trailing)
```

Types: `structural_lbs` = **bigint** (silly-kicks `Int64`); everything else new = **double**.

---

## Task 1: Branch + bump silly-kicks to 4.19.1 (env)

**Files:**
- Modify: `pyproject.toml:30`
- Modify: `uv.lock` (regenerated)

- [ ] **Step 1: Sync main and create the branch**

Run:
```bash
git fetch origin && git pull --ff-only origin main
git checkout -b feat/silly-kicks-4-19-1-action-context
git status && git log --oneline origin/main..HEAD
```
Expected: clean branch off latest `origin/main`, no commits ahead. (Per memory: never branch from stale local main.)

- [ ] **Step 2: Raise the pyproject pin**

In `pyproject.toml:30`, change:
```
    "silly-kicks[das,ghost-gk]>=4.13.0,<5",
```
to:
```
    "silly-kicks[das,ghost-gk]>=4.19.1,<5",
```

- [ ] **Step 3: Refresh the lock + sync the venv**

Run (per `reference_uv_dep_adoption` — NEVER pip force-reinstall):
```bash
uv lock --refresh-package silly-kicks
uv sync --inexact --extra das --extra ghost-gk --extra sdk
```
Expected: `uv.lock` updates silly-kicks to 4.19.1.

- [ ] **Step 4: Verify installed version**

Run: `uv run python -c "import silly_kicks; print(silly_kicks.__version__)"`
Expected: `4.19.1`

- [ ] **Step 5: Verify the new aggregators import**

Run:
```bash
uv run python -c "from silly_kicks.tracking import add_structural_pass, add_xcross_attempt, validate_id_dtypes; from silly_kicks.tracking.features import add_player_influence; print('ok')"
```
Expected: `ok`

---

## Task 2: Force 4.19.1 across wheel + trainers + orchestrators + terraform

**Files:**
- Modify: `src/shared/wheel.py:18` (via `scripts/bump_wheel.py`)
- Modify: `scripts/train_football2vec.py:81`, `train_football2vec_360.py:76`, `train_football2vec_v2.py:78`, `train_scoutgpt_hf.py:82`, `train_vaep_model_hf.py:73`, `train_xg_v2_hf.py:96` (`_REQUIRED_SK_MIN`)
- Modify: `scripts/submit_ac1_oneshot.py:49`, `scripts/sk3_mig_b_retrain.py:7` and `:464`
- Modify: `terraform/modules/workflows/main.tf` (silly-kicks env dep)

- [ ] **Step 1: Bump the wheel version (propagates to trainer PEP-723 URLs)**

Run:
```bash
uv run python scripts/bump_wheel.py 0.5.23
```
Expected: `src/shared/wheel.py` `WHEEL_VERSION = "0.5.23"`; all 6 `scripts/train_*.py` PEP-723 headers now reference `luxury_lakehouse-0.5.23-py3-none-any.whl`. Verify:
```bash
git grep -n "0.5.22" -- src/shared/wheel.py scripts/train_*.py ; echo "exit=$?"
```
Expected: no `0.5.22` matches remain (exit 1).

- [ ] **Step 2: Raise `_REQUIRED_SK_MIN` in all 6 trainers**

In each of `scripts/train_football2vec.py`, `train_football2vec_360.py`, `train_football2vec_v2.py`, `train_scoutgpt_hf.py`, `train_vaep_model_hf.py`, `train_xg_v2_hf.py`, change:
```python
_REQUIRED_SK_MIN: tuple[int, int, int] = (4, 13, 0)
```
to:
```python
_REQUIRED_SK_MIN: tuple[int, int, int] = (4, 19, 1)
```

- [ ] **Step 3: Raise the orchestrator-script pins**

In `scripts/submit_ac1_oneshot.py:49`, change `"silly-kicks>=4.13.0,<5",` → `"silly-kicks>=4.19.1,<5",`.
In `scripts/sk3_mig_b_retrain.py:7` (PEP-723), change `#     "silly-kicks>=4.13.0,<5",` → `#     "silly-kicks>=4.19.1,<5",`.
In `scripts/sk3_mig_b_retrain.py:464`, change the runtime assertion `raise RuntimeError(f"silly-kicks {sk_version} < 4.13.0")` and its guard to `4.19.1` (update both the comparison tuple/string and the message; search the surrounding lines ~460-465 for the `(4, 13, 0)` / `4.13.0` comparison and raise it to `(4, 19, 1)` / `4.19.1`).

- [ ] **Step 4: Raise the terraform env dep**

In `terraform/modules/workflows/main.tf`, find the silly-kicks dependency pin (it carries a `>=...,<...` string parsed by `test_terraform_env_dep_parity.py`) and raise the floor to `4.19.1`. Run to locate:
```bash
git grep -n "silly-kicks" -- terraform/modules/workflows/main.tf
```

- [ ] **Step 5: Update CI workflow pins if present**

Run:
```bash
git grep -n "silly-kicks" -- .github/workflows/
```
Raise any pinned `4.13.0` floor to `4.19.1`. (If none reference a version, no change.)

(Tests for this task are updated + run in Task 3.)

---

## Task 3: Update + run the pin-parity / orchestrator-invariant tests

**Files:**
- Modify: `src/tests/test_sk3_mig_b_orchestrator_invariants.py` (lines ~230, ~257, ~300, ~304, ~327, ~329)
- Modify: `src/tests/test_terraform_env_dep_parity.py`
- Test: both of the above

- [ ] **Step 1: Update the orchestrator-invariant expectations**

In `src/tests/test_sk3_mig_b_orchestrator_invariants.py`, change every `(4, 13, 0)` expectation to `(4, 19, 1)` (the §2.10.5 trainer-constant check at ~304/327/329) and update the `[spadl]` / PEP-723 pin strings at ~230 and ~300 from `silly-kicks>=4.13.0,<5` to `silly-kicks>=4.19.1,<5`. Run to find them:
```bash
git grep -n "4, 13, 0\|4.13.0" -- src/tests/test_sk3_mig_b_orchestrator_invariants.py
```

- [ ] **Step 2: Update the terraform-parity expectation**

In `src/tests/test_terraform_env_dep_parity.py`, update the expected silly-kicks pin string to match the `4.19.1` floor set in Task 2 Step 4. Run to find it:
```bash
git grep -n "silly-kicks\|4.13.0" -- src/tests/test_terraform_env_dep_parity.py
```

- [ ] **Step 3: Run both tests**

Run:
```bash
uv run pytest src/tests/test_sk3_mig_b_orchestrator_invariants.py src/tests/test_terraform_env_dep_parity.py -v
```
Expected: PASS (all pins consistent at 4.19.1).

---

## Task 4: Schema — add 11 columns + ghost rename (`schema.py`)

**Files:**
- Modify: `src/analytics/action_context/schema.py` (`RESULT_COLUMNS` ~146-158, `ACTION_CONTEXT_DDL` ~160-216, header comment line 17)
- Test: `src/tests/action_context/test_schema.py`

- [ ] **Step 1: Add failing assertions to test_schema.py**

Append to `src/tests/action_context/test_schema.py`:
```python
_NEW_AC_FIELDS = [
    "structural_lbs", "structural_sgm", "structural_sdi",
    "actor_reachable_area_m2", "off_ball_xt_team", "off_ball_xt_opponent",
    "off_ball_xt_diff", "reachable_area_team", "reachable_area_opponent",
    "reachable_area_diff", "xcross_attempt",
]


def test_new_ac_fields_present_in_schema() -> None:
    for col in _NEW_AC_FIELDS:
        assert col in RESULT_COLUMNS, f"{col} missing from RESULT_COLUMNS"
    ddl_cols = [tok.strip().split()[0] for tok in ACTION_CONTEXT_DDL.split(",")]
    for col in _NEW_AC_FIELDS:
        assert col in ddl_cols, f"{col} missing from ACTION_CONTEXT_DDL"
    assert "structural_lbs BIGINT" in ACTION_CONTEXT_DDL


def test_ghost_gk_spread_renamed_to_density_spread() -> None:
    assert "ghost_gk_density_spread" in RESULT_COLUMNS
    assert "ghost_gk_spread" not in RESULT_COLUMNS
    assert "ghost_gk_density_spread DOUBLE" in ACTION_CONTEXT_DDL
    assert "ghost_gk_spread DOUBLE" not in ACTION_CONTEXT_DDL
```

- [ ] **Step 2: Run to verify failure**

Run: `uv run pytest src/tests/action_context/test_schema.py -k "new_ac_fields or renamed" -v`
Expected: FAIL (columns not yet in schema).

- [ ] **Step 3: Edit RESULT_COLUMNS**

In `src/analytics/action_context/schema.py`, replace the ghost block:
```python
    # Ghost-GK (3)
    "ghost_gk_x",
    "ghost_gk_y",
    "ghost_gk_spread",
```
with:
```python
    # Ghost-GK (3) — spread renamed to density_spread (silly-kicks 4.14.0; served value = boosted mean)
    "ghost_gk_x",
    "ghost_gk_y",
    "ghost_gk_density_spread",
    # Structural pass (TF-45; Karakus & Arkadas 2026) (3)
    "structural_lbs",
    "structural_sgm",
    "structural_sdi",
    # Player influence (silly-kicks add_player_influence) (7)
    "actor_reachable_area_m2",
    "off_ball_xt_team",
    "off_ball_xt_opponent",
    "off_ball_xt_diff",
    "reachable_area_team",
    "reachable_area_opponent",
    "reachable_area_diff",
    # xCrossAttempt (silly-kicks 4.18.0 bundled public model) (1)
    "xcross_attempt",
```
Update the header comment (line 17) feature count from `features (76)` to `features (87)` and total `= 111` to `= 122`.

- [ ] **Step 4: Edit ACTION_CONTEXT_DDL**

In the same file, replace:
```python
    "ghost_gk_x DOUBLE, ghost_gk_y DOUBLE, ghost_gk_spread DOUBLE, "
```
with:
```python
    "ghost_gk_x DOUBLE, ghost_gk_y DOUBLE, ghost_gk_density_spread DOUBLE, "
    "structural_lbs BIGINT, structural_sgm DOUBLE, structural_sdi DOUBLE, "
    "actor_reachable_area_m2 DOUBLE, off_ball_xt_team DOUBLE, off_ball_xt_opponent DOUBLE, "
    "off_ball_xt_diff DOUBLE, reachable_area_team DOUBLE, reachable_area_opponent DOUBLE, "
    "reachable_area_diff DOUBLE, xcross_attempt DOUBLE, "
```

- [ ] **Step 5: Run schema tests**

Run: `uv run pytest src/tests/action_context/test_schema.py -v`
Expected: PASS (incl. `test_ddl_matches_result_columns` — order parity holds).

---

## Task 5: Enrich — wire 3 aggregators + ghost rename (tracking path)

**Files:**
- Modify: `src/analytics/action_context/enrich.py` (`_enrich_tracking_match` ~159-374; imports ~171-202)

- [ ] **Step 1: Add the three aggregators to the tracking import block**

In `_enrich_tracking_match`, add to the `from silly_kicks.tracking import (...)` block:
```python
        add_structural_pass,
        add_xcross_attempt,
```
and add below the `add_ghost_gk` import line (`from silly_kicks.tracking.features import add_ghost_gk`):
```python
    from silly_kicks.tracking.features import add_player_influence
```

- [ ] **Step 2: Rename the ghost-GK spread output**

silly-kicks 4.14.0 emits `ghost_gk_density_spread` directly from `add_ghost_gk`, so no manual rename is needed in the call — but verify the column name flows through. After the `add_ghost_gk(...)` call (Step 12b, ~304-312), add an assertion-free confirmation comment and ensure no code references `ghost_gk_spread`. Run after Step 4:
```bash
git grep -n "ghost_gk_spread" -- src/analytics/
```
Expected: no matches in `src/analytics/` after Task 4+5.

- [ ] **Step 3: Insert the three new enrichment steps**

In `_enrich_tracking_match`, immediately **after** the xShotOccurrence step (Step 21, the `add_xshot_occurrence(...)` block ~365-367) and **before** the provenance assignment (`out["pitch_control_method"] = "spearman"`), insert:
```python
    # Step 22: Structural-pass primitives (TF-45; Karakus & Arkadas 2026, arXiv:2603.28916).
    # No xt / no pitch control. NaN for non-pass/non-cross + non-possessing-team actions.
    out = add_structural_pass(out, tracking_df, links=links, home_team_id=home_team_id)

    # Step 23: Player influence (xt positional; shared pitch-control cache; spearman = velocity-aware).
    out = add_player_influence(
        out,
        tracking_df,
        xt,
        links=links,
        home_team_id=home_team_id,
        method="spearman",
        pitch_control_cache=pc_cache,
    )

    # Step 24: xCrossAttempt (bundled "default" public model; no network; shared cache).
    # NaN for non-possessing-team action at the linked frame. actions_for_context supplies score_diff.
    out = add_xcross_attempt(
        out,
        tracking_df,
        model=None,
        links=links,
        home_team_id=home_team_id,
        actions_for_context=actions_df,
        pitch_control_cache=pc_cache,
    )
```

- [ ] **Step 4: Type/lint check the module**

Run: `uv run ruff check src/analytics/action_context/enrich.py && uv run pyright src/analytics/action_context/enrich.py`
Expected: no errors.

(Behavioral verification is via the golden recompute in Task 11.)

---

## Task 6: Enrich — SB360 path additions

**Files:**
- Modify: `src/analytics/action_context/enrich.py` (`_enrich_sb360_match` ~377-464)
- Test: `src/tests/action_context/test_sb360_coverage.py`

- [ ] **Step 1: Add imports to the SB360 import block**

In `_enrich_sb360_match`, add `add_structural_pass, add_xcross_attempt` to the `from silly_kicks.tracking import (...)` block and `add_player_influence` to the `from silly_kicks.tracking.features import (...)` line.

- [ ] **Step 2: Insert the three steps (voronoi where pitch-control is needed)**

After the `add_xshot_occurrence(...)` call (~457) and before the provenance assignment, insert:
```python
    # Structural-pass (single-frame supportable; no pitch control).
    out = add_structural_pass(out, frames, links=links, home_team_id=home_team_id)
    # Player influence — voronoi (freeze-frames have no velocity; spearman returns all-NaN).
    out = add_player_influence(
        out, frames, xt, links=links, home_team_id=home_team_id, method="voronoi"
    )
    # xCrossAttempt — bundled model; honest NULL where velocity features are unavailable.
    out = add_xcross_attempt(
        out, frames, model=None, links=links, home_team_id=home_team_id, actions_for_context=out
    )
```

> `actions_for_context=out` (the enriched action rows) on the SB360 path deliberately mirrors the
> existing SB360 `add_ghost_gk` call (`enrich.py:443`), which already uses `out` rather than a separate
> `actions_df`. The tracking path uses `actions_for_context=actions_df` (Task 5) per its existing
> `add_ghost_gk` convention (`enrich.py:310`). This asymmetry is the established pattern, not a
> copy-paste artifact.

- [ ] **Step 3: Update test_sb360_coverage.py for the rename + new columns**

In `src/tests/action_context/test_sb360_coverage.py`, replace any `ghost_gk_spread` reference with `ghost_gk_density_spread`. Add an assertion that the SB360 output schema includes the 11 new columns (they may be NaN, but must be present). Run to find current ghost refs:
```bash
git grep -n "ghost_gk_spread" -- src/tests/action_context/test_sb360_coverage.py
```

- [ ] **Step 4: Run SB360 coverage test**

Run: `uv run pytest src/tests/action_context/test_sb360_coverage.py -v`
Expected: PASS.

---

## Task 6B: Enrich-contract tests (set-equality + NaN contracts) — spec §9, golden-independent

> Why this exists (review M1): the golden recompute *can* catch an emit-rename (it surfaces as an
> all-NaN column vs the frozen golden) — but only until someone regenerates the golden, at which point
> the drift is silently re-baselined. These tests fail red **without** the golden, so a silly-kicks
> emit rename or a NaN-contract regression is caught even at regen time. They reuse `_recompute()`
> (the real `run_work_unit` → `enrich_batch` chain) already defined in `test_mini_golden.py`.

**Files:**
- Modify: `src/tests/action_context/test_mini_golden.py` (add tests; reuse `_recompute`)

- [ ] **Step 1: Add the emit + NaN-contract test**

Append to `src/tests/action_context/test_mini_golden.py`:
```python
_NEW_PLAYER_INFLUENCE = [
    "actor_reachable_area_m2", "off_ball_xt_team", "off_ball_xt_opponent",
    "off_ball_xt_diff", "reachable_area_team", "reachable_area_opponent", "reachable_area_diff",
]
_NEW_STRUCTURAL = ["structural_lbs", "structural_sgm", "structural_sdi"]
_PASS_OR_CROSS = {"pass", "cross"}


def test_new_ac_fields_emit_and_nan_contracts() -> None:
    """Golden-independent guards for the 11 new columns (spec §9).

    - Emit-drift: xcross_attempt + the 7 player-influence columns populate on the possessing-team
      tracking slice, so an upstream emit rename (column drops out of the enrich output and
      build_output fills it all-NaN) fails this RED without needing the frozen golden.
    - NaN contract: structural_* is NaN on every non-pass/cross action (silly-kicks contract).
    """
    result = _recompute()

    # All 11 declared columns must be present (build_output projects RESULT_COLUMNS).
    for col in ["xcross_attempt", *_NEW_PLAYER_INFLUENCE, *_NEW_STRUCTURAL]:
        assert col in result.columns, f"{col} missing from enrich output"

    # Emit-drift guard: these populate for the possessing team on the IDSSE mini slice.
    for col in ["xcross_attempt", *_NEW_PLAYER_INFLUENCE]:
        assert result[col].notna().any(), f"{col} all-NaN — aggregator not wired or emit renamed upstream"

    # Structural NaN contract: non-NaN only on pass/cross rows.
    non_pass = ~result["type_name"].isin(_PASS_OR_CROSS)
    for col in _NEW_STRUCTURAL:
        assert result.loc[non_pass, col].isna().all(), f"{col} must be NaN on non-pass/cross actions"
    # If the slice has any pass/cross, structural must populate on at least one (emit-drift guard).
    if result["type_name"].isin(_PASS_OR_CROSS).any():
        for col in _NEW_STRUCTURAL:
            assert result.loc[~non_pass, col].notna().any(), f"{col} all-NaN on pass/cross — emit drift?"
```

- [ ] **Step 2: Run ONLY the new contract test (golden-comparison test is red until Task 12 regen)**

Run: `uv run pytest src/tests/action_context/test_mini_golden.py -k "emit_and_nan_contracts" -v`
Expected: PASS. (The existing `test_mini_golden_recompute_matches_*` will FAIL until the golden is regenerated in Task 12 — that is expected; do not run it here.)

> Note: the xcross "NaN for a non-possessing-team action" contract is silly-kicks' own guarantee
> (covered by its test suite); the populated-check above guards OUR wiring. If the mini slice happens
> to contain no pass/cross action, the structural emit-drift guard is vacuous — `build_ac1_mini_golden`
> picks the slice, so confirm during Task 12 that the slice contains ≥1 pass/cross (it does on
> `J03WMXmini_p1`).

---

## Task 7: Dtype guard + TDD-gated GS coercion drop

**Files:**
- Modify: `src/analytics/action_context/pipeline.py` (work-unit entry; the `_coerce_gradientsports_frame_ids_to_native_str` call at :139)
- Modify (maybe): `src/analytics/action_context/convert.py:513` (the coercion helper)
- Modify (maybe): `src/tests/test_gradientsports_roster_dicts.py:82+`
- Test: new `src/tests/action_context/test_id_dtype_guard.py`

- [ ] **Step 1: Add the validate_id_dtypes guard (additive, always)**

In `pipeline.py`, at the tracking work-unit entry (where actions + frames are assembled before `_enrich_tracking_match`), add a loud pre-flight:
```python
    from silly_kicks.tracking import validate_id_dtypes

    validate_id_dtypes(actions, frames, home_team_id=home_team_id, on_mismatch="raise")
```
Place it AFTER `_resolve_enrichment_identity` (actions carry native ids) and AFTER any GS frame coercion currently applied, so it validates the actual shapes entering enrichment.

- [ ] **Step 2: Write the guard test**

Create `src/tests/action_context/test_id_dtype_guard.py`:
```python
from __future__ import annotations

import pandas as pd
import pytest


def test_validate_id_dtypes_raises_on_mismatch() -> None:
    from silly_kicks.tracking import validate_id_dtypes

    actions = pd.DataFrame({"action_id": [1], "team_id": [366], "player_id": [11], "game_id": ["g"]})
    frames = pd.DataFrame(
        {"game_id": ["g"], "period_id": [1], "frame_id": [1], "team_id": ["366"], "player_id": ["11"],
         "x": [0.0], "y": [0.0], "is_ball": [False]}
    )
    with pytest.raises(Exception):  # noqa: B017 — library raises its own diagnosis type
        validate_id_dtypes(actions, frames, home_team_id="366", on_mismatch="raise")
```
Run: `uv run pytest src/tests/action_context/test_id_dtype_guard.py -v` — Expected: PASS (proves the guard is wired + loud).

- [ ] **Step 3: Red-first GS Int64 resolution test (decides drop vs keep)**

Create a test that runs the GS enrich path with **Int64** frame ids (coercion bypassed) through carrier → possession → DAS + actor/opponent and asserts nonzero resolution under 4.19.1. Use the existing GS synthetic fixture machinery (`tests/datasets/tracking/gradientsports` parity in silly-kicks, or the lakehouse GS fixtures). Concretely, in a new `src/tests/action_context/test_gs_int64_seam_coverage.py`, build a minimal GS frames frame with `team_id`/`player_id` as `Int64`, run `infer_ball_carrier` → `derive_team_in_possession` → `add_das`, and assert `team_in_possession` is non-null on alive frames and `das_team` resolves (not all-NaN). Run it:
```bash
uv run pytest src/tests/action_context/test_gs_int64_seam_coverage.py -v
```

- [ ] **Step 4: Branch on the result**

- **If GREEN** (Int64 resolves end-to-end): remove `_coerce_gradientsports_frame_ids_to_native_str` (`convert.py:513`) and its call site (`pipeline.py:139`); update `src/tests/test_gradientsports_roster_dicts.py:82+` (drop the coercion-behaviour assertions, replace with the seam-coverage assertion). Keep the Step 1 guard.
- **If RED** (Int64 breaks resolution): **KEEP** the coercion unchanged. Keep only the Step 1 guard. Add a one-line comment at `convert.py:513` noting "4.19.1 seam coercion does NOT cover infer_ball_carrier/derive_team_in_possession — coercion retained; silly-kicks follow-up filed." Record the follow-up in the PR description.

- [ ] **Step 5: Run the affected tests**

Run: `uv run pytest src/tests/test_gradientsports_roster_dicts.py src/tests/action_context/test_id_dtype_guard.py -v`
Expected: PASS for whichever branch was taken.

---

## Task 8: dbt staging — add 11 casts + ghost rename

**Files:**
- Modify: `dbt_project/models/staging/action_context/stg_action_context__values.sql` (~169-177)

- [ ] **Step 1: Edit the ghost cast + add new casts**

Replace:
```sql
        -- Ghost GK
        cast(ghost_gk_x as double) as ghost_gk_x,
        cast(ghost_gk_y as double) as ghost_gk_y,
        cast(ghost_gk_spread as double) as ghost_gk_spread,
```
with:
```sql
        -- Ghost GK (spread renamed to density_spread; silly-kicks 4.14.0)
        cast(ghost_gk_x as double) as ghost_gk_x,
        cast(ghost_gk_y as double) as ghost_gk_y,
        cast(ghost_gk_density_spread as double) as ghost_gk_density_spread,
        -- Structural pass (TF-45)
        cast(structural_lbs as bigint) as structural_lbs,
        cast(structural_sgm as double) as structural_sgm,
        cast(structural_sdi as double) as structural_sdi,
        -- Player influence
        cast(actor_reachable_area_m2 as double) as actor_reachable_area_m2,
        cast(off_ball_xt_team as double) as off_ball_xt_team,
        cast(off_ball_xt_opponent as double) as off_ball_xt_opponent,
        cast(off_ball_xt_diff as double) as off_ball_xt_diff,
        cast(reachable_area_team as double) as reachable_area_team,
        cast(reachable_area_opponent as double) as reachable_area_opponent,
        cast(reachable_area_diff as double) as reachable_area_diff,
        -- xCrossAttempt
        cast(xcross_attempt as double) as xcross_attempt,
```

- [ ] **Step 2: dbt parse**

Run: `cd dbt_project && uv run --no-sync dbt parse --profiles-dir . ; cd ..`
Expected: parse succeeds (no SQL syntax error).

---

## Task 9: dbt mart — add 11 columns (both CTEs) + ghost rename

**Files:**
- Modify: `dbt_project/models/marts/fct_action_context.sql` (`action_raw` ~124-126; `final` ~267-269)

- [ ] **Step 1: Edit the `action_raw` CTE select**

Replace (lines ~124-129):
```sql
        ghost_gk_x,
        ghost_gk_y,
        ghost_gk_spread,
        xshot_occurrence,
        pitch_control_method,
        ghost_gk_method
```
with:
```sql
        ghost_gk_x,
        ghost_gk_y,
        ghost_gk_density_spread,
        structural_lbs,
        structural_sgm,
        structural_sdi,
        actor_reachable_area_m2,
        off_ball_xt_team,
        off_ball_xt_opponent,
        off_ball_xt_diff,
        reachable_area_team,
        reachable_area_opponent,
        reachable_area_diff,
        xcross_attempt,
        xshot_occurrence,
        pitch_control_method,
        ghost_gk_method
```

- [ ] **Step 2: Edit the `final` select**

Apply the IDENTICAL replacement to the `final` CTE select (lines ~267-272 — same old/new block as Step 1).

- [ ] **Step 3: dbt parse + compile**

Run: `cd dbt_project && uv run --no-sync dbt parse --profiles-dir . ; cd ..`
Expected: parse succeeds.

---

## Task 10: dbt contract yml — add 11 column entries + ghost rename

**Files:**
- Modify: `dbt_project/models/marts/_marts__models.yml` (`fct_action_context` block ~5121-5126)

- [ ] **Step 1: Edit the ghost entry + add 11 entries**

Replace:
```yaml
      - name: ghost_gk_x
        data_type: double
      - name: ghost_gk_y
        data_type: double
      - name: ghost_gk_spread
        data_type: double
```
with:
```yaml
      - name: ghost_gk_x
        data_type: double
      - name: ghost_gk_y
        data_type: double
      - name: ghost_gk_density_spread
        data_type: double
      - name: structural_lbs
        data_type: bigint
      - name: structural_sgm
        data_type: double
      - name: structural_sdi
        data_type: double
      - name: actor_reachable_area_m2
        data_type: double
      - name: off_ball_xt_team
        data_type: double
      - name: off_ball_xt_opponent
        data_type: double
      - name: off_ball_xt_diff
        data_type: double
      - name: reachable_area_team
        data_type: double
      - name: reachable_area_opponent
        data_type: double
      - name: reachable_area_diff
        data_type: double
      - name: xcross_attempt
        data_type: double
```

- [ ] **Step 2: dbt parse (contract sanity)**

Run: `cd dbt_project && uv run --no-sync dbt parse --profiles-dir . ; cd ..`
Expected: parse succeeds; the contract column list now matches the `final` select from Task 9.

---

## Task 11: Migrations (two files, flat dir, operator-applied)

**Files:**
- Create: `scripts/migrations/2026-06-08-add-ac-structural-xcross-playerinfluence.sql`
- Create: `scripts/migrations/2026-06-08-rename-ghost-gk-spread.sql`

> Per the corrected contract (memory `reference_bronze_migration_autoapply_gap`): there is NO CI auto-apply. These are applied by the operator at merge (see spec §5.2). Do NOT add them to any workflow.

- [ ] **Step 1: Write the ADD COLUMNS migration**

Create `scripts/migrations/2026-06-08-add-ac-structural-xcross-playerinfluence.sql`:
```sql
-- AC-1 silly-kicks 4.19.1: add structural-pass, player-influence, and xCross columns to
-- bronze.spadl_action_context. Emitted by add_structural_pass / add_player_influence /
-- add_xcross_attempt in the enrichment chain; NULL until the next compute run.
--
-- Operator-applied (no CI auto-apply). Idempotent: the runner skips ADD COLUMNS when the
-- leading column already exists (DESCRIBE pre-check); Delta applies the column list atomically.

ALTER TABLE soccer_analytics.bronze.spadl_action_context
  ADD COLUMNS (
    structural_lbs BIGINT,
    structural_sgm DOUBLE,
    structural_sdi DOUBLE,
    actor_reachable_area_m2 DOUBLE,
    off_ball_xt_team DOUBLE,
    off_ball_xt_opponent DOUBLE,
    off_ball_xt_diff DOUBLE,
    reachable_area_team DOUBLE,
    reachable_area_opponent DOUBLE,
    reachable_area_diff DOUBLE,
    xcross_attempt DOUBLE
  );
```

- [ ] **Step 2: Write the RENAME migration**

Create `scripts/migrations/2026-06-08-rename-ghost-gk-spread.sql`:
```sql
-- AC-1 silly-kicks 4.14.0: rename ghost_gk_spread -> ghost_gk_density_spread on
-- bronze.spadl_action_context (served value is now the boosted-HGBR mean; the spread is the
-- conditional-density dispersion).
--
-- RUN ONCE. Operator-applied (no CI auto-apply). NOT idempotent — the runner has no RENAME
-- idempotency. Before running, confirm the source column still exists:
--   DESCRIBE soccer_analytics.bronze.spadl_action_context;  -- expect ghost_gk_spread present
-- Delta column-mapping is a one-way protocol bump (minReader=2/minWriter=5) — irreversible
-- on this table (see ADR-042).

ALTER TABLE soccer_analytics.bronze.spadl_action_context SET TBLPROPERTIES (
  'delta.columnMapping.mode' = 'name',
  'delta.minReaderVersion' = '2',
  'delta.minWriterVersion' = '5'
);

ALTER TABLE soccer_analytics.bronze.spadl_action_context
  RENAME COLUMN ghost_gk_spread TO ghost_gk_density_spread;
```

(These are NOT applied during this PR's coding — they run in the operator runbook at merge, spec §5.2.)

---

## Task 11B: Symmetric `create_synced_table.py` CLI (review R-2)

> Why (review R-2): `delete_synced_table.py <name>` is a clean one-liner, but the create side only
> exists as the internal `_create_synced_table` helper inside the bulk `migrate_synced_tables.py`
> (whose `main()` migrates all 41). The §5.2 runbook needs a symmetric, single-table create command so
> the operator isn't hand-writing `python -c` against an internal helper. A freshly SDK-created
> TRIGGERED synced table **auto-starts its initial sync** (`synced_table_lifecycle.py:38-41`), so it
> populates without an explicit refresh.

**Files:**
- Create: `scripts/create_synced_table.py`
- Test: `src/tests/test_create_synced_table_cli.py` (import/smoke, mirrors any existing delete-CLI test)

- [ ] **Step 1: Write the CLI (mirror `delete_synced_table.py`)**

Create `scripts/create_synced_table.py`:
```python
#!/usr/bin/env python3
"""Create a single Databricks synced table from its canonical SYNCED_TABLES config.

Symmetric to scripts/delete_synced_table.py. The config (source mart, PK, scheduling policy) is
resolved from ingestion.refresh_synced_tables.SYNCED_TABLES (single source of truth). A freshly
created TRIGGERED synced table auto-starts its initial sync, so this also waits until it is online.

Usage:
    uv run --extra sdk python scripts/create_synced_table.py fct_action_context_synced
"""

from __future__ import annotations

import argparse
import re
import sys

IDENTIFIER_RE = re.compile(r"^[a-zA-Z_][a-zA-Z0-9_]*$")
CATALOG = "soccer_analytics"
SCHEMA = "dev_gold"


def main() -> int:
    parser = argparse.ArgumentParser(description="Create a single synced table from SYNCED_TABLES")
    parser.add_argument("table_name", help="Synced table name, e.g. fct_action_context_synced")
    args = parser.parse_args()
    table_name: str = args.table_name

    if not IDENTIFIER_RE.match(table_name):
        print(f"ERROR: Invalid table name '{table_name}': must match {IDENTIFIER_RE.pattern}")
        return 1

    from databricks.sdk import WorkspaceClient

    from ingestion.refresh_synced_tables import SYNCED_TABLES
    from ingestion.synced_table_lifecycle import SdkWriterAdapter, wait_until_online

    cfg = next((c for c in SYNCED_TABLES if c.name == table_name), None)
    if cfg is None:
        known = ", ".join(sorted(c.name for c in SYNCED_TABLES))
        print(f"ERROR: '{table_name}' not in SYNCED_TABLES. Known: {known}")
        return 1

    ws = WorkspaceClient()
    full_name = f"{CATALOG}.{SCHEMA}.{table_name}"
    print(f"[1/2] Creating synced table: {full_name}")
    SdkWriterAdapter(ws).create_synced_table(cfg, CATALOG, SCHEMA)
    print("  OK — create requested; initial sync auto-started.")
    print(f"[2/2] Waiting until online: {full_name}")
    wait_until_online(full_name, timeout_s=1200, poll_interval_s=15)
    print("  OK — synced table online.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
```

> Confirm the `SyncedTableConfig` name field is `.name` (see `refresh_synced_tables.py:72`); adapt the
> `c.name` lookup if the field differs.

- [ ] **Step 2: Smoke test (import + config resolution, no live SDK)**

Create `src/tests/test_create_synced_table_cli.py`:
```python
from __future__ import annotations


def test_create_cli_resolves_known_config() -> None:
    from ingestion.refresh_synced_tables import SYNCED_TABLES

    names = {c.name for c in SYNCED_TABLES}
    assert "fct_action_context_synced" in names


def test_create_cli_module_imports() -> None:
    import importlib.util
    from pathlib import Path

    spec = importlib.util.spec_from_file_location("create_synced_table", Path("scripts/create_synced_table.py"))
    assert spec and spec.loader
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)  # import-time only; main() not invoked
    assert hasattr(mod, "main")
```

- [ ] **Step 3: Run + lint**

Run: `uv run ruff check scripts/create_synced_table.py && uv run pytest src/tests/test_create_synced_table_cli.py -v`
Expected: PASS.

---

## Task 12: Regenerate both goldens + oracle_map + HF card

**Files:**
- Modify: `src/tests/action_context/oracle_map.py` (~23, ~83-85)
- Regenerate: `src/tests/fixtures/action_context/idsse/J03WMX_p1/golden.parquet`, `.../J03WMXmini_p1/golden.parquet`
- Modify: `docs/huggingface/dataset-cards/spadl-action-context.md`

- [ ] **Step 1: Update oracle_map ghost rename + new INVARIANT_ONLY entries**

In `src/analytics/.../oracle_map.py` (test dir), in the module docstring (~23) replace `ghost_gk_*` wording referencing spread if needed, and in `INVARIANT_ONLY` (~83-85) replace:
```python
    "ghost_gk_x": ("float", 0.0, 105.0),
    "ghost_gk_y": ("float", 0.0, 68.0),
    "ghost_gk_spread": ("float", 0.0, None),
```
with:
```python
    "ghost_gk_x": ("float", 0.0, 105.0),
    "ghost_gk_y": ("float", 0.0, 68.0),
    "ghost_gk_density_spread": ("float", 0.0, None),
    # Structural pass (TF-45; Karakus & Arkadas 2026): no legacy oracle. Range-check only.
    "structural_lbs": ("int", 0.0, None),
    "structural_sgm": ("float", 0.0, None),
    "structural_sdi": ("float", 0.0, None),
    # Player influence: areas are non-negative; diffs may be negative (team - opponent).
    "actor_reachable_area_m2": ("float", 0.0, None),
    "off_ball_xt_team": ("float", 0.0, None),
    "off_ball_xt_opponent": ("float", 0.0, None),
    "off_ball_xt_diff": ("float", None, None),
    "reachable_area_team": ("float", 0.0, None),
    "reachable_area_opponent": ("float", 0.0, None),
    "reachable_area_diff": ("float", None, None),
    # xCrossAttempt (silly-kicks 4.18.0): probability in [0,1]. Range-check only.
    "xcross_attempt": ("float", 0.0, 1.0),
```

> `structural_lbs` is bigint and NULL on most rows (non-pass/cross). The differential range-check
> `.dropna()`s before checking (`test_differential.py:76`), so an `("int", 0.0, None)` invariant
> tolerates `<NA>` — same precedent as `elastic_frame_id` (NULL on event-only rows). No special-casing
> needed.

- [ ] **Step 1b: Fixture pre-check (fail loud if the full-golden fixture is missing)**

Run:
```bash
test -f src/tests/fixtures/action_context/idsse/J03WMX_p1/golden.parquet && \
  ls src/tests/fixtures/action_context/idsse/J03WMX_p1/ || \
  { echo "MISSING J03WMX_p1 fixture — full golden cannot be regenerated locally"; exit 1; }
```
Expected: the fixture dir lists (frames/actions/xt/meta + golden). If missing, STOP and obtain the fixture before continuing — the mini-golden remains the CI gate, but the full golden needs this fixture.

- [ ] **Step 2: Regenerate the full golden**

Run: `uv run python scripts/build_ac1_full_golden.py`
Expected: writes `.../J03WMX_p1/golden.parquet`. ALWAYS review the column diff before trusting (capture-before-cleanup): confirm the 11 new columns are present, `ghost_gk_density_spread` replaces `ghost_gk_spread`, `xcross_attempt` is non-NaN on possessing-team passes, and `structural_*` non-NaN on at least one pass.

- [ ] **Step 3: Regenerate the mini golden**

Run: `uv run python scripts/build_ac1_mini_golden.py`
Expected: writes `.../J03WMXmini_p1/golden.parquet` with the same new columns.

- [ ] **Step 4: Run the golden + differential tests**

Run: `uv run pytest src/tests/action_context/test_mini_golden.py src/tests/action_context/test_differential.py -v`
Expected: PASS (recompute matches the regenerated golden; new columns range-check via oracle_map).

- [ ] **Step 5: Update the HF dataset card**

In `docs/huggingface/dataset-cards/spadl-action-context.md`, rename `ghost_gk_spread` → `ghost_gk_density_spread` and document the 11 new columns (structural-pass, player-influence, xCross) in the column list. Note that the dataset republish itself rides with the deferred full recompute (PR-1 corrects only the in-repo card).

- [ ] **Step 6: Confirm HF card parity test stays green**

Run: `uv run pytest src/tests/test_hf_publish_parity.py -v`
Expected: PASS (filename==repo-basename + inventory parity; it does not assert live column-level parity).

---

## Task 13: ADR-042 + ARCHITECTURE.md Appendix D + NOTICE + CLAUDE.md fix

**Files:**
- Create: `docs/superpowers/adrs/ADR-042-silly-kicks-4-19-1-adoption.md`
- Modify: `ARCHITECTURE.md` (§8 "D. Academic References")
- Modify: `src/tests/test_architecture_md_appendix.py` (`expected_authors`)
- Modify: `NOTICE` (if the structural-pass citation must mirror)
- Modify: `CLAUDE.md:216`

- [ ] **Step 1: Write ADR-042**

Create `docs/superpowers/adrs/ADR-042-silly-kicks-4-19-1-adoption.md` using `docs/superpowers/adrs/ADR-TEMPLATE.md`. Context/Decision/Consequences must cover: ghost-GK boosted-mean serve + `ghost_gk_density_spread` rename (Hyrum value flip ≈4.65 m mode → ≈1.07 m mean; verified no runtime/mart/UI consumers); 4.15.0 dtype-contract value shifts + the §3.4 handshake decision (drop-vs-keep outcome from Task 7); the 11 new AC fields; the Delta column-mapping protocol bump as irreversible on `bronze.spadl_action_context`; and the next-cycle watchdog re-check note for the heavier chain.

- [ ] **Step 2: Add the author to ARCHITECTURE.md Appendix D**

In `ARCHITECTURE.md` § 8 "D. Academic References", add (mirror NOTICE exactly, ASCII):
```
- Karakus, O., & Arkadas, H. (2026). Structural Pass Analysis in Football. arXiv:2603.28916. (Line Bypass Score, Space Gain Metric, Structural Disruption Index — fct_action_context structural_lbs/sgm/sdi.)
```

- [ ] **Step 3: Extend expected_authors + run the strict test**

In `src/tests/test_architecture_md_appendix.py`, add `"Karakus"` (and `"Arkadas"` if the test checks both surnames) to the `expected_authors` list. Run:
```bash
uv run pytest src/tests/test_architecture_md_appendix.py -v
```
Expected: PASS. (Strict gate — caused the D56 audit; spelling must match Appendix D + NOTICE.)

- [ ] **Step 4: NOTICE check**

Confirm `NOTICE` already carries the Karakus & Arkadas 2026 structural-pass citation (it ships in silly-kicks; the lakehouse `NOTICE` may need the lakehouse-side reference). If absent, add it mirroring the silly-kicks `NOTICE:351` line.

- [ ] **Step 5: Fix the stale CLAUDE.md auto-apply bullet (spec §12)**

In `CLAUDE.md:216`, replace the stale "Bronze migrations … auto-applied at live-build CI time" bullet with the corrected text from spec §12 (operator-applied; no CI auto-apply; apply-with-merge; runner runs any DDL but only ADD COLUMNS idempotent; RENAME = run-once; destructive ops operator-driven; verify live).

---

## Task 14: Full shift-left gate + single commit (GATED ON APPROVAL)

**Files:** all changed.

- [ ] **Step 1: Run the full local gate**

Run:
```bash
uv run ruff check src/ scripts/
uv run ruff format --check src/ scripts/
uv run pyright src/
uv run pytest src/tests/action_context -v
uv run pytest src/tests/test_schema.py src/tests/test_sk3_mig_b_orchestrator_invariants.py src/tests/test_terraform_env_dep_parity.py src/tests/test_architecture_md_appendix.py src/tests/test_hf_publish_parity.py -v
```
Expected: all green. Fix any violation before proceeding.

- [ ] **Step 2: Review the full diff**

Run: `git status && git diff --stat`
Confirm the change set matches this plan (schema, enrich, dbt ×3, migrations ×2, oracle_map, goldens ×2, ADR, ARCHITECTURE, CLAUDE.md, pins, HF card, dtype guard).

- [ ] **Step 3: STOP — request explicit commit approval**

Do NOT commit. Present the diff summary and ask the user for explicit approval to commit (per CLAUDE.md). Only after approval:

```bash
git add -A
git commit -F tmp/commit-msg.txt
```
with a single message (write to `tmp/commit-msg.txt` first), e.g.:
```
feat(action-context): silly-kicks 4.19.1 + structural/xcross/player-influence fields

Force silly-kicks 4.19.1 everywhere (pyproject/uv.lock/wheel 0.5.23/trainers/
orchestrators/terraform/CI). Add 11 AC columns (structural_lbs/sgm/sdi,
xcross_attempt, player-influence ×7); rename ghost_gk_spread -> ghost_gk_density_spread
(boosted-mean serve); add validate_id_dtypes guard. Regenerate both goldens.
ADR-042 + Appendix D (Karakus & Arkadas 2026). Operator runbook for migrations +
synced-table rebuild in the PR description (spec §5.2). No live recompute (deferred).

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>
```

- [ ] **Step 4: Post-merge operator runbook (NOT part of the commit — execute at merge)**

Per spec §5.2 (exact commands):
1. `uv run --extra sdk python scripts/migrations/_runner.py scripts/migrations/2026-06-08-add-ac-structural-xcross-playerinfluence.sql`
2. `uv run --extra sdk python scripts/migrations/_runner.py scripts/migrations/2026-06-08-rename-ghost-gk-spread.sql`
3. `uv run --extra sdk python scripts/delete_synced_table.py fct_action_context_synced`
4. `cd dbt_project && uv run --no-sync dbt run --full-refresh --select stg_action_context__values fct_action_context --profiles-dir . ; cd ..` (verify `delta.enableChangeDataFeed=true` post-build; SET if absent)
5. `uv run --extra sdk python scripts/create_synced_table.py fct_action_context_synced` (Task 11B; auto-starts the initial sync + waits online — no separate refresh needed, `synced_table_lifecycle.py:38-41`)
6. `uv run --extra sdk python scripts/maintain_synced_tables.py --skip-refresh --skip-heal` (grants + 3 PG indexes; `--skip-refresh` because the fresh table already auto-synced in step 5, `--skip-heal` because the recreate is explicit)

This breaks the Lakebase app silently if skipped.

---

## Self-review notes (coverage)

- Spec §2 (inventory) → Tasks 4,5,6,8,9,10. §3 (compute) → 4,5,6,7. §4 (dbt) → 8,9,10. §5 (migrations+runbook) → 11, 11B (symmetric create CLI), + Task 14 Step 4. §6 (goldens) → 12. §7 (pins) → 1,2,3. §8 (ADR/citation) → 13. §9 (tests) → 4, **6B (emit set-equality + NaN contracts, golden-independent)**, 7, 12 + Task 14 gate. §12 (CLAUDE.md) → 13 Step 5.
- Review M1 → Task 6B; review R-2 → Task 11B + Task 14 Step 4 + spec §5.2 step 5.
- `structural_lbs` is bigint in every layer (DDL, staging cast, mart, yml, migration, oracle_map int). xCross/player-influence column names match the verified silly-kicks emit set exactly.
- No per-task commits; single approval-gated commit (Task 14).
