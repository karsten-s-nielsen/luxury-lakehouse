# Tracking-marts Stage-2 Window-Bounded Scoring (+ retry-enforcement fix) — Implementation Plan

> **For the implementer:** execute INLINE in the main session (this repo's standing rule [no-subagents-for-implementation] overrides the writing-plans default of subagent-driven execution; read-only research subagents are fine). Steps use checkbox (`- [ ]`) syntax. TDD throughout.
>
> **Commits:** ONE commit for the whole coherent change, fully tested, on explicit approval ([no-micro-commits] / [no-commit-no-approval]). Do NOT commit per task or per phase. The per-task "verify green" steps are checkpoints, not commit points.

**Goal:** Make tracking-marts Stage-2 scoring memory-bounded by window size (byte-identical to whole-unit scoring) so the 8 dense skillcorner units materialize under the serverless ~1 GB UDF-group cap, and fold in the retry-enforcement fix so the release leaves live `max_retries = 0`.

**Architecture:** Extend the ADR-089 haloed frame-window seam from Stage-1 (build) to Stage-2 (scoring): re-window the spilled built frames with a per-mart scoring halo, route each scored entity to its linked frame's window, and inject whole-unit invariants (GoalMap; gkdv ball-carrier) precomputed once over the unit so every window reproduces the whole-unit metric. Six marts; four are lakehouse-only, two (`gk_decision`, `gkdv`) gate on a silly-kicks `goal_map=` injection seam shipped by the parallel sk session.

**Tech Stack:** PySpark `mapInPandas` (ADR-045 transport), pandas scorers (ingestion + silly-kicks), serverless Databricks, pytest + pytest-benchmark.

**Spec:** `docs/superpowers/specs/2026-10-11-tracking-marts-stage2-window-scoring-design.md` (read it alongside this plan; the plan argues from it).

## Global Constraints

- **Python 3.10** (`>=3.10,<3.11`); line length 120; ruff rules E,W,F,I,N,UP,B,S,BLE,RUF; all public signatures typed; pyright basic over the CI target (NOT `pyright src/` — includes `hf_taipy_app/src/` + `sync_tf_env_pins`).
- **Byte-identity is the acceptance bar.** Windowed mart output == whole-unit output under the existing oracle (`assert_frame_equal` ≈ 1e-8). Any metric change is a blocker, not a deferral.
- **Serverless constraints:** ~1 GB UDF-group cap (800 MB working target), no broadcast/cache, lazy closure capture, frozen dataclasses for config, `pyarrow.parquet.read_table().to_pandas()` (never `pd.read_parquet`) on serverless-reachable paths.
- **sk dependency — SATISFIED.** `gk_decision` + `gkdv` windowing needs the sk `goal_map=` seam (spec §4.4); it SHIPPED in sk 4.132.0 (reviewed APPROVE) and the lakehouse floor is bumped + verified (Task E.0 done). Phase E proceeds after this spec + plan are approved. silly-kicks is READ-ONLY here.
- **Scoring halo per mart** sourced from sk params where applicable (off_ball `pre_seconds=1.5`) with a sentinel assertion (ADR-067 lockstep).
- **measure-before-optimize:** invoke `mad-scientist-skills:measure-before-optimize` before editing any benchmarked scorer; re-run `src/tests/test_benchmarks.py` (baselines `docs/performance-baselines.md`; budgets AGENTS.md) — no latency regression.
- **No wheel-consuming job run until post-merge main CI green.** The 8-unit re-run is a separate post-merge operator step under its own approval.

---

## File Structure

- `src/analytics/action_context/frame_windows.py` (MODIFY) — add scoring-window helpers (`score_halo_frames`, a built-frame window-assignment that reuses the `frame_id` ordinal with a scoring halo). Pure, no Spark.
- `src/ingestion/frame_window_dispatch.py` (MODIFY) — add a Stage-2 scoring-window dispatch (`windowed_score_sdf`) mirroring `windowed_build_sdf`, grouping by `(game_id, period_id, _window_id)` and injecting invariants.
- `src/ingestion/tracking_marts_dispatch.py` (MODIFY) — `build_stage2_mart_sdf` re-windows via the scoring seam; `make_mart_scorers` accepts injected `goal_map` (+ gkdv `carrier`); add `_TM_SCORE_WINDOW_FRAMES`; add a Stage-2a invariant-precompute.
- `src/ingestion/restdefense_writer.py` (MODIFY) — `compute_rest_defense_samples` gains a `goal_map=` param, forwarded to sk `compute_rest_defense(..., goal_map=...)`.
- `src/ingestion/gkdv_writer.py` (MODIFY, Phase E) — thread injected `goal_map` into the gkdv build path (needs the sk seam).
- `src/ingestion/gk_decision_writer.py` (MODIFY, Phase E) — thread injected `goal_map` into `score_gk_decision_reconstructed` (needs the sk seam).
- `src/tests/tracking_marts/test_stage2_window_scoring.py` (CREATE) — per-mart neutrality + halo-sufficiency + invariant-injection tests.
- `scripts/patch_job_retries.py` + `src/tests/test_job_retry_policy.py` (MODIFY) — Fix B (staged on `fix/patch-job-retries-absent-zero`).
- `pyproject.toml` + ~30 wheel consumers via `scripts/bump_wheel.py`; `uv.lock` + tf env pins via `scripts/sync_tf_env_pins.py`; sk floor sentinels (`_REQUIRED_SK_MIN`); `TODO.md`.

---

## Phase 0 — Preconditions & measurement (gates, no production code)

### Task 0.1: Branch + carry Fix B
- [ ] Fetch + `git pull --ff-only origin main`; confirm `main` = `885515a5` (or newer; re-verify alignment). Create the feature branch off main.
- [ ] Carry the two staged Fix B files from `fix/patch-job-retries-absent-zero` onto the branch (cherry-pick the diff or re-apply). Confirm `git diff` matches the staged change (spec §4.7). If the staged copy is gone, re-create from `project_tracking_marts_dense_unit_oom_fix` memory.
- [ ] Run `uv run pytest src/tests/test_job_retry_policy.py -v` → the 3 new guards + existing pass (7 total). This is a checkpoint, NOT a commit.

### Task 0.2: Fit-profile the Stage-2 memory (spec §4.6, §9 Q1)
- [ ] Read the ADR-089 fit-profile (`D:\Development\_reviews\2026-10-06-tracking-marts-fit-profile.md`) for the built-frame per-column byte accounting.
- [ ] From the spilled built-frame schema (`game_id/period_id/frame_id` + positions/velocities/smoothed/derived), compute, per mart, (i) the column subset the Stage-2a invariant precompute needs (positions/GK only) and (ii) the column subset each scorer reads. Estimate the Stage-2a pruned-whole-unit peak and the Stage-2b per-window peak for a dense skillcorner unit (~90k frames).
- [ ] If the offline estimate is within margin, set `_TM_SCORE_WINDOW_FRAMES` from (ii) targeting ≤ 800 MB. If the estimate is ambiguous, note that a scoped canary measurement on `1562415:1` is required (separate approval, post-infra) and leave the knob provisional pending that measurement.
- [ ] Write the fit-profile to `D:\Development\_reviews\2026-10-11-stage2-scoring-fit-profile.md` (capture-before-cleanup; echo the absolute path).
- [ ] **Gate:** if Stage-2a (pruned precompute) does NOT fit the densest unit, take the §9 Q2 fallback (Spark-groupby GoalMap + carried-forward-state windowed carrier) — surface to the owner before proceeding; do not silently redesign. NOTE (TM-PLAN-01 / S2W-SPEC-02): the GoalMap groupby-mean is order-independent, but the carried-forward carrier is a STATEFUL reimplementation of sk `infer_ball_carrier` (hysteresis γ=0.25) — if that fallback is taken it requires its OWN neutrality proof (carried-forward boundary state == whole-unit sequential), not an assumption; add that proof as an explicit task then.

---

## Phase A — Scoring-window seam core (pure, lakehouse-only)

### Task A.1: `score_halo_frames` (per-mart scoring reach)

**Files:**
- Modify: `src/analytics/action_context/frame_windows.py`
- Test: `src/tests/tracking_marts/test_stage2_window_scoring.py` (create)

**Interfaces:**
- Produces: `score_halo_frames(mart: str, provider: str) -> int` — scoring reach in frames. off_ball = `ceil(1.5 * provider_hz(provider))`; action_defensive / defensive_credit_attributions / rest_defense / gk_decision = `ceil(link_tolerance_s * provider_hz(provider))` (link tolerance from sk, default 0.2 s); gkdv = 0 (arms are single-frame). A module-level sentinel asserts the derived off_ball reach against sk `pre_seconds` and fails loud on drift.

- [ ] **Step 1: Write the failing test** (halo values per the spec §4.2 table):

```python
import math
from analytics.action_context.frame_windows import score_halo_frames, provider_hz


def test_score_halo_off_ball_exceeds_build_halo():
    # off_ball reaches 1.5 s pre-action; skillcorner 10 Hz -> 15, idsse 25 Hz -> 38.
    assert score_halo_frames("off_ball_runs", "skillcorner") == math.ceil(1.5 * provider_hz("skillcorner"))
    assert score_halo_frames("off_ball_runs", "skillcorner") == 15
    assert score_halo_frames("off_ball_runs", "idsse") == 38


def test_score_halo_gkdv_is_zero_frame_arms():
    assert score_halo_frames("gkdv_observations", "skillcorner") == 0


def test_score_halo_link_tolerance_marts():
    # action-linked marts need only the link tolerance neighbourhood.
    assert score_halo_frames("action_defensive", "skillcorner") == math.ceil(0.2 * provider_hz("skillcorner"))
```

- [ ] **Step 2: Run → FAIL** (`score_halo_frames` not defined). `uv run pytest src/tests/tracking_marts/test_stage2_window_scoring.py -k score_halo -v`.
- [ ] **Step 3: Implement** `score_halo_frames` in `frame_windows.py`, sourcing `1.5` from sk `RunValuationParams.pre_seconds` (import + assert) and `0.2` from the sk link-tolerance default, with a `_SCORE_HALO_SENTINEL` module-level assertion on the derived off_ball value.
- [ ] **Step 4: Run → PASS.**

### Task A.2: built-frame scoring-window assignment (pure Spark transform)

**Files:**
- Modify: `src/ingestion/frame_window_dispatch.py`
- Test: `src/tests/tracking_marts/test_stage2_window_scoring.py`

**Interfaces:**
- Consumes: `score_halo_frames` (A.1); the `assign_core_window_ids` / halo-replication mechanism (existing, keyed on `match_id`/`period`/`frame` for raw; here keyed on `game_id`/`period_id`/`frame_id` for built).
- Produces: `assign_score_windows(built_sdf, mart, provider, *, target_window_frames) -> built_sdf'` — adds `_window_id` + `_is_halo` on the `frame_id` dense ordinal with the mart's scoring halo; row count grows by the halo duplicates only.

- [ ] **Step 1: Write the failing test** — a built-frame fixture spanning ≥2 windows; assert every core frame appears once with `_is_halo=False`, boundary frames within `score_halo_frames` replicate into the neighbour window with `_is_halo=True`, and window ids come from the `frame_id` DISTINCT ordinal (not row index):

```python
def test_assign_score_windows_replicates_scoring_halo():
    from ingestion.frame_window_dispatch import assign_score_windows

    sdf = _built_fixture_two_windows(provider="skillcorner", frames=40, target=20)  # 2 windows of 20
    out = assign_score_windows(sdf, "off_ball_runs", "skillcorner", target_window_frames=20).toPandas()
    core = out[~out["_is_halo"]]
    assert core.groupby("frame_id").ngroups == 40  # every frame core exactly once
    halo = out[out["_is_halo"]]
    assert set(halo["_window_id"]) == {0, 1}  # both boundaries haloed
    # a frame <15 of the boundary is replicated; one 16 away is NOT
    assert 19 in set(halo["frame_id"]) and 24 in set(halo[halo["_window_id"] == 0]["frame_id"])
```

- [ ] **Step 2: Run → FAIL.**
- [ ] **Step 3: Implement** `assign_score_windows` reusing `assign_frame_windows`' ordinal/halo logic parameterised for built-frame keys (`game_id`/`period_id`/`frame_id`) and the per-mart halo from A.1.
- [ ] **Step 4: Run → PASS.**

### Task A.3: Stage-2a whole-unit invariant precompute (pure, column-pruned)

**Files:**
- Modify: `src/ingestion/tracking_marts_dispatch.py`
- Test: `src/tests/tracking_marts/test_stage2_window_scoring.py`

**Interfaces:**
- Produces: `precompute_unit_invariants(spill_dir, spark, *, need_carrier: bool) -> UnitInvariants` where `UnitInvariants` is a frozen dataclass `{goal_map: GoalMap, carrier: pd.DataFrame | None}`. Reads only the GK/position columns from the spill (column-pruned), runs sk `resolve_defended_goals` (+ `infer_ball_carrier` when `need_carrier`), returns small serializable values captured into the UDF closures. Peak bounded per the §4.6 fit-profile.

- [ ] **Step 1: Write the failing test** — on a built fixture, assert `precompute_unit_invariants` returns the SAME `goal_map`/`carrier` as running `resolve_defended_goals`/`infer_ball_carrier` on the full whole-unit frame (equivalence), and that it reads only the pruned column set:

```python
def test_precompute_invariants_equals_whole_unit():
    from ingestion.tracking_marts_dispatch import precompute_unit_invariants
    from silly_kicks.tracking import resolve_defended_goals, infer_ball_carrier

    full = _built_fixture_whole_unit("skillcorner")
    inv = precompute_unit_invariants(_spill_of(full), _spark, need_carrier=True)
    assert inv.goal_map == resolve_defended_goals(full)
    pd.testing.assert_frame_equal(inv.carrier, infer_ball_carrier(full))
```

- [ ] **Step 2: Run → FAIL.**
- [ ] **Step 3: Implement** `precompute_unit_invariants` with the column-pruned serverless-safe read (`pyarrow.parquet.read_table(columns=[...]).to_pandas()`).
- [ ] **Step 4: Run → PASS.**

### Task A.4: Stage-2 scoring dispatch (`windowed_score_sdf`)

**Files:**
- Modify: `src/ingestion/frame_window_dispatch.py`, `src/ingestion/tracking_marts_dispatch.py`
- Test: `src/tests/tracking_marts/test_stage2_window_scoring.py`

**Interfaces:**
- Consumes: `assign_score_windows` (A.2), `_make_streaming_group_mapper`, `precompute_unit_invariants` (A.3).
- Produces: `windowed_score_sdf(spark, spill_dir, mart: MartSpec, provider, *, target_window_frames, invariants) -> SparkDataFrame` — re-windows the spilled built frames with the mart's scoring halo, groups by `(game_id, period_id, _window_id)`, runs `mart.score` (closing over injected `invariants`), trims `_is_halo`-origin output rows, returns the lazy mart DF. `build_stage2_mart_sdf` is rewritten to call this instead of the whole-unit regroup (spec §4.5).

- [ ] **Step 1: Write the failing test** — a trivial frame-local mart (identity-count over frames) scored via `windowed_score_sdf` across 2 windows == whole-unit result.
- [ ] **Step 2: Run → FAIL.**
- [ ] **Step 3: Implement**; change `build_stage2_mart_sdf` group key from `["game_id","period_id"]` to the windowed dispatch. Halo-origin output trimming: outputs whose linked `frame_id` is a halo row in this window are dropped (produced as core in the neighbour window).
- [ ] **Step 4: Run → PASS.**

---

## Phase B — Lakehouse-only marts windowed + neutrality

### Task B.1: off_ball_runs neutrality (binding halo case)

**Files:**
- Modify: `src/ingestion/tracking_marts_dispatch.py` (`make_mart_scorers` off_ball unchanged; routing via A.4)
- Test: `src/tests/tracking_marts/test_stage2_window_scoring.py`

- [ ] **Step 1: Write the failing neutrality + halo-sufficiency test** on a committed fixture forced to span ≥2 windows with an off-ball run whose pre-action 1.5 s straddles a boundary:

```python
def test_off_ball_windowed_equals_whole_unit():
    whole = compute_off_ball_runs(actions, built_whole, xt)
    windowed = _run_windowed("off_ball_runs", actions, built_whole, xt, target_window_frames=20)
    pd.testing.assert_frame_equal(
        windowed.sort_values(["game_id", "action_id", "runner_id"]).reset_index(drop=True),
        whole.sort_values(["game_id", "action_id", "runner_id"]).reset_index(drop=True),
    )


def test_off_ball_halo_sufficiency_non_vacuous():
    # an action exactly score_halo_frames from the boundary is identical; one at halo-1 DIFFERS.
    at_h = _run_windowed(..., action_at_offset=score_halo_frames("off_ball_runs", "skillcorner"))
    under = _run_windowed(
        ...,
        action_at_offset=score_halo_frames("off_ball_runs", "skillcorner") - 1,
        halo_override=score_halo_frames("off_ball_runs", "skillcorner") - 1,
    )
    assert at_h.equals(whole_slice) and not under.equals(whole_slice)
```

- [ ] **Step 2: Run → FAIL** (routing not wired / halo too small). **Step 3:** wire off_ball through A.4 with its halo. **Step 4: → PASS.**

### Task B.2: action_defensive + defensive_credit_attributions neutrality
- [ ] Write neutrality tests (union == whole-unit) for both, with an action linked near a window boundary (link-tolerance halo). Run → FAIL → wire through A.4 → PASS. (Repeat the B.1 pattern; both are frame-local, link-tolerance halo, no invariant.)

### Task B.3: rest_defense GoalMap wiring (lakehouse-only seam)

**Files:**
- Modify: `src/ingestion/restdefense_writer.py`
- Test: `src/tests/tracking_marts/test_stage2_window_scoring.py` + `src/tests/test_restdefense_writer.py`

**Interfaces:**
- Produces: `compute_rest_defense_samples(actions, frames, xt, *, access_tier, goal_map: GoalMap | None = None)` — forwards `goal_map` to sk `compute_rest_defense(..., goal_map=goal_map)` (seam exists, `_compute.py:264-275`). `make_mart_scorers` passes the precomputed whole-unit `goal_map`.

- [ ] **Step 1: Write the failing test** — windowed rest_defense with the injected whole-unit `goal_map` == whole-unit rest_defense; and a guard that omitting `goal_map` (sk recomputes per window) DIFFERS on a dense skillcorner fixture (proves injection is load-bearing):

```python
def test_rest_defense_windowed_with_injected_goal_map_equals_whole_unit():
    gm = resolve_defended_goals(built_whole)
    whole = compute_rest_defense_samples(actions, built_whole, xt, access_tier=None, goal_map=gm)
    windowed = _run_windowed("rest_defense", actions, built_whole, xt, invariants=_inv(gm))
    pd.testing.assert_frame_equal(windowed_sorted, whole_sorted)


def test_rest_defense_per_window_goal_map_diverges():  # guards-that-cannot-fail: injection matters
    # GUARD-FOR-THE-GUARD (S2W-SPEC-01): first prove the committed fixture CAN diverge per-window,
    # else the assertion below passes vacuously on a fixture where per-window == whole-unit GoalMap.
    per_window_gm = resolve_defended_goals(built_whole[built_whole["_window_id"] == 0])
    assert per_window_gm != resolve_defended_goals(built_whole), (
        "fixture is vacuous: its per-window GoalMap already equals the whole-unit map "
        "(skillcorner ~7.1% divergence must be present) — the diverges test below cannot fail"
    )
    windowed_no_inject = _run_windowed("rest_defense", ..., goal_map=None)  # sk recomputes per window
    assert not windowed_no_inject.equals(whole_sorted)  # skillcorner 7.1% estimator shift
```

The guard-for-the-guard (the first assertion above) is REQUIRED on every per-window-diverges test in B.3 / E.1 / E.2: watch it hold on the committed fixture before trusting the divergence test (S2W-SPEC-01; the HFW-SPEC-03 "detector must be able to fail" lesson applied to the guard itself).

- [ ] **Step 2: Run → FAIL. Step 3:** add the `goal_map=` param + forward; pass the injected map via A.4. **Step 4: → PASS.**

---

## Phase C — Fix B hardening (lakehouse-only)

### Task C.1: retry-enforcement guards + CI-gap closure
- [ ] Confirm the 3 new `test_job_retry_policy.py` guards pass (carried in 0.1). Add a test that `_verify_post_apply` RAISES on a simulated job whose compute task has `max_retries` absent (prove the detector is not vacuous — watch it fail first).
- [ ] Investigate why the prior post-merge `terraform-apply.yml` did not run `patch_job_retries.py` (the CI gap, `project_preflight_canary_serverless_cycle`). If the apply does not invoke the patcher, add the step so the combined release leaves live retries = 0; capture the finding. If it DOES invoke it and the no-op was purely the `.get(...,0)` bug, record that the code fix alone closes it.
- [ ] Run → the retry tests + any added CI-gap test PASS.

---

## Phase D — Fit-profile knob lock + benchmarks

### Task D.1: lock `_TM_SCORE_WINDOW_FRAMES`
- [ ] From Phase B's per-window peaks (unit or canary-measured per 0.2), set `_TM_SCORE_WINDOW_FRAMES` ≤ the value keeping Stage-2b ≤ 800 MB. Add a module-level comment with the fit-profile reference (mirror ADR-089's `_TM_WINDOW_FRAMES` docstring).

### Task D.2: measure-before-optimize benchmarks
- [ ] Invoke `mad-scientist-skills:measure-before-optimize`. Run `src/tests/test_benchmarks.py` (`test_bench_batched_pitch_control` / `off_ball_xt_frame` / `defcon_credit_assignment` / `line_breaking_detection` / `team_shape`) → compare to `docs/performance-baselines.md`; assert no regression beyond threshold. If the re-windowing changed the scorer call pattern (esp. gkdv per-window), record the delta; do NOT rewrite the gkdv loop for perf without separate approval (Non-goal (e), R5).

---

## Phase E — gk_decision + gkdv windowing (sk `goal_map=` seam RELEASED; E.0 done)

> **Precondition SATISFIED:** the `goal_map=` seam shipped in sk 4.132.0 (spec §4.4; impl reviewed APPROVE — `D:\Development\_reviews\2026-10-11-silly-kicks-goalmap-injection-impl.md`), is vendored (`.venv` sk = 4.132.0), and the sk floor is bumped (Task E.0 done). These tasks author/TDD-test against the released API. (They were parked on the sk release; that gate is cleared — the windowing code itself still gates on spec/plan approval.)

### Task E.0: sk floor bump — DONE (applied on this branch 2026-10-11)
- [x] Bumped the sk floor to 4.132.0 across 21 `_REQUIRED_SK_MIN` sentinels + `pyproject` floor (`>=4.132.0,<5`) + tf env pin (`terraform/modules/workflows/main.tf`) + `uv.lock` + `submit_ac1_oneshot.py`; §2.10.5 + `test_train_xt_gk_v2` sentinel tests updated. Verified: `sync_tf_env_pins --check` clean; `test_sk3_mig_b_orchestrator_invariants` + `test_executor_env_guard` pass; vendored sk = 4.132.0; `build_ghost_frames(..., goal_map=)` / `serve_ghost_gk_positions(..., goal_map=)` / `ReconstructedOptionSet(..., goal_map=)` / `serve_ghost_outfield_positions(..., goal_map=)` / `build_restdefense_ghost_frames(..., goal_map=)` all present in the vendored sk.

### Task E.1: gk_decision windowed with injected GoalMap

**Files:** Modify `src/ingestion/gk_decision_writer.py` (thread injected `goal_map` into `score_gk_decision_reconstructed` → `ReconstructedOptionSet`), routed via A.4.

- [ ] **Step 1:** neutrality test — windowed gk_decision with injected whole-unit `goal_map` == whole-unit; + a per-window-goal_map-diverges guard WITH its guard-for-the-guard (assert the fixture's per-window GoalMap ≠ whole-unit first; as B.3, S2W-SPEC-01). **Step 2: FAIL. Step 3:** thread `goal_map` through; `make_mart_scorers` passes the precomputed map. **Step 4: PASS.**

### Task E.2: gkdv windowed with injected GoalMap + carrier

**Files:** Modify `src/ingestion/gkdv_writer.py` — inject `goal_map` into the build path (sk seam), and the whole-unit `carrier` (precomputed in A.3) via the existing `build_ghost_frames(..., carrier=)`, routed via A.4.

- [ ] **Step 1:** neutrality test — windowed gkdv with injected whole-unit `goal_map` + `carrier` == whole-unit; + two guards, EACH with its guard-for-the-guard (S2W-SPEC-01): per-window `goal_map` diverges (fixture's per-window GoalMap ≠ whole-unit asserted first), and per-window carrier (reset `incumbent`) diverges at a boundary mid-hysteresis (fixture's boundary must actually straddle a hysteresis flip asserted first) — proves BOTH injections are load-bearing on a non-vacuous fixture. **Step 2: FAIL. Step 3:** thread both. **Step 4: PASS.**

---

## Phase F — Integration, gates, wheel bump, commit

### Task F.1: end-to-end two-stage neutrality (Docker/live-posture)
- [ ] Extend/author the two-stage dispatch test (`test_two_stage_dispatch` / `test_windowed_stage1` siblings) to cover Stage-2 windowed scoring end-to-end for all six marts on the bundled fixture: whole-unit vs windowed, byte-identical.

### Task F.2: canary on a dense unit (post-infra; separate approval for any run)
- [ ] Prepare a scoped drain/preflight canary on `1562415:1` (or another of the 8). This REQUIRES a job run → separate explicit approval, only after post-merge CI green. Expected: no OOM, mart rows non-empty. (Not run during Python CI — [no-jobs-in-ci].)

### Task F.3: wheel bump + TODO + full suite
- [ ] Bump `pyproject.toml` version; run `scripts/bump_wheel.py` (~30 consumers); never hand-edit consumers. Run `scripts/sync_tf_env_pins.py`; re-run `test_topandas_boundedness` if any exemption changed.
- [ ] Update `TODO.md` in the shipping change; verify `git status TODO.md` + `bump_wheel --check`.
- [ ] Run ALL seven local gates + `validate_workflow_cards` + a REAL full `uv run pytest` (not `--collect-only`, not a subset; capture exit code, never `| tail`). Fix to green.

### Task F.4: commit gate (STOP — explicit approval)
- [ ] Show the full diff / file list. Do NOT `git commit`/`push`/`gh pr create`/`merge`. Wait for explicit per-action approval ([no-commit-no-approval]). One commit, fully-tested coherent state. Commit message ends with `Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>`.

---

## Post-merge (separate approvals, not this branch)
- After post-merge main CI green: **targeted re-run of ONLY the 8 units** (`1204769p2, 1374493p2, 1463749p1, 1562415p1, 1562415p2, 1654689p2, 2015213p1, 2015213p2`), live retries = 0, fail-loud. Verify via OUTPUT tables (ground truth; per-unit `succeeded` events are fail-open/lag). Mega-job `302697362345215`; auth OAUTH + `DATABRICKS_AUTH_STORAGE=plaintext`.
- Then the rest of the combined re-materialize chain (idsse, §8 AC das_*/gkdv/frame-geometry, sk4118 marts, xt_gk_v2 retrain) — each its own approval, per `project_preflight_canary_serverless_cycle`.

---

## Self-review (against the spec)
- **Coverage:** §4.1 seam → A.2/A.4; §4.2 per-mart halo → A.1 + B/E neutrality; §4.3 invariant precompute+inject → A.3 + B.3/E.1/E.2; §4.4 sk dependency → Phase E gate + E.0; §4.5 dispatch/routing → A.4; §4.6 fit-profile → 0.2/D.1; §4.7 Fix B → 0.1/C.1; §6 tests → B/E neutrality + halo-sufficiency + invariant-injection + benchmarks (D.2) + canary (F.2) + full suite (F.3). All spec sections map to a task.
- **sk-gated honesty:** Phase E is explicitly parked on the sk release; no task authors code against a non-existent API before E.0 verifies the seam is vendored.
- **Commit discipline:** single commit gate (F.4); no per-task/per-phase commits.
