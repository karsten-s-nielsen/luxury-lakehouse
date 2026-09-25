# xImpact + win-prob leverage on `fct_action_values` — Implementation Plan

**Spec:** `docs/superpowers/specs/2026-09-22-ximpact-leverage-action-values-design.md` (DRAFT — approve first).
**Cycle:** sk4118 P2. **Branch:** the SAME P2 branch as the ExT-producer rewrite (`feat/ext-producer-fit-from-counts`
or the cycle branch). **This is the 2nd of two commits in one PR**; the ExT-rewrite commit lands first (it owns
the sk 4.123.0 pin bump + wheel bump). This commit is additive on top.

## Prerequisites

1. The ExT-rewrite commit (sibling plan) has done the sk floor bump to **4.123.0** + `uv lock` + tf env pins +
   wheel bump. This commit does **not** re-bump; it depends on 4.123.0 being installed (`WinProbabilityModel`,
   `goal_leverage`, `ximpact_values` available). Verify `from silly_kicks.win_probability import goal_leverage,
   WinProbabilityModel` and `from silly_kicks.vaep.ximpact import ximpact_values` import.
2. This spec + plan APPROVED by the owner.

## Global constraints

- **TDD red-green:** Task-1 tests written failing first, observed red, then Task 2/3 green.
- **No commit / push / PR / run_now without explicit owner approval** (STOP at Task 6). Plan approval ≠ commit
  authority.
- **Honest-NaN:** an unresolved WP state → NaN leverage → NaN ximpact; never fabricated (sk contract).
- **Local gate set (all, before push):** ruff check + ruff format --check + lint-imports + bump_wheel --check +
  pip_audit_ignores --check + `pytest src/tests/ -v` + full pyright target + validate_workflow_cards +
  `test_ai_governance_md` + `test_architecture_md_appendix` + HF publish-parity/leak-guard.

---

### Task 1: Red — the failing tests

**File:** `src/tests/test_ximpact.py` (new) + extend `test_spadl_vaep.py` parity.

- [ ] **Step 1: Write the tests:**
  1. `test_ximpact_equals_adjusted_times_leverage` — on an in-code SPADL game fixture, the writer's `ximpact`
     column == `vaep_adjusted_value × win_prob_leverage` (element-wise, `allclose`, `equal_nan=True`).
  2. `test_leverage_high_for_late_go_ahead_low_for_decided` (**non-vacuity**) — a fixture action at a late tied
     state has `win_prob_leverage` strictly greater than an action at a decided garbage-time state (≈ 0). Mirrors
     sk's boundary; proves leverage is state-sensitive, not a constant.
  3. `test_unresolved_state_propagates_nan` (**honest-NaN control**) — a single-team / missing-home-id group →
     NaN `win_prob_leverage` AND NaN `ximpact` (never 0-filled).
  4. `test_leverage_within_unit_range` — non-NaN `win_prob_leverage ∈ [−1, 1]`.
  5. `test_home_team_id_is_hashed_bigint` — the `games` frame the writer builds uses
     `hash_native_id_to_bigint(home_team_id_native)` (== the `team_id` space), asserted by a resolved-state
     leverage being non-NaN on a fixture whose `home_team_id_native` is a string (would be NaN/unresolved if the
     raw string were passed).
  6. `test_vaep_writer_schema_carries_ximpact` (extend `test_spadl_vaep_writer_parity`) — `_VAEP_SCHEMA` and the
     applyInPandas `StructType` both declare `ximpact DOUBLE, win_prob_leverage DOUBLE`.
  7. `test_writer_ximpact_matches_vaep_rate_ximpact` (**XIMP-02 — end-to-end byte-consistency**) — on the
     fixture game, the writer's `ximpact` column == `vaep_model.rate_ximpact(game, game_actions,
     XSuccessModel.bundled(), win_prob_model=WinProbabilityModel.bundled())` (`allclose`, `equal_nan=True`).
     Closes the args-wiring claim end-to-end (not just the `adjusted × leverage` multiply of test 1) — it would
     fail on the XIMP-01 `adjusted["vaep_value"]` bug or an index/`games` misalignment.
- [ ] **Step 2: Run → RED.** `uv run pytest src/tests/test_ximpact.py src/tests/test_spadl_vaep*.py -q` → fail
      (column absent). Capture the red (not via `| tail`).

---

### Task 2: Green — compute + schema

- [ ] **Step 1: Cache the bundled WP model.** In `spadl_vaep.py`, add `WinProbabilityModel.bundled()` to the
      per-executor cache alongside `XSuccessModel.bundled()` (loaded once, SHA256-verified).
- [ ] **Step 2: Compute in the per-game path.** After the existing `rate_adjusted` (`:594`):
      `games = pd.DataFrame([{ "game_id": …, "home_team_id": hash_native_id_to_bigint(home_team_id_native) }])`;
      `leverage = goal_leverage(game_actions, model=wp_model, games=games)`;
      `ximpact = ximpact_values(adjusted, leverage)` — **pass the `rate_adjusted` DataFrame, NOT
      `adjusted["vaep_value"]`** (XIMP-01; `ximpact_values` reads `v = adjusted["vaep_value"]` internally,
      `vaep/ximpact.py:36`). **Index alignment (XIMP-01):** `ximpact_values` does `leverage.reindex(v.index)`, so
      `adjusted`, `game_actions`, and `leverage` must share one index — compute `ximpact`/`leverage` on the
      `game_actions` index BEFORE the existing `.reset_index(drop=True)` (as `VAEP.rate_ximpact` does), or reset
      all three consistently, else every row NaNs. Assign `game_out["win_prob_leverage"]` (= `leverage`),
      `game_out["ximpact"]`. NaN-safe (goal_leverage returns NaN on unresolved). Turns tests 1-5 + 7 green.
- [ ] **Step 3: Schema (ADR-016 parity).** Add `ximpact DOUBLE, win_prob_leverage DOUBLE` to `_VAEP_SCHEMA` +
      the applyInPandas `StructType`. Turns test 6 green. Run `test_spadl_vaep_writer_parity`.

---

### Task 3: dbt (staging → mart → agg)

- [ ] **Step 1: Bronze migration.** `scripts/migrations/2026-09-22-vaep-action-values-add-ximpact.sql` —
      `ALTER TABLE {catalog}.{bronze}.vaep_action_values ADD COLUMNS (ximpact DOUBLE, win_prob_leverage DOUBLE)`
      (idempotent, single-leading-column; operator-applied WITH the merge).
- [ ] **Step 2: staging.** `stg_spadl__action_values.sql` — select the 2 new cols through.
- [ ] **Step 3: mart.** `fct_action_values.sql` — carry `ximpact`, `win_prob_leverage`; add both to
      `_marts__models.yml` (contract enforced). Run `test_marts_models_yml_completeness`.
- [ ] **Step 4: player rollup.** `fct_vaep_breakdown_agg.sql` — `sum(ximpact) as total_ximpact` (cast double),
      mirroring `total_vaep`.

---

### Task 4: Governance + docs

- [ ] **Step 1:** `AI_GOVERNANCE.md` §5 — note the xImpact/leverage output under wf-vaep.
- [ ] **Step 2:** `docs/huggingface/model-cards/vaep-model.md` — xImpact intended-use / non-use + bundled WP-model
      dependency; keep the `EU AI Act — Intended Use and Non-Use` stanza + `AI_GOVERNANCE.md` reference.
- [ ] **Step 3:** `ARCHITECTURE.md` Appendix D — add Dixon & Robinson (1998) + Robberechts, Van Haaren & Davis
      (2019); extend `expected_authors` in `test_architecture_md_appendix.py`.
- [ ] **Step 4:** Re-run `test_ai_governance_md.py` + `test_architecture_md_appendix.py` → green.

---

### Task 5: HF

- [ ] **Step 1:** The spadl_vaep publisher stages the 2 new cols through the ADR-072 guarded frame
      (`prepare_public_upload`; `access_tier` unchanged). No direct `HfApi` / `split_restricted` (seam only).
- [ ] **Step 2:** Update the spadl_vaep HF dataset card + `docs/huggingface/org-card.md` + `README.md` (artifact
      column list). Run `test_hf_publish_parity` + `test_hf_leak_guard` + `test_publisher_seam_conformance`.

---

### Task 6: Full gates + commit gate

- [ ] **Step 1:** Full local gate set (Global constraints) → green; capture the pytest exit code (not `| tail`).
- [ ] **Step 2:** `git status` shows only the expected files (spadl_vaep.py, migration, staging, 3 marts/yml,
      governance/arch docs + their tests, HF publisher/card/org-card/README, test_ximpact.py, this spec+plan).
- [ ] **Step 3: STOP — request explicit owner approval to commit.** On approval: the 2nd commit on the P2 branch
      (the ExT-rewrite commit is 1st). Then owner-gated push → PR (both commits) → CI (incl. live DDL-parity for
      BOTH migrations + dbt build) → admin-merge → operator applies the migration + runs the P2 recompute
      (compute_spadl_vaep re-run backfills ximpact/leverage), each its own explicit approval.

## Self-review

1. **Spec coverage:** §Design 1 → Task 2; §Design 2 (schema) → Task 2 Step 3; §Design 3 (migration) → Task 3
   Step 1; §Design 4 (dbt) → Task 3; §Design 5 (governance) → Task 4; §Design 6 (HF) → Task 5; X1-X6 → tests +
   docs. ✓
2. **Honest-NaN non-vacuity:** test 3 asserts unresolved → NaN (not 0-fill); test 2 asserts state-sensitivity
   (late-tied > decided). ✓
3. **ADR-016 id-space:** test 5 pins home_team_id = hashed BIGINT (the goal_leverage `same_id` trap). ✓
4. **No scope creep:** no in-game p_win trio (name collision avoided); no WP-model fit (bundled); no new
   mart/card; governance folds under wf-vaep. ✓
5. **Coupling to the sibling commit:** depends on the ExT-rewrite commit's sk 4.123.0 bump; this commit is
   additive; both in one PR, both merge before the recompute. ✓
6. **Commit gate:** STOP at Task 6 Step 3; plan approval ≠ commit authority. ✓
