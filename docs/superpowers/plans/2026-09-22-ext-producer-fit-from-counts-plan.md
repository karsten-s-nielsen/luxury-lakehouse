# ExT producer — distributed `fit_from_counts` rewrite — Implementation Plan

**Spec:** `docs/superpowers/specs/2026-09-22-ext-producer-fit-from-counts-design.md` (**APPROVED** — fresh-session review 2026-09-22).
**Cycle:** sk4118 P2. **Branch:** the existing P2 branch (one branch per cycle) — `git fetch && git pull --ff-only` first.
**Supersedes** the *fit mechanism* in `docs/superpowers/plans/2026-09-17-sk4118-p1-metrics.md` Task G1.

## Prerequisites (BLOCKING — do not start Task 2 until both true)

1. **silly-kicks `ExpectedThreat.fit_from_counts` (SK-XT-COUNTS) is RELEASED — MET (2026-09-22).** Merged to
   sk `main` (`df46c85` / PR #250 `4ac26d0`, ADR-102), tagged **`v4.123.0`**, PyPI-live (2 files). Impl
   reviewed + APPROVED by me (`_reviews/2026-09-22-sk-expectedthreat-fit-from-counts-impl.md`). Released
   signature (keyword-only, reconciled): `fit_from_counts(*, shot_counts, goal_counts, move_counts,
   transition_start_counts, transition_counts, params=None)` — note `transition_start_counts` precedes
   `transition_counts`; the producer calls with kwargs so order is immaterial.
2. This spec + plan APPROVED by the owner. **(Spec APPROVED by fresh-session review 2026-09-22; plan
   REQUEST-CHANGES → this revision folds EXT-PLAN-01/02/03; re-submit for approval.)**

Task 1 (the failing tests) can be written against the approved sk contract **before** the release, but it will
not turn green until the wheel is pinned (Task 3). Do NOT commit a red suite.

## Global constraints

- **TDD red-green:** the Task-1 tests are written failing first, observed red, then Task 2/3 turn them green.
- **No commit / push / PR / run_now without explicit owner approval** for that specific action (STOP at Task 5).
  Plan approval is NOT commit authority; "tests pass" is NOT commit authority.
- **Byte-identity is the bar:** the grids `fit_from_counts` produces must equal `.fit(actions)` on the same
  corpus — asserted on a fixture (Task 1 test 3) and spot-checked live post-merge (P2).
- **Local gate set (all, before any push):** `ruff check src/ scripts/` + `ruff format --check` +
  `lint-imports` + `bump_wheel.py --check` + `pip_audit_ignores.py --check` + `pytest src/tests/ -v` +
  `pyright src/ hf_taipy_app/src/ scripts/_tf_env_pins.py scripts/sync_tf_env_pins.py` +
  `validate_workflow_cards` (the 8th gate) + `test_topandas_boundedness`.

---

### Task 1: Red — the failing producer tests

**File:** `src/tests/test_expected_threat_producer.py` (extend). Build a small SPADL `actions` fixture in-code
carrying the §Tests boundary rows: a synthesized valid-start/NaN-end move; a 0-goal / 0-action zone; an
off-pitch end; **AND excluded-type rows (EXT-PLAN-01) — a `shot_penalty` and a `shot_freekick` (must NOT count
as `shot`), a `take_on` and a `throw_in`/`freekick_short` (must NOT count as `move`), each placed in an
OTHERWISE-EMPTY zone** so a filter bug (counting `shot_penalty` as a shot, `take_on` as a move) makes that
zone's count non-zero and is caught. A helper `_aggregate_counts(actions)` computes the five arrays via the
SAME filters (mirror of the producer's aggregation, computed in pandas for the test) so the test does not
depend on Spark.

- [ ] **Step 1: Write the 6 tests (spec §Tests):**
  1. `test_sql_binning_matches_sk_get_flat_indexes` — the producer's flat-index expression (extracted as a pure
     function, D1) == sk `_get_flat_indexes(pd.Series(xs), pd.Series(ys), 16, 12)` on a probe grid incl. edges
     `x∈{0,105,120}`, `y∈{0,68,-5}`, and the `15`/`11` clamp.
  2. `test_count_aggregation_matches_sk_internals` — on the fixture, `_aggregate_counts` equals sk's own
     `_count(shot.start_x, …)` (shot/goal), `_count(move.start_x, …)` (move), and the singh
     `start_counts`/`counts` intermediates.
  3. `test_fit_from_counts_byte_identical_to_fit` (**the D6 gate**) — `ExpectedThreat(16,12).fit_from_counts(
     **_aggregate_counts(fixture))` vs `.fit(_to_sk_actions(fixture))`: `np.array_equal` on
     `scoring_prob_matrix`/`shot_prob_matrix`/`move_prob_matrix`/`transition_matrix` + `np.allclose(xT)`. The
     fixture's synthesized valid-start/NaN-end move makes `move_counts != transition_start_counts` — asserts
     `array_equal` on BOTH `move_prob_matrix` AND `transition_matrix` (non-vacuity for the split). Because the
     fixture now also carries excluded-type rows (`shot_penalty`/`take_on`/…) and `.fit` ignores them, this
     byte-identity ALSO requires `_aggregate_counts` to exclude them — but only non-vacuously if test 6 proves
     those rows would otherwise land somewhere, so keep both.
  4. `test_grid_dims_match_spadlconfig` — `_PITCH_LENGTH/_WIDTH/_XT_L/_XT_W` == `spadlconfig.field_length/width`
     + sk `ExpectedThreat` defaults.
  5. `test_global_equals_sum_of_per_comp_counts` — on a 2-competition fixture, the element-wise sum of the
     per-comp arrays fits equal to `.fit(pd.concat([A,B]))` (matrices `array_equal`, xT `allclose`).
  6. `test_excluded_action_types_are_not_counted` (**EXT-PLAN-01 non-vacuity for the type filter**) — the
     `shot_penalty`/`shot_freekick` sit in an otherwise-empty zone: assert `shot_counts`/`goal_counts` there ==
     0; the `take_on`/`throw_in`/`freekick_short` sit in another empty zone: assert `move_counts` and
     `transition_start_counts` there == 0. **Non-vacuity control:** assert those same zones are NON-zero when
     the rows are re-typed to `shot`/`pass` respectively (proves the zone is reachable, so a filter bug WOULD
     surface). This catches a SQL bug that counts `shot_penalty` as a shot or `take_on` as a move — which ships
     green on a fixture with none and then silently diverges on the real corpus (which HAS penalties/take_ons).
- [ ] **Step 2: Run → RED.** `uv run pytest src/tests/test_expected_threat_producer.py -q` → the new tests fail
      (`AttributeError: fit_from_counts` pre-pin, or `NameError` on the not-yet-extracted binning function).
      Capture the red (not via `| tail`).

---

### Task 2: Green — the count-aggregation producer

- [ ] **Step 1: Extract the binning primitive (pure, testable).** A module-level pure function returning the
      flat-index SQL fragment (or a small pure `flat_index(x, y)` used by both a Spark `expr` and the test),
      sourced from `_XT_L/_XT_W/_PITCH_LENGTH/_PITCH_WIDTH`. Turns test 1 green.
- [ ] **Step 2: `_aggregate_counts_spark(spark, catalog)` — one distributed pass.** A single query over
      `{catalog}.{DEFAULT_GOLD_SCHEMA}.fct_action_values` restricted to the move+shot types, computing per
      `competition_id`. **Build the type lists from the sk-exposed constants, not literals** — the shot filter
      is `action_type = ExpectedThreat.SHOT_TYPE_NAME` and the move filter is `action_type IN
      ExpectedThreat.MOVE_TYPE_NAMES` (`('pass','dribble','cross')`), so a future sk change to the filter set
      propagates and cannot silently drift from `.fit` (complements test 6). The `IN` list is therefore
      `MOVE_TYPE_NAMES + (SHOT_TYPE_NAME,)`. Aggregates:
      - `shot_counts` / `goal_counts` — `GROUP BY competition_id, flat_start` over `action_type = SHOT_TYPE_NAME`
        (+ `action_result='success'` for goals), valid start.
      - `move_counts` — `GROUP BY competition_id, flat_start` over moves (`MOVE_TYPE_NAMES`), valid start.
      - `transition_start_counts` — `GROUP BY competition_id, flat_start` over moves, valid start AND end.
      - `transition_counts` — `GROUP BY competition_id, flat_start, flat_end` over moves,
        `action_result='success'`, valid start AND end.
      Collect the (bounded) count tables to the driver and build, per `competition_id`, the four `(12,16)`
      arrays (via `vector[192].reshape`) + the dense `(192,192)` transition matrix. **No `.toPandas()` on
      actions.** Then compute `global` as the element-wise sum of the per-comp arrays. Turns tests 2/5 green.
- [ ] **Step 3: `_fit_grid_from_counts(counts)`** — `ExpectedThreat(l=_XT_L, w=_XT_W).fit_from_counts(**counts)`;
      runtime-assert `silly_kicks.__version__ >= _REQUIRED_SK_MIN`. Turns test 3 green.
- [ ] **Step 4: Rewrite `run_pipeline`.** Replace the per-comp `.toPandas()` accumulation + `pd.concat` global
      fit with: `counts_by_comp = _aggregate_counts_spark(...)`; for each comp in `comps_to_visit` fit from its
      counts + apply the **unchanged** directionality guard + `_write_grid_if_material`; if `need_global`, fit
      from the summed counts + `validate_structural` + `assert_directional("global")` + write; then
      `record_watermarks`. The guard/scope decision (`_ExpectedThreatGuard`, `_decide_rebuild`), the zone
      projection, and the materiality gate are **untouched**.
- [ ] **Step 5: Delete the driver-pull path.** Grep `src/` + `scripts/` for importers of
      `_load_actions_for_competition` / `_to_sk_actions` / `_fit_sk_grid`; delete those with no other importer
      (Chesterton's fence — keep any still used, note why). Remove the `_topandas_exemptions.yml` entry for the
      global fit; update its line reference + re-run `test_topandas_boundedness`.
- [ ] **Step 6: Decide `scripts/compute_xt_grid_hf.py`.** It is a manual HF producer, off the timeout critical
      path. Mirror the rewrite (preferred — one fit mechanism) unless the review says leave it on `.fit`; keep
      its human-readable grid CSV/parquet derivation from `.xT` either way.

---

### Task 3: sk pin bump + full gates

- [ ] **Step 1: Bump the sk pin (D5).** `pyproject.toml` sk floor → **4.123.0** (the `fit_from_counts` release); `uv lock`;
      `python scripts/sync_tf_env_pins.py` (never hand-edit the TF env pins); bump `_REQUIRED_SK_MIN` at all 7
      sites (grep `src/` + `scripts/`); `submit_ac1_oneshot.py` pin; the sk-bump sentinel test. Confirm
      `test_terraform_env_dep_parity` + `test_ci_dbt_pin_parity` green.
- [ ] **Step 2: Wheel bump.** `pyproject.toml` version → next; `python scripts/bump_wheel.py` (~30 consumers,
      never hand-edit); `bump_wheel.py --check` green.
- [ ] **Step 3: Full local gate set** (all nine from Global constraints) → green; capture the pytest exit code
      (not via `| tail`). `pyright` on the full CI target, not `pyright src/`.

---

### Task 4: Docs

- [ ] **ADR:** amend ADR-085 (or a short new ADR) recording that the single-canonical surface is now fit via the
      distributed `fit_from_counts` count aggregation — the OPT-1 single-pass restoration, undoing ADR-085's
      driver-pull reversal now that sk exposes an additive fit. Cross-ref SK-XT-COUNTS.
- [ ] **TODO.md** groomed in this commit (the cycle-close rule). **No C4 change** (no new
      model/task/dependency). Verify `git status TODO.md` + `bump_wheel --check`.

---

### Task 5: Commit gate

- [ ] **Step 1:** Re-run the full nine-gate set green; capture the pytest exit code.
- [ ] **Step 2:** `git status` shows ONLY the expected files: `expected_threat.py`,
      `test_expected_threat_producer.py`, sk-pin sites, `_topandas_exemptions.yml`, `pyproject.toml`, `uv.lock`,
      the TF env pins + wheel-consumer bumps, the ADR + TODO + spec + plan, possibly `compute_xt_grid_hf.py`.
- [ ] **Step 3: STOP — request explicit owner approval to commit.** Show the diff / file list. On approval:
      single commit on the P2 branch. Then owner-gated push → PR → CI (incl. live DDL-parity + dbt build) →
      admin-merge → **P2 recompute** (`run_now(only=["compute_expected_threat"])` FIRST — it repopulates the
      grid + zones the AC drain reads — then the AC re-materialize etc. per the P2 run order), each its own
      explicit approval.

## Self-review

1. **Spec coverage:** §Design 1 → Task 2 Step 1; §Design 2 → Task 2 Step 2 + tests 2/6; §Design 3 → Task 2 Step 2
   (type filter from sk constants) + test 6; §Design 4 → Task 2 Steps 3-4; §Design 5 → Task 2 Step 5-6; D1-D6 →
   tests 1-6 + ADR (Task 4). ✓
2. **Byte-identity:** test 3 (fixture, all 4 matrices + xT) + the post-merge live spot-check; the count filters
   mirror sk's `_count`/singh exactly (§Design 2). ✓
3. **The move/transition-start split** (the SK-XT-COUNTS r1 BLOCKING mirror): `move_counts` valid-start vs
   `transition_start_counts` valid-start+end — pinned by test 3's synthesized NaN-end move (real data has 0, so
   it MUST be synthesized or the test is vacuous). ✓
6. **Type-filter non-vacuity (EXT-PLAN-01):** the fixture carries excluded-type rows
   (`shot_penalty`/`shot_freekick`/`take_on`/`throw_in`) in otherwise-empty zones; test 6 asserts they are NOT
   counted AND (control) that the same zones ARE counted when re-typed to `shot`/`pass` — so a SQL filter bug
   that counts a penalty as a shot or a take-on as a move surfaces here instead of silently diverging on the
   real corpus. The SQL type lists derive from `ExpectedThreat.SHOT_TYPE_NAME`/`MOVE_TYPE_NAMES` (single source
   vs sk). ✓
4. **No accidental scope:** guard/scope/zone-projection/materiality/watermark untouched; mart schema + card +
   dbt + C4 unchanged; the only behavioural change is the fit mechanism, gated on byte-identity. ✓
5. **Prereq honoured:** implementation blocked on the sk release + a signature reconciliation check; Task 1 may
   be written early but not committed red. ✓
