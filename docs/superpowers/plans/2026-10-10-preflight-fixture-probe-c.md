# Plan — Preflight Probe C on a bundled fixture unit

Implements `docs/superpowers/specs/2026-10-10-preflight-fixture-probe-c-design.md`. Branch `fix/preflight-fixture-probe-c` off `origin/main` (`7606ffa0`). One coherent commit. No Databricks job until post-merge CI green. **Assumes §2.6 = (a) tracking-marts ONLY** (confirm before impl).

## Global constraints
- Implement INLINE. import-linter: the lifted module lives in `analytics` + calls the `ingestion` scorer cores → it must sit where the existing `_oracle` import direction already works (the scorers are `ingestion.*_writer`; `_oracle` imports them from `src/tests`, which is not import-linter-scoped — the WHEEL module importing `ingestion` from `analytics` would VIOLATE `analytics ⇏ ingestion`). RESOLVE AT T1: put the lifted build+score in an `ingestion` module (ingestion → analytics is allowed), not analytics. (Spec R1 said analytics; the import direction forces ingestion — a plan-time correction to verify.)
- TEST-FIRST (red-green). 7 gates + validate_workflow_cards + the fixture-packaging test.

## Task 1 — confirm two unknowns before edit (research, no edit)
- **T1.1 import direction:** the scorers are `ingestion.{off_ball_runs,defensive_credit,gk_decision,gkdv,restdefense}_writer`. A wheel module that both builds (`analytics.build_unit_inputs`) AND calls those scorers must live in `ingestion` (ingestion→analytics OK; analytics→ingestion BANNED). Decide the module home = `ingestion.tracking_marts_canary`. Confirm `lint-imports`.
- **T1.2 fixture non-vacuity (HARD gate, FIXTURE-PLAN-01):** confirm `idsse J03WMX_p1` (full, 97 actions) yields **≥ 1 row for EVERY one of the 6 marts** (not merely "declared columns"). If ANY mart no-ops, CURATE the fixture (add the shot/keeper/players/possession that mart needs) until all 6 are non-empty. NO skillcorner/GS (restricted, ADR-064). This is a blocking gate — a no-op mart = a vacuous canary.
- **T1.3 wheel packaging:** confirm the hatchling build config (`pyproject.toml`) and how to `force-include` a data dir so the fixture ships (and is read via `importlib.resources`, not a `src/tests` path).

## Task 2 — lift build+score into the wheel (R1) — `ingestion/tracking_marts_canary.py`
- **T2.a (red)** `test_tracking_marts_canary.py`: `score_fixture_marts()` builds the bundled fixture + runs all 6 scorers → all `MART_KEYS` present AND **each has ≥ 1 row** (FIXTURE-PLAN-01) + deterministic; reads via `importlib.resources` (NOT `src/tests`). Fails — module absent.
- **T2.b (green)** Create `ingestion.tracking_marts_canary`. Lift the REUSABLE CODE from `_oracle`: `build_inputs(fixture_root, provider)` + `score_all_marts(...)` (parameterized by fixture root + provider). `score_fixture_marts()` = `score_all_marts` on the bundled canary fixture via `importlib.resources.files("ingestion") / "canary_fixture"`. Refactor `src/tests/tracking_marts/_oracle.py` to IMPORT the lifted `build_inputs`/`score_all_marts` (single source for the CODE) while keeping its OWN `FIXTURE_ROOT` + per-provider `UNITS` (its transport-neutrality test needs the per-provider src/tests fixtures — DO NOT repoint them) + its test-only `assert_mart_equal`/`sorted_for_compare`.

## Task 3 — ship a DEDICATED canary fixture (R2, single-source) — wheel package-data
- **T3.a (red)** `test_canary_fixture_packaged`: the canary fixture parquet set resolves via `importlib.resources.files("ingestion")/"canary_fixture"/...` and loads. Fails — not bundled.
- **T3.b (green)** Create a DEDICATED canary fixture at `src/ingestion/canary_fixture/` (frames/actions/meta + xT grid, curated from idsse `J03WMX_p1` data to clear T1.2's all-6-non-empty gate). **Keep it a COMPLETE, self-consistent real-idsse unit** (a contiguous subset of a real match — NOT synthetic-stitched/sliced), so the whole-unit action→frame link holds (B r2 caveat; a stitched/sliced unit reintroduces the mass-unlinked failure the fixture approach exists to avoid). It is the canary's OWN single copy — NOT a duplicate of a `src/tests` fixture, so no drift (SPEC-05/PLAN-03). **Do NOT touch** `src/tests/fixtures/.../idsse/J03WMX_p1` (the AC full-golden gate + `_oracle` still use it) — the canary fixture is separate data with a single consumer. Add the hatchling include so `canary_fixture/` ships; verify `lint-imports`.

## Task 4 — Probe C = fixture smoke (R3, R4) — `canary.py` + `tracking_marts_drain.py`
- **T4.a (red)** `test_canary.py::test_probe_c_fixture_smoke`: a fixture-Probe-C callable runs the 6-scorer smoke + RAISES on a seeded raising scorer (non-vacuous). Fails — no fixture probe.
- **T4.b (green)** Add the fixture Probe C (calls `ingestion.tracking_marts_canary.score_fixture_marts` + asserts each of the 6 marts present AND **≥ 1 row** — R3; raises naming the empty/missing mart). The tracking-marts preflight (`main_tracking_marts_preflight`) calls it as Probe C (NOT `select_canary_probes`' real-unit Probe C); Probe M still from the global-densest window. `run_canary` stays generic; a thin tracking-marts wrapper injects the fixture probe. (AC preflight unchanged — §2.6a.)

## Task 5 — drop R1 unit_counts (R5) — `tracking_marts_driver.py` + `tracking_marts_drain.py`
- **T5.a (red)** update `test_tracking_marts_canary_spark.py` + any caller test to the window-only return. Fails on the old 2-tuple.
- **T5.b (green)** `compute_tracking_size_signals` returns ONLY `list[WindowRef]` (drop `unit_counts`); the preflight builds Probe M from it; remove the `n_frames`/`replace` sizing for tracking-marts. (`WorkUnit.n_frames` stays — AC still uses it under §2.6a.)

## Task 6 — ADR-087 note + TODO (spec §6)
Append the Probe-C-fixture amendment (R5 evidence, pure-core fixture smoke, Probe M unchanged, §2.6a). TODO.md updated in-commit.

## Task 7 — wheel bump + gates + ship (approval-gated)
- **T7.1** Wheel 0.5.120 → **0.5.121** (pyproject + `bump_wheel.py` + uv.lock root key); `--check` consistent.
- **T7.2** 7 gates + `validate_workflow_cards` + the fixture-packaging + Docker legs (`test_tracking_marts_canary_spark.py` updated to window-only).
- **T7.3** STOP at commit — diff/file list, await approval → push → PR (spec+plan bundled) → CI → `/review-impl` (2 reviewers) → merge `--admin` → post-merge CI.

## Task 8 — POST-MERGE R6 (SEPARATE approval, operator)
Re-run `preflight_tracking_marts --full --providers idsse`; expect ≤ 600 s (fixture Probe C is seconds; residual = aggregate + Probe M build). If over → data-gated `timeout_seconds` raise. Only on PASS resume the combined re-materialize GATE 0.

## Self-review
- Spec §2.1→T2, §2.2→T3, §2.3→T4, §2.5→T5, §6→T6, R6→T8. ✓
- IMPORT-DIRECTION correction surfaced (T1.1: module in `ingestion`, not `analytics` as spec R1 loosely said) — lint-imports is the gate. ✓
- Fixture non-vacuity (T1.2) + packaging (T1.3) are pre-edit research gates. ✓
- AC untouched (§2.6a); wheel 0.5.121; R6 the measurement proof. ✓
