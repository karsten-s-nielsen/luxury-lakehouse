# Plan — Serverless-safe parquet read for the preflight canary

Implements `docs/superpowers/specs/2026-10-10-serverless-safe-canary-parquet-read-design.md`. Branch `fix/serverless-safe-canary-parquet-read` off `origin/main`. One coherent commit. No Databricks job until post-merge CI green (R7 is a separate post-merge approval). Implement INLINE.

## Global constraints
- Dtype-neutral swap (hard): the new read MUST produce frames byte-identical to `pd.read_parquet` — `_oracle` transport-neutrality + AC mini-golden stay green, and R-EQ proves it. numpy-backed (default `to_pandas()`), NOT Arrow-backed.
- TEST-FIRST (red-green) for R-GUARD: watch it FAIL on the current `pd.read_parquet` code before the swap.
- 7 local gates + `validate_workflow_cards` + the Docker legs.

## Task 0 — confirm the serverless constraint (research, no edit)
- **T0.0** Base branch: `origin/main` = `a1b973d2` (#609 MERGED) — contains `tracking_marts_canary` + `canary_fixture/` (verified via `git cat-file -e`), so `score_fixture_marts` + the R7 preflight exist on the base. `git fetch && pull --ff-only` before branching.
- **T0.1** Confirm `preflight_tracking_marts`'s `environment_key` (expected `analytics`) and that the `analytics` serverless environment does NOT pin pyarrow (→ base-runtime 8.0.0 stands). Locate the `environments {}` spec (job resource) — record the dependency list.
- **T0.2** Confirm `pyarrow.parquet.read_table(path).to_pandas()` default is numpy-backed (not ArrowExtensionArray) in this pandas, so the swap is dtype-neutral. (If Arrow-backed, pin `types_mapper=None`.)

## Task 1 — R-AUDIT: enumerate every `src/` parquet read MECHANICALLY + classify by compute
- `grep -rn "pd\.read_parquet" src/` (mechanical — the seed list already undercounted). Current set: `parquet_sources.py` (×5, incl FrameSource sb360 `:57`), `player_embeddings_v2.py` (×2), `skillcorner_events.py` (BytesIO), `tracking_frame_spill.py:147` (`read_built_frames`).
- For each, determine the `environment_key`/compute. `tracking_frame_spill.py:147`: confirm prod Stage-2 reads via Spark `built_frame_struct_type` (processor imports that, not `read_built_frames`) and `:147` is local/test → record as safe.
- Serverless-reachable sites → fold into T2's primitive. Non-serverless (HF Jobs / classic, modern pyarrow) → record site + compute + why safe (commit body) AND add to the R-GATE allowlist (T2.5). **No silent deferral.**

## Task 2 — R1/R2: engine-agnostic read primitive, single-sourced — `parquet_sources.py`
- **T2.a (red)** R-GUARD test `test_canary_parquet_read_is_engine_agnostic`: monkeypatch `pandas.read_parquet` → raises; `score_fixture_marts()` + each Source adapter still succeed. FAILS on current code.
- **T2.b (green)** Add `_read_parquet(path) -> pd.DataFrame` = `pyarrow.parquet.read_table(path).to_pandas()` (numpy-backed). Replace ALL FIVE `pd.read_parquet` call sites — FrameSource `:49` tracking **and** `:57` sb360, Actions `:65`, Xt `:82`, Metadata `:98` — plus any other serverless-reachable site from T1. `import pyarrow.parquet` at module top (pyarrow always present on serverless). R-GUARD passes.
- **T2.c** R-EQ test: on pyarrow ≥ 10, `_read_parquet(f)` `assert_frame_equal` (check_dtype=True) to `pd.read_parquet(f)` for a committed fixture → neutrality.

## Task 2.5 — R-GATE: mechanical `pd.read_parquet` ban (the durable class-killer)
- **T2.5.a (red)** `test_no_pd_read_parquet_on_serverless_paths.py` (pattern: `test_topandas_boundedness.py`): scan `src/` for `pd.read_parquet`; FAIL on any occurrence not in a justified allowlist (e.g. `_parquet_read_exemptions.yml` or an in-test list, each with a reason). Fails NOW (5 sites in `parquet_sources.py` + the audited others).
- **T2.5.b (green)** After T2.b, `parquet_sources.py` has ZERO `pd.read_parquet`; populate the allowlist with ONLY the R-AUDIT-confirmed non-serverless sites (`player_embeddings_v2`, `skillcorner_events`, `tracking_frame_spill` — each justified). Gate passes; a new `pd.read_parquet` fails CI until justified. (If editing an exemptions YAML, splice text — no `safe_load`/`safe_dump` round-trip.)

## Task 3 — R-PARITY: Docker serverless-parity leg
- Extend `docker/pyspark-test/` (new test or compose leg) pinned `pyarrow==8.0.0`: run `run_fixture_probe_c` / `score_fixture_marts` END-TO-END (read + 6 scorers) → asserts all-6 ≥1 under serverless's pyarrow. Document the run command (mirrors `test_tracking_marts_canary_spark.py`'s header). This is what catches the class pre-merge (read AND scorer-level).

## Task 4 — R-DOC
- AGENTS.md: a Class-1 invariant (serverless = base-runtime pkg versions, not uv.lock; serverless-reachable parquet IO uses the engine-agnostic primitive, never `pd.read_parquet`) + its `docs/context` pointer. Respect the anti-bloat budget test.

## Task 5 — ADR note (spec §6)
- Append to ADR-087: engine-agnostic fixture read + the `local-env ≠ serverless-env` lesson + the serverless-parity test requirement. TODO.md updated in-commit.

## Task 6 — wheel bump + gates + ship (approval-gated)
- **T6.1** Wheel 0.5.121 → **0.5.122** (pyproject + `bump_wheel.py` + uv.lock root); `--check`.
- **T6.2** 7 gates + `validate_workflow_cards` + R-GUARD/R-EQ/R-GATE + the `_oracle`/mini-golden suites + Docker (existing Spark legs + the new pyarrow-8 parity leg). Format-check ALL changed files (lesson: last cycle 2 files slipped the format hook).
- **T6.3** STOP at commit — diff/file list, await approval → push → PR (spec+plan bundled) → CI → `/review-impl` (2 reviewers) → merge `--admin` → post-merge CI.

## Task 7 — POST-MERGE R7 (SEPARATE approval, operator)
Re-run `preflight_tracking_marts --full --providers idsse` → SUCCESS, ≤ 600 s (now reads via the engine-agnostic primitive on serverless pyarrow 8). Only on PASS resume the combined re-materialize at GATE 0.

## Self-review
- Spec R1/R2→T2 (all 5 sites), R3→(no env change), R-AUDIT→T1 (mechanical), R-GUARD→T2.a, R-GATE→T2.5, R-EQ→T2.c, R-PARITY→T3, R-DOC→T4, R7→T7. ✓
- Dtype-neutrality guarded by R-EQ + the unchanged goldens. ✓
- Round-2 fixes folded: 5 call sites (incl sb360 `:57`); R-AUDIT mechanical + `tracking_frame_spill:147`; the mechanical R-GATE added; branch base verified on main (`a1b973d2`). ✓
- The class-killer = single-sourced engine-agnostic primitive (T2) + env-independent guard (T2.a) + **mechanical CI ban/allowlist (T2.5)** + serverless-parity Docker leg (T3) + the documented invariant (T4). ✓
