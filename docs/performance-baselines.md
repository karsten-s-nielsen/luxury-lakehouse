# Performance Baselines

Initial: 2026-03-10 (branch: perf/initial-optimization-audit)
Updated: 2026-03-31 (branch: feature/cycle2-training)
Updated: 2026-05-02 (branch: chore/opt-1-config-docs-hygiene) — OPT-1 cycle refresh, full benchmark re-run + 30-day pipeline audit

## Function Benchmarks (pytest-benchmark)

Measured locally on Windows 11, Python 3.10.19, NumPy 2.2.6, SciPy 1.15.3, PyTorch 2.5.1.
Source: `uv run pytest src/tests/test_benchmarks.py src/tests/test_shape_graph.py src/tests/test_taipy_workflows_perf.py --benchmark-only` (21 benchmarks). Note: column previously labeled "p95" replaced with **IQR** (interquartile range), which is what pytest-benchmark reports directly. Max column flagged where outliers (GC pause, JIT cold start) dominate.

| Function | Input Size | Median | IQR | Max | Budget |
|----------|-----------|--------|-----|-----|--------|
| compute_pitch_control_at_points (batched) | 22 targets, 22 players | 341.5 µs | 34.4 µs | 1,753 µs | ≤5 ms |
| compute_off_ball_xt_frame | 22 players, 12×8 grid | 502.3 µs | 63.5 µs | 2,558 µs | — |
| assign_defensive_credits | 11 defenders | 410.3 µs | 31.1 µs | 2,766 µs | — |
| detect_line_breaking | 10 opponents | 457.0 µs | 77.6 µs | 2,406 µs | ≤2 ms |
| perturb_positions | 10 perturbations | 1,000.2 µs | 157.6 µs | 2,884 µs | ≤5 ms |
| compute_obso_surface | 104×68 grid | 523.8 µs | 54.9 µs | 1,953 µs | ≤5 ms |
| compute_team_shape | 10 outfield players | 514.7 µs | 110.4 µs | 3,194 µs | ≤1 ms |
| compute_team_shape_frame | 22 players (both teams) | 1,379.1 µs | 185.1 µs | 8,822 µs | ≤2 ms |
| compute_shape_graph | 10 outfield players | 1,188.0 µs | 121.7 µs | 3,998 µs | ≤2 ms |
| compute_shape_graph_10_players | 10 outfield players (alt fixture) | 1,228.6 µs | 152.4 µs | 3,599 µs | ≤2 ms |
| infer_positions | 10 outfield players + shape graph | 1,820.6 µs | 195.3 µs | 9,591 µs | ≤3 ms |
| infer_positions_10_players | 10 outfield players (alt fixture) | 1,450.0 µs | 138.2 µs | 7,705 µs | ≤3 ms |
| numba_pitch_control (warm) | 22 players, single point | 1.5 µs | 0.1 µs | 117 µs | — |
| scoutgpt_getitem | single sample | 10.1 µs | 0.3 µs | 1,752 µs | — |
| f2v_getitem | single sample | 34.6 µs | 1.7 µs | 136 µs | — |
| f2v360_getitem | single sample | 34.9 µs | 4.7 µs | 382 µs | — |
| scoutgpt_forward | full batch | 1,285.5 µs | 317.5 µs | 2,465 µs | — |
| f2v_forward | full batch | 981.4 µs | 177.4 µs | 1,456 µs | — |
| f2v360_forward | full batch | 1,026.4 µs | 208.7 µs | 1,541 µs | — |
| wf_filter_change (Taipy state) | filter selectbox change | 706.7 µs | 152.0 µs | 3,710 µs | ≤500 ms (UI cached) |
| wf_refresh (Taipy state) | full refresh | 1,567.1 µs | 573.6 µs | 212,049 µs[†] | ≤3 s (UI first load) |

[†] `wf_refresh` Max column dominated by occasional GC pauses on the heavy fixture; see test source for the bounded-fixture variant.

## Pipeline Timing (Databricks Serverless)

Measured from `observability.system_lakeflow_job_task_run_timeline` (definer's-rights view per ADR-007), 30-day window 2026-04-03 → 2026-05-02 filtered to `result_state = 'SUCCEEDED'`. Daily-job id `980461192099048`. Per-task numbers reflect the post-PR-Cycle-A IDSSE fan-out + pre-PR-γ synced-table state. p50/p95 in seconds.

| Pipeline | p50 | p95 | Max | Runs | Notes |
|----------|----:|----:|----:|----:|-------|
| backfill_statsbomb_360 | 112 | 744 | 1,263 | 60 | Per-extra StatsBomb 360 freeze frames |
| backfill_statsbomb_extra | 105 | 1,168 | 1,551 | 37 | StatsBomb extra metadata backfill |
| compute_defcon_lite | 312 | 450 | 929 | 51 | applyInPandas, 360 freeze frames |
| compute_elastic_sync | 85 | 114 | 186 | 64 | 7 IDSSE matches |
| compute_embeddings_360 | 57 | 102 | 119 | 16 | 360-aware behavioral embedding refresh |
| compute_embeddings_v1 | 149 | 262 | 291 | 62 | Doc2Vec inference (legacy baseline) |
| compute_embeddings_v2 | 106 | 192 | 284 | 65 | v2 transformer embeddings (cross_attention default since PR #176) |
| compute_expected_threat | 124 | 198 | 208 | 51 | xT grid (v1 in production; ExT v2 Phase 0+1 reproduction landed PR #206/#213) |
| compute_formations_efpi | 258 | 423 | 473 | 60 | EFPI template matching |
| compute_formations_shape_graph | 242 | 349 | 545 | 58 | Shape graph detection |
| compute_line_breaking | 202 | 250 | 350 | 72 | Line-breaking pass detection |
| compute_off_ball_xt | 107 | 163 | 178 | 48 | applyInPandas, 1fps sampling |
| compute_pausa | 87 | 157 | 295 | 62 | OBSO + ghost trajectories, 7 IDSSE matches |
| compute_pitch_control | 114 | 164 | 683 | 64 | applyInPandas, 20 tracking matches |
| compute_spadl_vaep | 246 | 779 | 1,043 | 56 | applyInPandas; post-Kimball-PR-7 (PR #214) shifted higher than 2026-03-25 baseline (was 168s p50) |
| compute_xg_model | 294 | 419 | 483 | 50 | XGBoost training + scoring |
| compute_xg_model_v2 | 300 | 430 | 480 | 46 | RETIRED 2026-07-10 (v2 producer chain, ADR-066); historical baseline. Successor: compute_xg_shot_scores (xg_model_v3) |
| dbt_build_input_marts | — | — | — | 0 | New from PR-Cycle-C PR-β 2026-05-02; no measurements yet |
| dbt_build_intermediate_marts | — | — | — | 0 | New from PR-Cycle-C PR-β 2026-05-02 |
| dbt_build_output_marts | — | — | — | 0 | New from PR-Cycle-C PR-β 2026-05-02 |
| extract_tracking_metadata | 178 | 515 | 540 | 40 | IDSSE + SkillCorner tracking metadata |
| hf_sync | 139 | 191 | 205 | 23 | HF Hub bidirectional sync |
| import_obso_results | 120 | 176 | 179 | 18 | Split out of hf_sync in PR-Cycle-B (2026-05-01) |
| ingest_idsse_events | 87 | 123 | 188 | 65 | XML events |
| ingest_idsse_iteration | 229 | 477 | 479 | 9 | Per-match IDSSE fan-out (PR-Cycle-A PR #231, 2026-04-30) |
| ingest_metrica | 171 | 199 | 395 | 69 | 3 matches |
| ingest_skillcorner | 170 | 197 | 787 | 69 | 10 matches; tail risk close to 900s ceiling — timeout grown 900→1200 in OPT-1 |
| ingest_statsbomb | 170 | 200 | 744 | 69 | 5 competitions, ~3,000 matches; tail risk — timeout grown 900→1200 |
| ingest_wyscout | 166 | 199 | 748 | 68 | 3 competitions; tail risk — timeout grown 900→1200 |
| preflight_idsse | 74 | 81 | 83 | 9 | IDSSE preflight (Cycle A PR #231) |
| refresh_synced_tables | 855 | 2,227 | 2,379 | 3 | Pre-PR-γ; 3-table TRIGGERED+CDF pilot (2026-05-01) expected to drop wall-clock materially |
| resolve_players | 293 | 676 | 729 | 65 | TF-IDF + rapidfuzz, ~12K players; tail risk — timeout grown 900→1200 |
| run_model_validation | 96 | 150 | 176 | 59 | PSI, Wasserstein, KS across 10 models |

## Major changes since 2026-03-31

- **2026-04-21/22 ScoutGPT cross_attention promoted** (PR #176, default flipped from `additive`); Fourier kept as enum alternative. Wheel 0.3.10 → 0.3.11. See `docs/evolve/cross-attention-promote/SUMMARY.md`.
- **2026-04-22 XG2 production unblock + ADR-012** (PR #177). Daily `compute_xg_model_v2` back to SUCCESS after 7-day failure; new `ingestion.artifact_deploy` module codifies training→production delivery contract (`require_mlflow_env`, `set_and_verify_mlflow_champion`, `upload_weights_to_uc_volume`). Wheel 0.3.11 → 0.3.12.
- **2026-04-23/25 Football2Vec L2 adversarial harvest** (PR #201). 6-seed sweep — no promotions; `docs/engineering/orchestration.md` 7-rule hardening + ADR-002 §5 lineage are the durable deliverables.
- **2026-04-26 ExT v2 Phase 0** (PR #206). Singh-2018 baseline NLL **3.78924** held-out, 8.8M actions, 5,404 matches, 22 competitions; −17% vs uniform `log(96)=4.564`.
- **2026-04-27 ExT v2 Phase 1** (PR #213). KDE-smoothed Singh NLL **3.74823** (+1.082% over Phase 0; bandwidth saturated upper edge of `[0.01, 2.0]` — Phase 2 widens prior). Phase 2 stop condition pre-registered: `3.71 ≤ nll_primary ≤ 3.79`.
- **2026-04-27/28 Kimball PR 7 + 6 hotfixes** (#214 + #215–#220). `fct_action_values` rebuild times shifted; visible in `compute_spadl_vaep` p50 going 168s → 246s vs 2026-03-25 baseline.
- **2026-04-30 PR-Cycle-A** (#231). IDSSE `for_each_task` fan-out — `ingest_idsse` replaced by `preflight_idsse` + `ingest_idsse_iteration`; eliminated cross-source-of-truth sequential timeout.
- **2026-05-01/02 PR-Cycle-C** (#243 PR-α + #247 PR-β). Single `dbt_build` task split into 3 sequential tasks (`dbt_build_input_marts` / `_intermediate_marts` / `_output_marts`) per ADR-019; PR-β phase-0 fixed `compute_pausa` race vs `hf_sync`; ADR-020 CAN_RUN auto-heal step added to Lakebase Maintenance workflow.
- **2026-05-02 OPT-1** (this branch). 25 `timeout_seconds` right-sizing edits across the 33-task daily-job per the live audit above (21 shrinks + 4 grows for tail-risk ingest tasks); 5 `max_retries=1` additions on previously-retryless tasks (3 dbt_build_* + hf_sync + refresh_synced_tables).
