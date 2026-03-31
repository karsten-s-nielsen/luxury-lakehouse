# Performance Baselines

Initial: 2026-03-10 (branch: perf/initial-optimization-audit)
Updated: 2026-03-31 (branch: feature/cycle2-training)

## Function Benchmarks (pytest-benchmark)

Measured locally on Windows 11, Python 3.10.19, NumPy 2.2.6, SciPy 1.15.3.

| Function | Input Size | Median | p95 | Budget |
|----------|-----------|--------|-----|--------|
| compute_pitch_control_at_points | 60 targets, 22 players | 347 µs | 512 µs | ≤5 ms |
| compute_off_ball_xt_frame | 22 players, 12×8 grid | 482 µs | 770 µs | — |
| assign_defensive_credits | 11 defenders | 420 µs | 799 µs | — |
| detect_line_breaking | 10 opponents | 445 µs | 719 µs | ≤2 ms |
| perturb_positions | 10 perturbations | 998 µs | 1,527 µs | ≤5 ms |
| compute_obso_surface | 104×68 grid | 529 µs | 814 µs | ≤5 ms |
| compute_team_shape | 10 outfield players | 479 µs | 921 µs | ≤1 ms |
| compute_team_shape_frame | 22 players (both teams) | 1,388 µs | 2,610 µs | ≤2 ms |
| compute_shape_graph | 10 outfield players | 1,158 µs | 1,098 µs | ≤2 ms |
| infer_positions | 10 outfield players + shape graph | 1,787 µs | 1,683 µs | ≤3 ms |
| numba_pitch_control (warm) | 22 players, single point | 2 µs | 2 µs | — |

## Pipeline Timing (Databricks Serverless)

Measured from job run 311181772997773 (2026-03-25). All pipelines run with skip guards — timings include Spark session startup, data read, compute, and Delta write. Idempotent re-runs with no new data complete faster (skip guard exits early).

| Pipeline | Wall Clock | Status | Notes |
|----------|-----------|--------|-------|
| ingest_statsbomb | 121s | ✅ | 5 competitions, ~3,000 matches |
| ingest_metrica | 118s | ✅ | 3 matches |
| ingest_wyscout | 122s | ✅ | 3 competitions |
| ingest_idsse | 116s | ✅ | 7 matches (DFL XML) |
| ingest_skillcorner | 117s | ✅ | 10 matches |
| ingest_idsse_events | 44s | ✅ | 7 matches (XML events) |
| compute_spadl_vaep | 168s | ✅ | Was OOM pre-optimization; now per-partition |
| compute_off_ball_xt | 89s | ✅ | applyInPandas, 1fps sampling |
| compute_defcon_lite | 134s | ✅ | applyInPandas, 360 freeze frames |
| compute_embeddings | 221s | ✅ | Was OOM pre-optimization; now per-competition |
| compute_pitch_control | 87s | ✅ | applyInPandas, 20 tracking matches |
| compute_elastic_sync | 67s | ✅ | 7 IDSSE matches |
| compute_pausa | 47s | ✅ | 7 IDSSE matches (OBSO + ghost trajectories) |
| compute_expected_threat | 58s | ✅ | Markov chain, 2.2M SPADL actions |
| compute_xg_model | 128s | ✅ | XGBoost training + scoring |
| resolve_players | 135s | ✅ | TF-IDF + rapidfuzz, ~12K players |
| run_model_validation | 77s | ✅ | PSI, Wasserstein, KS across 10 models |
| compute_formations | 186s (EFPI only) | ⚠️ | Pre-Cycle 2 timing. Now runs dual-detector (EFPI + shape graph) with temp Delta table materialization to avoid double-read of tracking data. Re-measure needed. |
| compute_line_breaking | N/A | ✅ | Line-breaking pass detection |
| export_embeddings_training_data | N/A | ✅ | SPADL sequence export → UC Volume → HF Hub (Cycle 2) |
| **Full workflow** | **486s** | ⚠️ | Pre-Cycle 2 timing (19 tasks). Now 21 tasks with xg_model_v2 + formations dual-detector. |
