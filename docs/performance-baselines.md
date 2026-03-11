# Performance Baselines

Measured on: 2026-03-10 (branch: perf/initial-optimization-audit)

## Function Benchmarks (pytest-benchmark)

| Function | Input Size | Median | p95 |
|----------|-----------|--------|-----|
| compute_pitch_control_at_points | 60 targets, 22 players | 372 µs | TBD |
| compute_off_ball_xt_frame | 22 players, 12x8 grid | 521 µs | TBD |
| assign_defensive_credits | 11 defenders | 761 µs | TBD |
| detect_line_breaking | 10 opponents | 458 µs | TBD |

## Pipeline Timing (Databricks Serverless)

| Pipeline | Pre-Optimization | Post-Optimization | Change |
|----------|-----------------|-------------------|--------|
| compute_off_ball_xt | TBD | TBD | TBD |
| compute_defcon_lite | TBD | TBD | TBD |
| compute_spadl_vaep | TBD | TBD | TBD |
| compute_embeddings | OOM (failed) | TBD | TBD |
