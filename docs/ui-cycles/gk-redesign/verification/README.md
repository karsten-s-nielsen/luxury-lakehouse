# GK redesign — live data verification scripts

These read-only query scripts back the load-bearing claims in
[`../../../superpowers/specs/2026-06-21-gk-insight-views-redesign-design.md`](../../../superpowers/specs/2026-06-21-gk-insight-views-redesign-design.md).
Run any with `uv run <script>` (PEP-723 deps inline; needs `DATABRICKS_HOST` /
`DATABRICKS_HTTP_PATH` / `DATABRICKS_TOKEN` in env). All query `soccer_analytics.dev_gold`
read-only. The `*_out.txt` files are the **2026-06-21 captured results**; re-run to re-confirm.

| Script | Verifies | Key 2026-06-21 result |
|---|---|---|
| `gk_volume_check.py` | distribution volume / GK (offensive) + sweeper-family non-null vs shots-faced (defensive) | Sweeper metrics (`gk_pitch_control_share_weighted`/`gk_reachable_area_m2`/`gk_closing_time_*`) non-null on ~100% of defended actions: GS 873 / IDSSE 843 / SkillCorner 145 avg/GK vs avg 2.8–16 shots → **sweeper-led defensive hero is grounded**. Distributions/GK: GS 55.8 / IDSSE 35.4 / SC 9.0. |
| `gk_review_verify.py` | B1 xT-GK sign + components; C2a canonical span; C3 variance cols; C2b provider effect | **B1**: `xt_gk` avg −0.019, 83% negative; `xt_gk_base` drives it, `xt_gk_rav` +89% positive, `gk_completion` 0.751. **C2b**: provider effect real — pc-share **0.137 GS vs 0.200 SC**, closing 1.54 vs ~1.8 s → **per-provider cohort**. **C3**: `fct_gk_shot_stopping_pooled` has `ci_low/high` (no variance); `fct_gk_shot_stopping` has `psxg_variance_sum`. |
| `gk_probe2.py` | where tracking goals-prevented actually lives | **`fct_goalkeeper_stats` = statsbomb + wyscout ONLY (no tracking rows)** — do NOT use for this page. Tracking goals-prevented = `fct_gk_shot_stopping` (GS 112 / SC 27 / IDSSE 6 rows). |
| `dist_value_degeneracy_check.py` | **(2026-06-22)** whether the OFFENSIVE 6-preset fit-ladder hero carries per-keeper signal | **It does NOT.** Presets never reorder keepers (Spearman ρ 0.99; Counter best for 55/62), level dominates the preset axis 3.4×, xT-GK 97% negative; the vs-cohort-delta "fix" residual is noise (0.0018). → **fit-ladder is dead; recommend a 2-axis distribution profile (%-adds-threat × completion↔progression).** Full writeup: [`2026-06-22-distribution-value-degeneracy-finding.md`](2026-06-22-distribution-value-degeneracy-finding.md). Output: `dist_value_degeneracy_out.txt`. Uses SDK `WorkspaceClient` (PAT via `~/.databrickscfg`), not the `DATABRICKS_HTTP_PATH` env path the others use. |

Originally authored under the planning session's `.superpowers/brainstorm/`; copied here so the
implementing session has them in-repo. The 2026-06-22 distribution-value finding was added later
(it overturns the locked offensive "fit-ladder" hero in the spec — see the writeup).
