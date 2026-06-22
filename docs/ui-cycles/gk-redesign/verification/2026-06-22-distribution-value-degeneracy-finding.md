# GK "Distribution Value" view — data investigation & redesign recommendation

_Investigated 2026-06-22 against live Databricks (`soccer_analytics.dev_gold`, warehouse `soccer-analytics-warehouse-dev` / `6c3b36ca64d183fe`). WC cohort = 62 keepers, 3,458 distributions. Method: drove the running app (`localhost:7860/Goalkeeper-Analytics`) via Puppeteer to observe the rendered view, then queried the marts directly. Reproducible script: `dist_value_degeneracy_check.py` (this folder)._

## TL;DR
The current 6-preset "which game model best fits his passing?" ladder is **statistically degenerate** and cannot be salvaged by reframing the same six columns. An earlier suggested fix (per-model vs-cohort-median delta ladder) **also fails**. Replace it with a **two-axis cohort-positioned distribution profile** built on signals that actually vary. This view is **WC-only** given current data.

## 1. The current design is dead — three independent confirmations (WC, n=62)
| Test | Result | Meaning |
|---|---|---|
| Keeper rank-correlation across preset pairs | Spearman ρ = 0.985–0.995 | The presets essentially **never reorder keepers** |
| Best-fit model across the cohort | **Counter for 55 of 62** (89%) | "Best-fit" is a near-constant label |
| Between-keeper level SD vs within-keeper preset spread | 0.0152 vs 0.0045 → **3.4×** | The preset axis is ~⅓ the size of the thing it's drowning out |

Structural, not a bug: the six presets are scalar reparameterizations (δ, η) of one formula (`src/analytics/action_context/enrich.py:448`, `XtGkParams.for_philosophy`), so they rescale every keeper monotonically. xT-GK is **97% negative** (mean −0.0226), so "best-fit" = "least-negative preset" = always Counter.

## 2. The suggested reframe (per-model vs-cohort-median ladder) ALSO fails
De-leveled, the per-model "shape" residual SD = **0.0018 ≈ noise** (vs 0.0152 of level repeated six times). **Conclusion: drop any view built on the six preset columns.**

## 3. What the data CAN support — two real, ~orthogonal axes (action grain, 39 keepers with n≥20)
| Signal | Range across keepers | CV | Verdict |
|---|---|---|---|
| **% of distributions that add threat** (xt_gk>0) | 3% → 24% (mean 12.6%) | 0.46 | **Best headline** — interpretable; r=0.80 w/ value, r=0.93 w/ DZV |
| value per distribution (raw xT-GK) | −0.034 → −0.013 | 0.21 | True signal but a tiny negative number — use as tooltip |
| **completion% ↔ progression distance** | 62–87% / 14–33m, **r = −0.91** | — | ONE "style" axis: short-safe vs long-direct |
| PEV component | ≈ 0.0000 | — | **Inert by construction — must not be displayed** |
| DZV (danger-zone) | drives value, r=0.93 | — | The mechanism behind the threat axis |

Threat (A) and style (B) are nearly independent (value↔completion r=0.39) → genuine 2-D story:
- **Onana** — adds threat 24%, mid-completion, long (22m): proactive distributor
- **Alisson** — safest & shortest (87%, 14m), lower threat: secure recycler
- **Keylor Navas** — long (26m), low completion (65%), adds threat only 6%: risk without reward

## 4. RECOMMENDATION
Replace the game-model fit ladder with a **cohort-positioned 2-axis distribution profile**:
- **Axis A — distribution threat:** "% of distributions that add threat" vs WC cohort band (raw xT-GK as tooltip).
- **Axis B — distribution style:** completion% ↔ progression distance (one safe↔direct axis).
- Volume (n_distributions) as point size / sample-confidence.
- **Sample floor n≥20** — only 39/62 WC keepers qualify (32 at ≥40).
- **Drop PEV everywhere.**

**Scope finding (drives per-competition behavior):** WC (`gradientsports`) is the ONLY cohort with both breadth and per-keeper volume. A-League (`skillcorner`) has 61 keepers but ~9 distributions each (below any floor); 2.Bundesliga/Metrica/1.Bundesliga have ≤12 keepers. For those, show a single value with "cohort too small / too few distributions," NOT a profile.

Cohort sizes:
| competition | source | keepers | total dist |
|---|---|---|---|
| FIFA Men's World Cup | gradientsports | 62 | 3458 |
| A-League | skillcorner | 61 | 547 |
| 2. Bundesliga (DFL) | idsse | 12 | 461 |
| Metrica Sample | metrica | 6 | 117 |
| 1. Bundesliga (DFL) | idsse | 5 | 141 |

## 5. Affected code / specs
- `services/gk_insight.py` — `fit_ladder()` / `offensive_verdict()` are candidates for removal/replacement.
- `queries/gk_analytics.py` — `_DIST_MODEL_COLS`, `build_distribution_stats_sql` (the 6 preset means).
- Spec `docs/superpowers/specs/2026-06-21-gk-insight-views-redesign-design.md` — the locked offensive "System Fit / Mis-deployed" Big Story + fit-ladder hero is overturned by this finding.

## 6. Reviewer facts
- xT-GK 97% negative (mean −0.0226, median −0.0232, range −0.0792…+0.0324). Mean is a poor headline; use "% adds threat."
- Preset best-fit Counter for 89% of cohort; the 7 exceptions are tiny-sample.
- completion & progression are the SAME axis (r=−0.91). Don't show both as independent.
- PEV = 0 by construction on unpressured restarts (silly-kicks verdict 2026-06-11; see `fct_gk_tracking_actions.xt_gk_pev` column comment).
- Marts used: `fct_gk_tracking_stats` (per gk×match means), `fct_gk_tracking_actions` (action grain w/ xt_gk + components base/rav/dzv/pev/pressure + coords + gk_completion).
- Repro note: the `~/.databrickscfg` DEFAULT PAT was rejected by the SQL-connector Thrift handshake; the SDK `WorkspaceClient.statement_execution` path (used by `dist_value_degeneracy_check.py`) works.
