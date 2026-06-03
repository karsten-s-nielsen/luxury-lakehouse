# Design: action-context GK metrics (xShotOccurrence + gk_influence zones) + SB360 freeze-frame coverage expansion

| Field | Value |
|---|---|
| **Date** | 2026-06-03 |
| **Status** | Draft (brainstorming output; revised after external review — pending re-review) |
| **Branch** | `feat/ac1-gk-metrics-sb360-coverage` |
| **silly-kicks** | 4.9.1 (floor bumped from 4.9.0 — adds the DAS empty-frame-batch fix; xS + gk-zone APIs unchanged) |
| **ADR** | ADR-039 (new) |

## 1. Context & goals

silly-kicks 4.9.0 ships the trained **xShotOccurrence (xS / TF-16)** model — the xS sub-model of
**Pipping-Gamón, Feng & Sabin (2026), arXiv:2512.00203, "Beyond Expected Goals: A Probabilistic Framework for
Shot Occurrences in Soccer"** (weights bundled as `silly-kicks/xshot-occurrence-v1`) — plus the
`gk_influence` per-zone API. AC-1 does not yet consume xS, and `add_gk_influence` runs with its default
single zone (`six_yard_box`). Goals:

1. **GK metrics** — persist xShotOccurrence and the full `gk_influence` zone set on `fct_action_context`
   for all tracking providers, via the established ghost-GK touchpoint pattern.
2. **SB360 coverage expansion** — wire every action-context enrich step the StatsBomb-360 freeze-frame
   data **empirically supports** (measured on match 3835328 via `tmp/sb360_tierb_probe.py`) into the
   SB360 path — partial/sparse, honest NULL, with explicit pitch-control-method provenance.

## 2. New columns (6)

| Column | Type | Producer | Notes |
|---|---|---|---|
| `gk_closing_time_mean_s__near_post` | DOUBLE | `add_gk_influence(zone_names=…)` | mean-before-min per existing DDL order |
| `gk_closing_time_min_s__near_post` | DOUBLE | same | |
| `gk_closing_time_mean_s__far_post` | DOUBLE | same | |
| `gk_closing_time_min_s__far_post` | DOUBLE | same | |
| `xshot_occurrence` | DOUBLE | `add_xshot_occurrence` (bundled `default`, `model=None`) | ∈ [0,1] |
| `pitch_control_method` | STRING | enrich path (C1 provenance) | `'spearman'` (tracking) / `'voronoi'` (SB360) / NULL (event-only) |

The SB360 coverage expansion adds **no further new columns** — it populates *existing*
`fct_action_context` columns (pressure, shape_graph, ghost_gk, gk_*, obso_*, pausa_*, xshot_occurrence)
for SB360 rows that were previously all-NULL.

### 2.1 `pitch_control_method` — C1 provenance (resolves the Hyrum trap)
SB360 computes the pitch-control-dependent metrics (`obso_*`, `pausa_*`, `gk_influence`,
`gk_closing_time_*`, `gk_pitch_control_share_weighted`, `gk_reachable_area_m2`) with **voronoi**
(position-only) pitch control, while the tracking path uses **spearman** (velocity-aware). These land in
the *same* named columns. To avoid a silent cross-provider value-format divergence (ADR-018 spirit; the
repo's "never silently substitute" UX rule), `pitch_control_method` records, per row, which method
produced that row's pitch-control-derived metrics: `'spearman'` for the tracking path, `'voronoi'` for
SB360, NULL for event-only rows (no pitch-control metrics). This makes the divergence **queryable**, not
buried in prose. (`data_source` already discriminates SB360 ⇔ statsbomb, but that coupling is implicit;
this column is explicit.)

**Scope of the label (L-NEW1):** it names the method behind the *persisted* pitch-control-derived
metrics (`obso_*`, `pausa_*`, `gk_influence`/`gk_closing_time_*`/`gk_pitch_control_share_weighted`/
`gk_reachable_area_m2`, plus cover_shadows on tracking) — NOT "the only pitch-control method computed":
the tracking `pitch_control_at_action` step (Step 6) computes all three methods
(spearman/fernandez_bornn/voronoi) for its own columns. A column comment + dataset-card line must scope
it to the persisted metrics so it isn't misread.

## 3. `enrich.py` changes

### 3.1 Tracking path (`_enrich_tracking_match`)
- Step 13 `add_gk_influence(...)`: add `zone_names=["six_yard_box","near_post","far_post"]` (keep
  `method="spearman"` — full tracking has velocity).
- New step after the GK block:
  `out = add_xshot_occurrence(out, frames=tracking_df, model=None, links=links, home_team_id=home_team_id, pitch_control_cache=pc_cache)`
  (L1: reuse the shared pitch-control cache; L2: 2nd param is `frames`).
- Set `out["pitch_control_method"] = "spearman"`.

### 3.2 SB360 path (`_enrich_sb360_match`)
Append the empirically-supported steps after the existing chain. **Pitch-control-dependent steps use
`method='voronoi'` / `pitch_control_method='voronoi'`** (freeze-frames have no velocity; `spearman`
returns all-NaN). All are partial/sparse (honest NULL).

| Step | SB360 call | Measured coverage (1760 actions) |
|---|---|---|
| `add_pressure_on_actor` | `andrienko_oval` (confirmed); add `bekkers_pi` only if it computes without velocity (verify at impl — tracking runs both) | andrienko_oval 1520 |
| `add_shape_graph` | as tracking | defending ~1437–1518 (attacking NULL) |
| `add_ghost_gk` | `model="default", kde_backend="fft-cic"` | 610 |
| `add_gk_influence` | **`method="voronoi"`**, `zone_names=[3 zones]` | 610 (0 with spearman) |
| `add_obso` | **`pitch_control_method="voronoi"`** | 634 |
| `add_pausa` | after obso, **`pitch_control_method="voronoi"`** | 634 |
| `add_xshot_occurrence` | `model=None` | 73 (4% — sparse; see §6.1 policy) |

Then set `out["pitch_control_method"] = "voronoi"`. Excluded (measured all-NaN / structurally
impossible): `add_das`, `add_cover_shadows`, `add_pre_shot_gk_position/angle` (§11 follow-up),
`add_actor_pre_window`, `add_off_ball_context`, `add_space_creation`, `add_elastic_sync`,
`add_sync_score`.

The event-only path leaves `pitch_control_method` NULL.

## 4. Schema / contract / migration
- `schema.py`: add the 6 columns to `RESULT_COLUMNS` + DDL string (mean→min zone order; 5 DOUBLE + 1
  STRING). The applyInPandas StructType is DDL-derived (`_parse_ddl_to_struct_type`) — single source, no
  separate StructType to drift.
- Bronze migration `scripts/migrations/2026-06-03-add-xshot-gk-zones-to-action-context.sql`: idempotent
  `ALTER TABLE bronze.spadl_action_context ADD COLUMNS (...)` (6 cols). **L4 caveat:** `_runner.py`'s
  skip-check inspects only the first column of an `ADD COLUMNS (...)`; one ALTER for all 6 is all-or-
  nothing on a fresh apply (fine here).
- dbt: `stg_action_context__values.sql` (select 6), `fct_action_context.sql` (passthrough + casts: 5
  `double`, 1 `string`), `_marts__models.yml` contract (6 cols).
- **M-NEW2 — `pitch_control_method` is the first non-DOUBLE feature column on this table; the all-DOUBLE
  plumbing must not be assumed.** Verify end-to-end at impl: (a) `_parse_ddl_to_struct_type` maps
  `STRING`→`StringType` for this table; (b) staging casts `as string`; (c) the contract uses
  `data_type: string` (not double); (d) `oracle_map.py` treats it as a categorical invariant (§5), not a
  numeric range; (e) `fct_action_context` is Lakebase-synced — confirm an additive STRING column
  auto-evolves on synced-table refresh (it does, but confirm in one line).

## 5. Range / invariant checks (`oracle_map.py` INVARIANT_ONLY)
- `xshot_occurrence` ∈ [0, 1].
- `gk_closing_time_{mean,min}_s__{near,far}_post` ≥ 0 (NaN-tolerant, matching six_yard_box entries).
- `pitch_control_method` categorical ∈ {`spearman`, `voronoi`} (NULL allowed) — invariant, not ranged.

## 6. Golden re-baseline + SB360 regression (test-first)
- **Full golden (`J03WMX_p1`)** — re-baselined manually (no script exists; only
  `build_ac1_mini_golden.py`): `AC1_E2E=1 uv run pytest …test_e2e.py` to recompute, then freeze the
  parquet (ADR-036 precedent). Run on the **fft-cic** ghost-GK backend (current default after #336) so
  values match serverless. **L-NEW3:** the `AC1_E2E=1` recompute must actually be run and the parquet
  column-diff reviewed (new columns populated, existing columns unchanged) **before** freezing — not
  assumed.
- **Mini golden** — regenerate via `scripts/build_ac1_mini_golden.py`.
- **SB360 regression (M3, test-first):** the SB360 path has no golden/e2e coverage today. Sequence:
  1. Extract + commit a small SB360 fixture (`statsbomb/<match>/{actions,sb360,xt_grid,meta}.parquet`;
     hexagon already supports `tier="sb360"`, parquet_sources.py:50).
  2. Write the supported-vs-NULL assertions first (RED — all NULL today) driven through `run_work_unit`
     (same orchestration as the IDSSE mini-golden; **no bespoke harness**).
  3. Add the 7 SB360 steps one at a time to turn them green.

### 6.1 H2 — SB360 xshot coverage policy
`xshot_occurrence` on SB360 is ~4% non-NULL (freeze-frame sparsity), vs the other SB360 steps at ≥~35%.
It is **kept** (per "best available for the data"), but the ADR + dataset card MUST state that SB360 `xshot_occurrence` (and, to a lesser degree, all SB360 metrics) is a **sparse, non-random subsample** — consumers must not compute naive provider-level averages over it. The `pitch_control_method` column +
NULL semantics are the queryable guard.

## 7. HF publishing
- **Auto-included:** `publish_action_context_hf.py` does `SELECT * FROM dev_gold.fct_action_context` and
  partitions Hive-style by `data_source` (drops only that partition key). All 6 new columns + the
  now-populated SB360 values flow automatically — **no publisher code change**.
- Update the `spadl-action-context` dataset card if it enumerates columns; add the §2.1 + §6.1 notes
  (voronoi-vs-spearman provenance; SB360 sparsity). `test_hf_publish_parity` enforces card↔repo presence.

## 8. Governance & references
- **Academic reference (C2 — verified published):** add the xS author to `ARCHITECTURE.md` § Appendix D
  + `expected_authors` in `test_architecture_md_appendix.py`, **create** a `references:` block on
  `wf-action-context.yaml` (L5 — none today), and add the NOTICE entry. **Use the canonical surname
  `Pipping-Gamón`** (M-NEW1 — arXiv:2512.00203's first author is Jonathan Pipping-Gamón; silly-kicks'
  docstring abbreviates to "Pipping"). The Appendix-D test substring-matches so "Pipping" would pass, but
  the human-facing ARCHITECTURE.md/NOTICE rows must use the accurate name — **verify exact spelling
  against the arXiv:2512.00203 page at impl** before writing it.
- **AI governance (H1 — stated judgment, not deferral):** xS is a **per-action** shot-occurrence
  probability, not a per-player rating. `wf-action-context` is not in `PER_PLAYER_EVALUATIVE_CARDS`
  (consistent with ghost_gk/gk_influence/obso/pausa already on the table) → **no governance change, no
  model card.** The xS model is silly-kicks-bundled, not lakehouse-published.

## 9. Dependencies / version
- silly-kicks **4.9.1** — floor bumped from 4.9.0 (pyproject `[spadl]`, TF env, 6 trainer `_REQUIRED_SK_MIN=(4,9,1)`, sentinel, submit/retrain) to adopt the **DAS empty-frame-batch fix** (guards accessible-space's `None` `simulation_result` on a zero-frame subset — the GS-10502 class). The xS + gk_influence-zone APIs are unchanged from 4.9.0.
- `xgboost-cpu==3.2.0` already in the analytics env / wheel `[analytics]` / `submit_ac1_oneshot` —
  satisfies `add_xshot_occurrence`'s `import xgboost`. No new dependency. (Do NOT add silly-kicks'
  `[xgboost]` extra — full GPU xgboost, conflicts with the deliberate xgboost-cpu.)
- Wheel bump 0.5.14 → 0.5.15 via `bump_wheel.py`. Check `test_silly_kicks_boundary.py` for new-xfn coverage.

## 10. Testing
- `test_action_context_enrichment.py`: assert the 6 new columns exist + in range on the tracking fixture.
- **H3 — real (unpatched) xS smoke:** invoke `add_xshot_occurrence(model=None)` (bundled model) on the
  fixture and assert finite ∈ [0,1] — exercises the xgboost 2.1.4-trained → 3.2.0-runtime load path.
  (Pre-confirmed: the probe got 73 finite values on xgboost-cpu 3.2.0; this codifies it.)
- SB360 enrichment test (§6, test-first): supported metrics populate (non-NULL on their subset),
  excluded ones NULL, `pitch_control_method='voronoi'`.
- `test_action_context_live_ddl_parity.py`: covered by schema edits. Full `uv run pytest` + ruff +
  pyright; `AC1_E2E=1` e2e; mini-golden gate.

## 11. Bronze-migration application (operator-side this PR; CI re-wiring split out — H-NEW)
`scripts/migrations/_runner.py` exists but is wired into **no** CI workflow on `main`. The previous
"Apply pending bronze migrations" step (added in #236, removed in `7497458` when `dbt-live-ci.yml` moved
from per-PR to a **daily schedule**) was diff-based (`git diff --diff-filter=A origin/main...HEAD`),
which is a **silent no-op on a scheduled run against main** (the diff is empty). Re-adding it verbatim
(as the v1 spec proposed) would look wired while doing nothing — worse than today's honest gap.

**This PR (decision: option 3) — operator-applied:** apply the one migration operator-side post-merge
(the documented fallback) — run the `ALTER TABLE bronze.spadl_action_context ADD COLUMNS (...)` via
`_runner.py` (or directly). It MUST run **before** the AC-1 compute writes the new columns AND **before**
the next live `dbt build` selects them, or the live build fails (the chicken-and-egg #236 addressed). See
§13 sequencing. The feature PR does not touch the migration-runner plumbing.

**Separate follow-up PR (out of scope here):** design + test the CI re-wiring properly — either (1) route
the diff-based step to a push/PR-triggered, warehouse-capable workflow (`dbt-ci.yml` triggers on
`push:[main]` + `pull_request` and already runs `--diff-filter=A origin/main..HEAD` at line 79 — *verify
it has Databricks creds to ALTER bronze*; slim CI may not connect), or (2) make the daily job apply
**all** `scripts/migrations/*.sql` idempotently/unconditionally (safe + self-healing — migrations are
required idempotent). Flag the CLAUDE.md↔CI drift in that PR.

## 12. ADR-039 (M4 — dedicated, not an amendment)
ADR-039 records: (a) xShotOccurrence adoption (new published methodology — Pipping et al. 2026); (b) the
gk_influence zone expansion; (c) **the cross-provider pitch-control method divergence persisted into
shared columns + the `pitch_control_method` provenance column** (the cross-cutting value-format-contract
decision); (d) SB360 coverage expansion + the sparsity caveat. Bundled with the PR.

## 13. Post-merge operator sequencing + follow-ups

**Sequencing (critical — the migration is operator-applied, §11):** the updated dbt staging/mart models
ship to `main` on merge and reference the 6 new columns, so the bronze `ALTER` must precede any live
build that uses them:
1. Merge → wait for post-merge CI (wheel 0.5.15 deploy) to finish.
2. **Operator applies the bronze migration** (`ALTER TABLE bronze.spadl_action_context ADD COLUMNS (…)`)
   — *before* steps 3–4 and before the next scheduled `dbt-live-ci` run.
3. Re-run the AC-1 compute (the writer schema now includes the 6 columns; bronze has them).
4. Live `dbt build` (staging/mart + contract with the new columns) — now succeeds (no chicken-and-egg).
5. Synced-table refresh auto-evolves the additive columns; HF publish (`SELECT *`) auto-includes them.

**Follow-ups / out of scope:**
- **CI migration-runner re-wiring** — its own PR (§11 options 1/2); flag the CLAUDE.md↔CI drift there.
- SB360 `pre_shot_gk_position/angle = 0` anomaly — likely SB360's `add_pre_shot_gk_context` runs
  SPADL-only (no `frames=`), leaving `defending_gk_player_id` unresolved; possibly recoverable by
  resolving GK from the synthetic frame. Investigate separately.
- Publishing the spadl-action-context HF dataset; the broader recompute (pending the silly-kicks
  DAS-empty-batch fix tracked separately).

## 14. PR shape
One bundled feature PR. The plan may phase it internally: (A) GK metrics (6 cols + tracking +
provenance) → (B) SB360 coverage (enrich + fixture/golden). Shared golden re-baseline, one migration
(operator-applied per §11/§13), one wheel bump. **The CI migration-runner re-wiring is a *separate* PR**
(H-NEW option 3) — this feature PR must not depend on or fix the migration plumbing.
