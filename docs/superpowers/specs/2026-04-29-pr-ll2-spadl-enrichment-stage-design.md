# PR-LL2 — SPADL post-conversion enrichment stage + four-source coverage + LL1 cleanup

**Date:** 2026-04-29
**Author:** Karsten (with Claude Opus 4.7)
**Status:** Design locked, awaiting writing-plans
**Branch (planned):** `feat/spadl-enrichment-stage`
**Wheel bump:** 0.3.20 → 0.3.21
**silly-kicks dep:** `>=1.5.0,<2.0` → `>=1.7.0,<2.0`
**Related ADR:** ADR-016 (new — to be drafted as part of this PR); ADR-011 footnote amendment
**Predecessor PR:** PR-LL1 (#223, commit `e88b35b`) — silly-kicks 1.5.0 `preserve_native` integration

## Executive summary

LL2 establishes a **named SPADL post-conversion enrichment stage** (`apply_spadl_enrichments`) as the architectural home for provider-agnostic silly-kicks helpers, with **`add_possessions`**, **`add_gk_role`**, and **`add_pre_shot_gk_context`** as the first occupants. The same PR fixes three latent gaps from PR-LL1 (`action_id` never written to bronze, `vaep_schema` dropped `statsbomb_*` at the applyInPandas boundary, `bronze.spadl_actions` missing four `statsbomb_*` columns physically), expands SPADL coverage from two sources to **four** (StatsBomb, Wyscout, IDSSE/Bundesliga, Metrica — silly-kicks 1.7.0 ships dedicated DataFrame converters for the latter two), and applies a **β-consistent naming rule** to `fct_action_values` (computed enrichments use plain canonical names; provider-native passthroughs use `<provider>_<field>` everywhere). The `fct_funnel_stages_agg` mart and the Taipy `funnel.py` Wyscout-synthetic-possession workaround retire — the canonical heuristic possession ID replaces them. One destructive backfill at LL2 close populates everything cleanly.

## Background

### What PR-LL1 shipped (2026-04-28)

PR-LL1 surfaced StatsBomb's native `possession_id` and three sibling provider-native fields (`possession_team_id`, `play_pattern`, `under_pressure`) through SPADL conversion via the silly-kicks 1.5.0 `preserve_native` kwarg. The implementation:

- Added the four `statsbomb_*` columns to the `_SPADL_SCHEMA` and `_VAEP_SCHEMA` DDL constants in `src/ingestion/spadl_vaep.py`
- Updated the StatsBomb UDF in `src/ingestion/spadl_conversion.py` to emit the four columns
- Aliased the four columns to non-prefixed names in `fct_action_values` (`possession_id`, `possession_team_id`, `play_pattern`, `under_pressure`)
- Applied an out-of-band ALTER to `bronze.vaep_action_values` to add the columns physically

### Three latent gaps discovered during LL2 design

Backend fact-checks while designing LL2 surfaced three problems with PR-LL1's deployment:

1. **`bronze.spadl_actions` is missing the four `statsbomb_*` columns physically.** The PR-LL1 ALTER touched only `bronze.vaep_action_values`, leaving `bronze.spadl_actions` (the intermediate write target between conversion and VAEP scoring) on its 19-column pre-LL1 schema. The codebase's `_SPADL_SCHEMA` constant declares the four columns but the existing physical table doesn't have them. Spark's `mergeSchema=true` on `write_delta_table` would lazily add them on the next StatsBomb write — but no such write has happened since PR-LL1 merged (the latest StatsBomb ingest predates the merge), so the bronze table remains in its pre-LL1 shape.

2. **`vaep_schema` (the applyInPandas StructType in `_make_scoring_udf` at `src/ingestion/spadl_vaep.py:534`) does not include `statsbomb_*`.** The VAEP scoring UDF's output frame contains the four columns (built via the `_output_cols` projection), but Spark's applyInPandas drops columns not declared in the StructType passed to it. So even after the bronze ALTER on `vaep_action_values` made the columns physically present, every row written would have NULL `statsbomb_*` values. **0 of 7,151,510 StatsBomb rows in `bronze.vaep_action_values` have a non-NULL `statsbomb_possession_id`** — the LL1 feature is silently broken in production.

3. **`bronze.spadl_actions.action_id` is declared but 100% NULL.** The schema declares `action_id BIGINT`, but the applyInPandas writer schemas in `src/ingestion/spadl_conversion.py:286-312` don't emit `action_id`. silly-kicks's `convert_to_actions` already produces it (`actions["action_id"] = range(len(actions))` per match), but luxury-lakehouse drops it at the projection boundary. This blocks `silly_kicks.spadl.utils.add_possessions(actions)`, which requires `action_id` as input.

### What LL2 needs to do

Beyond fixing those three gaps, LL2 needs to:

- Establish the architectural pattern for provider-agnostic post-conversion enrichments (silly-kicks ships a family of these helpers; the next ~5 PRs will reach for them)
- Wire in the first three helpers as a useful demonstration of the pattern + as features valuable in their own right (heuristic possession reconstruction, GK role tagging, pre-shot GK context)
- Apply the β-consistent naming rule across the entire StatsBomb-native column family — fix LL1's mart-level alias inconsistency one day after it landed (zero hardened consumers)
- Expand SPADL coverage from two providers to four (silly-kicks 1.7.0 unblocks IDSSE and Metrica)
- Modernize `fct_funnel_stages_agg` and the Taipy Conversion Funnel page to use the canonical heuristic possession_id, retiring the Wyscout-synthetic-possession workaround that exists only because heuristic possessions weren't available pre-LL2

## Goals and non-goals

### Goals

1. Establish `apply_spadl_enrichments` as the named architectural pattern for provider-agnostic SPADL post-conversion helpers
2. Wire in `add_possessions`, `add_gk_role`, `add_pre_shot_gk_context` (silly-kicks 1.4.0+ + 1.5.0+) for all four data sources
3. Fix PR-LL1's three latent gaps in the same PR (action_id surfacing, vaep_schema gap, bronze.spadl_actions ALTER)
4. Apply β-consistent naming rule to `fct_action_values`: canonical column names for computed enrichments, `<provider>_<field>` namespacing for provider-native passthroughs
5. Add IDSSE and Metrica to the SPADL pipeline using silly-kicks 1.7.0's dedicated DataFrame converters
6. Modernize `fct_funnel_stages_agg` + Taipy `funnel.py` to use the canonical heuristic possession_id, retiring the Wyscout-synthetic-possession workaround
7. Lock writer/DDL parity testing across all four source UDFs + the VAEP scoring UDF, preventing any LL1-class regression from recurring

### Non-goals (deferred to other cycles)

- `add_gk_distribution_metrics` (silly-kicks 1.4.0+) — requires xT grid loading at conversion time, its own architecture decision
- Opta SPADL conversion — silly-kicks's `kloppy.py` Opta line still commented; no luxury-lakehouse Opta event source today
- **SkillCorner SPADL** — only `bronze.skillcorner_tracking` exists; no SkillCorner *event* table. Adding SPADL coverage requires either (a) a SkillCorner-events ingestion cycle (separate workflow + bronze table + silly-kicks converter), or (b) a research effort to derive events from tracking data. Both are weeks-to-months efforts of their own; neither is LL2 scope.
- Schema-migration tooling refactor — `scripts/migrate_bronze_for_pr_ll2.py` is purpose-built; a general-purpose framework is its own cycle if more ALTERs accumulate
- `test_marts_live_schema.py` expansion to all 33 fact + 4 dim marts — separate cycle
- Atomic-SPADL integration — luxury-lakehouse uses standard SPADL only; future cycle if needed
- Cross-source `possession_team_key` — `add_possessions` doesn't emit team_id; would require a silly-kicks enhancement (e.g., `add_possessions_with_team(...)` or extending `add_possessions` to also emit `possession_team_id`)

## Architecture

### Data flow

```
Provider raw events:
  bronze.statsbomb_events ──┐
  bronze.wyscout_events    ──┴──► silly_kicks.spadl.{statsbomb,wyscout}.convert_to_actions
                                  (dedicated converters; preserve_native=[...] on StatsBomb path)
                                  │
  bronze.idsse_events    ──┐      │
  bronze.metrica_events ──┴──► silly_kicks.spadl.{sportec,metrica}.convert_to_actions
                                  (silly-kicks 1.7.0 dedicated DataFrame converters;
                                   accept normalized event DataFrames matching luxury-lakehouse's
                                   bronze schemas; output uses KLOPPY_SPADL_COLUMNS schema —
                                   slight column-order nuance vs SPADL_COLUMNS, handled in UDF projection)
                                  │
                                  ▼
                  apply_spadl_enrichments(actions, *, source)
                  ── NEW: src/ingestion/spadl_enrichments.py
                  • add_possessions(actions)            → possession_id_heuristic   (BIGINT, always populated)
                  • add_gk_role(actions)                 → gk_role                    (STRING, NULL on non-GK rows)
                  • add_pre_shot_gk_context(actions)    → gk_was_distributing (BOOL),
                                                          gk_was_engaged (BOOL),
                                                          gk_actions_in_possession (BIGINT),
                                                          defending_gk_player_id (BIGINT)
                                  │
                                  ▼
              Per-provider UDF column projection + dtype enforcement
              (statsbomb_* renamed for SB, NULL-filled for non-SB)
                                  │
                                  ▼
              write_delta_table → bronze.spadl_actions (mergeSchema=true)
                                  │
                                  ▼
              spadl_vaep.run_pipeline reads bronze.spadl_actions
                                  │
                                  ▼
              _make_scoring_udf VAEP scoring UDF — projects + carries through
              statsbomb_* AND enrichment columns AND action_id
              (vaep_schema StructType updated to include all of these — closes
               LL1 latent bug class)
                                  │
                                  ▼
              write_delta_table → bronze.vaep_action_values
                                  │
                                  ▼
              dbt staging (stg_spadl__action_values.sql) — passthrough
                                  │
                                  ▼
              dbt mart (fct_action_values.sql) — β-consistent shape
              + fct_funnel_stages_agg.sql consumer modernization
```

### Key architectural invariants

1. **Naming rule**: post-conversion enrichments use plain canonical names (`possession_id`, `gk_role`, `action_id`); provider-native passthroughs use `<provider>_<field>` (`statsbomb_possession_id`, `statsbomb_play_pattern`) everywhere — bronze, staging, and mart. No mart-level alias drops.
2. **Schema parity**: every applyInPandas StructType must agree column-for-column with the corresponding `_SPADL_SCHEMA` / `_VAEP_SCHEMA` DDL constant. Enforced by extended `test_spadl_vaep_writer_parity.py` covering all 4 source paths plus the VAEP scoring UDF (closes LL1's latent-bug class).
3. **Enrichment is per-match, deterministic given SPADL output**. `apply_spadl_enrichments` runs inside the per-match `groupBy(match_id).applyInPandas(...)` of each source's UDF — no extra Spark stages, no driver-side operations.
4. **`apply_spadl_enrichments` is pure pandas** (silly-kicks dependency only) — testable without Spark.
5. **Direction of play unification** (silly-kicks 1.7.0 bonus fix): all six silly-kicks SPADL converters now apply `_fix_direction_of_play` consistently. luxury-lakehouse's UDFs receive semantically equivalent SPADL output regardless of source.

### Source coverage

| Source | Bronze input | silly-kicks path | Native passthrough columns |
|---|---|---|---|
| StatsBomb | `bronze.statsbomb_events` | `silly_kicks.spadl.statsbomb` (dedicated, DataFrame in) | `statsbomb_possession_id`, `statsbomb_possession_team_id`, `statsbomb_play_pattern`, `statsbomb_under_pressure` |
| Wyscout | `bronze.wyscout_events` | `silly_kicks.spadl.wyscout` (dedicated, DataFrame in) | None |
| IDSSE / Sportec | `bronze.idsse_events` (DFL/Bassek format, ~210 cols) | `silly_kicks.spadl.sportec` (silly-kicks 1.7.0 dedicated DataFrame converter) | None (kloppy strips provider-native fields; if a future need arises, `preserve_native` is wired through) |
| Metrica | `bronze.metrica_events` (CSV/EPTS-derived normalized, ~20 cols) | `silly_kicks.spadl.metrica` (silly-kicks 1.7.0) | None |
| ~~SkillCorner~~ | only `bronze.skillcorner_tracking` (no events) | n/a | Blocked at events-ingestion layer; separate cycle |

### Combined backfill semantics

- **StatsBomb + Wyscout**: existing rows in `bronze.spadl_actions` (~7.15M + ~2.47M = ~9.6M rows). Backfill = `DELETE WHERE data_source IN ('statsbomb', 'wyscout')` + re-run wf-vaep. The LL1 `statsbomb_*` + new enrichment columns get populated cleanly on re-conversion.
- **IDSSE + Metrica**: greenfield — zero rows in `bronze.spadl_actions` today. The existing `_read_existing_match_ids` → "skip already-converted games" logic processes their matches naturally on the first wf-vaep run after merge. No destructive op needed for them.

## Bronze schema changes

### Current state (pre-LL2, verified live)

- `bronze.spadl_actions`: 19 cols. `action_id` declared but 100% NULL on every row. **Missing the four PR-LL1 `statsbomb_*` columns physically.**
- `bronze.vaep_action_values`: 28 cols. Has the four PR-LL1 `statsbomb_*` columns physically, but every row has them NULL because the `vaep_schema` gap drops them at the applyInPandas boundary.

### LL2 ALTERs (out-of-band, idempotent script)

```sql
-- 1. bronze.spadl_actions:
--    (a) Backfill the 4 PR-LL1 statsbomb_* columns missed by LL1's ALTER
--    (b) Add 6 LL2 enrichment columns
ALTER TABLE soccer_analytics.bronze.spadl_actions ADD COLUMNS (
    statsbomb_possession_id        BIGINT,
    statsbomb_possession_team_id   BIGINT,
    statsbomb_play_pattern         STRING,
    statsbomb_under_pressure       BOOLEAN,
    possession_id_heuristic        BIGINT,
    gk_role                        STRING,
    gk_was_distributing            BOOLEAN,
    gk_was_engaged                 BOOLEAN,
    gk_actions_in_possession       BIGINT,
    defending_gk_player_id         BIGINT
);

-- 2. bronze.vaep_action_values:
--    (a) Add action_id (newly surfaced — was never carried through to vaep_action_values)
--    (b) Add 6 LL2 enrichment columns
ALTER TABLE soccer_analytics.bronze.vaep_action_values ADD COLUMNS (
    action_id                      BIGINT,
    possession_id_heuristic        BIGINT,
    gk_role                        STRING,
    gk_was_distributing            BOOLEAN,
    gk_was_engaged                 BOOLEAN,
    gk_actions_in_possession       BIGINT,
    defending_gk_player_id         BIGINT
);
```

### Dtype rationale

| Column | Bronze dtype | Why |
|---|---|---|
| `statsbomb_possession_id`, `statsbomb_possession_team_id` | BIGINT | StatsBomb-native ints; silly-kicks 1.5.0 `preserve_native` surfaces as `Int64` nullable, lands as BIGINT NULL on synthetic dribbles or non-SB sources |
| `statsbomb_play_pattern` | STRING | StatsBomb category name (free text) |
| `statsbomb_under_pressure` | BOOLEAN | StatsBomb flag |
| `possession_id_heuristic` | BIGINT | silly-kicks emits int64 per-match counter, always populated |
| `gk_role` | STRING | silly-kicks emits `pd.Categorical` (5 values + None); STRING with NULL on non-GK rows is the cleanest Delta representation. dbt mart adds `accepted_values` data test for the 5 categories |
| `gk_was_distributing`, `gk_was_engaged` | BOOLEAN | silly-kicks emits bool; defaults to False on non-shot rows (per `add_pre_shot_gk_context` design) — never NULL by construction |
| `gk_actions_in_possession` | BIGINT | int64; defaults to 0 on non-shot rows |
| `defending_gk_player_id` | BIGINT | silly-kicks emits float64 with NaN coding (pandas int64 doesn't support NaN). The UDF converts to nullable Int64 before write; lands as nullable BIGINT in Delta |
| `action_id` | BIGINT | silly-kicks `convert_to_actions` already produces it as int64 per match — surfacing requires only stop dropping it at projection |

### Idempotency + safety

Per CLAUDE.md PR-LL1 lessons: `ALTER TABLE ... ADD COLUMN IF NOT EXISTS` is NOT supported on Databricks Delta in our runtime. Plain `ADD COLUMN(S)` is also not idempotent.

`scripts/migrate_bronze_for_pr_ll2.py` (NEW) implements idempotency at the application layer:

1. Connects via Databricks SQL connector
2. For each target table, runs `DESCRIBE TABLE` and computes the set of columns missing relative to a target column list defined as a module-level constant
3. If any are missing, emits `ALTER TABLE ... ADD COLUMNS (only_missing_columns)`
4. If none missing, prints "table already at target schema" and exits 0
5. Logs the resulting DESCRIBE output

Pattern lifted from `scripts/maintain_synced_tables.py`. Safe to re-run during PR development; no-op after first successful run.

## Code changes (file-by-file)

### A. New code modules

| File | Status | What |
|---|---|---|
| `src/ingestion/spadl_enrichments.py` | NEW | `apply_spadl_enrichments(actions: pd.DataFrame, *, source: str) -> pd.DataFrame` — calls `add_possessions`, `add_gk_role`, `add_pre_shot_gk_context` in defined order, returns enriched frame with 6 new columns. Pure pandas, no Spark |
| `scripts/migrate_bronze_for_pr_ll2.py` | NEW | Idempotent ALTER-or-noop script per the spec above |

### B. Existing ingestion code — modified

`src/ingestion/spadl_conversion.py`:

1. `_make_sb_spadl_udf`: surface `action_id` in `_spadl_cols` + StructType; call `apply_spadl_enrichments(actions, source="statsbomb")` after the rename block; add 6 new enrichment columns to both projections
2. `_make_ws_spadl_udf`: same surface-action_id + enrichment-call + 6 new columns
3. **NEW** `_make_idsse_spadl_udf` + `_convert_idsse_from_bronze` + `_make_idsse_replace_where`
4. **NEW** `_make_metrica_spadl_udf` + `_convert_metrica_from_bronze` + `_make_metrica_replace_where`
5. Each new UDF mirrors StatsBomb/Wyscout structure: import silly-kicks dedicated converter inside closure (`silly_kicks.spadl.sportec` / `silly_kicks.spadl.metrica`), read bronze rows via `groupBy(match_id).applyInPandas(...)`, call `apply_spadl_enrichments` post-conversion, project canonical schema

`src/ingestion/spadl_vaep.py`:

1. `_SPADL_SCHEMA` constant: add 6 enrichment column declarations (already has action_id and 4 statsbomb_*)
2. `_VAEP_SCHEMA` constant: add `action_id BIGINT` + 6 enrichment columns
3. `_make_scoring_udf` `_output_cols` projection: append `action_id` + 6 enrichment columns
4. `_make_scoring_udf` `vaep_schema` (the applyInPandas StructType): **add `statsbomb_*` (closes LL1 latent bug) AND `action_id` AND 6 enrichment columns** — every column in `_VAEP_SCHEMA` must be in this StructType
5. `_VaepGuard.check()` Stage 1: query 4 event-source tables (`statsbomb_events`, `wyscout_events`, `idsse_events`, `metrica_events`) via `find_new_ids`; union the four sets
6. `run_pipeline`: call 4 converter functions sequentially (StatsBomb, Wyscout, IDSSE, Metrica)

`src/ingestion/spadl_adapter.py`:

1. **NEW** `adapt_idsse_events_for_silly_kicks(idsse_events_pdf: pd.DataFrame) -> pd.DataFrame` — column-rename / pass-through adapter from luxury-lakehouse's `bronze.idsse_events` shape to silly-kicks's expected sportec converter input. Likely identity passthrough (silly-kicks 1.7.0 brief targeted bronze schemas exactly)
2. **NEW** `adapt_metrica_events_for_silly_kicks(...)` — same for metrica
3. **NEW** `resolve_idsse_home_team_ids(...)` and `resolve_metrica_home_team_ids(...)` — derive home team per match. Pattern mirrors existing StatsBomb/Wyscout home team resolution

### C. dbt models

| File | Status | What changes |
|---|---|---|
| `dbt_project/models/staging/spadl/stg_spadl__action_values.sql` | UPDATE | Passthrough new bronze columns: `action_id`, 4 `statsbomb_*` (already there post-LL1), 6 enrichment columns. β-consistent: keep `statsbomb_*` named as-is in staging (no aliasing) |
| `dbt_project/models/marts/fct_action_values.sql` | UPDATE | β-consistent rewrite of the SELECT projection: rename 4 mart-level aliases to their bronze names (`statsbomb_possession_id`, `statsbomb_possession_team_id`, `statsbomb_play_pattern`, `statsbomb_under_pressure`); introduce canonical `possession_id` (sourced from `av.possession_id_heuristic`); add 5 GK columns + `action_id`; keep `possession_team_key` Kimball surrogate; drop the legacy `possession_team_id` alias |
| `dbt_project/models/marts/_marts__models.yml` | UPDATE | `fct_action_values` contract: add 7 new column entries; rename 4; drop `possession_team_id` legacy; add data tests on new columns (`not_null` on most; `accepted_values` on `gk_role` for the 5 categories) |
| `dbt_project/models/staging/spadl/_spadl__sources.yml` | UPDATE | `vaep_action_values` source descriptions for new columns |
| `dbt_project/models/marts/fct_funnel_stages_agg.sql` | UPDATE (substantive) | Drop the Wyscout-synthetic-possession workaround per Option ii. Replace `count(distinct case when possession_id is not null then possession_id end)` with `count(distinct possession_id)` (canonical possession_id is now populated for all sources). Retire `wy_match_flag` or rename to `heuristic_possession_flag = max(case when statsbomb_possession_id is null then 1 else 0 end)` (final naming locked during TDD; recommend rename for generality). Update line-95 own-possession filter to `where statsbomb_possession_team_id is null or statsbomb_possession_team_id = team_id`. Rewrite header comments to reflect new semantics |

### D. Taipy app

| File | Status | What changes |
|---|---|---|
| `hf_taipy_app/src/queries/funnel.py` | UPDATE (substantive) | Remove driver-side synthetic-possession compensation logic that previously inflated Wyscout match counts. Funnel chart now reads `pos_in_gs` and `pos_in_match` directly. Update reference to `wy_match_flag` (or its renamed equivalent) in the data-access layer. Update line-88 docstring |

### E. Tests

| File | Status | What |
|---|---|---|
| `src/tests/test_marts_live_schema.py` | UPDATE | `_FCT_ACTION_VALUES_EXPECTED_COLS` dict: add 7 new entries, rename 4, drop `possession_team_id`. Optionally add `_FCT_FUNNEL_STAGES_AGG_EXPECTED_COLS` dict + accompanying test for the funnel mart's β-consistent shape |
| `src/tests/test_spadl_vaep_writer_parity.py` | UPDATE (substantive) | Extend to cover all 4 source UDFs vs `_SPADL_SCHEMA` (Wyscout untested in LL1; IDSSE + Metrica new); cover VAEP scoring UDF's `vaep_schema` vs `_VAEP_SCHEMA` (closes LL1 latent-bug class); cover all 6 new enrichment columns + `action_id` in both DDLs; type parity across the two DDLs for enrichment cols |
| `src/tests/test_spadl_enrichments.py` | NEW | Unit + plausibility + boundary-F1 tests per Section 5 of the design; ~14 tests, runtime <3s |
| `src/tests/fixtures/spadl_3match_statsbomb_for_f1.parquet` | NEW | 3-match StatsBomb fixture (~30K rows / ~5MB) for boundary-F1 test |
| `scripts/build_test_fixtures.py` | NEW | One-shot fixture builder. Runs against live StatsBomb open data once via `silly_kicks.spadl.statsbomb.convert_to_actions(events, home_team_id, preserve_native=['possession'])`, writes the parquet output. Script is committed for reproducibility (future maintainers can rebuild the fixture if silly-kicks's StatsBomb converter output shape changes) |

### F. Wheel + version

| File | Status | What |
|---|---|---|
| `pyproject.toml` | UPDATE | `version = "0.3.20"` → `"0.3.21"`; `silly-kicks>=1.5.0,<2.0` → `silly-kicks>=1.7.0,<2.0` (in `[analytics]` extra) |
| 22 PEP 723 + Terraform `wheel_path` consumers | UPDATE | Synced via `uv run python scripts/bump_wheel.py` after `pyproject.toml` edit |
| `src/shared/wheel.py` | UPDATE | Auto-synced by `bump_wheel.py` |

### G. Documentation

| File | Status | What |
|---|---|---|
| `docs/superpowers/adrs/ADR-016-spadl-enrichment-stage-canonical-naming.md` | NEW | Captures: (1) the named `apply_spadl_enrichments` stage as the architectural pattern; (2) the canonical/native naming rule; (3) the LL1 latent-bug post-mortem and how writer/DDL parity tests prevent recurrence |
| `docs/superpowers/adrs/ADR-011-unified-kimball-match-dimension.md` | UPDATE | Footnote: `possession_team_id` legacy alias closed early — column existed for 24 hours before β-rename, no consumers built up dependence |
| `docs/engineering/conventions.md` | UPDATE | New § SPADL Pipeline subsection: "Post-conversion enrichments live in `src/ingestion/spadl_enrichments.py`; new helpers added to `apply_spadl_enrichments` + a column added to `_SPADL_SCHEMA` + `_VAEP_SCHEMA` + applyInPandas StructTypes (parity-tested). Provider-native passthroughs use `<provider>_<field>` everywhere; computed enrichments use plain canonical names." |
| `CLAUDE.md` (project root) | UPDATE | One-line addition referencing ADR-016 |

### H. Workflow / governance — verify during TDD

- `src/workflows/cards/wf-vaep.yml` (or wherever the wf-vaep card lives): probably no change. Verify card validation still passes
- `AI_GOVERNANCE.md`: no change — heuristic possession reconstruction is a deterministic algorithm, not an ML model

### Total file scope

NEW:

- `src/ingestion/spadl_enrichments.py` (source module — `apply_spadl_enrichments`)
- `src/tests/test_spadl_enrichments.py` (unit + plausibility + boundary-F1 tests)
- `scripts/migrate_bronze_for_pr_ll2.py` (idempotent bronze ALTER script)
- `scripts/build_test_fixtures.py` (one-shot fixture builder; committed for reproducibility)
- `scripts/validate_pr_ll2_post_deploy.py` (post-deploy validation — non-NULL counts)
- `scripts/measure_boundary_f1_full_corpus.py` (empirical F1 on full StatsBomb subset)
- `src/tests/fixtures/spadl_3match_statsbomb_for_f1.parquet` (~30K rows / ~5MB)
- `docs/superpowers/adrs/ADR-016-spadl-enrichment-stage-canonical-naming.md` (NEW ADR)

UPDATED (~15 files):

- `src/ingestion/spadl_conversion.py` — substantive (existing UDFs + 2 new UDFs + adapter calls)
- `src/ingestion/spadl_vaep.py` — substantive (DDL constants + scoring UDF + guard + run_pipeline)
- `src/ingestion/spadl_adapter.py` — substantive (2 new adapter functions + 2 new home-team resolvers)
- `dbt_project/models/staging/spadl/stg_spadl__action_values.sql` — passthrough
- `dbt_project/models/marts/fct_action_values.sql` — substantive (β-consistent rewrite)
- `dbt_project/models/marts/_marts__models.yml` — contract update
- `dbt_project/models/staging/spadl/_spadl__sources.yml` — column doc additions
- `dbt_project/models/marts/fct_funnel_stages_agg.sql` — substantive (Option ii reconciliation)
- `hf_taipy_app/src/queries/funnel.py` — substantive (drop synthetic-possession compensation)
- `src/tests/test_marts_live_schema.py` — expected-cols dict update
- `src/tests/test_spadl_vaep_writer_parity.py` — substantive (extends to 4 source UDFs + VAEP scoring UDF + new columns)
- `pyproject.toml` — version + silly-kicks dep
- `docs/superpowers/adrs/ADR-011-unified-kimball-match-dimension.md` — footnote amendment
- `docs/engineering/conventions.md` — new § SPADL Pipeline subsection
- `CLAUDE.md` (project root) — one-line ADR-016 reference

AUTO-SYNCED (via `scripts/bump_wheel.py`):

- 22 PEP 723 + Terraform `wheel_path` consumers
- `src/shared/wheel.py`

## Mart shape (β-consistent details)

### Column-by-column shape of `fct_action_values` after LL2

| Group | Column | Type | Notes |
|---|---|---|---|
| Identifiers | `action_value_id` | string | Surrogate key |
| | `match_key`, `competition_key`, `team_key`, `player_key`, `possession_team_key` | bigint | Kimball surrogates |
| | `match_id`, `competition_id`, `team_id`, `player_id`, `season_id` | bigint/int | Legacy native; ADR-011 dual-column window through 2026-07-22 |
| | `action_id` | bigint | NEW — surfaced from silly-kicks `convert_to_actions` per-match counter |
| Temporal | `period`, `time_seconds`, `minute`, `second` | int/double | Unchanged |
| Geometry | `start_x`, `start_y`, `end_x`, `end_y` | double | SPADL 105×68 |
| Classification | `action_type`, `action_result`, `bodypart` | string | Unchanged |
| VAEP | `offensive_value`, `defensive_value`, `vaep_value` | double | Unchanged |
| Canonical enrichments | `possession_id` | bigint | NEW canonical (heuristic, populated for all sources) — semantic flip from LL1 |
| | `gk_role` | string | NEW — categorical 5 values + NULL |
| | `gk_was_distributing`, `gk_was_engaged` | boolean | NEW — False on non-shot rows |
| | `gk_actions_in_possession` | bigint | NEW — 0 on non-shot rows |
| | `defending_gk_player_id` | bigint | NEW — NULL when absent |
| Provider-native passthrough | `statsbomb_possession_id` | bigint | RENAMED from `possession_id` alias; NULL on non-SB |
| | `statsbomb_possession_team_id` | bigint | RENAMED from `possession_team_id` alias; NULL on non-SB |
| | `statsbomb_play_pattern` | string | RENAMED from `play_pattern` alias; NULL on non-SB |
| | `statsbomb_under_pressure` | boolean | RENAMED from `under_pressure` alias; NULL on non-SB |
| Game state | `game_state` | string | `winning`/`drawing`/`losing` |
| Provenance | `data_source` | string | `'statsbomb'` / `'wyscout'` / `'idsse'` / `'metrica'` (4 valid values post-LL2) |
| | `original_event_id`, `_loaded_at` | string/timestamp | Unchanged |

Net delta: +7 new columns, 4 renamed, 1 dropped (`possession_team_id` legacy alias). Mart goes from ~30 cols to ~37 cols.

### The naming rule (codified by this PR; enforced by ADR-016)

| Origin of column value | Naming convention | Population |
|---|---|---|
| Computed post-conversion enrichment (deterministic from canonical SPADL) | Plain canonical name: `possession_id`, `gk_role`, `gk_was_engaged`, `action_id` | Always populated for all sources (or has a documented default) |
| Provider-native passthrough | `<provider>_<field>`: `statsbomb_possession_id`, `statsbomb_play_pattern` | NULL on sources without that provider's native concept |
| Kimball surrogate FK | `<entity>_key`: `match_key`, `team_key`, `possession_team_key` | Plain (Kimball convention wins). Population follows the underlying native data |
| Legacy native ID inside ADR-011 dual-column window | `<entity>_id`: `match_id`, `competition_id`, `team_id`, `player_id`, `season_id` | Always populated; sunset 2026-07-22 |

### Downstream consumer migration

| Consumer | File:line | Current | Post-LL2 |
|---|---|---|---|
| `fct_funnel_stages_agg` | `dbt_project/models/marts/fct_funnel_stages_agg.sql:109,128` | `count(distinct case when possession_id is not null then possession_id end)` | `count(distinct possession_id)` (canonical heuristic; populated for all sources) |
| | same file:95 | `where possession_team_id is null or possession_team_id = team_id` | `where statsbomb_possession_team_id is null or statsbomb_possession_team_id = team_id` |
| | same file:110 | `wy_match_flag = max(case when possession_id is null then 1 else 0 end)` | retire OR rename to `heuristic_possession_flag = max(case when statsbomb_possession_id is null then 1 else 0 end)` (TDD lock) |
| | same file:39-52 | comment narrative on Wyscout-synthetic-possession workaround | rewrite for canonical heuristic semantics |
| Taipy funnel | `hf_taipy_app/src/queries/funnel.py` | driver-side synthetic-possession compensation logic + line-88 docstring | remove compensation; read `pos_in_gs` / `pos_in_match` directly; update docstring |
| Live mart schema test | `src/tests/test_marts_live_schema.py:62-93` | uses old column names; missing LL1 `play_pattern` / `under_pressure` already (test was stale before LL2) | full update to new shape |

**User-facing behavior change**: the Conversion Funnel page on Wyscout matches goes from showing `pos_in_gs = 0` (with synthetic compensation at the driver level treating each match as 1 possession) to showing real heuristic possession counts. After LL2's combined backfill, a Wyscout team in a match might show ~80 possessions instead of 1 synthetic one. IDSSE and Metrica matches start showing in the funnel correctly out of the box. This is a **major UX improvement** but is a numerically-visible change — call it out in the LL2 PR description.

### ADR-011 footnote (proposed text)

To be appended to `docs/superpowers/adrs/ADR-011-unified-kimball-match-dimension.md`:

> **2026-04-29 (PR-LL2) — `possession_team_id` legacy alias closed early.** PR-LL1 (2026-04-28) introduced `possession_team_id` in `fct_action_values` as an alias of the bronze column `statsbomb_possession_team_id`, intended to live inside the standard 90-day dual-column window through 2026-07-22 alongside the original Kimball-migration legacy columns. PR-LL2 (2026-04-29) renamed the mart-level alias to its bronze name `statsbomb_possession_team_id` as part of the β-consistent SPADL post-conversion enrichment naming rule, dropping the alias 24 hours after introduction. Acceptable in this specific case because the column had no time to accrue downstream consumers (`hf_taipy_app/`, `src/`, `dbt_project/` greps confirm zero matches at the time of rename). The 90-day window remains in force for the original ADR-011 legacy columns; sunset date 2026-07-22 unchanged.

## Test plan

### Coverage matrix

| Subject | silly-kicks unit (existing) | luxury-lakehouse smoke / plausibility (NEW) | luxury-lakehouse integration (writer parity + live schema, NEW/UPDATED) | luxury-lakehouse boundary-F1 on real data (NEW) |
|---|---|---|---|---|
| `add_possessions` algorithm | ✓ (597 LOC) | — | — | ✓ (3-match fixture, F1≥0.85) |
| `add_gk_role` algorithm | ✓ (438 LOC) | ✓ (categories, distribution sanity) | — | n/a (no ground truth) |
| `add_pre_shot_gk_context` algorithm | ✓ (422 LOC) | ✓ (engaged-on-shots only, plausible rates) | — | n/a |
| `apply_spadl_enrichments` integration | — | ✓ | — | — |
| StatsBomb UDF schema | — | — | ✓ (writer parity, extended) | — |
| Wyscout UDF schema | — | — | ✓ (writer parity, NEW in LL2) | — |
| IDSSE UDF schema | — | — | ✓ (writer parity, NEW) | — |
| Metrica UDF schema | — | — | ✓ (writer parity, NEW) | — |
| VAEP scoring UDF schema | — | — | ✓ (writer parity, NEW — closes LL1 bug) | — |
| Bronze `_SPADL_SCHEMA` ↔ `_VAEP_SCHEMA` enrichment columns | — | — | ✓ | — |
| `fct_action_values` mart contract | — | — | ✓ (dbt contract enforced + live schema test) | — |
| `fct_funnel_stages_agg` post-Option-ii shape | — | — | ✓ (live schema test if added) | — |
| End-to-end production data | — | — | — | ✓ (post-deploy validation script) |

### CI test budget

Total new CI time impact: <10s. Negligible relative to existing CI budget.

### Boundary-F1 test specification

`src/tests/test_spadl_enrichments.py::test_boundary_f1_against_native_statsbomb`:

1. Load `src/tests/fixtures/spadl_3match_statsbomb_for_f1.parquet` (3 StatsBomb matches across competition classes, ~30K rows)
2. Run `apply_spadl_enrichments(actions, source="statsbomb")`
3. Compute boundary-F1 between heuristic `possession_id_heuristic` and native `statsbomb_possession_id` per match, then average
4. Assert F1 ≥ 0.85

If the empirical F1 lands 0.80–0.85, lower the CI threshold to `(measured − 0.02)` per silly-kicks's documented convention. If F1 < 0.80, investigate fixture quality and silly-kicks's algorithm correctness before merging.

### Post-deploy validation scripts (NEW)

- `scripts/validate_pr_ll2_post_deploy.py` — runs after combined backfill. Connects to Databricks, queries `bronze.vaep_action_values` for non-NULL counts of all 7 new columns + 4 statsbomb_* columns, asserts >0 populated rows per source where expected
- `scripts/measure_boundary_f1_full_corpus.py` — runs boundary-F1 on full StatsBomb subset (~7M actions / 3,463 matches), per-competition breakdown, logged with timestamp. Re-run quarterly or after any silly-kicks `add_possessions` change

## Migration runbook

### Pre-flight checklist

| Item | How to verify | Expected |
|---|---|---|
| silly-kicks 1.7.0 published | `pip index versions silly-kicks` | `1.7.0` listed |
| LL2 wheel published / available | UC Volume listing or Terraform `wheel_path` | Wheel `0.3.21` present |
| Bronze tables ALTERed | `scripts/migrate_bronze_for_pr_ll2.py` (idempotent) | Reports "already at target schema" both tables |
| dbt CI green | CI status on the merged commit | Green |
| All 4 event bronze tables have data | `SELECT COUNT(*) FROM bronze.{statsbomb,wyscout,idsse,metrica}_events` | >0 rows in all four |

### Phase ordering

1. Pre-merge: code + bronze ALTERs (idempotent)
2. Merge: PR-LL2 squash-merge
3. Snapshot: defensive Delta clone of bronze tables
4. Backfill: destructive DELETE + wf-vaep manual trigger
5. dbt rebuild: full-refresh `fct_action_values+ fct_funnel_stages_agg+`
6. Taipy deploy: `scripts/manage_space.py deploy production`
7. Validation: post-deploy scripts + smoke tests

Each phase requires explicit user OK before starting.

### Phase 3 — Defensive snapshot

```sql
CREATE TABLE soccer_analytics.bronze.spadl_actions_pre_ll2_backfill
DEEP CLONE soccer_analytics.bronze.spadl_actions;

CREATE TABLE soccer_analytics.bronze.vaep_action_values_pre_ll2_backfill
DEEP CLONE soccer_analytics.bronze.vaep_action_values;
```

Delta deep clones are metadata-only initially; negligible cost. Drop after Phase 7 holds green for ≥24 hours.

### Phase 4 — Destructive DELETE + wf-vaep trigger

```sql
DELETE FROM soccer_analytics.bronze.spadl_actions
WHERE data_source IN ('statsbomb', 'wyscout');

DELETE FROM soccer_analytics.bronze.vaep_action_values
WHERE data_source IN ('statsbomb', 'wyscout');
```

```bash
databricks jobs run-now --job-id <wf-vaep-job-id>
```

Expected duration: 10–20 minutes. Monitor every 30s; report progress every 60–120s per CLAUDE.md "Never disappear into long-running commands".

### Phase 5 — dbt full-refresh

```bash
uv run dbt run --full-refresh --select fct_action_values+ fct_funnel_stages_agg+
uv run dbt test --select fct_action_values fct_funnel_stages_agg
```

Full-refresh is required because: (1) column renames don't propagate via incremental append, (2) backfilled match_ids exist in the existing mart (the incremental skip-if-exists filter would skip them), (3) canonical `possession_id` has different semantics — full rebuild eliminates ambiguity.

Expected duration: 5–15 minutes for ~9.6M-row rebuild.

### Phase 6 — Taipy deploy

```bash
uv run python scripts/manage_space.py deploy production
```

Order matters: must follow Phase 5 (the dbt mart shape change) so the new Taipy code references columns that exist.

### Phase 7 — Validation

```bash
uv run pytest src/tests/test_marts_live_schema.py -v
uv run python scripts/validate_pr_ll2_post_deploy.py
uv run python scripts/measure_boundary_f1_full_corpus.py
# Manual smoke test on Conversion Funnel page (Wyscout / IDSSE / Metrica matches)
```

### Estimated total operational window

30–60 minutes from start to validated. User-facing impact during Phases 5–6 (10–20 minutes where Conversion Funnel may show stale or partial data). Worth a one-line announcement in any team channel watching the app.

### Rollback

| Failure point | Rollback |
|---|---|
| Phase 4 (DELETE succeeded, wf-vaep failed) | Re-trigger wf-vaep — the `_read_existing_match_ids` skip-already-converted logic handles partial state. If repeated failures, restore from snapshot |
| Phase 5 (dbt full-refresh failed) | Bronze data is correct; this is just downstream materialization. Investigate failure, fix, re-run |
| Phase 6 (Taipy deploy failed) | Roll back Taipy independently; bronze and mart remain in new shape |
| Phase 7 (validation failed — bad data) | Most serious. Investigate which validation/source/column. If recoverable in code: hotfix PR + re-trigger Phase 4. If structural rollback needed: restore bronze from `*_pre_ll2_backfill` clones; revert luxury-lakehouse main; redeploy old wheel; redeploy old Taipy app code |

The destructive DELETE in Phase 4 is the highest-risk step. Phase 3 snapshot is explicit insurance.

### What "done" looks like

- `bronze.vaep_action_values` row count restored to ~9.6M + ~17K (IDSSE + Metrica) ≈ ~9.62M
- All 4 `statsbomb_*` columns populated for StatsBomb rows (closes LL1 latent bug — currently 0/7M populated)
- `possession_id_heuristic` populated on every row across all 4 sources
- 5 GK enrichment columns populated according to per-helper semantics
- `action_id` populated on every row (was 100% NULL pre-LL2)
- `fct_action_values` mart in β-consistent shape; `test_marts_live_schema.py` passes against live Databricks
- `fct_funnel_stages_agg` shows non-zero `pos_in_gs` for Wyscout / IDSSE / Metrica matches
- Taipy "Conversion Funnel" page renders correctly on a Wyscout match (was previously broken / synthetic)

## Risks, open items, success metrics

### Risk register

| Risk | Likelihood | Impact | Mitigation |
|---|---|---|---|
| silly-kicks 1.7.0 API divergence from brief | LOW (verified `45ef2f8` matches brief) | MEDIUM | Already verified; minor `KLOPPY_SPADL_COLUMNS` vs `SPADL_COLUMNS` nuance handled by UDF projection |
| Boundary-F1 < 0.85 on the 3-match fixture | MEDIUM | LOW | Lower CI threshold to `(measured − 0.02)` if 0.80–0.85; investigate if <0.80 |
| Combined backfill exceeds 20-min estimate | MEDIUM | LOW | 60-min budget acceptable; run during low-traffic window if user prefers |
| New IDSSE/Metrica UDFs error on real data | MEDIUM | MEDIUM | TDD against luxury-lakehouse's bronze before merge; per-game errors propagate via `RuntimeError` with match_id (CLAUDE.md hard-fail-first UDF semantics) |
| dbt `--full-refresh` fails | LOW | HIGH | Re-runnable; bronze intact via Phase 3 snapshot |
| Taipy app references old mart columns post-deploy | LOW | MEDIUM | All known consumers enumerated in Section 4; `test_marts_live_schema.py` catches mart-shape drift in CI |
| Hyrum's Law: unknown external consumer of renamed columns | LOW | MEDIUM-HIGH | Internal grep clean; for external consumers, wheel version bump + PR description's column-rename table are the signals |
| LL1 latent bug fix doesn't actually populate columns | MEDIUM | HIGH | Two-layer defense: writer parity test + post-deploy validation script |
| IDSSE/Metrica match_ids not in dim_matches → NULL match_key | LOW | LOW | Existing `test_fct_action_values_match_key_not_null` asserts; verify in TDD |

### Open items deferred to TDD / writing-plans

1. **`wy_match_flag` rename**: keep semantic / rebind, or rename to `heuristic_possession_flag` (more general — catches IDSSE/Metrica too). Recommended: rename
2. **Column-rename adapter shape** between bronze schemas and silly-kicks 1.7.0 expected inputs. Brief targeted bronze schemas exactly; expect identity passthroughs in `spadl_adapter.py`'s new functions
3. **Test fixture match_id selection**: provisional 7298, 7584, 3855. Swap during TDD if any unavailable
4. **`possession_team_key` rename**: keep plain-named (Kimball convention wins) vs `statsbomb_possession_team_key` for full β-consistency
5. **Staging deploy first?**: User discretion; stays open until Phase 6

### Success metrics

| Metric | Target | Measurement |
|---|---|---|
| LL1 latent bug fixed | >7M StatsBomb rows have non-NULL `statsbomb_possession_id` (was 0/7M) | Phase 7 validation script |
| Boundary-F1 baseline | F1 ≥ 0.85 per StatsBomb competition (median across 22) | `scripts/measure_boundary_f1_full_corpus.py` post-deploy |
| 3 silly-kicks helpers integrated | `apply_spadl_enrichments` populates 6 columns + surfaces `action_id` for all 4 sources | Live mart test |
| Wyscout/IDSSE/Metrica funnel coverage | Conversion Funnel page shows non-zero `pos_in_gs` for these sources | Manual smoke test |
| Writer parity comprehensive | 5 writer parity tests pass | Unit tests |
| Schema gap class closed | `test_vaep_scoring_struct_matches_vaep_ddl` exists and passes | Unit test |
| Mart contract clean | dbt `contract: enforced: true` runs clean | dbt CI |
| 4-source guard works | `_VaepGuard.check()` correctly handles all 4 source tables | Manual verification post-merge |
| Wheel propagation | 22 PEP 723 + Terraform consumers reflect 0.3.21 | `bump_wheel.py` output |

## Decisions log

| # | Question | Decision |
|---|---|---|
| Q1 | Architecture pattern for post-conversion enrichments | A′ — named `apply_spadl_enrichments` stage in new module |
| Q1b | LL2 scope | LL2-cleanup: include action_id surfacing + LL1 vaep_schema fix + bronze.spadl_actions ALTER |
| Q1c | GK suite scope | 3 helpers: `add_possessions` + `add_gk_role` + `add_pre_shot_gk_context`; defer `add_gk_distribution_metrics` |
| Q2 | Mart shape for `possession_id` | β (canonical break — `possession_id` becomes canonical heuristic; `statsbomb_possession_id` exposed) |
| Q2.5 | β scope | β-consistent: rename all 4 StatsBomb-native mart aliases (`possession_id`, `possession_team_id`, `play_pattern`, `under_pressure`) |
| Q3a | Validation tiers | All three tiers (synthetic CI + small real CI + post-deploy full-corpus) |
| Q3b | F1 threshold | Conservative 0.85 (refine to `measured − 0.02` if needed) |
| Q3c | GK helper validation | Smoke + plausibility (silly-kicks owns algorithm tests; verified comprehensive) |
| Q3d | Writer parity test extension | Include in LL2 — extend to 4 source UDFs + VAEP scoring UDF + new enrichment columns |
| Q4 | Module placement for `apply_spadl_enrichments` | B — new module `src/ingestion/spadl_enrichments.py` |
| Q5 | `action_id` surfacing | i — surface from silly-kicks `convert_to_actions` output (stop dropping at projection) |
| Q6a | Branch name | `feat/spadl-enrichment-stage` |
| Q6b | Wheel + commit + backfill | 0.3.20 → 0.3.21; single squash-merge commit; combined backfill at LL2 close |
| Source coverage | Source-coverage scope | 4 sources in LL2: StatsBomb + Wyscout + IDSSE + Metrica (silly-kicks 1.7.0 unblocks IDSSE/Metrica) |
| silly-kicks side | Sportec + Metrica converter shape | Dedicated DataFrame converters in silly-kicks (mirrors statsbomb.py / wyscout.py); shipped in 1.7.0 (commit `45ef2f8`) |
| funnel mart migration | Reconciliation strategy | Option ii — full reconciliation (drop the Wyscout-synthetic-possession workaround; use canonical heuristic possession_id everywhere; update Taipy app driver-side logic) |
| SkillCorner | SPADL coverage | Deferred — blocked at events-ingestion layer |

## References

- silly-kicks 1.6.0 release: commit `0cff18e`, PyPI `silly-kicks==1.6.0`. Adds `Provider.SPORTEC` + `Provider.METRICA` to kloppy converter; fixes `_SoccerActionCoordinateSystem` instantiation bug
- silly-kicks 1.7.0 release: commit `45ef2f8`, PyPI `silly-kicks==1.7.0`. Adds dedicated `silly_kicks.spadl.sportec` + `silly_kicks.spadl.metrica` DataFrame converters; unifies `_fix_direction_of_play` across all six converters
- ADR-002: silent exception swallow elimination (writer/target schema drift guards — relevant pattern for the new writer parity tests)
- ADR-011: Kimball surrogate key migration (dual-column window for legacy native IDs; this PR amends with the `possession_team_id` early-sunset footnote)
- ADR-013: ML inference outputs dbt mart (canonical pattern for ML predictions through bronze → staging → mart; this PR follows analogous discipline for non-ML enrichments)
- ADR-014: HF card inventory parity (separate but referenced for the publisher-side delivery contract pattern)
- ADR-016 (NEW, this PR): SPADL post-conversion enrichment stage and canonical/native naming convention
- PR-LL1: silly-kicks 1.5.0 `preserve_native` integration; commit `e88b35b`; PR #223
- CLAUDE.md (project root): § Architecture Principles (SOLID, separation of concerns, ML inference outputs); § Code Quality (writer/DDL parity, hard-fail-first UDF semantics); § Project Conventions (silly-kicks library identity, wheel version policy)
