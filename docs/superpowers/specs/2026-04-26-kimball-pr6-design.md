# Kimball PR 6 — Defensive + Goalkeeper Mart Migration + IDSSE `is_progressive`

| Field | Value |
|---|---|
| **Date** | 2026-04-26 |
| **Author** | Karsten Skyt |
| **Status** | Approved (brainstorming) |
| **Cycle** | ADR-011 staged Kimball migration, PR 6 of 8 |
| **Branch** | `kimball-pr6-defensive-gk-pitch-control` |
| **Predecessor** | PR 5b shipped 2026-04-25 (`4cac3af`); PR #203 + #204 follow-ups merged 2026-04-26 (`a3aad58`, `65986b4`). `main` at `65986b4`. |
| **Successor** | PR 7 — Tracking + formations + pausa + tail facts + `fct_pausa_values` ADR-013 promotion. |

---

## 1. Goal

Bring all defensive marts, all goalkeeper marts, and the IDSSE pass classifier onto the conformed Kimball framework defined by ADR-011. After this PR ships and deploys:

- Every defensive and goalkeeper mart carries `match_key BIGINT`, `team_key BIGINT` (where applicable), and `player_key BIGINT` FKs alongside the legacy `match_id` / `team_id` / `player_id` columns during the 2026-07-22 dual-column window.
- Every defensive and goalkeeper mart carries `data_source` as a column (closing a latent multi-provider correctness gap on `fct_goalkeeper_stats`).
- Every surrogate row identifier (`defensive_value_id`, `pressure_id`, `defcon_action_id`, `gk_stat_id`, `gk_action_id`) hashes `data_source` into the grain.
- IDSSE rows in `fct_passes` carry a real `is_progressive` boolean computed by the same cross-provider definition used for SB / Wyscout / Metrica, replacing the literal `false` in `stg_idsse__passes`.
- The PR 5b live-invariant test is renamed and parameterized to cover all Kimball-keyed marts under one harness (`test_marts_kimball_contracts.py`), gating dev and prod deploys.

## 2. Architectural principles in force

Five rules govern every PR-6 decision; they extend uniformly to future provider onboarding (Respo.Vision, sn-gamestate, full SkillCorner, etc.):

1. **Conformed facts share one definition per metric.** Same column name = same semantics across every provider. `is_progressive` has one rule applied uniformly; tactical refinements ride separate columns.
2. **Every fact carries `data_source`.** No exceptions. Multi-provider warehouse correctness depends on this.
3. **Surrogate-key grain ⊇ business grain.** Every grain-defining column is hashed into the surrogate.
4. **Richer semantics ride a separate column, not a redefined one.** `is_line_breaking` already covers the tracking-aware tactical concept; we do not redefine `is_progressive`.
5. **Migration PRs migrate; analytics PRs design.** No new aggregates, no new derived metrics in PR 6 unless they're a side-effect of the migration's correctness floor (e.g. `fct_goalkeeper_stats.data_source`).

## 3. Scope

### 3.1 In scope

**Five marts migrated:**

| Mart | Material. | Current `match_id` type | Provider scope today |
|---|---|---|---|
| `fct_defensive_values` | incremental | string | `statsbomb_360`, `metrica_tracking` |
| `fct_defcon_actions` | incremental | string | `statsbomb_360`, `metrica_tracking` |
| `fct_defcon_pressure` | incremental | string | `statsbomb_360`, `metrica_tracking` |
| `fct_goalkeeper_stats` | table | bigint | `statsbomb` (+ `wyscout` via fct_action_values) |
| `fct_gk_actions_detail` | table | bigint | inherits `fct_action_values` |

**One staging classifier populated:** `stg_idsse__passes.is_progressive` derived from `end_frame` × `stg_idsse__tracking.ball_x/ball_y` lookup + existing `distance_to_goal` rule.

**One contract-level column addition:** `data_source` permanent column on `fct_goalkeeper_stats` (the only mart in scope where it's absent today; latent multi-provider correctness fix).

**Surrogate-key updates** on three marts whose hash inputs miss `data_source`: `fct_defensive_values`, `fct_defcon_pressure`, `fct_goalkeeper_stats`. Clean break — no `legacy_*_id` shim columns.

**Eight Taipy consumer files dual-read** (forward-compat plumbing only — no behaviour change):
`hf_taipy_app/src/queries/defensive.py`, `state/goalkeeper.py`, `queries/goalkeepers.py`, `state/pitch_control.py`, `queries/tracking.py`, `filters.py`, `main.py`, `test_render.py`. The last three may be import-only — verify during implementation, narrow if so.

**Test harness rename:** `test_marts_player_key_contracts.py` → `test_marts_kimball_contracts.py`, parameterized over `(mart, key_column, threshold)` covering PR 5b's 6 embedding marts (player_key) + PR 6's 5 defcon/GK marts (player_key, team_key, match_key, action_player_key where present).

**New live-invariant test:** `test_idsse_is_progressive_coverage.py` asserts non-NULL `is_progressive` rate on IDSSE rows in `fct_passes` ≥ implementation-measured threshold (committed to spec post-measurement).

**dbt deploy hygiene:**
- `on_schema_change='append_new_columns'` audit on the three incremental defcon marts BEFORE first push (PR 5b precedent).
- Post-merge `dbt run --select <marts>+ --full-refresh --target dev` step in deploy runbook (mandatory — surrogate-hash change orphans rows otherwise).
- Synced-table dual-defense audit: terminal `QUALIFY ROW_NUMBER() OVER (...)=1` AND `dbt_utils.unique_combination_of_columns` schema test on every PG-PK grain. Add the missing layer where present.

**Pitch-control staging promotion:** `stg_pitch_control__values` is consumed by `notebooks/publish_datasets.py:248` (HF dataset publisher for `luxury-lakehouse/pitch-control-tracking`, ~38M rows). Used → promoted to first-class treatment per the no-deprecation rule. Schema additions and provider-derivation logic detailed in §4.7. Includes:
- `match_key BIGINT` + `data_source STRING` columns derived via prefix CASE on `match_id`.
- Bronze Live Schema CI entry for `bronze.pitch_control_values` (closes drop-safety gap).
- `_ingested_at TIMESTAMP` declaration in `_pitch_control__sources.yml` + `_pitch_control__models.yml` (closes source/writer schema mismatch).
- YAML docstring clarification (per-player grain, not per-team).
- New `test_pitch_control_bronze_coverage.py` (parser-level) + entry in `test_staging_coverage.py` (live-DESCRIBE).
- Schema-test additions: `unique_combination_of_columns(['match_id','tracking_id'])` + `relationships severity: warn` from `match_key` to `dim_matches.match_key`.
- HF dataset card update (`pitch-control-tracking.md` 2026-07-22 dual-column window stanza). HF parquet payload itself stays unchanged in PR 6 (publish-time INNER JOIN unaffected by additive columns); payload column publish deferred to PR 8.

**Documentation drift fix:** `docs/huggingface/model-cards/defcon.md` — one-line edit to mention `match_key` / `team_key` / `player_key` / `data_source` invariants on its output marts during the dual-column window.

### 3.2 Out of scope

- **`fct_pitch_control_*` aggregate mart.** No such mart exists today; the live Pitch-Control page computes pitch-control on demand in Python from raw tracking frames. Deferred until a use case lands; that use case will trigger its own brainstorming → ADR → PR cycle.
- **Tracking + formations + pausa marts.** `fct_off_ball_xt`, `fct_space_creation`, `fct_tracking_avg_positions`, `fct_tracking_shape_timeline`, `fct_player_positions`, `fct_position_maps`, `fct_formation_labels`, `fct_pausa_rankings`, `fct_physical_stats`, `fct_pass_timing` all stay on `match_id` until PR 7. ADR-011 explicitly groups these as "tracking + formations + pausa + tail facts."
- **`fct_player_percentiles` `match_id` references.** PR 5b touched this mart for `player_key` but four `match_id` references remain. Verify in PR 7 implementation whether residual or legitimate season-aggregate metadata.
- **Tactical / line-breaking refinement of `is_progressive`.** Defending-line proximity at pass-release is already covered by `fct_line_breaking_results`. No new column on `fct_passes` (architectural rule 4).
- **HF dataset card payload updates.** No HF dataset publishes payload from any of the 5 PR-6 marts. Verified by grep against `docs/huggingface/dataset-cards/`.
- **HF model-card updates beyond `defcon.md`.** `off-ball-xt.md`, `space-creation.md`, `pitch-control.md`, `obso-pausa.md` describe models whose outputs land in PR 7-scope marts; updates fold into PR 7.
- **Legacy column drops.** `match_id`, `team_id`, `player_id` all preserved through 2026-07-22 sunset window (PR 8 owns the cleanup).
- **`canonical_player_id` rename.** Kept verbatim per Hyrum's Law / 57-file consumer cascade (PR 5b precedent).

## 4. Data model

### 4.1 IDSSE `is_progressive` derivation

`stg_idsse__passes` line 121 currently emits `false as is_progressive` because the DFL `<Play>` row has no end coordinate.

**Mechanism:**
- `stg_idsse__passes.end_frame` is already a column (DFL bronze attribute, surfaced verbatim in the PR 1.8 staging passthrough).
- `stg_idsse__tracking` exposes `ball_x`, `ball_y` per frame in 120×80 (replicated across player rows for the same frame).
- New CTE `ball_at_end_frame` selects DISTINCT `(match_id-without-prefix, period, frame, ball_x, ball_y)` from `stg_idsse__tracking`. Prefix-strip aligns with `stg_idsse__passes.match_id` which has the `idsse_` prefix already removed.
- LEFT JOIN on `(match_id, period, end_frame=frame)` populates `end_x`, `end_y`.
- `is_progressive` becomes the standard `{{ distance_to_goal('end_x','end_y') }} < {{ var('progressive_pass_ratio') }} * {{ distance_to_goal('start_x','start_y') }}` — the same expression `int_unified_passes` already uses for SB / WS / Metrica.

**Null handling:** when `end_frame` is NULL or the tracking lookup misses, `end_x/end_y` stay NULL and `is_progressive` evaluates to NULL (preserves "unknown" semantics rather than false-positive). Acceptable across the union: SB rows already produce NULL `is_progressive` when start_x or end_x is NULL.

**Coverage target:** ≥ 95% non-NULL `is_progressive` on IDSSE rows in `fct_passes`. Tightened to actual measurement during implementation (likely 99%+ given tracking coverage — measured before live-invariant threshold is committed).

### 4.2 Defensive marts — key resolution

`stg_defcon__results.data_source ∈ {'statsbomb_360', 'metrica_tracking'}` maps to `dim_matches.provider ∈ {'statsbomb', 'metrica'}` via inline CASE.

```sql
LEFT JOIN dim_matches dm
  ON dm.provider = CASE b.data_source
                     WHEN 'statsbomb_360' THEN 'statsbomb'
                     WHEN 'metrica_tracking' THEN 'metrica'
                   END
 AND dm.native_match_id = b.match_id
```

**LEFT JOIN with `severity: warn`** on `relationships` schema test rather than INNER JOIN — preserves row count during the dual-column window. PR 8 can tighten to INNER once legacy columns sunset.

`team_key` resolution: same provider CASE, JOIN on `dim_teams.native_team_id = cast(defender_team_id as string)`. `player_key`: same CASE, JOIN on `dim_players.native_player_id = cast(defender_player_id as string)`.

`fct_defcon_actions` carries two player roles: `defender_player_id` and `action_player_id`. Both resolve to dim_players via the same provider CASE. Output columns: `player_key` (defender) AND `action_player_key` (action). Single `team_key` for `defender_team_id` (no `action_team_id` exists today; introducing one is "new analytics" and out of scope).

### 4.3 Goalkeeper marts — `data_source` propagation + key resolution

**`fct_goalkeeper_stats` is the bigger surgery.** Today's `gk_actions` CTE pulls from `fct_action_values` but **drops `data_source`** in the projection.

Changes:
1. Add `av.data_source` to `gk_actions` projection.
2. Update `gk_matches` to group by `(player_id, match_id, data_source)`.
3. Update every downstream JOIN (`save_stats`, `collection_stats`, `pass_stats`, `sweeper_stats`, `psxg_agg`) to include `data_source` in the equality condition.
4. Final SELECT projects `data_source` as a permanent column.
5. Surrogate `gk_stat_id` hash inputs become `(player_id, match_id, data_source)`.
6. `match_key` / `team_key` / `player_key` resolved in final SELECT via the same provider-CASE pattern (data_source values from fct_action_values are `'statsbomb'` and `'wyscout'`, mapping 1:1 to `dim_matches.provider`).
7. **Net simplification:** the existing PR-3-transitional `dim_matches` bridges in `shot_save_stats` and `psxg_shots` (currently doing `try_cast(dm.native_match_id as bigint) as match_id`) get retired. Once `gk_matches` carries `match_key`, those CTEs join `fct_shots` / `stg_psxg__predictions` on `match_key` directly. File becomes shorter.

**`fct_gk_actions_detail` is simpler:** `data_source` already projected, surrogate `gk_action_id` is a passthrough cast of `fct_action_values.action_value_id`. Just add `match_key` / `team_key` / `player_key` in final SELECT via the provider-CASE pattern. **Verify during implementation:** `fct_action_values.action_value_id` surrogate construction must already encode `data_source` — re-read the file before finalizing. If not, prepend `data_source` to the cast in `fct_gk_actions_detail`.

### 4.4 Surrogate-key migration

| Mart | Current `<id>` hash inputs | New hash inputs | Effect on existing rows |
|---|---|---|---|
| `fct_defensive_values` | `(defender_player_id, match_id)` | `(defender_player_id, match_id, data_source)` | All IDs change |
| `fct_defcon_actions` | `(event_id, defender_player_id, data_source)` | unchanged | None |
| `fct_defcon_pressure` | `(action_player_id, match_id)` | `(action_player_id, match_id, data_source)` | All IDs change |
| `fct_goalkeeper_stats` | `(player_id, match_id)` | `(player_id, match_id, data_source)` | All IDs change |
| `fct_gk_actions_detail` | `cast(action_value_id as string)` (passthrough) | unchanged pending verification (§4.3) | None pending verification |

**Hyrum's Law check before implementation:** grep for hardcoded `defensive_value_id`, `pressure_id`, `gk_stat_id` literals across `src/tests/`, `docs/`, HF card markdowns. Best guess: zero hits (these are internal row identifiers; HF dataset payloads don't carry them). Surface findings to user before edit.

### 4.5 Column additions summary

| Mart | + match_key | + team_key | + player_key | + action_player_key | + data_source |
|---|:-:|:-:|:-:|:-:|:-:|
| `fct_defensive_values` | ✓ | ✓ (defender) | ✓ (defender) | — | (already there) |
| `fct_defcon_actions` | ✓ | ✓ (defender) | ✓ (defender) | ✓ | (already there) |
| `fct_defcon_pressure` | ✓ | — | ✓ (action) | — | (already there) |
| `fct_goalkeeper_stats` | ✓ | ✓ | ✓ | — | ✓ (NEW) |
| `fct_gk_actions_detail` | ✓ | ✓ | ✓ | — | (already there) |

All new key columns: `relationships severity: warn` to corresponding dim during the 2026-07-22 dual-column window.

### 4.6 Synced-table dual-defense audit

For every mart in scope that's synced to Lakebase, audit:
- Terminal `QUALIFY ROW_NUMBER() OVER (PARTITION BY <pg_pk_grain> ORDER BY <stable_tiebreaker>) = 1` in the final SELECT.
- `dbt_utils.unique_combination_of_columns` schema test in `_marts__models.yml` on the same column set.

Add the missing layer where present. Reference application: `fct_workflow_costs` / PR #203.

### 4.7 Pitch-control staging promotion

`stg_pitch_control__values` is consumed by `notebooks/publish_datasets.py:248` (HF dataset publisher for `luxury-lakehouse/pitch-control-tracking`). Per the Yoda rule (used → first-class), it gets the full migration in PR 6.

**Provider derivation (today):** `data_source` and `match_key` are derived at staging time via prefix CASE on `match_id`:

```sql
data_source = CASE
  WHEN match_id LIKE 'idsse_%' THEN 'idsse'
  WHEN match_id LIKE 'Sample_Game_%' THEN 'metrica'
  WHEN match_id LIKE '<skillcorner_prefix>%' THEN 'skillcorner'  -- TBD §10 #9
  ELSE CAST(NULL AS STRING)
END
```

Then LEFT JOIN `dim_matches`:

```sql
LEFT JOIN dim_matches dm
  ON dm.provider = <derived_data_source>
 AND dm.native_match_id = regexp_replace(match_id, '^(idsse_|Sample_Game_|<sk_prefix>)?', '')
```

**Schema additions:**

| Column | Source | Tests |
|---|---|---|
| `data_source STRING` | Prefix CASE | `not_null`, `accepted_values: ['idsse', 'metrica', 'skillcorner']` |
| `match_key BIGINT` | LEFT JOIN dim_matches | `relationships severity: warn` to `dim_matches.match_key` |
| `_ingested_at TIMESTAMP` | Bronze passthrough (writer already emits it) | Declared in source-YAML; no test |

**PR 7 coordination:** once `fct_tracking_frames` migrates in PR 7 and `pitch_control_batch.py` (the writer) is updated to emit `data_source` + `match_key` natively into bronze, the staging model collapses to a passthrough. Tracked in `project_kimball_migration_cycle.md` PR 7 row as a writer-side cleanup dependency.

**Consumer impact:** `notebooks/publish_datasets.py:248` (the HF dataset publisher) does INNER JOIN on `tracking_id` — additive columns don't break the JOIN. New columns are not yet exposed in the HF parquet payload (deferred to PR 8 with the rest of the dual-column closures).

## 5. Edge cases

| # | Edge case | Behavior |
|---|---|---|
| 1 | IDSSE pass with NULL `end_frame` | `end_x/end_y` stay NULL → `is_progressive` = NULL. Downstream `sum(case when is_progressive then 1 else 0 end)` correctly treats NULL as not-progressive. |
| 2 | IDSSE tracking miss at `end_frame` (half-time, ball-out-of-play) | Same as #1 — NULL `is_progressive`. Acceptable. |
| 3 | DEFCON row with unexpected `data_source` (future provider, typo) | CASE → NULL → no dim_matches match → `match_key` NULL → `relationships severity: warn` fires. Surfaces drift without blocking. |
| 4 | DEFCON 360-synthetic defender | `defender_player_id` is synthesized (not in dim_players). `defender_player_key` resolves to NULL. `action_player_key` (real player) resolves cleanly. Both correct under warn-severity. |
| 5 | Metrica anonymized DEFCON rows | dim_players carries synthesized rows for Metrica anonymous players (PR 5a). Coverage should be 100% by construction; verify during implementation. |
| 6 | SB+WS GK with same `match_id` BIGINT value | Pre-PR-6 their `gk_stat_id` would collide and the `unique` test would fail; today's data avoids this by happy accident. Post-PR-6 they hash separately via `data_source`. **Latent correctness fix.** |
| 7 | Surrogate-hash change on first incremental build | New IDs don't match existing IDs in target → MERGE inserts new rows next to old → row count doubles silently. **Mandatory mitigation:** post-merge `--full-refresh` (PR 5b precedent). |
| 8 | Live-CI `state:modified+` cascades into downstream marts that haven't built since PR 5b | PR 4b/5b precedent: latent bugs surface. Path X (fold-into-PR with one-line warn-suppression + YAML pointer to resolving PR) approved if blocker is small + isolated. |
| 9 | `fct_gk_actions_detail` surrogate stability hinges on `fct_action_values.action_value_id` being data_source-aware | Verify during implementation (§4.3). |
| 10 | Lakebase synced-table behavior on surrogate hash change | Schema doesn't change (column type stays STRING). Old hashed rows replaced on next SNAPSHOT refresh; new hashes are unique by construction. **Additive auto-evolve only — no UI recreate.** |
| 11 | Test fixtures or HF payloads with hardcoded surrogate IDs | Pre-implementation grep (§4.4 Hyrum's Law check). |
| 12 | DEFCON `data_source = 'metrica_tracking'` joining on `dim_matches.native_match_id = 'Sample_Game_1'` | Both sides STRING. ✓ |
| 13 | Pitch-control reference `fct_defcon_actions.pitch_control_at_action` | Numeric value, not an FK. No migration concern. |
| 14 | SkillCorner `match_id` prefix unverified at design time | If SkillCorner match_ids are pure BIGINTs or use an unexpected prefix, the §4.7 prefix CASE misroutes them. Mitigated by §10 #9 verification before merging — sample `bronze.pitch_control_values WHERE match_id NOT LIKE 'idsse_%' AND match_id NOT LIKE 'Sample_Game_%'` to confirm. |
| 15 | Pitch-control HF parquet payload schema drift | Adding `data_source` + `match_key` to staging is additive. JOIN on `tracking_id` in `notebooks/publish_datasets.py:248` is unaffected. Smoke-publish in dev before declaring ship (§10 #10). |

## 6. Testing

### 6.1 Test inventory

| Test | Type | Scope |
|---|---|---|
| `test_marts_kimball_contracts.py` (renamed from `test_marts_player_key_contracts.py`) | Live invariant | Parameterized over `(mart, key_column, threshold)` covering PR 5b's 6 embedding marts (player_key) + PR 6's 5 defcon/GK marts (player_key, team_key, match_key, action_player_key where present). One harness; threshold per row. |
| `test_idsse_is_progressive_coverage.py` | Live invariant | Asserts ≥ implementation-measured threshold (committed post-measurement) on IDSSE rows in `fct_passes`. |
| dbt schema tests (per mart, in `_marts__models.yml`) | Compile-time | `unique` on every renamed surrogate; `relationships severity: warn` on each new FK to dim; `dbt_utils.unique_combination_of_columns` on every synced mart's PG PK grain. |
| `test_dbt_passes_kimball_migration.py` (existing) | Compile-time + live | Extend to assert `int_unified_passes` IDSSE branch produces non-NULL `is_progressive` post-migration. |
| `test_marts_live_schema.py` (existing — Bronze Live Schema CI sibling) | Live | Add 5 PR-6 marts to the live DESCRIBE assertions so future schema drift is caught. |
| `test_bronze_live_schema.py` (existing — drop-safety net from PR #174/#175) | Live | Add `bronze.pitch_control_values` to the live DESCRIBE assertions (closes a previously-uncovered bronze table). |
| `test_pitch_control_bronze_coverage.py` (new) | Parser-level | Per `feedback_coverage_test_pattern` — every `bronze.pitch_control_values` column surfaced in `stg_pitch_control__values`. |
| `test_staging_coverage.py` (existing) | Live | Add `stg_pitch_control__values` entry. |

### 6.2 Pyright / ruff

- No new Python modules expected outside the test files themselves.
- Test files follow PR 5b's pattern: `pytest.importorskip("databricks.sql")` + `requires_databricks` skip marker on env vars.
- `# ruff: noqa: S608` header on the test file (string-formatted SQL is read-only mart-name interpolation, not user input).

### 6.3 Pre-push gates

```
uv run ruff check src/ scripts/ dbt_project/
uv run ruff format --check src/ scripts/
uv run pyright src/
uvx --from "dbt-core>=1.10.0,<1.12.0" --with dbt-databricks dbt parse --project-dir dbt_project
uv run pytest src/tests/ -v --ignore=src/tests/test_marts_kimball_contracts.py --ignore=src/tests/test_idsse_is_progressive_coverage.py
```

Live-invariant tests deferred to post-merge (require dev_gold env).

### 6.4 CI gates

- `validate` — dbt parse, schema YAML lint. Should be green on first push.
- `semgrep` — green; no new patterns.
- `lint-and-test` — green; pyright + ruff + pytest.
- `live-build` (PR 4a's serverless dbt run) — **expect surfacing**. Triage per PR 4b/5b playbook. Path X authority approved.

## 7. Ship criteria

### Pre-merge
- All four CI checks green: validate, semgrep, lint-and-test, live-build.
- No new ruff or pyright violations.
- No new Semgrep findings.
- All dbt schema tests green (unique + relationships warn + unique_combination_of_columns).

### Post-merge dev deploy
- Five marts + pitch-control staging rebuilt via `dbt run --select fct_defensive_values+ fct_defcon_actions+ fct_defcon_pressure+ fct_goalkeeper_stats+ fct_gk_actions_detail+ stg_pitch_control__values+ --full-refresh --target dev` complete with WARN=0, ERROR=0 (PASS count varies by selected model graph).
- All five PR-6 synced tables transition to `ONLINE_NO_PENDING_UPDATE` after `refresh_synced_tables.py --tables ... --wait`.
- `maintain_synced_tables.py` completes Steps 0.5 (grants) + 1 (refresh) + 2 (create_indexes) + 3 (verify_indexes) cleanly.
- `test_marts_kimball_contracts.py` parameterized over the 11 (mart, key) pairs — all PASS at ≥99% non-NULL.
- `test_idsse_is_progressive_coverage.py` PASS at the implementation-measured threshold.
- `test_pitch_control_bronze_coverage.py` PASS — every `bronze.pitch_control_values` column surfaced in `stg_pitch_control__values`.
- `stg_pitch_control__values.match_key` non-NULL ≥ implementation-measured threshold (target ≥99% post-prefix-CASE; lower if SkillCorner match_ids don't resolve in dim_matches).
- `notebooks/publish_datasets.py` smoke-publish in dev → HF dataset still emits ~38M rows post-migration (no JOIN regression).
- IDSSE rows in `fct_passes` show non-zero `progressive_passes` count when grouped by `data_source = 'idsse'` (smoke check confirming the literal `false` is gone).

### Documentation
- Memory entry captured (`project_kimball_pr6_shipped.md`).
- Cycle-state memory updated (`project_kimball_migration_cycle.md`) — PR 6 row marked SHIPPED, PR 7 row promoted to NEXT.
- ADR-011 staged-rollout table updated — PR 6 row Status: Shipped (date, commit hash).
- `docs/huggingface/model-cards/defcon.md` — one-line edit reflecting the new key columns.
- HF dataset cards: zero changes.

## 8. Risks

| # | Risk | Severity | Mitigation |
|---|---|---|---|
| R1 | Surrogate-hash break orphans rows on first incremental build (defensive_values, defcon_pressure) | High | **Mandatory** post-merge `--full-refresh` in deploy runbook. PR 5b precedent. |
| R2 | Live-CI `state:modified+` cascade surfaces latent bugs in downstream marts | Medium | Path X authority approved. Same playbook as PR 4b. |
| R3 | DEFCON 360-synthetic defenders don't resolve in dim_players → `defender_player_key` NULL on a fraction of rows | Low | `relationships severity: warn` accepts NULL FKs during dual-column window. Live-invariant test threshold accommodates the 360-synthetic floor. |
| R4 | `fct_gk_actions_detail.gk_action_id` stability assumes `fct_action_values.action_value_id` already encodes data_source | Medium | Verify by reading `fct_action_values.sql` surrogate construction. If not, add `data_source` to the cast. |
| R5 | Lakebase synced-table refresh fails on a PR-6 mart | Low | Parallel-poll via PR #204 surfaces per-table failures within max-of-pipeline duration. |
| R6 | Hardcoded surrogate IDs in test fixtures or HF payloads break on hash change | Low–Medium | Pre-implementation grep. Best guess: zero hits. |
| R7 | IDSSE tracking coverage at `end_frame` materially below 95% → `is_progressive` mostly NULL | Medium | Coverage measured during implementation; threshold set to actual. |
| R8 | dbt incremental marts without `on_schema_change='append_new_columns'` fail live-CI build on first push | Low | Pre-push audit: grep `materialized='incremental'` in touched files, confirm config. |
| R9 | DEFCON `data_source` mapping CASE drifts as new providers arrive | Low | Inline CASE acceptable for two providers; helper macro if a third arrives. |
| R10 | `fct_goalkeeper_stats` rebuild changes existing row identity → consumers holding `gk_stat_id` references break | Low | Same Hyrum's Law check as R6. |
| R11 | Provider-prefix CASE in `stg_pitch_control__values` drifts as new tracking providers arrive | Low | Inline CASE acceptable for 3 providers today. PR 7 collapses staging to passthrough once writer emits `data_source` natively. Helper macro if a 4th provider arrives before then. |
| R12 | `notebooks/publish_datasets.py:248` JOIN breaks if column is renamed (rather than added) | Very Low | Migration is additive only — JOIN on `tracking_id` unaffected. Verify with a publish smoke test in dev before declaring ship (§10 #10). |
| R13 | SkillCorner `match_id` doesn't fit the prefix CASE → `data_source` NULL → `match_key` NULL → relationships warn fires across all SkillCorner rows | Medium | §10 #9 verifies the prefix BEFORE merge. If SkillCorner match_ids are pure BIGINTs (no prefix), the CASE adds an `ELSE 'skillcorner'` branch as the residual. |

## 9. Rollout

### Phase 0 — Branch + first push

1. `git checkout main && git pull` → confirm at or after `65986b4`.
2. `git checkout -b kimball-pr6-defensive-gk-pitch-control`.
3. Implementation: SQL changes in `dbt_project/models/`, YAML contract updates in `_marts__models.yml`, test rename + parameterization, IDSSE staging change, `defcon.md` model card edit.
4. Pre-push gate: `uv run ruff check && ruff format --check && pyright src/ && dbt parse && pytest src/tests/`.
5. **Single commit per branch.** First push, open PR.

### Phase 1 — CI green + merge approval

1. CI gates: validate / semgrep / lint-and-test / live-build all green. Triage live-CI cascade per Path X authority (R2).
2. Pre-existing CI blockers folded into 2nd commit on the same branch — squash-merge collapses to one commit on `main`.
3. Pause for explicit user approval before `gh pr merge`.

### Phase 2 — Post-merge dev deploy (autonomous, no per-step approval)

1. `dbt run --select fct_defensive_values+ fct_defcon_actions+ fct_defcon_pressure+ fct_goalkeeper_stats+ fct_gk_actions_detail+ --full-refresh --target dev` (R1 mitigation).
2. `uv run python scripts/refresh_synced_tables.py --tables fct_defensive_values_synced fct_defcon_actions_synced fct_defcon_pressure_synced fct_goalkeeper_stats_synced fct_gk_actions_detail_synced --wait` (parallel-poll via PR #204).
3. `uv run python scripts/maintain_synced_tables.py --skip-refresh` (Steps 0.5 + 2 + 3).
4. Live-invariant test: `uv run --with databricks-sql-connector pytest src/tests/test_marts_kimball_contracts.py src/tests/test_idsse_is_progressive_coverage.py -v`.
5. Smoke check: `select data_source, count(*), sum(case when is_progressive then 1 else 0 end) as prog_count from fct_passes group by data_source` — confirm IDSSE row has nonzero `prog_count`.

### Phase 3 — Documentation + memory

1. Update `project_kimball_migration_cycle.md` — PR 6 marked SHIPPED with commit hash + date.
2. Write `project_kimball_pr6_shipped.md` (mirrors PR 5b memory shape: delivered scope, key coverage numbers, follow-up list, don't-re-run list).
3. Update ADR-011 staged-rollout table — PR 6 row Status changes "Planned" → "Shipped (YYYY-MM-DD, hash)".
4. Update `MEMORY.md` index entry.

### Phase 4 — Branch cleanup

Per `feedback_only_git_gates_need_approval`, branch deletion is a user-controlled git operation. Pause for approval after Phase 3 before `git branch -d kimball-pr6-defensive-gk-pitch-control`.

## 10. Open implementation-time verifications

Items resolved at implementation time, not at design time:

1. `fct_action_values.action_value_id` surrogate construction — does it already encode `data_source`? If not, prepend `data_source` in `fct_gk_actions_detail.gk_action_id` cast (§4.3).
2. dim_players coverage on Metrica anonymized DEFCON defender_player_ids (§5 #5).
3. Hyrum's Law grep results for hardcoded surrogate IDs (§4.4).
4. Eight Taipy consumer files — narrow to the subset that actually queries the migrated marts; the other three may be import-only (§3.1).
5. Synced-table dual-defense audit — which marts already have QUALIFY + `unique_combination_of_columns`, which need adding (§4.6).
6. `on_schema_change='append_new_columns'` config audit on the three incremental defcon marts (§3.1, R8).
7. Live coverage measurement of IDSSE `is_progressive` (§4.1, R7) → committed threshold for live-invariant test.
8. Per-(mart, key) coverage measurement at first dev rebuild → committed thresholds in `test_marts_kimball_contracts.py` parameterization. `fct_defcon_actions.defender_player_key` is expected to have a lower threshold than `action_player_key` and other keys due to 360-synthetic defenders that don't resolve in `dim_players`.
9. SkillCorner `match_id` prefix in `bronze.pitch_control_values` — sample DISTINCT match_ids (`SELECT DISTINCT match_id FROM bronze.pitch_control_values WHERE match_id NOT LIKE 'idsse_%' AND match_id NOT LIKE 'Sample_Game_%' LIMIT 50`) to confirm the format. Lock the prefix CASE in §4.7 before merging.
10. `notebooks/publish_datasets.py` smoke test in dev — verify the HF dataset still publishes ~38M rows post-migration. Ensures the JOIN on `tracking_id` continues to work after the additive `data_source` + `match_key` columns are introduced.

## 11. Related references

- **ADR:** `docs/superpowers/adrs/ADR-011-unified-kimball-match-dimension.md`.
- **Predecessor specs:** `docs/superpowers/specs/2026-04-22-kimball-pr3-shots-xg-design.md`, `2026-04-23-kimball-pr4-action-values-plus-deferrals-design.md`, `2026-04-24-kimball-pr5-design.md`.
- **Predecessor plans:** `docs/superpowers/plans/2026-04-25-kimball-pr5b-embedding-marts.md`.
- **Memory anchors:** `project_kimball_pr5b_shipped.md`, `project_kimball_migration_cycle.md`, `feedback_dbt_incremental_on_schema_change.md`, `reference_synced_pg_pk_dual_defense.md`, `reference_live_ci_surfaces_latent_bugs.md`, `feedback_only_git_gates_need_approval.md`, `feedback_no_approval_asks_in_plan_execution.md`, `feedback_agent_tool_requires_per_call_approval.md`, `feedback_single_commit_squash.md`.
- **Reference applications for patterns:** `fct_player_stats.sql` (PR 5a, INNER JOIN dim_players + on_schema_change config), `fct_workflow_costs.sql` (PR #203, QUALIFY tiebreaker + unique_combination_of_columns dual-defense).
- **Pitch-control consumer:** `notebooks/publish_datasets.py:248` — INNER JOIN to `stg_pitch_control__values` for HF dataset `luxury-lakehouse/pitch-control-tracking` publish. Surfaces the staging model as a runtime consumer; promotion driver (§4.7).
- **Workflow card + writer:** `workflow-cards/wf-pitch-control.yaml`, `src/ingestion/pitch_control_batch.py` — PR 7 coordination point for writer-side `data_source`/`match_key` propagation.
