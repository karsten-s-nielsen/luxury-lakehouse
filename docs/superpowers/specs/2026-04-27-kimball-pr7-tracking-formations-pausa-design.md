# Kimball PR 7 — Tracking + Formations + Pausa + Tail Facts + Conformed-Fact Closure + ADR-013 Promotion

| Field | Value |
|---|---|
| **Date** | 2026-04-27 |
| **Author** | Karsten Skyt |
| **Status** | Approved (brainstorming) |
| **Cycle** | ADR-011 staged Kimball migration, PR 7 of 8 (final migration PR; PR 8 = pure cleanup) |
| **Branch** | `kimball-pr7-tracking-formations-pausa` |
| **Predecessor** | PR 6 shipped 2026-04-27 (`#207` + 5 followups #208/#209/#210/#211/#212, last commit `e86c251`); main currently at `f81d2b8` (PR #213, ext-v2 Phase 1 — out-of-cycle, does not affect PR 7 base). |
| **Successor** | PR 8 — pure cleanup: drop legacy `*_id` INT columns + HF dataset legacy-column sunsets aligned to 2026-07-22 + sunset `canonical_player_id`. PR 8 scope is LOCKED per `project_kimball_pr8_scope_locked` — no further migration work defers to it. |

---

## 1. Goal

Close the ADR-011 staged Kimball migration in one PR. After PR 7 ships and deploys:

- **Every fact mart in the warehouse carries the appropriate Kimball surrogate FKs** (`match_key`, `team_key`, `player_key`, `opponent_team_key` where applicable) alongside legacy `match_id` / `team_id` / `player_id` columns during the 2026-07-22 dual-column window. PR 8 will then drop the legacy columns; no smart-key island remains.
- **`fct_pausa_values` is promoted to a dbt-built mart with `contract: enforced: true`** per ADR-013 (second application after PR 3's xG v2). Python writer emits only native identifiers + predictions to `bronze.pausa_values`; the gold mart resolves keys via INNER JOIN to `fct_passes` ON `pass_id`.
- **`stg_pitch_control__values` collapses to a passthrough** as `pitch_control_batch.py` writer emits `data_source` + `match_key` natively into `bronze.pitch_control_values` (closes the prefix-CASE bridge introduced by PR 6 §4.7).
- **Tracking marts resolve `team_key` cleanly via staging-level `team_id` derivation** in `stg_idsse__tracking` (already done in PR 5a), `stg_metrica__tracking` (new), `stg_skillcorner__tracking` (new), and any formation/positions staging that surfaces team — not via fragile name-matching at mart layer.
- **Every affected HF artifact (model card + dataset card + dataset payload) ships within PR 7 scope** — including absorption of PR 5b's 5 football2vec deferred payload publishes and PR 6's pitch-control HF parquet payload publish. Per `feedback_hf_artifacts_in_scope_pr` + `project_kimball_pr8_scope_locked`.
- **`test_marts_kimball_contracts.py` extends to cover all PR 7 marts**; new `test_marts_kimball_completion.py` invariant catches any future smart-key resurfacing.
- **ADR-011 §Notes staged-rollout table** updates PR 7 row to Shipped; PR 8 row stays as cleanup-only.

## 2. Architectural principles in force

Six rules govern every PR-7 decision; numbered to align with PR 6's five plus one new (HF artifact parity):

1. **Conformed facts share one definition per metric.** Same column name = same semantics across every provider. (Carried from PR 6.)
2. **Every fact carries `data_source`.** No exceptions. Multi-provider warehouse correctness depends on this. (Carried.)
3. **Surrogate-key grain ⊇ business grain.** Every grain-defining column is hashed into the surrogate. (Carried.)
4. **Richer semantics ride a separate column, not a redefined one.** (Carried.)
5. **Migration PRs migrate; analytics PRs design.** No new aggregates or derived metrics in PR 7 unless they're a side-effect of correctness floor (e.g., adding `team_id` to staging is a correctness floor for team_key resolution). (Carried.)
6. **HF artifact parity ships in the same PR as the source change.** Cards AND payloads. Per `feedback_hf_artifacts_in_scope_pr` + ADR-014. (NEW.)

## 3. Scope

### 3.1 In scope — Mart-layer Kimball completion (21 marts modified + 1 new)

**Tracking subsystem (Q1 staging team_id derivation feeds these):**

| Mart | Material. | Current keys | + match_key | + team_key | + player_key |
|---|---|---|:-:|:-:|:-:|
| `fct_tracking_frames` | incremental | match_id, team(STR), player_id | ✓ | ✓ | ✓ |
| `fct_tracking_avg_positions` | incremental | inherits | ✓ | ✓ | ✓ |
| `fct_tracking_shape_timeline` | incremental | inherits | ✓ | ✓ | ✓ |
| `fct_player_positions` | incremental | from shape graphs | ✓ | ✓ | ✓ |
| `fct_position_maps` | incremental | downstream | ✓ | ✓ | ✓ |
| `fct_formation_labels` | incremental | match_id, team(STR), period | ✓ | ✓ | — (no player) |
| `fct_physical_stats` | incremental | match_id, player_id | ✓ | — (no team grain) | ✓ |

**Pausa subsystem (Q2 ADR-013 promotion + key extension):**

| Mart | Material. | Current keys | Action |
|---|---|---|---|
| `fct_pausa_values` (NEW) | table | n/a — currently Python-only | Create dbt mart, `contract: enforced: true`, INNER JOIN fct_passes ON pass_id; inherits match_key/team_key/player_key |
| `fct_pausa_rankings` | table | player_id | + player_key (career grain — no match/team) |
| `fct_pass_timing` | table | player_id, match_id | + match_key + player_key |

**Off-ball / space-creation:**

| Mart | Material. | Current keys | + match_key | + team_key | + player_key |
|---|---|---|:-:|:-:|:-:|
| `fct_off_ball_xt` | incremental | player_id, match_id | ✓ | — | ✓ |
| `fct_space_creation` | table | match_id, frame_id, player_id, team(STR) | ✓ | — (team is 'home'/'away') | ✓ |

**Tail facts (kickoff scope):**

| Mart | Material. | Current keys | + match_key | + team_key | + player_key |
|---|---|---|:-:|:-:|:-:|
| `fct_discipline_events` | table | match_id BIGINT, team_id, player_id | ✓ | ✓ | ✓ |

**Conformed-fact closures (Q3 — best-practice extension beyond kickoff):**

| Mart | Has | Adds | Notes |
|---|---|---|---|
| `fct_passes` | match_key | + team_key + player_key (passer + recipient) | Identity fact for pausa INNER JOIN; surrogate-hash gains `data_source` |
| `fct_shots` | match_key | + team_key + player_key | Identity fact for `fct_xg_predictions_v2` (ADR-013) |
| `fct_action_values` | match_key | + team_key + player_key | 9.5M-row spine |
| `fct_match_summary` | match_key | + home_team_key + away_team_key | line 230 acknowledged |
| `fct_line_breaking_results` | match_key | + team_key + player_key (passer) | per-pass conformed |

**Aggregate marts:**

| Mart | Has | Adds | Notes |
|---|---|---|---|
| `fct_heatmap_agg` | comp_id, team_id (match-collapsed) | + team_key | from fct_passes + fct_shots |
| `fct_vaep_breakdown_agg` | comp_id, team_id, player_id (match-collapsed) | + team_key + player_key | from fct_action_values |

**Pull-through extensions (one-line column additions per mart):**

| Mart | Adds | Notes |
|---|---|---|
| `fct_xg_predictions` | + team_key + player_key (via fct_shots) | conformed-fact rule extension |
| `fct_xg_predictions_v2` | + team_key + player_key (via fct_shots) | same |

**Bridge retirement (PR 6 carryover):**

- `fct_player_percentiles.physical_by_comp` CTE — currently routes via `dim_matches.native_match_id` because `fct_physical_stats` is native-keyed. Once `fct_physical_stats` has `match_key` in PR 7, the bridge collapses to `INNER JOIN fct_match_summary ON match_key`.

### 3.2 In scope — Staging-level team_id derivation (Q1)

- `stg_idsse__tracking` — already has `team_id` (PR 5a, line 38-42). Verify propagation through `fct_tracking_frames` (currently dropped from the SELECT).
- `stg_metrica__tracking` — new derivation: JOIN `dim_teams` on `(provider='metrica', match_id, team='home'|'away')` to pull synthesized anonymized team_id from PR 5a's Metrica pseudo-comp.
- `stg_skillcorner__tracking` — new derivation: `CASE WHEN team='home' THEN home_team_id ELSE away_team_id END` (existing bronze passthroughs).
- `stg_formations__labels` — verify column shape during implementation; add `team_id` derivation matching tracking pattern.
- `stg_shape_graphs__positions` — verify column shape during implementation; add `team_id` derivation.
- `fct_tracking_frames` final SELECT propagates `team_id` (currently drops to STRING `team` only).
- All 5 tracking-derivative marts gain `team_id` passthrough.

### 3.3 In scope — Writer-layer schema reconciliation

**`src/ingestion/pausa.py` — ADR-013 retarget:**
- `_TABLE_NAME` constant moves from `"fct_pausa_values"` → `"pausa_values"`.
- SkipGuard `results_table` reference moves from `{catalog}.{DEFAULT_GOLD_SCHEMA}.fct_pausa_values` → `{catalog}.bronze.pausa_values`.
- `write_delta_table()` schema arg moves to `bronze`.
- `_RESULTS_SCHEMA` unchanged — already emits only native IDs + predictions per ADR-013 §2.

**`src/ingestion/pitch_control_batch.py` — schema widening:**
- `_RESULTS_SCHEMA` adds `data_source STRING, match_key BIGINT` (becomes: `tracking_id STRING, match_id STRING, data_source STRING, match_key BIGINT, pitch_control_value DOUBLE, _ingested_at TIMESTAMP`).
- `_PITCH_CONTROL_BRONZE_COLS` extends with the two new cols.
- Reader pulls `source_provider` + `match_key` from `fct_tracking_frames` (post-PR-7 has `match_key`).
- `applyInPandas` UDF emits both new columns.
- Bronze MERGE schema additive auto-evolve via Delta (no manual drop/recreate).

**Single wheel bump:** 0.3.18 → 0.3.19 covers both writer changes.

### 3.4 In scope — Synced-table grant + PG-PK changes

For every mart in §3.1 that's synced to Lakebase:
- Audit `terraform/modules/synced_tables/main.tf` — verify PG-PK grain matches new mart contract.
- Surrogate-hash changes that update grain (e.g., adding `data_source` to grain) require synced-table delete + UI-recreate per `reference_synced_table_pk_recreation` — additive auto-evolve will not handle PK changes.
- All PG-PK-grain-stable marts: additive auto-evolve via `scripts/refresh_synced_tables.py --wait`.
- Grants automation (PR #179 maintain_synced_tables Step 0.5) handles re-grant after recreate; no manual SQL.
- New `fct_pausa_values` synced-table TF resource + `SYNCED_TABLES` registry + `create_indexes.py` index set.

### 3.5 In scope — HF artifact parity (cards AND payloads, all in PR 7)

**Affected by PR 7 mart changes:**

| HF artifact | Card path | Publish script | Why affected |
|---|---|---|---|
| `obso-pausa-values` (dataset) | `docs/huggingface/dataset-cards/obso-pausa-values.md` | `notebooks/publish_obso_data.py` | PAUSA mart promotion (Q2); INNER JOIN to fct_passes surfaces match_key/team_key/player_key |
| `pitch-control-tracking` (dataset) | `docs/huggingface/dataset-cards/pitch-control-tracking.md` | `notebooks/publish_datasets.py:248` | `stg_pitch_control__values` gains `data_source` + `match_key` (writer collapse) |
| `space-creation-values` (dataset) | `docs/huggingface/dataset-cards/space-creation-values.md` | `notebooks/publish_datasets.py` (verify) | `fct_space_creation` gains match_key + player_key |
| `line-breaking-passes` (dataset) | `docs/huggingface/dataset-cards/line-breaking-passes.md` | `notebooks/publish_datasets.py` (verify) | `fct_line_breaking_results` gains team_key + player_key |
| `spadl-vaep-action-values` (dataset) | `docs/huggingface/dataset-cards/spadl-vaep-action-values.md` | `scripts/publish_spadl_vaep_hf.py` | `fct_action_values` gains team_key + player_key |
| `statsbomb-shots-on-target` (dataset) | `docs/huggingface/dataset-cards/statsbomb-shots-on-target.md` | `scripts/publish_xg_shots_hf.py` | `fct_shots` gains team_key + player_key |
| `xg-shot-data` (dataset) | `docs/huggingface/dataset-cards/xg-shot-data.md` | `scripts/publish_xg_shots_hf.py` | `fct_shots` gains team_key + player_key |
| `xg-freeze-frame-data` (dataset) | `docs/huggingface/dataset-cards/xg-freeze-frame-data.md` | `scripts/publish_freeze_frame_hf.py` | `fct_shots` lineage |

**Absorbed from PR 5b / PR 6 deferrals:**

| HF artifact | Card path | Publish script | Why absorbed |
|---|---|---|---|
| `football2vec-player-embeddings` (dataset) | `docs/huggingface/dataset-cards/football2vec-player-embeddings.md` | (verify; PR 5b export script) | PR 5b deferred payload publish to PR 8; PR 8 is locked-cleanup |
| `football2vec-360-embeddings` (dataset) | same dir | (verify) | same |
| `football2vec-training-data` (dataset) | same dir | (verify) | same |
| `football2vec-360-training-data` (dataset) | same dir | (verify) | same |
| `football2vec-statsbomb-wyscout` (dataset) | same dir | (verify) | same |

**Model cards (text-only updates + push):**

| Card | Path | Change |
|---|---|---|
| `off-ball-xt.md` | `docs/huggingface/model-cards/` | `fct_off_ball_xt` gains keys |
| `space-creation.md` | same | `fct_space_creation` gains keys |
| `pitch-control.md` | same | bronze schema widens |
| `obso-pausa.md` | same | mart promotion; key inheritance from fct_passes |
| `xg.md` | same | `fct_xg_predictions` + `_v2` pull-through |
| `defcon.md` | same | one-line update if any PR 6-touched line evolves; verify at impl |

**Per-artifact action shape:**
- For each dataset: read publish script at impl time → determine if SELECT needs updating to surface new columns → update script + card text → include in Phase 2 publish run.
- For each model card: text-only edit + push via `scripts/publish_hf_cards.py --kind model --name <card>.md`.
- Filename-equals-repo-basename invariant per ADR-014.
- Per `reference_hf_publish_pattern`: never re-implement README push inline — use `ingestion.hf_publish.upload_hf_readme` or `scripts/publish_hf_cards.py` exclusively.

### 3.6 Out of scope

- **Drop legacy `match_id`/`team_id`/`player_id` INT columns.** PR 8 (locked cleanup, post-2026-07-22).
- **Sunset `canonical_player_id`.** PR 8.
- **`fct_pitch_control_*` aggregate mart.** No such mart exists; PR 6 §3.2 still applies — deferred until use case.
- **HF dataset payload sunsets.** PR 8 cleanup (drop legacy columns from payloads aligned to 2026-07-22).
- **New analytics or derived metrics.** Per principle 5; PR 7 is migration only.
- **`canonical_player_id` rename.** Kept verbatim per Hyrum's Law / 57-file consumer cascade (carried from PR 5b precedent).

## 4. Data model

### 4.1 ADR-013 fct_pausa_values promotion

**Mechanism per ADR-013 §3 normative + Q2 lock:**

#### 4.1.1 Bronze raw table
Python writer (`src/ingestion/pausa.py`) retargets:
- Old: `dev_gold.fct_pausa_values` (gold-direct write)
- New: `bronze.pausa_values` (bronze raw)

Schema unchanged from current `_RESULTS_SCHEMA` — already complies with ADR-013 §2 (only native IDs + predictions, no surrogate keys):
```
pass_id STRING, match_id STRING, player_id STRING, team STRING, period INT,
timestamp_seconds DOUBLE, frame_id INT, temporal_judgment DOUBLE,
spatial_selection DOUBLE, pausa_score DOUBLE, actual_obso DOUBLE,
peak_obso DOUBLE, optimal_obso DOUBLE, receiver_x DOUBLE, receiver_y DOUBLE,
_ingested_at TIMESTAMP
```

#### 4.1.2 Staging
`stg_pausa__values.sql` repoints source from `pausa_gold.fct_pausa_values` → `bronze.pausa_values`. Dedup logic unchanged. `_pausa__sources.yml` drops the `pausa_gold` source; adds `bronze.pausa_values` source under the existing `pausa` source name.

#### 4.1.3 New gold mart `fct_pausa_values.sql`
Mirrors PR 3's `fct_xg_predictions_v2.sql`:

```sql
{{ config(
    materialized='table',
    enabled=var('pausa_enabled', false),
    liquid_clustered_by=['match_key'],
    on_schema_change='fail',
    contract={'enforced': true}
) }}

select
    p.pass_id,
    p.match_id,
    fp.match_key,
    fp.team_key,
    fp.player_key,
    p.player_id,
    p.team,
    p.period,
    p.timestamp_seconds,
    p.frame_id,
    p.temporal_judgment,
    p.spatial_selection,
    p.pausa_score,
    p.actual_obso,
    p.peak_obso,
    p.optimal_obso,
    p.receiver_x,
    p.receiver_y
from {{ ref('stg_pausa__values') }} p
inner join {{ ref('fct_passes') }} fp on p.pass_id = fp.pass_id
```

`fp.match_key`, `fp.team_key`, `fp.player_key` available via Q2 fct_passes extension. Single INNER JOIN inherits all 3 keys.

#### 4.1.4 Workflow card update
`workflow-cards/wf-obso-pausa.yaml` `outputs.tables` adds gold entry alongside new bronze:

```yaml
outputs:
  tables:
    - id: "{catalog}.bronze.pausa_values"
      destination: delta-table
    - id: "{catalog}.dev_gold.fct_pausa_values"
      destination: delta-table
      dbt_model: fct_pausa_values
```

Per `reference_workflow_card_destination_literal`: `destination` is `Literal['delta-table']`; `dbt_model:` is the discriminator.

#### 4.1.5 Migration
1. Drop existing `dev_gold.fct_pausa_values` (writer's prior direct-write).
2. Re-run `wf-obso-pausa` workflow → populates `bronze.pausa_values` (~$14, 7 IDSSE matches per workflow card cost line).
3. `dbt run --select +fct_pausa_values --full-refresh --target dev` → builds gold from bronze via staging.
4. No backfill code; clean re-derivation.

### 4.2 fct_passes Kimball-key extension (Q2)

`fct_passes` gains `team_key` + `player_key` for both passer and recipient where the passes have a recipient. Mechanism:

- LEFT JOIN `dim_players` on `(provider, native_player_id = passer_player_id)` → `passer_player_key`.
- LEFT JOIN `dim_players` on `(provider, native_player_id = recipient_player_id)` → `recipient_player_key` (where passes have recipients; NULL otherwise).
- LEFT JOIN `dim_teams` on `(provider, native_team_id = team_id)` → `team_key`.
- Surrogate-hash inputs add `data_source` if not already present.

Provider mapping (CASE) per PR 6 §4.2 precedent: `data_source ∈ {'statsbomb', 'wyscout', 'idsse', 'metrica'}` already maps 1:1 to `dim_matches.provider`. Same pattern.

### 4.3 fct_shots / fct_action_values / fct_line_breaking_results / fct_match_summary / fct_heatmap_agg / fct_vaep_breakdown_agg key extensions

Each mart gains `team_key` + `player_key` (where applicable) via LEFT JOIN dim_teams + dim_players using the standard provider-CASE pattern. `fct_match_summary` gains `home_team_key` + `away_team_key` via two separate dim_teams joins.

`fct_xg_predictions` and `fct_xg_predictions_v2` extensions are pure pull-through additions to the existing INNER JOIN fct_shots — add `s.team_key, s.player_key` to the SELECT list. One-line per mart.

### 4.4 Tracking subsystem team_id derivation (Q1)

#### stg_idsse__tracking
Already surfaces `team_id` as the real DFL TeamId since PR 5a (line 38-42). PR 7 verifies propagation through `fct_tracking_frames`:
- `fct_tracking_frames` final SELECT must add `team_id` (currently drops to STRING `team` only).
- Resolution: LEFT JOIN `dim_teams` on `(provider='idsse', native_team_id = team_id)`.

#### stg_metrica__tracking
Currently has `team` as `'home'|'away'` STRING + `home_players`/`away_players` JSON bronze passthroughs but no `team_id`.

New derivation:
```sql
left join {{ ref('dim_teams') }} dt
    on  dt.provider = 'metrica'
   and dt.native_match_id = match_id  -- pseudo-comp encoding
   and dt.team_role = team  -- 'home'/'away' synthesized in PR 5a
```

If `dim_teams` doesn't expose `team_role`, fall back to: synthesize `team_id = match_id || '_' || team` (matches PR 5a Metrica pseudo-team convention; verify at impl time).

#### stg_skillcorner__tracking
Has `home_team_id` + `away_team_id` as bronze passthroughs.

New derivation:
```sql
case
    when team = 'home' then home_team_id
    when team = 'away' then away_team_id
end as team_id
```

#### stg_formations__labels + stg_shape_graphs__positions
Verify column shapes during implementation. Apply tracking-pattern team_id derivation matching the upstream provider source.

### 4.5 Surrogate-key updates

Every PR 7 mart gaining `data_source` to its grain updates `dbt_utils.generate_surrogate_key([...])` inputs. Per PR 6 §4.4 precedent. Effect: surrogate IDs change → mandatory `--full-refresh` post-merge for incremental marts.

| Mart | Old hash inputs | New hash inputs | Effect |
|---|---|---|---|
| `fct_off_ball_xt` | (player_id, match_id) | + data_source | All IDs change |
| `fct_space_creation` | (match_id, frame_id, player_id) | + data_source | All IDs change |
| `fct_tracking_frames` | (match_id, period, frame, player_id) at staging | + data_source at mart | All IDs change |
| `fct_tracking_avg_positions` | (match_id, period, player_id) | + data_source | All IDs change |
| `fct_tracking_shape_timeline` | (match_id, period, time_bucket, player_id) | + data_source | All IDs change |
| `fct_player_positions` | (match_id, frame_id, player_id) | + data_source | All IDs change |
| `fct_position_maps` | (player_id, match_id, position_label, 'all') | + data_source | All IDs change |
| `fct_formation_labels` | (match_id, period, team, window_start_s, detector) | + data_source | All IDs change |
| `fct_physical_stats` | (player_id, match_id) | + data_source | All IDs change |
| `fct_pass_timing` | (no surrogate today — table grain) | New surrogate `pass_timing_id` over (player_id, match_id, data_source) | All rows new IDs |
| `fct_pausa_rankings` | (no surrogate — player grain) | New surrogate `pausa_ranking_id` over (player_id) — every mart gets a stable surrogate per Kimball convention |
| `fct_discipline_events` | event_id passthrough | unchanged (event_id is unique already) | None |

### 4.6 Surrogate-table dual-defense audit

For every mart in §3.1 synced to Lakebase, audit:
- Terminal `QUALIFY ROW_NUMBER() OVER (PARTITION BY <pg_pk_grain> ORDER BY <stable_tiebreaker>) = 1` in final SELECT.
- `dbt_utils.unique_combination_of_columns` schema test in `_marts__models.yml` on the same grain.

Add the missing layer where present. Reference application: `fct_workflow_costs` (PR #203). Per `reference_synced_pg_pk_dual_defense`.

### 4.7 fct_player_percentiles bridge retirement

Current `physical_by_comp` CTE routes via `dim_matches.native_match_id`:

```sql
inner join {{ ref('dim_matches') }} dm
    on cast(ps.match_id as string) = dm.native_match_id
inner join {{ ref('fct_match_summary') }} ms
    on dm.match_key = ms.match_key
```

After PR 7 (`fct_physical_stats` gains `match_key`):

```sql
inner join {{ ref('fct_match_summary') }} ms
    on ps.match_key = ms.match_key
```

One CTE simplified; bridge retired.

## 5. Edge cases

| # | Edge case | Behavior |
|---|---|---|
| 1 | `fct_passes` row with NULL `recipient_player_id` (incomplete pass) | `recipient_player_key` resolves NULL via LEFT JOIN — preserved semantic. |
| 2 | Tracking row with NULL `player_id` (ball-only row in IDSSE/SkillCorner) | `player_key` NULL via LEFT JOIN; consumers already filter ball rows. |
| 3 | Metrica anonymized player (synthesized in PR 5a) | `dim_players` Metrica synth rows resolve correctly; verify coverage at impl. |
| 4 | Metrica anonymized team_id derivation (Q1 §4.4) | If PR 5a synthesized `team_role`-based teams in dim_teams, JOIN works; else fall back to synthesized team_id concatenation. Verify at impl time. |
| 5 | IDSSE 360-anonymous defenders (per PR 6 #4) | Not relevant to PR 7 (PR 6 already handled defender_player_key); tracking-frame player_keys are real players. |
| 6 | SkillCorner referee row (player_id present, team unknown) | `team_id` from `home_team_id`/`away_team_id` CASE — returns NULL when team is neither 'home' nor 'away'; relationships warn fires. |
| 7 | `fct_xg_predictions_v2` post-extension | Existing rows survive — new cols populated via the existing INNER JOIN to fct_shots; no row delta. |
| 8 | Surrogate-hash break orphans rows on first incremental build (most PR 7 marts) | **Mandatory** post-merge `--full-refresh`. Carries from PR 5b/6 precedent. |
| 9 | `fct_pausa_values` first build after promotion | Drop dev_gold table → re-run wf-obso-pausa → dbt builds. Single migration step. |
| 10 | Live-CI `state:modified+` cascades into mart graph that hasn't built since PR 6 | Path X authority approved (carried from PR 4b/5b/6). Surface latent bugs as in-PR fixes. |
| 11 | Hardcoded surrogate IDs in test fixtures or HF payloads | Pre-impl grep across `src/tests/`, `docs/`, HF cards. PR 5b/6 precedent: zero hits expected. |
| 12 | `fct_match_summary` home_team_key vs away_team_key | Resolved via two separate dim_teams JOINs on `home_team_id` and `away_team_id` independently. |
| 13 | `pitch_control_batch.py` schema MERGE drift on bronze.pitch_control_values widening | Additive Delta auto-evolve via `mergeSchema=true` on write; ADR-002 §4 writer/DDL parity guard test. |
| 14 | `pausa.py` writer transition window | Drop dev_gold table BEFORE re-running workflow; otherwise both paths exist temporarily. |
| 15 | HF dataset publish failure mid-batch | Sequential publishes with explicit log of HF Hub commit URLs; resumable. |
| 16 | HF Hub rate-limit on 13 dataset republishes | Sequential publishes with waits; abort-resume on 429. |
| 17 | Lakebase synced-table PG-PK change on marts gaining `data_source` to PG-PK grain | Manual delete + UI-recreate per `reference_synced_table_pk_recreation`. Audit per-mart at impl. |
| 18 | `fct_player_stats.team_key` at career grain | Already NULL placeholder per PR 5a — leave unchanged at career grain (correct semantics). Verify the actual grain in fct_player_stats.sql at impl; if grain is finer, derive at that grain. |

## 6. Testing

### 6.1 Test inventory

| Test | Type | Scope |
|---|---|---|
| `test_marts_kimball_contracts.py` (existing) | Live invariant | `_CASES` extended with PR 7 marts × applicable keys; thresholds calibrated post-rebuild measurement |
| `test_marts_kimball_completion.py` (NEW) | Live invariant | Asserts NO `fct_*` mart has both legacy `*_id` smart key AND missing the corresponding `*_key` column. Pre-PR-8 catch-all. |
| `test_pausa_adr013_compliance.py` (NEW) | Compile-time + live | Asserts `fct_pausa_values` is dbt mart with `contract: enforced: true` + INNER JOIN to fct_passes (mirrors PR 3 xG v2 pattern; verify if such a test exists for v2 first) |
| `test_bronze_live_schema.py` (existing) | Live | Adds `bronze.pausa_values` entry; updates `bronze.pitch_control_values` schema for 2 new cols |
| `test_pitch_control_writer_parity.py` (NEW or extend) | Compile-time + live | ADR-002 §4 writer/DDL parity for pitch_control_batch.py — `_PITCH_CONTROL_BRONZE_COLS` constant matches DESCRIBE on `bronze.pitch_control_values` |
| `test_pausa_writer_parity.py` (NEW) | Compile-time + live | Same pattern for pausa.py — `_RESULTS_SCHEMA` parsed and matched to bronze.pausa_values DESCRIBE |
| dbt schema tests (per mart, in `_marts__models.yml`) | Compile-time | `unique` on every renamed surrogate; `relationships severity: warn` on each new FK; `dbt_utils.unique_combination_of_columns` on every PG-PK grain |
| `test_dbt_passes_kimball_migration.py` (existing) | Compile-time + live | Extend to assert fct_passes `player_key` + `team_key` non-NULL post-migration |
| `test_marts_live_schema.py` (existing) | Live | Add PR 7 marts to live DESCRIBE assertions |
| `test_idsse_is_progressive_coverage.py` (existing PR 6) | Live | Unchanged |

### 6.2 Pyright / ruff
- Expected new Python files: 2-3 test modules.
- Test files follow PR 5b/6 pattern: `pytest.importorskip("databricks.sql")` + `requires_databricks` skip on env vars.
- `# ruff: noqa: S608` header on test files using string-formatted SQL.

### 6.3 Pre-push gates

```bash
uv run ruff check src/ scripts/ dbt_project/
uv run ruff format --check src/ scripts/
uv run pyright src/
uvx --from "dbt-core>=1.10.0,<1.12.0" --with dbt-databricks dbt parse --project-dir dbt_project
uv run pytest src/tests/ -v --ignore=src/tests/test_marts_kimball_contracts.py --ignore=src/tests/test_marts_kimball_completion.py --ignore=src/tests/test_pausa_adr013_compliance.py --ignore=src/tests/test_pausa_writer_parity.py
```

Live-invariant tests deferred to post-merge (require dev_gold env).

### 6.4 CI gates

- `validate` — dbt parse, schema YAML lint. Should be green on first push.
- `semgrep` — green; no new patterns.
- `lint-and-test` — green; pyright + ruff + pytest.
- `live-build` (PR 4a's serverless dbt run) — **expect surfacing**. Triage per PR 4b/5b/6 playbook. Path X authority approved.

### 6.5 `on_schema_change='append_new_columns'` audit

Per `feedback_dbt_incremental_on_schema_change`. Every incremental mart in §3.1 that gains a column must be audited at first push:
- `fct_tracking_frames` — already incremental
- `fct_tracking_avg_positions`
- `fct_tracking_shape_timeline`
- `fct_player_positions`
- `fct_position_maps`
- `fct_formation_labels`
- `fct_physical_stats`
- `fct_off_ball_xt`
- `fct_passes` (incremental? verify)
- `fct_action_values` (incremental? verify)
- `fct_line_breaking_results` (incremental? verify)

Mirror `fct_player_stats.sql` config (PR 5a precedent) where missing.

## 7. Ship criteria

### Pre-merge
- All four CI checks green: validate, semgrep, lint-and-test, live-build.
- No new ruff or pyright violations.
- No new Semgrep findings.
- All dbt schema tests green (unique + relationships warn + unique_combination_of_columns).
- `on_schema_change='append_new_columns'` audit passed.
- Surrogate-hash impact analysis documented (which marts need full-refresh).

### Post-merge dev deploy
- Wheel 0.3.18 → 0.3.19 deployed via Python CI on push to main.
- `wf-obso-pausa` workflow re-run → `bronze.pausa_values` populated.
- Existing `dev_gold.fct_pausa_values` dropped.
- Full graph rebuild: `dbt run --select <PR-7 marts>+ --full-refresh --target dev` complete with WARN ≤ acceptable, ERROR=0.
- All PR-7 synced tables transition to `ONLINE_NO_PENDING_UPDATE` after `refresh_synced_tables.py --wait`.
- `maintain_synced_tables.py --skip-refresh` completes Steps 0.5 (grants) + 2 (indexes) + 3 (verify) cleanly.
- `test_marts_kimball_contracts.py` parameterized over all (mart, key) pairs — all PASS at calibrated thresholds.
- `test_marts_kimball_completion.py` PASS — no smart-key islands remain.
- `test_pausa_adr013_compliance.py` PASS.
- `test_pitch_control_writer_parity.py` + `test_pausa_writer_parity.py` PASS.
- `test_marts_live_schema.py` PASS for PR-7 marts.

### HF artifact publish (Phase 2 step 9-11)
- All 13 dataset republishes succeed (8 PR-7-affected + 5 PR-5b absorbed).
- All 6 model card pushes succeed.
- HF Hub commit URLs logged for every publish.
- Smoke-test: `notebooks/publish_datasets.py` HF dataset (~38M rows) post-PR-7 still publishes correctly — JOIN on `tracking_id` unaffected.

### Documentation
- Memory entry captured (`project_kimball_pr7_shipped.md`).
- Cycle-state memory updated (`project_kimball_migration_cycle.md`) — PR 7 row marked SHIPPED, PR 8 row remains LOCKED cleanup.
- ADR-011 §Notes staged-rollout table — PR 7 row Status: Shipped (date, hash); PR 8 row unchanged.
- ADR-013 §Notes — second-application entry for `fct_pausa_values` confirmed as Shipped.
- HF dataset cards × 13 + model cards × 6 pushed via `scripts/publish_hf_cards.py`.

## 8. Risks

| # | Risk | Severity | Mitigation |
|---|---|---|---|
| R1 | Surrogate-hash break orphans rows on first incremental build (12+ marts) | High | **Mandatory** post-merge `--full-refresh`. Carries from PR 5b/6. |
| R2 | Live-CI `state:modified+` cascade surfaces latent bugs (4-6 expected, scaled from PR 4b/5b/6) | Medium | Path X authority approved. Same playbook. |
| R3 | applyInPandas schema drift in `pausa.py`/`pitch_control_batch.py` | Medium | ADR-002 §4 writer/DDL parity guard tests (new). |
| R4 | `monotonically_increasing_id().cast("int")` overflow elsewhere in PR 7 graph | Medium | Per-PR audit at impl; use `row_number().over(...)` if INT-fitting needed. PR 6 #209-#210 lesson. |
| R5 | Live-invariant thresholds calibrated against pre-fix data | Medium | Measure post-Phase-2-step-4 rebuild; lock thresholds last. PR 6 #4 lesson. |
| R6 | Hardcoded surrogate IDs in HF payloads / test fixtures | Low | Pre-impl grep. PR 5b/6 precedent: zero hits. |
| R7 | Lakebase synced-table PK change on marts gaining data_source to PG-PK grain | Medium | Manual delete + UI-recreate per `reference_synced_table_pk_recreation`. Audit per-mart at impl. |
| R8 | Pitch-control bronze schema MERGE drift | Medium | ADR-002 §4 writer/DDL parity guard. |
| R9 | Metrica anonymized team_id resolution depends on PR 5a synthesized dim_teams structure | Medium | Verify dim_teams provider='metrica' shape pre-impl. Fallback derivation if structure differs. |
| R10 | fct_passes player_key/team_key resolution variance per provider | Medium | Per-provider dim coverage measurement before threshold lock. |
| R11 | fct_shots → fct_xg_predictions_v2 contract change | Low | One-line additive pull-through; covered by extension marts in same PR. |
| R12 | HF publish script SELECT update needed to surface new keys | Medium | Read each publish script at impl; update SELECTs; verify post-publish via HF Hub commit URL. |
| R13 | HF Hub rate-limit on 13 dataset republishes | Low | Sequential publishes with waits; resume on 429. |
| R14 | PR 5b/6 deferred publishes absorption — 5 football2vec datasets + pitch-control parquet | Medium | Each is a single script invocation; verify SELECT update needed per dataset. |
| R15 | `fct_pausa_values` ADR-013 promotion vs existing pausa_gold source breakage | Low | Drop dev_gold table BEFORE workflow re-run; staging YAML source repointing in same commit. |
| R16 | dbt incremental marts without `on_schema_change='append_new_columns'` fail live-CI build on first push | Low | §6.5 audit. |
| R17 | Test fixture coverage gap on new staging team_id derivation | Medium | New unit tests on Metrica/SkillCorner team_id derivation paths. |

## 9. Rollout

### Phase 0 — Branch + first push (single commit per `feedback_single_commit_squash`)

1. `git checkout main && git pull` → confirm clean from current `f81d2b8` or later.
2. `git checkout -b kimball-pr7-tracking-formations-pausa`.
3. Implementation: SQL changes, YAML contract updates, test additions, IDSSE/Metrica/SkillCorner staging, pausa.py + pitch_control_batch.py writer changes, workflow card updates, HF model card edits, ADR/memory updates.
4. Pre-push gate (§6.3).
5. `on_schema_change='append_new_columns'` audit (§6.5).
6. Surrogate-hash impact analysis (§4.5) committed in spec or PR description.
7. **Single commit per branch.** First push, open PR.

### Phase 1 — CI green + merge approval

1. CI gates: validate / semgrep / lint-and-test / live-build all green. Triage live-CI cascade per Path X authority.
2. Pre-existing CI blockers folded into 2nd commit on same branch — squash-merge collapses to one commit on `main`.
3. **PAUSE for explicit user approval** before `gh pr merge`.

### Phase 2 — Post-merge dev deploy (autonomous per `feedback_only_git_gates_need_approval`)

1. Wheel 0.3.18 → 0.3.19 deploy via Python CI on push to main.
2. Trigger `wf-obso-pausa` to populate `bronze.pausa_values` (~$14, 7 IDSSE matches). The wheel-shipped writer now targets `bronze.pausa_values`; first run creates the bronze table via `ensure_table` SkipGuard call.
3. `dbt run --select <PR-7 marts>+ --full-refresh --target dev`. `materialized='table'` on `fct_pausa_values` does CREATE OR REPLACE — old Python-written rows in `dev_gold.fct_pausa_values` are replaced by dbt-built rows from bronze via staging. No manual drop needed.
4. **Lakebase synced-table PK recreation** for `fct_pausa_values_synced` (PG-PK grain changes from string-keyed to BIGINT-keyed `(match_key, pass_id)`): manual delete from Lakebase UI, then `terraform apply` to recreate from updated TF resource. Per `reference_synced_table_pk_recreation`. Audit other PR-7 marts gaining `data_source` to PG-PK grain — apply same pattern per-mart.
5. `uv run python scripts/refresh_synced_tables.py --tables <changed synced> --wait` (parallel-poll via PR #204) — for marts NOT requiring PK recreation (additive auto-evolve).
6. `uv run python scripts/maintain_synced_tables.py --skip-refresh` (Steps 0.5 + 2 + 3) — re-applies grants and indexes including for newly-recreated synced tables from step 4.
7. Live-invariant tests:
   ```
   uv run --with databricks-sql-connector pytest \
     src/tests/test_marts_kimball_contracts.py \
     src/tests/test_marts_kimball_completion.py \
     src/tests/test_pausa_adr013_compliance.py \
     src/tests/test_pausa_writer_parity.py \
     src/tests/test_pitch_control_writer_parity.py \
     src/tests/test_marts_live_schema.py -v
   ```
8. Smoke-test HF publish chain integrity (`notebooks/publish_datasets.py` non-destructive run).
9. **HF dataset payload re-publishes (8 PR-7-affected):**
   - `notebooks/publish_obso_data.py` → `obso-pausa-values`
   - `notebooks/publish_datasets.py` → `pitch-control-tracking`, `space-creation-values`, `line-breaking-passes` (verify scope)
   - `scripts/publish_spadl_vaep_hf.py` → `spadl-vaep-action-values`
   - `scripts/publish_xg_shots_hf.py` → `xg-shot-data`, `statsbomb-shots-on-target`
   - `scripts/publish_freeze_frame_hf.py` → `xg-freeze-frame-data`
10. **HF dataset payload re-publishes (5 PR-5b absorbed):**
   - `football2vec-player-embeddings`
   - `football2vec-360-embeddings`
   - `football2vec-training-data`
   - `football2vec-360-training-data`
   - `football2vec-statsbomb-wyscout`
   (Publish scripts to be verified at impl time per `feedback_hf_artifacts_in_scope_pr`.)
11. **HF model card pushes** — `scripts/publish_hf_cards.py --kind model --name <card>.md` × 6.
12. **HF dataset card pushes** — `scripts/publish_hf_cards.py --kind dataset --name <card>.md` × 13.
13. Smoke check: query `fct_passes` and confirm `team_key` + `player_key` non-NULL counts; query `fct_pausa_values` and confirm 3-key INNER JOIN inheritance.

### Phase 3 — Documentation + memory

1. Update `project_kimball_migration_cycle.md` — PR 7 row marked SHIPPED with commit hash + date.
2. Write `project_kimball_pr7_shipped.md` (mirrors PR 6 memory shape: delivered scope, key coverage numbers, follow-up list, don't-re-run list).
3. Update ADR-011 staged-rollout table — PR 7 row Status changes "Planned" → "Shipped (YYYY-MM-DD, hash)".
4. Update ADR-013 §Notes — second-application entry for `fct_pausa_values` confirmed.
5. Update `MEMORY.md` index entry for PR 7.

### Phase 4 — Branch cleanup

Per `feedback_only_git_gates_need_approval`, branch deletion is user-controlled. Pause for approval after Phase 3 before `git branch -d kimball-pr7-tracking-formations-pausa`.

## 10. Open implementation-time verifications

Items resolved at implementation time, not at design time:

1. `dim_teams` Metrica pseudo-comp structure from PR 5a — does it expose `team_role` ('home'/'away'), or do we need synthesized `team_id` concatenation? Sample `SELECT * FROM dim_teams WHERE provider = 'metrica' LIMIT 20`.
2. `stg_formations__labels` source schema — what's the team column type? Verify upstream bronze provides `team_id`.
3. `stg_shape_graphs__positions` source schema — same.
4. `fct_player_stats.team_key` grain — currently NULL placeholder; verify the actual grain. If career-aggregate, leave NULL (correct). If finer, derive.
5. `fct_passes`, `fct_action_values`, `fct_line_breaking_results` materialization (incremental vs table) — confirm `on_schema_change` config.
6. `fct_action_values.action_value_id` surrogate construction — does it already encode `data_source`? Critical for downstream consumer stability.
7. Hyrum's Law grep results for hardcoded surrogate IDs across `src/tests/`, `docs/`, HF cards.
8. Eight tracking marts — narrow which surrogate-hash changes vs. which stay stable.
9. Per-(mart, key) coverage measurement at first dev rebuild → committed thresholds in `test_marts_kimball_contracts.py`.
10. SkillCorner `home_team_id` / `away_team_id` actual presence per row — verify staging passthrough is reliable.
11. `notebooks/publish_datasets.py` complete dataset list — which datasets does it publish vs `notebooks/publish_obso_data.py` vs `scripts/publish_*.py`?
12. PR 5b deferred payload publish scripts — read PR 5b plan task 12-16 for the exact scripts; verify they're in repo.
13. PR 6 pitch-control HF parquet payload deferral — confirm what was deferred (column publish vs full re-publish) and the exact script path.
14. `terraform/modules/synced_tables/main.tf` — pre-impl audit of which marts have synced-table resources, which need new ones, which need PK updates.
15. `SYNCED_TABLES` registry in `src/ingestion/refresh_synced_tables.py` — register `fct_pausa_values_synced`.
16. `scripts/create_indexes.py` — index set for `fct_pausa_values_synced`.

## 11. Related references

- **ADRs:** `docs/superpowers/adrs/ADR-011-unified-kimball-match-dimension.md`, `ADR-013-ml-inference-outputs-dbt-mart.md`, `ADR-014-hf-card-inventory-parity.md`, `ADR-002-silent-exception-swallow-elimination.md`, `ADR-005-lakebase-synced-table-grants.md`, `ADR-012-training-to-production-delivery-hardening.md`.
- **Predecessor specs:** `docs/superpowers/specs/2026-04-26-kimball-pr6-design.md`, `2026-04-24-kimball-pr5-design.md`, `2026-04-23-kimball-pr4-action-values-plus-deferrals-design.md`, `2026-04-22-kimball-pr3-shots-xg-design.md`.
- **Predecessor plans:** `docs/superpowers/plans/2026-04-26-kimball-pr6-defensive-gk-pitch-control.md`, `2026-04-25-kimball-pr5b-embedding-marts.md`.
- **Memory anchors:**
  - `project_kimball_pr6_shipped.md`
  - `project_kimball_pr5b_shipped.md`
  - `project_kimball_pr5a_shipped.md`
  - `project_kimball_migration_cycle.md`
  - `project_kimball_pr8_scope_locked.md`
  - `project_kimball_endgame_bronze_staging_stability.md`
  - `feedback_dbt_incremental_on_schema_change.md`
  - `reference_synced_pg_pk_dual_defense.md`
  - `reference_synced_table_pk_recreation.md`
  - `reference_live_ci_surfaces_latent_bugs.md`
  - `feedback_only_git_gates_need_approval.md`
  - `feedback_no_unapproved_deferrals.md`
  - `feedback_no_pr_decomposition_proposals.md`
  - `feedback_lead_with_best_practice.md`
  - `feedback_hf_artifacts_in_scope_pr.md`
  - `feedback_no_approval_asks_in_plan_execution.md`
  - `feedback_agent_tool_requires_per_call_approval.md`
  - `feedback_single_commit_squash.md`
  - `feedback_evidence_before_claim.md`
  - `reference_workflow_card_destination_literal.md`
  - `reference_hf_publish_pattern.md`
  - `reference_adr_012_vs_013.md`
- **Reference applications for patterns:**
  - `dbt_project/models/marts/fct_player_stats.sql` (PR 5a — INNER JOIN dim_players + on_schema_change config)
  - `dbt_project/models/marts/fct_workflow_costs.sql` (PR #203 — QUALIFY tiebreaker + unique_combination_of_columns dual-defense)
  - `dbt_project/models/marts/fct_xg_predictions_v2.sql` (PR 3 — first ADR-013 application; INNER JOIN identity fact pattern)
  - `src/tests/test_defcon_schema_parity.py` (PR 6 — ADR-002 §4 writer/DDL parity guard)
  - `dbt_project/models/marts/fct_funnel_stages_agg.sql` (PR 5a — match_key + team_key + opponent_team_key resolution via dim_teams provider-CASE)
- **Workflow cards:**
  - `workflow-cards/wf-obso-pausa.yaml` — PR 7 outputs.tables update target
  - `workflow-cards/wf-pitch-control.yaml` — bronze schema widening downstream
- **Writers:**
  - `src/ingestion/pausa.py` — ADR-013 retarget
  - `src/ingestion/pitch_control_batch.py` — schema widening
- **HF publish scripts (impl-time verification):**
  - `notebooks/publish_datasets.py`
  - `notebooks/publish_obso_data.py`
  - `scripts/publish_spadl_vaep_hf.py`
  - `scripts/publish_xg_shots_hf.py`
  - `scripts/publish_freeze_frame_hf.py`
  - `scripts/publish_hf_cards.py`
