# Kimball PR 7 — Tracking + Formations + Pausa + Tail Facts + Conformed-Fact Closure + ADR-013 Promotion Implementation Plan

> **For agentic workers:** This plan is executed inline (per `feedback_no_approval_asks_in_plan_execution` + `feedback_agent_tool_requires_per_call_approval` + `feedback_no_reviewer_subagents_in_execution`) — no subagent dispatch. Use checkbox (`- [ ]`) syntax for tracking. **Single commit at end of plan** per `feedback_single_commit_squash` (do NOT commit per task). Only git gates (commit, push, PR create, merge, branch delete) pause for explicit user approval per `feedback_only_git_gates_need_approval`.

**Goal:** Close the ADR-011 staged Kimball migration in one PR — every fact mart in the warehouse reaches Kimball completeness; `fct_pausa_values` promoted to dbt mart per ADR-013; pitch-control writer collapses staging bridge; all affected HF artifacts (cards + payloads) ship in scope including absorbed PR 5b/6 deferrals.

**Architecture:** Additive contract changes with full-refresh on incremental marts. Surrogate-hash inputs gain `data_source` where applicable. Tracking marts resolve `team_key` via staging-level `team_id` derivation per Q1. `fct_pausa_values` follows ADR-013 normative §3 (Python writer → bronze raw → dbt staging → gold mart with contract; INNER JOIN identity fact `fct_passes`). Pre-existing PR 5b/6 deferred HF publishes absorbed per `project_kimball_pr8_scope_locked`.

**Tech Stack:** dbt 1.10–1.12 / Databricks Spark SQL, pyright, ruff, pytest, Python 3.10, HuggingFace Hub, Lakebase (PostgreSQL), Terraform, Databricks SDK.

**Spec source:** `docs/superpowers/specs/2026-04-27-kimball-pr7-tracking-formations-pausa-design.md`.

---

## File map

**Modify (dbt staging — Q1 team_id derivation):**
- `dbt_project/models/staging/idsse/stg_idsse__tracking.sql` — verify team_id propagation (already in column list per PR 5a)
- `dbt_project/models/staging/metrica/stg_metrica__tracking.sql` — derive team_id via dim_teams JOIN
- `dbt_project/models/staging/skillcorner/stg_skillcorner__tracking.sql` — derive team_id via home/away CASE
- `dbt_project/models/staging/formations/_formations__sources.yml` (verify path) — add team_id surfacing
- `dbt_project/models/staging/formations/stg_formations__labels.sql` — add team_id derivation
- `dbt_project/models/staging/shape_graphs/stg_shape_graphs__positions.sql` (verify path) — add team_id derivation

**Modify (dbt staging — pausa ADR-013 promotion):**
- `dbt_project/models/staging/pausa/stg_pausa__values.sql` — repoint source from `pausa_gold` → bronze `pausa.pausa_values`
- `dbt_project/models/staging/pausa/_pausa__sources.yml` — drop `pausa_gold` source; add `pausa_values` table under `pausa` source

**Modify (dbt marts — tracking subsystem):**
- `dbt_project/models/marts/fct_tracking_frames.sql` — add match_key + team_key + player_key + propagate team_id from staging
- `dbt_project/models/marts/fct_tracking_avg_positions.sql` — add match_key + team_key + player_key
- `dbt_project/models/marts/fct_tracking_shape_timeline.sql` — same
- `dbt_project/models/marts/fct_player_positions.sql` — same
- `dbt_project/models/marts/fct_position_maps.sql` — same
- `dbt_project/models/marts/fct_formation_labels.sql` — add match_key + team_key (no player)
- `dbt_project/models/marts/fct_physical_stats.sql` — add match_key + player_key

**Modify (dbt marts — pausa subsystem):**
- `dbt_project/models/marts/fct_pausa_rankings.sql` — add player_key + new `pausa_ranking_id` surrogate
- `dbt_project/models/marts/fct_pass_timing.sql` — add match_key + player_key

**Modify (dbt marts — off-ball / space):**
- `dbt_project/models/marts/fct_off_ball_xt.sql` — add match_key + player_key
- `dbt_project/models/marts/fct_space_creation.sql` — add match_key + player_key

**Modify (dbt marts — tail facts):**
- `dbt_project/models/marts/fct_discipline_events.sql` — add match_key + team_key + player_key

**Modify (dbt marts — Q3 conformed-fact closures):**
- `dbt_project/models/marts/fct_passes.sql` — add passer_team_key + passer_player_key + recipient_player_key
- `dbt_project/models/marts/fct_shots.sql` — add team_key + player_key
- `dbt_project/models/marts/fct_action_values.sql` — add team_key + player_key
- `dbt_project/models/marts/fct_match_summary.sql` — add home_team_key + away_team_key
- `dbt_project/models/marts/fct_line_breaking_results.sql` — add team_key + player_key

**Modify (dbt marts — aggregates):**
- `dbt_project/models/marts/fct_heatmap_agg.sql` — add team_key
- `dbt_project/models/marts/fct_vaep_breakdown_agg.sql` — add team_key + player_key

**Modify (dbt marts — pull-through extensions):**
- `dbt_project/models/marts/fct_xg_predictions.sql` — add team_key + player_key (via fct_shots)
- `dbt_project/models/marts/fct_xg_predictions_v2.sql` — add team_key + player_key (via fct_shots)

**Modify (dbt marts — bridge retire):**
- `dbt_project/models/marts/fct_player_percentiles.sql` — retire `physical_by_comp` dim_matches bridge

**Create (dbt mart — ADR-013 promotion):**
- `dbt_project/models/marts/fct_pausa_values.sql` — NEW mart, contract: enforced: true, INNER JOIN fct_passes ON pass_id

**Modify (dbt YAML contracts):**
- `dbt_project/models/marts/_marts__models.yml` — add column entries + relationships warn for ALL new key columns across all marts above

**Modify (writers):**
- `src/ingestion/pausa.py` — retarget bronze table from `dev_gold.fct_pausa_values` → `bronze.pausa_values`
- `src/ingestion/pitch_control_batch.py` — widen `_RESULTS_SCHEMA` + `_PITCH_CONTROL_BRONZE_COLS` for data_source + match_key

**Modify (workflow cards):**
- `workflow-cards/wf-obso-pausa.yaml` — outputs.tables: add gold entry with `dbt_model: fct_pausa_values` alongside new bronze entry

**Modify (Terraform + registry + indexes):**
- `terraform/modules/synced_tables/main.tf` — verify/update `fct_pausa_values` synced-table resource for new BIGINT PG-PK grain
- `src/ingestion/refresh_synced_tables.py` — verify `SYNCED_TABLES` registry entry for `fct_pausa_values_synced`
- `scripts/create_indexes.py` — index set for `fct_pausa_values_synced`

**Create (tests):**
- `src/tests/test_marts_kimball_completion.py` — NEW invariant
- `src/tests/test_pausa_adr013_compliance.py` — NEW
- `src/tests/test_pausa_writer_parity.py` — NEW (ADR-002 §4 pattern)
- `src/tests/test_pitch_control_writer_parity.py` — NEW (ADR-002 §4 pattern)

**Modify (tests):**
- `src/tests/test_marts_kimball_contracts.py` — extend `_CASES` with PR 7 marts
- `src/tests/test_bronze_live_schema.py` — add `bronze.pausa_values` entry; update `bronze.pitch_control_values` schema
- `src/tests/test_marts_live_schema.py` — add PR 7 marts
- `src/tests/test_dbt_passes_kimball_migration.py` — extend with team_key + player_key non-NULL assertions

**Modify (HF model cards — text-only updates):**
- `docs/huggingface/model-cards/off-ball-xt.md`
- `docs/huggingface/model-cards/space-creation.md`
- `docs/huggingface/model-cards/pitch-control.md`
- `docs/huggingface/model-cards/obso-pausa.md`
- `docs/huggingface/model-cards/xg.md`
- `docs/huggingface/model-cards/defcon.md` (verify if any update needed)

**Modify (HF dataset cards — PR-7-affected payloads + dual-column window stanza):**
- `docs/huggingface/dataset-cards/obso-pausa-values.md`
- `docs/huggingface/dataset-cards/pitch-control-tracking.md`
- `docs/huggingface/dataset-cards/space-creation-values.md`
- `docs/huggingface/dataset-cards/line-breaking-passes.md`
- `docs/huggingface/dataset-cards/spadl-vaep-action-values.md`
- `docs/huggingface/dataset-cards/statsbomb-shots-on-target.md`
- `docs/huggingface/dataset-cards/xg-shot-data.md`
- `docs/huggingface/dataset-cards/xg-freeze-frame-data.md`

**Modify (HF dataset cards — PR-5b absorbed deferrals):**
- `docs/huggingface/dataset-cards/football2vec-player-embeddings.md`
- `docs/huggingface/dataset-cards/football2vec-360-embeddings.md`
- `docs/huggingface/dataset-cards/football2vec-training-data.md`
- `docs/huggingface/dataset-cards/football2vec-360-training-data.md`
- `docs/huggingface/dataset-cards/football2vec-statsbomb-wyscout.md`

**Modify (ADRs):**
- `docs/superpowers/adrs/ADR-011-unified-kimball-match-dimension.md` — staged-rollout table: PR 7 row Status → Shipped
- `docs/superpowers/adrs/ADR-013-ml-inference-outputs-dbt-mart.md` — §Notes: confirm second-application

**Modify (memory after deploy):**
- `C:\Users\Karsten\.claude\projects\D--Development-karstenskyt--luxury-lakehouse-d32\memory\project_kimball_migration_cycle.md`
- `C:\Users\Karsten\.claude\projects\D--Development-karstenskyt--luxury-lakehouse-d32\memory\MEMORY.md` (index entry)

**Create (memory after deploy):**
- `C:\Users\Karsten\.claude\projects\D--Development-karstenskyt--luxury-lakehouse-d32\memory\project_kimball_pr7_shipped.md`

---

## Phase 0 — Implementation (pre-push)

### Task 0: Branch + pre-flight verifications

- [ ] **Step 0.1: Branch from main**

```bash
git checkout main
git pull
git checkout -b kimball-pr7-tracking-formations-pausa
```

- [ ] **Step 0.2: Verify `dim_teams` Metrica synthesized rows from PR 5a**

Run via existing Databricks SQL connection (env vars set):

```bash
uv run python -c "
import os
from databricks import sql
host = os.environ['DATABRICKS_HOST'].replace('https://','').rstrip('/')
with sql.connect(server_hostname=host, http_path=os.environ['DATABRICKS_HTTP_PATH'], access_token=os.environ['DATABRICKS_TOKEN']) as c:
    cur = c.cursor()
    cur.execute('SELECT * FROM soccer_analytics.dev_gold.dim_teams WHERE provider = \\'metrica\\' LIMIT 20')
    for r in cur.fetchall(): print(r)
"
```

Expected: Metrica rows with `provider='metrica'`, some form of native_team_id encoding (either `match_id_home` / `match_id_away` synthesized OR a `team_role` column). Lock the derivation pattern per spec §4.4 based on actual structure.

- [ ] **Step 0.3: Sample tracking-mart team strings to confirm derivation paths**

```bash
uv run python -c "
import os
from databricks import sql
host = os.environ['DATABRICKS_HOST'].replace('https://','').rstrip('/')
with sql.connect(server_hostname=host, http_path=os.environ['DATABRICKS_HTTP_PATH'], access_token=os.environ['DATABRICKS_TOKEN']) as c:
    cur = c.cursor()
    for src in ['stg_metrica__tracking','stg_skillcorner__tracking','stg_idsse__tracking']:
        cur.execute(f'SELECT DISTINCT source_provider, team FROM soccer_analytics.dev_gold.{src} LIMIT 20')
        print(src, cur.fetchall())
"
```

Expected output documents the team-string shapes per provider. Used to lock derivation CASEs in Tasks 1.B*.

- [ ] **Step 0.4: Read `stg_formations__labels.sql` and `stg_shape_graphs__positions.sql` to verify team column shape**

```bash
ls dbt_project/models/staging/formations/
ls dbt_project/models/staging/shape_graphs/
```

Read each `*team*` column definition. Lock derivation pattern based on findings (§10 #2 + #3 of spec).

- [ ] **Step 0.5: Hyrum's-Law surrogate-ID grep across tests + HF cards**

```bash
uv run grep -rn "off_ball_xt_id\|space_creation_id\|tracking_id\|shape_timeline_id\|position_id\|position_map_id\|formation_label_id\|physical_stats_id\|pausa_ranking_id\|pass_timing_id" docs/huggingface/ src/tests/fixtures/ || echo "no hits — proceed"
```

Expected: zero hits in HF cards / test fixtures (PR 5b/6 precedent). If hits exist, surface to user — do NOT proceed silently.

- [ ] **Step 0.6: `terraform/modules/synced_tables/main.tf` audit for fct_pausa_values resource**

```bash
grep -n "fct_pausa_values" terraform/modules/synced_tables/main.tf
```

Expected: existing entry. Note current PG-PK grain — used in Task M1 to update if needed.

---

### Task 1: Staging-level team_id derivation (Q1)

#### Task 1.A: stg_idsse__tracking — verify team_id propagation (already exists)

**Files:**
- Verify: `dbt_project/models/staging/idsse/stg_idsse__tracking.sql` (line 38-42 — `team_id` per PR 5a)

- [ ] **Step 1.A.1: Confirm team_id is in the SELECT**

Read file; confirm `team_id` column is selected. Already verified during brainstorming. No-op.

#### Task 1.B: stg_metrica__tracking — derive team_id

**Files:**
- Modify: `dbt_project/models/staging/metrica/stg_metrica__tracking.sql`

- [ ] **Step 1.B.1: Add team_id derivation in `normalized` CTE**

Per Step 0.2 findings, choose ONE of two paths:

**Path A — if dim_teams has `team_role` column:**

```sql
left join {{ ref('dim_teams') }} dt
    on  dt.provider = 'metrica'
   and dt.native_match_id = match_id
   and dt.team_role = team
```

Add `dt.native_team_id as team_id,` to the SELECT after the `team` column.

**Path B — if dim_teams uses synthesized `match_id_home`/`match_id_away` encoding:**

```sql
case
    when team = 'home' then concat(match_id, '_home')
    when team = 'away' then concat(match_id, '_away')
end as team_id
```

Add to SELECT after `team`. Document the derivation choice in a comment matching the dim_teams synthesis pattern.

- [ ] **Step 1.B.2: Verify dbt parse**

```bash
uvx --from "dbt-core>=1.10.0,<1.12.0" --with dbt-databricks dbt parse --project-dir dbt_project --profiles-dir dbt_project
```

Expected: parses with no errors.

#### Task 1.C: stg_skillcorner__tracking — derive team_id

**Files:**
- Modify: `dbt_project/models/staging/skillcorner/stg_skillcorner__tracking.sql`

- [ ] **Step 1.C.1: Add team_id derivation**

Insert after `team` column in the `normalized` CTE SELECT:

```sql
        case
            when team = 'home' then home_team_id
            when team = 'away' then away_team_id
        end                                             as team_id,
```

`home_team_id` and `away_team_id` are existing bronze passthroughs (lines 53-54).

#### Task 1.D: stg_idsse__tracking — surface team_id to fct_tracking_frames

Already in staging. Carries forward in Task 2.A.

#### Task 1.E: stg_formations__labels — derive team_id

**Files:**
- Modify: `dbt_project/models/staging/formations/stg_formations__labels.sql` (path verified in Step 0.4)

- [ ] **Step 1.E.1: Add team_id derivation matching tracking pattern**

Implementation depends on Step 0.4 findings. If staging UNIONs from per-provider tracking sources, derive team_id same as the tracking staging. Reference the upstream `stg_idsse__tracking.team_id` / `stg_metrica__tracking.team_id` / `stg_skillcorner__tracking.team_id` columns directly.

#### Task 1.F: stg_shape_graphs__positions — derive team_id

**Files:**
- Modify: `dbt_project/models/staging/shape_graphs/stg_shape_graphs__positions.sql` (path verified in Step 0.4)

- [ ] **Step 1.F.1: Add team_id derivation matching tracking pattern**

Same approach as 1.E. Reference upstream tracking staging team_id columns.

---

### Task 2: Tracking subsystem mart migrations

#### Task 2.A: fct_tracking_frames — match_key + team_key + player_key + team_id propagation

**Files:**
- Modify: `dbt_project/models/marts/fct_tracking_frames.sql`

- [ ] **Step 2.A.1: Add team_id to the per-source SELECTs**

In each of the three UNION ALL branches (Metrica, IDSSE, SkillCorner), add `team_id` to the projected column list:

```sql
        tracking_id, match_id, period, frame, timestamp_seconds,
        frame_rate, player_id, team, team_id, source_provider, is_goalkeeper,
        x, y, ball_x, ball_y
```

- [ ] **Step 2.A.2: Update the comment block above the SELECT to mention team_id**

Replace the "14 shared columns" comment with "15 shared columns (post-PR-7: team_id surfaced for team_key resolution per ADR-011)".

- [ ] **Step 2.A.3: Add LEFT JOINs to dim_matches + dim_teams + dim_players in `final` CTE**

```sql
final as (

    select
        tracking_id,
        match_id,
        dm.match_key,
        period,
        frame,
        timestamp_seconds,
        player_id,
        dp.player_key,
        team,
        team_id,
        dt.team_key,
        source_provider,
        is_goalkeeper,
        frame_rate,
        x,
        y,
        ball_x,
        ball_y,
        sqrt(power(x - ball_x, 2) + power(y - ball_y, 2))   as distance_to_ball,
        velocity_x,
        velocity_y,
        speed,
        velocity_x_ms,
        velocity_y_ms,
        speed_ms,
        (speed_ms - prev_speed_ms) * frame_rate              as acceleration_ms2,
        cast(null as double)                                 as pitch_control_value,
        cast(null as double)                                 as voronoi_area

    from with_lag_2
    left join {{ ref('dim_matches') }} dm
        on  dm.provider = source_provider
       and dm.native_match_id = cast(match_id as string)
    left join {{ ref('dim_teams') }} dt
        on  dt.provider = source_provider
       and dt.native_team_id = cast(team_id as string)
    left join {{ ref('dim_players') }} dp
        on  dp.provider = source_provider
       and dp.native_player_id = cast(player_id as string)

)
```

- [ ] **Step 2.A.4: Update surrogate-key inputs (if mart has its own — fct_tracking_frames inherits tracking_id from staging, so no surrogate change at mart layer)**

Verify: tracking_id passthrough from staging stays unchanged. Staging's own surrogate doesn't need data_source because each provider's staging emits a different `source_provider`.

#### Task 2.B: fct_tracking_avg_positions — match_key + team_key + player_key

**Files:**
- Modify: `dbt_project/models/marts/fct_tracking_avg_positions.sql`

- [ ] **Step 2.B.1: Add team_id propagation in `tracking` CTE**

Add `team_id` to the SELECT in the `tracking` CTE (line 32-46):

```sql
tracking as (

    select
        match_id,
        match_key,
        period,
        player_id,
        player_key,
        team,
        team_id,
        team_key,
        source_provider,
        x,
        y,
        speed_ms,
        frame,
        frame_rate
    from {{ ref('fct_tracking_frames') }}
    ...
```

- [ ] **Step 2.B.2: Add new keys to `final` SELECT**

```sql
final as (

    select
        {{ dbt_utils.generate_surrogate_key(['match_id', 'period', 'player_id', 'source_provider']) }} as avg_position_id,
        match_id,
        match_key,
        period,
        player_id,
        player_key,
        team,
        team_id,
        team_key,
        source_provider                                     as data_source,
        avg(x)                                              as avg_x,
        avg(y)                                              as avg_y,
        avg(speed_ms)                                       as avg_speed,
        count(*)                                            as frame_count,
        min(frame)                                          as min_frame,
        max(frame)                                          as max_frame,
        max(frame_rate)                                     as frame_rate
    from tracking
    group by match_id, match_key, period, player_id, player_key, team, team_id, team_key, source_provider

)
```

Note `data_source` aliased from `source_provider` for consistency with other marts.

- [ ] **Step 2.B.3: Add `on_schema_change='append_new_columns'` to config**

```sql
{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='avg_position_id',
    on_schema_change='append_new_columns',
    liquid_clustered_by=['match_key']
) }}
```

Note `liquid_clustered_by=['match_key']` change from `['match_id']`.

#### Task 2.C: fct_tracking_shape_timeline — match_key + team_key + player_key

**Files:**
- Modify: `dbt_project/models/marts/fct_tracking_shape_timeline.sql`

- [ ] **Step 2.C.1: Add team_id + key propagations**

Apply pattern from Task 2.B (key propagations + group-by extension + on_schema_change update). Specifics:

```sql
{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='shape_timeline_id',
    on_schema_change='append_new_columns',
    liquid_clustered_by=['match_key']
) }}
```

`tracking` CTE adds `match_key, player_key, team_id, team_key, source_provider`. `final` SELECT preserves them with `data_source` alias. Surrogate `shape_timeline_id` unchanged (already encodes match_id + period + time_bucket + player_id).

#### Task 2.D: fct_player_positions — match_key + team_key + player_key

**Files:**
- Modify: `dbt_project/models/marts/fct_player_positions.sql`

- [ ] **Step 2.D.1: Add LEFT JOINs to dim_matches + dim_teams + dim_players in `final` CTE**

Add `match_key`, `team_key`, `player_key`, `team_id`, `data_source` propagations. `data_source` derived from `pp.detector` upstream OR from a new staging passthrough — verify upstream `stg_shape_graphs__positions` provides source_provider. If not, default to upstream provider (likely 'idsse' or 'metrica' based on actual shape-graphs source).

```sql
final as (

    select
        {{ dbt_utils.generate_surrogate_key([
            'pp.match_id',
            'pp.frame_id',
            'pp.player_id',
            'coalesce(pp.source_provider, \\'unknown\\')'
        ]) }}                                       as position_id,

        pp.match_id,
        dm.match_key,
        pp.frame_id,
        pp.player_id,
        dp.player_key,
        coalesce(tm.player_display_name, pp.player_id)
                                                    as player_display_name,
        pp.team,
        pp.team_id,
        dt.team_key,
        coalesce(tm.team_display_name, initcap(pp.team))
                                                    as team_display_name,
        pp.position_label,
        pp.vertical_level,
        pp.horizontal_level,
        pp.detector,
        pp.source_provider                          as data_source,
        pp._ingested_at

    from player_positions as pp
    left join tracking_meta as tm
        on  pp.match_id = tm.match_id
       and pp.player_id = tm.player_id
    left join {{ ref('dim_matches') }} dm
        on  dm.provider = pp.source_provider
       and dm.native_match_id = cast(pp.match_id as string)
    left join {{ ref('dim_teams') }} dt
        on  dt.provider = pp.source_provider
       and dt.native_team_id = cast(pp.team_id as string)
    left join {{ ref('dim_players') }} dp
        on  dp.provider = pp.source_provider
       and dp.native_player_id = cast(pp.player_id as string)

)
```

- [ ] **Step 2.D.2: Update config**

```sql
{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='position_id',
    on_schema_change='append_new_columns',
    liquid_clustered_by=['match_key']
) }}
```

#### Task 2.E: fct_position_maps — match_key + team_key + player_key

**Files:**
- Modify: `dbt_project/models/marts/fct_position_maps.sql`

- [ ] **Step 2.E.1: Propagate keys from upstream fct_player_positions**

Add `match_key, player_key, team_id, team_key, data_source` to `player_positions` CTE SELECT, then propagate through `frame_counts`, `total_frames`, and `final` CTEs. Update GROUP BYs accordingly.

- [ ] **Step 2.E.2: Update surrogate-hash inputs to include data_source**

```sql
{{ dbt_utils.generate_surrogate_key([
    'fc.player_id',
    'fc.match_id',
    'fc.position_label',
    'fc.data_source',
    "'all'"
]) }}                                       as position_map_id,
```

- [ ] **Step 2.E.3: Update config to `on_schema_change='append_new_columns'` + liquid_clustered_by=['match_key']**

#### Task 2.F: fct_formation_labels — match_key + team_key

**Files:**
- Modify: `dbt_project/models/marts/fct_formation_labels.sql`

- [ ] **Step 2.F.1: Add LEFT JOINs in `final` CTE**

```sql
final as (

    select
        {{ dbt_utils.generate_surrogate_key([
            'formation_labels.match_id',
            'formation_labels.period',
            'formation_labels.team',
            'formation_labels.window_start_s',
            'formation_labels.detector',
            'coalesce(formation_labels.source_provider, \\'unknown\\')'
        ]) }}                                       as formation_label_id,

        formation_labels.match_id,
        dm.match_key,
        formation_labels.period,
        formation_labels.team,
        formation_labels.team_id,
        dt.team_key,
        formation_labels.window_start_s,
        formation_labels.window_end_s,
        formation_labels.formation_label,
        formation_labels.cost,
        formation_labels.detector,
        formation_labels.source_provider             as data_source,
        formation_labels._ingested_at

    from formation_labels
    left join {{ ref('dim_matches') }} dm
        on  dm.provider = formation_labels.source_provider
       and dm.native_match_id = cast(formation_labels.match_id as string)
    left join {{ ref('dim_teams') }} dt
        on  dt.provider = formation_labels.source_provider
       and dt.native_team_id = cast(formation_labels.team_id as string)

)
```

- [ ] **Step 2.F.2: Update config to `on_schema_change='append_new_columns'` + liquid_clustered_by=['match_key']**

#### Task 2.G: fct_physical_stats — match_key + player_key

**Files:**
- Modify: `dbt_project/models/marts/fct_physical_stats.sql`

- [ ] **Step 2.G.1: Add key propagations from fct_tracking_frames**

In `frames` CTE, add `match_key, player_key, source_provider` to SELECT. Propagate through `player_match_stats` GROUP BY.

- [ ] **Step 2.G.2: Update `final` SELECT with new keys**

Add `match_key, player_key, source_provider as data_source` to the projected list.

- [ ] **Step 2.G.3: Update surrogate-hash inputs**

```sql
{{ dbt_utils.generate_surrogate_key(['s.player_id', 's.match_id', 's.source_provider']) }} as physical_stats_id,
```

- [ ] **Step 2.G.4: Update config to `on_schema_change='append_new_columns'` + liquid_clustered_by=['match_key']**

---

### Task 3: Pausa subsystem (ADR-013 promotion + key extension)

#### Task 3.A: src/ingestion/pausa.py — retarget bronze table

**Files:**
- Modify: `src/ingestion/pausa.py`

- [ ] **Step 3.A.1: Update _TABLE_NAME constant**

Change line 37:
```python
_TABLE_NAME = "pausa_values"
```

- [ ] **Step 3.A.2: Update SkipGuard's results_table reference**

Change line 56:
```python
        results_table = f"{catalog}.bronze.{_TABLE_NAME}"
```

(Was previously: `f"{catalog}.{DEFAULT_GOLD_SCHEMA}.{_TABLE_NAME}"`.)

- [ ] **Step 3.A.3: Update _process_matches write target**

Change line 268-273:
```python
    written = write_delta_table(
        result_df,
        catalog,
        "bronze",
        _TABLE_NAME,
        replace_where=f"match_id IN ({ids_sql})",
        logger=logger,
        row_count=row_count,
    )
```

(Was previously: `DEFAULT_GOLD_SCHEMA` for the schema arg.)

- [ ] **Step 3.A.4: Verify imports — DEFAULT_GOLD_SCHEMA may still be needed elsewhere; if not, remove**

Search file for remaining references; if `DEFAULT_GOLD_SCHEMA` is no longer used, remove its import (line 30).

#### Task 3.B: stg_pausa__values.sql — repoint source

**Files:**
- Modify: `dbt_project/models/staging/pausa/stg_pausa__values.sql`

- [ ] **Step 3.B.1: Replace source reference**

Change line 11:
```sql
    select * from {{ source('pausa', 'pausa_values') }}
```

(Was: `{{ source('pausa_gold', 'fct_pausa_values') }}`.)

#### Task 3.C: _pausa__sources.yml — drop gold source, add bronze table

**Files:**
- Modify: `dbt_project/models/staging/pausa/_pausa__sources.yml`

- [ ] **Step 3.C.1: Add `pausa_values` table under `pausa` source; drop `pausa_gold` source entirely**

```yaml
version: 2

sources:
  - name: pausa
    description: >
      PAUSA (Passing Ability Under Spatiotemporal Awareness) pipeline outputs.
      Contains per-pass OBSO raw scores from GPU batch (D16) and computed
      PAUSA values from the serverless pipeline (D10).
    database: soccer_analytics
    schema: bronze
    loader: python_wheel
    config:
      loaded_at_field: _ingested_at
      freshness:
        warn_after: {count: 24, period: hour}
        error_after: {count: 72, period: hour}

    tables:
      - name: pausa_raw_scores
        description: >
          Per-pass OBSO scalars produced by D16 GPU batch on HF Jobs.
        columns:
          - name: pass_id
          - name: match_id
          - name: actual_obso
          - name: peak_obso
          - name: optimal_obso

      - name: pausa_values
        description: >
          Per-pass PAUSA decomposition: temporal judgment, spatial selection,
          and composite PAUSA score. Written by the pausa Python writer
          (src/ingestion/pausa.py). PR 7 (ADR-013) retargeted from gold layer
          direct write to bronze raw → dbt staging view → gold mart with
          contract: enforced: true.
        columns:
          - name: pass_id
          - name: match_id
          - name: player_id
          - name: team
          - name: period
          - name: timestamp_seconds
          - name: frame_id
          - name: temporal_judgment
          - name: spatial_selection
          - name: pausa_score
          - name: actual_obso
          - name: peak_obso
          - name: optimal_obso
          - name: receiver_x
          - name: receiver_y
```

Removes the entire `- name: pausa_gold` block.

#### Task 3.D: NEW dbt mart fct_pausa_values.sql

**Files:**
- Create: `dbt_project/models/marts/fct_pausa_values.sql`

- [ ] **Step 3.D.1: Create file**

```sql
{{ config(
    materialized='table',
    enabled=var('pausa_enabled', false),
    liquid_clustered_by=['match_key'],
    on_schema_change='fail',
    contract={'enforced': true}
) }}
-- fct_pausa_values.sql
-- Per-pass PAUSA decomposition (temporal + spatial + composite). Keys
-- inherited via INNER JOIN to fct_passes on pass_id per ADR-013
-- (consumer-side ML inference output pattern; second application after
-- PR 3's fct_xg_predictions_v2).
--
-- Disabled by default (var pausa_enabled=false); flipped on per-run in
-- the Databricks job config when wf-obso-pausa runs. See
-- workflow-cards/wf-obso-pausa.yaml.

select
    p.pass_id,
    p.match_id,
    fp.match_key,
    fp.team_key                                  as passer_team_key,
    fp.player_key                                as passer_player_key,
    p.player_id,
    p.team,
    p.period,
    p.timestamp_seconds,
    p.frame_id,
    cast(p.temporal_judgment as double)         as temporal_judgment,
    cast(p.spatial_selection as double)         as spatial_selection,
    cast(p.pausa_score as double)               as pausa_score,
    cast(p.actual_obso as double)               as actual_obso,
    cast(p.peak_obso as double)                 as peak_obso,
    cast(p.optimal_obso as double)              as optimal_obso,
    cast(p.receiver_x as double)                as receiver_x,
    cast(p.receiver_y as double)                as receiver_y

from {{ ref('stg_pausa__values') }} p
inner join {{ ref('fct_passes') }} fp on p.pass_id = fp.pass_id
```

#### Task 3.E: workflow-cards/wf-obso-pausa.yaml — outputs.tables update

**Files:**
- Modify: `workflow-cards/wf-obso-pausa.yaml`

- [ ] **Step 3.E.1: Replace single output table with two entries**

Replace line 50-52:

```yaml
  tables:
    - id: "{catalog}.bronze.pausa_values"
      destination: delta-table
    - id: "{catalog}.dev_gold.fct_pausa_values"
      destination: delta-table
      dbt_model: fct_pausa_values
```

#### Task 3.F: fct_pausa_rankings — add player_key + new pausa_ranking_id surrogate

**Files:**
- Modify: `dbt_project/models/marts/fct_pausa_rankings.sql`

- [ ] **Step 3.F.1: Add player_key + surrogate**

In the `aggregated` CTE, JOIN dim_players on player_id. In `final`, add `player_key` and a new `pausa_ranking_id` surrogate via `dbt_utils.generate_surrogate_key(['player_id'])`.

```sql
final as (

    select
        {{ dbt_utils.generate_surrogate_key(['a.player_id']) }} as pausa_ranking_id,
        cast(a.player_id as string)                                  as player_id,
        dp.player_key                                                as player_key,
        cast(a.player_display_name as string)                        as player_display_name,
        cast(a.total_matches as int)                                 as total_matches,
        cast(a.total_passes as int)                                  as total_passes,
        cast(a.passes_with_value as int)                             as passes_with_value,
        cast(a.avg_pausa as double)                                  as avg_pausa,
        cast(a.avg_temporal_judgment as double)                      as avg_temporal_judgment,
        cast(a.avg_spatial_selection as double)                      as avg_spatial_selection,
        cast(a.median_pausa as double)                               as median_pausa,
        cast(pm.total_minutes as double)                             as total_minutes,
        current_timestamp()                                          as _loaded_at

    from aggregated a
    left join physical_minutes pm
        on a.player_id = pm.player_id
    left join {{ ref('dim_players') }} dp
        on cast(dp.player_id as string) = cast(a.player_id as string)

)
```

- [ ] **Step 3.F.2: Add equivalent column for the empty-fallback branch**

In the `{% else %}` branch of `pausa_enabled`, add `cast(null as string) as pausa_ranking_id` and `cast(null as bigint) as player_key` to the empty-table SELECT.

#### Task 3.G: fct_pass_timing — match_key + player_key

**Files:**
- Modify: `dbt_project/models/marts/fct_pass_timing.sql`

- [ ] **Step 3.G.1: Add LEFT JOINs + new keys**

In `aggregated` CTE, JOIN `dim_matches` on `(provider, native_match_id = match_id)` (provider derived from int_pausa__pass_quality data — likely IDSSE only currently; verify) and `dim_players`. Add `match_key` and `player_key` to `final` SELECT.

```sql
final as (

    select
        {{ dbt_utils.generate_surrogate_key(['a.player_id','a.match_id']) }} as pass_timing_id,
        cast(a.player_id as string)                                   as player_id,
        dp.player_key,
        cast(a.match_id as string)                                    as match_id,
        dm.match_key,
        cast(a.player_display_name as string)                         as player_display_name,
        cast(a.pass_count as int)                                     as pass_count,
        cast(a.avg_temporal_judgment as double)                       as avg_temporal_judgment,
        cast(a.avg_spatial_selection as double)                       as avg_spatial_selection,
        cast(a.avg_pausa as double)                                   as avg_pausa,
        cast(a.median_pausa as double)                                as median_pausa,
        cast(a.passes_above_median_pausa as int)                      as passes_above_median_pausa,
        current_timestamp()                                           as _loaded_at

    from aggregated a
    left join {{ ref('dim_matches') }} dm
        on dm.provider = 'idsse'
       and dm.native_match_id = cast(a.match_id as string)
    left join {{ ref('dim_players') }} dp
        on dp.provider = 'idsse'
       and cast(dp.native_player_id as string) = cast(a.player_id as string)

)
```

If int_pausa__pass_quality covers multi-provider data (verify at impl time), generalize the join via a `data_source` column in the upstream — defer until verified.

- [ ] **Step 3.G.2: Mirror in empty-fallback branch with cast(null as bigint) for player_key + match_key + pass_timing_id**

---

### Task 4: Off-ball / space marts

#### Task 4.A: fct_off_ball_xt — match_key + player_key

**Files:**
- Modify: `dbt_project/models/marts/fct_off_ball_xt.sql`

- [ ] **Step 4.A.1: Add LEFT JOINs + key propagation in final CTE**

```sql
final as (

    select
        {{ dbt_utils.generate_surrogate_key([
            'off_ball_xt.player_id',
            'off_ball_xt.match_id',
            'coalesce(off_ball_xt.data_source, \\'unknown\\')'
        ]) }}                                       as off_ball_xt_id,

        off_ball_xt.player_id,
        dp.player_key,
        off_ball_xt.match_id,
        dm.match_key,
        off_ball_xt.data_source,
        off_ball_xt.total_off_ball_xt,
        off_ball_xt.avg_off_ball_xt,
        off_ball_xt.frames_sampled

    from off_ball_xt
    left join {{ ref('dim_matches') }} dm
        on  dm.provider = off_ball_xt.data_source
       and dm.native_match_id = cast(off_ball_xt.match_id as string)
    left join {{ ref('dim_players') }} dp
        on  dp.provider = off_ball_xt.data_source
       and dp.native_player_id = cast(off_ball_xt.player_id as string)

)
```

This assumes `stg_off_ball_xt__results` exposes `data_source`. **VERIFY at impl time** — if not, add `source_provider` derivation similar to tracking marts. Adjust this task's specifics based on staging shape.

- [ ] **Step 4.A.2: Update config to on_schema_change='append_new_columns' + liquid_clustered_by=['match_key']**

#### Task 4.B: fct_space_creation — match_key + player_key

**Files:**
- Modify: `dbt_project/models/marts/fct_space_creation.sql`

- [ ] **Step 4.B.1: Add LEFT JOINs + key propagation**

Replace the existing select:

```sql
{% if var('space_creation_enabled', false) %}

with values as (
    select * from {{ ref('stg_space_creation__values') }}
),

players as (
    select cast(player_id as string) as player_id, canonical_player_id
    from {{ ref('dim_players') }}
)

select
    {{ dbt_utils.generate_surrogate_key([
        'v.match_id', 'v.frame_id', 'v.player_id',
        'coalesce(v.data_source, \\'unknown\\')'
    ]) }} as space_creation_id,
    v.match_id,
    dm.match_key,
    v.frame_id,
    coalesce(p.canonical_player_id, v.player_id) as player_id,
    dp.player_key,
    v.team, v.period,
    v.data_source,
    v.space_created_m2, v.space_destroyed_m2, v.net_space_m2
from values v
left join players p on v.player_id = p.player_id
left join {{ ref('dim_matches') }} dm
    on  dm.provider = v.data_source
   and dm.native_match_id = cast(v.match_id as string)
left join {{ ref('dim_players') }} dp
    on  dp.provider = v.data_source
   and dp.native_player_id = cast(v.player_id as string)

{% else %}

select
    cast(null as string) as space_creation_id,
    cast(null as string) as match_id,
    cast(null as bigint) as match_key,
    cast(null as int) as frame_id,
    cast(null as string) as player_id,
    cast(null as bigint) as player_key,
    cast(null as string) as team,
    cast(null as int) as period,
    cast(null as string) as data_source,
    cast(null as double) as space_created_m2,
    cast(null as double) as space_destroyed_m2,
    cast(null as double) as net_space_m2
where 1 = 0

{% endif %}
```

VERIFY upstream `stg_space_creation__values` has `data_source`. If not, derive as in tracking pattern.

- [ ] **Step 4.B.2: Update config to liquid_clustered_by=['match_key'] (was ['match_id'])**

---

### Task 5: Tail facts

#### Task 5.A: fct_discipline_events — match_key + team_key + player_key

**Files:**
- Modify: `dbt_project/models/marts/fct_discipline_events.sql`

- [ ] **Step 5.A.1: Add LEFT JOINs + key propagation in final SELECT**

```sql
final as (

    select
        cast(event_id as string)                        as event_id,
        cast(match_id as bigint)                        as match_id,
        dm.match_key,
        cast(competition_id as int)                     as competition_id,
        cast(season_id as int)                          as season_id,
        cast(period as int)                             as period,
        cast(minute as int)                             as minute,
        cast(second as int)                             as second,
        cast(player_id as int)                          as player_id,
        dp.player_key,
        cast(team_id as int)                            as team_id,
        dt.team_key,
        cast(event_type as string)                      as event_type,
        cast(card_name as string)                       as card_name,
        cast('statsbomb' as string)                     as data_source,
        current_timestamp()                             as _loaded_at

    from source
    left join {{ ref('dim_matches') }} dm
        on  dm.provider = 'statsbomb'
       and dm.native_match_id = cast(source.match_id as string)
    left join {{ ref('dim_teams') }} dt
        on  dt.provider = 'statsbomb'
       and dt.native_team_id = cast(source.team_id as string)
    left join {{ ref('dim_players') }} dp
        on  dp.provider = 'statsbomb'
       and dp.native_player_id = cast(source.player_id as string)

)
```

- [ ] **Step 5.A.2: Update config to liquid_clustered_by=['match_key']**

---

### Task 6: Conformed-fact closures (Q3)

#### Task 6.A: fct_passes — passer_team_key + passer_player_key + recipient_player_key

**Files:**
- Modify: `dbt_project/models/marts/fct_passes.sql`

- [ ] **Step 6.A.1: Add LEFT JOINs in `final` CTE (or equivalent post-window CTE)**

Reading current fct_passes.sql to confirm exact CTE name. Apply pattern:

```sql
-- Inside the final SELECT, add the new key columns
        ...
        unified_passes.passer_player_id,
        dp_passer.player_key                  as passer_player_key,
        unified_passes.recipient_player_id,
        dp_recipient.player_key               as recipient_player_key,
        unified_passes.team_id,
        dt.team_key                           as team_key,
        ...
    from unified_passes
    -- existing JOINs ...
    left join {{ ref('dim_players') }} dp_passer
        on  dp_passer.provider = unified_passes.data_source
       and dp_passer.native_player_id = cast(unified_passes.passer_player_id as string)
    left join {{ ref('dim_players') }} dp_recipient
        on  dp_recipient.provider = unified_passes.data_source
       and dp_recipient.native_player_id = cast(unified_passes.recipient_player_id as string)
    left join {{ ref('dim_teams') }} dt
        on  dt.provider = unified_passes.data_source
       and dt.native_team_id = cast(unified_passes.team_id as string)
```

Lock exact column names (passer_player_id vs player_id) by reading the file at impl time.

- [ ] **Step 6.A.2: Update surrogate-key inputs to include data_source if not already present**

Verify current surrogate construction. Add data_source if missing.

- [ ] **Step 6.A.3: Update on_schema_change config if incremental**

#### Task 6.B: fct_shots — team_key + player_key

**Files:**
- Modify: `dbt_project/models/marts/fct_shots.sql`

- [ ] **Step 6.B.1: Add LEFT JOINs + new keys**

Pattern:

```sql
-- final SELECT additions
        ...
        unified_shots.player_id,
        dp.player_key,
        unified_shots.team_id,
        dt.team_key,
        ...
    left join {{ ref('dim_players') }} dp
        on  dp.provider = unified_shots.data_source
       and dp.native_player_id = cast(unified_shots.player_id as string)
    left join {{ ref('dim_teams') }} dt
        on  dt.provider = unified_shots.data_source
       and dt.native_team_id = cast(unified_shots.team_id as string)
```

- [ ] **Step 6.B.2: Verify surrogate inputs include data_source**

#### Task 6.C: fct_action_values — team_key + player_key

**Files:**
- Modify: `dbt_project/models/marts/fct_action_values.sql`

- [ ] **Step 6.C.1: Add LEFT JOINs + new keys**

Same pattern as 6.B applied to fct_action_values' final SELECT. Verify provider source field name.

#### Task 6.D: fct_match_summary — home_team_key + away_team_key

**Files:**
- Modify: `dbt_project/models/marts/fct_match_summary.sql`

- [ ] **Step 6.D.1: Add two dim_teams JOINs**

```sql
-- Final SELECT additions:
        ...
        sb.home_team_id,
        dt_home.team_key                      as home_team_key,
        sb.away_team_id,
        dt_away.team_key                      as away_team_key,
        ...
    left join {{ ref('dim_teams') }} dt_home
        on  dt_home.provider = sb.data_source
       and dt_home.native_team_id = cast(sb.home_team_id as string)
    left join {{ ref('dim_teams') }} dt_away
        on  dt_away.provider = sb.data_source
       and dt_away.native_team_id = cast(sb.away_team_id as string)
```

Replace `sb` with the actual upstream CTE alias.

#### Task 6.E: fct_line_breaking_results — team_key + player_key

**Files:**
- Modify: `dbt_project/models/marts/fct_line_breaking_results.sql`

- [ ] **Step 6.E.1: Add LEFT JOINs + new keys**

Same pattern as 6.B.

---

### Task 7: Aggregate marts

#### Task 7.A: fct_heatmap_agg — team_key

**Files:**
- Modify: `dbt_project/models/marts/fct_heatmap_agg.sql`

- [ ] **Step 7.A.1: Add team_key via dim_teams JOIN**

The mart aggregates fct_passes + fct_shots. Both will have team_key post-Task-6 — pull through directly:

```sql
with pass_events as (

    select
        competition_id,
        team_id,
        team_key,                                    -- pulled from fct_passes (Task 6.A)
        cast(round(start_x / 10) * 10 + 5 as int)    as x_bin,
        cast(round(start_y / 10) * 10 + 5 as int)    as y_bin,
        'pass'                                       as action_type
    from {{ ref('fct_passes') }}
    where start_x is not null
      and start_y is not null
      and competition_id is not null
      and team_id is not null

),

shot_events as (

    select
        competition_id,
        team_id,
        team_key,                                    -- pulled from fct_shots (Task 6.B)
        cast(round(location_x / 10) * 10 + 5 as int) as x_bin,
        cast(round(location_y / 10) * 10 + 5 as int) as y_bin,
        'shot'                                       as action_type
    from {{ ref('fct_shots') }}
    where location_x is not null
      and location_y is not null
      and competition_id is not null
      and team_id is not null

),

unioned as (

    select * from pass_events
    union all
    select * from shot_events

),

aggregated as (

    select
        competition_id,
        team_id,
        team_key,
        action_type,
        x_bin,
        y_bin,
        count(*)                                     as event_count
    from unioned
    group by competition_id, team_id, team_key, action_type, x_bin, y_bin

),

final as (

    select
        cast(competition_id as int)                  as competition_id,
        cast(team_id as int)                         as team_id,
        team_key,
        cast(action_type as string)                  as action_type,
        cast(x_bin as int)                           as x_bin,
        cast(y_bin as int)                           as y_bin,
        cast(event_count as bigint)                  as event_count,
        current_timestamp()                          as _loaded_at

    from aggregated

)

select * from final
```

#### Task 7.B: fct_vaep_breakdown_agg — team_key + player_key

**Files:**
- Modify: `dbt_project/models/marts/fct_vaep_breakdown_agg.sql`

- [ ] **Step 7.B.1: Add team_key + player_key via passthrough from fct_action_values**

```sql
with action_values as (

    select * from {{ ref('fct_action_values') }}
    where competition_id is not null
      and team_id is not null
      and player_id is not null
      and action_type is not null

),

aggregated as (

    select
        competition_id,
        team_id,
        team_key,
        player_id,
        player_key,
        action_type,
        sum(vaep_value)                               as total_vaep,
        sum(offensive_value)                          as total_offensive,
        sum(defensive_value)                          as total_defensive,
        count(*)                                      as action_count
    from action_values
    group by competition_id, team_id, team_key, player_id, player_key, action_type

),

final as (

    select
        cast(competition_id as int)                   as competition_id,
        cast(team_id as int)                          as team_id,
        team_key,
        cast(player_id as int)                        as player_id,
        player_key,
        cast(action_type as string)                   as action_type,
        cast(total_vaep as double)                    as total_vaep,
        cast(total_offensive as double)               as total_offensive,
        cast(total_defensive as double)               as total_defensive,
        cast(action_count as bigint)                  as action_count,
        current_timestamp()                           as _loaded_at

    from aggregated

)

select * from final
```

---

### Task 8: Pull-through extensions

#### Task 8.A: fct_xg_predictions — team_key + player_key (via fct_shots)

**Files:**
- Modify: `dbt_project/models/marts/fct_xg_predictions.sql`

- [ ] **Step 8.A.1: Add s.team_key + s.player_key to SELECT**

The mart already INNER JOINs fct_shots. Add two columns to the SELECT.

#### Task 8.B: fct_xg_predictions_v2 — team_key + player_key (via fct_shots)

**Files:**
- Modify: `dbt_project/models/marts/fct_xg_predictions_v2.sql`

- [ ] **Step 8.B.1: Add s.team_key + s.player_key**

```sql
select
    p.shot_id,
    s.match_key,
    s.competition_key,
    s.competition_id,
    s.team_key,
    s.player_key,
    p.xg_set_encoder,
    p.xg_ci_lower,
    p.xg_ci_upper

from {{ ref('stg_xg__predictions_v2') }} p
inner join {{ ref('fct_shots') }} s on p.shot_id = s.shot_id
```

---

### Task 9: Bridge retirement

#### Task 9.A: fct_player_percentiles — retire physical_by_comp dim_matches bridge

**Files:**
- Modify: `dbt_project/models/marts/fct_player_percentiles.sql`

- [ ] **Step 9.A.1: Replace `physical_by_comp` CTE**

Replace lines 62-80 (the physical_by_comp CTE):

```sql
physical_by_comp as (

    select
        cast(ps.player_id as string) as player_id,
        ms.competition_id,
        ms.season_id,
        avg(ps.distance_per_minute_m)  as avg_distance_per_minute,
        avg(ps.max_speed_ms)           as avg_max_speed
    from {{ ref('fct_physical_stats') }} ps
    -- PR 7 (ADR-011): retired the dim_matches bridge added by PR 5b CI-triage.
    -- fct_physical_stats now carries match_key directly (Task 2.G).
    inner join {{ ref('fct_match_summary') }} ms
        on ps.match_key = ms.match_key
    group by cast(ps.player_id as string), ms.competition_id, ms.season_id

),
```

Bridge retired. One CTE simplified.

---

### Task 10: Pitch-control writer schema widening

#### Task 10.A: src/ingestion/pitch_control_batch.py — widen schema

**Files:**
- Modify: `src/ingestion/pitch_control_batch.py`

- [ ] **Step 10.A.1: Update _RESULTS_SCHEMA constant**

Change line 39:

```python
_RESULTS_SCHEMA = (
    "tracking_id STRING, match_id STRING, data_source STRING, match_key BIGINT, "
    "pitch_control_value DOUBLE, _ingested_at TIMESTAMP"
)
```

- [ ] **Step 10.A.2: Update _PITCH_CONTROL_BRONZE_COLS**

Change lines 45-50:

```python
_PITCH_CONTROL_BRONZE_COLS: tuple[str, ...] = (
    "tracking_id",
    "match_id",
    "data_source",
    "match_key",
    "pitch_control_value",
    "_ingested_at",
)
```

- [ ] **Step 10.A.3: Update reader to pull source_provider + match_key from fct_tracking_frames**

Change lines 226-241 (the SELECT in `_process_matches`):

```python
    tracking_df = (
        spark.table(gold_table)
        .filter(F.col("match_id").isin(new_ids_str))
        .select(
            "tracking_id",
            "match_id",
            "match_key",
            "source_provider",
            "player_id",
            "team",
            "x",
            "y",
            "velocity_x",
            "velocity_y",
            "frame",
            "period",
            "frame_rate",
        )
    )
```

- [ ] **Step 10.A.4: Update UDF to emit data_source + match_key**

In `_make_batch_udf` (lines 60-158), update the closure to include `match_key` and `data_source` in the per-row results:

```python
            for tid, mid, dsrc, mkey, pcv in zip(
                frame_clean["tracking_id"],
                frame_clean["match_id"],
                frame_clean["source_provider"],
                frame_clean["match_key"],
                pc_values,
                strict=False,
            ):
                results.append(
                    {
                        "tracking_id": str(tid),
                        "match_id": str(mid),
                        "data_source": str(dsrc),
                        "match_key": int(mkey) if mkey is not None else None,
                        "pitch_control_value": float(pcv),
                    }
                )
```

Update `_empty` to include the new columns:

```python
        _empty = _pd.DataFrame(columns=_pd.Index([
            "tracking_id", "match_id", "data_source", "match_key", "pitch_control_value"
        ]))
```

- [ ] **Step 10.A.5: Update output_schema in _process_matches**

Change lines 262-268:

```python
    output_schema = StructType(
        [
            StructField("tracking_id", StringType(), nullable=False),
            StructField("match_id", StringType(), nullable=False),
            StructField("data_source", StringType(), nullable=True),
            StructField("match_key", LongType(), nullable=True),
            StructField("pitch_control_value", DoubleType(), nullable=False),
        ]
    )
```

Add `LongType` import on line 215:

```python
from pyspark.sql.types import DoubleType, LongType, StringType, StructField, StructType
```

#### Task 10.B: stg_pitch_control__values.sql — collapse prefix-CASE to passthrough

**Files:**
- Modify: `dbt_project/models/staging/pitch_control/stg_pitch_control__values.sql` (path verified at impl time)

- [ ] **Step 10.B.1: Replace prefix-CASE with passthrough of writer-emitted columns**

After PR 7's writer update, bronze.pitch_control_values has data_source + match_key natively. Staging becomes:

```sql
with source as (

    select * from {{ source('pitch_control', 'pitch_control_values') }}

)

select
    tracking_id,
    match_id,
    data_source,
    match_key,
    pitch_control_value,
    _ingested_at
from source
```

Drop the prefix-CASE CTE entirely. PR 6's bridging logic retired.

- [ ] **Step 10.B.2: Update _pitch_control__sources.yml to surface new columns**

Add `data_source` and `match_key` to the bronze source columns list.

---

### Task 11: dbt YAML contract updates

#### Task 11.A: _marts__models.yml — add column entries + relationships warn for ALL new key columns

**Files:**
- Modify: `dbt_project/models/marts/_marts__models.yml`

- [ ] **Step 11.A.1: For each mart in §3.1 of spec, add YAML column entries**

Pattern per new key column (apply to all marts × keys):

```yaml
      - name: match_key
        data_type: bigint
        description: >
          Kimball surrogate FK to dim_matches (PR 7, ADR-011). Resolved via
          LEFT JOIN dim_matches on (provider, native_match_id). Coexists with
          match_id during the 2026-07-22 dual-column window. PR 8 will drop
          legacy match_id.
        data_tests:
          - relationships:
              to: ref('dim_matches')
              field: match_key
              config:
                severity: warn

      - name: team_key
        data_type: bigint
        description: >
          Kimball surrogate FK to dim_teams (PR 7, ADR-011). Resolved via
          LEFT JOIN dim_teams on (provider, native_team_id). Coexists with
          team_id during the 2026-07-22 dual-column window.
        data_tests:
          - relationships:
              to: ref('dim_teams')
              field: team_key
              config:
                severity: warn

      - name: player_key
        data_type: bigint
        description: >
          Kimball surrogate FK to dim_players (PR 7, ADR-011). Resolved via
          LEFT JOIN dim_players on (provider, native_player_id). Coexists
          with player_id during the 2026-07-22 dual-column window.
        data_tests:
          - relationships:
              to: ref('dim_players')
              field: player_key
              config:
                severity: warn
```

Apply variants where columns are passer_player_key, recipient_player_key, home_team_key, away_team_key, opponent_team_key — adjust description and field reference accordingly.

For `fct_pausa_values` (NEW mart), add the entire model entry with `config: contract: enforced: true` block. Mirror PR 3's `fct_xg_predictions_v2` YAML entry as the structural template.

- [ ] **Step 11.A.2: Add `dbt_utils.unique_combination_of_columns` schema test on every PG-PK grain (synced-table dual-defense)**

For each mart synced to Lakebase, add to its data_tests block:

```yaml
    data_tests:
      - dbt_utils.unique_combination_of_columns:
          combination_of_columns:
            - <pg_pk_col_1>
            - <pg_pk_col_2>
            ...
```

Verify the PG-PK grain per `terraform/modules/synced_tables/main.tf` for each synced mart. Reference application: `fct_workflow_costs` per PR #203.

---

### Task 12: Terraform + registry + indexes

#### Task 12.A: terraform/modules/synced_tables/main.tf — verify/update fct_pausa_values resource

**Files:**
- Modify: `terraform/modules/synced_tables/main.tf`

- [ ] **Step 12.A.1: Update fct_pausa_values resource**

Per Step 0.6 audit, update the existing resource to reflect the new mart's PG-PK grain `(match_key, pass_id)`:

```hcl
module "fct_pausa_values_synced" {
  source = "..."  # existing module path
  source_table = "soccer_analytics.dev_gold.fct_pausa_values"
  primary_key  = ["match_key", "pass_id"]
  # ... other parameters per existing resource shape
}
```

If the resource doesn't exist yet, add per the pattern of an existing synced-table resource (e.g., fct_action_values_synced).

#### Task 12.B: SYNCED_TABLES registry

**Files:**
- Modify: `src/ingestion/refresh_synced_tables.py`

- [ ] **Step 12.B.1: Verify fct_pausa_values_synced is registered**

Search for the SYNCED_TABLES tuple/list in the file. If absent, add `"fct_pausa_values_synced"` entry.

#### Task 12.C: scripts/create_indexes.py — index set for fct_pausa_values_synced

**Files:**
- Modify: `scripts/create_indexes.py`

- [ ] **Step 12.C.1: Add index set**

Add an entry for `fct_pausa_values_synced` with indexes on common filter columns: `match_key`, `passer_player_key` (per Lakebase rule "Index every filtered column on fact tables >100K rows").

---

### Task 13: Test additions/extensions

#### Task 13.A: test_marts_kimball_contracts.py — extend _CASES

**Files:**
- Modify: `src/tests/test_marts_kimball_contracts.py`

- [ ] **Step 13.A.1: Add PR 7 mart entries to `_CASES` tuple**

Append entries (thresholds calibrated post-rebuild — initial commit uses 0.0 placeholder; tightened in 2nd commit on same branch after Phase 2 measurement):

```python
    # PR 7 — Tracking marts (post-staging team_id derivation)
    ("fct_tracking_frames", "match_key", 0.0),
    ("fct_tracking_frames", "team_key", 0.0),
    ("fct_tracking_frames", "player_key", 0.0),
    ("fct_tracking_avg_positions", "match_key", 0.0),
    ("fct_tracking_avg_positions", "team_key", 0.0),
    ("fct_tracking_avg_positions", "player_key", 0.0),
    ("fct_tracking_shape_timeline", "match_key", 0.0),
    ("fct_tracking_shape_timeline", "team_key", 0.0),
    ("fct_tracking_shape_timeline", "player_key", 0.0),
    ("fct_player_positions", "match_key", 0.0),
    ("fct_player_positions", "team_key", 0.0),
    ("fct_player_positions", "player_key", 0.0),
    ("fct_position_maps", "match_key", 0.0),
    ("fct_position_maps", "team_key", 0.0),
    ("fct_position_maps", "player_key", 0.0),
    ("fct_formation_labels", "match_key", 0.0),
    ("fct_formation_labels", "team_key", 0.0),
    ("fct_physical_stats", "match_key", 0.0),
    ("fct_physical_stats", "player_key", 0.0),
    # PR 7 — Pausa subsystem
    ("fct_pausa_values", "match_key", 0.0),
    ("fct_pausa_values", "passer_team_key", 0.0),
    ("fct_pausa_values", "passer_player_key", 0.0),
    ("fct_pausa_rankings", "player_key", 0.0),
    ("fct_pass_timing", "match_key", 0.0),
    ("fct_pass_timing", "player_key", 0.0),
    # PR 7 — Off-ball/space
    ("fct_off_ball_xt", "match_key", 0.0),
    ("fct_off_ball_xt", "player_key", 0.0),
    ("fct_space_creation", "match_key", 0.0),
    ("fct_space_creation", "player_key", 0.0),
    # PR 7 — Tail
    ("fct_discipline_events", "match_key", 0.0),
    ("fct_discipline_events", "team_key", 0.0),
    ("fct_discipline_events", "player_key", 0.0),
    # PR 7 — Conformed-fact closures (Q3)
    ("fct_passes", "team_key", 0.0),
    ("fct_passes", "passer_player_key", 0.0),
    ("fct_passes", "recipient_player_key", 0.0),
    ("fct_shots", "team_key", 0.0),
    ("fct_shots", "player_key", 0.0),
    ("fct_action_values", "team_key", 0.0),
    ("fct_action_values", "player_key", 0.0),
    ("fct_match_summary", "home_team_key", 0.0),
    ("fct_match_summary", "away_team_key", 0.0),
    ("fct_line_breaking_results", "team_key", 0.0),
    ("fct_line_breaking_results", "player_key", 0.0),
    # PR 7 — Aggregates
    ("fct_heatmap_agg", "team_key", 0.0),
    ("fct_vaep_breakdown_agg", "team_key", 0.0),
    ("fct_vaep_breakdown_agg", "player_key", 0.0),
    # PR 7 — Pull-through
    ("fct_xg_predictions", "team_key", 0.0),
    ("fct_xg_predictions", "player_key", 0.0),
    ("fct_xg_predictions_v2", "team_key", 0.0),
    ("fct_xg_predictions_v2", "player_key", 0.0),
```

Phase 2 step S7 measures actual thresholds; thresholds tightened in 2nd commit on same branch (squash-merge collapses).

#### Task 13.B: test_marts_kimball_completion.py — NEW invariant

**Files:**
- Create: `src/tests/test_marts_kimball_completion.py`

- [ ] **Step 13.B.1: Create file**

```python
# ruff: noqa: S608 — _SMART_KEY_MAPPINGS are module-level tuples, not user input.
"""PR 7 / pre-PR-8 invariant — every fact mart with a smart *_id column has
the corresponding *_key column.

Catches future smart-key resurfacing (e.g., a new ML mart being added with
match_id but no match_key would fail this test). Skips when DATABRICKS_*
env vars are absent.
"""

from __future__ import annotations

import os

import pytest

databricks_sql = pytest.importorskip("databricks.sql")

requires_databricks = pytest.mark.skipif(
    not all(os.environ.get(v) for v in ("DATABRICKS_HOST", "DATABRICKS_HTTP_PATH", "DATABRICKS_TOKEN")),
    reason="Databricks SQL env vars not set",
)


# (smart_id_col, kimball_key_col) — every fact mart that has the smart
# col MUST have the kimball key col post-PR-7. PR 8 will drop the smart cols.
_SMART_KEY_MAPPINGS: tuple[tuple[str, str], ...] = (
    ("match_id", "match_key"),
    ("team_id", "team_key"),
    ("player_id", "player_key"),
)


@pytest.fixture(scope="module")
def conn():
    c = databricks_sql.connect(
        server_hostname=os.environ["DATABRICKS_HOST"].replace("https://", "").rstrip("/"),
        http_path=os.environ["DATABRICKS_HTTP_PATH"],
        access_token=os.environ["DATABRICKS_TOKEN"],
    )
    yield c
    c.close()


@requires_databricks
def test_no_smart_key_island(conn) -> None:
    """No fact mart in dev_gold has both legacy *_id smart key AND missing the corresponding *_key column."""
    cur = conn.cursor()
    cur.execute(
        "SHOW TABLES IN soccer_analytics.dev_gold LIKE 'fct_*'"
    )
    facts = [r[1] for r in cur.fetchall()]

    violations: list[str] = []
    for mart in facts:
        cur.execute(f"DESCRIBE TABLE soccer_analytics.dev_gold.{mart}")
        cols = {r[0] for r in cur.fetchall()}
        for smart_col, key_col in _SMART_KEY_MAPPINGS:
            # passer_player_id / recipient_player_id / etc. variants — match base smart_col as substring
            has_smart = any(smart_col in c for c in cols)
            has_key = any(key_col in c for c in cols)
            if has_smart and not has_key:
                violations.append(f"{mart}: has *{smart_col}* but no *{key_col}*")

    assert not violations, "Smart-key islands detected:\n" + "\n".join(violations)
```

#### Task 13.C: test_pausa_adr013_compliance.py — NEW

**Files:**
- Create: `src/tests/test_pausa_adr013_compliance.py`

- [ ] **Step 13.C.1: Create file**

```python
# ruff: noqa: S608 — string-format SQL is read-only mart name interpolation.
"""PR 7 ADR-013 compliance — fct_pausa_values is dbt-built with contract enforced and INNER-JOINs fct_passes."""

from __future__ import annotations

import os
from pathlib import Path

import pytest
import yaml

databricks_sql = pytest.importorskip("databricks.sql")

requires_databricks = pytest.mark.skipif(
    not all(os.environ.get(v) for v in ("DATABRICKS_HOST", "DATABRICKS_HTTP_PATH", "DATABRICKS_TOKEN")),
    reason="Databricks SQL env vars not set",
)


def test_fct_pausa_values_sql_exists() -> None:
    """fct_pausa_values.sql must exist as a dbt mart."""
    path = Path(__file__).parents[2] / "dbt_project" / "models" / "marts" / "fct_pausa_values.sql"
    assert path.exists(), f"{path} missing — ADR-013 promotion not complete"


def test_fct_pausa_values_inner_joins_fct_passes() -> None:
    """ADR-013 requires INNER JOIN to identity fact (fct_passes for pausa)."""
    path = Path(__file__).parents[2] / "dbt_project" / "models" / "marts" / "fct_pausa_values.sql"
    src = path.read_text()
    assert "inner join" in src.lower() and "fct_passes" in src.lower(), (
        "fct_pausa_values must INNER JOIN fct_passes per ADR-013"
    )
    assert "on p.pass_id = fp.pass_id" in src.lower() or "on pass_id" in src.lower(), (
        "INNER JOIN must be on pass_id"
    )


def test_fct_pausa_values_yaml_contract_enforced() -> None:
    """ADR-013 requires contract: enforced: true."""
    path = Path(__file__).parents[2] / "dbt_project" / "models" / "marts" / "_marts__models.yml"
    with path.open() as f:
        data = yaml.safe_load(f)
    matches = [m for m in data.get("models", []) if m.get("name") == "fct_pausa_values"]
    assert matches, "fct_pausa_values not in _marts__models.yml"
    cfg = matches[0].get("config", {}) or {}
    contract = cfg.get("contract", {}) or {}
    assert contract.get("enforced") is True, (
        f"fct_pausa_values must have contract: enforced: true; got {contract}"
    )


@requires_databricks
def test_fct_pausa_values_table_exists_and_keys_inherited(conn) -> None:
    """fct_pausa_values dev_gold table has the inherited match_key from fct_passes."""
    pass  # Implementation: query DESCRIBE table; assert match_key column type bigint
```

(Conn fixture pattern same as test_marts_kimball_completion.py.)

#### Task 13.D: test_pausa_writer_parity.py — NEW (ADR-002 §4)

**Files:**
- Create: `src/tests/test_pausa_writer_parity.py`

- [ ] **Step 13.D.1: Create file matching test_defcon_schema_parity pattern**

Parse `_RESULTS_SCHEMA` string from `src/ingestion/pausa.py` into a Spark StructType; DESCRIBE `bronze.pausa_values`; assert column-name + type equality. Mirror `src/tests/test_defcon_schema_parity.py` structure.

#### Task 13.E: test_pitch_control_writer_parity.py — NEW (ADR-002 §4)

**Files:**
- Create: `src/tests/test_pitch_control_writer_parity.py`

- [ ] **Step 13.E.1: Create file**

Same pattern as 13.D applied to `pitch_control_batch.py:_RESULTS_SCHEMA` + `bronze.pitch_control_values` DESCRIBE.

#### Task 13.F: test_bronze_live_schema.py — extensions

**Files:**
- Modify: `src/tests/test_bronze_live_schema.py`

- [ ] **Step 13.F.1: Add `bronze.pausa_values` entry**

Add to the bronze tables tuple/dict with expected columns matching `_RESULTS_SCHEMA` from `pausa.py`.

- [ ] **Step 13.F.2: Update `bronze.pitch_control_values` entry**

Update expected columns to include the new `data_source` + `match_key`.

#### Task 13.G: test_marts_live_schema.py — add PR 7 marts

**Files:**
- Modify: `src/tests/test_marts_live_schema.py`

- [ ] **Step 13.G.1: Add live DESCRIBE assertions for all PR 7 marts**

Append entries for fct_tracking_frames, fct_tracking_avg_positions, fct_tracking_shape_timeline, fct_player_positions, fct_position_maps, fct_formation_labels, fct_physical_stats, fct_pausa_values, fct_pausa_rankings, fct_pass_timing, fct_off_ball_xt, fct_space_creation, fct_discipline_events. Each entry asserts presence of expected column names + types from §3.1.

#### Task 13.H: test_dbt_passes_kimball_migration.py — extension

**Files:**
- Modify: `src/tests/test_dbt_passes_kimball_migration.py`

- [ ] **Step 13.H.1: Add team_key + player_key non-NULL assertions**

Extend existing test cases or add new cases asserting `fct_passes.team_key` + `fct_passes.passer_player_key` non-NULL post-migration on dev_gold.

---

### Task 14: HF model card updates (text-only)

#### Task 14.A: off-ball-xt.md

**Files:**
- Modify: `docs/huggingface/model-cards/off-ball-xt.md`

- [ ] **Step 14.A.1: Add dual-column window stanza**

Insert after the existing model description / before the methodology section:

```markdown
## Schema Migration — 2026-04-25 → 2026-07-22 Dual-Column Window

PR 7 of the lakehouse Kimball migration (ADR-011) added BIGINT surrogate FKs `match_key`, `team_key`, `player_key` to the `fct_off_ball_xt` mart that this model writes to. Legacy columns `match_id`, `player_id` coexist during the dual-column window; PR 8 (~2026-07-22) will drop them.
```

#### Task 14.B: space-creation.md

Same stanza pattern, mart-specific reference.

#### Task 14.C: pitch-control.md

Stanza notes the bronze schema widening (data_source + match_key columns landed in bronze.pitch_control_values).

#### Task 14.D: obso-pausa.md

Stanza notes the ADR-013 promotion (`fct_pausa_values` is now a dbt-built mart with contract enforced).

#### Task 14.E: xg.md

Stanza notes pull-through extension on fct_xg_predictions + fct_xg_predictions_v2.

#### Task 14.F: defcon.md (verify)

Read at impl time; only edit if PR 7 introduces new key references.

---

### Task 15: HF dataset card updates (text-only — payload publishes happen in Phase 2)

#### Task 15.A-H: PR-7-affected dataset cards (8)

For each card in §3.5 PR-7-affected list, add the dual-column window stanza adapted to the dataset's specific mart:

```markdown
## Schema Migration — 2026-04-25 → 2026-07-22 Dual-Column Window

PR 7 of the lakehouse Kimball migration (ADR-011) added BIGINT surrogate FKs (`match_key`, `team_key`, `player_key` as applicable) to the underlying `<mart>` mart. **This dataset payload now includes the new BIGINT key columns** alongside legacy `match_id` / `team_id` / `player_id` during the dual-column window; PR 8 (~2026-07-22) will drop the legacy columns.

Recommended consumer behaviour:
- New consumers: filter / join on `match_key` / `team_key` / `player_key` (BIGINT, hash-stable across Kimball's `provider`+`native_id` derivation).
- Existing consumers: continue using `match_id` / `team_id` / `player_id` until 2026-07-22; migrate at your convenience inside the window.
```

Cards: obso-pausa-values.md, pitch-control-tracking.md, space-creation-values.md, line-breaking-passes.md, spadl-vaep-action-values.md, statsbomb-shots-on-target.md, xg-shot-data.md, xg-freeze-frame-data.md.

#### Task 15.I-M: PR-5b absorbed dataset cards (5)

For each: add same dual-column stanza. PR 5b's earlier card text said "payloads ship in PR 8" — replace that text with the corrected stanza noting PR 7 ships the payload columns.

Cards: football2vec-player-embeddings.md, football2vec-360-embeddings.md, football2vec-training-data.md, football2vec-360-training-data.md, football2vec-statsbomb-wyscout.md.

---

### Task 16: ADR + spec updates

#### Task 16.A: ADR-011 staged-rollout table — PR 7 row

**Files:**
- Modify: `docs/superpowers/adrs/ADR-011-unified-kimball-match-dimension.md`

- [ ] **Step 16.A.1: Update PR 7 row Status**

The exact final hash + date are filled in Phase 3; for the Phase-0 commit, set Status to "In progress (date)" or leave "Planned" — final update happens Phase 3.

#### Task 16.B: ADR-013 §Notes — second-application entry

**Files:**
- Modify: `docs/superpowers/adrs/ADR-013-ml-inference-outputs-dbt-mart.md`

- [ ] **Step 16.B.1: Verify the second-application bullet is accurate**

Existing text says "PR 7 (planned)". Update to "PR 7 (in progress)" for first commit; final update in Phase 3 to "Shipped (date, hash)".

---

### Task 17: Pre-push gates

- [ ] **Step 17.1: ruff lint**

```bash
uv run ruff check src/ scripts/ dbt_project/
```

Expected: zero violations. Fix any inline.

- [ ] **Step 17.2: ruff format check**

```bash
uv run ruff format --check src/ scripts/
```

Expected: clean.

- [ ] **Step 17.3: pyright**

```bash
uv run pyright src/
```

Expected: zero errors.

- [ ] **Step 17.4: dbt parse**

```bash
DATABRICKS_HOST=placeholder DATABRICKS_TOKEN=placeholder DATABRICKS_HTTP_PATH=placeholder \
  uvx --from "dbt-core>=1.10.0,<1.12.0" --with dbt-databricks dbt parse \
    --project-dir dbt_project --profiles-dir dbt_project
```

Expected: parses with no errors. Catches YAML/SQL contract mismatches.

- [ ] **Step 17.5: pytest (excluding live tests)**

```bash
uv run pytest src/tests/ -v \
  --ignore=src/tests/test_marts_kimball_contracts.py \
  --ignore=src/tests/test_marts_kimball_completion.py \
  --ignore=src/tests/test_pausa_adr013_compliance.py \
  --ignore=src/tests/test_pausa_writer_parity.py \
  --ignore=src/tests/test_pitch_control_writer_parity.py \
  --ignore=src/tests/test_marts_live_schema.py \
  --ignore=src/tests/test_bronze_live_schema.py \
  --ignore=src/tests/test_dbt_passes_kimball_migration.py
```

Expected: all green. Live tests deferred to Phase 2.

- [ ] **Step 17.6: on_schema_change audit**

```bash
uv run python -c "
import re, pathlib
issues = []
for f in pathlib.Path('dbt_project/models/marts').glob('*.sql'):
    src = f.read_text()
    if re.search(r\"materialized\\s*=\\s*'incremental'\", src) and \"on_schema_change='append_new_columns'\" not in src:
        issues.append(f.name)
print('Marts missing on_schema_change=append_new_columns:', issues)
"
```

Expected output: `Marts missing on_schema_change=append_new_columns: []`. Add the config to any incremental mart in PR 7 scope that's missing it.

---

### Task 18: Single commit + push + open PR (USER GIT GATE)

- [ ] **Step 18.1: Stage and review diff**

```bash
git status
git diff --stat
git diff | head -500
```

Confirm only PR 7 files are touched. Halt and ask user before committing if anything looks off.

- [ ] **Step 18.2: PAUSE — surface diff to user; ask for explicit commit approval**

Per `feedback_no_commits_without_approval`. Do NOT auto-commit.

- [ ] **Step 18.3: After approval — single commit + push + open PR**

```bash
git add -A  # carefully — verify Step 18.1 first
git commit -m "$(cat <<'EOF'
feat(kimball-pr7): close ADR-011 migration — tracking + formations + pausa + tail facts + conformed-fact closures + ADR-013 promotion

Closes the ADR-011 staged Kimball migration. Every fact mart in the warehouse
reaches Kimball completeness:
- Tracking subsystem (fct_tracking_frames + 5 derivatives) gains match_key + team_key + player_key
- Formations (fct_formation_labels) gains match_key + team_key
- Pausa: fct_pausa_values promoted to dbt mart with contract: enforced: true
  (ADR-013 second application after PR 3 xG v2); fct_pausa_rankings + fct_pass_timing
  gain Kimball keys
- Off-ball / space marts (fct_off_ball_xt, fct_space_creation) gain match_key + player_key
- Tail facts (fct_discipline_events, fct_physical_stats) gain Kimball keys
- Conformed-fact closures (Q3 best-practice): fct_passes, fct_shots, fct_action_values,
  fct_match_summary, fct_line_breaking_results gain team_key + player_key
- Aggregates (fct_heatmap_agg, fct_vaep_breakdown_agg) gain team_key + player_key
- Pull-through extensions: fct_xg_predictions + _v2 surface team_key + player_key from fct_shots
- fct_player_percentiles physical_by_comp dim_matches bridge retired
- pitch_control_batch.py writer emits data_source + match_key natively;
  stg_pitch_control__values prefix-CASE bridge collapsed to passthrough
- Staging-level team_id derivation in stg_metrica__tracking + stg_skillcorner__tracking
  + stg_formations__labels + stg_shape_graphs__positions
- HF artifact parity: 13 dataset cards + 6 model cards updated; 13 dataset payloads
  republished in Phase 2 (8 PR-7-affected + 5 PR-5b absorbed deferrals)
- 4 new tests + extensions to 5 existing
- ADR-011 §Notes: PR 7 staged-rollout row updated; ADR-013 §Notes second-application confirmed

PR 8 (locked cleanup, ~2026-07-22) drops legacy *_id INT columns + sunsets
canonical_player_id + sunsets HF dataset legacy-column payloads.

Spec: docs/superpowers/specs/2026-04-27-kimball-pr7-tracking-formations-pausa-design.md
Plan: docs/superpowers/plans/2026-04-27-kimball-pr7-tracking-formations-pausa.md (local-only)

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
git push -u origin kimball-pr7-tracking-formations-pausa
gh pr create --title "feat(kimball-pr7): close ADR-011 migration — tracking + formations + pausa + tail + conformed-fact closures" --body "$(cat <<'EOF'
## Summary
- Closes ADR-011 staged Kimball migration: every fact mart reaches Kimball completeness
- ADR-013 second application: fct_pausa_values promoted to dbt mart with contract: enforced: true
- Conformed-fact closures (Q3): fct_passes, fct_shots, fct_action_values, fct_match_summary, fct_line_breaking_results gain team_key + player_key
- Staging-level team_id derivation across 3 tracking staging models
- pitch_control_batch.py writer emits data_source + match_key natively; staging bridge collapsed
- HF artifact parity: 13 dataset cards + 6 model cards + 13 payload republishes (8 PR-7-affected + 5 PR-5b absorbed)
- 4 new tests + extensions to 5 existing

## Scope (~21 marts modified + 1 new)
Tracking subsystem, formations, pausa, tail facts, off-ball/space, conformed-fact closures, aggregates, pull-through extensions, bridge retirement.

## Test plan
- [ ] Local: ruff, pyright, dbt parse, pytest excluding live tests
- [ ] CI gates: validate / semgrep / lint-and-test / live-build all green; triage cascade per Path X authority
- [ ] Post-merge: wheel deploy + wf-obso-pausa re-run + dbt --full-refresh + Lakebase PK recreate (fct_pausa_values_synced) + refresh_synced_tables + maintain_synced_tables
- [ ] Live invariant tests: test_marts_kimball_contracts.py + completion + ADR-013 + writer parity all green
- [ ] HF: 8 dataset republishes (PR-7-affected) + 5 absorbed PR-5b + 6 model card pushes + 13 dataset card pushes
- [ ] Smoke check: fct_passes team_key + passer_player_key non-NULL; fct_pausa_values 3-key INNER JOIN inheritance

## Risks
- 12+ marts surrogate-hash break → mandatory --full-refresh post-merge
- Live-CI cascade likely surfaces 4-6 latent bugs (PR 4b/5b/6 precedent, scaled)
- Lakebase synced-table PK recreation needed for fct_pausa_values_synced (string-keyed → BIGINT-keyed grain)
- HF Hub rate-limit on 13 republishes — sequential with waits

🤖 Generated with [Claude Code](https://claude.com/claude-code)
EOF
)"
```

---

## Phase 1 — CI green + merge approval

### Task 19: Watch CI + triage

- [ ] **Step 19.1: Watch CI**

```bash
gh pr checks --watch
```

Expected: validate / semgrep / lint-and-test green within ~5-10 min; live-build within ~15-20 min.

- [ ] **Step 19.2: Triage live-CI cascade**

If failures — apply Path X authority per `reference_live_ci_surfaces_latent_bugs`:
- Compile errors in PR 7 scope: fix in PR.
- Compile errors in adjacent unmigrated marts: fix in PR (cheap).
- Data-test failures outside PR scope: warn-severity flip with YAML pointer to closing PR (in this case, all PRs are closed — no follow-up to point to; investigate as actual bugs).

Pre-existing CI blockers folded into 2nd commit on same branch — squash-merge collapses to one commit on `main`.

### Task 20: User merge approval (USER GIT GATE)

- [ ] **Step 20.1: PAUSE — request user approval to merge**

After all CI green, surface PR URL + summary to user; ask for explicit `gh pr merge` approval. Do NOT auto-merge.

- [ ] **Step 20.2: After approval — squash-merge**

```bash
gh pr merge <PR#> --squash --delete-branch=false
```

(Branch deletion in Phase 4 — separate gate.)

---

## Phase 2 — Post-merge dev deploy (autonomous per `feedback_only_git_gates_need_approval`)

### Task 21: Wheel deploy verification

- [ ] **Step 21.1: Verify wheel 0.3.18 → 0.3.19 deployed via Python CI**

```bash
gh run list --workflow=python-ci.yml --branch=main --limit=3
```

Expected: latest run on main green; `bump_wheel.py` step shows 0.3.19 deployed to UC Volume.

If wheel didn't bump, run `python scripts/bump_wheel.py` (verify rule applies — wheel changes only if `src/` content changed, which it did for pausa.py + pitch_control_batch.py).

### Task 22: Trigger wf-obso-pausa to populate bronze.pausa_values

- [ ] **Step 22.1: Trigger workflow**

```bash
uv run python scripts/trigger_dbt_job.py --workflow wf-obso-pausa
```

(Verify exact script path/name.) Expected: ~$14, 7 IDSSE matches; populates `bronze.pausa_values`.

- [ ] **Step 22.2: Verify bronze.pausa_values populated**

```bash
uv run python -c "
import os
from databricks import sql
host = os.environ['DATABRICKS_HOST'].replace('https://','').rstrip('/')
with sql.connect(server_hostname=host, http_path=os.environ['DATABRICKS_HTTP_PATH'], access_token=os.environ['DATABRICKS_TOKEN']) as c:
    cur = c.cursor()
    cur.execute('SELECT COUNT(*) FROM soccer_analytics.bronze.pausa_values')
    print(cur.fetchone())
"
```

Expected: row count > 0.

### Task 23: dbt run --full-refresh

- [ ] **Step 23.1: Trigger live-build dbt job for full PR-7 graph**

Use the existing live-build trigger pattern (Databricks serverless dbt job); pass `--select <PR-7 marts>+ --full-refresh` selectors.

```bash
uv run python scripts/trigger_dbt_job.py --select '+fct_pausa_values fct_passes+ fct_shots+ fct_action_values+ fct_match_summary+ fct_line_breaking_results+ fct_tracking_frames+ fct_formation_labels+ fct_player_positions+ fct_position_maps+ fct_physical_stats+ fct_off_ball_xt+ fct_space_creation+ fct_discipline_events+ fct_pass_timing+ fct_pausa_rankings+ fct_heatmap_agg fct_vaep_breakdown_agg fct_xg_predictions fct_xg_predictions_v2 fct_player_percentiles' --full-refresh
```

Expected: all marts rebuild; WARN ≤ acceptable, ERROR=0.

### Task 24: Lakebase synced-table PK recreation (fct_pausa_values)

- [ ] **Step 24.1: Apply Terraform**

```bash
cd terraform && terraform apply -target=module.fct_pausa_values_synced
```

If the existing resource has a different PG-PK grain, this will fail — manual delete from Lakebase UI first, then `terraform apply`. Per `reference_synced_table_pk_recreation`.

- [ ] **Step 24.2: Audit other PR-7 marts for PG-PK changes**

For each mart whose surrogate-hash inputs changed (per spec §4.5), verify if PG-PK grain changes. If yes, repeat the manual-delete + terraform apply cycle per-mart.

### Task 25: refresh_synced_tables.py + maintain_synced_tables.py

- [ ] **Step 25.1: Refresh additive-evolve marts**

```bash
uv run python scripts/refresh_synced_tables.py --tables \
  fct_tracking_frames_synced fct_tracking_avg_positions_synced \
  fct_tracking_shape_timeline_synced fct_player_positions_synced \
  fct_position_maps_synced fct_formation_labels_synced \
  fct_physical_stats_synced fct_pausa_rankings_synced \
  fct_pass_timing_synced fct_off_ball_xt_synced \
  fct_space_creation_synced fct_discipline_events_synced \
  fct_passes_synced fct_shots_synced fct_action_values_synced \
  fct_match_summary_synced fct_line_breaking_results_synced \
  fct_heatmap_agg_synced fct_vaep_breakdown_agg_synced \
  fct_xg_predictions_synced fct_xg_predictions_v2_synced \
  fct_player_percentiles_synced \
  --wait
```

(Verify exact synced-table names exist in `SYNCED_TABLES` registry per Task 12.B.)

- [ ] **Step 25.2: Apply grants + indexes**

```bash
uv run python scripts/maintain_synced_tables.py --skip-refresh
```

Expected: Steps 0.5 (grants) + 2 (indexes) + 3 (verify) green.

### Task 26: Live-invariant tests

- [ ] **Step 26.1: Run all live tests**

```bash
uv run --with databricks-sql-connector pytest \
  src/tests/test_marts_kimball_contracts.py \
  src/tests/test_marts_kimball_completion.py \
  src/tests/test_pausa_adr013_compliance.py \
  src/tests/test_pausa_writer_parity.py \
  src/tests/test_pitch_control_writer_parity.py \
  src/tests/test_marts_live_schema.py \
  src/tests/test_bronze_live_schema.py \
  src/tests/test_dbt_passes_kimball_migration.py \
  -v
```

Expected: all green at calibrated thresholds.

- [ ] **Step 26.2: Calibrate test_marts_kimball_contracts.py thresholds + commit follow-up**

If thresholds locked at 0.0 placeholder show actual coverage values, surface measurements to user; tighten thresholds in 2nd commit on same branch (squash collapses to one). Per `feedback_evidence_before_claim` — measure post-rebuild data, never against pre-fix landmines (PR 6 #4 lesson).

### Task 27: HF dataset payload republishes

- [ ] **Step 27.1: PR-7-affected datasets (8)**

Run each publish script with the PR 7 mart updates surfaced. **At impl time**, READ each script first to determine if SELECT needs updating to surface new BIGINT key columns. If yes, update the script before running.

```bash
# obso-pausa-values
uv run python notebooks/publish_obso_data.py
# pitch-control-tracking + space-creation-values + line-breaking-passes (verify scope)
uv run python notebooks/publish_datasets.py
# spadl-vaep
uv run python scripts/publish_spadl_vaep_hf.py
# xg-shot-data + statsbomb-shots-on-target
uv run python scripts/publish_xg_shots_hf.py
# xg-freeze-frame-data
uv run python scripts/publish_freeze_frame_hf.py
```

Expected per publish: HF Hub commit URL logged; dataset row count > 0 post-publish.

- [ ] **Step 27.2: PR-5b absorbed datasets (5)**

Verify scripts at impl time. PR 5b's plan task 12-16 named the cards but not the publish scripts — read PR 5b plan + memory `project_kimball_pr5b_shipped.md` to identify the export scripts. Likely candidates:
- `src/ingestion/export_embeddings_dataset.py` or similar
- A football2vec-specific publisher

Run each. Same expected output as 27.1.

### Task 28: HF model card pushes

- [ ] **Step 28.1: Push 6 model cards**

```bash
uv run python scripts/publish_hf_cards.py --kind model --name off-ball-xt.md
uv run python scripts/publish_hf_cards.py --kind model --name space-creation.md
uv run python scripts/publish_hf_cards.py --kind model --name pitch-control.md
uv run python scripts/publish_hf_cards.py --kind model --name obso-pausa.md
uv run python scripts/publish_hf_cards.py --kind model --name xg.md
# defcon.md only if Task 14.F edited it
```

Expected per push: HF Hub commit URL logged.

### Task 29: HF dataset card pushes

- [ ] **Step 29.1: Push 13 dataset cards**

```bash
# 8 PR-7-affected
uv run python scripts/publish_hf_cards.py --kind dataset --name obso-pausa-values.md
uv run python scripts/publish_hf_cards.py --kind dataset --name pitch-control-tracking.md
uv run python scripts/publish_hf_cards.py --kind dataset --name space-creation-values.md
uv run python scripts/publish_hf_cards.py --kind dataset --name line-breaking-passes.md
uv run python scripts/publish_hf_cards.py --kind dataset --name spadl-vaep-action-values.md
uv run python scripts/publish_hf_cards.py --kind dataset --name statsbomb-shots-on-target.md
uv run python scripts/publish_hf_cards.py --kind dataset --name xg-shot-data.md
uv run python scripts/publish_hf_cards.py --kind dataset --name xg-freeze-frame-data.md

# 5 PR-5b absorbed
uv run python scripts/publish_hf_cards.py --kind dataset --name football2vec-player-embeddings.md
uv run python scripts/publish_hf_cards.py --kind dataset --name football2vec-360-embeddings.md
uv run python scripts/publish_hf_cards.py --kind dataset --name football2vec-training-data.md
uv run python scripts/publish_hf_cards.py --kind dataset --name football2vec-360-training-data.md
uv run python scripts/publish_hf_cards.py --kind dataset --name football2vec-statsbomb-wyscout.md
```

Expected per push: HF Hub commit URL logged.

### Task 30: Phase 2 verification smoke checks

- [ ] **Step 30.1: fct_passes team_key + player_key non-NULL**

```sql
SELECT
  COUNT(*) total,
  COUNT(team_key) team_key_set,
  COUNT(passer_player_key) passer_player_key_set,
  COUNT(recipient_player_key) recipient_player_key_set
FROM soccer_analytics.dev_gold.fct_passes
```

Expected: team_key + passer_player_key ≥ 99% non-NULL.

- [ ] **Step 30.2: fct_pausa_values 3-key inheritance from fct_passes**

```sql
SELECT
  COUNT(*) total,
  COUNT(match_key) mk_set,
  COUNT(passer_team_key) ptk_set,
  COUNT(passer_player_key) ppk_set
FROM soccer_analytics.dev_gold.fct_pausa_values
```

Expected: 100% non-NULL on all 3 keys (INNER JOIN to fct_passes guarantees).

- [ ] **Step 30.3: pitch-control bronze schema confirmed widened**

```sql
DESCRIBE TABLE soccer_analytics.bronze.pitch_control_values
```

Expected: includes `data_source STRING` + `match_key BIGINT` columns.

---

## Phase 3 — Documentation + memory

### Task 31: Memory updates

- [ ] **Step 31.1: Create `project_kimball_pr7_shipped.md`**

Mirror `project_kimball_pr6_shipped.md` shape. Capture: cycle close date + commit hash, delivered scope, key coverage numbers from Phase 2 §30 verification, follow-up list (none expected — PR 8 is the locked cleanup), don't-re-run list.

- [ ] **Step 31.2: Update `project_kimball_migration_cycle.md`**

Mark PR 7 row SHIPPED with commit hash + date; PR 8 row remains LOCKED cleanup; remove the "PR 7 next" line.

- [ ] **Step 31.3: Update `MEMORY.md` index entry for PR 7**

Add:
```
- [Kimball PR 7 — SHIPPED](project_kimball_pr7_shipped.md) — ADR-011 migration close-out: every fact mart reaches Kimball completeness; ADR-013 second application (fct_pausa_values); 13 HF datasets republished; PR 8 = cleanup only
```

### Task 32: ADR + spec finalization

- [ ] **Step 32.1: ADR-011 staged-rollout table — PR 7 row → Shipped (date, hash)**

Replace "Planned" or "In progress" with `Shipped (2026-04-XX, <commit_hash>)`.

- [ ] **Step 32.2: ADR-013 §Notes — second-application confirmed**

Update from "PR 7 (planned)" to `PR 7 (Shipped 2026-04-XX, <commit_hash>): fct_pausa_values promotion to dbt mart`.

---

## Phase 4 — Branch cleanup (USER GIT GATE)

### Task 33: Branch deletion

- [ ] **Step 33.1: PAUSE for user approval**

Per `feedback_only_git_gates_need_approval`. Surface to user; ask explicit approval for `git branch -d`.

- [ ] **Step 33.2: After approval — delete branch**

```bash
git branch -d kimball-pr7-tracking-formations-pausa
git push origin --delete kimball-pr7-tracking-formations-pausa
```

---

## Self-Review Checklist (before Task 18)

- [ ] **Spec coverage:** every section of `2026-04-27-kimball-pr7-tracking-formations-pausa-design.md` maps to tasks above:
  - §3.1 Mart Kimball completion (21 + 1 new) → Tasks 2-9 + 13.A
  - §3.2 Staging team_id derivation → Task 1
  - §3.3 Writer-layer schema reconciliation → Task 3.A + Task 10
  - §3.4 Synced-table grant + PG-PK changes → Task 12 + Task 24
  - §3.5 HF artifact parity → Tasks 14, 15, 27, 28, 29
  - §4 Data model → Tasks 2-10 implementation specifics
  - §5 Edge cases → Implementation-time handling per task
  - §6 Testing → Task 13 (4 new + 4 extensions)
  - §7 Ship criteria → Tasks 17 (pre-merge) + 21-30 (post-merge)
  - §8 Risks → Mitigations distributed across tasks
  - §9 Rollout → Phase 0-4 task organization mirrors spec phases
  - §10 Open implementation-time verifications → Task 0.2-0.6 + per-task VERIFY notes
  - §11 Related references → Plan references throughout
- [ ] **Placeholders:** `0.0` thresholds in Task 13.A are intentional placeholders calibrated post-rebuild per Task 26.2; `defcon.md verify` in Task 14.F flagged. No "TBD" / "TODO" / "fill in details" elsewhere.
- [ ] **Type consistency:** `match_key`, `team_key`, `player_key`, `passer_team_key`, `passer_player_key`, `recipient_player_key`, `home_team_key`, `away_team_key`, `opponent_team_key`, `pausa_ranking_id`, `pass_timing_id` — all BIGINT or STRING (surrogate hashes) per their corresponding dim or surrogate. Names consistent across all task references.
- [ ] **Hyrum's Law:** legacy columns (`match_id`, `team_id`, `player_id`, `canonical_player_id`) preserved verbatim everywhere. PR 8 owns the drops.
- [ ] **DELTA_MULTIPLE_SOURCE_ROW_MATCHING guard:** all incremental marts gain `on_schema_change='append_new_columns'` (Task 17.6 audit + per-task config updates). First build via `--full-refresh` per Task 23.1 selectors.
- [ ] **No commits without approval:** Task 18 explicitly pauses for user before commit + push + PR. Task 20 explicitly pauses before merge. Task 33 explicitly pauses before branch delete.
- [ ] **Single commit per branch:** Task 18.3 is the only commit. Pre-existing CI blockers may add a 2nd commit (Task 19.2) but squash-merge collapses to one on main per `feedback_single_commit_squash`.

---

**End of plan.**
