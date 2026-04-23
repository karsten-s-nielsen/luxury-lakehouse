# Kimball PR 4 — Action Values migration + PR 3 deferrals — design spec

| | |
|---|---|
| **Date** | 2026-04-23 |
| **Branches** | `kimball-pr4-live-ci`, `kimball-pr4-action-values`, `kimball-pr4-hf-readme` (three sub-PRs; each cut from main after the previous merges) |
| **Author** | Karsten Nielsen (with Claude Opus 4.7) |
| **Status** | Draft — awaiting user review |
| **Supersedes** | — |
| **Related** | ADR-011 (Kimball surrogate keys); ADR-012 (producer-side weight delivery — informational, not extended by this PR); ADR-005 (Lakebase synced-table grants); ADR-002 (silent-exception policy); `docs/superpowers/specs/2026-04-22-kimball-pr3-shots-xg-design.md` (PR 3 reference — this PR reuses the try_cast pushdown pattern and the dual-column HF deprecation pattern established there) |

## 1. Goal

Complete PR 4 of the ADR-011 staged Kimball migration: move `fct_action_values` (9.53M rows; the base-case mart from PR #134) off smart-keyed `match_id`/`competition_id` and onto the surrogate `match_key`/`competition_key` FKs, along with its Taipy consumer (Player Impact page queries in `queries/defensive.py`) and its HF dataset (`luxury-lakehouse/spadl-vaep-action-values` at 82 monthly downloads — the 90-day dual-column window applies).

In the same cycle, close two PR 3 deferrals:

- **Live dbt run in CI.** The current `.github/workflows/dbt-ci.yml` runs `dbt parse` only, because the Databricks Thrift endpoint is unreachable from public GH Actions runners (documented in the workflow file at lines 78–87, investigated 2026-04-03). PR 4a introduces a new workflow that triggers a Databricks one-shot Job via the REST API (OIDC-authenticated, the same auth pattern proven by `terraform-apply.yml`), which runs `dbt build` inside Databricks where Thrift works. This closes the specific gap where PR 3's `try_cast` bugs merged undetected because dbt never ran in CI.
- **HF dataset README auto-upload.** Per-dataset READMEs currently drift — they live ad-hoc on HF Hub with no in-repo source of truth. PR 4c introduces `src/ingestion/hf_publish.py` with a `upload_hf_readme(...)` helper; every `publish_*_hf.py` invokes it, and `scripts/publish_hf_org_card.py` is refactored to share the same helper (repo_type="space").

This PR also:

- Folds a one-line try_cast fix into PR 4b (Finding D — `publish_xg_shots_hf.py:99` uses `CAST` instead of `try_cast`; dormant today because `fct_shots` is StatsBomb/Wyscout-only, but latent against any future cross-provider widening of `fct_shots`).
- Ships a G1 `wait_until_online(...)` helper in `src/ingestion/refresh_synced_tables.py` using the `detailed_state` poll pattern; unused in PR 4 itself, ready for the future SDK-based synced-table recreation path.
- Adds an explicit On-Deck entry for G2 + G3 (the remaining synced-table SDK-path gaps — legacy REST endpoint in the refresh module, grants + event_log ownership verification after SDK-create) deferred with user approval.

ADR-013 (consumer-side inference-output contract) does **not** apply to this PR: Action Values is pure SPADL arithmetic, not ML inference output — there is no prediction-table shape.

## 2. Scope

### In scope (across all three sub-PRs)

**PR 4a — Live dbt CI.** New files only; existing `dbt-ci.yml` unchanged.

- `.github/workflows/dbt-live-ci.yml` — new `pull_request` workflow. Computes the `dbt --select` argument (`state:modified+` default; fallback to `+all` when `dbt_project.yml` / `packages.yml` / `profiles.yml` diff against main); submits a Databricks one-shot run via `/api/2.0/jobs/runs/submit`; polls to terminal state; on failure, posts a PR comment; exits with run state. `permissions: pull-requests: write` added.
- `scripts/trigger_dbt_job.py` — submit + poll helper. Embeds the Databricks job spec directly in the runs_submit payload (no permanent Job resource in Terraform). Returns `(state, run_id, run_page_url, result_state)`.
- `scripts/post_dbt_failure_comment.py` — fetches run output via `/api/2.0/jobs/runs/get-output`, parses `target/run_results.json` for failing models/tests, posts a GH PR comment via `$GITHUB_TOKEN`. Comment format: failing model/test names + first ~15 lines of error + link to Databricks run page.
- `src/tests/test_trigger_dbt_job.py` — unit tests (requests mocked).
- `src/tests/test_post_dbt_failure_comment.py` — unit tests (requests + GH API mocked).

**PR 4b — Action Values migration + G1 + Finding D.**

- `dbt_project/models/marts/fct_action_values.sql`:
  - Add `match_key` BIGINT NOT NULL via `LEFT JOIN dim_matches dm ON stg.match_id = dm.native_match_id AND stg.data_source = dm.data_source` (or the equivalent ADR-011 surrogate-resolution pattern used in PR 2).
  - Add `competition_key` BIGINT nullable via `LEFT JOIN dim_competitions`.
  - Retain legacy `match_id` BIGINT nullable (via `try_cast(dm.native_match_id as bigint)`) and legacy `competition_id` nullable (via `try_cast(dc.native_competition_id as bigint)` or equivalent) for the 90-day dual-column window.
  - Flip `liquid_clustered_by` from `['match_id']` to `['match_key']`.
  - Update incremental predicate from `where match_id not in (select distinct match_id from {{ this }})` to `where match_key not in (select distinct match_key from {{ this }})`.
- `dbt_project/models/marts/_marts__models.yml` (or the file that currently holds the `fct_action_values` contract) — update `columns:` to include `match_key`, `competition_key`, plus retained legacy columns. Contract stays `enforced: true`.
- `scripts/publish_spadl_vaep_hf.py`:
  - Rewrite `_ACTION_VALUES_SQL` to `LEFT JOIN dim_matches` + `LEFT JOIN dim_competitions`; emit both new (`match_key`, `competition_key`) and legacy (`match_id` via try_cast, `competition_id` via try_cast) columns.
  - Update `normalize_dtypes` to include `match_key` + `competition_key` (Int64 like other ID columns).
  - Log the 2026-07-22 sunset date in the summary line.
- `scripts/publish_xg_shots_hf.py:99` — Finding D: `CAST(dm.native_match_id AS BIGINT)` → `try_cast(dm.native_match_id as bigint)`. Update the comment immediately above to reflect try_cast semantics (NULL on unparseable, per `reference_try_cast_spark_pushdown.md` pattern).
- `hf_taipy_app/src/queries/defensive.py`:
  - `fetch_vaep_rankings`, `fetch_vaep_breakdown`, `fetch_vaep_timeline` — swap match_id filters to match_key; add `dim_matches_synced` joins where needed. `dim_matches_synced` is already in `refresh_synced_tables.py::SYNCED_TABLES` so the same-database join works.
- `hf_taipy_app/src/state/action_values.py` — call-site updates only (e.g., `get_match_id(...)` → `get_match_key(...)` if `state.shared` already exposes the latter; otherwise a matching helper lands in `state/shared.py` under PR 4b's scope). Declarative PageConfig stays unchanged (UX out of scope per Q10 decision).
- `src/ingestion/refresh_synced_tables.py` — G1: add `wait_until_online(table_fqn: str, timeout_s: int = 600) -> None` helper using `GET /api/2.0/database/synced_tables/<fqn>` polled at 15s intervals, returning when `status.detailed_state == "SYNCED_TABLE_ONLINE_NO_PENDING_UPDATE"`. Raises `TimeoutError` with the table FQN + last-seen state + elapsed time on timeout; `RuntimeError` on known-bad terminal states; propagates `HTTPError` on 404. Not called anywhere in PR 4.
- `src/tests/test_fct_action_values_contract.py` (new) — parses the dbt YAML contract and asserts column set + types for `fct_action_values`.
- `src/tests/test_refresh_synced_tables.py` — new test class for G1 helper (requests mocked).
- `src/tests/test_marts_live_schema.py` (or the equivalent file from PR 1.8 / drop-safety sweep) — add live-DESCRIBE test for `fct_action_values` matching the YAML contract.
- `scripts/publish_xg_shots_hf.py` (also) — new unit test asserting `try_cast` substring present in `_SHOTS_SQL` (regression guard for Finding D).
- On-Deck entry (location to verify during implementation — repo's current TODO file) for G2 + G3 with explicit blocking condition: "any future PR that switches synced-table creation to `w.postgres.synced_tables.*` SDK path must close these before shipping."

**PR 4c — HF README helper + dataset cards + org-card refactor.**

- `src/ingestion/hf_publish.py` (new): `upload_hf_readme(repo_id: str, readme_path: Path, hf_token: str, repo_type: Literal["dataset", "space"] = "dataset") -> dict`. Validates file exists + non-empty + HF-identifier-safe `repo_id`; LF-normalizes bytes; calls `HfApi.upload_file(path_or_fileobj=..., path_in_repo="README.md", repo_type=repo_type, commit_message="Update README (generated)")`; returns `{"commit_url": ..., "sha256": ...}`.
- `docs/huggingface/dataset-cards/spadl-vaep-action-values.md` (new) — description + 90-day dual-column sunset block (sunset 2026-07-22, aligned with statsbomb-shots-on-target per PR 3 memory).
- `docs/huggingface/dataset-cards/xg-shot-data.md` (new) — description + existing PR 3 dual-column warning pattern (uniform-rename precedent). Brings the in-repo copy into line with what currently lives on HF.
- `docs/huggingface/dataset-cards/statsbomb-shots-on-target.md` (new) — description + existing dual-column warning (sunset 2026-07-22, matching the content already pushed during PR 3).
- `docs/huggingface/dataset-cards/xg-freeze-frame-data.md` (new) — description only (no dual-column — dataset is untouched by the Kimball migration).
- `docs/huggingface/org-card.md` — refresh references to the 4 datasets: any stale schema mentions, add sunset references for the two dual-column windows, ensure dataset link list matches reality.
- `scripts/publish_spadl_vaep_hf.py`, `scripts/publish_xg_shots_hf.py`, `scripts/publish_freeze_frame_hf.py` — one new call at end of `main()` each: `upload_hf_readme(DATASET_REPO, Path("docs/huggingface/dataset-cards/<name>.md"), hf_token)`. Logs the commit URL.
- `scripts/publish_hf_org_card.py` — refactored to call `upload_hf_readme(..., repo_type="space", ...)` instead of its current ad-hoc `HfApi.upload_file` call. Behavior unchanged.
- `src/tests/test_hf_publish.py` (new) — unit tests covering: missing file, empty file, valid dataset upload, valid space upload, CRLF normalization, invalid repo_id pattern.

### Explicitly out of scope (all deferrals have explicit user approval)

- **G2.** `src/ingestion/refresh_synced_tables.py:178` hits `/api/2.0/database/synced_tables/` (legacy REST endpoint). SDK-created synced tables (via `w.postgres.synced_tables.*`) live under `/api/2.0/postgres/synced_tables/`. An SDK-created table is not addressable by the current refresh module. Confirmed real via code inspection during brainstorming. Deferred per Q9=(d). On-Deck entry in PR 4b.
- **G3.** Verification of `run_lakebase_grants.py` + `fix_event_log_ownership.py` against SDK-created tables — unverified whether ADR-005 grants flow and event_log ownership semantics hold on the new creation path. Deferred per Q9=(d). On-Deck entry in PR 4b.
- **Broader Action Values page UX cleanup.** Q10=(i) locks in query-only update. Any scale-label / help-text / glossary / layout work on the Player Impact page is a separate targeted cycle.
- **`fct_action_values` promotion to ADR-013 pattern.** Not applicable — Action Values is SPADL arithmetic (no ML inference output shape). ADR-013 applies only to prediction-table producers (xG, PAUSA, etc.).
- **`dim_teams` / `dim_players` Kimball migration.** team_id and player_id stay as INT across `fct_action_values` and its Taipy consumers. Targeted for PR 5.
- **Other `fct_*` Kimball migrations.** Player stats + embeddings (PR 5), defensive + goalkeeper (PR 6), tracking + formations + pausa (PR 7), cleanup (PR 8). All covered by ADR-011 rollout table; not touched here.
- **Bronze `action_values` schema change.** silly-kicks continues writing `match_id` to `bronze.action_values`. Bronze is provenance (ADR-011 — bronze stays on native IDs; Kimball resolution happens at the dbt gold layer via dim joins).
- **Wheel version bumps are scoped.** PR 4a and PR 4b do NOT bump the wheel (no wheel-shipped module changes). PR 4c DOES bump the wheel (0.3.13 → 0.3.14) because the `docs/huggingface/dataset-cards/` directory is force-included in `pyproject.toml` so `src/ingestion/export_shots_on_target.py` (a Databricks-workflow-installed writer) can read its dataset card at runtime. Decision made during plan-writing 2026-04-23 after discovering the wheel-vs-repo path-resolution problem; see plan `2026-04-23-kimball-pr4-hf-readme.md` D6 for the alternatives considered (separate one-shot script; package relocation; UC Volume upload — all rejected in favor of wheel bundling, which matches the existing precedent of shipping `dbt_project/dbt_packages/` in the wheel).

## 3. Approved design decisions

Resolved during brainstorming on 2026-04-23 (this session):

| # | Decision | Choice | Brainstorm Q |
|---|---|---|---|
| 1 | Sub-PR sequencing | **B** — Live dbt CI first (PR 4a) → Action Values migration (PR 4b) → HF README helper (PR 4c). Migration ships on the live-CI safety net it depends on; no rush, user-selected. | Q1 |
| 2 | Live dbt CI execution mechanism | **(b) GH Actions triggers a Databricks Job that runs dbt.** OIDC auth per `terraform-apply.yml` pattern; Thrift-from-public-runners constraint worked around by running dbt inside Databricks. | Q2 |
| 3 | Live dbt CI cadence | **(i) PR only.** Pre-merge gate closes the specific gap that motivated this item (try_cast-style bugs merging undetected). Main-push leg not added — low PR volume means merge races are rare. | Q3 |
| 4 | Live dbt CI model subset | **(ii) `dbt build --select state:modified+`** with fallback to `+all` when `dbt_project.yml` / `packages.yml` / `profiles.yml` diff against main. Manifest-baseline infrastructure already plumbed in `dbt-ci.yml:65–76`. | Q4 |
| 5 | Live dbt CI failure surfacing | **(ii) Block merge + PR comment summary.** Actionable feedback inline; required check blocks merge regardless of comment API outcome. | Q5 |
| 6 | HF README helper module placement | **(b) New `src/ingestion/hf_publish.py`.** Peer to `artifact_deploy.py`; separates dataset-README delivery from ML-weights delivery. | Q6 |
| 7 | README markdown source location | **(a) `docs/huggingface/dataset-cards/{repo-name}.md`.** Matches existing `docs/huggingface/org-card.md` + `docs/huggingface/model-cards/` convention. | Q7 |
| 8 | HF dataset schema policy for `spadl-vaep-action-values` | **(b) 90-day dual-column window, sunset 2026-07-22** (aligned with `statsbomb-shots-on-target`). 82 monthly downloads + 0 likes → conservative policy protects unknown external consumers; aligned sunset date lets PR 8 do one cutover sweep. | Q8 |
| 9 | Synced-table gap-closing scope | **(d) G1 in PR 4; G2 + G3 deferred with explicit On-Deck entry.** User-approved deferral. | Q9 |
| 10 | Taipy Action Values page scope | **(i) Query-only update.** No UX cleanup in this cycle. | Q10 |
| 11 | Finding D (try_cast in `publish_xg_shots_hf.py`) | **Folded into PR 4b.** One-line fix rides with the try_cast-pattern work. | — |
| 12 | Org-card update (PR 4c addition) | **(β) Extend `hf_publish.py` to cover the org Space too**; refactor `publish_hf_org_card.py` to use the helper. Unifies the HF-publish pattern. | — |

## 4. Architecture principle

PR 4 does **not** introduce a new ADR. The migration follows ADR-011 established in PR 1; the live dbt CI pattern is operational infrastructure that doesn't warrant an architectural record (no cross-cutting dependency change, no grant/ownership shift, no workaround encoding); the HF README helper is a utility module.

One architectural note worth recording in the plan (not the spec): the `(a) GH Actions → Databricks Job` pattern introduced by PR 4a becomes the canonical way to run Databricks-dependent work from CI. Future live checks (Taipy E2E against dev Lakebase, dbt data tests against fresh gold) can reuse the same trigger + poll + PR-comment pattern. If a second consumer of the pattern materializes, an ADR may be warranted to formalize the convention.

## 5. Live dbt CI design (PR 4a detail)

### 5.1 `dbt-live-ci.yml` flow

```
pull_request event
  ↓
checkout PR head + fetch main
  ↓
uv sync --frozen --extra dbt --no-install-project
dbt deps (PR) + dbt parse (PR) → target/manifest.json
checkout main → dbt deps + dbt parse → target-main/manifest.json
  ↓
diff dbt_project.yml / packages.yml / profiles.yml (PR vs main)
  ↓
if any diff → --select="+all"
else        → --select="state:modified+ --state=target-main/"
  ↓
scripts/trigger_dbt_job.py --select "<arg>"
  ↓ POST /api/2.0/jobs/runs/submit with OIDC-authed one-shot spec
  ↓ poll /api/2.0/jobs/runs/get every 15s (max 30min)
  ↓
result_state: SUCCESS → exit 0 (check passes, merge unblocked)
result_state: FAILED / CANCELED / INTERNAL_ERROR
  ↓
scripts/post_dbt_failure_comment.py --run-id <id>
  ↓ GET /api/2.0/jobs/runs/get-output → parse run_results.json
  ↓ POST /repos/.../issues/<pr>/comments via $GITHUB_TOKEN
  ↓
exit 1 (check fails, merge blocked)
```

### 5.2 Databricks one-shot Job spec (embedded in runs_submit payload)

```json
{
  "run_name": "dbt-live-ci (PR #<pr_number>, sha <short_sha>)",
  "timeout_seconds": 1800,
  "tasks": [
    {
      "task_key": "dbt_build",
      "spark_python_task": {
        "python_file": "dbfs:/.../ci/run_dbt_build.py",
        "parameters": ["--select", "<arg>", "--target", "dev"]
      },
      "new_cluster": { ... }
    }
  ]
}
```

**Open item for plan stage:** whether the one-shot uses `spark_python_task` with a DBFS-uploaded runner, a notebook task, or a `python_wheel_task` against the published wheel. Cleanest option is likely `python_wheel_task` invoking a new `ci.run_dbt_build` entry point, but this needs verification against how the daily Databricks job currently invokes dbt (`scripts/dbt_build_and_refresh.py` uses a warehouse SQL path locally; the CI path may want something different).

### 5.3 OIDC auth from GH Actions

Pattern from `.github/workflows/terraform-apply.yml` (lines 14–23, 29–42):

```yaml
permissions:
  id-token: write
env:
  DATABRICKS_HOST: ${{ vars.DATABRICKS_HOST }}
  DATABRICKS_CLIENT_ID: ${{ vars.DATABRICKS_CLIENT_ID }}
  DATABRICKS_AUTH_TYPE: github-oidc
```

No static `DATABRICKS_TOKEN` needed — the Databricks SDK's `databricks.sdk.WorkspaceClient()` picks up OIDC automatically.

### 5.4 PR comment format

```markdown
### ❌ dbt-live-ci failed

**Failing models/tests:**
- `fct_foo.sql` — compilation error
- `test_bar.sql` — data test failure (3 failing rows)

**Error excerpt:**
```
Runtime Error in model fct_foo
  [TABLE_OR_VIEW_NOT_FOUND] The table or view `soccer_analytics.dev_gold.dim_bar` cannot be found
  ... (see run log for full trace)
```

[Databricks run log →](https://...)
```

(Emoji in PR comment is part of the rendered output readability; not a CLAUDE.md violation — the spec file itself stays emoji-free.)

### 5.5 Failure-surface robustness

- Comment-post HTTP failure → log warning, exit with dbt's exit code (merge still blocked). The block is the primary signal.
- `$GITHUB_TOKEN` fork-scope limitation → detect and skip comment; log warning; exit with dbt's exit code. Fork PRs still get merge blocked.
- Databricks API submit failure → exit 1 without comment; GH Actions logs carry the diagnostic.

## 6. Kimball migration design (PR 4b detail)

### 6.1 `fct_action_values.sql` shape (final)

```sql
{{ config(
    materialized='incremental',
    unique_key='action_value_id',
    liquid_clustered_by=['match_key'],
    incremental_strategy='merge'
) }}

with action_values as (

    select * from {{ ref('stg_spadl__action_values') }}
    {% if is_incremental() %}
    where match_id not in (
        select distinct try_cast(match_id as bigint) from {{ this }}
    )
    {% endif %}

),

-- ... existing sb_events + running_score CTEs unchanged ...

actions_with_score as (

    select
        {{ dbt_utils.generate_surrogate_key([
            'av.match_id',
            'av.period',
            'av.time_seconds',
            'av.player_id',
            'av.type_id',
            'av.data_source'
        ]) }}                                       as action_value_id,

        dm.match_key,                               -- NEW: Kimball surrogate
        dc.competition_key,                         -- NEW: Kimball surrogate
        try_cast(dm.native_match_id as bigint)      as match_id,       -- LEGACY until 2026-07-22
        try_cast(dc.native_competition_id as bigint) as competition_id, -- LEGACY until 2026-07-22

        av.player_id,
        av.team_id,
        av.season_id,
        av.period,
        av.time_seconds,
        av.minute,
        av.second,

        -- ... rest unchanged ...

    from action_values av
    left join {{ ref('dim_matches') }} dm
        on av.match_id = dm.native_match_id
        and av.data_source = dm.data_source
    left join {{ ref('dim_competitions') }} dc
        on dc.competition_key = dm.competition_key
    left join sb_events sbe
        on av.original_event_id = sbe.event_id
        and av.data_source = 'statsbomb'
    left join running_score rs
        on rs.match_key = dm.match_key
        -- ... rest of running_score join unchanged modulo match_id → match_key ...

),

final as (

    select
        action_value_id,
        match_key,
        competition_key,
        match_id,          -- LEGACY
        competition_id,    -- LEGACY
        player_id,
        team_id,
        season_id,
        -- ... all other columns unchanged ...
        data_source,
        original_event_id,
        current_timestamp() as _loaded_at

    from actions_with_score
    where _score_rn = 1

)

select * from final
```

**Incremental predicate change note.** The predicate switches from `match_id not in (... match_id from this)` to a `try_cast`-aware form that compares BIGINT to BIGINT. First post-merge build may reprocess rows whose legacy `match_id` became NULL after try_cast (IDSSE/Metrica rows if those ever land in `bronze.action_values`). Document in PR description. Run `dbt build --select fct_action_values --full-refresh` once in dev before merging to confirm the incremental merge stays idempotent on subsequent runs.

**`running_score` join change.** PR 2 migrated `int_running_score` to emit `match_key` (memory: `project_kimball_migration_cycle.md` — "int_running_score.sql: unchanged" in PR 3 means PR 2 already touched it). Verify during plan stage.

### 6.2 `_marts__models.yml` contract update

`fct_action_values` columns block adds:

- `match_key` BIGINT NOT NULL, description: "Kimball surrogate FK to dim_matches (ADR-011)."
- `competition_key` BIGINT nullable, description: "Kimball surrogate FK to dim_competitions (ADR-011)."

Retained legacy columns:

- `match_id` BIGINT nullable, description: "LEGACY native match identifier; removed 2026-07-22 per ADR-011 dual-column policy. Use `match_key` for new work. NULL for rows whose provider native_match_id is non-BIGINT-parseable (IDSSE/Metrica)."
- `competition_id` BIGINT nullable, description: "LEGACY native competition identifier; removed 2026-07-22. Use `competition_key` for new work."

Contract stays `enforced: true`.

### 6.3 `publish_spadl_vaep_hf.py` SQL (final shape)

```python
_ACTION_VALUES_SQL = """\
SELECT
    av.action_value_id,
    av.match_key,
    av.competition_key,
    av.match_id,                -- LEGACY: sunset 2026-07-22
    av.competition_id,          -- LEGACY: sunset 2026-07-22
    av.player_id,
    av.team_id,
    av.season_id,
    av.period,
    av.time_seconds,
    av.minute,
    av.second,
    av.start_x,
    av.start_y,
    av.end_x,
    av.end_y,
    av.action_type,
    av.action_result,
    av.bodypart,
    av.offensive_value,
    av.defensive_value,
    av.vaep_value,
    av.original_event_id,
    av.data_source
FROM soccer_analytics.dev_gold.fct_action_values av
"""
```

(Note: since `fct_action_values` already carries both the new keys AND the dual-column legacy keys from §6.1, the publish script just selects from the mart. No additional `LEFT JOIN dim_matches` needed here — the mart did the join for us.)

`normalize_dtypes` updated to handle `match_key` + `competition_key` as `Int64` (nullable integer), alongside existing match_id/competition_id.

### 6.4 Finding D fix (one line in `publish_xg_shots_hf.py:99`)

Before:
```python
    CAST(dm.native_match_id AS BIGINT)                     AS match_id,
```

After:
```python
    try_cast(dm.native_match_id as bigint)                 as match_id,
```

Comment above updated to reference `reference_try_cast_spark_pushdown.md` semantics: NULL on unparseable, cast-pushdown-safe.

Regression test: new unit test asserts `try_cast` substring present in `_SHOTS_SQL` at module level (not runtime — just a text assertion).

### 6.5 `hf_taipy_app/src/queries/defensive.py` updates

VAEP query helpers:

- `fetch_vaep_rankings(competition_key, team_id, player_id)` — filter on `competition_key`. May need join to `dim_matches_synced` if any rankings logic currently keys on match_id.
- `fetch_vaep_breakdown(match_key=None, competition_key=None)` — filter on the Kimball keys directly.
- `fetch_vaep_timeline(match_key)` — filter on `match_key`. Join to `dim_matches_synced` if any native_match_id needs exposing for display.

**Open item for plan stage:** the exact current signatures and SQL of these three functions — they live in `hf_taipy_app/src/queries/defensive.py` which I didn't fully read during brainstorming. Plan verifies the actual filter columns and decides whether `state/shared.py` needs a `get_match_key` / `get_competition_key` peer helper alongside the existing `get_match_id` / `get_comp_id`.

### 6.6 G1 `wait_until_online` helper

Added to `src/ingestion/refresh_synced_tables.py`. Shape:

```python
SYNCED_TABLE_ONLINE_STATE = "SYNCED_TABLE_ONLINE_NO_PENDING_UPDATE"
_SYNCED_TABLE_TERMINAL_FAILURE_STATES = frozenset({
    "SYNCED_TABLE_OFFLINE",
    "SYNCED_TABLE_OFFLINE_FAILED",
})

def wait_until_online(
    table_fqn: str,
    *,
    timeout_s: int = 600,
    poll_interval_s: int = 15,
) -> None:
    """Poll a Lakebase synced table until it reaches SYNCED_TABLE_ONLINE_NO_PENDING_UPDATE.

    Raises:
        TimeoutError: with table FQN + last-seen detailed_state + elapsed time
            if the table hasn't reached the online state within timeout_s.
        RuntimeError: with state name if the table hits a terminal failure state.
        requests.HTTPError: propagated on 4xx/5xx from the status endpoint.
    """
    # ... uses _get_host() + _get_auth_headers() (existing module functions) ...
```

Endpoint: `GET /api/2.0/database/synced_tables/{table_fqn}` — reads `status.detailed_state`. Reuses the same endpoint the module already hits at line 178, so no API migration here.

### 6.7 On-Deck entry for G2 + G3

Exact file location verified during implementation (repo convention — memory references "TODO tables" but doesn't pin a file path). Entry content:

> **SDK synced-table path hardening (G2 + G3 from Kimball PR 4, 2026-04-23)**
>
> **G2 (confirmed real):** `src/ingestion/refresh_synced_tables.py:178` hits `/api/2.0/database/synced_tables/` (legacy REST endpoint). SDK-created synced tables (via `w.postgres.synced_tables.*`) live under `/api/2.0/postgres/synced_tables/`. An SDK-created table is not addressable by the current refresh module.
>
> **G3 (unverified):** `run_lakebase_grants.py` + `fix_event_log_ownership.py` behavior post-SDK-create — unclear whether ADR-005 grants flow and event_log ownership semantics hold on the new creation path.
>
> **Blocking condition:** any future PR that switches synced-table creation from Terraform / UI to `w.postgres.synced_tables.*` SDK path must close both gaps before shipping. G1 (`wait_until_online` helper) already landed in PR 4b and is ready for reuse.

## 7. HF README helper design (PR 4c detail)

### 7.1 Helper signature

```python
def upload_hf_readme(
    repo_id: str,
    readme_path: Path,
    hf_token: str,
    *,
    repo_type: Literal["dataset", "space"] = "dataset",
) -> dict[str, str]:
    """Upload README.md to an HF dataset or Space repo.

    Args:
        repo_id: Full HF repo id (e.g., 'luxury-lakehouse/spadl-vaep-action-values').
        readme_path: Path to the in-repo source markdown file.
        hf_token: HF API token.
        repo_type: 'dataset' (default) or 'space'.

    Returns:
        Dict with 'commit_url' + 'sha256' (of the uploaded bytes).

    Raises:
        ValueError: if the file is missing, empty, or repo_id is malformed.
        HfHubHTTPError: propagated on HF API failures.
    """
```

### 7.2 Dataset card markdown structure (example: `spadl-vaep-action-values.md`)

```markdown
---
license: cc-by-4.0
tags:
  - soccer
  - vaep
  - spadl
  - action-values
size_categories:
  - 1M<n<10M
---

# SPADL/VAEP Action Values

Valuing Actions by Estimating Probabilities (VAEP) scores for every on-ball action
across StatsBomb + Wyscout open data. Implemented via
[silly-kicks](https://github.com/karsten-s-nielsen/silly-kicks).

Unified to the SPADL format (105×68m pitch; 23 action types).

## Schema change (sunset 2026-07-22)

This dataset emits both legacy and canonical key columns. Legacy columns will be
removed on **2026-07-22** (90 days from initial dual-emit on 2026-04-23):

| Legacy column | Canonical replacement | Notes |
|---|---|---|
| `match_id` | `match_key` | BIGINT Kimball surrogate; collision-free across providers |
| `competition_id` | `competition_key` | BIGINT Kimball surrogate |

Update consumer code before the cut-over date. After cut-over, legacy columns
will be removed without further notice.

## Columns

... (full column list) ...

## Reference

Decroos, T., Bransen, L., Van Haaren, J., & Davis, J. (2019). *Actions Speak
Louder Than Goals: Valuing Player Actions in Soccer.* KDD '19.
<https://doi.org/10.1145/3292500.3330758>
```

Exact markdown text finalized during plan stage.

### 7.3 `scripts/publish_hf_org_card.py` refactor

Before: ad-hoc `HfApi.upload_file(..., repo_type="space", ...)` inside the script.

After: `upload_hf_readme("luxury-lakehouse/README", Path("docs/huggingface/org-card.md"), hf_token, repo_type="space")`.

No behavior change. Consolidation of the HF-publish pattern into a single code path.

## 8. Tests

### 8.1 New test files

| File | Sub-PR | What it asserts |
|---|---|---|
| `src/tests/test_trigger_dbt_job.py` | 4a | Happy path submit + poll; terminal state handling; timeout; HTTP error propagation. Requests mocked. |
| `src/tests/test_post_dbt_failure_comment.py` | 4a | `run_results.json` parsing (mixed, all-fail, empty); fork-PR scope detection; comment truncation. Requests + GH API mocked. |
| `src/tests/test_fct_action_values_contract.py` | 4b | dbt YAML contract for fct_action_values contains match_key + competition_key NOT NULL + legacy match_id/competition_id nullable. |
| `src/tests/test_hf_publish.py` | 4c | upload_hf_readme: missing file, empty file, dataset upload, space upload, CRLF normalization, invalid repo_id. HfApi mocked. |

### 8.2 Updated test files

| File | Sub-PR | Update |
|---|---|---|
| `src/tests/test_refresh_synced_tables.py` | 4b | New class TestWaitUntilOnline: transitions happy path, timeout, terminal failure states, HTTP 404. |
| `src/tests/test_marts_live_schema.py` (or equivalent) | 4b | Add fct_action_values live-DESCRIBE test (pattern from PR 1.8 drop-safety sweep). |
| `src/tests/test_staging_coverage.py` | 4b | Confirm action_values pair covered post-migration (if not already). |
| `src/tests/test_publish_xg_shots_hf.py` (new or existing) | 4b | Assert `try_cast` present in `_SHOTS_SQL` module constant — Finding D regression guard. |

### 8.3 dbt YAML tests

- `fct_action_values` schema YAML picks up new not_null + relationships tests on match_key (relationship to dim_matches.match_key).

### 8.4 Manual verification (recorded in PR descriptions)

- **PR 4a E2E:** scratch branch with intentionally broken model — verify PR comment + required-check red + merge button blocked; fix + push clears block.
- **PR 4b Taipy smoke:** local run (`cd hf_taipy_app && python src/main.py`), open Player Impact page, verify rankings / breakdown / timeline populate post-migration.
- **PR 4b HF dry-run** (gated on approval): `hf jobs uv run scripts/publish_spadl_vaep_hf.py ...` against dev_gold; inspect Parquet metadata for dual-column presence.
- **PR 4c E2E** (gated on approval): full publish cycle on xg-freeze-frame-data (safest candidate, no dual-column change). Verify HF dataset page shows updated README.

## 9. Deploy sequence (per sub-PR)

### PR 4a

1. Merge PR 4a to main (user-approved, green CI).
2. No Terraform changes; no Databricks workflow changes.
3. First PR opened after merge exercises the live dbt CI for real.
4. If anything surfaces as broken: log the issue, investigate; the existing `dbt-ci.yml` parse workflow is unchanged and still runs — PR 4a adds coverage, it doesn't remove any.

### PR 4b

1. Merge PR 4b to main (user-approved, green CI including PR 4a's live dbt build).
2. Next daily Databricks job run — `dbt build` produces new `fct_action_values` with both new + legacy columns.
3. **Synced-table schema propagation verification** (open during implementation): either Lakebase auto-evolves the synced table to pick up new columns on next refresh, OR manual recreation via `maintain_synced_tables.py`. Approach confirmed during PR 4b implementation, not guessed.
4. Daily 07:00 UTC cron OR manual `scripts/maintain_synced_tables.py --catalog soccer_analytics --schema dev_gold` — self-heal if indexes went away during any recreation.
5. Deploy Taipy Space: staging → E2E smoke (Player Impact page) → production.

### PR 4c

1. Merge PR 4c to main (user-approved, green CI).
2. Next scheduled (or manual) publish run of each `publish_*_hf.py` script auto-uploads the matching README.
3. User manually runs `python scripts/publish_hf_org_card.py` to push the refreshed org-card (unchanged operational pattern).

## 10. Rollback

### PR 4a

- If the live CI itself misbehaves (false positives, API drift): temporarily disable the workflow via branch protection toggle; `git revert` the PR 4a merge; parse-only CI continues to gate merges (as it did before PR 4a).

### PR 4b

- Warehouse-layer break (dbt build fails): `git revert` the PR 4b merge. `fct_action_values` mart regenerates on old schema on next daily run. Taipy queries revert via the same revert (code colocated).
- Taipy-layer break: redeploy previous Space commit via `manage_space.py deploy production --ref <prior-sha>`. No warehouse touch needed.
- HF dataset consumer complaint on dual-column additions: unlikely — additions don't break existing consumers. If a schema-strict consumer rejects the added columns, narrow the publish SQL to `SELECT *` minus new columns via a publish-script flag.

### PR 4c

- Markdown-only and helper module — revert the merge; the last manually-pushed READMEs on HF Hub stay in place (no data loss).

## 11. Risk register

| # | Risk | Mitigation |
|---|---|---|
| 1 | Live dbt CI's one-shot Job spec (§5.2) introduces an auth or Python-runtime quirk not seen in `terraform-apply.yml`'s Terraform use case | Plan-stage E2E on a scratch branch before merging PR 4a. `--select state:modified+` naturally picks nothing on a pure-docs PR, which is a cheap first smoke test |
| 2 | Databricks Job cold-start + dbt build on state:modified+ takes longer than the 30-minute workflow budget | Budget tuned at 30 min based on memory (8-12 min typical full build); add a plan-stage measurement pass. If over budget, raise to 45 min or switch to permanent Job resource |
| 3 | `fct_action_values` has orphan match_ids that don't resolve in dim_matches | Surfaced as failing live dbt build on PR 4b itself. Contract test fails loudly. Fix at source (dim_matches gap) or at dbt layer (DEDUP filter in staging) before merging PR 4b |
| 4 | Lakebase auto-evolution of `fct_action_values_synced` schema silently lags, leaving Taipy queries failing post-PR-4b | Deploy Taipy AFTER verifying synced-table schema via DESCRIBE. Run `maintain_synced_tables.py` if auto-evolution didn't pick up the new columns |
| 5 | Hyrum's Law on `spadl-vaep-action-values`: unknown external consumers at 82 mo/downloads break on schema addition | Dual-column window mitigates — new columns are additive; legacy columns stay for 90 days. Monitor HF discussions tab during the window |
| 6 | `hf_publish.upload_hf_readme` happens AFTER data upload in each publish script; if README upload fails, the data landed but the README is stale | Explicit log + raise — publish script fails loudly with data-landed-README-missing state. Re-running the publish script is idempotent (HfApi handles duplicate uploads). Document in the helper's docstring |
| 7 | Finding D regression: a future edit to `publish_xg_shots_hf.py:99` switches back to `CAST` without noticing | Regression test asserts `try_cast` substring present in `_SHOTS_SQL`. Single-line guard |
| 8 | Manifest-baseline diff check (§5.1) misses a config change not in the three hardcoded files (e.g., a new `.dbtconfig`-style file lands in the future) | Accept the risk; the three-file check covers the known configs. On future config additions, update the workflow in the same PR |
| 9 | Token rotation lag — GH Actions secrets, HF org secrets, Databricks OIDC trust | Plan-stage verification: OIDC trust set up for the repo; HF_TOKEN valid; DATABRICKS_HTTP_PATH valid. Tested by running PR 4a on a scratch PR before production use |

## 12. Open items for plan stage

1. Exact on-disk location of the project's TODO / On-Deck file (memory references "TODO tables" — I'll find the current convention during implementation; likely under `docs/` but needs verification).
2. Whether `fct_action_values_synced` schema picks up new columns via Lakebase auto-evolution or requires manual synced-table recreation. Inspection during the first post-merge refresh.
3. `hf_taipy_app/src/queries/defensive.py` — exact current signatures and SQL of `fetch_vaep_rankings`, `fetch_vaep_breakdown`, `fetch_vaep_timeline`. Plan verifies whether `state/shared.py` needs `get_match_key` / `get_competition_key` peer helpers.
4. `dbt_project/models/staging/stg_spadl__action_values.sql` — confirm this still emits `match_id` (silver level) so the mart-layer JOIN to `dim_matches` is the only Kimball-resolution step. If staging already resolves, mart layer simplifies.
5. `int_running_score` column names post-PR 2 — confirm the mart's LEFT JOIN `running_score` uses `match_key` not `match_id`.
6. One-shot Job task type in the Databricks runs_submit payload — `spark_python_task` vs `python_wheel_task` vs notebook. Plan stage confirms by reading how `scripts/dbt_build_and_refresh.py` currently invokes dbt.
7. `DATABRICKS_CLIENT_ID` (for OIDC auth in PR 4a) — verify the service principal has the permissions to submit jobs + read dev catalog (Run + execute workloads + SELECT on dev_gold).
8. HF dataset card markdown for all 4 cards — exact wording reviewed at plan stage (schema tables, license, tags, references).
9. HF Hub discussions tab on `spadl-vaep-action-values` — any existing conversations that would hint at external consumer patterns, informing the dual-column window decision stability (already approved, but worth surfacing).
10. `docs/huggingface/org-card.md` current content — identify stale references to the 4 datasets and plan the edits.
11. Confirm `dim_matches` primary resolution key is `(data_source, native_match_id)` as assumed in §6.1 — look at PR 2's shipped join pattern.
12. Statsbomb-shots-on-target dataset card — does the existing on-HF README match what we'd check in under `docs/huggingface/dataset-cards/statsbomb-shots-on-target.md`? If not, this PR realigns them.

## 13. Sequencing rationale (why B, why three PRs)

Q1 brainstorming chose B: Live dbt CI first → migration → README helper. Rationale recorded here in canonical form:

- Live dbt CI must exist before PR 4b merges so the migration gets the safety net it's designed to provide. A migration that ships without live CI is the exact failure mode that motivated PR 4a.
- README helper (PR 4c) is orthogonal hygiene. Lands after PR 4b because the dual-column data in `spadl-vaep-action-values` is only published manually (via `hf jobs uv run scripts/publish_spadl_vaep_hf.py`). Until someone runs the publish script, HF Hub sees no change from PR 4b's data-layer work. PR 4c landing before a post-PR-4b publish run is sufficient — no drift window.
- Three PRs rather than one: each stream is independently reviewable on its own merits. "Single / minimal commits" per user rule is honored per-PR; each branch squash-merges to one commit. Ten small commits across one mega-PR would be harder to review.
