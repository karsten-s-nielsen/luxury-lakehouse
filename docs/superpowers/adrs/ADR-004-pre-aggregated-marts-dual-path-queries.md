# ADR-004: Pre-aggregated `fct_*_agg` marts with dual-path Taipy queries for base-case bottlenecks

| Field | Value |
|---|---|
| **Date** | 2026-04-17 |
| **Status** | Accepted |
| **Deciders** | Karsten Nielsen |

## Context

PR #122 (2026-04-15) eliminated silent VAEP scoring swallows. The side-effect was that `fct_action_values` grew from a partial subset to ~9.5M rows. Several Taipy queries that land on page-open with only a competition filter (the "base case") crossed a Lakebase scale cliff from "Index Scan" to "Parallel Seq Scan + external merge sort spill". Measurements against live Lakebase 2026-04-16:

- `fetch_vaep_breakdown(comp=11)` — 2,800 ms Parallel Seq Scan on 9.5M rows
- `fetch_gk_passes(comp=11)` — 13,247 ms Parallel Seq Scan on 9.5M rows + `dim_players` join (position_group filter only eliminates ~95% of rows, not selective enough for the planner to prefer Index Scan)
- `fetch_heatmap_actions(comp=11)` — 6,864 ms Parallel Seq Scan on the 5.05M-row `fct_passes` with external 12 MB/worker sort spill

Page-open latency must fit the <500 ms cached-interaction budget (CLAUDE.md "App Performance"). Raw-table filters with only `WHERE competition_id = X` cannot hit the existing composite indexes (which are leftmost `(competition_id, team_id, ...)`) at a selectivity where the planner chooses them. Four prior PRs of query rewrites (PR #129 and earlier) tried to push the raw-table plan back to an Index Scan and failed — the filter cardinality is the problem, not the SQL shape.

The forcing function is product-facing: a user lands on Defensive Impact / Heat Map / Goalkeeper Analytics and waits 3–13 seconds before anything renders. Caching does not help the first hit. Synced-table recreation does not help — the underlying row count is the constraint.

## Decision

When a Taipy query's base-case filter pattern triggers a Parallel Seq Scan on a fact table >1M rows, we add a dedicated pre-aggregated mart with suffix `_agg` (or a pre-filtered narrow projection when aggregation is not possible) to `dbt_project/models/marts/`, and rewrite the query to use a **dual-path pattern**: the mart serves the base case(s), and the original raw-table query remains as a fall-through for filter combinations outside the mart's grain. The mart is materialized by `dbt build`, synced to Lakebase alongside the 34 existing synced tables, and indexed with a composite covering its access paths. Three marts land in this cycle:

- `fct_heatmap_agg` — grain `(competition_id, team_id, action_type, x_bin, y_bin)`, ~53K rows
- `fct_vaep_breakdown_agg` — grain `(competition_id, team_id, player_id, action_type)`, ~162K rows
- `fct_gk_actions_detail` — pre-filtered narrow projection (GK + pass/goalkick), ~168K rows (no `_agg` suffix because it is a projection, not an aggregation)

## Alternatives considered

| Option | Pros | Cons | Why rejected |
|---|---|---|---|
| A. More query rewrites on raw tables (continue the PR #129 path) | Zero new marts, zero new sync state, zero new indexes. | Verified not to work: PR #129 tried CTE shape changes, subquery-DISTINCT, recursive loose-index-scan and correlated `EXISTS` on the 9.5M-row `fct_action_values` and the 5M-row `fct_passes`. None moved the comp-only plan off Parallel Seq Scan. The filter cardinality is the constraint, not the SQL shape. | Proven not to fix the cardinality problem. Four PRs in and still 2.8–13 s on base case. |
| B. Denormalize — add aggregate columns directly to the raw fact tables | No new Delta tables, no new synced tables, no new PG indexes. | Breaks dbt contract isolation (fact vs. mart layers have different grains by design). Inflates row size on 9.5M-row `fct_action_values`. Confuses read-side contract: `fct_action_values` is per-action; summed columns would need per-row sentinel semantics. Violates Kimball-style medallion discipline. | Breaks layer separation; adds schema complexity for downstream consumers. |
| C. Mart-only (swap the Taipy query to the mart, delete the raw path entirely) | Simpler: one access path per query. | Mart grain does not cover `player_id` or `match_id` filters — those filters already have fast paths on the raw tables via `idx_passes_comp_player`, `idx_shots_comp_team_player`, and friends. Forcing everything through the mart means either (a) adding player/match to every mart's grain (row-count explosion; `fct_heatmap_agg` becomes O(players × x × y) instead of O(teams × x × y)) or (b) scanning the whole mart and filtering in memory (defeats the purpose). | Loses the ability to serve deep-filter queries from existing fast indexes. |
| D. Pre-aggregated `_agg` mart + dual-path query (mart for base case, fall-through for edge cases) | Base-case comp-only queries drop to Index Scan on a ~100K-row mart. Deep filters still hit `idx_*_comp_player` / `idx_*_comp_team_match` on the raw tables at <100 ms. Both access paths return identical columns, so caller shape is unchanged. | Three new Delta tables, three new synced tables, seven new PG indexes. Mart freshness is tied to `dbt build` (daily); same as every other gold-layer mart. | — |

## Consequences

### Positive

- Base-case page-landing queries measured against live Lakebase 2026-04-17 drop from 2,800–13,247 ms to 20–66 ms (43×–335× speedup). Evidence: `/tmp/audit_explain_v5_marts.jsonl`, seven queries, all show `first_op ∈ {HashAggregate, GroupAggregate, Limit}`, `has_index_scan=True`, `has_parallel_seq_scan=False`, no external merge sort.
- The dual-path pattern is reusable. When a future Taipy query surfaces the same symptom, the template is now established: add an `fct_<name>_agg` mart with a chosen grain, add a `_synced` terraform resource with `ignore_changes = all`, add composite PG indexes on the leading filter columns, and put the mart path behind `if base_case_filter: ... else: fall_through_to_raw` in the Taipy query module.
- Mart suffix convention (`_agg`) signals "pre-aggregated, not a raw fact" to dbt authors, downstream SQL readers, and anyone scanning `dbt_project/models/marts/`. Narrow projections without aggregation (`fct_gk_actions_detail`) do not take the suffix — the naming distinguishes the two operations.

### Negative

- Three new synced tables and seven new PG indexes to maintain. Synced-table refresh cost scales O(N) on the number of synced tables; adds ~3× on top of the existing 34-table refresh (empirically still <60 s end-to-end because the marts are <200K rows each).
- Mart freshness lag. The daily-job chain is `compute → dbt_build → refresh_synced_tables`, so the marts are at worst ~24 h stale relative to bronze. Acceptable for dashboard queries over historical matches; not acceptable for near-real-time use cases. None of the three Taipy pages served by these marts are near-real-time.
- Dual-path queries add a branch per query module. The branch is clear (if `player_id is None and match_id is None: mart_path`; else `fall_through_path`) but adds code that the mart-only option would not require. The branch must return identical columns from both paths — enforced by convention in each module's docstring, not by a test.
- Three new Terraform resources depend on UI-create-then-import (Path A from the 2026-04-17 work) because the `databricks_database_synced_database_table` provider resource in v1.110.0 does not support create-from-Autoscaling-endpoint. This is a platform constraint inherited from the existing synced-table pattern, not introduced by this ADR.

### Neutral

- Mart refresh is guarded by the existing `refresh_synced_tables.py` flow; no new cron, no new hook.
- `workflow-cards/wf-dbt-build.yaml` grows by three entries (per ADR-003 enforcement) and `_marts__models.yml` grows by three contract blocks (per the project-wide `contract: {enforced: true}` convention on gold marts).

## CLAUDE.md Amendment

None required. The "Database Performance" section already encourages pre-aggregation for bounded-subset reads ("Avoid `.toPandas()` on unbounded tables — Budget: <5M rows"). The `_agg` suffix and dual-path pattern are refinements of existing guidance, not exceptions to a project-wide rule.

## Related

- **Branch:** `perf/base-case-query-bottlenecks`
- **Baseline measurements:** removed (see Notes) — they were the `scripts/_audit_*.py` diagnostic files deleted at commit time. Representative numbers are embedded in each mart's `.sql` header comment and in this ADR's Context.
- **Tests:** `src/tests/test_refresh_synced_tables.py::test_synced_tables_list_has_37_entries`, `src/tests/test_card_dbt_model_field` (enforces ADR-003 → wf-dbt-build.yaml parity with the three new marts).
- **Marts:** `dbt_project/models/marts/fct_heatmap_agg.sql`, `dbt_project/models/marts/fct_vaep_breakdown_agg.sql`, `dbt_project/models/marts/fct_gk_actions_detail.sql`.
- **Query modules:** `hf_taipy_app/src/queries/tracking.py::fetch_heatmap_actions` (dual-path), `hf_taipy_app/src/queries/defensive.py::fetch_vaep_breakdown` (mart-only for all three filter combos), `hf_taipy_app/src/queries/goalkeepers.py::fetch_gk_passes` (mart-only).
- **PG indexes:** `scripts/create_indexes.py` — seven new composite B-tree indexes (`idx_heatmap_agg_comp`, `idx_heatmap_agg_comp_team`, `idx_vaep_breakdown_agg_comp_team_player`, `idx_gk_actions_detail_comp_team_player`, `idx_gk_actions_detail_comp_player`, `idx_gk_actions_detail_match`, `idx_gk_actions_detail_match_player`).
- **Terraform:** `terraform/modules/synced_tables/main.tf` — three new `databricks_database_synced_database_table` resources with `lifecycle { ignore_changes = all }`.
- **Prior related work:** PR #129 (raw-table query rewrites — rejected after measurement), PR #122 (VAEP scoring fix that surfaced the bottleneck).

## Notes

The naming distinction between `_agg` (aggregation) and no-suffix narrow projection is deliberate. `fct_gk_actions_detail` keeps per-action rows (it is a pre-joined filtered projection of `fct_action_values` × `dim_players` on `position_group = 'Goalkeeper' AND action_type IN ('goalkick', 'pass')`), while `fct_heatmap_agg` and `fct_vaep_breakdown_agg` collapse rows via `SUM` / `COUNT`. A future mart that projects without aggregating should follow `fct_gk_actions_detail`; a mart that aggregates should take the `_agg` suffix.

The dual-path pattern's "which path to take" decision is encoded in the query module itself, not in a config layer. This is intentional: the branching logic is specific to the mart's grain, and centralizing it in a router would obscure the coupling between Taipy filter inputs and mart columns. Each query module's docstring names the mart grain and the fall-through conditions explicitly (see `fetch_heatmap_actions` for the canonical example).
