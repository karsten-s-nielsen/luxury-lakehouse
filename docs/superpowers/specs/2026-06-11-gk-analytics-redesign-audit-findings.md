# GK Analytics Redesign — planning-mode audit findings (pre-review)

**Date:** 2026-06-11 · **Scope:** the spec + plan only (planning modes; audit-mode passes run
post-implementation per plan Task 9). All findings below were **fixed into the spec/plan inline**
before the cross-session review — this record exists so the reviewer can verify the fixes rather
than rediscover the issues.

## Architecture audit (planning mode)

| # | Severity | Finding | Resolution (in docs) |
|---|----------|---------|----------------------|
| A1 | High | Mart stored `ghost_deviation_m` but not the canonical actual GK position — the app scene renderer would re-derive the orientation heuristic client-side, defeating the single-macro design | Mart adds `gk_actual_x`, `gk_actual_y`, `gk_frame_mirrored` (spec §4.1; plan Task 2 SQL + contract) |
| A2 | Med-High | `ModelGridProvider` fetched frames from `queries` itself — adapter doing I/O (services→queries edge, lazy import), untestable without a DB | Port signature takes `frame_players: DataFrame \| None`; STATE fetches and passes; adapters are pure (spec §5; plan Task 6 code + new no-frame fallback test) |
| A3 | Medium | `PRESET_COLUMN` + query column lists are connascent with the dbt contract across a deployment boundary with no read-side check | New `src/tests/test_gk_tracking_read_contract.py` parses `_marts__models.yml` and asserts app-expected columns ⊆ contract (ADR-002 §4 pattern, read-side; plan Step 5.5) |
| A4 | Medium | `SELECT *` in `build_gk_actions_sql` — stamp coupling under `on_schema_change: append_new_columns` | Explicit `GK_ACTIONS_COLUMNS` constant, single source shared with A3's test (plan Task 5) |

Clean: dependency directions otherwise sound (pages→state→{queries,services}); mart→mart ref has
precedent (`fct_goalkeeper_stats`→`fct_action_values`); twelve-factor config-via-env for both
flags; ADR-051 covers the decisions; env-flag is fail-safe default-off. Noted Low: `services/` is
a new app-layer directory name — recorded in ADR-051, acceptable.

## Security audit (planning mode, STRIDE on new surfaces)

| # | Severity | STRIDE | Finding | Resolution |
|---|----------|--------|---------|------------|
| S1 | High | Information Disclosure | Staging Space with the flag on exposes an unreviewed page + GS per-player WC2022 metrics; if the staging Space is public the "nobody will know" premise fails | Rollout requires staging Space visibility = private/org-only before setting the flag; GS-display decision re-confirmed at cutover (spec §8; plan Step 12.3) |
| S2 | Medium | Tampering / CWE-494 | Ghost model pulled from HF Hub at `main` = mutable supply-chain edge in a public-facing runtime | Loader pins the model REVISION (commit hash); npz loader verified for no `allow_pickle` path (spec §8; plan Step 6.5) |
| S3 | Low | DoS | Per-scene model render on a CPU Space, unbounded re-renders | Grid memoized per `gk_action_id` (plan Step 6.5) |
| S4 | Low | Elevation | Accidental prod flag-flip exposes the page | Already mitigated: default-off fail-safe, ADR-documented, prod-variables check in Step 12.3 |

Clean: query layer is parameterized `%s` with a closed `family` set and constants-only
interpolation (matches repo S608 conventions); no new secrets; existing OAuth M2M auth unchanged.

## Observability audit (planning mode)

| # | Severity | Finding | Resolution |
|---|----------|---------|------------|
| O1 | Medium | Ghost model render sits on the user path (≤3 s first load / ≤500 ms cached budgets) with no planned duration signal | INFO-level render log (action, source, cache hit, duration_ms); e2e asserts scene interaction within budget under `LL_GHOST_GRID=model` (plan Steps 6.5 + 11.4) |
| O2 | Low | `gkt_data_freshness` may reflect a global freshness source, not the new marts | Step 8.6: verify `fetch_data_freshness()` coverage; repoint to the GK synced tables if needed |

Clean (by existing convention, already in spec): ERROR-level loud fallback + on-chart `source`
provenance for the ghost service (ADR-002); state-module `logger.exception` + `warning_var`
pattern; dbt singular tests as quality gates in live CI; game-state split hidden WITH caption
(no silent substitution); row-count INFO logs per the existing page pattern.

## Cross-session review (AC-pipeline session, 2026-06-11) — resolutions

All four blockers + all improvement-grade items applied to the spec/plan:

| # | Severity | Finding (reviewer-verified) | Resolution |
|---|----------|------------------------------|------------|
| C1 | Critical | Domain test falsified by live GS data (139 completion vs 124 xt_gk rows) | Singular test now one-directional (`xt_gk → gk_completion`); completion-only rows relayed upstream as open question |
| C2 | Critical | TRIGGERED synced marts need DUAL registration (`triggered_synced_marts` yml var + `refresh_synced_tables.py`); parity enforced by `test_strand_safe_rederive.py` | Step 4.1b adds both model names to the yml var; parity test added to Step 4.2's gate |
| H1 | High | `table`-materialized TRIGGERED synced mart strands its synced table on every rebuild (ADR-043 am. 2) | Stats mart → incremental/merge with full-recompute body (no `is_incremental()` filter), documented in SQL + spec |
| H2 | High | Flagship bump chart had no pool-wide query | `build_gk_pool_stats_sql`/`fetch_gk_pool_stats` (distribution-weighted preset means) + unit test; bump + all "vs sample" deltas wired to it |
| H3 | High | `|Δx|>52.5` mirror mis-orients sweeping keepers (~15 m error on Tab 3's most interesting rows) | Macro re-anchored on stored `pre_shot_gk_distance_to_goal` residuals (exact for all positions); 52.5 survives only as residual-tie tiebreak |
| M1/M2 | Medium | PEV≈0 is BY DESIGN (silly-kicks verdict), not a bug; caveat list mixed severities | Spec caveats split: confirmed-by-design (PEV, with display caption rule) / verify-first (base sign) / open upstream (game_state) |
| M3/M4 | Medium | Stats `SELECT *`; hard-coded placeholder count | `GK_STATS_COLUMNS` + contract test; `_PROVIDER_SQL` derived from the providers tuple |
| M5 | Medium | Scene-frame query on the platform's largest synced table, no index plan | Composite index `(match_key, period, frame)` verify/create + EXPLAIN gate |
| M6 | Medium | `dbt parse` PASS overstated as schema proof | Step 2.5 reworded: parse-level only |
| LOW | — | INNER/LEFT wording; owngoal note; conftest path + importorskip; `dim_players_synced` column verify; private `_ghost_gk_model_cached` symbol | All applied; model adapter DEFERRED to fast-follow gated on a public silly-kicks loader API (spec §9 res. 3) |

Reviewer's current-state facts adopted into Task 11.1 (staging view schema-stale live; full AC
population expected imminently per owner 2026-06-11; silly-kicks state per round 2: 4.22.2
rejected, 4.23.0 staged on wheel 0.5.35 — Tasks 0–10 independent).

## Cross-session review ROUND 2 (same reviewer, 2026-06-11) — APPROVED, resolutions

All round-1 fixes verified by the reviewer (incl. H3 geometry checked). Round-2 items applied:

| # | Type | Finding | Resolution |
|---|------|---------|------------|
| R1 | Blocking doc fix | ADR task still recorded the superseded `\|Δx\|>52.5` heuristic | Task 1.1 item 4 rewritten to the distance-residual anchor (tiebreak demoted) |
| R2 | Blocking decision | merge never deletes — AC wipes (twice this week) would leave orphan rows; stats would disagree with actions | **Hybrid chosen:** stats mart gets an orphan-sweep `post_hook` (exact anti-join vs actions, cheap at grain); actions mart orphans follow the existing AC-family operator practice (DELETE before re-derive, ADR-043 tooling) — recorded as ADR item 4b |
| N1 | Note | Pool query mixed weighting schemes | Defense-side means now weighted (deviation by `shots_faced`; closing/reachable by new `n_defended_actions` column, added to mart + contract + constants) |
| N2 | Note | LOV test hard-coded 3 placeholders | Assertion derives the expected string from `GK_TRACKING_PROVIDERS` |
| N3 | Note | Stale silly-kicks reference | Task 11.1 updated (4.22.2 rejected; 4.23.0 staged, wheel 0.5.35) |
| N4 | Note | Self-review said "§9 reviewer decides" | Updated: §9 RESOLVED, no open implementer decisions |
| N5 | Note | `empty_message=""` semantics unverified | New Step 8.1b: verify `build_page` empty-string handling against `page_template.py` |

## Deferred to post-implementation (plan Task 9)

`chart-choice-audit` re-run on the real page (mockup-stage baseline:
`docs/ui-cycles/gk-redesign/kirk-chart-audit.md`), `cognitive-interface-audit` (audit mode),
`optimization-audit` + EXPLAIN ANALYZE on the synced queries, AI-governance/citation/NOTICE
parity tests.
