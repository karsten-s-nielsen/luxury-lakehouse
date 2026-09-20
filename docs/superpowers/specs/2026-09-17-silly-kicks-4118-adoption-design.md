# silly-kicks 4.118.0 Full Adoption — Design Spec

- **Date:** 2026-09-17
- **Status:** Draft — pending independent review
- **Author:** implementation session (Claude Opus 4.8)
- **Scope decision (owner-set, final):** ONE cycle, ONE feature branch, minimal commits, squash-merged. Adopt silly-kicks **4.90.1 → 4.118.0** in full: bump + every new metric family + all forced recomputes/re-fits, **over the existing match corpus only**. Also fixes the two currently-red scheduled CI jobs (Data Quality CI, Lakebase Maintenance).
- **Explicitly out of scope:** ingesting the newly-added pining-for-the-data SB360/SkillCorner matches (deferred; recompute runs over the current corpus). Demoted sk metrics (territorial_defense, TF-60 Layer-3 deterrent arms) have no public surface and are not adopted.

## 1. Motivation

Production runs silly-kicks 4.90.1. Local `karstenskyt__silly-kicks` main is at **v4.118.0** (28 releases ahead). The interval bundles ~20 feature PRs — eight new analyst-facing metric families, several forced recomputes of existing lakehouse outputs, a handful of breaking API changes, and a large block of transparent perf/infra work. This cycle brings the lakehouse fully onto 4.118.0 and materializes the new capability over the existing corpus.

Bumping is not optional-piecemeal: silly-kicks is the SPADL/VAEP/xT/tracking engine behind the ingestion writers, the Action-Context (AC) drain, the tracking-marts drain, and the training scripts. Once the pin moves, several existing outputs change value, so the bump and the recomputes must ship as one coherent state.

> **ADR reference convention:** `ADR-NNN` citations are **lakehouse-repo** ADRs by default (ADR-013 ML-output, ADR-043 strand-safe re-derive, ADR-046 env-pins, ADR-072 publish seam, ADR-082 tracking-marts drain, ADR-014 HF cards). ADR numbers that appear inside a silly-kicks feature description (sourced from the sk CHANGELOG — e.g. the turnover-sort ADR-065, batched-gkdv ADR-075, elastic-v2 ADR-093, ghost re-fit ADR-089) are the **silly-kicks** series and are collision-prone with lakehouse numbers. The plan must resolve each cited ADR to its repo explicitly.

## 2. Change classification (4.90.1 → 4.118.0)

Derived from the silly-kicks CHANGELOG, entries 4.91.0–4.118.0, cross-checked against lakehouse usage.

### 2.1 New metric families to materialize (8)

| # | metric | sk ver | grain | injects | notes |
|---|--------|--------|-------|---------|-------|
| 1 | `team_metrics` | 4.115.0 | (game_id, team_id) | xG (optional) | +44 glossary cols; pressing/progression/build-up KPIs |
| 2 | `match_outcome` | 4.118.0 | (game_id, team_id) | xg_model_v3 per-shot xG | win/draw/loss, xpoints, expected_goals; Poisson-binomial + Dixon-Coles |
| 3 | `shot_stopping` | 4.107.0 | (goalkeeper, game_id) | PSxG | GSAA / Goals Prevented; **reconciles with existing `fct_gk_shot_stopping`** (§5.3) |
| 4 | `gk_decision` | 4.113.0 | (keeper, game_id) | PassCompletionModel (bundled) | GK build-up decision quality; native (SkillCorner GI) + reconstruction (SB360/full-tracking) tiers |
| 5 | `territory` (TF-54) | 4.108.0 | (player, match) | fitted xT | trimmed-hull territorial dominance (12 cols) |
| 6 | `duels` (TF-55) | 4.108.0 | (game_id, player_id) | — | Glicko-2 duel ratings; **stateful** (resume seed / chronological) |
| 7 | `xsuccess` + `VAEP_adjusted` | 4.116.0 | action | bundled xSuccess weights | outcome-bias-free VAEP variant; re-scores existing VAEP, no retrain |
| 8 | `restdefense` L1+L2 | 4.102.0 / 4.103.0 | action grid (tracking) | fitted xt (L2 only) | descriptive rest-defense structure KPIs; L3 deterrent arms demoted (not adopted) |

### 2.2 Forced recomputes of existing outputs (over current corpus)

- **AC drain re-materialize** — driven by:
  - `elastic_sync` (4.114 TF-57): values move + 3 new `elastic_receive_*` columns + breaking `ElasticSyncParams` / `add_elastic_sync` signature (greedy weights removed).
  - SB360 GK-domain tracking features NaN → VALUES (4.100).
  - position-only variants bundled + ghost `default` re-fit (4.93): SB360 xShot/xCross/ghost NaN → VALUES; ghost served positions move up to ~0.75 m.
  - ghost-GK both-axes convention fix + away-keeper y correction (4.111): all 5 ghost variants re-fit; away-team keeper ghost/gkdv values change.
  - Schema change (3 new columns) → follow the `[AC-recompute-new-column]` procedure: wipe bronze → `run_now [preflight, compute]` → rebuild staging → `rederive_synced_marts --rebuild` (~5.5 h; 4.98 ghost-GK numba, 11.8×, shortens it).
- **Tracking-marts drain** (ADR-082 `compute_tracking_marts`) — re-enable **gkdv** on the batched arms (4.97 = the GKDV-PERF lever: `delta_das_batch` / `delta_threat_suppression_batch`, ~90×/unit; gkdv had never completed a run) and re-materialize off_ball_runs + defensive_credit. gkdv values also move (4.111 away-keeper; 4.93 SB360 ghost arm).
- **xt_gk_v2 re-fit + re-materialize** — `EmpiricalTurnoverValue.fit` (V_opp) now sorts to `(game_id, period_id, time_seconds, action_id)` before the positional scan (4.92, ADR-065). Value-changing on the unordered mart corpus the lakehouse fits on. Re-fit via `train_xt_gk_v2_hf` and re-materialize `xt_gk_v2_predictions`.
- **Downstream trainers — NOT forced, CONDITIONAL on a materialization-version assumption.** `train_xg_v3` trains on raw `bronze.shot_freeze_frames` (unchanged by the bump). scoutgpt / football2vec read the action corpus. std SPADL output is byte-identical **across the 4.91–4.118 interval** (no changelog entry moves std SPADL values in that range; atomic is unused). VAEP has no default-config retrain trigger (all additive; the changed xfns are not in any default list). Only `xt_gk_v2` re-fits — **provided** the current corpus is already materialized under silly-kicks ≥ 4.89.0. **ADR-065 (sk 4.89.0) genuinely renumbered `action_id` (a join key) for sportec/IDSSE/wyscout** — one release below prod pin 4.90.1. The lakehouse's last full adoption shipped under 4.90.1 (retrain + tracking-marts drain, #546/#547), so the corpus IS ≥ 4.89.0-materialized and `action_id` is already renumbered → scoutgpt/football2vec inputs are stable through §6's re-materialize. **Confirm this against LIVE bronze provenance before relying on it** (see §11); if the corpus predates 4.89.0, §6 IS the ADR-065 re-materialize and scoutgpt/football2vec MUST re-fit.

### 2.3 Breaking API adoptions (lakehouse code edits)

- **`elastic_sync` signature (AC drain path — MANDATORY)** (4.114): `add_elastic_sync` / `ElasticSyncParams` kwargs changed, no shim, fail-loud. AC `enrich.py` already calls **sk's** `add_elastic_sync`; the bump forces adopting the new signature + the 3 new `elastic_receive_*` output columns into the AC schema. Not optional — the import/call breaks otherwise.
- **The SECOND, standalone elastic path is a separate reconciliation (§5.4).** CORRECTION to a prior-draft error: `src/analytics/elastic_sync.py` is **NOT vestigial** — it is the lakehouse's own simplified/greedy ELASTIC (old arXiv:2508.09238), imported by `src/ingestion/elastic_sync.py:121` (`align_events_to_frames`, `ElasticSyncParams`), which produces `bronze.elastic_sync_results`, consumed by `ingestion/pausa.py`, `scripts/publish_obso_pausa_inputs_hf.py:68` (INNER JOIN), the AC oracle fixture (`scripts/extract_action_context_fixture.py`), and dbt idsse sources. It is on the LOCAL greedy impl (not sk), so the sk bump does not force it to change. Whether to migrate this chain onto sk's TF-57 v2 is a scope decision — see §5.4.
- **`keeper_identity` import-path move** (4.106) — `silly_kicks.tracking.resolve_keeper_identities` / `KeeperIdentity*` / `add_defending_gk_player_id` / `apply_keeper_identities_to_frames` → `silly_kicks.keeper_identity`. Verified the lakehouse does **not** import these symbols directly, so no forced edit; re-verify at implementation time in case a new metric writer needs them.
- **`space_creation` one-team-frame raise** (4.99) — a one-team frame without `visible_area` now raises. Confirm no lakehouse call site hits this; if any does, thread `visible_area` or a two-team frame.
- **Additive trailing-column widenings** — `compute_defensive_credits` long-form gained `origin_x` / `origin_y` / `region_radius` (4.99). Column-name readers unaffected; confirm the defensive_credit writer reads by name.

### 2.4 Transparent (no lakehouse action)

ghost-GK serving numba (4.98, bit-identical), CI wall-clock / scale-guard harness (4.96 / 4.95, tests-only), version single-source (4.101), position-only Hub variant (4.94), receiver model + cover-shadow re-tune (4.91, no library change), model-publish hardening (4.110, scripts-only), sweeper ghost variant + GS keeper-clamp diagnostic (4.105, additive), TF-19 physics-arm probes (4.104, reported-not-gated), ghost-outfield model (4.109, infra with no consumer), territorial_defense (4.112) and TF-60 Layer-3 arms (4.111) both DEMOTED. Dependency floor: `ruthless-efficiency` 0.4.0 → 0.6.0 (transitive via silly-kicks; picked up by `uv lock`).

## 3. Mechanical bump surface

> **Target pin RESOLVED (2026-09-20) — silly-kicks `4.120.0`.** The cycle pins **4.120.0** (owner: include both further releases). 4.120.0 is additive over the 4.118.0 metric tip and contains: SK-EXPORT `metric_contracts.METRIC_CONTRACTS` (4.119.0), the `secured_reception` robust-sort packing fix (4.119.1 — unblocks the AC `add_packing` on non-chronological persisted bronze), and TF-54b Path B (4.120.0) = the `territory` counterfactual "threat-prevented" method (a NEW metric surface — see P1: territory materializes both `completed_failed` + `counterfactual`; +4 cf cols; ranking-LICENSED per the sk census; `PassCompletionModel.bundled()` re-fit is transparent for the new gk_decision). The per-feature release numbers in §2 (match_outcome 4.118.0, elastic 4.114, …) are unchanged historical facts.

All must move 4.90.1 → 4.120.0 in lockstep (`[sk-bump-sentinels]`):

- **7 trainer `_REQUIRED_SK_MIN`**: `scripts/train_{football2vec,football2vec_360,football2vec_v2,scoutgpt_hf,vaep_model_hf,xg_v3_hf,xt_gk_v2_hf}.py`.
- **6 ingestion `_REQUIRED_SK_MIN`**: `src/ingestion/{bravery_writer,defensive_credit_writer,exec_visibility,gkdv_writer,off_ball_runs_writer,xt_gk_v2_writer}.py`.
- **Terraform env pin**: `terraform/modules/workflows/main.tf` `silly-kicks[das,ghost-gk,parse-dfl]==4.120.0` (ADR-046; via `scripts/sync_tf_env_pins.py`, never hand-edited).
- **pyproject floor**: `silly-kicks[das,ghost-gk,parse-dfl]>=4.120.0,<5`.
- **uv.lock**: `uv lock --upgrade-package silly-kicks` (also pulls ruthless-efficiency 0.6.0 + any transitive moves).
- **`scripts/submit_ac1_oneshot.py`** PEP 723 pin `silly-kicks[...]==4.120.0`.
- **Hardcoded sentinel tuples** to update to `(4, 120, 0)`: `src/tests/test_sk3_mig_b_orchestrator_invariants.py` (`expected` + the `§2.10.5` header/comment) and `src/tests/test_train_xt_gk_v2.py`. `test_executor_env_guard.py` asserts equality to the pyproject floor (no literal to change, but must pass).
- Re-run `test_terraform_env_dep_parity`, `test_executor_env_guard`, `test_sk3_mig_b_orchestrator_invariants` after the sync.

Follow the uv silent-downgrade guidance: no explicit sk pin in PEP 723 scripts beyond the wheel `[spadl]` extra where possible; `submit_ac1_oneshot.py`'s explicit pin keeps its runtime `_REQUIRED_SK_MIN`-style assert.

## 4. Mart architecture (wide-by-granularity)

New marts follow the ADR-013 ML-output pattern (Python writer → bronze raw → dbt staging view → gold mart with `contract: enforced: true`; surrogate keys resolve in the mart via INNER JOIN on the identity fact; writers emit only native ids + values). Grain-bucketed:

- **team-match** — new `fct_team_metrics`: `team_metrics` (44 cols) + `match_outcome` (win/draw/loss, xpoints, expected_goals). `fct_match_summary` is match-grain, not team-grain, so a new mart is correct.
- **keeper-match** — `shot_stopping` (§5.3 reconciliation with `fct_gk_shot_stopping`) + `gk_decision` (new columns, same grain). Reuse the `defending_gk_player_key` identity resolution already in `fct_shot_psxg`.
- **player-match** — new `fct_player_match_metrics`: `territory` (12 cols) + `duels` (Glicko-2 ratings + counts). Distinct from the season/career-grain `fct_player_stats`.
- **action** — `xsuccess` + `VAEP_adjusted` as new columns on `fct_action_values` (VAEP variant; no new mart).
- **action-grid tracking** — `restdefense` L1+L2 as new columns in the AC drain output / a sibling tracking mart (decide during plan: extend `fct_action_context` vs. new `fct_rest_defense`; leaning new mart to avoid widening the AC contract for a distinct concept).

Per-metric bronze writers live in `src/ingestion/` (event-only metrics) or the AC/tracking drains (tracking metrics), importing canonical id helpers from `src/shared/identifiers.py` and stamping `access_tier` per ADR-064.

## 5. Per-metric adoption detail

### 5.1 Event-only metrics (team_metrics, match_outcome, territory, duels, shot_stopping, xsuccess/VAEP_adjusted)

Read the refreshed gold action corpus (post AC re-materialize). Each: a `src/ingestion/<metric>_writer.py` that calls the sk `compute_*`, injects the required model column (xg_model_v3 for match_outcome; PSxG for shot_stopping; fitted xT for territory), writes bronze, then dbt staging + gold mart. `duels` requires ordered per-`game_id` processing and a resume seed — the writer processes matches in ascending `game_id` and persists `final_ratings` as the resume state (or recomputes the full corpus each run; decide in plan — full recompute is simpler and the corpus is bounded).

### 5.2 Tracking metrics (gk_decision, restdefense)

`gk_decision` native tier reads SkillCorner GI passing-option rows; reconstruction tier consumes SB360 + full-tracking via the bundled PassCompletionModel (reachability default 0.85). `restdefense` L1+L2 sample at the in-possession action grid. Both run inside the tracking-marts drain (ADR-082 fan-out) or a new drain task; decide in plan.

### 5.3 shot_stopping reconciliation (DECISION)

`fct_gk_shot_stopping` + `fct_gk_shot_stopping_pooled` already compute GSAA (`goals_prevented = psxg_faced − goals_conceded`) as an inline SQL rollup of `fct_shot_psxg`. **`fct_goalkeeper_stats.sql` ALSO computes `goals_prevented` / `psxg_faced`** (the spec's D-B note says `fct_gk_shot_stopping` "replaces the inlined psxg_agg CTE in fct_goalkeeper_stats" — confirm whether that CTE is fully removed or still present), so the reconciliation surface is **all three** marts, not just the `fct_gk_shot_stopping(_pooled)` pair. The sk 4.107 `shot_stopping` package computes the same metric but adds: canonical-id keeper grouping (fixes the known `fct_gk_shot_stopping_pooled` NULL-season IDSSE-drop bug — see the open GK-insight bug), `_excl_penalties` companions, and an honest attribution census. **Recommendation:** adopt the sk `shot_stopping` writer as the source feeding the keeper-match mart, superseding the inline SQL rollup (delete-and-depend), keeping the `fct_gk_shot_stopping(_pooled)` + `fct_goalkeeper_stats` mart names/shapes where downstream consumers depend on them (Hyrum) but re-sourcing the GSAA columns from the sk output and fixing the IDSSE join. Reviewer to ratify supersede-vs-parallel across all three marts.

### 5.4 elastic_sync reconciliation (DECISION — was mis-stated as a trivial delete)

Two elastic paths exist (§2.3): (A) the **AC drain** uses sk's `add_elastic_sync` — the bump MANDATORILY moves it to TF-57 v2 (signature + values + 3 new `elastic_receive_*` cols → AC re-materialize, already in §6.2); (B) a **standalone** `ingestion/elastic_sync.py` on the LOCAL greedy `analytics/elastic_sync.py`, producing `bronze.elastic_sync_results`, consumed by PAUSA (`ingestion/pausa.py`), the OBSO/PAUSA HF publish (`publish_obso_pausa_inputs_hf.py` INNER JOIN), the AC oracle fixture, and dbt idsse sources. The bump does not force (B) to change.

**Option B1 — full delete-and-depend (recommended, matches the "adopt all" intent):** migrate `ingestion/elastic_sync.py` onto sk's TF-57 v2 `align_events_to_frames`, delete `analytics/elastic_sync.py` + its tests, re-materialize `bronze.elastic_sync_results` (values move; possibly +columns), rewire the PAUSA/OBSO consumers + AC-oracle fixture + dbt idsse source + any guard-module registration, and re-materialize the downstream PAUSA/OBSO outputs. Coherent (one elastic engine repo-wide) but materially enlarges the cycle.

**Option B2 — narrow (AC path only):** adopt v2 in the AC drain (mandatory), leave the standalone `bronze.elastic_sync_results` chain on the local greedy impl. Smaller, but the lakehouse then runs two different elastic aligners (v2 for AC, greedy for PAUSA/OBSO) — an inconsistency to document.

**DECIDED — B1 (owner, 2026-09-17):** full delete-and-depend. Migrate `ingestion/elastic_sync.py` onto sk's TF-57 v2 `align_events_to_frames`, delete `analytics/elastic_sync.py` + its tests, re-materialize `bronze.elastic_sync_results`, rewire the PAUSA/OBSO consumers + AC-oracle fixture + dbt idsse source + any guard-module registration, and re-materialize the downstream PAUSA/OBSO outputs. One elastic engine repo-wide. Sequenced in §6.

**GRAIN + PRODUCER sub-decision — Option B-ac / action-grain, source-from-AC (owner, 2026-09-20; supersedes the intermediate B-std draft).** Implementation revealed the plan's "map the columns" was under-specified on two axes — grain AND producer:

- **Grain.** sk's `align_events_to_frames(actions, frames, *, params)` consumes **SPADL actions** (`action_id, game_id, period_id, time_seconds, player_id, team_id, type_id`), requires **sk-canonical frames** (`frame_id/period_id/time_seconds/x/y/z/ball_*/is_ball/team_id/possession_id/control/ball_state` — home-LTR, velocity-preprocessed), does **not** support SB360 freeze-frames, and emits **`action_id`-keyed** rows with 7 columns (`elastic_frame_id/_confidence/_error_seconds` + `elastic_receive_frame_id/_confidence/_error_seconds`). The legacy standalone `elastic_sync_results` was `(match_id, event_id)`-keyed off raw `idsse_events` with `frame_id / alignment_confidence / alignment_error_seconds`. **Owner chose action grain** ("done properly": one engine AND one grain).
- **Producer (the deeper finding).** A re-keyed *standalone* producer (`wf-elastic-sync`) would have to rebuild the AC drain's per-match `MatchMeta` (home_team_id + DFL direction-of-play flags) + `_convert_tracking_batch` frame conversion and then run `align_events_to_frames` a **second time** on IDSSE — while the **AC drain already writes** those exact 7 action-keyed elastic columns to `fct_action_context` for IDSSE (same engine, same grain, same corpus). So a standalone action-grain `elastic_sync_results` is pure duplication. **Owner chose B-ac — retire the standalone producer and SOURCE elastic from the AC drain's output.** One engine, one grain, **one producer**; the values are identical because they *are* the AC drain's. **Codified in ADR-084** (`docs/superpowers/adrs/ADR-084-elastic-retire-standalone-source-from-ac-drain.md`).

Scope (in-scope, this cycle):
1. **Delete** `src/analytics/elastic_sync.py` + its tests (the local greedy engine is gone).
2. **Retire the `wf-elastic-sync` workflow entirely:** delete `src/ingestion/elastic_sync.py`; remove its mega-job task (`terraform/modules/workflows/main.tf`), workflow card, `dbt_project/seeds/task_workflow_mapping.csv` entry, card-map, and any `_GUARD_MODULES`/guard registration.
3. **Drop `bronze.elastic_sync_results`** (idempotent `scripts/migrations/*.sql` `DROP TABLE IF EXISTS`, operator-applied with the merge) and remove the dbt idsse source decl (`_idsse__sources.yml`) + `stg_idsse__elastic_sync.sql` (or repoint `stg_idsse__elastic_sync` as a thin VIEW selecting the IDSSE elastic columns from `stg_action_context__values` to preserve the consumer name — plan decides).
4. **Re-point the 4 consumers to the AC drain's action-grain elastic columns** (`fct_action_context` / `stg_action_context__values`, IDSSE rows): `ingestion/pausa.py`, `scripts/publish_obso_pausa_inputs_hf.py` (INNER JOIN → the AC elastic source on `action_id`; payload from SPADL/AC columns — this changes the **published** OBSO/PAUSA HF dataset grain event→action, so its HF card + artifact-list refs update in the same PR, UX "HF artifact link completeness"), `scripts/extract_action_context_fixture.py` (elastic oracle now derived from AC), `src/tests/action_context/oracle_map.py` (legacy-oracle note).

Coverage: IDSSE SPADL actions with a resolved elastic anchor (from the AC drain) — events with no SPADL action drop out, as accepted for action grain. No `wf-elastic-sync` re-run in §6; elastic ships with the AC drain re-materialize.

## 6. Recompute & re-fit sequencing (single ordered operator run)

1. Land the bump + code adoptions (breaking APIs; the B1 elastic **B-ac** change — RETIRE `wf-elastic-sync`, delete `ingestion/elastic_sync.py` + `analytics/elastic_sync.py`, re-point PAUSA/OBSO/oracle/dbt to the AC drain's elastic columns, DROP `bronze.elastic_sync_results`) + new-metric writers/marts (code only; CI green).
2. **AC drain re-materialize** (schema + values): wipe AC bronze → `run_now [preflight, compute_action_context, compute_action_context_statsbomb, verify_action_context_drain]` → rebuild AC staging → `rederive_synced_marts --rebuild` for AC-derived marts.
3. **B1 elastic-chain re-materialize (action grain, source-from-AC — §5.4 Option B-ac)**: NO separate `wf-elastic-sync` run — the action-grain elastic columns arrive with the AC drain re-materialize (step 2). Apply the `DROP TABLE IF EXISTS elastic_sync_results` migration → re-materialize the PAUSA/OBSO downstream (`fct_pausa_*`, OBSO) reading the AC drain's elastic columns → refresh the OBSO/PAUSA HF publish (`publish_obso_pausa_inputs_hf.py`, now action-grain, sourced from `fct_action_context`).
4. **Tracking-marts drain**: re-enable gkdv (batched) + re-materialize off_ball_runs / defensive_credit / gkdv (+ `compute_gkdv_pool`).
5. **xt_gk_v2 re-fit** (`train_xt_gk_v2_hf`) + re-materialize `xt_gk_v2_predictions`.
6. **New-metric writers** run over the refreshed corpus → bronze → dbt build → gold marts.
7. **Synced-mart re-derive** (ADR-043 `rederive_synced_marts.py`, never `dbt --full-refresh` on a TRIGGERED mart) — also clears the stuck synced-table pending-update state (§8).
8. HF publishes for any new public datasets/marts (ADR-072 guarded seam; ADR-014 card parity).

Each operator recompute run is a discrete step gated on explicit human approval (§10). No "deploy and observe": every step verified against LIVE state before the next.

## 7. Governance, references, UI

- **AI governance** (`AI_GOVERNANCE.md` §5 + HF model cards + `governance:` YAML block + `PER_PLAYER_EVALUATIVE_CARDS` in `test_ai_governance_md.py`): the per-player-evaluative new families are **shot_stopping, gk_decision, territory, duels**. `team_metrics` / `match_outcome` are per-team, not per-player-evaluative → excluded (confirm against the test's definition). Note each metric's HONEST-LIMIT posture from the CHANGELOG (gk_decision: instrument not ranking; territorial_defense already demoted). Re-run `test_ai_governance_md.py`.
- **Academic references** (`ARCHITECTURE.md` Appendix D + `expected_authors` in `test_architecture_md_appendix.py`): Dixon & Coles (1997); Glickman (Glicko-2); Paul, Klemp & Memmert (2025); Power et al. (2017); Forcher et al. (2023); Sumpter / Twelve.football; plus any newly-cited authors in the adopted packages' `Citation`/`references`/`NOTICE`.
- **UI** (Taipy, `hf_taipy_app/`): grain-bucketed pages per the template (team KPIs; keeper decision + shot-stopping; player territory + duels; VAEP-adjusted comparison). Every displayed value carries scale + direction (0–1 ranges, higher/lower = better), goal-oriented nav labels, no raw IDs. `NOTICE` + `GLOSSARY` + `PAGE_TERMS` updated in the same change. HF artifact-link completeness across Space header/footer + org-card + README.

## 8. CI-failure fix (Data Quality CI + Lakebase Maintenance)

Both jobs fail on the **same** synced-table health check: ~25 synced tables in `detailed_state='SYNCED_TABLE_ONLINE'` (a pending update) instead of `SYNCED_TABLE_ONLINE_NO_PENDING_UPDATE`. Systemic and pre-dating this cycle's merges. Root cause to confirm live (a stuck/queued TRIGGERED-sync backlog is the leading hypothesis; `[lakebase-red-herring]` — the cause is ours, not Lakebase support). Fix path: diagnose the pending-update source, then settle via `scripts/delete_synced_table.py` + `scripts/migrate_synced_tables.py` (or `rederive_synced_marts`), which the cycle's §6 step 7 re-derive performs anyway. Acceptance: both scheduled jobs green. (Lakebase Maintenance also logged an early OIDC-vars `::error::` but continued past it — verify it is non-fatal or fix the missing `vars.HF_APP_SP_APPLICATION_ID` / OIDC vars separately.)

## 9. Testing strategy

- Full local 7-gate suite green after the bump (ruff, ruff format, lint-imports, bump_wheel --check, pip_audit_ignores --check, pyright over the CI target, full pytest) + pip-audit.
- Regenerate BOTH AC goldens (mini-golden CI gate + drain golden) after any AC-column sk bump.
- Re-run benchmarks (production-scale where the changed path is tracking-scale); no regression past budget.
- Per-new-metric: unit tests for the writer (schema, honest-NaN/census, canonical-id grouping), dbt contract tests, format-contract tests (`src/shared/identifiers.py` join coverage), `_marts__models.yml` completeness. Note: git-scope the completeness/model-inventory checks to the tracked tree — the vendored dbt copy under `.venv/` is a grep counting trap.
- Governance + appendix tests (`test_ai_governance_md.py`, `test_architecture_md_appendix.py`).
- Publisher seam conformance for any new HF dataset (ADR-072 gates).
- Watch every new guard fail first (`[guards-that-cannot-fail]`).

## 10. Commit structure & human-approval gates

Single feature branch off `main`; minimal, fully-tested coherent commits; squash-merge at the end. Anticipated commits (subject to the plan):

1. Bump + pins/sentinels + breaking-API adopt (elastic signature + delete-and-depend, keeper_identity/space_creation verification) — 7-gate green.
2. New-metric writers + bronze + dbt staging/marts + governance + refs + UI + HF cards — 7-gate green.
3. CI-failure fix (synced-table health) if code/config changes are needed beyond the operator re-derive.

**Approval gates (each requires explicit owner "yes"):** before each `git commit`; before each `git push`; before opening the PR; before EACH operator recompute/re-fit run (AC drain, tracking-marts drain, xt_gk_v2 re-fit, synced re-derive, HF publish); before the squash-merge (`--admin`). None of these are implied by approval of this spec or the plan.

## 11. Risks & open items (for the reviewer)

- **elastic_sync standalone chain (§5.4) — DECIDED B1** (owner, 2026-09-17): full delete-and-depend + PAUSA/OBSO re-materialize. No longer open; sequenced in §6.
- **Corpus materialization version (§2.2) — CONFIRM against LIVE bronze that the corpus is ≥ 4.89.0-materialized** (ADR-065 `action_id` renumber). If not, scoutgpt/football2vec re-fit is added to §6.
- **shot_stopping supersede-vs-parallel across THREE marts** (§5.3) — `fct_gk_shot_stopping`, `fct_gk_shot_stopping_pooled`, AND `fct_goalkeeper_stats` all carry GSAA columns; confirm the delete-and-depend surface.
- **restdefense mart placement** — extend `fct_action_context` vs. new `fct_rest_defense`.
- **duels statefulness** — full-corpus recompute each run vs. persisted resume seed.
- **AC schema change blast radius** — the 3 new `elastic_receive_*` columns + any restdefense columns; synced-table + contract + downstream consumers (Hyrum).
- **Recompute cost/time** — one AC drain (~5.5 h) + tracking-marts drain + xt_gk_v2 re-fit + new-metric writes; the owner is capturing a Databricks spend baseline before/after to size the cycle.
- **Governance membership** — confirm team_metrics/match_outcome are correctly excluded from `PER_PLAYER_EVALUATIVE_CARDS`.
- **ADR coverage** — this cycle warrants ADR(s): the new metric families, the elastic delete-and-depend, the shot_stopping supersession, and the sk-4.118 adoption umbrella. Enumerate in the plan.

## 12. Out of scope

- Ingesting the new pining SB360/SkillCorner matches (recompute is over the existing corpus only).
- Demoted sk metrics (territorial_defense; TF-60 Layer-3 deterrent arms).
- Any relaxation of the ADR-046 exact-pin discipline.
