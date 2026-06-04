# ADR-019: Three-Stage `dbt_build` for Same-Day Gold-Reader Compute

| Field | Value |
|---|---|
| **Date** | 2026-05-01 |
| **Status** | Accepted |
| **Implementation status** | PR-α merged 2026-05-01 (mart tags + classification conformance test); PR-β merged 2026-05-02 (TF restructure + gold-read conformance test + 3 workflow cards) |
| **Deciders** | Karsten S. Nielsen |

## Context

PR #242 (PR-Cycle-B, 2026-05-01) closed 4 overnight CI failures + 6 session-69 hardening gaps + an 11-file SDK module-level-import architectural fix. While doing the DAG audit for #242, we identified an undocumented architectural drift: most gold-reader compute tasks (`compute_pitch_control`, `compute_off_ball_xt`, `compute_xg_model[_v2]`, `compute_formations_efpi/shape_graph`, `compute_embeddings_v2`) read **yesterday's** gold marts because `dbt_build` runs at the END of the daily-job DAG (after compute). New matches/data therefore appear in those compute outputs **one day late**.

ADR-017 (2026-04-29) documents one carve-out for `run_model_validation` reading yesterday's gold by design. That carve-out was a workaround for the single-stage `dbt_build` architecture: any `run_model_validation → dbt_build` edge would let validation regressions block today's mart refresh. Three-stage architecture removes the workaround's need by topology — see §5.

## Decision

Replace the single `dbt_build` Databricks task with **three** sequential dbt invocations, governed by a per-mart classification tag in each model's `{{ config(...) }}` block:

```
ingest_*  →  dbt_build_input_marts  →  compute_phase_1  →  dbt_build_intermediate_marts
                                                                                       ↘
                                            compute_phase_2  →  dbt_build_output_marts  →  refresh_synced_tables
                                                                                       ↘
                                                                                          run_model_validation
```

Where:
- `dbt_build_input_marts`: builds dimensions + marts built only from ingest output (e.g., `gold.fct_tracking_frames` from bronze tracking)
- `dbt_build_intermediate_marts`: builds marts that compute reads but that themselves depend on **other** compute output (e.g., `gold.fct_action_values` from `bronze.{spadl_actions, vaep_action_values}` written by `compute_spadl_vaep`)
- `dbt_build_output_marts`: builds remaining marts (built from compute outputs and consumed only by apps/dashboards/HF/`run_model_validation`)

## Mart taxonomy

Every mart gets exactly one of four tags (in addition to the inherited `marts` tag from `dbt_project.yml`):

| Tag | Definition | Stage | Count (PR-α) |
|---|---|---|---|
| `dimension` | Pure conformed dimensions; no compute task in lineage | 1 | 4 |
| `input_mart` | Built only from ingest output (no compute task in lineage); may or may not be compute-consumed | 1 | 3 |
| `intermediate_mart` | Has compute output in lineage AND consumed by at least one compute task | 2 | 1 |
| `output_mart` | Has compute output in lineage; not consumed by any compute task | 3 | 32 |

Locked classification (PR-α):

- **dimension** (4): `dim_competitions`, `dim_matches`, `dim_players`, `dim_teams`
- **input_mart** (3): `fct_tracking_frames`, `fct_shots`, `fct_discipline_events`
- **intermediate_mart** (1): `fct_action_values` (built from `compute_spadl_vaep` bronze; read by `compute_embeddings_v2`)
- **output_mart** (32): every other `fct_*.sql`

Note on the strict `input_mart` definition: `fct_passes`, `fct_match_summary`, `fct_physical_stats` were originally proposed as `input_mart` candidates because compute reads each of them in the conventional sense. They are tagged `output_mart` because their lineage contains compute-output bronze (`line_breaking_results` and `off_ball_xt_results`) via LEFT-JOIN enrichment columns. Stage-3 placement gives those enrichment columns same-day freshness — which is the entire point of the cycle. None of the gold-reader compute tasks read these three marts directly (verified against `_BRONZE_READ_REQUIREMENTS` in `src/tests/test_workflow_dag_bronze_reads.py`), so moving them to stage 3 does not break compute scheduling.

Enforcement: `src/tests/test_dbt_mart_classification.py` asserts at PR-CI time that every mart has exactly one tag and that the tag matches the lineage (input/dimension marts have no compute-output bronze; intermediate marts must be in the `_COMPUTE_READ_MARTS` registry).

## "Compute reads today's gold" principle

Any Databricks task that reads a `gold.fct_*` table reads **today's** gold (built earlier in the same daily-job run). **No exceptions in the new architecture.** ADR-017's pre-three-stage carve-out for `run_model_validation` is supplanted by the new topology — validation depends on `dbt_build_output_marts` (so reads today's gold) and runs as a sibling of `refresh_synced_tables` (so a validation regression cannot block synced-table refresh). The "signal not gate" guarantee is preserved by **structure**, not by stale reads.

## ADR-017 supersession

ADR-017's yesterday-gold carve-out for `run_model_validation` was a workaround for the single-stage `dbt_build` architecture. Three-stage replaces it with topology: validation is a sibling of `refresh_synced_tables` (both children of `dbt_build_output_marts`), so a validation regression cannot transitively block synced-table refresh. The "signal not gate" principle is preserved by **structure**, not by stale reads.

ADR-017 receives an "Amended" header line referencing ADR-019; the original narrative remains intact for historical context (PR-LL2 close-out forcing function).

## Treatment of ingest-helper compute tasks

`extract_tracking_metadata` is labeled a `compute_*` task in TF but functions as an ingest helper: it reads tracking bronze, derives metadata, writes `bronze.tracking_player_metadata`, and is a hard dependency of stage 1 (`fct_tracking_frames` reads `tracking_player_metadata`). Its bronze output is therefore available to stage-1 input_marts.

The classification conformance test exempts `tracking_player_metadata` from `_COMPUTE_OUTPUT_BRONZE_TABLES` for this reason. Future ingest-helper compute tasks that follow the same pattern (read bronze → write bronze, hard-dependency of stage 1) are similarly exempt. The exemption is documented inline in the test alongside the curated set.

## Migration sequence

- **PR-α** (this cycle's first PR) — adds tags to all 40 marts + classification conformance test + career mart v1 filter (deferred from PR #242) + ADR-019 itself + ADR-017 amendment + spec doc commit. Behaviour-neutral: TF still has the single `dbt_build` task. Tags are pure metadata until PR-β.
- **PR-β** (this cycle's second PR) — TF restructure into three dbt tasks; reorders compute task `depends_on`; removes 13 stale gold-reader edges (per PR #242's audit); adds `run_model_validation → dbt_build_output_marts` edge; adds `src/tests/test_workflow_dag_gold_reads.py` peer to the bronze-read conformance test from PR #242.

## Alternatives considered

| Option | Pros | Cons | Why rejected |
|---|---|---|---|
| **A.** Bronze-direct refactor of compute tasks (read bronze instead of gold) | Same-day freshness without dbt restructure | ~7 compute tasks need rewriting; loses dbt's schema-enforced inputs; duplicates ID-normalization | Larger refactor surface than B; bronze-direct compute loses the gold-mart benefits from ADR-013 |
| **B.** Two-stage strict, accept 1-day-lag for `embeddings_v2 → fct_action_values` | Simpler than three-stage | Forecloses on more `intermediate_mart` cases as ML pipelines compose (e.g. an OBSO-derived feature flowing to a downstream embedding model) | User direction (per spec brainstorm): "very likely to not be the only task long term" |
| **C.** Two-stage strict, accept the lag for ALL gold-reader compute | Zero structural change | Defeats the cycle goal (lag was the whole motivation) | Rejected by user |
| **D.** Three-stage **(chosen)** | Same-day freshness for all gold-reader compute; forward-compatible with future intermediate-mart cases | 3 dbt invocations/day (~5 min added wall-clock); mart classification adds discipline | — |
| **E.** Keep ADR-017's yesterday-gold carve-out for `run_model_validation` unchanged | No ADR-017 amendment needed | Three-stage makes the carve-out strictly worse than topology-based "signal not gate"; would leave stale reads on the table for no architectural reason | — |
| **F.** Keep `fct_passes` / `fct_match_summary` / `fct_physical_stats` as `input_mart` despite compute-output ancestors | Matches the initial intuition that compute reads them | Violates the strict `input_mart` definition; their LEFT-JOIN enrichment columns would silently lag by 1 day even under three-stage; defeats the cycle goal for those columns | Rejected during PR-α implementation when the conformance test surfaced the lineage discrepancy |

## Consequences

### Positive

- Same-day freshness for **all** gold-reader compute tasks, including `run_model_validation`. New matches in today's ingest produce today's xG predictions, today's pitch control, today's formations, today's embeddings inference, today's validation signals.
- Same-day freshness for LEFT-JOIN enrichment columns on `fct_passes` (line_breaking), `fct_match_summary` (transitively), and `fct_physical_stats` (off_ball_xt). Previously these columns lagged by 1 day even though the rest of the row was fresh.
- Mart classification taxonomy provides a single audit-friendly place to ask "which stage builds this mart". Adding a new mart is a 1-line `tags=[...]` decision; the conformance test enforces it.
- ADR-017's "signal not gate" principle is preserved by topology (sibling positioning) rather than by stale reads — a structural improvement.
- New `intermediate_mart` cases (future ML pipeline composition) just register one entry in `_COMPUTE_READ_MARTS` and inherit the existing 3-stage flow.

### Negative

- 3 dbt invocations per day instead of 1. Each invocation has a fixed warehouse warmup + parse cost (~1-2 min); combined ~5 min added wall-clock. Daily-job has a 4-hour budget; well within.
- Mart classification adds discipline: every new mart requires a tag + a justification. The conformance test catches missing tags at PR-CI time.
- ADR-017's narrative is now partially historical (the yesterday-gold workaround it documented is supplanted). The "Amended" header line preserves the original context.

### Neutral

- Wheel-resident library code is unchanged. PR-α is dbt models + tests + docs; PR-β is TF + tests.
- Daily-job behaviour during PR-α is identical to today. PR-α is purely metadata + documentation.

## CLAUDE.md Amendment

None. The classification taxonomy is enforced by `test_dbt_mart_classification.py` and documented in this ADR; CLAUDE.md doesn't need a new bullet.

## Related

- **Predecessor**: PR #242 (PR-Cycle-B) — surfaced the 1-day-lag class; deferred career mart fix
- **Spec**: `docs/superpowers/specs/2026-05-01-option-b-three-stage-dbt-build-design.md`
- **Plans**: `docs/superpowers/plans/2026-05-01-pr-alpha-three-stage-mart-tagging.md` (PR-α); PR-β plan TBD after PR-α merges
- **Conformance tests**: `src/tests/test_dbt_mart_classification.py` (PR-α); `src/tests/test_workflow_dag_gold_reads.py` (PR-β)
- **Sibling ADRs**:
  - ADR-017 — Model validation as signal not gate (amended by this cycle; the yesterday-gold carve-out is supplanted)
  - ADR-013 — ML inference outputs in dbt mart (governs the bronze→gold flow that this cycle restructures)
  - ADR-002 §6 — overwrite-writer schema drift guard (precedent for declarative metadata + conformance test)
  - ADR-018 — cross-table format contract testing (same enforcement pattern)

## Notes

The user explicitly chose three-stage over two-stage on the rationale that more `intermediate_mart` cases will likely emerge as ML pipelines compose (e.g. an OBSO-derived feature flowing to a downstream embedding model). Locking in the three-stage pattern now avoids a future cycle that would otherwise re-introduce the migration cost.

The reclassification of `fct_passes` / `fct_match_summary` / `fct_physical_stats` from the initially-proposed `input_mart` to `output_mart` was a strict-spec-compliance call made during PR-α implementation: when the semantic conformance test flagged compute-output bronze in their lineage, the choice was between (a) reclassifying or (b) relaxing the spec definition. The user's direction was "rip off the band-aid now and do this properly" — option (a). Option F in §Alternatives Considered captures the rejected (b) path.

## Amendment (2026-06-04): full model coverage — the staging/intermediate orphan gap

**Problem.** PR-β's stage 3 selector was `--select tag:output_mart` with **no leading `+`**, on the documented assumption that "all staging ancestors were built by stages 1 + 2." That assumption is false. dbt staging/intermediate models that are selected by **no** stage are never built by the daily flow — and because staging is materialized as **views** (a view's column list is frozen at creation), an additive schema change to such a model's SQL never takes effect on the live object. Two classes were orphaned:

1. **Output-mart-only staging** — staging that feeds ONLY an output mart (e.g. `stg_action_context__values` → `fct_action_context`). Stage 3's `tag:output_mart` (no `+`) never pulled them; stages 1–2 never reach them.
2. **Leaf staging/intermediate with no dbt consumer** but read externally (e.g. `stg_pitch_control__values`, consumed by the HF pitch-control publisher + the Taipy app; `stg_statsbomb__360`, consumed by the 360 training-data prep). Referenced by no `ref()`, so no `+tag:` selector reaches them.

This surfaced when the GradientSports period-relative fix (ADR-040 / PR #339) full-refreshed the marts: `fct_action_context` failed with `UNRESOLVED_COLUMN.WITH_SUGGESTION` on `gk_closing_time_mean_s__near_post` — a column added to `stg_action_context__values`'s SQL by PR #337 that the **stale live view** never exposed. A coverage audit found **22** orphaned models total.

**Decision (amendment).** Stage 3 now selects `+tag:output_mart path:models/staging path:models/intermediate --exclude +tag:input_mart +tag:dimension +tag:intermediate_mart`. Stage 3 runs LAST (after all ingest + compute), so every bronze source is available; it builds **output marts + ALL staging + ALL intermediate, minus everything stages 1+2 already built** (those marts + their ancestors). This makes `union(stage1, stage2, stage3) == every model` **by construction** — no model can be orphaned. The redundant-rebuild cost is nil: shared ancestors are subtracted by `--exclude`; only output-mart-only + leaf models (mostly cheap views) are added.

**Guard.** `src/tests/test_dbt_stage_selector_coverage.py` resolves the three stage selectors against the dbt ref-graph and asserts the union covers every model. A future model that no stage builds fails this test at PR-CI time. (`dbt_runner._SELECTOR_TO_CARD` and `test_terraform_workflow_dbt_task` updated to the new stage-3 selector.)

**Consequence.** The stale live views are only healed when the amended flow RUNS — the post-deploy validation run of this change rebuilds them (and resolves `fct_action_context`). No separate manual rebuild is needed.
