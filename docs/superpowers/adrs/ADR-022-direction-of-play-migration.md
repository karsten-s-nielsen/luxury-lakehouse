# ADR-022: silly-kicks 3.0.x direction-of-play migration (lakehouse side)

| Field | Value |
|---|---|
| **Date** | 2026-05-02 |
| **Status** | Accepted |
| **Deciders** | Karsten S. Nielsen (human), Claude Opus 4.7 (AI) |

## Context

OPT-1's e2e probe of `compute_expected_threat` against `dev_gold.fct_action_values` (2026-05-02) exposed a 9× magnitude divergence between the existing global xT grid (max=0.116, sum=7.79, monotonic in x — written 2026-03-13) and a freshly-computed grid (max=0.035, sum=0.87, **U-shaped**). The U-shape — high values at x=0 AND x=105, low in midfield — pointed at `bronze.spadl_actions` carrying actions from BOTH teams' attacks at BOTH ends of the pitch, indicating broken direction-of-play normalization.

Investigation traced the bug to silly-kicks's pre-3.0.0 dual-mirror inversion. Two byte-identical mirror operations were applied sequentially during VAEP fitting: `_fix_direction_of_play` inside the converter and `play_left_to_right` inside `vaep.compute_features`. The two mirrors only line up correctly for one provider family at a time — possession-perspective providers (StatsBomb, Wyscout) had broken raw SPADL but correct VAEP gamestates (mirrors cancelled), while absolute-frame providers (Sportec, Metrica) had correct SPADL but broken VAEP gamestates. The 2026-03-13 monotonic xT grid was VAEP-pipeline-derived (mirrors-cancel path); the U-shaped current grid was raw-SPADL-derived (mirrors-don't-cancel path).

silly-kicks 3.0.0 (PR-S22, commit `a1ebfa0`) corrected the converter layer: `_fix_direction_of_play` removed; per-converter `to_spadl_ltr(input_convention=...)` dispatch; VAEP framework no longer applies the second mirror; new `tests/invariants/` directory codifies cross-layer geometric invariants. silly-kicks 3.0.1 (PR-S23, commit `d7f86de`) followed-up with a per-period correction for Sportec + Metrica converters that were declaring `ABSOLUTE_FRAME_HOME_RIGHT` but processing `PER_PERIOD_ABSOLUTE` data — discovered during the lakehouse-side SK3-MIG migration session via empirical fixture testing.

The lakehouse-side migration replaces the broken upstream library with the corrected one and force-rebuilds every coord-dependent dev_gold artifact under the new converter behavior.

## Decision

Adopt silly-kicks 3.0.1 in a single PR (SK3-MIG Group A) covering data correctness only — pin bump, call-site adaptation, force-rebuild of `bronze.spadl_actions` and downstream coord-dependent marts, wipe + recompute `expected_threat_grids`, and Lakebase synced-table refresh. Defer model retraining and HF dataset republishing to a separate single-PR follow-up cycle (SK3-MIG-B / Group B) tracked as a TODO On Deck row at Group A merge time.

The Group A → Group B window will see drift detection fire harmlessly (model predictions are biased — current weights against new SPADL coords). `fct_model_validation_baselines` rebases as part of Group B.

## Alternatives considered

| Option | Pros | Cons | Why rejected |
|---|---|---|---|
| A. Defer migration + suppress xT | Avoids any production-data risk; preserves status quo until model retrains land | Leaves ~99% of `fct_action_values` rows with wrong away-team coordinates; xT grid stays broken; every coord-dependent inference inherits the bug; surface area of "deferred" expands every day | Suppresses the symptom, not the cause; no path back to a coord-correct state without eventually doing Group A anyway |
| B. One mega-PR with retrains | Single drift-free baseline; `validation_baselines` rebases once; no period where some models train on new data and others on old | Long wall-clock to first ship; large coordination cost; harder to roll back individual pieces | Wraps too much value in one merge gate; failure of any retrain blocks the data-correctness fix |
| C. Two-phase: Group A correctness now + Group B retrains as follow-up (chosen) | Foundational correctness ships fast; Group B retrains independently validatable; smaller PRs; precedent for "naturally a PR-cycle" pattern (xG2 unblock, ScoutGPT promotion cycles, EV2) | Brief window where some models score on new SPADL coords with old weights — biased predictions; drift detection fires harmlessly | — |

## Consequences

### Positive

- Coord-correct SPADL + xT for all 4 sources (`statsbomb`, `wyscout`, `idsse`, `metrica`).
- silly-kicks 3.0.1's `home_team_start_left` kwarg requirement enforced at lakehouse boundary via two derivation helpers (`derive_idsse_home_team_start_left` reads DFL XML's `kickoff_team_left` from bronze; `derive_metrica_home_team_start_left` infers from period-1 SHOT positions).
- ADR-012 §2 grace-period for the v2→v1 XGBoost feature-list fallback closed; `xg_model_v2._parse_v2_envelope_features` extracted as testable module-level helper; trainer hardened to inject `tabular_dim` defense-in-depth.
- New invariant unit test `src/tests/test_sk3_coord_correctness.py` codifies the canonical SPADL LTR contract at the lakehouse boundary — catches future regressions whether they land in silly-kicks or in our adapters.
- `SILLY_KICKS_ASSERT_INVARIANTS=1` set in CI (python-ci.yml + dbt-live-ci.yml) AND production (via `src/ingestion/bootstrap.py` module-level `setdefault` — Databricks serverless `compute.Environment` doesn't support per-job env vars natively). Future input-convention regressions fail loud.
- `expected_threat_grids` global + per-comp wiped + recomputed under correct SPADL (OPT-1 streaming refactor handles the `need_global=True` path).

### Negative

- **Group A → Group B drift window:** model predictions in `fct_xg_predictions[_v2]`, `fct_defcon_*`, `fct_pausa_values`, `fct_player_embeddings*` are biased (current weights against new coords) until Group B retrains land. `fct_model_validation_baselines` will fire harmlessly during this window.
- **Tracking-adapter migration deferred:** silly-kicks 3.0.0 added `output_convention` kwarg to `silly_kicks/tracking/*.py` adapters with a default flip to LTR. Lakehouse currently has zero `silly_kicks.tracking.*` consumers (verified via grep), so no opt-out needed in this PR. When tracking adapters are eventually adopted, callers must explicitly choose `output_convention="absolute_frame"` or migrate to LTR.
- **XG1-RETIRE TODO row added:** the v2→v1 fallback removal makes `compute_xg_predictions` (v1) dead-code from the inference path; only Shot Map's display columns (`xg_logistic`, `xg_gradient_boosted`) still consume v1 output. v1 retirement queued as a separate PR (Wicked-sized — includes Shot Map UX migration decision).

### Neutral

- silly-kicks 3.0.1 forced two new module-level helpers in `src/ingestion/spadl_adapter.py` for per-provider `home_team_start_left` derivation. The IDSSE helper is authoritative (reads `kickoff_team_left` from bronze, captured by our DFL XML parser). The Metrica helper is empirical (period-1 SHOT positions) because Metrica bronze does not capture a kickoff-side flag. Long-term, a Metrica bronze schema extension to store the flag explicitly would be cleaner; not in scope for this PR.

## Related

- **Commits:** `<filled at PR-merge time>` (squash), depends on silly-kicks v3.0.1 (commit `d7f86de`, PR-S23)
- **Specs:** `docs/superpowers/specs/2026-05-02-sk3-mig-direction-of-play-migration-design.md`
- **Plans:** `docs/superpowers/plans/2026-05-02-sk3-mig-direction-of-play-migration.md`
- **ADRs:** companion to silly-kicks ADR-006 (per-converter direction-of-play handling); closes ADR-012 §2 grace-period; cross-references ADR-014 (HF card inventory parity), ADR-018 (cross-table format-contract testing).
- **External references:** silly-kicks v3.0.0 (PR-S22, commit `a1ebfa0`); silly-kicks v3.0.1 (PR-S23, commit `d7f86de`); OPT-1 PR #248 (`b3c9d9e` on main) — the e2e probe that surfaced the original bug.

## Notes

The SK3-MIG cycle uncovered + drove a silly-kicks-side bug fix mid-execution: silly-kicks 3.0.0 shipped with `Sportec` + `Metrica` converters declaring `ABSOLUTE_FRAME_HOME_RIGHT` while real production fixtures from those providers ship `PER_PERIOD_ABSOLUTE`. The lakehouse session caught it via the new `test_sk3_coord_correctness.py` invariant test (2/4 sources passed, 2 failed with per-team x split). silly-kicks 3.0.1 (PR-S23) added required `home_team_start_left` / `home_team_start_left_extratime` / `home_attacks_right_per_period` kwargs (mutual exclusion + loud-failure default) on both converters within hours of the bug report. The lakehouse-side derivation pattern (authoritative-from-bronze for IDSSE; empirical-from-shots for Metrica) is the resulting consumer adaptation.

This sequence — lakehouse adoption test catches upstream library bug → upstream releases hotfix within hours → consumer adapts call sites — is exactly the cross-repo collaboration loop the SK3-MIG brainstorming spec contemplated under "Open implementation questions to resolve in the plan." The new invariant unit test now lives at the lakehouse boundary as a permanent regression gate against either silly-kicks regressions or our adapter regressions.
