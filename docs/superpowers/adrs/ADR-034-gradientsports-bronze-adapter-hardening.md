# ADR-034: GradientSports json_normalize-bronze adapter hardening

| Field | Value |
|---|---|
| **Date** | 2026-06-01 |
| **Status** | Accepted |
| **Deciders** | Karsten Nielsen |

## Context

GradientSports (GS) bronze tables are written via `pd.json_normalize` of the provider's
nested API payload, which produces **dot-named, very wide, per-match-varying** schemas:
`bronze.gradientsports_events` has **264 columns** (`gameEvents.*`, `possessionEvents.*`,
`grades.*`, `fouls.*`, …); `bronze.gradientsports_roster` has dot-named columns
(`team.id`, `shirtNumber`, `player.id`, `positionGroupType`). The tables are structurally
sound — schema complete, the SPADL conversion reads them and produces ~1,300 actions/match,
and narrow reads succeed.

GS is the newest tracking provider and the first whose **full AC-1 adapter path** (bronze →
`MatchMeta` + converted frames → `enrich_batch`) had never run end-to-end on serverless. The
hexagonal architecture (ADR-028) tests the *pure enrichment* with pre-built fixture
frames/meta, so the bronze→frames/meta **driver/adapter layer is invisible to the local
hexagon tests** (the existing rule in `feedback_test_production_driver_entry_point`). As a
result GS accumulated **four latent adapter bugs**, each surfacing only after the previous was
cleared, when the new `profile_action_context` tool first drove GS end-to-end:

1. The driver read `bronze.gradientsports_roster` with snake_case names (`team_id`,
   `jersey_number`, `player_id`, `position`) — none exist (the columns are dot-named) →
   KeyError / empty `MatchMeta` dicts → no carrier/possession resolution.
2. silly-kicks' `GRADIENTSPORTS_TRACKING_FRAMES_COLUMNS` forces frame `player_id`/`team_id`
   to `Int64` inside `convert_to_frames`, but every downstream consumer compares them to
   **native-string** action ids (`player_id_native` / `team_id_native`, and `home_team_id`
   is a `str`) → `Int64(366) == "366"` is `False` → silent actor/opponent/possession failure.
3. The driver's wide `spark.table(gradientsports_events).filter(...).toPandas()` (all 264
   dot-named columns) trips a Spark Connect Catalyst attribute-resolution bug on serverless
   (`Cannot find column index for attribute 'possessionEvents.carrySuccessful#…'`).
4. The driver renamed `period_elapsed_time → timestamp` (the batch/link/owned-action logic
   needs `timestamp`), but the GS converter reads `period_elapsed_time` → the destructive
   rename made it `KeyError`.

## Decision

Harden the **consumer side** of the GS adapter (the bronze→frames/meta driver) rather than
redesign GS bronze, with four changes, and close the test-coverage gap with a GS adapter e2e:

1. **Narrow, backtick-quoted bronze reads** for GS — project only the needed columns before
   `toPandas()` (`_GS_EVENTS_META_COLS`, `_GS_ROSTER_COLS` in `ingestion.action_context`),
   never the wide dot-named table.
2. **Coerce GS frame `player_id`/`team_id` from `Int64` back to native string** after
   `convert_to_frames` (`analytics.action_context.convert._coerce_gradientsports_frame_ids_to_native_str`),
   to match the native-string action-id space every consumer uses.
3. **Non-destructive `period_elapsed_time → timestamp` alias** (add `timestamp`, keep
   `period_elapsed_time`) at both dispatch sites (`_process_tracking_match` driver +
   `pipeline.run_work_unit`), so the batch/link logic and the converter both get what they need.
4. **Read the roster with its real dot-notation columns** (`_build_gradientsports_roster_dicts`)
   AND add a GS `run_work_unit` adapter e2e crash-guard
   (`test_e2e.py::test_gs_e2e_convert_and_enrich_does_not_crash`) that exercises the
   bronze→frames/meta layer the hexagon fixtures bypass.

## Alternatives considered

| Option | Pros | Cons | Why rejected |
|---|---|---|---|
| A. Redesign GS bronze ingestion to an explicit (non-`json_normalize`) stable schema | Eliminates dot-named/wide/union-schema at the source for all consumers | Large change; forces re-ingestion of all GS bronze; bronze is *not* the defect (narrow reads + SPADL conversion already work) | Bug is consumer-side read patterns, not ingestion; redesign is high-cost for no correctness gain |
| B. Adopt silly-kicks `add_gradientsports_player_ids` for player-id resolution | Robust dup-key/GK-vocab/unmatched-rate handling | Cross-layer (MatchMeta roster-records + convert.py); breaks the `test_convert_drift` AST guard; still needs the same Int64→string coercion | Deferred as robustness, not reliability — the column-fix + coercion already make GS correct; queued follow-up (helper is already in silly-kicks 4.1.1, so independent of any release) |
| C. (chosen) Consumer-side hardening — narrow reads + id coercion + non-destructive alias + correct roster cols + adapter e2e | Minimal, targeted, validated; no bronze re-ingestion; closes the test gap | Several small workarounds to carry; one is a workaround for a silly-kicks schema choice (flagged upstream) | — |

## Consequences

### Positive

- GS AC-1 enrichment is reliable: carriers/possession/actor/opponent/defensive-line resolve.
- The bronze→frames/meta **adapter layer now has crash-coverage** (the GS `run_work_unit`
  e2e), closing the gap that let four bugs stay latent — a template for onboarding future
  providers.
- The narrow GS reads are also more efficient and `.toPandas()`-bounded (4 columns, not 264).

### Negative

- Several consumer-side workarounds to maintain. The `Int64→native-string` coercion exists
  only because silly-kicks' `GRADIENTSPORTS_TRACKING_FRAMES_COLUMNS` forces `Int64` while
  sportec/kloppy use `object`/string; if silly-kicks aligns the GS frames schema (flagged to
  that session), the coercion can be removed.
- The `.toPandas()` boundedness exemption for the profiler is keyed by `(file, line)` and
  shifts when `action_context.py` is edited above it — must be re-checked on edits there.
- The GS converter-input builder still exists in two copies (legacy `ingestion.action_context`
  oracle + `analytics.action_context.convert`) kept in sync by the `test_convert_drift` AST
  guard; the period-alias fix sits in the dispatch layer, not the guarded function, so the
  guard is untouched.

### Neutral

- The committed GS fixture (`10517_p3`) is a ~15-second period-3 frame slice paired with
  full-match actions, so the GS e2e is a **crash-guard** (0 enriched rows by construction),
  not a resolution check. Resolution is validated by the serverless profiling run; a *matched*
  GS fixture (aligned frames+actions+roster-derived meta) for a full local resolution e2e is a
  tracked follow-up.

## Related

- **Issues / PRs:** `#327` (silly-kicks 4.1.1 floor-harden + AC-1 stage-profiler; this GS
  adapter hardening ships as the follow-up commit on the same branch).
- **ADRs:** builds on `ADR-028` (hexagonal architecture — whose test boundary is the reason
  the adapter bugs were latent), `ADR-030` (GS bronze frame dedup), `ADR-033` (explicit schema
  on createDataFrame); reuses the `ADR-031`/`ADR-032` serverless executor-visibility patterns
  via `profile_action_context`.
- **Notes:** `docs/superpowers/plans/notes/ac1-profile-results.md` (the profiling finding that
  drove this); project memory `project_gradientsports_player_id_space_bug`.
- **External references:** silly-kicks `GRADIENTSPORTS_TRACKING_FRAMES_COLUMNS` (Int64 frame-id
  schema — flagged to the silly-kicks session as an `object`/string-alignment candidate);
  Spark Connect wide-dot-column `toPandas` Catalyst attribute-resolution failure.

## Notes

The four bugs surfaced one-at-a-time because GS's full adapter path had never run end-to-end —
each fix unblocked the next. The structural lesson (decision 4) is the durable one: the
hexagon isolates the pure enrichment for fast local tests, but that boundary leaves the
bronze-coupled adapter/driver layer exercised only on serverless, so each new provider needs
explicit adapter coverage or it ships latent adapter bugs.
