# ADR-029: silly-kicks 4.0 adoption — extra-time direction symmetric guard + lakehouse-side possession synthesis

| Field | Value |
|---|---|
| **Date** | 2026-05-30 |
| **Status** | Accepted |
| **Deciders** | Karsten Nielsen |

## Context

silly-kicks 4.0.0 (PR-S70, published 2026-05-30) ships a symmetric extra-time direction guard via `require_et_direction` applied across all 5 per-period-absolute converters (Sportec tracking + Sportec/Metrica/Gradient Sports events + Gradient Sports tracking). Pre-4.0, the asymmetry was a latent correctness bug: Gradient Sports raised on `period_id in {3, 4}` without `home_team_start_left_extratime`, while Sportec/Metrica silently defaulted to `(p3=False, p4=True)` — silently flipping ET coordinates for every match whose true ET orientation differed from the default. The §8 historical audit (2026-05-30) confirmed zero IDSSE/Metrica ET matches existed in lakehouse bronze, so no production data was actually mis-oriented — but the bug was reachable the moment any cup-with-ET match was ingested.

silly-kicks 4.0 is a major-bump breaking change for ET-bearing matches: callers MUST pass `home_team_start_left_extratime` to every per-period-absolute converter or the guard raises. silly-kicks deliberately stays a pure pass-through (per PR-S67) — it does NOT synthesize possession, even where SPADL semantics make it unambiguous (set-piece restart actions). That synthesis is the caller's modeling responsibility.

This ADR records the lakehouse's adoption: bronze-side ET-flag derivation, MatchMeta plumbing, sentinel coverage, and the lakehouse-domain set-piece possession-fill helper that complements silly-kicks' purity to give finite DAS on SPADL-determinable dead-ball windows.

## Decision

Adopt silly-kicks 4.0.0 with the floor pin `silly-kicks[das,ghost-gk]>=4.0.0,<5`. Add `home_team_start_left_extratime: bool | None` to `MatchMeta`, derive it per-provider from bronze (`derive_idsse_home_team_start_left_extratime` reads DFL XML `extraTimeFirstHalf` KickOff; `derive_metrica_home_team_start_left_extratime` empirically infers from period-3 SHOT positions; GS reads `stadiumMetadata.homeTeamStartLeftExtraTime`), and plumb through `pipeline.py` + `spadl_conversion.py` to every silly-kicks converter call. Add a lakehouse-domain helper `_fill_possession_from_set_piece_actions` that synthesizes `team_in_possession` for SPADL set-piece restart actions (throw_in / freekick_* / corner_* / goalkick / shot_freekick / shot_penalty) where carrier inference returns NaN. Add a pre-flight sentinel test in `src/tests/action_context/test_et_direction_sentinel.py` that asserts the pipeline path actually reaches the silly-kicks guard on synthetic ET-bearing input.

## Alternatives considered

| Option | Pros | Cons | Why rejected |
|---|---|---|---|
| A. Stay on silly-kicks 3.30; defer 4.0 | No coordinated cross-repo work | Latent silent-mis-orient bug remains reachable; ADR-022 erratum unresolved | Rejected — the audit's clean-zero is a present accident, not a permanent guarantee |
| B. Adopt 4.0 without lakehouse possession-fill | Smallest diff; silly-kicks does its job; honest NaN DAS on dead-ball windows | Set-piece restart actions get NaN DAS where their possession is SPADL-unambiguous | Rejected — needlessly information-poor for the subset where the lakehouse can supply a correct value |
| C. Push possession-fill into silly-kicks | Every silly-kicks consumer benefits | Couples silly-kicks to SPADL modeling decisions it deliberately avoids (PR-S67) | Rejected — silly-kicks correctly declined the scope; this is caller-domain |
| D. Adopt 4.0 + lakehouse possession-fill + sentinel test (chosen) | Crash-free; bit-identical on RT-only; finite DAS for set-piece restarts; cross-repo pin-ordering caught at PR time | Larger diff (Phase A.0 + A.1 + Phase B); requires two new derivers per provider | — |

## Consequences

### Positive

- silly-kicks ET symmetric guard reaches every per-period-absolute converter; the asymmetric silent-default bug for Sportec/Metrica is permanently eliminated regardless of which downstream consumes them.
- AC-1 production handles ET matches correctly the moment they're ingested. No more "we got lucky because cup data hasn't been ingested" exposure.
- Set-piece restart actions in dead-ball windows produce finite DAS values via the lakehouse-domain helper. Non-set-piece dead-ball actions still produce honest NaN — the metric is undefined where no team is in possession.
- Cross-repo coordination caught at PR time via the sentinel test: any future regression that drops the `home_team_start_left_extratime` kwarg from `convert_to_frames` / `convert_to_actions` raises in CI before merge.
- silly-kicks stays pure (no SPADL modeling responsibility); lakehouse owns its modeling decisions (set-piece possession synthesis lives in `src/analytics/action_context/enrich.py`). Clean architectural split that other silly-kicks consumers can follow.

### Negative

- Mechanical cost of a major-bump: 25+ files touched (pyproject + 17 PEP 723 scripts + 6 trainer `_REQUIRED_SK_MIN` + enforcing test + TF env spec + wheel.py + deploy.sh). Every future silly-kicks bump within 4.x is now mechanical via `bump_wheel.py`; cross-major bumps will recur this cost.
- Two new per-provider derivers (`derive_idsse_home_team_start_left_extratime`, `derive_metrica_home_team_start_left_extratime`) add a deferred-execution path: Metrica's `home_team_start_left_extratime` plumbing is wired in `spadl_conversion.py` but NOT in `pipeline.py` for AC-1 tracking-side (Metrica AC-1 path doesn't yet read its own events to derive ET flag — comment in `action_context.py:_process_tracking_match` documents this deferred-until-needed state since the §8 audit found zero Metrica ET matches).
- The lakehouse `_fill_possession_from_set_piece_actions` helper is provider-agnostic but operates on already-converted SPADL `type_id` integers. Future SPADL type-id reordering by silly-kicks would invalidate the cached `_set_piece_restart_type_ids()` mapping; the lazy lookup against `silly_kicks.spadl.config.actiontypes` is the drift guard.

### Neutral

- ADR-022 (direction-of-play migration to silly-kicks 3.0.1) is conceptually the predecessor to this ADR. Both deal with silly-kicks tightening direction contracts; 3.0.1 handled regular-time per-period-absolute; 4.0.0 handles extra-time. ADR-022 stays Accepted; this ADR extends rather than supersedes.
- Lakehouse wheel bumped 0.4.3 → 0.5.0 to signal the silly-kicks-4 dep upgrade as a non-trivial dependency change. Wheel stays on 0.x per the project's "private artifact, not a public PyPI API" SemVer posture.

## Related

- **Commits:** TBD (single commit per branch, this PR)
- **Specs:** `D:\Development\karstenskyt__silly-kicks\docs\superpowers\specs\2026-05-30-et-direction-converter-consistency-design.md` (silly-kicks side)
- **Issues / PRs:** silly-kicks PR-S70 (4.0.0 ship), lakehouse PR (this ADR)
- **ADRs:** extends `ADR-022-direction-of-play-migration` (3.0.1 erratum); references `ADR-028-hexagonal-architecture-for-compute-pipelines` (AC-1 hexagon where the plumbing lives)
- **External references:** silly-kicks 4.0.0 PyPI release notes, lakehouse `memory/project_et_direction_section_8_audit.md`

## Notes

§8 historical-mis-orientation audit (2026-05-30): IDSSE 0, Metrica 0, Gradient Sports 5 ET matches in bronze. GS already raised pre-4.0 (its inline check) AND GS bronze carries the flag end-to-end, so GS ET data was never silently mis-oriented either. Net: no historical data corruption from the pre-4.0 silent-default Sportec/Metrica path. The lakehouse owes no remediation; the 4.0 adoption is forward-looking insurance against the next cup-with-ET ingestion.

GS ET fixture (match 10517) was extracted from lakehouse bronze and delivered to the silly-kicks repo for their PR-S70 Task 8 round-trip test (`tests/regressions/extratime/gs_et/`). IDSSE/Metrica equivalents were not deliverable (zero in bronze); silly-kicks synthesizes minimal fixtures for those converters' unit tests.
