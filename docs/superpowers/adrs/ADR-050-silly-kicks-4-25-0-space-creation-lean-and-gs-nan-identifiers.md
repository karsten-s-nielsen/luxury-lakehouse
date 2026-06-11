# ADR-050: silly-kicks 4.25.0 adoption — space-creation lean contract + GS null-actor NaN identifiers

| Field | Value |
|---|---|
| **Date** | 2026-06-11 |
| **Status** | Accepted |
| **Deciders** | Karsten Nielsen |

## Context

silly-kicks 4.25.0 bundles two upstream changes the lakehouse gated on:

1. **GS null-actor crash (production outage, 2026-06-11).** The Gradient Sports converter emitted the
   integer sentinel `0` as `team_id`/`player_id` on null-actor duel/foul events. Because `0` is non-NaN
   it masqueraded as a real id, bypassed every downstream `pd.isna` NaN-route, and crashed 4.23.0's strict
   opponent-resolution guard (`attacking_team_id '0' does not uniquely match the frame team ids [...]`),
   taking down all 10 GS units in the v5 action-context run. Ground truth on the canonical PFF WC2022 feed
   (64 matches): the null-team events are 594 `OTB`+`CH` challenges + 28 `FOUL`+`FO` fouls, and on every one
   `gameEvents.playerId` is *also* null — there is no acting player to attribute, and the only team-bearing
   ids are duel/foul *qualifiers* whose use as identifiers is the ADR-001 violation silly-kicks 2.0.0 removed.
   The lakehouse **withdrew** its original "resolve from the acting player's roster" prescription as a false
   premise; NaN is the architecturally-correct value (see silly-kicks ADR-027).

2. **Space-creation LEAN contract (4.24.0, breaking).** Under a complementary pitch-control model with a
   shared unmirrored multiplier, the 4.23.0 opponent triplet was informationally empty
   (`opponent_space_destroyed_m2 ≡ space_created_m2` bit-for-bit; structurally-zero columns). The lakehouse
   rejected it (rejection report 2026-06-11). 4.24.0 fixes the opponent surface (mirrored to the opponent's
   own attacking geometry) and reshapes `add_space_creation` to emit exactly two columns. No lakehouse
   consumer had adopted any 4.23.x space surface, so the rename is clean.

The lakehouse runs silly-kicks floors in eight places (pyproject `[spadl]`, `uv.lock`, terraform `==` env
pins, 6 trainer + `exec_visibility` `_REQUIRED_SK_MIN`) plus enforcing sentinel tests; a minor-version bump
is mechanical but wide.

## Decision

Adopt silly-kicks **4.25.0** as the floor everywhere (wheel 0.5.36). In action-context, replace the
`space_created_m2_team` / `space_created_m2_opponent` pair with the lean contract **`space_created_m2`**
(attacking leave-one-out, ≥0) + **`space_denied_m2_opponent`** (rest-defense leave-one-out on the mirrored
opponent surface, ≥0). Accept GS null-actor rows carrying NaN `team_id`/`player_id` (nullable `Int64`
upstream), which NaN-route through every action-context enrichment. Wipe `bronze.spadl_action_context` to a
clean slate via a surgical `RENAME`/`DROP`/`ADD COLUMN` + `DELETE` migration (not DROP+recreate).

## Alternatives considered

| Option | Pros | Cons | Why rejected |
|---|---|---|---|
| A. Stay on 4.23.0 | No churn | GS units crash hard; opponent space column is informationally empty | The crash is a live production outage; 4.23.0's opponent triplet was rejected upstream |
| B. Keep `space_created_m2_opponent`, add `space_denied_m2_opponent` (additive, non-breaking) | No rename | Resurrects a structurally-zero column the removal-based LOO can never populate; carries dead data | Upstream retired it as mathematically unsatisfiable; a retired-columns guard test blocks resurrection |
| C. DROP+recreate the bronze table for the clean slate | One statement | `ensure_table` (guards.py) only sets autoOptimize → silently loses `delta.enableChangeDataFeed='true'` set at table birth per spec §8.1 | Chesterton's Fence: CDF was enabled deliberately; a recreate is a silent regression |
| D. **Lean rename + surgical ALTER migration + 4.25.0 floor (chosen)** | Fixes crash; opponent column gains real signal; preserves table properties | Run-once non-idempotent migration; GS re-conversion + recompute needed downstream | — |

## Consequences

### Positive

- GS action-context units no longer crash; null-actor duel/foul rows NaN-route honestly (the contested-duel
  result), and the upstream NaN-safety audit additionally fixed a masked `add_line_break(method="ward")`
  crash + a silent GS-wide Ward miscompute (`'366' != 366`) — GS bronze AC was empty, so no historical cleanup.
- `space_denied_m2_opponent` now carries real, non-constant rest-defense signal (regenerated golden:
  97 non-null, 71 distinct, [0, 3.65]) where 4.23.0 produced structural 0.0.
- The degenerate `space_created_m2_opponent` oracle breadcrumb is retired; both columns are honest ≥0 ranges.

### Negative

- A run-once, operator-applied bronze migration (`RENAME`/`DROP`/`ADD COLUMN`) that must land *with* the merge
  or the next daily live build (and the `test_action_context_live_ddl_parity` guard) fails.
- GS `team_id`/`player_id` dtype `int64`→`Int64` + the 0→NaN value flip is a re-materialize trigger: GS SPADL
  must be re-converted and a full action-context recompute run before GS metrics are correct (owner-gated).
- Action-context is wiped to a clean slate; the gold mart rebuilds on the next recompute.

### Neutral

- The standalone `fct_space_creation` T-mart uses a lakehouse-local numpy `compute_space_created`
  (`src/analytics/space_creation.py`) and is unaffected by the upstream lean. `space_creation_xfns` is unused
  here, so the upstream VAEP-feature reshape is a no-op for the lakehouse.
- C4 container/component count unchanged (silly-kicks bump is C4-free); the diagram's silly-kicks version
  string was patched 4.23.0→4.25.0 (length-preserving).

## Related

- **Specs / migration:** `scripts/migrations/2026-06-11-action-context-space-creation-lean.sql`
- **ADRs:** extends `ADR-048` (4.22.0 xT-GK adoption), `ADR-042` (4.19.2 adoption), `ADR-036` (4.4.0),
  `ADR-035` (4.2.0); relates to `ADR-018` (cross-table format-contract testing — the live DDL-parity guard),
  `ADR-026` (silly-kicks space-creation, upstream), `ADR-001` (silly-kicks no-qualifier→identifier override).
- **External references:** silly-kicks CHANGELOG 4.24.0 + 4.25.0; silly-kicks ADR-027 (GS null-actor NaN
  identifiers).

## Notes

Adoption gates run clean: ruff check + format (588 files), pyright 0 errors, full pytest all-pass except
`test_action_context_live_ddl_parity` (the ADR-018 code-vs-live drift guard correctly reporting
`Missing in live: ['space_created_m2', 'space_denied_m2_opponent']` until the migration is applied). Both
AC goldens regenerated; the emitted column set was verified directly from the installed 4.25.0 package
(`add_space_creation` → `space_created_m2`, `space_denied_m2_opponent`), not assumed from the changelog.
