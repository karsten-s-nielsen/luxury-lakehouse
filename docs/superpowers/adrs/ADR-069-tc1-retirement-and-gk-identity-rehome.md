# ADR-069: Retire the TC-1 tracking-context pipeline; re-home GK identity + IDSSE minutes onto AC-1

| Field | Value |
|---|---|
| **Date** | 2026-07-15 |
| **Status** | Accepted |
| **Deciders** | Karsten Nielsen |

## Context

`compute_tracking_context` (TC-1) is not a redundant *mart* — it is a redundant *pipeline* producing worse
data. TC-1 and `compute_action_context` (AC-1) independently read the **same three** bronze tracking tables
(`idsse_tracking` / `metrica_tracking` / `skillcorner_tracking`), independently call `link_actions_to_frames`,
and run the **same silly-kicks enrichment chain in the same order**. AC-1 then runs ~14 more steps. AC-1 is a
strict superset in features (all 66 TC columns, same names, +~50) and in providers (6 vs 3).

Three facts, all measured live against production bronze, make TC-1's output the worse of the two:

- **Coverage: `TC-only = 0` for every provider.** Not one `(match, action)` TC-1 covers is missing from AC-1.
  TC-1's larger raw IDSSE count (9,209 vs 8,430) was **entirely its duplicate bug** (9,209 − 779 = 8,430).
- **Orientation:** TC-1's metrica/SkillCorner frames are **un-oriented** (hand-rolled `_bronze_*_to_frames`,
  pre-TF-23, no orient call), so its spatial columns are arguably wrong. AC-1's frames go through the
  silly-kicks builders + geometric LTR ([ADR-053](ADR-053-silly-kicks-4-27-0-geometric-frame-ltr-net.md) /
  [ADR-034](ADR-034-gradientsports-bronze-adapter-hardening.md)/[ADR-035](ADR-035-silly-kicks-4-2-vectorized-ghost-gk-adoption.md)).
- **Dedup:** TC-1 bronze `spadl_tracking_context` has **4,052 divergent duplicate keys** (779 idsse / 419
  metrica / 2,854 skillcorner) resolved by an **arbitrary, tiebreaker-less** pick. AC-1 bronze
  `spadl_action_context` is **0-dup by construction** (937,324 rows = 937,324 distinct keys) via the M13
  work-unit ownership model.

TC-1's gold `fct_tracking_context` has **zero dbt refs and zero Taipy consumers**. Its only two *real*
consumers read the tracking-context *staging* layer: `int_tracking_goalkeepers` (GK identity →
`fct_tracking_frames.is_goalkeeper`) and the idsse leg of `int_minutes_played_per_match` (→
`fct_goalkeeper_stats`). Retiring TC-1 therefore turns on re-homing exactly those two consumers.

## Decision

**Retire the TC-1 pipeline end-to-end and re-home its two consumers onto AC-1**, sourcing GK identity from
`stg_action_context__values` with an **`n_actions >= 2` goalkeeper mis-tag threshold**.

Deleted: `ingestion/tracking_context.py`, `fct_tracking_context` (gold), `stg_spadl__tracking_context` +
its source, the `compute_tracking_context` / `preflight_tracking_context` mega-job tasks + `wf-tracking-context`
card + entry points, the `fct_tracking_context_synced` config + its PG indexes, and the TC-1 unit-test suite.
`int_tracking_goalkeepers` and `int_minutes_played_per_match`'s idsse roster now `ref('stg_action_context__values')`.

## Alternatives considered

| Option | Pros | Cons | Why rejected |
|---|---|---|---|
| A. Keep TC-1 (status quo) | No change | A second full pipeline over the same bronze; un-oriented frames; 4,052 nondeterministic dup keys; ~compute cost of a redundant drain | It is strictly-worse duplicated compute; `TC-only = 0` proves AC-1 already covers it |
| B. Source GK identity from raw tracking-bronze `is_goalkeeper` (stage-1) | Fully decouples `fct_tracking_frames` from the compute layer; no staleness | Reverts TC-2's unified `derive_goalkeepers()` for per-provider heuristics (idsse `TW` flag, skillcorner `position_name`, metrica jersey-#1); loses substitute-GK detection | Less accurate — e.g. it misses a sub GK the roster lists as "Substitute" (Kepa, match `1552423`, 96 actions) that `derive_goalkeepers()` catches |
| C. Re-home onto AC-1 + `n_actions >= 2` threshold (chosen) | Reuses AC-1's existing `derive_goalkeepers()` run (no duplicate compute); oriented frames; 0-dup bronze; strips one-off mis-tags | `fct_tracking_frames` gains an AC-1 dependency (one-run-stale, see Consequences) | — |

The threshold value is not arbitrary. Measured live, the mis-tag gap is clean: **every** silly-kicks
mis-tag (an outfield player tagged defending-GK for a single action) has `n_actions = 1`, while confirmed
goalkeepers have `n_actions >= 53` (skillcorner) / `>= 463` (idsse), and a real substitute keeper is well
clear (Kepa = 96). `>= 2` therefore drops **0 confirmed goalkeepers** and removes all one-off mis-tags.

## Consequences

### Positive

- One entire compute pipeline removed. GK identity now derives from **oriented** frames and **0-dup** bronze.
- The `n_actions >= 2` threshold **improves** `int_tracking_goalkeepers`: it strips derive_goalkeepers()
  one-off mis-tags that were *already* in production via TC-1 (skillcorner went 229 → 219 GK identities,
  removing 17 `n_actions = 1` outfield mis-tags including 4 pre-existing defenders; idsse unchanged at 14).
- A parity gate proved the re-home safe before any deletion: idsse GK set **identical** (`assert_idsse_gk_parity`
  symmetric diff = 0); idsse minutes roster **byte-identical** (218 = 218); metrica strictly additive
  (`tc_only = 0`, 6 → 9). AC-1 additions are a coverage superset, never a loss.
- AC dedup hardened as defense-in-depth: a **bronze-source** zero-dup singular test
  (`assert_action_context_bronze_no_divergent_dups`) — the only layer where an M13-ownership regression is
  visible, since the staging `row_number()=1` makes any mart-grain test vacuous.

### Negative

- **`fct_tracking_frames` (a stage-1 `input_mart`, read by `compute_off_ball_xt` / `compute_formations_*` /
  `compute_defcon_lite`) now transitively reads AC-1's stage-2 output** for its `is_goalkeeper` column, making
  that column **one run stale**. This is *unchanged* from TC-1 (it already read `spadl_tracking_context`, a
  stage-2 compute output, while built in stage 1) — but the classification test now flags it because
  `spadl_action_context` is in `_COMPUTE_OUTPUT_BRONZE_TABLES` and `spadl_tracking_context` never was.
  Reclassifying to `intermediate_mart` was rejected: the mega-job builds `fct_tracking_frames` in
  `dbt_build_input_marts` so those compute tasks can read it, and moving it into the post-compute build risks
  a `compute → dbt_build_intermediate_marts → compute` cycle. Resolution: keep `input_mart`, add a narrow
  documented exemption (`_INPUT_MART_STALE_READ_EXEMPTIONS`).
- **metrica synthesis format mismatch surfaced.** AC-1 bronze's metrica `defending_gk_player_id_native` is
  `Side_jersey` format (`Home_1`); the staging synthesis regex strips only `^Player ?`, producing malformed
  `metrica_..._home_Home_1` that misses `dim_players` → 57 metrica GK candidates unresolved (warn-level,
  `assert_unresolved_gk_player_ids`). **Cosmetic**: the INNER JOIN drops them, and the resolved metrica set is
  a strict superset of TC-1 (`tc_only = 0`, no GK lost). Pre-existing (it also affects
  `fct_action_context.defending_gk_player_key`), tracked as a follow-up.

### Neutral

- The three shared `_{IDSSE,METRICA,SKILLCORNER}_TRACKING_SELECT_COLS` constants (imported by surviving
  `action_context.py` + `shot_freeze_frames.py`) were relocated from the deleted module into
  `action_context.py`, alongside the `_GRADIENTSPORTS_` set already there.
- The 9-file TC-1 unit-test suite was deleted (it tested deleted code). Bronze `spadl_tracking_context` DROP,
  the Lakebase synced-table teardown, and the `spadl-tracking-context` HF dataset decision are operator
  post-merge steps.

## CLAUDE.md Amendment

Two existing rules were updated to reflect the retirement (not carved out):

- The [ADR-053](ADR-053-silly-kicks-4-27-0-geometric-frame-ltr-net.md) amendment previously said the
  `tracking_context.py`/`fct_tracking_context` retirement was *"blocked on re-homing the GK-identity +
  IDSSE-minutes consumers"* — this ADR **does** that re-home, so the bullet now records TC-1 as retired.
- The [ADR-067](ADR-067-velocity-delete-and-depend-and-unit-write-atomicity.md) bullet referenced
  `ingestion/tracking_context.py` as a location of the deleted velocity helper; that module is now entirely
  gone, so only the `analytics/action_context/convert.py` copy remains guarded.

One new convention is introduced: `_INPUT_MART_STALE_READ_EXEMPTIONS` in
`src/tests/test_dbt_mart_classification.py` — a documented, per-`(mart, bronze)` allowance for an `input_mart`
to read a compute-output bronze **one run stale**, admissible only when the staleness is pre-existing and
accepted (never a new regression).

## Related

- **Specs:** `docs/superpowers/specs/2026-07-14-mart-consolidation-tc1-retirement-design.md` (+ REVIEW, REVIEW-2)
- **Plans:** `docs/superpowers/plans/2026-07-14-pr1-tc1-retirement.md`
- **Issues / PRs:** PR TBD (branch `feat/tc1-retirement`)
- **ADRs:** builds on [ADR-030](ADR-030-gradient-sports-bronze-frame-dedup.md) (divergent-dup class),
  [ADR-034](ADR-034-gradientsports-bronze-adapter-hardening.md)/[ADR-035](ADR-035-silly-kicks-4-2-vectorized-ghost-gk-adoption.md) (TF-23 geometric frame orientation),
  [ADR-053](ADR-053-silly-kicks-4-27-0-geometric-frame-ltr-net.md) (supersedes its "TC-1 retirement blocked"
  note), [ADR-067](ADR-067-velocity-delete-and-depend-and-unit-write-atomicity.md) (velocity ownership),
  [ADR-013](ADR-013-ml-inference-outputs-dbt-mart.md) (identity-fact JOIN pattern)

## Notes

**Parity-gate evidence (live, read-only, before any deletion).** GK-identity sets resolved to
`(match_key, player_key)`, TC-1 vs AC-1:

| provider | shared | TC-only | AC-only |
|---|---|---|---|
| idsse | 14 | 0 | 0 |
| metrica | 6 | 0 | 6 |
| skillcorner | 229 | 0 | 7 |

`TC-only = 0` everywhere. Of skillcorner's 7 AC-only additions, 6 are `n_actions = 1` mis-tags and one is a
real substitute keeper (`n_actions = 96`). The AC bronze zero-dup test passed live (0 divergent keys) while
TC-1 bronze showed the 4,052 dup keys above — the concrete reason TC-1 is the worse pipeline.

**Threshold safety (live).** Per provider × `position_group`, entries dropped by `n_actions >= 2`:
confirmed `Goalkeeper` dropped = **0** (min confirmed-GK `n_actions` = 53 skillcorner / 463 idsse); all 7
skillcorner defenders + 1 midfielder + the null-position one-offs dropped had `n_actions = 1`.

**Plan gaps discovered during execution** (recorded so future teardowns grep the module name across `src/`
first, not just dbt model names): `tracking_context.py` was *not* a leaf — surviving AC-1 code imported three
`SELECT_COLS` constants from it, and it had a 9-file unit-test suite the plan never enumerated; and the
`fct_tracking_frames` build-ordering ripple above was not modelled. Both were caught by a full `pytest` run,
not by the plan's grep.
