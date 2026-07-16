# Critical review — `2026-07-14-mart-consolidation-tc1-retirement-design.md`

**Reviewer:** external session · **Date:** 2026-07-14
**Verdict: PR-1 and PR-2 are sound with fixes; PR-3 has a blocker the spec does not see.**

Everything below is verified against the source tree (`dbt_project/models/…`), not the spec's prose.
An exhaustive consumer sweep of `dbt_project/`, `hf_taipy_app/`, `src/`, `scripts/`, `tests/`,
`terraform/`, `workflow-cards/` backs the "0 consumer" claims.

## What checks out — and it's most of it

- **The four orphan-mart deletions (PR-2) are genuinely consumer-free.** `fct_space_creation`,
  `fct_off_ball_xt`, `fct_line_breaking_results`, `fct_gk_actions_detail`: zero `ref()`, zero Taipy
  queries, only sync/index/registry plumbing. `fct_gk_actions_detail`'s former consumer
  `hf_taipy_app/src/queries/goalkeepers.py` **is confirmed gone** (successor `gk_analytics.py` reads
  four *other* GK marts, never this one). The mart-vs-task distinction is real and correctly drawn:
  `fct_passes.sql:61` reads `stg_line_breaking__results` directly, `fct_physical_stats.sql:128` reads
  `stg_off_ball_xt__results` directly — deleting those marts leaves both live consumers untouched.
  **PR-2 is approvable as written.**
- **The trap is a real catch.** `int_tracking_goalkeepers.sql:21` has only
  `where defending_gk_player_id_native is not null`, no `data_source` filter. The explicit-filter +
  test requirement is correct.
- **The re-home is column-complete for the columns that matter.** `int_tracking_goalkeepers` consumes
  exactly `data_source`, `native_match_id`, `defending_gk_player_id_native` — and AC's staging casts
  all three identically, **including the metrica-specific ID synthesis** (`stg_action_context__values.sql:37-41,55-63`
  matches `stg_spadl__tracking_context.sql:37-41,54-62` line-for-line). The minutes leg
  (`int_minutes_played_per_match.sql:162-171`) uses only `native_match_id` + `player_id_native` and
  **already filters `data_source = 'idsse'`**, so the trap cannot touch it. Both re-homes are safe on
  the column axis.
- **The dedup bug is exactly as described** (`stg_spadl__tracking_context.sql:16-19`, tiebreaker-less
  `order by _ingested_at desc`).

The investigation is strong. The findings below are about the edges it didn't reach.

---

## BLOCKER

### B1 · PR-3 — a view cannot back `fct_gk_tracking_actions`'s synced table, and the spec's own cited test will catch it

The spec treats view-ification as cosmetic: *"Convert to a view; keep the same columns and contract
so its 3 dbt consumers and its 1 live Taipy query are unaffected."* But the Taipy query hits the
**synced table**, and the sync mechanism is incompatible with a view:

- `refresh_synced_tables.py:237` — `SyncedTableConfig("fct_gk_tracking_actions_synced",
  "fct_gk_tracking_actions", ("gk_action_id",), "TRIGGERED")`.
- The mart sets `tblproperties={'delta.enableChangeDataFeed': 'true'}` (line 8).
- A **TRIGGERED** Lakebase synced table syncs incrementally from the source's **Change Data Feed**. A
  view has no CDF, no Delta history — it cannot be a TRIGGERED sync source.

So PR-3 as written breaks the live Goalkeeper Analytics page's data path. And the spec's **own §6 risk
table** names the guard that will fail: *"`test_strand_safe_rederive.py` enforces SYNCED_TABLES ↔
`dbt_project.yml:triggered_synced_marts` parity."* `fct_gk_tracking_actions` is in
`triggered_synced_marts` (`dbt_project.yml:140`). View-ify it and you must drop it from that list and
flip `refresh_synced_tables.py` to `"SNAPSHOT"` — neither of which PR-3 mentions, and the parity test
trips if you do one without the other.

The fix is not hard, but it is a **materialization decision the spec skipped**: either keep it
materialized (and PR-3 collapses to nothing — it's *already* built on AC, see below), or convert to a
view **and** switch the synced table to SNAPSHOT, accepting that every refresh now re-executes the
full query. Which brings up the second half:

**PR-3 understates the query it wants to run live.** The spec calls it "`AC ⋈ AV` + 4 derived". It is
actually **AC ⋈ dim_matches ⋈ dim_teams ⋈ dim_players ⋈ dim_players ⋈ fct_action_values** — five
joins, two of them self-joins on `dim_players`, and one to the repo's most-depended-on mart
(`fct_action_values`, N2's "anchor"). As a materialized incremental table that cost is paid once per
build; as a SNAPSHOT-synced view it is paid on every refresh.

**And this specific mart has a documented materialization-masking bug.** Lines 44-50:

> *"…that orphaned the final-select references and broke `--full-refresh`; the daily incremental
> masked it."*

A view is *permanent* full-refresh semantics. This mart has already been bitten once by a latent
dependency that only the incremental materialization hid. Converting it to a view is the most likely
way to re-expose exactly that class of bug — and the spec's risk table doesn't mention materialization
at all.

**Note:** `fct_gk_tracking_actions.sql:17` already reads `stg_action_context__values` — the mart is
**already on AC**. So PR-3 is purely a materialization change (incremental→view), not a re-source.
That makes the "what does it buy" case weaker than PR-1's, and the sync-mode cost realer. Consider
whether PR-3 earns its risk at all, or whether it should be dropped from this program.

---

## MAJOR

### M1 · The root-cause bug lives in AC too — "dedup-free keys" (G4) is asserted, never measured

The spec calls TC-1's tiebreaker-less dedup *"an ADR-030-class bug"* and measures its damage precisely
(4,052 divergent dup keys). But **AC's staging uses the identical code**:

```
stg_action_context__values.sql:16-19   row_number() over (partition by match_id, action_id
                                                           order by _ingested_at desc)   -- no tiebreaker
```

The spec measures TC's divergent-dup count and **never measures AC's**. Yet G4 promises the re-homed
consumers *"must gain … dedup-free keys"* and §5 lists *"an arbitrary pick among 4,052 divergent
duplicate keys"* as a bug the re-home **fixes**. Nothing shown establishes that AC's keys are any less
arbitrary — only that AC's *coverage* is a superset (which `TC-only = 0` proves) and that AC's frames
are *oriented* (which the converters prove). The dedup axis is unproven.

If AC's bronze also carries content-divergent dups, re-homing **relocates** the arbitrary pick; it does
not remove it. The disease is the missing tiebreaker, and it is present, identically, in the re-home
target.

**Required before PR-1 ships:** measure AC's divergent-dup rate on the same keys (the exact query that
produced 4,052 for TC, run against `spadl_action_context`). If it is zero, G4 is substantiated — say
so with the number. If it is non-zero, the honest fix is a **deterministic tiebreaker on AC's dedup**
(a stable secondary sort — e.g. a content hash or `action_id, frame_id`) as part of this work, so both
the surviving TC consumers and AC's own ~55 downstream models stop depending on ingest-order luck.

### M2 · The parity gate changed two variables and demands the difference be blamed on one

§4.1 says metrica/SkillCorner differences are *"possible … where TC-1's dedup arbitrarily picked among
divergent dups. **Any difference must be explained as a TC-1 dedup artefact**, not accepted silently."*

But re-homing changes **two** things at once for metrica/SkillCorner: the dedup pick **and** frame
orientation (TC un-oriented → AC LTR). GK identity comes from silly-kicks `derive_goalkeepers()`
(3-tier — `int_tracking_goalkeepers.sql:4`), whose positional tiers are orientation-sensitive. So a GK
that differs between TC and AC could be:

- a dedup artefact (TC picked a different divergent row), **or**
- an orientation correction (AC's oriented frames re-derived the GK correctly where TC's un-oriented
  frames didn't) — which is the spec's *own headline correctness argument*.

The acceptance criterion "explain as a **TC-1 dedup** artefact" excludes the second, legitimate
explanation. An implementer who sees a metrica GK differ, can't trace it to a specific dedup collision,
and follows the gate literally will **STOP on a correct improvement.** Widen the criterion: a
difference is acceptable if AC's value is verified correct against roster/ground truth, whether the
mechanism is dedup or orientation. And for idsse the spec reasons *"key sets are exactly equal →
expect identical"* — a non-sequitur (equal action coverage does not imply equal per-action GK values);
the real reason idsse should match is same-oriented-frames + same derivation, so state that instead,
because it tells you what an idsse difference would actually *mean* (a genuine derivation change worth
stopping for).

---

## MINOR

- **m1 · One consumer omitted from the re-home list.** `src/tests/test_staging_coverage.py:88` —
  `("spadl_tracking_context", "stg_spadl__tracking_context")` — references the staging model by bare
  string. Deleting the model without updating this test fails the suite. Add it to §3's re-home/retire
  table. (The sweep found no *other* omissions — the spec's consumer accounting is otherwise complete,
  including the HF publish, workflow card, and Terraform tasks it lists separately from the "0/0"
  line.)
- **m2 · The oracle becomes un-regenerable, and §3 doesn't say so.** §3 "Keep" preserves the frozen
  `oracle_fct_tracking_context.parquet` and updates `oracle_map.py`'s docstring — good. But
  `scripts/extract_action_context_fixture.py:411` regenerates that parquet via
  `SELECT * FROM …fct_tracking_context`. After PR-1 the source table is gone, so the regeneration path
  dies (silently — nothing tests it). For a regression oracle that AC-1 still validates against, losing
  the ability to *refresh* it is a real (if small) cost. Either point the regen query at
  `fct_action_context` (its superset successor) or explicitly document the oracle as frozen-forever and
  remove the dead regen branch so it doesn't mislead.

---

## §7 (pausa) — the right call, and the right reason

Deferring `fct_pausa_values` on a 0.425-MAD, 97%-divergent output disagreement — with the key bridge
*proven* (100%, 1:1) so it's demonstrably a values bug, not a join bug — is exactly right, and
"never silently substitute data under three live pages" is the correct principle. No notes.

---

## Bottom line

| PR | Verdict |
|---|---|
| **PR-1 (TC-1 kill + re-home)** | Sound. Ship after: (M1) measure AC's divergent-dup rate — add a tiebreaker if non-zero; (M2) widen the parity-gate acceptance criterion to admit orientation corrections; (m1) add `test_staging_coverage.py:88`; (m2) address the oracle regen path. |
| **PR-2 (orphan deletions)** | Approvable as written — verified consumer-free. |
| **PR-3 (view-ify)** | **Blocked (B1):** a view can't back the TRIGGERED synced table; needs a SNAPSHOT switch + `triggered_synced_marts` removal the spec omits, and the mart's own `--full-refresh`-masking history makes view semantics the riskiest option. It's already on AC, so reconsider whether PR-3 is worth doing at all. |

The investigation's core — TC-1 is a redundant *pipeline* producing a strict subset on worse frames —
is well-evidenced and I did not find a hole in it. The gaps are downstream of that conclusion: the
re-home target shares TC-1's dedup disease (M1), the parity gate can't tell the two correctness
improvements apart (M2), and the one PR framed as cosmetic is the one with a real mechanism break (B1).
