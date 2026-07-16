# Mart Consolidation: TC-1 Retirement + Orphan-Mart Sweep — Design

**Date:** 2026-07-14
**Status:** Draft — awaiting review
**Supersedes:** the standing "fct_tracking_context retirement" idea; the "T-mart retirement into AC" idea (partially — see §7)

---

## 1. Problem

`compute_tracking_context` (TC-1) is **not a redundant mart. It is a redundant pipeline**, and it is
producing **worse data** than the pipeline it duplicates.

Both `compute_action_context` (AC-1) and `compute_tracking_context` (TC-1) independently:

- read the same three bronze tracking tables (`idsse_tracking`, `metrica_tracking`, `skillcorner_tracking`),
- independently call `link_actions_to_frames`,
- then run **the same silly-kicks enrichment chain, in the same order**: `add_pre_shot_gk_context` →
  `add_action_context` → `add_actor_pre_window` → `add_pressure_on_actor` → `pitch_control_at_target` →
  `add_defensive_line` → `add_off_ball_context` → `add_line_break` → `add_team_shape` → DAS →
  `add_gk_influence` → `add_cover_shadows` → `add_sync_score`.

AC-1 then continues with ~14 further steps (OBSO, PAUSA, space-creation, ghost-GK, shape-graph, the `xt_gk_*`
family, …) that TC-1 never computes.

### Measured, live (2026-07-14)

| | `fct_tracking_context` | `fct_action_context` |
|---|---|---|
| Grain | `(match_key, action_id)` | **identical** |
| Feature columns | 66 | **all 66, same names** + ~50 more |
| Providers | 3 | **6** |
| Frame converters | hand-rolled, pre-TF-23 | silly-kicks builders |
| Orientation (metrica/SkillCorner) | **NONE** | geometric LTR |
| Grain-uniqueness test | **none** | yes |
| dbt refs / Taipy pages | **0 / 0** | 5 / 1 |

**Distinct `(match_id, action_id)` coverage — the decisive number:**

```
provider      TC keys    AC keys    TC-only    AC-only
idsse           8,430      8,430          0          0
metrica         4,319      6,159          0      1,840
skillcorner    73,839    134,753          0     60,914
```

**`TC-only = 0` for every provider.** AC is a *true* superset — there is not one action TC-1 covers that AC
does not. TC-1's raw IDSSE row count (9,209) *appears* larger only because of its **779 duplicate keys**;
9,209 − 779 = 8,430, exactly AC's count.

**TC-1's duplicate keys: 779 (idsse) + 419 (metrica) + 2,854 (skillcorner) = 4,052** — an arbitrary pick among
**content-divergent** rows (`stg_spadl__tracking_context.sql:16-19` dedups on `_ingested_at desc` with **no
tiebreaker**). This is an ADR-030-class bug, and it currently **taints TC-1's only two real consumers**.

> **The re-home TARGET must not share the disease — measured, live (review M1).** AC's staging uses the
> **byte-identical** tiebreaker-less dedup (`stg_action_context__values.sql:16-19`). The honest question is
> therefore whether AC's *bronze* also carries divergent dups. It does **not**:
> ```
> AC   bronze: 937,324 rows, 937,324 distinct (data_source, match, action) keys → 0 duplicate rows
> TC-1 bronze:  90,640 rows,  86,588 distinct keys                             → 4,052 duplicate rows
> ```
> AC is dedup-free **by construction**: the AC-1 drain's M13 work-unit ownership model writes exactly one row
> per `(match, period)` unit, so the dedup is a genuine no-op. TC-1 has no such ownership and re-computes the
> same action across overlapping frame batches — hence the 4,052 divergent picks. So the re-home **removes**
> the arbitrary pick; it does not relocate it. **G3's "dedup-free keys" claim is now substantiated with the
> number, not asserted.** But the missing tiebreaker in AC's staging is *latent* correctness — clean today,
> unguarded tomorrow — so this program adds a regression guard (§3, invariant test) and a deterministic
> tiebreaker to AC's dedup as cheap defense-in-depth.

So TC-1 is redundant *and* wrong: its metrica/SkillCorner spatial features are computed on **un-oriented
frames** (its `_bronze_metrica_to_frames` / `_bronze_skillcorner_to_frames` take no orientation flag and call
no orientation function — they predate the TF-23 migration), while AC's go through the silly-kicks builders +
geometric LTR.

### The rest of the mart layer

The same investigation found **four gold marts with zero dbt refs and zero Taipy consumers** that still pay
for Lakebase synced tables, and one mart that is a pure projection of another.

---

## 2. Goals / Non-goals

**Goals**

- G1. Delete the TC-1 pipeline end-to-end (task → bronze → staging → mart → synced table → HF dataset),
  re-homing its two real consumers onto AC.
- G2. Delete four orphaned gold marts (0 dbt refs, 0 Taipy consumers).
- G3. Leave the platform strictly *more* correct: the re-homed consumers must gain oriented frames, dedup-free
  keys, and broader provider coverage — **demonstrated, not asserted**. Status: oriented frames = proven by the
  converters; dedup-free = **measured** (AC bronze 0 divergent dups, §1); coverage = proven (`TC-only = 0`).
  ~~G4 was the old "view-ify `fct_gk_tracking_actions`" goal — **dropped**, review B1, §3 Phase 3.~~

**Non-goals**

- N1. **`fct_pausa_values` is OUT OF SCOPE.** See §7 — it is blocked on a newly-discovered value-disagreement
  bug, not on anything this program can fix.
- N2. `fct_action_values` is not touched. It is the most-depended-on mart in the repo (15 dbt refs + 5 Taipy
  modules) and covers **all** providers where AC covers only tracking ones. It is the anchor, not a target.
- N3. `fct_defcon_actions` (action × **defender** fan-out), `fct_shot_xg` / `fct_shot_psxg` (legitimate ADR-013
  ML-output marts) are not consolidation candidates and are not touched.
- N4. No change to what AC-1 computes. This program **deletes** duplicate compute; it does not add any.

---

## 3. Scope

### Phase 1 — TC-1 vertical kill

**Delete:**

| Layer | Object |
|---|---|
| Terraform task | `preflight_tracking_context`, `compute_tracking_context` |
| Bronze | `bronze.spadl_tracking_context` |
| Producer | `src/ingestion/tracking_context.py` (+ its entry points in `pyproject.toml`) |
| Staging | `stg_spadl__tracking_context` |
| Mart | `fct_tracking_context` |
| Lakebase | `fct_tracking_context_synced` (SNAPSHOT) + its 3 PG indexes (`create_indexes.py:224-230`) |
| HF | `luxury-lakehouse/spadl-tracking-context` dataset + `scripts/publish_tracking_context_hf.py` |
| Card | `workflow-cards/wf-tracking-context.yaml`; rows in `task_workflow_mapping.csv` |

**Re-home (the actual work):**

| Consumer | Today | After |
|---|---|---|
| `int_tracking_goalkeepers` (`:14-22`) | `stg_spadl__tracking_context` | `stg_action_context__values` |
| `int_minutes_played_per_match` idsse leg (`:162-171`) | `stg_spadl__tracking_context` | `stg_action_context__values` |
| `assert_idsse_minutes_roster_vs_tracking_context.sql` | asserts vs TC-1 | re-home or retire |
| `assert_unresolved_gk_player_ids.sql` | asserts vs TC-1 | re-home |
| `assert_idsse_gk_parity.sql` | TC-1-derived vs `stg_idsse__tracking` | re-home |
| `src/tests/test_staging_coverage.py:87-88` | lists `("spadl_tracking_context", "stg_spadl__tracking_context")` by bare string | **remove the entry** (review m1 — deleting the model without this fails the suite) |

**Also in Phase 1 (review m1/m2 + M1 guard):**

- **Add a deterministic tiebreaker to AC's dedup** — `stg_action_context__values.sql:16-19` gains a stable
  secondary sort (e.g. `_ingested_at desc, action_id` or a content hash) so the pick is deterministic if AC
  ever *does* write a divergent dup. Inert today (0 dups) but removes the latent ingest-order dependency for
  AC's ~55 downstream models, not just the two re-homed TC-1 consumers.
- **Add a BRONZE-source zero-dup invariant test (review-2, the correction that matters).** The guard MUST
  count `(data_source, match_id, action_id)` duplicates in **`bronze.spadl_action_context`** and assert zero —
  a dbt singular test over the source table. A `unique_combination_of_columns` on the *mart* is **vacuous
  here**: `fct_action_context` is built on the staging dedup (`row_number() = 1`), so its grain is unique *by
  construction* and passes whether or not bronze has dups. Worse, the deterministic tiebreaker added above
  makes a bronze regression **silent and reproducible** at the staging layer — so **bronze is the only place
  an M13-ownership regression is visible.** The test must live there, or it does not guard the thing it claims
  to.
- **Also add the mart-grain `unique_combination_of_columns([match_key, action_id])` test** that
  `fct_action_context` currently lacks — but it defends a **different** property (join integrity / no
  accidental fan-out downstream), NOT ownership. It must **not** stand in for the bronze guard above.
- **The AC-1 oracle becomes un-regenerable — fix the regen path.**
  `scripts/extract_action_context_fixture.py:411` regenerates the kept `oracle_fct_tracking_context.parquet`
  via `SELECT * FROM …fct_tracking_context`. After the mart is dropped this path dies silently. **Re-point it
  at `fct_action_context`** (the superset successor, filtered to TC-1's column set) so the oracle stays
  refreshable, rather than leaving a dead branch that regenerates from a deleted table.

The re-home is **column-complete**: `stg_action_context__values.sql:27-41,53-63` casts the identical block
(`data_source`, `native_match_id`, `player_id_native`, `defending_gk_player_id_native`) that
`stg_spadl__tracking_context.sql` does. It is a `ref()` swap.

> **⚠ THE ONE TRAP.** `int_tracking_goalkeepers.sql:21` filters only `where defending_gk_player_id_native is
> not null` — it has **no `data_source` filter**. Swapping the source therefore silently admits
> **gradientsports** and **statsbomb-360** rows (AC populates the GK column for both; TC-1 covered neither).
> They are *inert* today — `fct_tracking_frames` only carries idsse/metrica/skillcorner, so the new rows
> orphan on its `left join` — but relying on that is exactly the kind of accidental-correctness this codebase
> has been bitten by. **Add an explicit `data_source in ('idsse','metrica','skillcorner')` filter** and pin it
> with a test. Widening coverage is a *separate, deliberate* decision, not a side-effect of a refactor.

**Keep:** `src/tests/action_context/` oracle fixtures (`oracle_fct_tracking_context.parquet`). They are a
**frozen parquet snapshot**, not a live query — AC-1's regression suite still validates against them. Update
`oracle_map.py`'s docstring to record that the live TC-1 pipeline is retired and the oracle is historical.

### Phase 2 — orphan-mart deletions (0 dbt refs, 0 Taipy consumers)

| Mart | Synced | Why it is safe | Note |
|---|---|---|---|
| `fct_space_creation` | TRIGGERED | values already on AC (`space_created_m2`) | the **only** mart with `contract: enforced: false`; memory flags `space_created_m2_opponent ≡ 0` as known-bad |
| `fct_off_ball_xt` | TRIGGERED | AC carries `off_ball_xt_team/opponent/diff`; `fct_physical_stats` already **bypasses this mart**, reading `stg_off_ball_xt__results` directly | |
| `fct_line_breaking_results` | TRIGGERED | `fct_passes` reads `stg_line_breaking__results` **directly** (`fct_passes.sql:133-135`), not through this mart | |
| `fct_gk_actions_detail` | yes | a **100% filtered projection** of `fct_action_values` (GK + pass/goalkick). Its former Taipy consumer (`hf_taipy_app/src/queries/goalkeepers.py`) **no longer exists** | |

> **These are MART-only deletions. The producing TASKS STAY.** `compute_line_breaking` and
> `compute_off_ball_xt` keep running — their *staging views* feed other marts directly. Only TC-1 gets the
> full vertical kill. Confusing "delete the mart" with "delete the task" here would break `fct_passes` and
> `fct_physical_stats`.

### Phase 3 — ~~`fct_gk_tracking_actions` → view~~ **REJECTED (review B1)**

**This phase is dropped.** The original idea was to convert `fct_gk_tracking_actions` from a materialized
incremental mart into a view, on the theory that it is "`AC ⋈ AV` + 4 derived" and therefore cosmetic. Review
found a hard blocker and a weak payoff:

1. **A view cannot back its synced table.** `fct_gk_tracking_actions_synced` is **TRIGGERED**
   (`refresh_synced_tables.py:237`), and the mart sets `delta.enableChangeDataFeed = true` (`:8`). A TRIGGERED
   Lakebase sync reads the source's **Change Data Feed** — a view has no CDF and no Delta history, so it
   **cannot be a TRIGGERED sync source**. The live Goalkeeper Analytics page reads that synced table
   (`hf_taipy_app/src/queries/gk_analytics.py:79`), so view-ifying breaks its data path.
2. **It is not cosmetic — it is a materialization + sync-mode change the spec skipped.** View-ifying forces
   dropping `fct_gk_tracking_actions` from `triggered_synced_marts` (`dbt_project.yml:140`) **and** flipping
   `refresh_synced_tables.py` to `SNAPSHOT` — together, or `test_strand_safe_rederive.py` trips.
3. **The query is 5 joins, not 2.** It is `AC ⋈ dim_matches ⋈ dim_teams ⋈ dim_players ⋈ dim_players ⋈
   fct_action_values` — two self-joins on `dim_players`, one to the anchor mart. Paid once as a table; paid on
   **every refresh** as a SNAPSHOT view.
4. **The mart has a documented `--full-refresh`-masking bug.** `fct_gk_tracking_actions.sql:44-50` records that
   a latent AC-schema dependency once "orphaned the final-select references and broke `--full-refresh`; the
   daily incremental masked it." A view is *permanent* full-refresh semantics — the single most likely way to
   re-expose exactly that class of bug.
5. **It is already on AC** (`fct_gk_tracking_actions.sql:17` reads `stg_action_context__values`). So the change
   would be a pure materialization swap that re-sources nothing and buys nothing — while taking on (1)–(4).

**Net: the payoff is ~zero and the risk is real. `fct_gk_tracking_actions` stays a materialized incremental
mart.** If a future need arises to reduce its storage, revisit as its own change with the sync-mode decision
made explicitly.

---

## 4. The parity gate (G3) — non-negotiable

TC-1's two consumers feed **`fct_tracking_frames.is_goalkeeper`** (a TRIGGERED mart behind 3 Taipy pages) and
**`fct_goalkeeper_stats`**. Re-homing them **changes their inputs**. Before any deletion:

1. **GK-identity parity.** For idsse/metrica/skillcorner, compare `int_tracking_goalkeepers` built from TC-1
   vs from AC.
   - **idsse — expect identical, and know WHY** (review M2): *not* "because the key sets are equal" (equal
     action coverage does not imply equal per-action GK values — a non-sequitur). The real reason is that
     idsse frames are **oriented in both** pipelines (TC-1's idsse leg uses silly-kicks' native
     `output_convention="ltr"`; AC's idsse leg is oriented too) **and** both derive GK via the same
     silly-kicks `derive_goalkeepers()`. So an idsse difference would signal a **genuine derivation change**
     worth stopping for.
   - **metrica/SkillCorner — a difference has TWO legitimate mechanisms, and the gate must admit both.**
     Re-homing changes *two* variables at once: the dedup pick **and** frame orientation (TC-1 un-oriented →
     AC LTR). `derive_goalkeepers()` is a positional 3-tier method (`int_tracking_goalkeepers.sql:4`), so its
     output is **orientation-sensitive**. A metrica GK that differs between TC-1 and AC could be a dedup
     artefact **or** an orientation correction — and the latter is *the spec's own headline argument*. An
     acceptance criterion of "explain as a **TC-1 dedup** artefact" would make an implementer **STOP on a
     correct improvement.**
   - **Correct criterion (widened):** a metrica/SkillCorner difference is acceptable if **AC's value is
     verified correct against roster ground truth** (the provider's own GK designation / `dim_players`
     position), regardless of whether the mechanism is dedup or orientation. Only a difference where AC is
     *wrong* against ground truth is a STOP.
2. **IDSSE-minutes parity.** The `idsse_roster` leg must yield the same distinct (match, player) set.
   Confirmed context: IDSSE minutes never reach `fct_player_stats` anyway (`int_minutes_played.sql:20,40` —
   `try_cast(native_player_id as bigint)` NULLs DFL strings and they are dropped), so this leg feeds
   `fct_goalkeeper_stats` only. That narrows the blast radius but does not remove the need for the check.
3. **Row-count deltas** on `fct_tracking_frames.is_goalkeeper` and `fct_goalkeeper_stats`, before vs after.

**If the parity check shows an unexplained difference, STOP.** Do not proceed on the theory that "AC is newer
so AC is right."

---

## 5. What this buys

- **Removes an entire duplicate compute pass** — a 4-way `for_each` task re-doing AC-1's enrichment chain.
- **Fixes a correctness bug**: metrica/SkillCorner GK identity is currently derived from **un-oriented**
  frames and an **arbitrary pick among 4,052 divergent duplicate keys**.
- **Removes 5 gold marts and 5 Lakebase synced tables** — **3 TRIGGERED** (`fct_off_ball_xt`,
  `fct_space_creation`, `fct_line_breaking_results`) + **2 SNAPSHOT** (`fct_tracking_context`,
  `fct_gk_actions_detail` — neither carries a policy arg, so both default to SNAPSHOT) — with zero consumer
  impact. **Teardown consequence:** only the 3 TRIGGERED tables appear in `dbt_project.yml`
  `triggered_synced_marts` (`:136-137,142`); removing them from `SYNCED_TABLES` without also removing them
  there (or vice-versa) trips `test_strand_safe_rederive.py`. The 2 SNAPSHOT tables are absent from that var,
  so they need removal from `SYNCED_TABLES` (+ any PG-index defs) only.
- **Deletes a whole `src/ingestion/` module** (`tracking_context.py`) and its hand-rolled, pre-TF-23 frame
  converters — the last remaining copy of converter logic that silly-kicks now owns (the ADR-055 / ADR-067
  "delete-and-depend" precedent).

---

## 6. Risks

| Risk | Mitigation |
|---|---|
| GK identity values change | §4 parity gate (widened per M2): a metrica/SkillCorner difference is acceptable if AC is verified correct vs roster ground truth — dedup *or* orientation. Only AC-wrong is a STOP. |
| Re-home relocates the dedup arbitrary pick | **Retired** — measured: AC bronze has 0 divergent dups (M13 ownership). Plus a deterministic tiebreaker + zero-dup invariant test are added to AC's staging as defense-in-depth. |
| `int_tracking_goalkeepers` silently gains GS/SB360 rows | Explicit `data_source in ('idsse','metrica','skillcorner')` filter + a test. (§3, THE TRAP) |
| An **external** Lakebase/HF consumer we cannot see from source | Unknowable from the repo. **Owner decision required** on the `spadl-tracking-context` HF dataset (retire / freeze / republish from AC). |
| Missed teardown file → parity test fails | `test_strand_safe_rederive.py` enforces `SYNCED_TABLES` ↔ `dbt_project.yml:triggered_synced_marts` parity. Removing from one list only will trip it. |
| Deleting a mart whose *task* is still needed | §3 Phase 2 note. `compute_line_breaking` / `compute_off_ball_xt` **stay**. |
| AC-1 regression oracle silently un-regenerable | §3 — re-point `extract_action_context_fixture.py:411` at `fct_action_context`, don't leave a dead branch. |

---

## 7. Explicitly deferred: `fct_pausa_values`

**Not deferred for lack of a key.** The bridge is **proven**: `fct_passes.pass_id` is reconstructible from
`fct_action_values` via `original_event_id`, and live it resolves **all 1,627** pausa rows to a unique
`(match_key, action_id)` — 100%, 1:1, no fan-out.

**Deferred because the two pausa pipelines disagree.** Live, over the 1,627 shared IDSSE actions:

```
MAD(pausa_score vs pausa_composite) = 0.42545    <-- on a 0-1 SCALE
MAD(actual_obso  vs obso_actual)    = 0.03635
rows differing by >1%               = 1,574 / 1,627  (97%)
```

The OBSO **inputs** agree; the pausa **outputs** do not. `fct_pausa_values` is an **in-repo reimplementation**
(`src/analytics/pausa.py`) over GPU-batch OBSO scalars and is **idsse-only** (1,627 rows); AC's `pausa_*` comes
from `silly_kicks.tracking.add_pausa` on live frames for **all** tracking providers.

**One of them is wrong and we do not know which.** Consolidating would silently swap 97% of the values under 3
live Taipy pages — precisely the "never silently substitute data" rule. It needs its own investigation:
hand-compute pausa for a handful of IDSSE actions from raw OBSO, decide which pipeline is canonical, *then*
re-key or retire.

---

## 8. Sequencing

**Two PRs** (PR-3 was dropped — §3 Phase 3, review B1). Each is independently shippable and revertable.

1. **PR-1 (TC-1)** — the widened parity gate, the AC dedup tiebreaker + zero-dup invariant, the re-home
   (incl. `test_staging_coverage.py` + the oracle regen re-point), then the vertical kill. Riskiest, most
   valuable.
2. **PR-2 (orphans)** — the four zero-consumer mart deletions (`fct_space_creation`, `fct_off_ball_xt`,
   `fct_line_breaking_results`, `fct_gk_actions_detail`). Mechanical; verified consumer-free by review.
   Gated on PR-1 only to keep the diffs legible.

`fct_gk_tracking_actions` is untouched — it stays a materialized incremental mart (review B1).
