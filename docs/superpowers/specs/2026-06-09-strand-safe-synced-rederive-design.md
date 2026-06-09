# Strand-safe re-derive for TRIGGERED synced tables — Design

**Date:** 2026-06-09
**Status:** Approved-pending-review (rev 5 — adds the **T (plain rebuild) action** for the 2 `table` marts (B-1/C-1: zero-downtime, no synced delete, matches the strand-free daily build) on top of rev 4's unified `--full-refresh` tripwire. Plus B-2/C-2/C-3/m-1/m-2/m-3 from the 2nd plan review. See §2 + §8.)
**Author:** Karsten Nielsen (with Claude)
**ADR:** ADR-043 (new, written in this PR)

## 1. Context

A `dbt --full-refresh` of a gold mart whose Lakebase synced table is **`scheduling_policy = TRIGGERED`**
(CDF-streamed) **overwrites** the source Delta table and strands the synced table's streaming checkpoint →
`SYNCED_TABLE_ONLINE_PIPELINE_FAILED` / `DIFFERENT_DELTA_TABLE_READ_BY_STREAMING_SOURCE`. **SNAPSHOT** synced
tables are immune (they re-copy in full; no streaming checkpoint).

### Evidence (live-verified 2026-06-09)

- **Strand ledger** (`soccer_analytics.observability.synced_table_strand_state`): the only `stranded`
  events ever recorded are `fct_action_values_synced`, `fct_passes_synced`, `fct_pausa_values_synced`, all
  on **2026-06-08** — a single incident: an operator `dbt build stg_spadl__action_values+ --full-refresh`
  (the v8 VAEP re-score). A prior **2026-06-06** mass `healed` row exists (an earlier incident, recovered).
  **No strand is recorded from any routine daily build.**
- **`DESCRIBE HISTORY`**: `fct_pausa_values` (a `table` mart) is rebuilt via `CREATE OR REPLACE TABLE`
  repeatedly — 06-04, 06-05, 06-06, 06-08, 06-09 — yet **only the 06-08 `--full-refresh` build correlates
  with a strand**. The 06-04/05/06 routine `CREATE OR REPLACE` builds stranded nothing.
- **Conclusion:** the strand vector in practice is **`--full-refresh` (and the job-level
  `dbt_full_refresh=true` parameter)**, *not* routine builds. Routine daily builds — incremental `MERGE`
  and `table` `CREATE OR REPLACE` alike — do not strand.

**Honest caveat (mechanism unresolved, design does not depend on it):** the heal e2e empirically found a raw
`CREATE OR REPLACE TABLE` *keeps* the Delta table id (does not strand), yet the 06-08 `--full-refresh`
(which for these marts also emits `CREATE OR REPLACE TABLE`) *did* strand — so the exact id-minting
condition is not pinned down. This design is robust regardless: **D never overwrites** (MERGE), **B deletes
the synced table before rebuilding** (nothing to strand by construction), and the **tripwire blocks
`--full-refresh`** (the one vector with recorded strands).

**Why this design (not just the existing self-heal):** ADR-041 *recovers* a strand, but (a) its dispatch
leg is unwired (`GH_DISPATCH_TOKEN` provisioned nowhere → "Heal dispatch skipped"), so recovery waits for
the daily 07:00 cron — up to ~24 h; and (b) an operator should not be *able* to strand a TRIGGERED table
with a routine command. This design makes operator re-derives strand-safe via one tool + a runtime tripwire.

**Non-negotiable operator constraint:** SNAPSHOT-vs-TRIGGERED and `table`-vs-`incremental` are deliberate
**size-based** decisions. This design changes **no scheduling policy and no mart materialization**; it
changes the operator re-derive *path* and adds a guardrail.

## 2. The 13 TRIGGERED marts (verified) and how the tool handles each

Strand risk is **exactly** the TRIGGERED set (`scheduling_policy='TRIGGERED'` in `SYNCED_TABLES`):

| Bucket | Marts | Tool action |
|---|---|---|
| **D — incremental + `match_id`-filter** (7) | `fct_action_values`, `fct_defcon_actions`, `fct_defcon_pressure`, `fct_defensive_values`, `fct_off_ball_xt`, `fct_tracking_frames`, `fct_tracking_shape_timeline` | **MERGE-reprocess** (no `--full-refresh`): per-match DELETE + re-MERGE → CDF partial-update, no overwrite, no strand |
| **T — `table`-materialized** (2): `fct_pausa_values`, `fct_space_creation` | **plain rebuild**: `dbt build` (atomic `CREATE OR REPLACE`, no `--full-refresh`) → refresh synced. Count-safe (full replace), strand-free (exactly the daily stage-3 path), **zero downtime** (synced never deleted) |
| **B — merge-all incremental** (4): `fct_action_context`, `fct_line_breaking_results`, `fct_passes`, `fct_player_embeddings` | **rebuild**: delete synced → `dbt build --full-refresh` → recreate synced → grants+indexes. Count-safe + strand-safe **by construction** (synced deleted before the overwrite) |

**Why merge-all marts go to B, not plain-build (B3):** a plain MERGE with no `is_incremental` filter
updates+inserts but **never deletes** → a count-reducing re-derive orphans rows in gold and PG. `fct_passes`
is pure-surrogate (`unique_key='pass_id'`, no `match_id`) so it cannot use the D macro. They are `incremental`,
so a plain `dbt build` only inserts-new (won't shrink); a `--full-refresh` would, but that strands → hence
delete→recreate around it. Routing all merge-all marts to B avoids making the operator judge "is this
count-reducing?".

**Why table marts go to T, not B (B-1/C-1):** a `table` mart's plain `dbt build` is an atomic
`CREATE OR REPLACE` — a full replace (so count-safe, including shrink) that **the daily stage-3 already runs
every day strand-free** (§1 evidence: fct_pausa_values `CREATE OR REPLACE` on 06-04/05/06 stranded nothing;
only the 06-08 `--full-refresh` did). So T needs neither `--full-refresh` (the recorded strand vector) nor a
synced delete/recreate → **zero downtime**. This also removes a footgun B would have had for
`fct_space_creation`: it has **no node-level `enabled=`** (only a body gate), so it always builds 0 rows in
production; T's plain build reproduces that 0-row state without injecting `space_creation_enabled` (which
would wrongly populate it). `--rebuild` still routes a T mart through B when the operator needs to refresh the
synced **schema** (a plain `CREATE OR REPLACE` won't propagate new columns to an existing synced table).

**C-1 honesty caveat:** the table-mart plain-vs-`--full-refresh` behaviors were *observed* (daily plain
builds vs the 06-08 `--full-refresh` incident), not A/B-tested in isolation. Inferred mechanism: a `table`
`--full-refresh` does a drop+recreate (new Delta id → strand) while a plain build is an atomic replace (same
id → no strand). **T is safe under either reading** because it uses only the plain-build path the daily
pipeline already runs.

**Why `fct_player_embeddings` is B, not D (N-e):** incremental + has `match_id` + deterministic key
(`fct_player_embeddings.sql:28,45`) — so D *looks* possible. It stays B **deliberately**: its incremental
body is merge-all (no `not in (this)` filter), so adding the D filter would change its **daily** merge
semantics. B is a choice, not an oversight.

**Surrogate-key determinism (D safety, verified):** 6 of the 7 D marts build their PK in-mart via
`dbt_utils.generate_surrogate_key([...])` and the `unique_key` equals the synced PK in `SYNCED_TABLES`.
**`fct_tracking_frames` is the exception (B1):** it *selects* `tracking_id` from staging, which generates it
deterministically (`stg_idsse__tracking.sql:20`, `stg_metrica__tracking.sql:89`,
`stg_skillcorner__tracking.sql:57` = `generate_surrogate_key(['match_id','period','frame','player_id'])`) —
stable, but **inherited upstream**. The whole-match DELETE pre_hook makes D safe regardless; the §8 e2e
spike covers `fct_tracking_frames`.

**Enablement (no override needed — verified):** every node-level `enabled=var('…')` gate resolves **true**
via `dbt_project.yml` defaults (`pausa_enabled`, `embeddings_enabled`, `defcon_enabled` all `true`), and
body-level gates likewise. The one exception, `fct_space_creation`'s `space_creation_enabled`, is
**intentionally absent (false) in production** (the mart is 0 rows by design). The re-derive therefore passes
**no enable vars** — it reproduces the daily build's enablement exactly. (Earlier rev's "inject enable vars"
step is dropped: it was redundant for the enabled marts and would have *diverged* `fct_space_creation` from
its intended-empty production state.)

**SNAPSHOT marts** are out of scope (immune; `--full-refresh` stays safe).

## 3. Component — D macros (CDF-preserving per-match re-derive)

Two macros in `dbt_project/macros/reprocess_match_ids.sql`, applied to the 7 D marts.

- **`reprocess_predicate(match_col='match_id')`** — an **OR-include** appended *inside parentheses that wrap
  the existing `not in (...)` filter* (precedence-critical — see below), so a reprocessed match re-enters
  the SELECT:
  ```sql
  where (match_id not in (select …) {{ reprocess_predicate('match_id') }})
  ```
  renders `or match_id in (1, 2)` when ids are set, else empty. `| length > 0` avoids `in ()`; `| map('int')`
  is injection-safe (C4).
- **`reprocess_delete_hook(match_col='match_id')`** — model `pre_hook`. Deletes the reprocessed matches
  up-front so a re-derive that drops rows can't orphan them (MERGE never deletes) and a surrogate-key shift
  can't strand the old key. No-op unless `is_incremental()` and ids set.

CDF effect: DELETE → delete events; re-MERGE → upsert events; the TRIGGERED synced table converges on the
next refresh. **No overwrite → no strand.**

**Precedence (implementation-critical):** the filter sites differ — some marts use
`where match_id not in (select distinct match_id from {{ this }} …)`, others an `existing_matches` CTE
(`where match_id not in (select match_id from existing_matches)`), and `fct_tracking_frames` has **three**
union arms. The OR-include MUST be wrapped so it scopes only the match-exclusion, e.g.
`where player_id is not null and (match_id not in (…) or match_id in (…))` — not
`… and match_id not in (…) or match_id in (…)` (which would re-admit null-player rows). The plan specifies
the exact parenthesized replacement per site.

**N1 (ADR rationale):** after the pre_hook DELETE, the existing `not in (this)` filter *already* re-includes
the reprocessed matches, so `reprocess_predicate` is redundant **in the happy path** — but it is a
**data-loss safety net**: if the DELETE is not yet visible to the SELECT (commit ordering), the OR-include
still re-admits the match, so a deleted match is never left un-reinserted. Keep it; the ADR records why.

**Daily behavior is identical** — var unset → both macros render empty.

## 4. Component — single dispatching tool `scripts/rederive_synced_marts.py` (C1)

**The only operator entry point.**

```
uv run --extra sdk python scripts/rederive_synced_marts.py --select <selector> [--provider P | --match-ids a,b,c] [--rebuild] [--dry-run] [--force]
```

**Port/adapter split (N-c — hexagonal, TDD seam):**
- **Pure planner** `src/ingestion/rederive_planner.py` — `plan_rederive(selected_models, match_ids, rebuild=False) -> list[PlanStep]`,
  zero IO. `PlanStep(model, synced_table, action: "D"|"T"|"B", full_refresh, dbt_vars)`. All classification
  (D/T/B routing via `D_REPROCESS_MODELS` + `_TABLE_MARTS`, TRIGGERED filtering, D→T→B ordering, `--rebuild`
  routing) lives here, fully unit-testable.
- **Thin executor adapter** (the script) — resolves the selection + match ids, guards, runs the plan by
  composing existing scripts (`dbt`, `delete_synced_table.py`, `create_synced_table.py`,
  `maintain_synced_tables.py`, `ingestion.refresh_synced_tables`). The only layer touching network/fs.

Algorithm:
1. `dbt ls --select <selector> --resource-type model --output name --quiet` → selected model names (m3:
   `--quiet` so a stray log line can't be mistaken for a model).
2. `plan_rederive` intersects with the TRIGGERED set from `SYNCED_TABLES` (SNAPSHOT/non-synced skipped).
3. `reprocess_match_ids` from `--match-ids` or `--provider` (→
   `select distinct match_id from soccer_analytics.bronze.spadl_actions where data_source = :p` —
   `bronze` schema live-confirmed, NOT `dev_bronze`). Fail loud if a **D** step has no match ids (no-op).
4. **Dispatch per step:**
   - **D** → `dbt build --select <mart> --vars '{reprocess_match_ids: [...]}'` (no `--full-refresh`), then
     `python -m ingestion.refresh_synced_tables --tables <synced> --wait` to pull the CDF into the TRIGGERED
     synced table. (C4 verified: `refresh_synced_tables` is **policy-agnostic** — `main()` triggers the
     backing pipeline for any table by name, no SNAPSHOT-only filter; C-3 verifies `--wait` reaches the
     incremental update's terminal `COMPLETED` before returning.)
   - **T** → `dbt build --select <mart>` (plain, no `--full-refresh`, no vars), then
     `refresh_synced_tables --tables <synced> --wait`. No synced delete/recreate.
   - **B** → `delete_synced_table.py <synced>` → `dbt build --select <mart> --full-refresh --vars
     '{allow_triggered_full_refresh: true}'` → `create_synced_table.py <synced>` (waits online). After all
     B steps, run `maintain_synced_tables.py --skip-heal --skip-refresh` once (grants + indexes).
5. **`--rebuild` (C3/m-3):** routes **every** selected mart through the **B** path
   (delete→full-refresh→recreate) — the sanctioned full-rebuild for a **D** mart's schema/contract change,
   or to refresh a **T** mart's synced **schema** (a plain `CREATE OR REPLACE` won't propagate new columns to
   an existing synced table). The tripwire blocks a bare `dbt --full-refresh`, so this flag is the intended
   escape.
6. **`--dry-run` (N-d):** prints the resolved plan (per mart: D/T/B, vars, downtime estimate) and exits.
7. **Idempotency (C5) + downtime (T5):** `delete_synced_table` no-ops if absent; steps are re-runnable;
   **D and T have no downtime** (synced never deleted). B downtime = synced re-snapshot, row-count-driven:
   `fct_passes` / `fct_line_breaking_results` are seconds-to-minutes; **`fct_action_context` /
   `fct_player_embeddings` (multi-million rows) need a real maintenance window** — the tool prints this and
   `--dry-run` shows it.

**Concurrency guard (N3 / T4):** before executing (not for `--dry-run`), the tool queries real job state —
`w.jobs.list_runs(job_id=302697362345215, active_only=True)` — and **refuses** if the daily ingestion job is
active, unless `--force`. Tied to job state, not the clock.

## 5. Component — runtime tripwire (`on-run-start`)

`dbt_project/macros/assert_no_triggered_full_refresh.sql`, wired via `on-run-start`.

**Unified rule (rev 4 — supersedes rev 3's T1 materialization split):** abort iff `flags.FULL_REFRESH` **and**
a selected node is in the TRIGGERED registry, unless `allow_triggered_full_refresh` is set.

**Why the rev-3 "abort any `table`-mart build" rule was wrong (P1, evidence §1):** the two `table` marts are
daily `output_mart`s — the scheduled stage-3 build selects `+tag:output_mart` and materializes them via
`CREATE OR REPLACE` every run (`terraform/modules/workflows/main.tf:765`). A rule that aborts *any* table
build would **abort the daily production build**. And the evidence shows routine table builds **don't
strand** — only `--full-refresh` does. So the correct, production-safe rule keys on `flags.FULL_REFRESH`
alone, for incremental and table marts alike.

```jinja
{% macro assert_no_triggered_full_refresh() %}
  {% if not execute or not flags.FULL_REFRESH %}{{ return('') }}{% endif %}
  {% if var('allow_triggered_full_refresh', false) == true %}{{ return('') }}{% endif %}
  {% set triggered = var('triggered_synced_marts', []) %}     {# committed registry of model names #}
  {% set hit = [] %}
  {% for uid in selected_resources %}                          {# N-a: on-run-start exposes unique_ids (dbt >=1.5) #}
    {% set node = graph.nodes.get(uid) %}
    {% set name = node.name if node else uid.split('.')[-1] %}
    {% if name in triggered %}{% do hit.append(name) %}{% endif %}
  {% endfor %}
  {% if hit | length > 0 %}
    {{ exceptions.raise_compiler_error(
        "Refusing --full-refresh of TRIGGERED synced source(s) " ~ (hit | join(', ')) ~
        " — it strands the Lakebase synced table. Use scripts/rederive_synced_marts.py (strand-safe).") }}
  {% endif %}
{% endmacro %}
```

- **N-a:** the live selection in `on-run-start` is `selected_resources` (unique_ids, dbt ≥1.5); map to node
  name via `graph.nodes` (bare `graph.nodes` is the whole graph; `selected_resources` is the narrowing).
- **N-b:** the registry holds **dbt model names** (`fct_passes`), matching `graph.nodes[*].name` — NOT the
  `_synced` names that key `SYNCED_TABLES`. The §6 parity test compares against `source_table`.
- **Registry mechanism:** a committed **`dbt_project.yml` var** `triggered_synced_marts: [<model>, …]`, read
  in-memory at `on-run-start` (a CSV seed would need a warehouse query, which `on-run-start` precedes).

**What aborts / what does not (with the unified rule):**
- `dbt --full-refresh --select fct_passes` → **abort** (operator footgun).
- daily stage-3 (`dbt build … +tag:output_mart`, no `--full-refresh`) → **runs** (table marts build normally).
- the re-derive tool's **D** path (no `--full-refresh`) and **T** path (plain `dbt build` of a table mart) →
  **run** (no `--full-refresh`).
- the re-derive tool's **B** path (`--full-refresh --vars allow_triggered_full_refresh: true`) → **runs**.
- mega-job with `dbt_full_refresh=true` → **abort** for stages 2 & 3 (they select TRIGGERED marts) — see §9 P2.

## 6. Component — static guard (`src/tests/test_strand_safe_rederive.py`, always-on CI, filesystem-only)

1. **Exhaustive D/T/B partition (T3):** for every TRIGGERED mart in `SYNCED_TABLES`, classify as exactly one
   of **D** (`∈ D_REPROCESS_MODELS`, incremental, SQL carries both reprocess macros), **T** (`∈ _TABLE_MARTS`,
   materialized `table`), or **B** (the remainder, incremental). Assert totality + pairwise disjointness. A new
   TRIGGERED mart with a novel idiom lands in no set → **fails CI**.
2. **Registry parity (N-b):** `dbt_project.yml`'s `triggered_synced_marts` == the TRIGGERED `source_table`
   set from `SYNCED_TABLES` (model names, not `_synced`).
3. **CDF coverage (C2 / m-1 — live-confirmed):** every TRIGGERED mart's SQL declares
   `delta.enableChangeDataFeed: 'true'` (value asserted, not just the key — m-1) in `tblproperties`.
4. **No bare full-refresh of a TRIGGERED source in committed automation:** scan `.github/workflows/`,
   `scripts/`, `workflow-cards/`, **and `terraform/`** (P2) for `--full-refresh` hitting a TRIGGERED source by
   name outside `rederive_synced_marts.py`. (The parameterized `--dbt-full-refresh {{job.parameters…}}` is a
   runtime lever, not flagged — the tripwire guards it; see §9 P2.) **m-2: name-based defense-in-depth only —
   a tag-selector `--full-refresh` (e.g. `--select tag:output_mart --full-refresh`) is NOT caught here; the
   runtime tripwire (§5) is the real guard for that.**
5. **Tripwire wiring:** `assert_no_triggered_full_refresh` is present in `dbt_project.yml` `on-run-start`.

## 7. (removed — classification is code in §4; no human procedure)

## 8. Testing

- **Macro render (C1) — NOT a CI gate (C-2 accepted limitation):** offline string-presence (§6.1) is the
  always-on guard; the *rendered* output is asserted **live** — `dbt compile --select fct_action_values --vars
  '{reprocess_match_ids:[1,2]}'` then grep the compiled SQL for `delete from … where match_id in (1, 2)` and
  `or match_id in (1, 2)`, plus the empty case renders neither. A pure offline unit test can't reproduce dbt's
  Jinja+adapter context (`is_incremental()`/`this`) and `dbt compile` needs a live connection, so macro
  *rendering* is not CI-gated; the offline string-presence test + this live grep + the e2e D-proof are the
  guards. **Documented so the gap is known, not silent.**
- **Tripwire (live, via `dbt run` — NOT `dbt compile`):** `on-run-start` hooks are inert under `dbt compile`
  (compile doesn't materialize, can't strand), so the tripwire is verified with `dbt run --full-refresh
  --select fct_passes` — it **aborts at on-run-start, before any model builds** ("Refusing --full-refresh of
  TRIGGERED synced source(s) fct_passes"), making it a *safe* check (nothing overwritten; verified live
  2026-06-09, `fct_passes_synced` stayed online). Same for `fct_pausa_values`. The allowed paths (plain
  `dbt build`; the `allow_triggered_full_refresh` override) are not run destructively — the plain build is the
  production daily path and the override is the tool's B path.
- **Planner units:** D/T/B classification, D→T→B ordering, `--rebuild` (D→B and T→B), TRIGGERED filtering,
  SNAPSHOT skip, both table marts → T — pure, offline, against `SYNCED_TABLES`.
- **Static guard (§6)** — always-on CI.
- **C-3 (live):** `refresh_synced_tables --tables <TRIGGERED> --wait` reaches the incremental update's
  terminal `COMPLETED` before returning (the D/T propagation step), so the tool never reports "done" while
  Lakebase is still consuming CDF.
- **C2 e2e — positive no-strand proof** in the existing `synced-table-heal-e2e.yml` harness (dbt-decoupled,
  raw SQL): add a test that the **D mechanism** — in-place `DELETE … WHERE id IN (…)` + `INSERT` on the same
  table (id unchanged) + one incremental refresh — keeps the synced table `SYNCED_TABLE_ONLINE` and converges
  counts. The negative half (new-id overwrite strands) is already locked by the existing test. The tripwire
  is **not** exercised here (the harness never runs dbt). Nightly + on-demand; not a PR gate. Uses **unique
  throwaway table names** (m1 — xdist-safe; the row-count assertion queries this test's own table — B-2).

## 9. Non-goals & documented behavior changes

- **No** scheduling-policy or materialization changes; SNAPSHOT marts unchanged.
- **Not** wiring `GH_DISPATCH_TOKEN` — the tripwire prevents the strand at the source; self-heal stays the
  backstop.
- **P2 — documented breaking change to the `dbt_full_refresh` job parameter
  (`terraform/modules/workflows/main.tf:67`):** with the unified tripwire, running the mega-job with
  `dbt_full_refresh=true` now **aborts stages 2 & 3 at `on-run-start`** (they select TRIGGERED marts —
  `fct_action_values` in stage 2; the rest in stage 3). This is **intentional**: a job-level full-refresh of
  a TRIGGERED mart is exactly the 2026-06-08 strand vector. **Decision:** keep the parameter (Chesterton's
  fence — it still legitimately full-refreshes SNAPSHOT-only selections like stage 1), but document that it
  is blocked for TRIGGERED-containing stages and the error message routes the operator to
  `rederive_synced_marts.py`. Recorded in ADR-043 + the CLAUDE.md Lakebase runbook pointer.

## 10. Risks / notes

- **N2 — CDF auto-reenables on rebuild:** every TRIGGERED mart carries `delta.enableChangeDataFeed:'true'`
  in `config.tblproperties` (live-confirmed all 13), which dbt reapplies on every CREATE/CREATE-OR-REPLACE. No
  manual `ALTER TABLE … ENABLE CDF` needed.
- **`fct_space_creation` is 0 rows in production — dated assumption (2026-06-09):** no node-level `enabled=`;
  the body gate `space_creation_enabled` defaults false (it has an `{% else %}` 0-row typed fallback,
  `fct_space_creation.sql:64–79`, so the plain build compiles to a runnable 0-row statement). The **T**
  re-derive reproduces that 0-row state via a plain build with no enable var injected. **If production ever
  enables `space_creation` (e.g. a per-run job var), the tool must be updated to inject
  `space_creation_enabled=true`** or T would shrink it back to 0 rows. Recorded in ADR-043.
- **Mechanism caveat (§1 / §2 C-1):** the exact Delta-id condition under which a `--full-refresh` strands a
  table mart is not pinned down; the design avoids depending on it (D never overwrites; T uses the plain-build
  path the daily pipeline already runs strand-free; B deletes-then-recreates; the tripwire blocks
  `--full-refresh`).
- **fct_tracking_frames key inherited upstream (B1)** — covered by the whole-match DELETE + the e2e spike.
