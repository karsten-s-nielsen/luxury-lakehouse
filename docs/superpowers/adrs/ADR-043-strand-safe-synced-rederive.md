# ADR-043: Strand-safe re-derive for TRIGGERED synced tables

| Field | Value |
|---|---|
| **Date** | 2026-06-09 |
| **Status** | Accepted |
| **Deciders** | Karsten Nielsen |

## Context

A `dbt --full-refresh` of a gold mart whose Lakebase synced table is `scheduling_policy = TRIGGERED`
(CDF-streamed) overwrites the source Delta table and strands the synced table's streaming checkpoint
(`SYNCED_TABLE_ONLINE_PIPELINE_FAILED` / `DIFFERENT_DELTA_TABLE_READ_BY_STREAMING_SOURCE`). `SNAPSHOT`
synced tables are immune (full re-copy, no streaming checkpoint). ADR-041 *recovers* a strand, but its
dispatch leg is unwired (recovery waits for the daily 07:00 cron, up to ~24 h), and an operator should not
be *able* to strand a TRIGGERED table with a routine command.

**Live evidence (2026-06-09).** The strand ledger (`soccer_analytics.observability.synced_table_strand_state`)
shows the only `stranded` events ever recorded are `fct_action_values_synced`, `fct_passes_synced`,
`fct_pausa_values_synced` — all on 2026-06-08, from a single operator `dbt build … --full-refresh` (the v8
VAEP re-score). `DESCRIBE HISTORY` shows `fct_pausa_values` (a `table` mart) is rebuilt via
`CREATE OR REPLACE TABLE` repeatedly (06-04/05/06/08/09) yet only the 06-08 `--full-refresh` correlates with
a strand. **Conclusion:** the strand vector in practice is `--full-refresh` (and the job-level
`dbt_full_refresh=true` parameter), not routine builds — routine incremental MERGE and `table`
CREATE OR REPLACE alike do not strand.

There are 13 TRIGGERED marts. SNAPSHOT-vs-TRIGGERED and `table`-vs-`incremental` are deliberate size-based
decisions; this work changes **no** scheduling policy and **no** materialization — only the operator
re-derive *path*, plus a guardrail.

## Decision

Re-derive TRIGGERED marts through one tool (`scripts/rederive_synced_marts.py`) that dispatches each mart by
a pure planner into one of three strand-safe actions — **D** (per-match MERGE-reprocess for the 7 incremental
+ `match_id`-filtered marts), **T** (plain `dbt build` / atomic create-or-replace for the 2 `table` marts,
zero downtime), **B** (delete synced → `--full-refresh` → recreate for the 4 merge-all incremental marts) —
and add an `on-run-start` tripwire that aborts any `--full-refresh` selecting a TRIGGERED mart unless the tool
passes `allow_triggered_full_refresh`.

## Alternatives considered

| Option | Pros | Cons | Why rejected |
|---|---|---|---|
| A. Wire ADR-041 auto-heal dispatch (`GH_DISPATCH_TOKEN`) | reuses existing heal | reactive — strand still happens, ~minutes-to-hours RED window | doesn't prevent the strand; the tripwire makes it unnecessary |
| B. Convert TRIGGERED marts to SNAPSHOT, or to incremental-merge-on-`unique_key` | strand-immune | CDF+TRIGGERED overhead was a deliberate size-based choice; SNAPSHOT loses partial updates | out of scope; explicitly not changing scheduling/materialization |
| C. rev-3 materialization-split tripwire (abort *any* `table` build) | catches the table-overwrite case | the 2 table marts are daily `output_mart`s — it would abort the production stage-3 build every run | falsified by live evidence (routine table builds don't strand) |
| D. Route table marts through B (delete→recreate) | strand-safe by construction | needless synced-table downtime for an operation a plain build does safely | superseded by T (plain build is the daily, strand-free path) |
| E (chosen). Three-action D/T/B + unified `--full-refresh` tripwire | each action strand-safe; daily build untouched; zero downtime for D/T | a tag-selector `--full-refresh` bypasses the static scan (caught by the runtime tripwire) | — |

## Consequences

### Positive

- An operator re-derive of any TRIGGERED mart cannot strand its synced table: D never overwrites (MERGE), T
  uses the daily strand-free plain-build path, B deletes the synced table before the overwrite.
- The `on-run-start` tripwire blocks the proven footgun (`--full-refresh` of a TRIGGERED mart) locally *and*
  in automation — including the mega-job `dbt_full_refresh=true` parameter (see Negative / P2).
- Registries are self-policing: an exhaustive D/T/B partition test + a `dbt_project.yml`↔`SYNCED_TABLES`
  parity test + a CDF-coverage test fail CI if a future mart is added to one place but not the other.
- D and T have zero synced-table downtime; only B (4 merge-all marts) re-snapshots.

### Negative

- **P2 — `dbt_full_refresh=true` on the mega-job (`terraform/modules/workflows/main.tf:67`) now aborts
  stages 2 & 3 at `on-run-start`** (they select TRIGGERED marts: `fct_action_values` in stage 2; the rest in
  stage 3). This is intentional (a job-level full-refresh of a TRIGGERED mart is exactly the 2026-06-08
  vector). The parameter is **kept** (Chesterton's fence — it still legitimately full-refreshes SNAPSHOT-only
  stage 1); the compiler-error message routes the operator to `scripts/rederive_synced_marts.py`.
- B's `--full-refresh` of `fct_action_context` / `fct_player_embeddings` (multi-million rows) needs a real
  maintenance window for the synced re-snapshot (the tool prints the estimate; `--dry-run` shows it).
- Macro *rendering* is not CI-gated (`dbt compile` needs a live adapter connection): guards are the offline
  string-presence test + a live `dbt compile` grep + the e2e D/T proofs.

### Neutral

- `reprocess_predicate` is kept despite being redundant after the pre_hook DELETE (the existing `not in (this)`
  filter already re-includes a reprocessed match once its rows are deleted) — it is a **data-loss safety net**:
  if the DELETE is not yet visible to the SELECT (commit ordering), the OR-include still re-admits the match.
  A future maintainer must not "simplify" it away.
- The exact Delta-id condition under which a `table` `--full-refresh` strands while a plain build does not is
  inferred (drop+recreate vs atomic replace), not A/B-tested in isolation. The design is safe under either
  reading because T uses only the plain-build path the daily pipeline already runs strand-free.
- **Dated assumption (2026-06-09): `fct_space_creation` is 0 rows in production** (no node-level `enabled=`;
  the body gate `space_creation_enabled` defaults false, with an `{% else %}` 0-row typed fallback). The T
  re-derive reproduces that by passing no enable var. **If production ever enables `space_creation` (e.g. a
  per-run job var), the re-derive tool must be updated to inject `space_creation_enabled=true`** — otherwise T
  would shrink it back to 0 rows.

## CLAUDE.md Amendment

No exception to a project-wide rule. A pointer is added under "Database Performance → Lakebase" directing
operators to `scripts/rederive_synced_marts.py` and noting that `--full-refresh` of a TRIGGERED mart (and
`dbt_full_refresh=true` on stages 2/3) is now blocked by the tripwire.

## Related

- **Specs:** `docs/superpowers/specs/2026-06-09-strand-safe-synced-rederive-design.md` (rev 5)
- **ADRs:** complements `ADR-041` (synced-table checkpoint self-heal); relates to `ADR-038` (concurrent-commit
  retry) and `ADR-019` (dbt stage selectors)
- **Enforcement:** `src/tests/test_strand_safe_rederive.py`, `src/tests/test_rederive_planner.py`,
  `src/tests/test_synced_table_heal_e2e.py` (D/T mechanism proofs)

## Notes

The strand ledger + `DESCRIBE HISTORY` evidence above was gathered live during planning; the table-mart
plain-vs-`--full-refresh` distinction was observational (daily builds vs the 06-08 incident), not a controlled
experiment — hence the T action is chosen to be safe under either mechanism.
