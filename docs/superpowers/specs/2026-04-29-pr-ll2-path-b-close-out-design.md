# PR-LL2 Path B Close-Out — Cross-Table Format-Contract Foundation

**Date:** 2026-04-29
**Author:** Karsten (with Claude Opus 4.7)
**Status:** Design locked, awaiting writing-plans
**Branch (planned):** `fix/pr-ll2-path-b-close-out`
**Wheel bump:** 0.3.21 → 0.3.22
**silly-kicks dep:** `>=1.8.0,<2.0` → `>=2.0.0,<3.0`
**New ADR:** ADR-018 (cross-table format-contract testing)
**Predecessor PRs:** #224 (PR-LL2 main), #225–228 (model_validation + scoping fixes)
**Successor:** PR-LL3 (per `docs/superpowers/plans/PR-LL3-scope.md`)

## Executive summary

Close PR-LL2 by fixing six production bugs surfaced at full mart-refresh time **and** establish the cross-table format-contract testing foundation that prevents the same bug class from recurring. The bugs span IDSSE/Metrica integration (4 distinct bugs), a mart-level test scoping mirror, and an IDSSE bronze parser period misclassification — but they share a single systemic root: code is the source of truth for cross-file conventions that no test enforces. This PR fixes the symptoms (bug-by-bug) **and** the disease (a single-source-of-truth identifier module + format-contract tests at the bronze writer ↔ dim staging boundary, runnable in slim CI).

The forcing function: PR-LL2 itself shipped clean through the bronze write path (4-source LL2 + Path B columns populated correctly across 13.6M rows of `bronze.spadl_actions`) but mart `fct_action_values` showed 100% NULL `match_key` for IDSSE (2521 rows) and 100% NULL `team_key` + `competition_key` for Metrica (5835 rows). Five PR-LL waves (LL1 + LL2 + #225 + #226 + #227 + #228) over 36 hours had moved through `slim CI + dbt build (small fixture) + post-deploy validator` without surfacing any of these — they only fired at full-refresh `dbt build` against production-scale data after the Databricks ingestion job rebuilt bronze. ADR-018 names the gap: every existing test asserts properties of a single file in isolation; no test asserts that bronze writer output values agree with dim staging input values.

This PR also adopts silly-kicks 2.0.0 (per upstream's ADR-001 — caller's `team_id` / `player_id` are sacred), surfacing the four new tackle qualifier columns (`tackle_winner_player_id`, `tackle_winner_team_id`, `tackle_loser_player_id`, `tackle_loser_team_id`) end-to-end into `fct_action_values` for tackle analytics.

## The six bugs (all verified against production backend on 2026-04-29)

| # | Symptom | Root cause | Fix layer |
|---|---|---|---|
| 1 | `fct_action_values.match_key` 100% NULL for IDSSE (2521/2521 rows) | `bronze.idsse_events.match_id` adds `'idsse_'` prefix (idsse.py:1131); `dim_matches.native_match_id` uses bare DFL ID (`stg_idsse__matches.sql:57` strips prefix). Bronze writer's prefix never reaches dim. | Strip prefix at bronze writer. |
| 2 | `fct_action_values.team_key` 100% NULL for Metrica (5835/5835 rows) | `bronze.metrica_events.home_team_id_native` emits `'Sample_Game_1-Home'` (capital, hyphen — metrica_events.py:202); `dim_teams.native_team_id` carries `'metrica_Sample_Game_1_home'` (lowercase, prefixed — `stg_metrica__team_players.sql:74`). | Align bronze writer to dim format. |
| 3 | `bronze.spadl_actions.team_id_native` 93.5% NULL for IDSSE (2358/2522 rows) | silly-kicks 1.7.0 sportec.convert_to_actions overwrites caller's `team` column with `tackle_winner_team` (DFL CLU id) on TacklingGame events. Our `_team_label_to_dfl_id` only handles `'home'`/`'away'`. | Bump silly-kicks to 2.0.0 (caller's `team_id` now sacred per their ADR-001). No code change to our mapper required. |
| 4 | `fct_action_values.competition_key` 100% NULL for Metrica (5835/5835 rows) | `dim_matches.sql:71` Metrica CTE hardcodes `cast(null as string) as competition_id` even though `stg_metrica__matches.sql:26` emits `'metrica-sample'` and `dim_competitions` already has the matching row. IDSSE cascade resolves automatically once Bug #1 fixes match_key. | 1-line dbt CTE change. |
| 5 | Five `not_null` mart tests on `player_id`/`team_id`/`vaep_value`/`offensive_value`/`defensive_value` will fail post-bronze-rebuild for IDSSE/Metrica (PR-LL3 scope) | PR #228 added `where: data_source IN ('statsbomb', 'wyscout')` at staging only; mart-level mirror was missed. | Mirror the staging filter on the 5 mart tests. |
| 6 | `assert_fct_action_values_minute_match_absolute` test fails on 21 rows (all IDSSE period 2, negative `time_seconds` ranging −3577 to −486 sec) | IDSSE bronze parser `_parse_events_xml` uses XML-stream `current_period` state machine. DFL XML emits secondary blocks (BallClaiming/RefereeBall) **after** the secondHalf KickOff with first-half event_times; these get tagged period=2 with negative period-relative `timestamp_seconds`. Backend probe confirmed 27–41 misclassified events per match × 7 matches in `bronze.idsse_events` (~210 corrupted bronze rows; 21 surface in mart only because few are action-shaped). | 2-pass parser refactor: kickoff scan first, then per-event period derivation by event_time. |

Bug #3's 759 NULL rows from bronze `team='unknown'` are legitimate (Foul/Caution/FairPlay/etc. without team attribution per DFL XML schema) — post-fix expected NULL ratio for IDSSE drops from 93.5% to ~30%.

## The seven recurring patterns (the disease)

PR-LL1 + PR-LL2 + this close-out have produced bugs falling into seven recurring patterns. ADR-018 catalogs them; this PR closes them all except the deferred ones marked PR-LL3.

| # | Pattern | Bugs in this close-out | Fixed by | Caught in future by |
|---|---|---|---|---|
| P1 | Bronze writer format ↔ dim staging format drift | #1, #2 | Bugs #1+#2 fixes (use `shared.identifiers`) | F1 + F2 + F3 + F6 |
| P2 | State-machine parser assumes data layout | #6 | F4 (2-pass parser) | F4 pattern documented; future parsers follow it |
| P3 | Silent third-party API contract drift | #3 | silly-kicks 2.0.0 bump | F5 (boundary tests at OUR repo) + F6 |
| P4 | dbt CTE forgets to pass through metadata | #4 | Bug #4 fix (1-line CTE) | F2 (12 dbt singular tests) + F3 (ref-integrity probe) |
| P5 | Mart-level test scoping forgotten when staging gets `where:` filter for deferred PR | #5 | Bug #5 fix (mirror filter on 5 mart tests) | F2 covers at scale; PR-LL3 S1 closes the underlying deferral |
| P6 | 4 SPADL UDFs duplicate ~80% of logic | None directly; future drift surface | — | Deferred to PR-LL3 (S5) |
| P7 | Test gate fires only at full-refresh dbt build, not slim CI | All 6 + PR-LL1's latent statsbomb_* zero-rows | F2 (singular tests slim-CI-tagged) + F3 (probe in slim CI) | Same |

## Goals and non-goals

### Goals

1. Fix all six bugs end-to-end (bronze writer → bronze table → mart → synced table)
2. Bump silly-kicks to 2.0.0 + surface the 4 new tackle qualifier columns through bronze.spadl_actions → bronze.vaep_action_values → fct_action_values
3. Establish `src/shared/identifiers.py` as the single source of truth for native ID format generation
4. Establish cross-table format-contract testing as a project pattern (ADR-018) with concrete tests for all 4 sources
5. Establish silly-kicks API boundary testing at OUR repo (catches API drift even when silly-kicks doesn't break us obviously)
6. Establish two-pass parser as the documented pattern for parsers handling time-ordered data with period markers
7. Make slim CI catch the bug class that this close-out cycle had to chase (every test addition runs in slim CI, not just full-refresh)

### Non-goals (deferred to PR-LL3 — see `docs/superpowers/plans/PR-LL3-scope.md`)

- **S5** — DRY the 4 SPADL UDFs via shared post-processing helpers (~80 lines per UDF currently duplicated)
- **S6** — Source-onboarding contract test class (parametrized invariant tests)
- **S7** — Type-safe Pydantic identifier dataclasses (Python-side type safety throughout pipeline)
- **S8** — bronze.idsse_events period coverage extension to ET + penalties (no current matches need it; future-proofing only)
- **S1, S2** (carried over from ADR-016) — per-player Kimball mapping for IDSSE/Metrica
- **S3** (carried over from ADR-016) — silly-kicks sportec `CornerKick` / `OtherBallAction` event coverage gap (~16% of IDSSE bronze events)
- **S4** (carried over from ADR-016) — `fct_action_values.game_state` switch to `team_id_native` comparison

## Architecture

### Single source of truth for native identifiers — `src/shared/identifiers.py`

Pure-Python module, stdlib-only (no Spark/dbt imports). Functions return canonical strings; raise `ValueError` on format mismatch.

```python
# src/shared/identifiers.py
import re
from typing import Literal

_IDSSE_MATCH_ID_PATTERN = re.compile(r'^[A-Z0-9]+$')

def idsse_native_match_id(raw_dfl_match_id: str) -> str:
    """Canonical IDSSE native match id — bare DFL MatchId (e.g. 'J03WMX').

    Source of truth for the format that lands in:
    - bronze.idsse_events.match_id
    - bronze.idsse_tracking.match_id
    - bronze.spadl_actions.match_id_native (for IDSSE rows)
    - dim_matches.native_match_id (for IDSSE rows)
    """
    if not _IDSSE_MATCH_ID_PATTERN.match(raw_dfl_match_id):
        raise ValueError(
            f"invalid IDSSE match id: {raw_dfl_match_id!r} "
            "(expected bare DFL MatchId like 'J03WMX')"
        )
    return raw_dfl_match_id

def metrica_native_team_id(match_id: str, side: Literal['home', 'away']) -> str:
    """Canonical Metrica native team id — 'metrica_<match>_<home|away>'.

    Source of truth for the format that lands in:
    - bronze.metrica_events.home_team_id_native / away_team_id_native / team_id_native
    - bronze.spadl_actions.team_id_native (for Metrica rows)
    - dim_teams.native_team_id (for Metrica rows)
    """
    if side not in ('home', 'away'):
        raise ValueError(f"side must be 'home' or 'away', got {side!r}")
    return f"metrica_{match_id}_{side}"

# Companions (added for parity even where dim already matches the bronze writer):
def idsse_native_competition_id(raw_dfl_competition_id: str) -> str: ...
def metrica_native_competition_id() -> str:  # constant 'metrica-sample'
def metrica_native_season_id() -> str:       # constant 'metrica-open-2017'
```

`bronze.idsse_events`, `bronze.metrica_events`, the SPADL UDFs, and the migration script all call these functions. `dbt staging` references the same canonical strings via `dbt_utils.expect_column_values_to_match_regex` data tests AND the new `assert_*_native_join_resolves.sql` singular tests.

### Cross-table format-contract tests — F2 + F3

**F2 — dbt singular tests (12 of them).** One per `(bronze_table.native_id_col, dim_table.native_id_col)` pair × 4 sources. Tagged `slim_ci` so they run on every PR's CI cycle, not just full refresh. Format:

```sql
-- dbt_project/tests/assert_idsse_match_id_native_join_resolves.sql
SELECT DISTINCT b.match_id_native
FROM {{ ref('stg_spadl__action_values') }} b
LEFT JOIN {{ ref('dim_matches') }} m
    ON b.match_id_native = m.native_match_id
   AND b.data_source = m.provider
WHERE b.data_source = 'idsse'
  AND m.match_key IS NULL
```

Returns rows ⇒ test failure. The 12 tests cover (statsbomb, wyscout, idsse, metrica) × (match, team, competition) — all join paths used by `fct_action_values`.

**F3 — Pre-merge ref-integrity probe (Python).** Rename `scripts/validate_pr_ll2_post_deploy.py` → `scripts/validate_native_id_integrity.py`. Extend with JOIN-coverage assertions on every `(bronze.spadl_actions.<X>_native, dim_<entity>.native_<entity>_id)` pair. Runs as a pre-merge gate via the wf-vaep-light flow (already in slim CI tag set).

### silly-kicks API boundary tests at OUR repo — F5

`src/tests/test_silly_kicks_boundary.py`. Parametrized over (source, fixture). Test pattern (mirrors silly-kicks's own ADR-001 cross-provider parity gate but at OUR boundary, against OUR fixtures):

```python
@pytest.mark.parametrize("source,converter,fixture", [
    ("statsbomb", silly_kicks.spadl.statsbomb, "fixtures/sb_match_7298.parquet"),
    ("wyscout",   silly_kicks.spadl.wyscout,   "fixtures/ws_match_2576335.parquet"),
    ("idsse",     silly_kicks.spadl.sportec,   "fixtures/idsse_J03WMX.parquet"),
    ("metrica",   silly_kicks.spadl.metrica,   "fixtures/metrica_sample_game_1.parquet"),
])
def test_team_id_mirrors_input_team(source, converter, fixture):
    """ADR-018 boundary contract: silly-kicks's output team_id values are a subset
    of input team values. Catches any future override-style behavior at the seam."""
    events = pd.read_parquet(fixture)
    actions, _ = converter.convert_to_actions(events, home_team_id=...)
    assert set(actions["team_id"].dropna().unique()) <= set(events["team"].dropna().unique())
```

Three additional invariants per source: action_id non-null, period_id ∈ {1..5}, time_seconds ≥ 0.

Total: 4 sources × 4 invariants = 16 boundary tests. Fixture parquet files are committed to the repo (small — 1 match each).

### Two-pass IDSSE parser — F4

Replace the state-machine `current_period` in `_parse_events_xml` with a kickoff-scan-first / event-emit-second structure:

```python
def _parse_events_xml(event_path, player_team_map, match_id, logger, metadata=...):
    # PASS 1: scan ONLY KickOff events to build period→time map
    period_kickoff_times: dict[int, datetime] = {}
    for _, elem in ET.iterparse(event_path, events=("end",)):
        if elem.tag == "Event":
            first_child = next(iter(elem), None)
            if first_child is not None and first_child.tag == "KickOff":
                section = first_child.get("GameSection", "")
                period = _SECTION_TO_PERIOD.get(section)
                if period is not None:
                    event_time = _parse_event_time(elem)
                    if event_time is not None:
                        period_kickoff_times.setdefault(period, event_time)
        elem.clear()

    # PASS 2: emit per-event rows with period derived by event_time lookup
    rows: list[dict[str, object]] = []
    for _, elem in ET.iterparse(event_path, events=("end",)):
        if elem.tag != "Event":
            ...
        # ... usual parsing ...
        # Period: largest period whose kickoff_time ≤ event_time
        event_time = _parse_event_time(elem)
        period, period_start = _derive_period_from_kickoffs(event_time, period_kickoff_times)
        timestamp_seconds = (event_time - period_start).total_seconds() if period_start else None
        ...
```

Synthetic XML fixture covering interleaved-block shape (primary EventList + secondary BallClaiming block after secondHalf KickOff) added to `src/tests/fixtures/idsse_interleaved_periods.xml`. Test `test_idsse_period_derivation.py` asserts the secondary-block events get the correct period (per their event_time, not stream order).

### silly-kicks 2.0.0 4-column tackle qualifier passthrough — F7

silly-kicks 2.0.0 sportec converter emits 4 new columns: `tackle_winner_player_id`, `tackle_winner_team_id`, `tackle_loser_player_id`, `tackle_loser_team_id`. NaN on non-tackle rows or when qualifier absent.

This PR threads these through:
- IDSSE SPADL UDF in `spadl_conversion.py` (added to `_spadl_cols` + applyInPandas StructType)
- Other 3 source UDFs add the 4 columns NULL-filled with `Int64`/`string` dtypes (multi-source schema parity)
- `_SPADL_SCHEMA` + `_VAEP_SCHEMA` DDL constants
- VAEP scoring UDF StructType
- `bronze.spadl_actions` + `bronze.vaep_action_values` ALTER TABLE
- `stg_spadl__action_values` projection
- `fct_action_values` final SELECT (with type-correct nullable dtypes)
- `_marts__models.yml` column descriptions
- writer/DDL parity test extensions (`test_spadl_vaep_writer_parity.py`)

The 4 columns become analytics-grade fields on `fct_action_values` for tackle analytics (tackle winner/loser identity).

### ADR-018 — Cross-table format-contract testing

Captures the pattern as a project-level ADR. Decision text:

> **Every cross-file convention that produces a value used as a JOIN key must be enforced by a test that runs at PR-time, not at full-refresh-build time.** Concretely: every `(bronze_table.native_id_col, dim_table.native_id_col)` pair has either (a) a dbt singular test asserting JOIN coverage, or (b) a Python unit test asserting format-string equality between bronze writer and dim staging. Both run in slim CI. The test file or singular test must be added in the same PR as the bronze writer / dim staging code that produces the values.

Alternatives considered: (a) Documentation only, (b) Type-safe identifiers via Pydantic, (c) Static schema parity (already have ADR-002 for that — extends naturally to cross-table joins). Decision is (c)-extended: explicit JOIN-coverage tests are simpler to author per source than full Pydantic refactor and provide the same CI-time enforcement.

CLAUDE.md amendment: one rule pointing to ADR-018 in the Project Conventions section.

## Component boundaries

| Component | Responsibility | Imports |
|---|---|---|
| `src/shared/identifiers.py` | Native ID format generators (single source of truth) | stdlib only (re, typing) |
| `src/ingestion/idsse.py` | DFL XML → bronze.idsse_events / bronze.idsse_tracking. **2-pass parsing.** | `shared.identifiers.idsse_native_match_id`, `shared.identifiers.idsse_native_competition_id` |
| `src/ingestion/metrica_events.py` | Metrica CSV/EPTS → bronze.metrica_events | `shared.identifiers.metrica_native_team_id`, `shared.identifiers.metrica_native_competition_id` |
| `src/ingestion/spadl_conversion.py` | bronze events → bronze.spadl_actions (4 source UDFs) | `silly_kicks 2.0.0`, `shared.identifiers` |
| `dbt_project/models/marts/dim_matches.sql` | Conformed match dim | `stg_*__matches` (passes competition_id through for Metrica) |
| `dbt_project/tests/assert_*_native_join_resolves.sql` | F2 cross-boundary tests | `stg_spadl__action_values` + `dim_*` |
| `src/tests/test_format_contract.py` | F1+F2 Python boundary tests (parametrized over 4 sources) | `shared.identifiers` |
| `src/tests/test_silly_kicks_boundary.py` | F5 silly-kicks API boundary tests | `silly_kicks.spadl.*` + parquet fixtures |
| `scripts/validate_native_id_integrity.py` | F3 pre-merge probe | `databricks-sql-connector` |

## Required tests (TDD shape — failing test before fix per item)

| # | Test | Asserts | Currently | Post-fix |
|---|---|---|---|---|
| T1 | `test_idsse_native_match_id_format` | `idsse_native_match_id('J03WMX') == 'J03WMX'`; raises on `'idsse_J03WMX'` | n/a | ✓ |
| T2 | `test_metrica_native_team_id_format` | `metrica_native_team_id('Sample_Game_1', 'home') == 'metrica_Sample_Game_1_home'` | n/a | ✓ |
| T3 | `test_format_contract::test_idsse_match_id_format_matches_dim` | bronze writer output regex matches dim staging output regex | n/a | ✓ |
| T4 | `test_format_contract::test_metrica_team_id_format_matches_dim` | same for Metrica team | n/a | ✓ |
| T5 | `assert_idsse_match_id_native_join_resolves.sql` | bronze.spadl_actions IDSSE → dim_matches JOIN coverage | RED (2521 unmatched) | GREEN |
| T6 | `assert_metrica_team_id_native_join_resolves.sql` | bronze.spadl_actions Metrica → dim_teams JOIN coverage | RED (5835 unmatched) | GREEN |
| T7 | 10 sibling JOIN-coverage tests (4 src × 3 entity − 2 already covered) | full coverage matrix | RED on Metrica competition; GREEN on rest | GREEN |
| T8 | `test_idsse_period_derivation::test_secondary_block_event_gets_correct_period` | synthetic interleaved-block XML; secondary-block first-half event lands in period 1 | RED | GREEN |
| T9 | `assert_fct_action_values_minute_match_absolute` (existing) | 0 rows of negative period-relative timestamps | RED (21 rows) | GREEN (0 rows) |
| T10 | `test_silly_kicks_boundary::test_team_id_mirrors_input_team` (4 sources) | output `team_id` ⊆ input `team` | GREEN (silly-kicks 2.0.0 enforces) | GREEN |
| T11 | `test_spadl_vaep_writer_parity::test_*_struct_includes_tackle_qualifiers` | 4 new tackle qualifier columns in StructType + `_SPADL_SCHEMA` | RED | GREEN |
| T12 | `test_format_contract::test_dim_matches_metrica_competition_id_passthrough` | dim_matches Metrica row has competition_id = 'metrica-sample' | RED (NULL) | GREEN |
| T13 | `test_idsse_parser::test_kickoff_scan_pass_one` | pass 1 builds `{1: ts, 2: ts}` for 2-period match | n/a | ✓ |
| T14 | `test_format_contract::test_5_mart_not_null_filter_present` | _marts__models.yml has where: filter on the 5 deferred not_null tests | RED | GREEN |

T5–T7 + T9 + T11–T12 + T14 are RED on `main` and GREEN after the fix — the TDD evidence trail.

## Bronze re-ingestion playbook

Phase order — destructive operations marked **DESTRUCTIVE**.

| Phase | Action | Idempotent? | Risk |
|---|---|---|---|
| 0 | Wheel 0.3.22 deployed to UC Volume `/Volumes/soccer_analytics/bronze/libs/` | yes | none |
| 1 | DEEP CLONE backups: `bronze.idsse_events`, `bronze.metrica_events`, `bronze.spadl_actions`, `bronze.vaep_action_values` to `bronze.<table>_pre_pr_ll2_path_b_backup` (24h retention) | yes | none |
| 2 | ALTER `bronze.spadl_actions` + `bronze.vaep_action_values` to add the 4 silly-kicks 2.0.0 tackle qualifier columns (`tackle_winner_player_id BIGINT`, `tackle_winner_team_id STRING`, `tackle_loser_player_id BIGINT`, `tackle_loser_team_id STRING`) — extension to `scripts/migrate_bronze_for_pr_ll2.py` | yes | none |
| 3 | **DESTRUCTIVE**: DELETE FROM `bronze.idsse_events` (small — ~10K rows) and `bronze.metrica_events` (small — ~14K rows) | no | low (DEEP CLONE backup exists) |
| 4 | **DESTRUCTIVE**: DELETE FROM `bronze.spadl_actions` WHERE `data_source IN ('idsse', 'metrica')` (~8.5K rows) and equivalent on `bronze.vaep_action_values` | no | low (DEEP CLONE backup exists) |
| 5 | Re-run `wf-idsse` (with 2-pass parser) | yes | low |
| 6 | Re-run `wf-metrica-events` + `wf-metrica-tracking` | yes | low |
| 7 | Re-run `wf-vaep-light` (with silly-kicks 2.0.0 + new tackle qualifier passthrough) — converts only IDSSE/Metrica matches given existing-match skip | yes | low |
| 8 | Run `python scripts/validate_native_id_integrity.py` (extended) | yes | none |
| 9 | Local `uv run --extra dbt python scripts/dbt_build_and_refresh.py` | yes | full mart rebuild required |
| 10 | Verify ERROR=0 (target: PASS≥796 / WARN=21 / ERROR=0 / SKIP=68) | yes | none |
| 11 | Drop DEEP CLONE backups after 24h post-deploy | yes | none |

Statsbomb + Wyscout rows in `bronze.spadl_actions` and `bronze.vaep_action_values` are NOT touched — the 4 new tackle qualifier columns NULL-fill on those rows via Spark `mergeSchema` on next StatsBomb/Wyscout incremental write (no rewrite required). The mart projection emits NULL for them which is correct (only sportec's TacklingGame events carry the qualifiers).

## Risks and mitigations

| Risk | Mitigation |
|---|---|
| 2-pass IDSSE parser regression on existing match coverage | Fixture-based unit test on synthetic XML; integration test re-converts all 7 IDSSE matches and asserts no regressions in row count or period distribution vs pre-fix |
| silly-kicks 2.0.0 surprise behavior change beyond the documented breaking contract | F5 (silly-kicks boundary tests, 16 invariants × 4 sources) catches at PR-CI time |
| dim_matches Metrica competition_id passthrough breaks downstream mart that assumed NULL | Grep audit + deliberate review of every consumer of `dim_matches.competition_id` for Metrica; downstream consumers should benefit not break |
| Bronze re-ingestion fails partway → mixed-format spadl_actions | DEEP CLONE backup + 24h retention; restore via `RESTORE TABLE` if needed |
| New 12 dbt singular tests too slow on full mart | Tag them `slim_ci`; sample mart slice via `where data_source = 'X'` so each test scans bounded rows |
| F1/F3 functions raise on legitimate edge case (e.g., DFL adds new MatchId format) | Functions raise `ValueError` with clear message; bronze writer wraps with try/except and logs ERROR — not silent NULL |

## Hard rules from project (per CLAUDE.md)

- Single commit per PR. Squash merge.
- Every commit / push / PR-create / merge requires separate explicit chat approval.
- Hook `~/.claude/hooks/git_commit_guard.py` blocks `git commit` until user touches sentinel.
- ADR rule: ADR-018 must be drafted as part of this PR.
- AI Governance + Architecture Appendix D: not affected by this PR (no new ML system; no new academic references).
- Performance budgets: no benchmarked function modified; `mad-scientist-skills:measure-before-optimize` skill not required.
- Hyrum's Law: 4 new mart columns (`tackle_winner_*` / `tackle_loser_*`) — additive only; no consumer break.

## Approval gates (chronological)

1. **User reviews this spec** — gate before writing-plans.
2. After writing-plans produces `docs/superpowers/plans/2026-04-29-pr-ll2-path-b-close-out.md` — user reviews plan.
3. **No commits during implementation** without explicit approval per phase.
4. Branch creation, push, PR-create, merge — each is a separate approval gate.
5. Bronze re-ingestion (Phase 3 onward) — requires explicit approval given destructive nature.

## Related

- **ADRs:** ADR-016 (SPADL canonical/native naming — the immediate predecessor, this PR is its close-out), ADR-002 (writer/DDL parity — this PR extends the discipline cross-table), ADR-017 (model validation as signal not gate — partial cause of this close-out's scope), **ADR-018 (cross-table format-contract testing — drafted as part of this PR)**.
- **Specs:** `docs/superpowers/specs/2026-04-29-pr-ll2-spadl-enrichment-stage-design.md` (PR-LL2 main).
- **PRs predecessor:** #224, #225, #226, #227, #228.
- **PR-LL3 scope tracker:** `docs/superpowers/plans/PR-LL3-scope.md`.
- **External references:** silly-kicks 2.0.0 release + ADR-001 (caller's identifier conventions are sacred); silly-kicks CHANGELOG `[2.0.0]` 2026-04-29.

## Notes

### Empirical findings recorded for future reference

| Probe | Finding |
|---|---|
| `bronze.spadl_actions` IDSSE row count | 2522 |
| `bronze.spadl_actions` Metrica row count | 5835 |
| `bronze.spadl_actions` IDSSE NULL `team_id_native` (pre-fix) | 2358 (93.5%) |
| Bronze events with `team='unknown'` (legitimate NULLs post-fix) | 1478 (14.1% of 10498 IDSSE bronze events) |
| TacklingGame events corrupted by silly-kicks 1.7.0 override | 1412 (712 away + 700 home) — exact match to bronze TacklingGame distribution |
| Bronze idsse_events with negative period-2 `timestamp_seconds` | 27–41 per match × 7 matches = ~210 rows (only 21 surface in mart because most are non-action-shaped events) |
| dim_matches.competition_id NULL for Metrica | 3/3 (100%) — root cause: dim_matches.sql:71 hardcodes NULL |
| dim_competitions Metrica row | EXISTS — 'metrica-sample' / 'Metrica Sample Dataset' |

### Wheel version policy

0.3.21 (PR-LL2 main) → 0.3.22 (this PR). silly-kicks 2.0.0 is a major bump but luxury-lakehouse continues to expose its own 0.x series (no consumer outside the repo); SemVer caller is luxury-lakehouse internal only (Databricks workflows, Taipy app via wheel install). Bumping to 0.4.0 considered but rejected: the 4 new mart columns are additive and the bug fixes don't change any existing column's semantics. 0.3.22 is appropriate.
