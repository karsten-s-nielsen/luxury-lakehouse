# LL3-CO — Kimball-Derived Data-Completeness Close-Out

> **Design spec** for the 8 deferred items (S1–S8) from PR-LL1 + PR-LL2 +
> PR-LL2 Path B close-out. Scope tracker:
> `docs/superpowers/plans/PR-LL3-scope.md`.

**Last updated**: 2026-05-10.

---

## 1. Goal

Ship 8 deferred SPADL data-completeness items in a single PR on branch
`feat/ll3-co`. These items make the SPADL pipeline correct and maintainable
across all 4 providers (StatsBomb, Wyscout, IDSSE, Metrica), unblock
full-coverage mart-level `not_null` tests, and lay the groundwork for TC-1
(`fct_tracking_context`).

**What this does NOT do:** wire into VAEP training, update Taipy pages,
create `fct_tracking_context` (TC-1), touch OPT-2/3/4 target files, or
drop legacy columns (K8 scope, locked to 2026-07-22).

---

## 2. Sequencing Note

The TODO previously stated "LL3-CO unblocks once OPT-1..4 ship." This
constraint is relaxed: LL3-CO's S1–S8 are schema/test/UDF work that does
not touch the files OPT-2/3/4 targets (`defcon_lite.py`, `spadl_vaep.py`,
`xg_model.py`, analytics modules). The two workstreams are independent.

---

## 3. Items

### S2 — `player_id_native` on `bronze.spadl_actions` (ALL 4 sources)

**Companion to S1 — must ship first.**

#### Current state

**No source currently populates `player_id_native`.** All 4 SPADL UDFs
(StatsBomb `_spadl_cols` line 110, Wyscout line 464, IDSSE line 843, Metrica
line 1208) omit it. The only `*_player_id_native` columns in the schema are
`tackle_winner_player_id_native` and `tackle_loser_player_id_native` (tackle
qualifier enrichments, IDSSE-only).

Each source's silly-kicks converter emits the raw source player identifier
on the output `player_id` column before the lakehouse assigns surrogate
keys or NULL-fills legacy BIGINTs.

#### Design

**All 4 UDFs** need `player_id_native` population (same pattern as
`team_id_native` at StatsBomb line 247, Wyscout line ~600, IDSSE line 959,
Metrica line 1296):

**StatsBomb + Wyscout** (numeric native IDs → stringify):
```python
actions["player_id_native"] = actions["player_id"].astype("Int64").astype("string")
```

**IDSSE** (DFL OBJ IDs, already string-shaped):
```python
actions["player_id_native"] = actions["player_id"].astype("string")
```

**Metrica** (anonymous `Player<N>` labels):
```python
actions["player_id_native"] = actions["player_id"].astype("string")
```

Each line must appear BEFORE the legacy BIGINT NULL-fill
(`actions["player_id"] = pd.array([NA]*n, ...)`).

In all 4 UDFs:

1. **Add the `player_id_native` assignment** as shown above.

2. **Add `"player_id_native"` to `_spadl_cols`** (after `"team_id_native"`).

3. **Add `player_id_native` to ALL 4 `_SPADL_SCHEMA` StructTypes** in the
   applyInPandas schema definitions:
   ```python
   StructField("player_id_native", StringType()),
   ```

4. **Bronze migration** (`scripts/migrations/`): `ALTER TABLE
   bronze.spadl_actions ADD COLUMNS (player_id_native STRING)` — idempotent,
   auto-applied by live-CI runner.

5. **Update `fct_action_values.sql` player JOIN** (line 221): change
   `dp.native_player_id = cast(av.player_id as string)` to
   `dp.native_player_id = av.player_id_native`. The existing comment at
   line 217 ("player_id_native is NOT yet on spadl_actions (PR-LL3)")
   documents this deferred change.

6. **Re-ingest** all 4 sources' SPADL after deploy to populate the new
   column on existing rows (StatsBomb + Wyscout re-ingest is fast since
   they only add one column; IDSSE + Metrica also pick up S3 recovered
   events).

#### ADR-018 compliance

Per ADR-018, `player_id_native` as a new JOIN-key native ID requires:

- **(a) Canonical generators** in `src/shared/identifiers.py`:
  `statsbomb_native_player_id(raw)`, `wyscout_native_player_id(raw)`,
  `idsse_native_player_id(raw)`, `metrica_native_player_id(raw)` — each
  with compiled-regex format validation matching the source's conventions.

- **(b) Format-contract test parametrization** in
  `src/tests/test_silly_kicks_boundary.py` (or equivalent): verify
  silly-kicks output `player_id` column matches the expected format per
  source.

- **(b′) Writer parity test extension** in
  `src/tests/test_spadl_vaep_writer_parity.py`: extend
  `test_spadl_ddl_includes_native_id_columns` (line ~348) expected-column
  set to include `player_id_native`. Guards against accidental removal in
  future refactors.

- **(c) 4 dbt singular JOIN-coverage tests**:
  `assert_statsbomb_player_id_native_join_resolves.sql`,
  `assert_wyscout_player_id_native_join_resolves.sql`,
  `assert_idsse_player_id_native_join_resolves.sql`,
  `assert_metrica_player_id_native_join_resolves.sql` — same pattern as
  the existing 12 tests (4 sources × 3 entities).

#### Verification

- `player_id_native IS NOT NULL` on all rows in `bronze.spadl_actions`
  post-re-ingest (all 4 sources).
- StatsBomb/Wyscout values are stringified integers (e.g. `"3009"`, `"25413"`).
- IDSSE values match DFL OBJ player IDs (e.g. `DFL-OBJ-002G1Q`).
- Metrica values match anonymous player IDs (e.g. `Player11`).
- All 4 dbt singular JOIN-coverage tests pass.
- `fct_action_values` player JOIN resolves for IDSSE/Metrica (non-NULL
  `player_key` where `dim_players` has matching entries).

---

### S1 — Per-player Kimball mapping completeness

**Depends on S2.**

#### Current state

`_marts__models.yml` has 4 `not_null` tests with `where: "data_source IN
('statsbomb', 'wyscout')"` guards on `player_id` (line 680), `team_id`
(line 690), `team_key` (line 697), and `player_key` (line 710) in the
`fct_action_values` contract.

#### Design

1. **Remove the `where:` filter** from all 4 `not_null` tests so they
   cover all sources.

2. **Verify the `dim_players` JOIN resolves for IDSSE/Metrica.** The
   `fct_action_values` mart currently JOINs `dim_players` via `player_key`.
   For IDSSE/Metrica, `player_key` is derived from `player_id_native` via
   `hash_native_id_to_bigint()` in the staging layer. Confirm that
   `dim_players` contains entries for all IDSSE/Metrica players — if not,
   the `not_null` test on `player_key` will fail, surfacing the gap.

3. **If `dim_players` coverage is incomplete for IDSSE/Metrica** (likely —
   Metrica has anonymous IDs not in dim_players), document the gap and
   tighten the filter to `where: "data_source IN ('statsbomb', 'wyscout',
   'idsse')"` as a stepping stone, with a code comment pointing to TC-1
   for full resolution.

#### Exit criteria

- All 4 `not_null` tests pass without the `data_source` scope filter, OR
- The filter is tightened to include IDSSE with a documented Metrica
  exclusion reason.

---

### S3 — Sportec event coverage gap (RESOLVED in silly-kicks 3.10.1)

#### Root cause (investigated 2026-05-10)

DFL XML uses `CornerKick` as the first-child tag. silly-kicks
`_MAPPED_EVENT_TYPES` contained `"Corner"` (not `"CornerKick"`). The
whitelist dispatch at `sportec.py:739` dropped the row. Same for
`OtherBallAction` — not in either mapped or excluded sets.

#### Fix in silly-kicks 3.10.1 (PR-S35, commit `f0706a4`)

- `_EVENT_TYPE_ALIASES = {"CornerKick": "Corner"}` normalized before
  dispatch.
- `OtherBallAction` added to `_MAPPED_EVENT_TYPES`:
  `DefensiveClearance=true` → `clearance`; else → `non_action`.
- 8 new tests in `TestSportecCornerKickAlias` + `TestSportecOtherBallAction`.

#### Lakehouse-side changes

1. **Bump silly-kicks pin** in `pyproject.toml` from `>=3.7.0,<4` to
   `>=3.10.1,<4`. **Pre-bump check:** review the silly-kicks CHANGELOG
   for 3.8.0–3.10.1 to confirm no breaking changes to StatsBomb/Wyscout
   converter output shape. If output shape changed for any source, those
   sources also need re-ingest (already planned in deploy step 5).

2. **Log `ConversionReport.unrecognized_counts`** instead of discarding
   `_report`. In all 4 SPADL UDFs (`_make_statsbomb_spadl_udf`,
   `_make_wyscout_spadl_udf`, `_make_idsse_spadl_udf`,
   `_make_metrica_spadl_udf`), after the `convert_to_actions()` call:
   ```python
   if _report.unrecognized_counts:
       logger.warning(
           "SPADL conversion unrecognized event types for match %s: %s",
           match_id_str,
           _report.unrecognized_counts,
       )
   ```
   This requires passing a `logger` into each UDF closure (or using a
   module-level logger; prefer the latter since UDF closures run on
   executors where module-level loggers are safe).

3. **Re-ingest IDSSE SPADL** to pick up the previously-dropped CornerKick +
   OtherBallAction events.

#### Verification

- Post-re-ingest, `bronze.spadl_actions` row count for IDSSE matches
  increases (CornerKick events now produce `corner_short`/`corner_crossed`
  actions; OtherBallAction with `DefensiveClearance=true` produce
  `clearance` actions).
- No `unrecognized_counts` warnings in re-ingest logs for IDSSE matches.

---

### S4 — IDSSE/Metrica `game_state` derivation via `team_id_native`

#### Current state

`int_running_score.sql` (ephemeral model) has **two gaps** for
IDSSE/Metrica:

1. **No match-team CTEs** — `match_teams` UNION only covers StatsBomb
   (lines 33–48) and Wyscout (lines 60–71). IDSSE/Metrica have zero rows.

2. **No goal events** — the `goals` CTE (lines 95–110) reads from
   `int_unified_shots`, which only covers StatsBomb + Wyscout (confirmed:
   only `statsbomb_shots` and `wyscout_shots` CTEs, lines 13/44). Even
   after adding IDSSE/Metrica to `match_teams`, the `goals` CTE would
   produce ZERO rows for those providers → every IDSSE/Metrica action
   would get `COALESCE(0, 0) = COALESCE(0, 0)` → `'drawing'`. This is
   incorrect — IDSSE has 7 Bundesliga matches with real goals.

`fct_action_values.sql` (line 290–308) derives `game_state` via
`team_id = _rs_home_team_id` comparison. For IDSSE/Metrica, `team_id` is
NULL → comparison is NULL → `game_state` falls into the `'losing'` default
on non-tied matches. A comment at line 294 documents this.

#### Design

**Two changes to `int_running_score.sql`:**

**(A) Add match-team CTEs for IDSSE + Metrica:**

```sql
idsse_matches as (
    select
        native_match_id   as match_id,
        'idsse'           as provider,
        home_team_id      as home_team_id_native,
        away_team_id      as away_team_id_native
    from {{ ref('stg_idsse__matches') }}
),

metrica_matches as (
    select
        native_match_id   as match_id,
        'metrica'         as provider,
        home_team_name    as home_team_id_native,
        away_team_name    as away_team_id_native
    from {{ ref('stg_metrica__matches') }}
),
```

(`stg_idsse__matches.sql:30–31` emits DFL CLU native IDs as `home_team_id`
/ `away_team_id`. `stg_metrica__matches` has `home_team_name` /
`away_team_name` — these ARE the native team IDs per
`metrica_native_team_id()` in `identifiers.py`.)

**(B) Add SPADL-derived goals for IDSSE + Metrica:**

`int_unified_shots` is NOT extended (it is a shots-specific intermediate
model scoped to StatsBomb + Wyscout). Instead, add a `spadl_goals` CTE
that queries `stg_spadl__action_values` directly:

```sql
spadl_goals as (
    -- IDSSE + Metrica goals extracted from SPADL actions.
    -- int_unified_shots only covers StatsBomb + Wyscout; this CTE
    -- fills the gap for sources that lack dedicated shot staging models.
    select
        dm.match_key,
        av.team_id_native   as scoring_team_id_native,
        av.period_id        as period,
        av.minute,
        cast(floor(av.time_seconds % 60) as int) as second
    from {{ ref('stg_spadl__action_values') }} av
    inner join {{ ref('dim_matches') }} dm
        on dm.provider = av.data_source
       and dm.native_match_id = av.match_id_native
    where av.action_type = 'shot'
      and av.action_result = 'success'
      and av.data_source in ('idsse', 'metrica')
),
```

**UNION `match_teams` with explicit types:**

The existing UNION uses INTEGER `match_id`, `home_team_id`,
`away_team_id`. IDSSE/Metrica use STRING native IDs. To reconcile:

```sql
match_teams as (
    select
        cast(match_id as string) as match_id,
        provider,
        cast(home_team_id as string) as home_team_id_native,
        cast(away_team_id as string) as away_team_id_native
    from sb_matches
    union all
    select
        cast(match_id as string) as match_id,
        provider,
        cast(home_team_id as string) as home_team_id_native,
        cast(away_team_id as string) as away_team_id_native
    from ws_matches
    union all
    select match_id, provider, home_team_id_native, away_team_id_native
    from idsse_matches
    union all
    select match_id, provider, home_team_id_native, away_team_id_native
    from metrica_matches
),
```

All columns are STRING post-UNION. The `match_teams_keyed` CTE already
casts `mt.match_id` to STRING for the `dim_matches` JOIN (line 91), so
this is a no-op there. Legacy INTEGER `home_team_id` / `away_team_id`
columns are dropped from the UNION output — the game_state derivation
switches entirely to `_native` STRING columns.

**UNION goals with scoring_team_id_native:**

```sql
all_goals as (
    -- SB + WS goals from int_unified_shots
    select
        g.match_key,
        cast(g.scoring_team_id as string) as scoring_team_id_native,
        g.period,
        g.minute,
        g.second
    from goals g
    union all
    -- IDSSE + Metrica goals from SPADL actions
    select match_key, scoring_team_id_native, period, minute, second
    from spadl_goals
),
```

**`goals_with_scores`** — switch scoring comparison to native STRING:
```sql
sum(case when g.scoring_team_id_native = mt.home_team_id_native
         then 1 else 0 end) over (...) as home_score_after,
sum(case when g.scoring_team_id_native = mt.away_team_id_native
         then 1 else 0 end) over (...) as away_score_after
```

**Output schema** — `int_running_score` output adds
`home_team_id_native STRING` and `away_team_id_native STRING`. The legacy
INTEGER `home_team_id` / `away_team_id` columns are removed (only
consumed by the game_state derivation, which switches to `_native`).

**Downstream mart changes:**

In `fct_action_values.sql`, change the game_state derivation to:
```sql
case
    when coalesce(home_score_after, 0) = coalesce(away_score_after, 0)
        then 'drawing'
    when (team_id_native = _rs_home_team_id_native
              and home_score_after > away_score_after)
         or (team_id_native != _rs_home_team_id_native
              and away_score_after > home_score_after)
        then 'winning'
    else 'losing'
end as game_state,
```

**Also update `fct_shots.sql` (line 172–180) and `fct_passes.sql`
(line 208–216)** — both derive `game_state` using the same
`team_id = _rs_home_team_id` pattern. Switch to `team_id_native` in both.
Remove the comment at `fct_passes.sql:34` ("game_state defaults to
'drawing' for IDSSE/Metrica") — no longer true after this fix.

(Note: `fct_shots` currently only covers StatsBomb + Wyscout per its header
comment. The `team_id_native` switch is future-proofing — SB/WS existing
rows get `CAST(team_id AS STRING)` as `team_id_native`, which stringifies
correctly. `fct_passes` has IDSSE/Metrica rows today, so the fix is
immediately correctness-critical there.)

#### Verification

- Pick a specific IDSSE match known to have goals (e.g., from existing
  bronze fixture data). Verify `game_state` transitions: `'drawing'` →
  `'winning'`/`'losing'` at the minute of the first goal.
- `fct_action_values` rows with `data_source = 'idsse'` have all 3
  `game_state` values represented (drawing + winning + losing) across
  matches with goals.
- Same for `data_source = 'metrica'`.
- StatsBomb/Wyscout `game_state` values unchanged (regression check).

---

### S5 — DRY the SPADL UDFs

#### Current state

4 SPADL UDFs (StatsBomb, Wyscout, IDSSE, Metrica) in
`spadl_conversion.py` duplicate ~80 lines each of post-conversion logic:
- NULL-fill `statsbomb_*` columns (4 columns)
- Enrichment dtype casts (7 columns)
- Match-level constant population (`home_team_id_native`,
  `competition_native_id`, `season_native_id`, `match_id_native`)
- Tackle qualifier NULL-fill (8 columns on non-IDSSE paths)

#### Design

Create `src/ingestion/spadl_udf_shared.py` with 4 helpers:

```python
def apply_native_columns(
    actions: pd.DataFrame,
    *,
    source: str,
    match_id_native: str,
    home_team_id_native: str,
    competition_native_id: str,
    season_native_id: str,
) -> pd.DataFrame:
    """Populate _native identifier columns.

    For all sources: match_id_native, home_team_id_native,
    competition_native_id, season_native_id, player_id_native.

    player_id_native is derived from actions["player_id"] using
    Int64→string cast (numeric sources) or direct string cast (string
    sources). Must be called BEFORE the legacy BIGINT NULL-fill.
    """
```

```python
def apply_enrichments_and_cast(
    actions: pd.DataFrame,
    *,
    source: str,
) -> pd.DataFrame:
    """Apply post-conversion enrichments + dtype casts.

    Calls apply_spadl_enrichments(actions, source=source), casts
    enrichment columns to their target dtypes, and stringifies
    original_event_id.
    """
```

```python
def null_fill_statsbomb_columns(
    actions: pd.DataFrame,
    *,
    n: int,
) -> pd.DataFrame:
    """NULL-fill the 4 statsbomb_* namespace columns for non-SB sources."""
```

**TC-1 forward compatibility:** The 4 helpers are designed as composable
steps that TC-1's tracking-context UDF can call in sequence. If TC-1 needs
a different player ID extraction pattern, it can extend the shared module
at that time.

**Tackle qualifier handling:** Extracted into a 4th helper:
```python
def null_fill_or_resolve_tackle_qualifiers(
    actions: pd.DataFrame,
    *,
    source: str,
    n: int,
    hash_fn: Callable[[str], int] | None = None,
) -> pd.DataFrame:
    """For IDSSE: resolve tackle qualifier native IDs + surrogate keys.
    For all others: NULL-fill the 8 tackle qualifier columns."""
```

Per-source UDFs shrink from ~120 lines to ~30 lines each: adapter call,
converter call, report logging, and 3–4 shared helper calls.

**Source-branching in `apply_native_columns`:** The `player_id_native`
derivation differs by source type. StatsBomb/Wyscout `player_id` is
float64-with-NaN (pandas nullable integer convention) — the intermediate
`.astype("Int64")` is required to avoid `"3009.0"` stringification.
IDSSE/Metrica `player_id` is already string-shaped (DFL OBJ IDs / Player
labels) — direct `.astype("string")` suffices. The branch is
`if source in ('statsbomb', 'wyscout'):` → `Int64→string`, else →
`string`. Same reason `team_id_native` at line 247 uses the
`Int64→string` path.

#### Verification

- All 4 UDFs produce identical output (bit-for-bit on a sample match)
  before and after the refactor — verified via a golden-file test.
  **Fixture strategy:** reuse existing silly-kicks boundary test fixtures
  under `src/tests/fixtures/silly_kicks_boundary/` (one match per source).
  Golden files are generated once (pre-refactor), then compared
  post-refactor. Exclude `_ingested_at` and other audit columns from
  comparison. Column ordering is checked (the `_spadl_cols` list defines
  output order).
- `uv run pytest src/tests/ -v` passes.
- `uv run pyright src/` passes.

---

### S6 — Source-onboarding contract test class

#### Design

New file: `src/tests/test_source_onboarding_contracts.py`.

```python
@pytest.mark.parametrize("source", ["statsbomb", "wyscout", "idsse", "metrica"])
class TestSourceOnboardingContracts:
    """Per-source invariants that must hold for any SPADL source."""

    def test_native_id_format(self, source):
        """native_match_id, team_id_native, player_id_native follow the
        format contract in src/shared/identifiers.py."""

    def test_dim_join_coverage(self, source):
        """player_key and team_key JOIN to dim_players / dim_teams
        without NULL residuals (on populated rows)."""

    def test_enrichment_columns_populated(self, source):
        """Post-conversion enrichment columns (possession_id_heuristic,
        gk_role, etc.) are non-NULL on applicable rows."""

    def test_game_state_populated(self, source):
        """game_state is non-NULL for all rows (post-S4)."""

    def test_spadl_schema_parity(self, source):
        """Output column set matches the canonical _SPADL_SCHEMA StructType."""
```

**Gradient Sports:** Not included — add when Gradient Sports ingestion ships. A TODO comment in
the test module header is sufficient.

**Test infrastructure:** Fixture-based (matching the established pattern in
`test_statsbomb_bronze_coverage.py`, `test_wyscout_bronze_coverage.py`,
`test_silly_kicks_boundary.py`). Each source gets a JSON/parquet fixture
snapshot generated once from live data. Tests validate schema shape, column
population, and format contracts offline — no Databricks dependency, runs
in GitHub Actions Python CI.

Live-data invariants (e.g. "game_state is non-NULL for all IDSSE rows")
belong in **dbt singular tests** (same pattern as the ADR-018
`assert_*_join_resolves.sql` tests), not in Python CI.

#### Verification

- All 4 active sources pass all 5 invariant tests in CI (fixture-based).
- Corresponding dbt singular tests pass against live data post-deploy.

---

### S7 — Type-safe identifier dataclasses

#### Current state

`src/shared/identifiers.py` exports functions that return bare strings:
`statsbomb_native_match_id()`, `wyscout_native_match_id()`,
`idsse_native_match_id()`, `metrica_native_match_id()`, etc. Format
validation happens at call time via regex, but the return type is `str` —
no downstream type safety.

#### Design

Add `NamedTuple` wrappers alongside the existing functions (non-breaking —
existing call sites continue to work). **No Pydantic** — `src/shared/` has
zero external dependencies per CLAUDE.md; validation reuses the existing
compiled-regex validators.

```python
class NativeMatchId(NamedTuple):
    """Type-safe wrapper for a native match identifier."""
    provider: str
    value: str

    @classmethod
    def statsbomb(cls, raw: int | str) -> "NativeMatchId":
        value = statsbomb_native_match_id(raw)  # existing regex validator
        return cls(provider="statsbomb", value=value)

    @classmethod
    def idsse(cls, raw: str) -> "NativeMatchId":
        value = idsse_native_match_id(raw)  # existing regex validator
        return cls(provider="idsse", value=value)

    # ... etc for each provider
```

Similarly for `NativePlayerId`, `NativeTeamId`, `NativeCompetitionId`.

**Adoption checkpoint:** S7 is independent of S5. S5 uses bare strings
(matching existing pattern). S7 wrappers are available for NEW call sites
that construct native IDs — if adoption demonstrates value (catches a real
bug class), expand to existing call sites. If not, S7 stays as a leaf
utility with no downstream consumers beyond its own tests.

#### Verification

- Existing unit tests for `identifiers.py` continue to pass.
- New unit tests for the wrapper constructors validate format enforcement.
- `uv run pyright src/` passes (wrappers are fully typed).

---

### S8 — bronze.idsse_events period coverage extension

#### Current state

`_SECTION_TO_PERIOD` in `src/ingestion/idsse.py` maps:
```python
{"firstHalf": 1, "secondHalf": 2}
```

Extra time (`extraTimeFirstHalf`, `extraTimeSecondHalf`) and penalties
(`penaltyShootout`) are not recognized. No current 7-match IDSSE collection
exercises these paths, but any future Bundesliga match with extra time would
silently truncate events.

#### Design

Extend the mapping:
```python
_SECTION_TO_PERIOD: dict[str, int] = {
    "firstHalf": 1,
    "secondHalf": 2,
    "extraTimeFirstHalf": 3,
    "extraTimeSecondHalf": 4,
    "penaltyShootout": 5,
}
```

These period numbers follow the SPADL convention (silly-kicks uses the same
numbering).

**Unknown section handling:** The current code uses `.get(section)` →
`if period is None: continue` (silent skip, lines 686–689). Add a
`logger.warning("Unrecognized GameSection %r in match %s — skipping
FrameSet", section, match_id)` before the `continue` so that future DFL
format changes surface loudly instead of silently truncating data. The
`.get()` + skip pattern is correct (fail-loud without crashing the ingest),
but must log.

#### Verification

- Unit test with synthetic XML fixture containing `extraTimeFirstHalf` and
  `penaltyShootout` events — verify they land in bronze with correct period
  values.
- Existing tests continue to pass (no behavioral change for firstHalf /
  secondHalf).
- Code comment on the mapping: "Verified with synthetic fixture only — no
  production data exercises periods 3–5 yet."

---

## 4. Internal Dependencies

```
S2 (player_id_native) ──► S1 (remove not_null guards)
                      ──► S5 (DRY UDFs extract S2's pattern)
                      ──► S6 (contract tests validate completeness)
S3 (silly-kicks bump) ──► S3 re-ingest (pick up recovered events)
S4 (int_running_score) ──► S6 (game_state test validates)
S7 (type-safe IDs) — independent leaf, no downstream consumers in this PR
S8 (period coverage) — independent leaf
```

**Recommended implementation order:** S8 → S3 (bump) → S2 → S5 → S4 → S7
→ S1 → S6 → re-ingest → verify.

Rationale: S8 is an independent leaf. S3's bump is a `pyproject.toml`
one-liner. S2 provides `player_id_native` across all 4 UDFs. S5 extracts
the shared pattern from all 4 UDFs (must follow S2). S4 extends
`int_running_score` (dbt SQL, independent of Python UDFs). S7 is
independent — implements type-safe wrappers with no downstream coupling.
S1 removes test guards after S2 is validated. S6 validates everything.
Re-ingest is the final deploy step.

---

## 5. Files at Risk

| File | Items |
|------|-------|
| `src/ingestion/spadl_conversion.py` | S2 (all 4 UDFs), S3 (report logging), S5 |
| `src/ingestion/spadl_udf_shared.py` (NEW) | S5 |
| `src/shared/identifiers.py` | S2 (ADR-018 generators), S7 |
| `src/ingestion/idsse.py` | S8 |
| `pyproject.toml` | S3 (silly-kicks pin bump) |
| `dbt_project/models/intermediate/int_running_score.sql` | S4 (match_teams + spadl_goals + UNION types) |
| `dbt_project/models/marts/fct_action_values.sql` | S2 (player JOIN), S4 |
| `dbt_project/models/marts/fct_shots.sql` | S4 (game_state derivation) |
| `dbt_project/models/marts/fct_passes.sql` | S4 (game_state derivation) |
| `dbt_project/models/marts/_marts__models.yml` | S1 |
| `src/tests/test_source_onboarding_contracts.py` (NEW) | S6 |
| `scripts/migrations/2026-05-XX-add-player-id-native.sql` (NEW) | S2 |
| `dbt_project/tests/assert_*_player_id_native_join_resolves.sql` (4 NEW) | S2 (ADR-018) |
| `src/tests/test_silly_kicks_boundary.py` | S2 (format-contract parametrization) |

**Do NOT touch** (OPT-2/3/4 scope):
- `src/ingestion/defcon_lite.py`
- `src/ingestion/spadl_vaep.py`
- `src/ingestion/xg_model.py`
- `src/analytics/*.py`

---

## 6. Deploy Checklist

1. Merge PR to `main`.
2. Wheel bump (automatic via CI — `bump_wheel.py` called in Python CI).
3. Bronze migration auto-applied by live-CI (`player_id_native` column).
4. `dbt build --select int_running_score+ fct_action_values fct_shots fct_passes`
   (or full build) to propagate S4 changes.
5. Re-ingest ALL 4 sources' SPADL (picks up S2 `player_id_native` for
   all sources + S3 recovered CornerKick/OtherBallAction events for IDSSE).
6. `dbt build --full-refresh --select fct_action_values fct_shots fct_passes`
   to recompute `game_state` (all 3 marts derive it from
   `int_running_score`) and `player_key` for IDSSE/Metrica rows.
7. Verify S6 fixture-based contract tests pass in CI.
8. Verify S2's 4 dbt singular JOIN-coverage tests pass against live data.
9. Verify S1 `not_null` tests pass without `data_source` filter.

---

## 7. References

- `int_running_score.sql` — `materialized='ephemeral'` (recomputed on every
  `dbt build`; no separate refresh needed)
- `docs/superpowers/plans/PR-LL3-scope.md` — durable scope tracker
- ADR-016 — SPADL post-conversion enrichment + canonical/native naming
- ADR-018 — Cross-table format-contract testing
- `docs/superpowers/specs/2026-04-29-pr-ll2-path-b-close-out-design.md`
- silly-kicks 3.10.1 PR-S35 — CornerKick alias + OtherBallAction dispatch
