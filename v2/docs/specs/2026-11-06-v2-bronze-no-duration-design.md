# v2 Bronze Durations as Explicit-Unit Integers — Design

**Status:** APPROVED (A/B round 3; owner 2026-11-06), with AMENDMENTS. Plan reviews r2-r3 (A/B-approved, owner 2026-11-06): the resolution guard and drop-and-add in one step — both SUPERSEDED. Owner amendment 2026-11-06 (after implementation): §6 is a CLEAN-SLATE rebuild; the in-place migration route and its code are removed (A/B-approved round 8, owner-approved 2026-11-06). Rounds 1-2: REQUEST CHANGES from both reviewers; round 3: APPROVE from both
(`D:\Development\_reviews\2026-11-06-v2-bronze-no-duration-spec[-r2|-r3]-{A,B}.md`); every finding is answered in §10-§12 (round-3 CONSIDERs applied).
No code until this doc is approved and a plan is written, reviewed, and approved.

**Spec owner:** Karsten. **Author session:** calc-layer (branch `feat/v2-calc-tracking-metrics`).

**Amends** the v2 ingestion bronze contract (`2026-10-26-v2-ingestion-design.md`, the parser type rules) and the
calc-layer slice (`2026-11-04-v2-calc-layer-tracking-metrics-design.md`, R8 `fct_tracking_ball.time_seconds`).

**Owner decisions (2026-11-06):** fix the class, not one column ("gold standard, long-term best practice; throwing
away and rebuilding the database is not a concern"); do it in the calc-layer cycle, before the Mac e2e.

---

## 1. The problem, with evidence

v2 bronze stores 16 time fields as Arrow `pa.duration(...)`. The SAME logical column then surfaces as two different
DuckDB types, depending on how the run writes bronze (both reviewers re-probed this on DuckDB 2.0.0, top-level and
nested):

- **Encrypted lake** → `LocalNativeWriter` (native DuckLake insert): the duration surfaces as `INTERVAL`
  (verified on the Mac lake 2026-11-05: `typeof(timestamp) = INTERVAL` on idsse and gradientsports tracking frames).
- **Unencrypted lake** → `RegisterWriter` (`ducklake_add_data_files` of parquet): DuckLake has no duration type, so
  the column surfaces as `BIGINT` in the duration's unit. `select_writer` picks the writer by `cat.encryption_key`
  (`bronze/writers.py:170-174`).

Consumers that assume one type break, or go silently wrong, on the other:

- `fct_tracking_ball.time_seconds`: the original `cast(timestamp as bigint)/1e6` failed on the encrypted lake
  (`Unimplemented type for cast (INTERVAL -> BIGINT)`; pitch_control 980/980 failed, 2026-11-05). The fix
  `epoch(timestamp)` fails on every unencrypted lake: `Binder Error: No function matches ... 'epoch(BIGINT)'`. The
  seeded e2e `tests/e2e/test_compute_local.py` now fails at the dbt build of `fct_tracking_ball` (run 2026-11-06),
  so every compute after it fails.
- **statsbomb SPADL is silently wrong on unencrypted lakes.** The adapter passes `timestamp` through
  (`spadl_actions.py:665-677`) and the converter does `pd.to_timedelta(ts).dt.total_seconds()`. On a register-path
  lake `ts` is `int64` milliseconds, and `pd.to_timedelta(int64)` reads NANOSECONDS: 2250 ms becomes 2.25e-06 s
  (Reviewer B probe). Every statsbomb action time is 10^6 too small, with no error.
- skillcorner SPADL carries dual-path code for the same reason (`spadl_actions.py:479-488`:
  `ts.dt.total_seconds() if is_timedelta64_dtype(ts) else ts.astype("float64") / 1000`).

A type whose physical form depends on the write path is a latent defect in every present and future consumer.

## 2. Goal and rule

**Rule (new bronze contract):** no bronze column has an Arrow duration type, at the top level or nested in a struct
or list. This covers provider bronze AND compute bronze (`spadl_actions`, `pitch_control_values`,
`shape_graph_labels`, `line_breaking_results`). A time quantity is stored as an **integer in an explicit unit**, and
the unit is in the column (or nested field) name, following the existing `<name>_mm` convention for coordinates
(`parsers/csvmap.py`):

| unit | Arrow type | name suffix |
|---|---|---|
| microseconds | `int64` | `_us` |
| milliseconds | `int64` | `_ms` |
| seconds | `int64` | `_s` |

Each field keeps its source precision exactly (no float). Integers surface as `BIGINT` on both write paths, so every
consumer has one type. Consumers convert explicitly and name the unit (`timestamp_us / 1e6`,
`pd.to_timedelta(timestamp_ms, unit="ms")`). `pa.timestamp` (wall-clock instants) is out of scope: Reviewer B probed
timestamps, time zones, and `int8` as identical on both write paths.

## 3. The 16 fields

| table (parser) | today | new name | where it is produced |
|---|---|---|---|
| `gradientsports_tracking_frames` | `timestamp` duration(us) | `timestamp_us` | code-built, `parsers/gradientsports.py:416` (`periodElapsedTime`, kind `us`, `gradientsports_fields.py:372`) |
| `idsse_tracking_frames` | `timestamp` duration(us) | `timestamp_us` | code-built, `parsers/idsse.py:718` |
| `skillcorner_tracking_frames` | `timestamp` duration(us) | `timestamp_us` | code-built, `parsers/skillcorner.py:392` |
| `skillcorner_tracking_frames` | `match_clock` duration(ms) | `match_clock_ms` | code-built, `parsers/skillcorner.py:398` |
| `skillcorner_players` | `start_time`, `end_time` duration(s) | `start_time_s`, `end_time_s` | `Field` kind `clock_hms`, `skillcorner_fields.py:146-147` |
| `skillcorner_dynamic_events` | `time_start`, `time_end` duration(ms) | `time_start_ms`, `time_end_ms` | `csvmap` kind `clock_ds`, `skillcorner_fields.py:180-181` |
| `skillcorner_phases_of_play` | `time_start`, `time_end` duration(ms) | `time_start_ms`, `time_end_ms` | `csvmap` kind `clock_ds`, `skillcorner_fields.py:502-503` |
| `statsbomb_events` | `timestamp` duration(ms) | `timestamp_ms` | `Field` kind `clock_ms`, `parsers/statsbomb.py:232` |
| `statsbomb_lineups` (nested) | cards `time` duration(s); positions `from`, `to` duration(ms) | `time_s`; `from_ms`, `to_ms` | `jsonmap` nested converters, `parsers/statsbomb.py:284, 295-296` |
| `statsbomb_lineup_teams` (nested) | `events[].timestamp`, `formations[].timestamp` duration(ms) | `timestamp_ms` in each | `jsonmap` `nested={...}` converters, `parsers/statsbomb.py:61, 323-351` |

The time-producing converter kinds are `jsonmap` `clock_ms`, `clock_s`, `clock_ms_or_s`, `clock_hms`
(`parsers/jsonmap.py:44-47, 149-175`; they feed 8 of the 16 fields, including skillcorner's
`start_time`/`end_time` jsonmap `Field`s), `csvmap` `clock_ds`, and the gradientsports
tracking kind `us`. Each kind now returns an `int64` in its unit, and its documentation string says so. The guards of
§4, not this table, are the authority for completeness.

## 4. Guards (each written first and watched fail)

1. **No duration anywhere in bronze.** A test walks every parser's declared output schema, the shared
   `bronze/schemas.py` schemas, and every compute bronze schema (`SPADL_ACTIONS_SCHEMA`, `PITCH_CONTROL_SCHEMA`,
   `SHAPE_GRAPH_SCHEMA`, `LINE_BREAKING_SCHEMA`), recursively through struct and list types, and fails on any
   `pa.types.is_duration`. It also runs every parser over its existing fixtures and checks the produced tables, so a
   computed column cannot escape it.
2. **Time columns are declared, suffixed, and come from a declared path.** The population is explicit:
   - every `Field` (jsonmap) and CSV column (csvmap) whose kind is a time kind (`clock_ms`, `clock_s`,
     `clock_ms_or_s`, `clock_hms`, `clock_ds`, gradientsports `us`) has a name, or nested sub-path name, ending in
     the kind's unit suffix (`_ms`, `_s`, `_us`);
   - the four code-built tracking time columns are declared: `timestamp_us` in the shared core schema
     (`bronze/schemas.py` `TRACKING_FRAMES_CORE`), which `check_tracking_schema` already enforces on every tracking
     write (`writer.py:120-123`, `writers.py:273`); skillcorner's `match_clock_ms`, built inline today
     (`parsers/skillcorner.py:388-402`), moves into a new declared per-provider extension schema
     `TRACKING_FRAMES_EXTRA["skillcorner"]` in `bronze/schemas.py`, and `check_tracking_schema` is extended to enforce
     a provider's declared extension columns (name and type) the same way it enforces the core;
   - a test asserts each code-built time column's declared type is `int64` and its name ends in a unit suffix.
   A time quantity has no other route into bronze: guard 1 blocks the duration route, and the time kinds and the
   declared tracking schemas are the only producers of time integers. (Detecting an arbitrary undeclared integer as
   "a time" is not decidable; making the declared paths the only paths is.)
3. **One value on both write paths.** An e2e-level test writes the same matches through `RegisterWriter`
   (unencrypted) and through `LocalNativeWriter` (encrypted), builds the gold views, and asserts equal values for:
   `fct_tracking_ball.time_seconds` (idsse, gradientsports); statsbomb SPADL `time_seconds`; skillcorner SPADL
   `time_seconds`. Each is also asserted against a known source value (for example a statsbomb event at
   `00:00:02.250` gives `time_seconds == 2.25`), so a dropped `unit=` (nanosecond read) fails. If the encrypted arm
   cannot run in CI, the plan says so and runs it in the bench tier on the Mac, and a unit-level type-parity test
   (one per consumer, both types fed directly) runs in CI.
4. **Encoding lists name real columns.** A test asserts that every name in `bronze/tracking.py` `_SORT`, `_DELTA`
   and `_DICTIONARY` is a column of the core schema of its tracking kind. (Today `_DELTA` lists `timestamp`
   (`bronze/tracking.py:29`); after the rename a stale name is silently ignored by pyarrow and the column loses delta
   encoding — Reviewer A probe on pyarrow 26.)
5. The existing bronze-doc contract test (`tests/contract/test_bronze_docs.py`, `scripts/document_bronze.py`) and the
   five provider inventory tests stay green with the renamed columns and types.

## 5. Consumers (to change; the plan re-greps `src/`, `dbt/`, `scripts/`, `tests/`)

- `dbt/models/gold/fct_tracking_ball.sql`: `time_seconds = timestamp_us / 1e6` for idsse and gradientsports (one
  expression); skillcorner keeps its frame-based derivation. The INTERVAL guard
  (`test_dbt_materialisation.py::test_fct_tracking_ball_reads_an_interval_timestamp_as_seconds`) is replaced by guard
  3. Its stale INTERVAL comments go.
- `dbt/models/gold/_gold__models.yml:201-203` (the stale doc) and every `_<provider>__sources.yml` column doc.
- `compute/spadl_actions.py`: statsbomb passes `pd.to_timedelta(timestamp_ms, unit="ms")` to the converter;
  skillcorner formats `time_start_ms` into the converter's input string, which stays `M:SS.sss`
  (`spadl_actions.py:488`). Both dual-path branches are deleted.
- `bronze/tracking.py` `_DELTA` (`timestamp` → `timestamp_us`) and its `_SORT` / `_DICTIONARY` lists (guard 4).
- `bronze/schemas.py` `TRACKING_FRAMES_CORE`, shared by idsse, skillcorner and gradientsports and enforced by
  `check_tracking_schema`.
- `parsers/jsonmap.py`, `parsers/csvmap.py`, the gradientsports `us` kind, and the four code-built columns.
- `scripts/document_bronze.py` output and the parser/inventory tests.
- **Re-keying:** the tracking computes hash gold-view rows, and `time_seconds` keeps its name and value, so the
  rename alone does not re-key them. The full re-compute comes from the lake rebuild (§6).

## 6. Lake rebuild: a clean slate (amendment, owner 2026-11-06)

**Owner decision (2026-11-06, superseding the in-place route of rounds 1-3 and the plan's Tasks 4-5):** every v2 lake
is rebuilt from a CLEAN SLATE. The v2 lakes hold test data, the Mac is the only install, and v2 is built to be
installable from a clean slate. So there is NO in-place migration: no column retirement, no reparse-in-place, no
migration guards, no rebuild pre-check. ("Absolutely not, I specifically told you this had to be from a clean slate.")
That machinery was built (plan Tasks 4-6) and is removed; schema evolution stays additive-only (`bronze/evolve.py`).

The route, per lake:

1. **Bump `PARSER_VERSION`** of the four affected parsers (statsbomb, idsse, skillcorner, gradientsports), because
   their bronze output changed. (On a clean slate no old version exists; the bump keeps the version history true.)
2. **Stop the lake's writers:** a local schedule and the web app; on HF the owner suspends the scheduled job and
   pauses the ops Space (its token can write the config).
3. **Record the lake's non-secret config** (the modules and their filters) before anything is deleted.
4. **Delete the lake's data** (irreversible; the owner's yes at that step). A local lake: delete the `lake` and
   `lake-restricted` roots, which removes everything under them: bronze, catalog, raw archive, runs, and `config/`
   (the settings with their `history/`, the operator account, the review `decisions` and `requests`). An HF lake:
   empty the `lake` and `restricted` buckets; the buckets, their tokens and grants stay, and the lease ledger
   repository (`<namespace>/lake-control`) is KEPT (`open_storage` checks it; the lease heals a kept ledger over an
   emptied bucket).
5. **Install fresh:** deploy the code, then `lakehouse init --lake <root> --restricted <root> --profile <kind>`. For a
   local lake init creates the roots and the bootstrap file (`laptop`, `staging`). For HF roots (`--profile hf`) the
   owner creates the buckets and init only saves the `hf` defaults on them, as a one-off HF job with the scheduled
   job's image, env and secrets; it never overwrites an existing config. Then set the recorded modules with
   `lakehouse config set`. A local install re-creates its operator account (`lakehouse account create`, the password
   never entering a development session); the HF ops Space signs in with OAuth and has no account.
6. **Ingest fresh** with `lakehouse run` (online; it computes too). Local: repeat until no compute step defers. HF:
   one one-off `cpu-basic` job, or more only until the ingest is complete (the plan's Task 8 Step 6 names the perf
   calibration run, which leaves a compute backlog); after the resume, the nightly scheduled runs continue the
   backfill.
7. **Verify:** the lake's `catalog/CURRENT` records the expected `encrypted` flag (`true` for an encrypted local lake,
   the `LocalNativeWriter` path; `false` for HF, whose buckets are platform-encrypted, the `RegisterWriter` path);
   `DESCRIBE` shows every bronze time column as `BIGINT` with its unit suffix and no `INTERVAL`; guard 3's values
   hold (`fct_tracking_ball.time_seconds` is period-relative seconds, statsbomb SPADL `time_seconds` are seconds); the
   run records no `compute_failed`. Then restart the web app or resume the Space and the schedule.

**Mac scope for this cycle (owner 2026-11-06).** Full scale was already measured, so the fresh Mac ingest is capped by
competition filter at about 100 matches per provider: `skillcorner_pining` LaLiga 2023/24 + 2024/25 and Champions
League 2023/24 + 2024/25 (98 matches); `statsbomb_pining` FIFA World Cup 2022 + UEFA Women's Euro 2025, 360 only (95
matches); `idsse_pining` (7) and `gradientsports_pining` (64) in full. Its time and space give the estimate for the
full set (for example the 909 skillcorner matches).

**Tests:** guards 1-4 (§4) and the consumer tests stand. The retirement and migration-guard tests are removed with
their code. The HF install route is tested in `tests/unit/test_cli.py`
(`test_init_saves_the_hf_defaults_on_existing_buckets`, `test_init_profile_must_match_the_roots`).

## 7. Non-goals

- No change to `pa.timestamp` instants (§2).
- No change to silly-kicks.

## 8. Definition of done

- Guards 1-5 written first, watched fail, then green.
- No `pa.duration` and no time `timedelta` construction in `src/lakehouse` bronze paths (grep over `src/`, `dbt/`,
  `scripts/`), and no dual-path duration code in `compute/`.
- The seeded e2e `tests/e2e/test_compute_local.py` passes again.
- Both lakes rebuilt from a clean slate by §6, with §6 step 7's checks recorded; the Mac and HF e2e of the
  calc-layer cycle run on the rebuilt lakes.
- v2 ADR-018: "bronze stores durations as explicit-unit integers", with the evidence of §1 and the clean-slate
  rebuild of §6.

## 9. Interaction with the perf plan

This change lands before the perf plan's Task 9 (Mac e2e). The perf plan's input key re-keys nothing by itself here
(§5), and the lake rebuild re-computes every match anyway. The perf plan's `fct_tracking_ball` expectations (time in
seconds) are unchanged.

> **Note (2026-11-06):** sections 10-12 record rounds 1-3. Every row about `RETIREMENTS` /
> `RETIRED_COLUMNS`, drop-first or drop-and-add, `reparse --all`, the compute or resolution migration guard and the
> rebuild pre-check is SUPERSEDED by the owner's clean-slate amendment of section 6.

## 10. Dispositions (round 1)

Reviewer A = `…-spec-A.md` (DUR-SPEC-nn). Reviewer B = `…-spec-B.md` (NODUR-SPEC-nn). They agree on every shared point.

| ID | Severity | Disposition |
|---|---|---|
| A-01 / B-01 rebuild route does not work | SHOULD FIX | Fixed. §6: parser-version bumps, declarative `RETIRED_COLUMNS` in `bronze/evolve.py` under the run's lease, offline `lakehouse reparse` per lake, raw-archive completeness check, verification (versions, counts, no retired column). |
| A-02 / B-02 missed consumers | SHOULD FIX | Fixed. §3 names the jsonmap clock kinds; §5 adds `_DELTA`, `TRACKING_FRAMES_CORE`, `_gold__models.yml:201-203`, stale comments; grep scope `src/ dbt/ scripts/ tests/`; guard 4 for the encoding lists. |
| A-03 / B-04 guard 2 has no population | SHOULD FIX | Fixed. §4 guard 2: time kinds + the declared tracking schemas are the population and the only routes; the code-built columns are declared in `TRACKING_FRAMES_CORE` / skillcorner's schema. |
| A-04 / B-03 unguarded unit conversions; statsbomb is not dual-path | SHOULD FIX | Fixed. §1 states the live statsbomb 10^6 error on register-path lakes. Guard 3 covers both SPADL consumers on both write paths with known source values. |
| A-05 / B-05 wrong table name | CONSIDER | Fixed. §3: `statsbomb_lineup_teams` `events[]` / `formations[]`, `nested={...}`. |
| A-06 re-key reason | CONSIDER | Fixed. §5: the rebuild re-computes; the rename alone does not re-key. |
| A-07 compute bronze outside guard 1 | CONSIDER | Fixed. §2 and guard 1 include the compute bronze schemas. |
| A-08 / B-06 skillcorner format; SPADL equality | CONSIDER | Fixed. §5 keeps `M:SS.sss`; guard 3 asserts SPADL time values from known sources on both paths. |

## 11. Dispositions (round 2)

Reviewer A = `…-spec-r2-A.md`. Reviewer B = `…-spec-r2-B.md`. One factual disagreement: round 1 A said `reparse` has
no `--all` flag; B shows `--all` / `--match` is a required group (`cli.py:579-581`). B is right (verified); §6 now
says `reparse --all`.

| ID | Severity | Disposition |
|---|---|---|
| A r2 DUR-SPEC-03 / B r2 NODUR-SPEC-04 `match_clock_ms` undeclared | SHOULD FIX | Fixed. §4 guard 2: a declared `TRACKING_FRAMES_EXTRA["skillcorner"]` schema, enforced by an extended `check_tracking_schema`. |
| A r2 DUR-SPEC-09 / B r2 NODUR-SPEC-07 drop can fire before every match is rewritten | SHOULD FIX | Fixed. §6: drop only in `reparse`, first; invariant + interim NULL state stated; compute guard (`compute_deferred_migration`) keeps NULL time out of every compute; HF suspend → deploy → reparse → resume order; tests incl. a reparse stopped midway. |
| B r2 NODUR-SPEC-08 same-name type changes | SHOULD FIX | Fixed. §6: `RETIRED_COLUMNS` covers the same-name type-changed columns; drop-first + re-add with the new type; test. |
| B r2 NODUR-SPEC-09 `reparse --all` | CONSIDER | Fixed (see above). |
| A r2 DUR-SPEC-10 jsonmap feeds 8 | CONSIDER | Fixed. §3. |

## 12. Dispositions (round 3)

Both reviewers APPROVE (`…-spec-r3-{A,B}.md`). Their CONSIDERs are applied:

| ID | Disposition |
|---|---|
| A r3 11: "six" fields | Fixed. §6: five fields in four same-name columns. |
| A r3 12: guard per canonical key | Fixed. §6 step 4: skip a match key whose contributing records include a provider in migration, resolved through `ops_canonical_pointers` like `_match_visibility`. |
| A r3 13 / B r3 10: a run before the first drop fails | Fixed. §6 step 5: the failure is stated (loud, not lossy); every lake's schedule (Mac too) is suspended before the deploy and resumed after the first drop; HF "safe" only after the first drop. |
| A r3 14: step reference | Fixed. §8 points at step 6. |
| B r3 11: archive-less matches | Fixed. §6 step 5: an online run does not re-version an unchanged source; a lake with any archive-less match is rebuilt by a fresh ingest. |
