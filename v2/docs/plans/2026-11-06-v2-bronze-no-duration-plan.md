# v2 Bronze Durations as Explicit-Unit Integers — Implementation Plan

> **For agentic workers:** execute with superpowers:executing-plans, inline, in one session. No subagents, no
> worktree; branch `feat/v2-calc-tracking-metrics`. Steps use checkbox (`- [ ]`) syntax. **No step commits:** this
> work rides in the calc-layer cycle's single owner-gated commit.

**Status:** A/B-approved round 3 and owner-approved (2026-11-06); then AMENDED by the owner's clean-slate decision
(2026-11-06, the Amendment below; Tasks 4-5 removed). The amendment is A/B-approved (round 8) and owner-approved (2026-11-06) (reviews from round 4:
`D:\Development\_reviews\2026-11-06-v2-bronze-no-duration-spec-r4-{A,B}.md`; dispositions at the end).

**Goal:** Remove every Arrow duration type from v2 bronze, store time as explicit-unit integers, and rebuild the lakes
so every consumer reads one type on both write paths.

**Architecture:** The time converter kinds return `int64` in their unit; column names carry the unit suffix; the
tracking schemas declare the code-built time columns; consumers convert explicitly. Each lake is rebuilt from a clean slate
(delete, `init`, fresh ingest); there is no in-place migration.

**Tech Stack:** Python 3.12, pyarrow, DuckDB 2.0.0 / DuckLake, dbt-duckdb, pandas, silly-kicks 4.139.1 (read-only).

**Spec:** `v2/docs/specs/2026-11-06-v2-bronze-no-duration-design.md` (A/B-approved round 3, owner-approved
2026-11-06; §6 amended by the owner on 2026-11-06 to a clean-slate rebuild). Section refs (§) are the spec's.

## Amendment (owner 2026-11-06, after Tasks 1-6 were built): clean slate, no in-place migration

The owner rejected the in-place route: every v2 lake is rebuilt from a CLEAN SLATE (spec §6, amended). Tasks 4 and 5
and the pre-check of Task 6 are REMOVED: their code (`RETIREMENTS`/`retire_columns`, `parsers/declared.py`, the
reparse hook, `compute/migration.py` and `ComputeControl.skip_match`, `scripts/rebuild_precheck.py`), their tests and
the deploy.md migration runbook are deleted. Tasks 7 and 8 are rewritten below as clean-slate rebuilds. The
HF clean slate needs a first-config route the code lacked, so Task 8 Step 0 adds `lakehouse init --profile hf`
(owner 2026-11-06, after the round-4 reviews A DUR-SPEC-15 / B NODUR-PLAN-06). The Mac ingest
is capped by competition filter at about 100 matches per provider (owner). Tasks 1-3 and the rest of Task 6 (ADR-018,
the AGENTS bullet, the greps, the full gate) stand; ADR-018 is rewritten for the clean slate. This amendment goes to
the A/B reviewers with the amended spec §6 and ADR-018.

## Global Constraints

- pyright strict, ruff, `ruff format`, `lint-imports`, and the default pytest tier
  (`uv run pytest -m "not live and not hf and not bench"`) stay green; run from `v2/`.
- Unit suffixes are exactly `_us` (int64 microseconds), `_ms` (int64 milliseconds), `_s` (int64 seconds); no float.
- No `pa.duration` anywhere under `src/lakehouse`; no dual-path duration code in `compute/`.
- Every outward action (Mac deploy and run, HF deploy, HF job, schedule suspend/resume) needs the owner's yes at that
  step. The HF schedule suspend and resume are owner actions (ops Settings).
- This plan's Task 7 (Mac rebuild) replaces the perf plan's Task 9 Steps 2-4, and its Task 8 (HF rebuild) runs in
  place of the perf plan's Task 11 Steps 1-2 (the perf plan is amended to say so; see its "Amendment").

## Review Focus

1. **A nested time field inside a list of structs** (statsbomb lineups `positions[].from`, lineup-teams
   `events[].timestamp`): the converter gives `int64` inside the nested type. Test in Task 1 (guard 1 walks nested
   types over parsed fixtures).
2. **A null time value** (a skillcorner tracking frame with no `timestamp` source value; a blank CSV `time_end`):
   stays NULL as an int64, never 0. Test in Task 1.
3. **An extra-time match** (gradientsports period 3/4) after the rename: `fct_tracking_ball.time_seconds` holds on
   both write paths. Test in Task 3.
4. **A fresh ingest that is stopped part-way** (a SIGTERM or the compute deadline): the next `lakehouse run`
   continues it (resume, no match computed twice). Checked on the Mac in Task 7 Step 4 (it happened on 2026-11-06:
   run 1 was stopped by SIGTERM after 233 of 264 matches).
5. **The HF clean slate**: `lakehouse init --profile hf` saves the hf defaults on the owner's existing, emptied
   buckets and never overwrites a config. Tests in Task 8 Step 0 (`test_init_saves_the_hf_defaults_on_existing_buckets`,
   `test_init_profile_must_match_the_roots`).

---

### Task 1: The bronze contract — converters, names, tracking schemas, guards 1, 2, 4

**Files:**
- Modify: `v2/src/lakehouse/parsers/jsonmap.py:149-214` (`_clock_ms`, `_clock_s`, `_clock_ms_or_s`, `_clock_hms`
  return `int` in their unit; each kind's Arrow type is `pa.int64()`), `v2/src/lakehouse/parsers/csvmap.py:38-52`
  (`clock_ds` → `pa.int64()` milliseconds), `v2/src/lakehouse/parsers/gradientsports.py:120, 193, 416` (kind `us` →
  `int64` microseconds; output column `timestamp_us`), `v2/src/lakehouse/parsers/idsse.py:718` (`timestamp_us`),
  `v2/src/lakehouse/parsers/skillcorner.py:388-402` (`timestamp_us`; `match_clock_ms` — its source field is
  `timestamp` per `skillcorner.py:754`), `v2/src/lakehouse/parsers/skillcorner_fields.py:146-147, 180-181, 502-503`,
  `v2/src/lakehouse/parsers/statsbomb.py:232, 284-285, 295-296, 306, 323-351` (renamed columns and nested fields, §3:
  cards `time` → `time_s`; positions `from`/`to` → `from_ms`/`to_ms`; lineup-teams `events[].timestamp` and
  `formations[].timestamp` → `timestamp_ms`; the `nested={...}` converter keys renamed to match), and the four
  `PARSER_VERSION` bumps: gradientsports "2"→"3", idsse "1"→"2", skillcorner "1"→"2", statsbomb "1"→"2" (wyscout
  unchanged: no duration field).
- Modify: `v2/src/lakehouse/bronze/schemas.py:47-56` (`TRACKING_FRAMES_CORE`: `timestamp_us` `int64`; new
  `TRACKING_FRAMES_EXTRA: Final[Mapping[str, pa.Schema]] = {"skillcorner": pa.schema([pa.field("match_clock_ms",
  pa.int64())])}`), `v2/src/lakehouse/bronze/tracking.py:22-53` (`check_tracking_schema(kind, schema, *, provider:
  str)` also enforces `TRACKING_FRAMES_EXTRA.get(provider)` for `frames`; `_DELTA["frames"]` `timestamp` →
  `timestamp_us`), and its one call site `bronze/writer.py:123` passes the provider (the table-name prefix).
- Test: `v2/tests/unit/test_bronze_no_duration.py` (new); the parser unit tests that assert old names/types.

**Interfaces:**
- Produces: `TRACKING_FRAMES_EXTRA`; `check_tracking_schema(kind, schema, *, provider)`; the §3 column names.
- `TIME_KINDS: Final = {"clock_ms": "_ms", "clock_s": "_s", "clock_ms_or_s": "_ms", "clock_hms": "_s", "clock_ds":
  "_ms"}` in `parsers/jsonmap.py` (csvmap imports `clock_ds` from it). `clock_ms_or_s` returns milliseconds (a
  seconds-only text becomes `s * 1000`). The gradientsports `us` kind (keyed by source name `periodElapsedTime` in
  `FRAME_KINDS`) is checked through its OUTPUT column `timestamp_us` (guard 2, second bullet).

- [ ] **Step 1: Write the failing tests** in `test_bronze_no_duration.py`:
  - `test_no_duration_type_in_any_bronze_schema` (guard 1): every parser's declared schemas, the `bronze/schemas.py`
    schemas, and `SPADL_ACTIONS_SCHEMA` / `PITCH_CONTROL_SCHEMA` / `SHAPE_GRAPH_SCHEMA` / `LINE_BREAKING_SCHEMA`,
    walked recursively (struct + list), contain no `pa.types.is_duration`.
  - `test_no_duration_type_in_parsed_fixture_tables` (guard 1, Review Focus 1): each parser runs over its existing
    test fixtures; every produced table passes the same walk.
  - `test_time_kind_columns_carry_their_unit_suffix` (guard 2): every jsonmap `Field`, CSV column, and `nested`
    converter path whose kind is in `TIME_KINDS` has a name (or last sub-path segment) ending in that kind's suffix.
  - `test_code_built_tracking_time_columns_are_declared` (guard 2): `TRACKING_FRAMES_CORE` has `timestamp_us`
    `int64` (this also covers the gradientsports `us` kind's output); `TRACKING_FRAMES_EXTRA["skillcorner"]` has
    `match_clock_ms` `int64`; `check_tracking_schema(..., provider="skillcorner")` rejects a frames schema lacking it
    or typing it otherwise.
  - `test_tracking_encoding_lists_name_core_columns` (guard 4): every `_SORT` / `_DELTA` / `_DICTIONARY` name is a
    column of `_CORE[kind]`.
  - `test_null_time_stays_null` (Review Focus 2): a skillcorner tracking line with no `timestamp` value and a CSV row
    with a blank `time_end` give NULL `int64`, not 0.
  - `test_time_values_keep_their_precision`: `"00:00:02.250"` → `timestamp_ms == 2250`; GS `periodElapsedTime`
    `12.345678` → `timestamp_us == 12345678`; `clock_hms "01:02:03"` → `3723` (two-digit hours, `jsonmap.py:55`).
- [ ] **Step 2: Run, expect FAIL** (durations present; old names).
- [ ] **Step 3: Implement** the converters, renames, schemas, `check_tracking_schema`, `_DELTA`, version bumps.
- [ ] **Step 4: Run** `test_bronze_no_duration.py` and every parser / fields / csvmap / jsonmap unit test; update each
  old-name/old-type assertion to the §3 names (each change is a rename, not a behaviour change). Expected: PASS, and
  `grep -rn "pa.duration" src/lakehouse` prints nothing.

### Task 2: Docs and contracts — sources, bronze docs, inventories

**Files:**
- Modify: `v2/dbt/models/staging/*/_*__sources.yml` (renamed columns, types, descriptions), `v2/dbt/models/gold/
  _gold__models.yml:201-203` (stale doc).
- Regenerate: `uv run python scripts/document_bronze.py <provider>` for statsbomb, idsse, skillcorner, gradientsports.
- Test: `v2/tests/contract/test_bronze_docs.py` and the five `test_*_inventory.py`.

- [ ] **Step 1: Run** `uv run pytest tests/contract -q`. Expected: FAIL on the renamed columns.
- [ ] **Step 2: Regenerate** the four providers' docs; update the sources YAML where the generator does not own it;
  update `_gold__models.yml:201-203`.
- [ ] **Step 3: Run** `uv run pytest tests/contract -q` and `uv run pytest tests/unit/test_dbt_materialisation.py -q`,
  expect PASS.

### Task 3: Consumers — `fct_tracking_ball`, SPADL adapters, guard 3

**Files:**
- Modify: `v2/dbt/models/gold/fct_tracking_ball.sql` (`time_seconds = timestamp_us / 1e6` for idsse and
  gradientsports; delete the INTERVAL comments).
- Modify: `v2/src/lakehouse/compute/spadl_actions.py:467-488` (skillcorner: `time_start_ms` → the converter's
  `M:SS.sss` string; delete the dual-path branch), `:663-677` (statsbomb: `timestamp` =
  `pd.to_timedelta(timestamp_ms, unit="ms")`).
- Replace: `tests/unit/test_dbt_materialisation.py::test_fct_tracking_ball_reads_an_interval_timestamp_as_seconds`.
- Create: `v2/tests/support/computekit.py` (the fixture adapter builders moved out of `tests/e2e/test_compute_local.py`,
  unchanged; that file imports them).
- Test: `v2/tests/e2e/test_time_both_write_paths.py` (new), `tests/unit/test_spadl_actions_compute.py`.

**Interfaces:**
- Guard 3 fixture: the seeded `kit` with the gradientsports, idsse, skillcorner, statsbomb fixture adapters (from
  `computekit`), run twice: plain (`RegisterWriter`) and with `monkeypatch.setenv(ENCRYPTION_KEY_ENV, key)`
  (`LocalNativeWriter`, as `tests/e2e/test_no_private_rows.py:108`). Review Focus 3: the gradientsports adapter in
  this test appends extra-time frames (period 3) to the bytes of `tests/fixtures/gradientsports/tracking_9001.jsonl`
  in memory (the fixture file is not changed), and its metadata sets the ET direction.

- [ ] **Step 1: Write the failing tests:**
  - `test_tracking_ball_time_is_equal_on_both_write_paths` (guard 3): `fct_tracking_ball.time_seconds` per
    `(canonical_match_key, period, frame)` is identical on both lakes, one known frame has its known seconds, and the
    ET-period frames are present.
  - `test_statsbomb_spadl_time_is_equal_on_both_write_paths`: SPADL `time_seconds` identical on both lakes; a fixture
    event at `00:00:02.250` gives `2.25` (a dropped `unit=` reads nanoseconds and fails).
  - `test_skillcorner_spadl_time_is_equal_on_both_write_paths`: the same for `time_start_ms`.
  - Unit: `test_statsbomb_input_converts_timestamp_ms`, `test_skillcorner_input_formats_time_start_ms`.
- [ ] **Step 2: Run, expect FAIL** (today the register-path lake fails the dbt build of `fct_tracking_ball`).
- [ ] **Step 3: Implement** the view, the two adapters, and the `computekit` move; delete the INTERVAL test and the
  dual-path branches.
- [ ] **Step 4: Run** `test_time_both_write_paths.py`, `test_spadl_actions_compute.py`, `test_dbt_materialisation.py`,
  and the whole `tests/e2e/test_compute_local.py` (unchanged tests must still pass after the helper move), expect
  PASS. That e2e file is the perf plan's Task 5 e2e (spec §8: the seeded e2e passes again); record it in the perf
  ledger.

### Task 4: Retirement — REMOVED (clean slate, amendment above)

### Task 5: The migration guards — REMOVED (clean slate, amendment above)

### Task 6: ADR-018, runbook, and the spec's greps (pre-check REMOVED, amendment above)

**Files:**
- Create: `v2/docs/adrs/ADR-018-bronze-durations-as-unit-integers.md` (Nygard): the rule (§2), the evidence (§1), the
  guards (§4), the clean-slate rebuild (§6), the rejected alternatives (compile-time macro, writer normalisation,
  in-place migration).
- Modify: `v2/docs/ops/deploy.md`: "Bronze schema rebuild (durations)" with the clean-slate order (§6) and the
  §6 step 7 checks; `v2/AGENTS.md`: one class-1 bullet "bronze stores no duration type; time is `_us`/`_ms`/`_s`
  int64 → ADR-018".

- [ ] **Step 1: Write** both docs.
- [ ] **Step 2: The spec's greps** (§5, §8), from `v2/`:
  - `grep -rnE "\b(timestamp|match_clock|start_time|end_time|time_start|time_end)\b" src dbt scripts tests` —
    Expected: no hit reads an old (pre-ADR-018) bronze time column name (hits on `timestamp_us`/`_ms`,
    `pa.timestamp`, `_ingested_at`, wall-clock `timestamp` types, source payload keys and converter input columns are
    listed and each explained in the ledger).
  - `grep -rnE "timedelta\(|pa\.duration|\.dt\.total_seconds|INTERVAL" src dbt scripts` — Expected: no bronze
    path constructs or reads a duration (remaining hits, e.g. the compute deadline `timedelta` in `control.py`, are
    listed and explained).
- [ ] **Step 3: Run** the full default tier `uv run pytest -m "not live and not hf and not bench"`, plus ruff, format,
  pyright, lint-imports. Expected: all green; report the counts.

### Task 7: Mac rebuild from a clean slate (spec §6) — replaces perf Task 9 Steps 2-4; owner yes at each outward step

- [ ] **Step 1:** Stop the Mac's writers (a launchd/cron schedule if installed; the web app). Deploy the code
  (tar-over-ssh) and delete on the Mac the files the cycle removed, because tar does not delete:
  `src/lakehouse/parsers/declared.py`, `src/lakehouse/compute/migration.py`, `scripts/rebuild_precheck.py`,
  `tests/unit/test_bronze_retire.py`, `tests/unit/test_compute_migration_guard.py`,
  `tests/unit/test_rebuild_precheck.py`, `tests/e2e/test_reparse_retire.py`, `tests/e2e/test_migration_window.py`,
  `tests/support/retirekit.py`. Then `uv sync --locked`, and confirm `numba_active()` prints `True`.
- [ ] **Step 2 (owner yes):** record the non-secret settings (the modules and their filters), then delete `lake` and
  `lake-restricted`. That deletes everything under them: bronze, catalog, raw archive, runs, and `config/` (the
  settings and their `history/`, the operator account, the review `decisions` and `requests`).
- [ ] **Step 3:** `lakehouse init --lake <lake> --restricted <restricted> --profile staging` with the encryption key in
  the environment (`env.sh`); set the modules with `lakehouse config set` (recorded in
  `/Volumes/Development/lakehouse/clean-slate-config.sh`): the four pining modules; skillcorner and statsbomb (360 only)
  capped by competition at about 100 matches each. Re-create the operator account without the password entering the
  session: generate it on the Mac into an owner-only file and pass it with `--password-stdin`.
- [ ] **Step 4 (perf Task 9 Steps 2-4, the approved budgets):** state at the start (2026-11-06): run 1 (default
  18,000 s) ingested 233 of the 264 capped matches and computed none (0 `compute step` lines, 0 dbt builds; SIGTERM
  during the ingest), so every compute is still pending. Record per run the wall time, peak RSS
  (`/usr/bin/time -l`), lake size (`du`) and the `compute step` lines.
  - Phase A: `compute.budget_seconds 5800`, `lakehouse run` until the ingest is complete (the compute window is about
    100 s, so the compute defers). Record the ingest seconds per provider.
  - Phase B, the deadline rehearsal: `compute.budget_seconds 7200`, one `lakehouse run`. Expected (non-vacuity): at
    least one match written (`"compute match"` with `outcome=written`), `deferred_matches > 0` for a tracking step,
    more than one in-run publish, and the run ends `ok`. The cold first-match seconds come from its first
    `"compute match"` line per step (numba compiles there).
  - Phase C, resume and backfill: `compute.budget_seconds 18000`, `lakehouse run` until `deferred_matches == 0`
    for every step in `DEFAULT_COMPUTE_STEPS` (Amendment 2026-11-23, perf plan; perf Task 9 Step 4b completes this phase and closes this checkbox). Expected: no match computed twice (per-match logs). Record per-match seconds per provider
    and step.
- [ ] **Step 5: Verify** (spec §6 step 7): `catalog/CURRENT` records `encrypted: true` (the encrypted
  `LocalNativeWriter` path); `DESCRIBE` time columns are `BIGINT` with unit suffixes and no `INTERVAL`;
  `fct_tracking_ball.time_seconds` and statsbomb/skillcorner SPADL `time_seconds` spot-checked against raw values;
  no `compute_failed`. Report time and space per provider and the extrapolation to the full set (for example 909
  skillcorner matches). Restart the web app. Continue with perf Task 9 Steps 5-8.

### Task 8: HF rebuild from a clean slate (spec §6) — replaces perf Task 11 Steps 1-2; every step owner-gated

Read `project_v2_hf_production_topology` and `docs/ops/deploy.md` first; never probe ids. The perf plan's Task 10
review gate has passed before this task. The HF ops Space signs in with OAuth and an allow-list, so the HF lake has no
operator account to re-create.

- [ ] **Step 0 (TDD, local):** `lakehouse init --profile hf` on HF roots opens the owner's existing buckets
  (`open_storage`: the token and the lease ledger are checked), saves the `hf` defaults when no config exists, keeps
  an existing config, creates no root and writes no bootstrap file; a local profile on HF roots, and `--profile hf` on
  local roots, exit 2. Tests: `test_init_saves_the_hf_defaults_on_existing_buckets`,
  `test_init_profile_must_match_the_roots`, and the existing `test_init_profile_and_usage`.
- [ ] **Step 1 (owner):** suspend the scheduled job `lakehouse-ingest`; confirm (`HfApi().inspect_scheduled_job`).
  Pause the ops Space (`karstenskyt/ops`) so no config write lands during the empty.
- [ ] **Step 2:** record the HF lake's non-secret config (`lakehouse config show` as a one-off job, or the ops
  Settings page): the active modules and their filters.
- [ ] **Step 3:** Deploy the Job image and the ops Space code from the reviewed branch stage. The Space stays
  paused after its deploy (re-pause it if the upload restarts it) until Step 8.
- [ ] **Step 4 (owner yes, irreversible):** empty the `luxury-lakehouse/lake` and `luxury-lakehouse/restricted`
  buckets (the buckets themselves, their tokens and grants stay). KEEP the lease ledger repository
  `luxury-lakehouse/lake-control`: `open_storage` checks it, and the lease heals a kept ledger over an emptied bucket.
- [ ] **Step 5:** one-off `hf jobs run --flavor cpu-basic` with the scheduled job's image, env and secrets, command
  `sh -c "python -m lakehouse.mask -- lakehouse init --lake hf://buckets/luxury-lakehouse/lake --restricted
  hf://buckets/luxury-lakehouse/restricted --profile hf"`; then the Step 2 modules with `lakehouse config set` the same
  way. Expected: `"config": "saved"`, `config_version 1`, then one version per set. A `"config": "kept"`
  here means the Step 4 empty was incomplete (a config survived): stop and ask the owner; do not ingest.
- [ ] **Step 6: The fresh ingest and the perf calibration run.** One one-off `hf jobs run --flavor cpu-basic
  --timeout 21600` of `sh -c "python -m lakehouse.mask -- lakehouse run"` (the scheduled job's image, env and
  secrets). It ingests the lake and computes until its own deadline, so it leaves a compute backlog. Record its job id
  and keep its logs: perf Task 11 Step 3 calibrates from them. The nightly scheduled runs do the backfill (perf Task 11
  Steps 2 and 4).
  - Expected: the run ends `ok` inside 21,600 s; the ingest is complete (`counts.failed == 0`, no listed match left
    unparsed); every dbt build succeeds; the `"compute backend"` line says `numba: true`; no compute step fails
    (`failed_matches == 0`). Each compute step, the SPADL steps included (layer 1 has the earlier deadline), either
    writes matches or defers them (`deferred_matches > 0`): both pass.
  - Branch, the ingest did not finish (the job hit `--timeout`, or the run stopped): the ingest resumes. Run another
    one-off job of the same command, only until the ingest is complete. After a `--timeout` kill the lease is still
    held, so a job started within `LEASE_TTL` + `TAKEOVER_GRACE` (10 + 2 minutes, `storage/lease.py:83-85`) ends
    `skipped`: wait 12 minutes, or re-run it after a `skipped` result. The first of these runs whose compute starts
    with a backlog is the calibration run.
  - Branch, the ingest finished but no tracking match was computed (the ingest used the window): that is fine for the
    rebuild. The calibration moves to the perf Task 11 Step 2 run (a compute-only night with the whole backlog).
- [ ] **Step 7:** verify on the state Step 6 left. The checks that need no compute run now: the `encrypted` flag,
  `DESCRIBE` of the time columns, and `fct_tracking_ball.time_seconds` (a dbt view over bronze) against raw values. The
  SPADL `time_seconds` spot-checks run now if the SPADL steps wrote rows, else on the first nightly run after the
  resume (perf Task 11 Step 2), and the resume does not wait for them. Otherwise verify as Task 7 Step 5 (expect `encrypted: false`: an HF lake takes no key, its buckets are platform-encrypted, deploy.md "Encryption key at rest"; so it is the `RegisterWriter` path).
- [ ] **Step 8 (owner):** resume the ops Space and the schedule only after Step 7. Continue with perf Task 11 Step 3.

> **Note (2026-11-06):** the round 1-3 dispositions below are history. Rows about the retirement, the reparse
> rebuild, the migration guards and the pre-check are SUPERSEDED by the owner's clean-slate amendment.

## Dispositions (plan review round 1)

Reviewer A = `…-plan-A.md` (DUR-PLAN-nn). Reviewer B = `…-plan-B.md` (NODUR-PLAN-nn). They agree on every shared point.

| ID | Severity | Disposition |
|---|---|---|
| A-01 / B-01 HF rebuild not executable | SHOULD FIX | Fixed. New Task 8: owner suspend → read-only pre-checks → deploy → `reparse --all` jobs until done → verify → owner resume; the perf plan's Task 11 Steps 1-2 are replaced (perf "Amendment"). The archive-less branch is an explicit owner decision gate (Task 7 Step 1, Task 8 Step 2), as the spec sanctions a fresh ingest for that case. |
| A-02 Mac rebuild consumes perf Task 9 | SHOULD FIX | Fixed. Task 7 Steps 3-5 ARE perf Task 9 Steps 2-4 (deadline rehearsal = the rebuild reparse; resume; backfill); the perf plan says so. |
| A-03 / B-03 spec greps missing | SHOULD FIX / CONSIDER | Fixed. Task 6 Step 2 runs both greps over the spec's scopes with stated expectations. B's skillcorner SPADL spot-check is in Task 7 Step 6; statsbomb.py:285/306 nested keys are named in Task 1. |
| B-02 resolution reads a retired column | SHOULD FIX | Fixed. Spec §6 step 4 amended (resolution guard, `resolution_deferred_migration`); Task 5 implements and tests it; Task 6's grep re-checks for any other reader. |
| A-04 test data and call sites | CONSIDER | Fixed. `"01:02:03"`; skillcorner `match_clock_ms` source field `timestamp`; only `writer.py:123` calls `check_tracking_schema`. |
| A-05 GS `us` kind | CONSIDER | Fixed. Guard 2 checks the output column `timestamp_us` through `TRACKING_FRAMES_CORE`. |
| A-06 NULL parser_version | CONSIDER | Fixed. NULL counts as old; SQL uses `IS DISTINCT FROM`; tested. |
| A-07 ET fixture | CONSIDER | Fixed. Task 3: ET frames appended in memory to `tracking_9001.jsonl`'s bytes; the file is unchanged. |
| B note: the perf plan's hash differs from the approved r2 | — | Expected: the perf plan's round-2 CONSIDERs (A's `--step` and `-0.0`, B's `combine`) were applied after approval, plus this round's amendment of Tasks 9 and 11. Both are in this round's review package. |

## Dispositions (plan review round 2)

Reviewer B (`…-plan-r2-B.md`) APPROVE with 1 CONSIDER; Reviewer A (`…-plan-r2-A.md`) REQUEST CHANGES with 1 SHOULD FIX.
No disagreement.

| ID | Severity | Disposition |
|---|---|---|
| A r2 DUR-PLAN-08 a table with neither column | SHOULD FIX | Fixed, with a spec amendment (§6 steps 2-3, pending the owner): `RETIREMENTS` pairs; `retire_columns` drops the old AND adds the new nullable column with its declared type in one step; new test (dbt builds and views read after the step with no write); the stopped-reparse test stops right after that step, before any write. |
| A r2 09 rehearsal could test nothing | CONSIDER | Fixed. Task 7: the rebuild reparse first (parse seconds recorded), then the 7200 s rehearsal is a plain run with no parse work left, expecting at least one written match. |
| A r2 10 / B r2 04 pre-check script undefined | CONSIDER | Fixed. Task 6 creates `scripts/rebuild_precheck.py` (read-only, `CatalogReader`, archive-less lineage count via `RawArchive.locate`, version histogram, row counts) with a test; Task 8 runs it on the NEW image after the deploy, with the schedule still suspended. |
| A r2 11 status lines | CONSIDER | Fixed. The spec and perf-plan status lines record the post-approval amendments as pending the A/B re-review and the owner. |

## Dispositions (plan review round 3)

Both reviewers APPROVE (`…-plan-r3-{A,B}.md`). Their CONSIDERs are applied:

| ID | Disposition |
|---|---|
| A r3 12 Mac backfill could finish in Step 3 | Fixed. Task 7 Step 3 runs with `compute.budget_seconds 5800` (compute window ≤ ~100 s); the cold first-match seconds come from Step 4's first `"compute match"` line. |
| A r3 13 / B r3 05a step reference | Fixed. Task 8 Step 5 names the Step 3 pre-check. |
| A r3 14 type derivation | Fixed. Task 4: the new type comes from `relation_columns` over a registered empty Arrow table of the declared schema, as `reconcile_columns` does; one transaction, no `maybe()` inside. |
| B r3 05b triples vs pairs | Fixed. Spec §6 step 2 now says `(old, new)` pairs with the type derived from the declared schema. |
| B r3 05c pre-check halt | Fixed. Task 8 Step 3: the schedule stays suspended until the owner decides. |
| B r3 05d both writers | Fixed. Task 4's reparse-retire e2e is parametrised over both writers. |

## Dispositions (amendment review round 4)

Reviewer A = `D:\Development\_reviews\2026-11-06-v2-bronze-no-duration-spec-r4-A.md` (REQUEST CHANGES, 3 SHOULD FIX,
2 CONSIDER). Reviewer B = `…-spec-r4-B.md` (REQUEST CHANGES, 0 BLOCKING, 2 SHOULD FIX, 2 CONSIDER). Both found the code
removal complete. No disagreement between A and B.

| Finding | Sev | Disposition |
|---|---|---|
| A DUR-SPEC-15 / B NODUR-PLAN-06 the HF clean slate has no install route | SHOULD FIX | Fixed with the owner's decision ("Add HF to init", 2026-11-06): `lakehouse init --profile hf` on existing HF buckets saves the `hf` defaults, keeps an existing config, creates no root and no bootstrap file (`cli.py` `_init`); tests `test_init_saves_the_hf_defaults_on_existing_buckets`, `test_init_profile_must_match_the_roots` RED → GREEN. Task 8 rewritten: pause the ops Space, record the config, empty the buckets, keep `lake-control`, init as a one-off job, resume only after verify. Spec §6 and deploy.md say the same. The HF account point does not apply: the Space signs in with OAuth (`web/app.py:4-5`). |
| A DUR-PLAN-15 / B NODUR-PLAN-08 linked text still describes the removed route | SHOULD FIX | Fixed. Perf plan Task 9 Steps 2-4, Task 11 Steps 1-2, the Amendment and the status line; this plan's Status, Architecture, Spec line, Review Focus 4-5 and Task 6 Step 2. |
| A DUR-PLAN-16 the Mac rehearsal and backfill lost their approved budgets | SHOULD FIX | Fixed. Task 7 Step 4 restores Phase A 5800 s, Phase B 7200 s with the non-vacuity checks, Phase C 18,000 s with "no match computed twice". The running Mac loop (default 18,000 s) was stopped by SIGTERM after 233 of 264 matches and restarted as `fresh-ingest2.sh` with those phases. |
| B NODUR-PLAN-07 the fresh Mac lake's encryption is not verified | CONSIDER | Applied. Task 7 Step 5, spec §6 step 7 and deploy.md step 6 check `catalog/CURRENT` `encrypted`; the Mac lake records `true` (checked 2026-11-06). |
| B NODUR-PLAN-09 resume before verify; name the files tar does not delete | CONSIDER | Applied. Task 8 Step 8 resumes only after Step 7; Task 7 Step 1 names the nine removed files. |
| A DUR-PLAN-17 stale plan text; spec dispositions not marked | CONSIDER | Applied (with -15 above) and a superseded note before the old dispositions in the spec and this plan. |
| A DUR-SPEC-16 the irreversible step should list what it deletes and keep `lake-control` | CONSIDER | Applied. Spec §6 step 4, deploy.md step 3 and Tasks 7-8 list `config/history`, the account, `decisions` and `requests`, and keep `lake-control`. |
| B r5 NODUR-PLAN-10 (a) kept at Task 8 Step 5, (b) step refs, (c) Space paused after deploy | CONSIDER | Applied: Step 5 stops on `kept`; refs say §6 step 7; Step 3 keeps the Space paused until Step 8. |
| A r5 DUR-PLAN-18 the HF rebuild uses up the cpu-basic calibration run | SHOULD FIX | Fixed. Task 8 Step 6 is ONE cpu-basic one-off `lakehouse run` (the calibration run; it leaves a backlog; job id and logs kept); the nightly runs do the backfill. Perf Task 11 Step 1 and Step 3 say the same. |
| A r5 DUR-PLAN-19 / -20 step refs; Space paused after deploy | CONSIDER | Applied (same as B r5 -10). |
| A r5 DUR-PLAN-21 what "233 of 264" counts | CONSIDER | Applied. Task 7 Step 4 records it: 233 matches ingested, none computed (0 `compute step` lines, 0 dbt builds). |
| B r6 NODUR-PLAN-11 Task 8 Step 6 has no Expected or branch; a partly computed lake can stall Steps 7-8 | SHOULD FIX | Fixed. Step 6 states the Expected and two branches (ingest not finished: more one-off jobs until it is; no tracking match computed: calibration moves to perf Task 11 Step 2). Step 7 splits the checks that need no compute (run at once) from the SPADL spot-checks (now or on the first nightly run), so the resume never stalls. |
| A r7 DUR-PLAN-22 / B r7 NODUR-PLAN-12(a) the SPADL line of the Step 6 Expected is unconditional | SHOULD FIX | Fixed. Each compute step, SPADL included, either writes or defers; both pass; none fails. |
| A r7 DUR-PLAN-23 / B r7 -12(c) "ONE" and "the first run" wording | CONSIDER | Applied: "one, or more only until the ingest is complete" in Task 8 Step 6, spec §6 step 6, deploy.md step 5 and perf Task 11 Step 1; perf Step 3 points at the calibration run that Task 8 Step 6 names. |
| B r7 -12(b) the lease after a `--timeout` kill | CONSIDER | Applied: wait 12 minutes (LEASE_TTL + TAKEOVER_GRACE), or re-run after a `skipped` result. |
