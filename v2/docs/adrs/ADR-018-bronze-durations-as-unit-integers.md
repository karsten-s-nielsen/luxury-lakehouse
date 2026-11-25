# ADR-018: Bronze stores no duration type; time is an int64 with a unit suffix

## Status

Accepted (2026-11-06). Spec `docs/specs/2026-11-06-v2-bronze-no-duration-design.md` and plan
`docs/plans/2026-11-06-v2-bronze-no-duration-plan.md`, both A/B-reviewed and owner-approved. Owner amendment
(2026-11-06): the rebuild is a clean slate; the in-place migration route of the reviewed spec was removed (the
amendment is A/B-approved round 8 and owner-approved 2026-11-06). Uncommitted, pending the owner's commit approval.

## Context

- Bronze held 16 time fields as Arrow `pa.duration` (scalar columns and fields inside nested lists of structs).
- The physical type of a duration depends on the bronze WRITE PATH. On a register-path lake (`RegisterWriter`, parquet
  registered with `ducklake_add_data_files`) it reads back as `BIGINT`. On an encrypted lake (`LocalNativeWriter`,
  native inserts) it reads back as `INTERVAL`. A pandas read gives a `Timedelta` on one path and an integer on the
  other.
- Every consumer therefore broke on one of the two paths (measured): `cast(INTERVAL as bigint)` is not implemented, so
  `fct_tracking_ball` failed on encrypted lakes, and `epoch()` failed on register lakes. The statsbomb SPADL adapter
  handed an int64 of milliseconds to `pd.to_timedelta` without a unit, which reads NANOSECONDS: every action time on a
  register-path lake was 10^6 too small (2.25 s became 2.25e-06 s), silently.
- The fixtures and most tests ran on one write path, so each consumer was fixture-green and real-red.

## Decision

1. **No duration anywhere in bronze.** Every time is an exact `int64` in a stated unit, and the column name carries
   the unit: `_us` (microseconds), `_ms` (milliseconds) or `_s` (seconds). The JSON and CSV time converters
   (`parsers/jsonmap.py` `TIME_KINDS`, `parsers/csvmap.py` `clock_ds`) return integers and suffix the column; a nested
   time key inside a struct is renamed with its suffix (`cards[].time` -> `time_s`). The code-built tracking time
   columns are declared: `timestamp_us` in the shared core schema, `match_clock_ms` in
   `TRACKING_FRAMES_EXTRA["skillcorner"]`, which `check_tracking_schema` enforces per provider.
2. **Consumers convert with an explicit unit**: `timestamp_us / 1e6` in `fct_tracking_ball`;
   `pd.to_timedelta(timestamp_ms, unit="ms")` in the statsbomb SPADL adapter; `time_start_ms` formatted as `M:SS.sss`
   for the skillcorner converter.
3. **Guards.** Guard 1: no parser declaration, CSV column set, shared or compute bronze schema holds a duration,
   nested types included. Guard 2: every time-kind column carries its unit suffix, and the code-built tracking time
   columns are declared `int64`. Guard 3: an e2e ingests the same matches through BOTH writers and asserts equal
   `fct_tracking_ball.time_seconds` and statsbomb/skillcorner SPADL `time_seconds`, each also equal to a known source
   value (`00:00:02.250` -> `2.25`). Guard 4: the tracking encoding lists name real core columns.
4. **The PARSER_VERSION of the four affected parsers is bumped** (statsbomb, idsse, skillcorner, gradientsports):
   their bronze output changed.
5. **Rebuild from a clean slate, never in place** (owner 2026-11-06). The v2 lakes hold test data and v2 is built to
   be installable from a clean slate, so a lake whose bronze shape changes is deleted and installed fresh (`init`,
   config, a fresh `lakehouse run`). There is no column retirement, no reparse-in-place and no migration guard; schema
   evolution stays additive-only (`bronze/evolve.py`). An HF lake installs the same way: the owner's emptied
   buckets get their first config from `lakehouse init --profile hf` (it creates no bucket and never overwrites a
   config), added for this rebuild.

## Consequences

- One value on both write paths; a consumer can no longer be right on one lake and wrong on the other.
- Each lake is rebuilt once from a clean slate in the order of `docs/ops/deploy.md` "Bronze schema rebuild
  (durations)": stop the writers, delete the lake roots (the owner's yes), install, ingest fresh, verify.
- The bronze column names change (`timestamp` -> `timestamp_us`, and so on): every reader of an old name is moved in
  the same change (dbt sources and staging, SPADL adapters, the gold docs).
- A lake on the old shape cannot take a write from the new code (additive-only evolution raises `IntegrityError`);
  that is loud, and the answer is the clean-slate rebuild.

## Alternatives rejected

- **A compile-time dbt macro that reads either type.** It hides the write-path split instead of removing it, and it
  cannot help a pandas consumer (the statsbomb 10^6 error was in Python, not SQL).
- **Normalise durations in the writer.** The writer would have to know every nested time path and rewrite it per
  path, and the parquet would still carry a type whose meaning depends on how it was registered. Declaring the unit
  integer at the parser keeps one representation from the first byte.
- **Additive migration (keep the old columns, add the new ones).** The same-name nested columns change type, which
  additive evolution cannot express, and keeping the renamed old columns would leave two meanings of one field.
- **In-place migration (drop-first retirement inside `reparse`, compute and resolution migration guards).** Built and
  then removed (owner 2026-11-06): it adds permanent code and a migration window to update test data that a clean
  slate rebuilds in one step.
