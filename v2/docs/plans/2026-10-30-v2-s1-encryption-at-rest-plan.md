# v2 S1 — app-level encryption at rest (local lake) Implementation Plan (round 5)

> Round 3 resolved plan-round-2 B-13..18. Round 4 resolved plan-round-3 B-19 (owner: **rotation deferred, out of
> S1**) + B-20 (`PageConfig.reads_match_data`). Round 5 resolves plan-round-4 B-22: the self-declared
> `reads_match_data` flag is independently verified by running a `False`-declaring page's `refresh` against a
> recording services double (no match-scoped read) + a mutant. A APPROVED plan round 4 (sha 46e8a22a); B had 0
> BLOCKING and expected APPROVE once B-22 is in.


> **For agentic workers:** execute task-by-task with TDD (red → green). **Owner rules override the generic
> execution skill:** no per-task commits, no micro-commits, no subagents, no worktrees. The whole cycle is **one
> commit** after the A/B implementation review and the owner's explicit approval. Work on the feature branch
> `feat/v2-ingestion`. Steps use checkbox (`- [ ]`) tracking.

**Goal:** Encrypt an encrypted-local lake at rest — native parquet, catalog, raw deliveries, and spill — with an
operator-supplied, lakehouse-controlled key, approach A′, leaving unencrypted-local and HF unchanged.

**Architecture:** Three modes (spec "Local modes"): **encrypted-local** (key present) = A′ — one encrypted
DuckLake catalog, single root, DuckLake-native bronze writes, visibility in `ops_matches` + a `_visibility` row
column, raw-in-catalog, atomic single-catalog flip; **unencrypted-local** (no key) = today — `add_data_files`,
`place_root`, two roots, file-move flip, no `_visibility`; **HF** = today. The write/visibility/guard/raw paths
fork behind one `BronzeWriter` interface keyed off encrypted-local.

**Tech Stack:** Python 3.12 (`>=3.12,<3.13`), DuckDB 2.0.0 + ducklake, pyarrow, dbt-duckdb, Taipy UI; uv; ruff +
pyright strict + pytest + import-linter + pip-audit.

**Spec:** `docs/specs/2026-10-30-v2-s1-encryption-at-rest-design.md` (APPROVED by the owner and both reviewers,
2026-10-30).

## Global Constraints

- Three modes keyed off **encrypted-local** (`roots.lake.local_path is not None` AND the key present); never off
  "local" alone. HF and unencrypted-local are unchanged, including their bronze schema (no `_visibility`), their
  `add_data_files`/`place_root`/two-root layout, and their file-move flip.
- The key enters the app only through one resolver reading `LAKEHOUSE_ENCRYPTION_KEY`; the app never knows how
  the environment was populated and never writes the key to any file it produces (`profiles.yml`, logs,
  `target/`, a query result). The var name matches the scheduler secret guard `schedule/base.py:39`
  (`(^|_)(TOKEN|SECRET|PASSWORD|PASSWD|KEY)(_|$)`). Per-platform environment population is operator ops-config,
  not app code (owner decision 2).
- Key: fixed charset (hex or base64url) and length, validated before any SQL; ≥ `MIN_SECRET_LENGTH` (8); resolver
  raises its own message, never echoing the key; the attach SQL is never logged; the var is in
  `lakehouse.mask.SECRET_ENV_VARS`.
- Fail-closed both directions: encrypted lake + no/wrong key stops (DuckDB rejects); unencrypted lake + key set
  stops (via the plaintext `encrypted` flag), except the migration that encrypts the lake.
- `DATA_INLINING_ROW_LIMIT 0` at every writing attach (per-attach, not persisted).
- `init` on a local root with no key creates an unencrypted lake and warns loudly, naming the var (owner
  decision, 2026-10-30).
- No private bronze row to the public HF bucket; no public-context read returns a private row; no DuckLake-native
  write to an HF or unencrypted-local bronze table.
- Line length 120; ruff E,W,F,I,N,UP,B,S,BLE,RUF; pyright strict (whole project, not `src/` only); import-linter
  green; pip-audit clean.
- One commit per cycle, docs riding it; **no wheel bump and no `TODO.md`** — v2 has neither, and the owner set
  the v2 cycle-close as code + docs only (owner decision, 2026-10-30); the commit needs the owner's explicit
  approval; no self-commit.

## Review Focus

- **The three-mode selection** — a native bronze write reaching an HF or unencrypted-local bronze table, or the
  A′ path invoked without a key, breaks a backend or silently drops encryption. The selection is tested for all
  three modes (T8, T10).
- **The read-side choke point** — `scoped_reads` is a post-fetch Python filter with no production caller today,
  and `assert_bronze_registered_only` likewise has no production caller; (c) and the guard hold only once both
  are wired at the production path and `_row_visibility` reads the authoritative `_visibility` (T10, T11).
- **The migration is a transform, not a copy** — it must convert raw→BLOB, bronze→native+`_visibility`, seed the
  content hash and the ledger, or raw is lost and the first post-migration run trips the guard (T13).
- **Fail-closed both directions** and the migration exemption (T3, T13).
- **Key never on disk/in a result** — rendered `profiles.yml`, dbt logs, `target/`, `duckdb_databases().options`,
  a plist/unit file (T1, T5, T10).
- **Spike dead-ends** — the dbt-`MAP` spike (T5) and the spill spike (T7) each carry a fallback; the migration
  drops pre-migration restore points (owner decision 4), so there is no re-encrypt-history cost.

---

### Task 0: Baseline pin — gate green and the unchanged-mode oracle

**Files:** read the full gate; the unchanged-mode pin. **Implemented (2026-10-30, audit S1-IMPL-20): the pin is
`tests/unit/test_writer_mode_selection.py`** (the three-mode selection, including HF via composition), with the
`add_data_files`/`place_root`/`move_match_files` register path exercised by `test_bronze_writer.py` and
`test_match_writer.py`; there is no separate `test_s1_unchanged_modes_pin.py`. Every "Task 0 pin" reference below
means this pin.

**Interfaces:** Produces a recorded baseline (gate green) and a pin that HF **and unencrypted-local**
bronze use `place_root` + `ducklake_add_data_files` + `move_match_files` (no native INSERT, no `_visibility`), so
later tasks prove those two modes unchanged.

- [ ] **Step 1:** Run the full gate; record counts. Expected: green (M1 baseline 1147 passed/3 skipped).
- [ ] **Step 2:** Write the pin asserting the add_data_files/place_root/move path for an `hf://` Storage double
  and a local Storage double **with no key**. Run. Expected: PASS on today's code.
- [ ] **Step 3:** Record the baseline in the ledger.

---

### Task 1: Key resolver, validation, masking

**Files:** Create `src/lakehouse/storage/encryption.py`; Modify `src/lakehouse/mask.py`; Test
`tests/unit/test_encryption_key.py`.

**Interfaces:** Produces `ENCRYPTION_KEY_ENV: Final = "LAKEHOUSE_ENCRYPTION_KEY"`;
`resolve_encryption_key(roots: RootMap, env: Mapping[str, str]) -> str | None` (validated key when local + set +
valid; `None` when local + unset; `None` when `hf://`; raises `ConfigError` when set but invalid);
`validate_key(value: str) -> str`.

- [ ] **Step 1:** Failing tests: valid hex and base64url keys ≥ 8 resolve; a short key and an out-of-charset key
  each raise `ConfigError` whose message contains neither the key nor `ATTACH`/`METADATA_PARAMETERS`; unset on a
  local lake → `None`; `hf://` with the var set → `None`; `ENCRYPTION_KEY_ENV` matches the secret pattern;
  `ENCRYPTION_KEY_ENV in mask.SECRET_ENV_VARS`.
- [ ] **Step 2:** Run. Expected: FAIL (absent).
- [ ] **Step 3:** Implement; add the var to `SECRET_ENV_VARS`.
- [ ] **Step 4:** Run. Expected: PASS.
- [ ] **Step 5 (mutants):** (a) drop the `local_path is not None` gate → the `hf://` test returns the key → RED;
  (b) drop the charset check in `validate_key` → the out-of-charset test passes → RED. Revert both.

---

### Task 2: `init` warns and creates unencrypted on a keyless local root

**Files:** Modify `src/lakehouse/cli.py` (`init`), `src/lakehouse/storage/roots.py` (`init_lake`); Test
`tests/unit/test_init_warns_no_key.py`.

**Interfaces:** Consumes `resolve_encryption_key` (T1). Produces: `init` on a local root with no key creates an
unencrypted lake (today's behavior) and emits a loud, structured warning naming `LAKEHOUSE_ENCRYPTION_KEY` and
stating the lake is not encrypted; with the key it creates the encrypted-local lake and records `encrypted=True`
(T3).

- [ ] **Step 1:** Failing tests: `init` on a local root, no key → an unencrypted lake is created AND a warning
  record (kind `local_lake_unencrypted`) names the var; with the key → `encrypted=True` recorded; on an `hf://`
  root → no warning.
- [ ] **Step 2:** Run. Expected: FAIL.
- [ ] **Step 3:** Implement the warn-and-create branch.
- [ ] **Step 4:** Run. Expected: PASS.
- [ ] **Step 5 (mutant):** Drop the warning emit → the warning-record test → RED; revert.

---

### Task 3: The `encrypted` flag and both-direction fail-closed

**Files:** Modify `catalog/publish.py` (`roots_record`, `storage_roots`), `catalog/working.py` (`check_roots`,
`WorkingCatalog.open`); Test `tests/unit/test_encrypted_flag.py`.

**Interfaces:** Consumes T1. Produces: `roots_record(..., encrypted: bool)` adds a plaintext `encrypted` field;
`check_roots(..., migrating: bool = False)` (read before the attach, `working.py:139`) raises `GuardStopError`
when the record says `encrypted` and no key resolves, or says not-encrypted and a key resolves — `migrating=True`
exempts the unencrypted-source-with-key case.

- [ ] **Step 1:** Failing tests: a record round-trips `encrypted=True`; opening an `encrypted=True` lake with
  no/wrong key fails closed (DuckDB `CatalogException` → exit 2, message never echoes the key); opening an
  `encrypted=False` lake with a key raises `GuardStopError`; the same with `migrating=True` does not.
- [ ] **Step 2:** Run. Expected: FAIL.
- [ ] **Step 3:** Implement; map the DuckDB reject to exit 2.
- [ ] **Step 4:** Run. Expected: PASS.
- [ ] **Step 5 (mutant):** Remove the unencrypted-with-key branch → the mixed-lake test passes → RED; revert.

Note: the wrong-key sub-test depends on T4's `attach_options`; order T4 before this step or stub the attach.

---

### Task 4: Encrypted attach at every open site

**Files:** Modify `catalog/working.py` (`attach`), `catalog/reader.py` (:96), `catalog/restore.py` (:154),
`catalog/relocate.py` (:93); Test `tests/unit/test_encrypted_attach_sites.py`.

**Interfaces:** Consumes T1. Produces `attach_options(roots, env, *, mode) -> str` with a per-site shape:
`working` (new catalog) emits `DATA_PATH … , ENCRYPTED, DATA_INLINING_ROW_LIMIT 0, METADATA_PARAMETERS …`;
`reader` (READ_ONLY) emits `ENCRYPTED, METADATA_PARAMETERS …` (+ `OVERRIDE_DATA_PATH`/`DATA_PATH` only when
re-rooting); the plain-`duckdb.connect` sites (restore/relocate) add the key + `STORAGE_VERSION 'latest'`. No
`DATA_PATH` where the site must not set one.

- [ ] **Step 1:** Failing tests: each open site opens a key-created encrypted lake and reads; a wrong key is
  rejected at each; the reader does not pass `DATA_PATH` except when re-rooting; the plain sites pass
  `STORAGE_VERSION 'latest'`.
- [ ] **Step 2:** Run. Expected: FAIL.
- [ ] **Step 3:** Implement `attach_options` per-site; wire the four sites.
- [ ] **Step 4:** Run. Expected: PASS.
- [ ] **Step 5 (mutant):** Drop `DATA_INLINING_ROW_LIMIT 0` from the writing attach → a T8 small-write file-count
  assert → RED; revert.

---

### Task 5: The key to subprocesses — dbt and the scheduler

**Files:** Modify `runs/dbt.py`, `dbt/profiles.yml.j2`, `schedule/base.py`/`launchd.py`/`systemd.py` (env
pass-through); Test `tests/unit/test_dbt_profile_key.py`, `tests/unit/test_schedule_key_passthrough.py`.

**Interfaces:** Produces: the dbt subprocess receives the key via its **environment**; the profile does not render
the key. The scheduler never writes the key to a plist/unit file (the secret guard). Per-platform environment
population for scheduled runs is operator ops-config (launchd does **not** inherit a shell env — owner decision
2), documented in `docs/ops/deploy.md`; the app does not populate it.

- [ ] **Step 1 (spike, with fallback):** Confirm dbt-duckdb opens the encrypted local catalog. Primary: the dbt
  subprocess creates a **temporary** (session-scoped, never persisted) DuckDB `SECRET (TYPE ducklake, DATA_PATH …,
  ENCRYPTED, METADATA_PARAMETERS MAP {'encryption_key': env_key})` at startup and the profile attaches
  `ducklake:<secret>` — no inline `MAP`, no key in `profiles.yml`. Note (B-14): `duckdb_secrets().options` then
  holds the key, so the T11 AST gate must forbid `duckdb_secrets` on a keyed connection too, and this task greps
  the dbt log and `target/` for the secret value. Fallback A: render `METADATA_PARAMETERS` via dbt's `env_var`
  as a map if dbt-duckdb supports it. Fallback B (owner decision): if neither opens an encrypted catalog, dbt on
  an encrypted-local lake is unsupported and surfaced to the owner. Record which path works.
- [ ] **Step 2:** Failing tests: the rendered `profiles.yml`, `target/`, and the dbt log hold no fake key or
  secret value (grep a dbt invocation on a key-created encrypted lake); the written plist/unit file does not
  contain the key (`check_env` rejects the var name). Do **not** assert env inheritance by the scheduled child
  (launchd does not inherit a shell env — per-platform population is operator ops-config, out of the app).
- [ ] **Step 3:** Run. Expected: FAIL.
- [ ] **Step 4:** Implement the dbt secret/attach and the scheduler's secret-guard handling; document per-platform
  env population as operator ops-config in `docs/ops/deploy.md`.
- [ ] **Step 5:** Run. Expected: PASS. **Mutants:** render the key literal into `profiles.yml` → the grep test
  RED; make `check_env` accept the key var → the plist/unit-file test RED. Revert both.

---

### Task 6: Raw deliveries encrypted (spike then implement)

**Files:** Modify `raw/archive.py` (`RawArchive`, `raw_key`, `RAW_PREFIX`), `catalog/moves.py`; Test
`tests/unit/test_raw_encryption.py`.

**Interfaces:** Produces: on an encrypted-local lake, raw deliveries are DuckLake-native rows (a
content-addressed BLOB table) with their public/private classification; the local raw move does not run (one
root). Unencrypted-local and HF raw are unchanged.

- [ ] **Step 1 (spike, with fallback + surface):** Prove a native BLOB under `ENCRYPTED` holds a provider-scale
  raw object with no cleartext on disk and reads back; pin per-BLOB handling. Fallback: a vetted AEAD over the
  `raw/<sha256>.zst` files with the same key (record the ruling). If neither is feasible, stop and surface to the
  owner — never leave private raw plaintext silently.
- [ ] **Step 2:** Failing tests: a private raw delivery on an encrypted-local lake is not cleartext on disk and
  reparses; the local raw move is skipped; the public/private tag survives; unencrypted-local/HF raw unchanged.
- [ ] **Step 3:** Run. Expected: FAIL.
- [ ] **Step 4:** Implement the chosen mechanism.
- [ ] **Step 5:** Run. Expected: PASS. **Mutant:** store raw outside the catalog on encrypted-local → the
  cleartext grep → RED; revert.

---

### Task 7: Spill encryption

**Files:** Modify `catalog/duck.py` (keyed-connection setup); Test `tests/unit/test_spill_encryption.py`.

- [ ] **Step 1 (spike, positive control + fallback):** First establish a **positive control** — prove a marker
  IS detectable in a plaintext spill (so the oracle is reliable; a lone grep can pass on plaintext because DuckDB
  may not keep a contiguous marker — B spike9). Use a reliable oracle (a known marker shape, or a spill-file
  format/header check). Then prove `SET temp_file_encryption = true` hides it. Fallback (spec-sanctioned): if the
  setting cannot be shown to encrypt spill, set a dedicated `temp_directory` cleaned on exit and surface the named
  residual to the owner (confirmation, not a silent residual).
- [ ] **Step 2:** Failing test (positive control passes on plaintext; keyed run hides the marker).
- [ ] **Step 3:** Run. Expected: FAIL (setting not applied).
- [ ] **Step 4:** Set `temp_file_encryption = true` for keyed connections + a cleaned `temp_directory`.
- [ ] **Step 5:** Run. Expected: PASS. **Mutant:** drop the setting → the keyed-run oracle → RED; revert.

---

### Task 8: The `BronzeWriter` seam and mode selection (native writes for encrypted-local)

**Files:** Modify `bronze/writer.py` (`BronzeWriter` protocol, `LocalNativeWriter`, the current path as
`RegisterWriter`), `bronze/listings.py`, `runs/history.py` (a per-match content hash on `ops_matches`); Test
`tests/unit/test_writer_mode_selection.py`, `tests/unit/test_local_native_writer.py`, re-run the Task 0 pin.

> **RULING (implementation, 2026-10-30, audit S1-IMPL-13): the per-match `content_version` column on `ops_matches`
> was NOT built.** It is moot: change detection runs earlier and mode-independently, off the lineage change
> signals and content hashes (`runs/run.py` `_match`: `same_signals` on `newest[artifact]`, then `same_content` on
> each delivery's `content_sha256`), so a re-parse is already skipped before any writer is chosen. Adding a column
> no code reads would be dead schema. The encrypted-local "same content → skip" path is therefore the same
> lineage-signal skip as every other mode; the native writer adds no second content check.

**Interfaces:** Consumes T4. Produces: `select_writer(cat, roots, env) -> BronzeWriter` returns `LocalNativeWriter`
only for encrypted-local, else `RegisterWriter` (today's pyarrow + `store_written_file` +
`ducklake_add_data_files` + `place_root`, byte-unchanged for unencrypted-local AND HF). `LocalNativeWriter`
writes rows with `INSERT`; swap = delete-by-`match_key` + `INSERT`. **Change detection is confined to
encrypted-local** (B-16): there the filename `content_version` is gone, so `LocalNativeWriter` persists the
**same per-match `content_version` the writer computes today** (the value `write_bronze_file` puts in the
filename) as a column on `ops_matches` and skips a re-write when it is unchanged. Unencrypted-local and HF
(`RegisterWriter`) keep today's filename `content_version` detection **unchanged** — no `ops_matches` content
column, no behavior change.

- [ ] **Step 1:** Failing tests: `select_writer` returns `LocalNativeWriter` for encrypted-local and
  `RegisterWriter` for unencrypted-local and HF; on an encrypted-local lake a bronze write makes a native parquet
  with no cleartext and reads back; a same-content re-write is skipped by the `ops_matches` content_version and a
  changed re-write re-parses; a swap replaces the match's rows. The Task 0 pin still passes, and a
  **detection-outcome pin** (A-PLAN-06) asserts unencrypted-local/HF skip a same-content re-list and re-parse a
  changed one exactly as today.
- [ ] **Step 2:** Run. Expected: FAIL.
- [ ] **Step 3:** Extract `BronzeWriter`; implement `LocalNativeWriter` + `select_writer`; persist the per-match
  `content_version` on `ops_matches` for the encrypted-local skip check (register modes keep the filename check);
  repoint every `parse_file_name`/`match_of` consumer on the encrypted-local path (`bronze/writer.py`,
  `catalog/moves.py`, `runs/match_writer.py`, `catalog/restore.py`, `swap_files`, `ops_match_files`).
- [ ] **Step 4:** Run. Expected: PASS.
- [ ] **Step 5 (mutant):** `select_writer` returns `LocalNativeWriter` for unencrypted-local too → the Task 0 pin
  → RED; revert.

---

### Task 9: The `_visibility` column, the atomic flip, and the visibility ledger

**Files:** Modify `bronze/schemas.py` (the encrypted-local `_visibility` audit column), `bronze/writer.py`/
`listings.py`, `runs/match_files.py` (`move`), `runs/history.py`; Create the ledger (`ops_visibility_changes` or
reuse `ops_move_records`); Test `tests/unit/test_visibility_column.py`, `tests/unit/test_visibility_ledger.py`.

**Interfaces:** Consumes `LocalNativeWriter` (T8). Produces: encrypted-local bronze/listing tables carry a
non-null `_visibility`; `MatchFiles.move` on encrypted-local = `UPDATE` `ops_matches` (`set_match_visibility`) +
`UPDATE` `_visibility` + a **ledger record** (`match_key`, `new_visibility`, `at`, `run_id`; no file keys), one
transaction; on unencrypted-local and HF = today's `move_match_files`. Invariant: a match-scoped encrypted-local
row's `_visibility` equals `ops_matches.visibility`. The ledger is **defined here** and consumed by T12.

- [ ] **Step 1:** Failing tests: an encrypted-local write sets `_visibility` from the source flag / `ops_matches`;
  a flip updates `ops_matches` + `_visibility` + writes one ledger record in one transaction (a simulated crash
  between leaves no split); `_visibility` is absent on unencrypted-local and HF tables; the equality invariant
  holds; the ledger round-trips.
- [ ] **Step 2:** Run. Expected: FAIL.
- [ ] **Step 3:** Add `_visibility` to the encrypted-local bronze schema; set it on write; define the ledger;
  fork `MatchFiles.move` by mode.
- [ ] **Step 4:** Run. Expected: PASS.
- [ ] **Step 5 (mutant):** Split the flip into two transactions → the crash test finds a mixed state → RED;
  revert.

---

### Task 10: The guard wired at the production seam (all three modes)

**Files:** Modify `bronze/writer.py` (`assert_visibility_integrity`), `catalog/working.py` or the writer seam
(the production call point), retaining `assert_bronze_registered_only`; Test
`tests/unit/test_guard_wired.py`.

**Interfaces:** Produces: the guard runs in production. At the commit/writer seam, `RegisterWriter`
(unencrypted-local + HF) runs `assert_bronze_registered_only` (native-write prohibition — a bug in those modes);
`LocalNativeWriter` (encrypted-local) runs `assert_visibility_integrity(cat)` (every encrypted-local bronze row
`_visibility` non-null and, match-scoped, equal to `ops_matches`). Both run on every write/commit, not only in a
test.

- [ ] **Step 1:** Failing tests, wired through the production write/commit path: (HF and unencrypted-local) a
  DuckLake-native bronze write raises `GuardStopError`; (encrypted-local) a null `_visibility` row and a
  `_visibility`≠`ops_matches` row each raise. Assert the guard is invoked by the production path, not only
  callable.
- [ ] **Step 2:** Run. Expected: FAIL (guard not wired today — no production caller).
- [ ] **Step 3:** Wire both guards at the seam, selected by mode.
- [ ] **Step 4:** Run. Expected: PASS.
- [ ] **Step 5 (mutant):** Remove the guard call from the seam → both the HF-native-write and the
  encrypted-local-integrity tests → RED (the mutant now bites because the test runs through production); revert.

---

### Task 11: The read side — authoritative filter, route test, key-leak gate, dbt views

**Files:** Modify `services/authz.py` (`_row_visibility`), `ui/template.py` (`PageConfig.reads_match_data`),
`ui/pages/monitoring.py` (and the other 7 pages' `CONFIG`: `reads_match_data`), `ui/shell.py`, the dbt
silver/gold models that read bronze; Create `tests/unit/test_row_visibility_field.py`,
`tests/unit/test_anon_routes_seam.py`, `tests/unit/test_no_key_introspection_on_keyed.py`,
`tests/unit/test_dbt_visibility_carry.py`.

**Interfaces:** Produces: `_row_visibility` reads `_visibility` when the row has it (encrypted-local
bronze/listing), else `visibility` (`ops_matches`, `dim_*`). "Carries no match identity" is defined mechanically
by a **new `reads_match_data: bool = True` field on `PageConfig`** (`ui/template.py:150-159`) — no declared-reads
mechanism exists today (B-20), so one is added, default-safe (True). A route carries no match identity iff its
page's `reads_match_data is False`; `monitoring` sets it `False` (it carries no match identity, `shell.py:115`).
The standing test asserts every `ANON_VIEWABLE_ROUTES` route has `reads_match_data is False`, plus an
**independent** test that a page declaring `reads_match_data=False` truly reads no match data (so the
self-declared flag cannot lie): run its `refresh` against a **recording services double** and assert no value it
reads or binds carries `match_key`/`native_match_id`/a match-scoped bronze column. dbt silver/gold models that read encrypted-local bronze **CARRY** `_visibility` (they do not
filter private rows out — filtering is the read seam's job at `scoped_reads`), so an operator read sees a private
row and a public read does not; the local-vs-HF difference is stated (HF relies on the bucket split,
encrypted-local on the column). The AST gate forbids `duckdb_databases` **and `duckdb_secrets`** on a keyed
connection (B-14: the temporary dbt secret makes `duckdb_secrets().options` hold the key).

- [ ] **Step 1:** Failing tests: a listing row with source `visibility='public'` but a private match is dropped
  by `visible()` (reads `_visibility`); a bronze data row with no source `visibility` but `_visibility='public'`
  is returned; a **standing** test over the whole `ANON_VIEWABLE_ROUTES` set that each route's page has
  `reads_match_data is False`; an AST test that no `src/`
  reads `duckdb_databases` or `duckdb_secrets` on a keyed connection; a dbt test that a silver/gold model over
  encrypted-local bronze **carries** `_visibility` — an operator-scope read returns a private match's row and a
  public-scope read of the same model does not.
- [ ] **Step 2:** Run. Expected: the field-preference and dbt tests FAIL; the route and AST gates PASS on the
  clean tree (they are standing gates).
- [ ] **Step 3:** Change `_row_visibility` to prefer `_visibility`; add the standing route test, the AST gate
  (`duckdb_databases` + `duckdb_secrets`), and the dbt `_visibility` carry (not a filter); add
  `PageConfig.reads_match_data` (default True) and set the 8 pages' values (`monitoring=False`). **Implemented
  (2026-10-30, audit S1-IMPL-03/A): only `monitoring` sets `reads_match_data=False` explicitly; the other 7
  operator pages intentionally rely on the fail-closed default `True` (a page that forgot to declare is treated as
  reading match data, never anonymously viewable). The standing route test over `ANON_VIEWABLE_ROUTES` and the
  recording-double test enforce correctness regardless of which pages declare it.**
- [ ] **Step 4:** Run. Expected: PASS.
- [ ] **Step 5 (mutants):** `_row_visibility` reads the source `visibility` → the leak test RED; returns `None`
  without the `_visibility` fallback → the over-restriction test RED; add a match-reading route to
  `ANON_VIEWABLE_ROUTES` → the standing route test RED; add a `duckdb_databases` or `duckdb_secrets` call on a
  keyed connection → the AST gate RED; make the dbt model filter private rows out instead of carrying
  `_visibility` → the operator-sees-the-row test RED; give `monitoring.refresh` a match-scoped read while its
  `reads_match_data=False` → the recording-double test RED. Revert all.

---

### Task 12: Restore re-apply and the allowlist adaptation

**Files:** Modify `catalog/restore.py` (encrypted-local re-apply from the ledger), `runs/dbt.py`
(`NATIVE_TABLE_ALLOWLIST` bronze-aware for encrypted-local); Test `tests/unit/test_restore_reapply.py`,
`tests/unit/test_allowlist_local_bronze.py`.

**Interfaces:** Consumes the ledger (T9). Produces: `restore` reads the latest visibility per match from the
ledger in the pre-restore `CURRENT` and re-applies it before publishing (encrypted-local: `UPDATE` `_visibility`
+ `set_match_visibility`; unencrypted-local + HF: the existing physical re-placement); the §23.6 allowlist check
treats bronze as expected-native **only on encrypted-local**.

- [ ] **Step 1:** Failing tests: restore a version from before a public→private flip on an encrypted-local lake →
  the match reads private (`_visibility` + `ops_matches`); the allowlist check passes with bronze native on
  encrypted-local and still fails a non-allowlisted dbt-materialized native table; on unencrypted-local the
  allowlist check still rejects a native bronze file (that mode has none).
- [ ] **Step 2:** Run. Expected: FAIL.
- [ ] **Step 3:** Implement the re-apply before publish; make the allowlist check mode-aware.
- [ ] **Step 4:** Run. Expected: PASS.
- [ ] **Step 5 (mutant):** Skip the re-apply → the restored pre-flip match reads public → RED; revert.

---

### Task 13: The migration (transform) and the relocate refusal

> **AMENDED (owner, 2026-10-30): the migration is DEFERRED to a future cycle; see the spec Non-goals.** Only the
> relocate refusal ships in S1, and it is **narrowed**: `relocate` refuses an encrypted-local lake for **any**
> destination (`→ hf://` permanently; a new local path raises a clear "S1 does not relocate an encrypted lake"
> error), because the keyed local relocate shares the deferred migration's keyed-plain-open machinery. NOT built in
> S1: `catalog/encrypt_migrate.py`, the `lakehouse encrypt` CLI, `test_encrypt_migration.py`, and the keyed local
> relocate (`STORAGE_VERSION 'latest'` plain open). The `migrating` fail-closed exemption seam is kept dormant.
> `test_relocate_refuses_encrypted_hf.py` covers the refusal. The rest of this task below is the future-cycle plan.

**Files:** Create `src/lakehouse/catalog/encrypt_migrate.py`; Modify `cli.py` (`lakehouse encrypt`),
`catalog/relocate.py`; Test `tests/unit/test_encrypt_migration.py`,
`tests/unit/test_relocate_refuses_encrypted_hf.py`.

**Interfaces:** Consumes the `migrating` exemption (T3), the encrypted attach (T4), `LocalNativeWriter` (T8), the
ledger (T9). Produces: `encrypt_migrate(storage, *, operator) -> MigrationRecord` — **transforms** an
unencrypted-local (two-root, `add_data_files`, no `_visibility`) lake into the encrypted-local (A′) model: raw
files → BLOB rows, bronze → native + `_visibility` set from the current visibility, the per-match content hash
seeded, the visibility ledger seeded from `ops_move_records`; opens the source under the `migrating` exemption;
repoints `CURRENT` atomically. **Pre-migration restore points are dropped** (owner decision 4): the migration
starts the encrypted lake fresh from the transformed `CURRENT`, and removes old plaintext (catalog, bronze
files, raw files, restricted root, **and the pre-migration version + manifest objects**) **only after** the new
`CURRENT`. `relocate` raises `GuardStopError` on an encrypted-local → `hf://` move. Rotation/re-key of an
already-encrypted lake is **out of S1 scope** (deferred, owner decision; spec Non-goals) — `encrypt_migrate`
handles an unencrypted source only.

- [ ] **Step 1:** Failing tests: migrate an unencrypted-local lake → bronze is native + encrypted with
  `_visibility` matching the pre-migration visibility, raw is BLOB + encrypted (old raw files gone), the ledger
  is seeded, `CURRENT` repoints to the single transformed version, old plaintext (incl. the restricted root and
  the pre-migration version/manifest objects) is gone only after the new `CURRENT`, a **post-migration run passes
  the T10 guard**; a **pre-migration version is NOT restorable** afterward (its objects are gone, and `restore`
  of it raises `FileNotFoundError`/`GuardStopError`); `relocate` of an encrypted-local lake to `hf://` raises.
- [ ] **Step 2:** Run. Expected: FAIL.
- [ ] **Step 3:** Implement the transform, the fresh-CURRENT drop of pre-migration versions, and the relocate
  refusal.
- [ ] **Step 4:** Run. Expected: PASS. **Mutants:** delete old plaintext before the new `CURRENT` → a crash test
  finds an unrecoverable state → RED; skip the `_visibility` seeding → the post-migration guard → RED; allow the
  relocate → its test → RED. Revert.

---

### Task 14: Oracle fork and the full gate

**Files:** Modify `tests/e2e/test_no_private_rows.py` (forked by mode); the full gate.

- [ ] **Step 1:** Fork the oracle: HF **and unencrypted-local** keep checks 1–4 (private bronze in the restricted
  root/bucket only; no native bronze file); encrypted-local asserts (a) a cleartext grep of the data path,
  catalog, raw store, and a keyed-run spill dir finds no private marker, (b) no public-materialized table holds
  row-level data of a private match (a metadata/`dim_*` row listing a private match is allowed), (c) the route
  gate / `scoped_reads` returns no private row; checks 1–3 (allowlist) retained adapted for bronze-native on
  encrypted-local.
- [ ] **Step 2:** Run the forked oracle. Expected: PASS in all three modes.
- [ ] **Step 3:** Run the full gate (`ruff`, `ruff format --check`, `uv run pyright` whole-project,
  `lint-imports`, `pytest -m "not live and not hf and not bench"` `LAKEHOUSE_OS_TESTS=1`, `scripts/pip_audit.py`).
  Expected: green. Record counts.
- [ ] **Step 4:** No wheel bump and no `TODO.md` (owner decision: v2 cycle-close is code + docs only). The spec,
  this plan, the audit-ledger update, and any ADR ride the single cycle commit.

---

## Self-Review

- **Spec coverage:** key resolver (T1), init default (T2, owner), `encrypted` flag + both-direction (T3), attach
  sites + `DATA_INLINING 0` (T4), key to dbt + scheduler (T5), raw (T6), spill (T7), writer seam + three-mode +
  identity (T8), `_visibility` + atomic flip + ledger (T9), guard wired both backends (T10), read side + route
  test + duckdb_databases + dbt views (T11), restore re-apply + allowlist (T12), migration-transform + relocate
  refusal (T13), oracle fork + gate, no wheel/TODO (T14). Every spec section, every "Left to the plan stage"
  item (init, per-platform env, raw, spill, dbt MAP, re-encrypt, relocate refusal, choke-point gate), and both
  reviewers' plan findings map to a task. HF-native-write unification remains an owner decision (not in scope).
- **Three-mode selection** is explicit (`select_writer`, T8) and guards/`_visibility`/one-root/raw all key off
  encrypted-local; the Task 0 pin + T8/T10/T14 prove unencrypted-local and HF unchanged.
- **Type/interface consistency:** `BronzeWriter` (T8) → T9/T10; the ledger (T9) → T12; the `migrating` exemption
  (T3) → T13; `resolve_encryption_key` (T1) → T2/T3/T4; `attach_options` (T4) → T5/T8/T13.
- **No-production-caller gaps closed:** `assert_bronze_integrity` (dispatching `assert_bronze_registered_only` /
  `assert_visibility_integrity`) is wired at the production write/commit seam (`match_writer.write`, the listing
  commit) and tested through it (T10). **Corrected after review (S1-IMPL-10, 2026-10-30):** `scoped_reads` is NOT
  production-wired, because no anonymous route reads match rows today — `ANON_VIEWABLE_ROUTES` is `{monitoring}`,
  which carries no match identity (enforced by `test_anon_routes_seam.py`), and operators see every row. The
  enforced public-read guards today are the route gate plus the bronze `_visibility` column; `scoped_reads`
  reading the authoritative `_visibility` is the ready seam for a future anonymous page that needs match rows (the
  spec Goals state it conditionally, "when an anonymous page needs match rows"). It is unit-tested, not yet called
  in production.
- **Spike dead-ends** each carry a fallback or a stop-and-surface (T5 dbt, T6 raw, T7 spill); T13 drops
  pre-migration restore points (owner decision 4), no re-encrypt cost.
- **Proportion:** signatures, test names, spec values; no code bodies.

## Execution handoff

Native execution in this session, then a two-reviewer A/B implementation review, then the owner's explicit
approval, then one commit (code + docs; no wheel, no `TODO.md`). No per-task commits, no subagents, no worktrees.
