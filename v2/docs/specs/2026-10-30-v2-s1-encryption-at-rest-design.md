# v2 S1 — app-level encryption at rest for the local lake design

**Status:** APPROVED by the owner 2026-10-30. Reviewers A and B both APPROVE (A round 5 + a round-6 confirm; B
round 6). Part of the gold-standard pre-commit program (audit ledger
`docs/audits/2026-10-30-v2-audit-ledger.md`, finding S1). Approach **A′**. The implementation plan is
`docs/plans/2026-10-30-v2-s1-encryption-at-rest-plan.md`. This supersedes the earlier `ducklake_add_data_files`
approach (proven infeasible). A later clarification (plan review, 2026-10-30) makes the mode boundary explicit
(see "Local modes"); it does not change the approved design.

## Intent and why

A **local** lake stores its bronze parquet, its DuckDB catalog, and its raw deliveries as plain files on disk,
unencrypted (audit finding S1, CWE-311). Private matches are Confidential-class, so at-rest encryption is wanted,
not only access control. The hosted paths are already covered: HF Storage Buckets are AES-256 at rest (platform)
and Lakebase is provider-encrypted. So this cycle encrypts the **local** lake only. The owner's bar: gold
standard, the key controlled by the lakehouse (not the operating system — FileVault and BitLocker are ruled out).

## Owner scope decisions (2026-10-30)

1. **Raw deliveries are encrypted too.** All local raw is encrypted at rest with the same lakehouse key.
2. **Key delivery is platform-agnostic.** The application resolves the key through one seam and **never knows how
   it was retrieved**. It reads the key only from its process environment. Platform tooling populates that
   environment **outside** the application (for example the macOS standard keychain toolchain, or a Windows
   user/machine-scope environment variable). The environment variable is the accepted source **for now**; a
   different retrieval can be plugged in later behind the same seam without changing application code. The
   application never writes the key to any file it produces.
3. **An encrypted local lake need not relocate to HF.** Moving a lake between local and HF is not a requirement;
   each lake is populated directly where it belongs. So `relocate` refusing an encrypted-local → `hf://` move is
   accepted, and S1 ships with no decrypt → re-split path. Publishing datasets or model cards to HF later is a
   separate use case, not this relocate.
4. **The encryption migration drops pre-migration restore points.** Converting an unencrypted-local lake to the
   encrypted A′ model is a clean one-time break: versions created before the migration are not restorable
   afterward (their old-model files are deleted and cannot carry the A′ model). The operator keeps a copy of the
   old lake folder before migrating if a pre-encryption snapshot is wanted.

## Decision history and evidence (seven throwaway spikes, DuckDB 2.0.0 + ducklake; not committed)

1. `ducklake_add_data_files` **cannot register an encrypted parquet** (three ways: an `ENCRYPTED` catalog rejects
   it; a DuckDB-encrypted file errors "encryption_config was not set"; the function has no `encryption_config`
   parameter in any form). Reviewers A and B each reproduced the first case.
2. `ENCRYPTED` encrypts **only** DuckLake-native-written parquet, not externally written files.
3. **No per-schema data path** (fact pinned by its decisive commands):
   `CALL lake.set_option('data_path', '<p>', schema => 'restricted')` → `Not implemented Error: Unsupported
   option data_path`; `CREATE TABLE … WITH (data_path = '<p>')` → the same. `DATA_PATH` is a catalog-level
   `ATTACH` option only. So one catalog writes all native data under one root.
4. **Two encrypted catalogs cannot move a match atomically** (fact pinned by its decisive command): with `lake`
   and `restricted` both attached, `INSERT INTO lake … ; DELETE FROM restricted …` in one transaction →
   `TransactionException: a single transaction can only write to a single attached database`. A non-atomic
   cross-root move risks leaving a now-private match in the public root.
5. One `ENCRYPTED` catalog with `METADATA_PARAMETERS MAP {'encryption_key': <key>}` encrypts both the native
   parquet and the catalog file; reopen with no/wrong key is REJECTED. A single-catalog `UPDATE` is atomic.

Facts 3 and 4 establish that A′ (one encrypted catalog, visibility by metadata) is the **only** supported
approach that encrypts the local data and keeps the visibility change atomic. They are also recorded in the
durable memory `reference_v2_ducklake_encryption_constraints`.

## Verified mechanism

A single local DuckLake catalog is encrypted with one `ATTACH`:

```sql
ATTACH 'ducklake:<catalog>' AS lake
  (DATA_PATH '<data>', ENCRYPTED, DATA_INLINING_ROW_LIMIT 0,
   METADATA_PARAMETERS MAP {'encryption_key': '<key>'})
```

- `ENCRYPTED` encrypts every parquet DuckLake writes natively. Verified: a native-written secret is not
  cleartext in the data path.
- `METADATA_PARAMETERS MAP {'encryption_key': <key>}` encrypts the catalog file. Verified: reopen with no/wrong
  key REJECTED (fail-closed for free), right key reads; no cleartext secret in the catalog file.
- `DATA_INLINING_ROW_LIMIT 0` makes every write produce a parquet file (not an inlined catalog row); the default
  is 10. It is **per attach** (not persisted in the catalog — B spike), so every writing attach sets it.
- A keyed **plain** open (restore/relocate/migration, which use `duckdb.connect`) needs `STORAGE_VERSION 'latest'`
  alongside the key (B spike); the plan pins this.

Encryption is **fixed at catalog creation** (DuckDB storage ≥ 1.4.0; no in-place re-key); DuckDB 2.0.0 is v2's
pin, so no version bump.

## Goals

- Encrypt a **local** lake at rest — the native parquet, the catalog, raw deliveries, and DuckDB spill files —
  with an operator-supplied key, when the key is present.
- Resolve the key through one platform-agnostic seam; mask it; validate it; never persist it to a file the app
  writes.
- Rewrite the local bronze write path to DuckLake-native writes so `ENCRYPTED` covers the data.
- Carry each match's visibility in `ops_matches.visibility` (the existing per-match authoritative flag) and, on
  the local backend, in a per-row audit column so a public-context read can filter it; a visibility change is one
  atomic single-catalog update and no bronze data moves between physical roots on a local lake.
- Guarantee, on the local backend, that **no public-context read returns a private row**, enforced today by the
  anonymous route gate (`ANON_VIEWABLE_ROUTES`, `ui/shell.py:118`) and, when an anonymous page needs match rows,
  the row-level `scoped_reads` seam reading the authoritative `_visibility`; keep the HF backend's native-write
  prohibition and its physical split.
- Make restore re-apply each match's latest visibility, so a restored older version never republishes an
  out-of-date visibility.
- Fail **closed** in both directions: encrypted lake + no/wrong key stops; unencrypted lake + key set stops
  (no mixed lake), except the (deferred) migration that is encrypting the lake — the `migrating` exemption seam is
  kept for that future cycle.
- Leave the hosted paths (HF buckets, Lakebase) at-rest posture unchanged, including the HF bronze schema.

## Non-goals

- **The encrypt migration is deferred to a future cycle (owner, 2026-10-30).** `encrypt_migrate` (the transform of
  an existing unencrypted-local lake to A′) and the `lakehouse encrypt` command are NOT built in S1: the operator
  recreates the Mac-Mini and HF lakes fresh (and the local lakes are throwaway), so an encrypt-in-place path is not
  needed now. The `migrating` fail-closed exemption seam (`catalog/working.py`, `catalog/publish.check_encryption`)
  is kept, unit-tested and dormant, as the hook for that cycle. The migration design is retained below (§"The
  migration (transform)") as the future-cycle specification, not an S1 deliverable.
- **Keyed relocation of an encrypted lake to a new LOCAL path is deferred with the migration (owner, 2026-10-30).**
  The keyed plain open of the metadata file (`duckdb.connect` + `STORAGE_VERSION 'latest'`) that a local relocate
  needs is the same machinery the deferred migration uses, so `relocate` refuses an encrypted-local lake for **any**
  destination: `hf://` is the permanent refusal (see Hosted paths), and a new local path raises a clear "S1 does
  not relocate an encrypted lake" error (the keyed local rewrite lands with the migration cycle).
  - **Amended (T3, owner-approved 2026-10-31, decision 9):** the keyed plain open primitive itself moved into T3
    (`catalog/duck.py` `keyed_plain_attach`: `ATTACH '<file>' AS <alias> (ENCRYPTION_KEY '<key>', STORAGE_VERSION
    'latest')`, `temp_file_encryption` on, only a wrong key maps to `ConfigError`), because the T3 catalog compaction
    uses it. Keyed relocation, the encrypt migration and key rotation stay deferred.
- No change to the HF or Lakebase at-rest encryption (platform-encrypted). HF keeps its two buckets, its
  `place_root` write-time split (`bronze/writer.py:75`), its `ducklake_add_data_files` registration, its
  `assert_bronze_registered_only` guard, its file-move visibility change (`runs/visibility.py:97-98`), and its
  bronze schema (no `_visibility` column).
- No in-place re-key and no rotation in S1. Re-keying an encrypted lake (an encrypted source under the old key
  plus a new key at once) is **deferred to a future cycle** (owner decision, 2026-10-30): the single-env-var key
  resolver holds one key, and the migration only transforms an unencrypted source, so rotation needs its own
  two-key seam and is out of S1 scope.
- Not a certified-compliance guarantee (DuckDB states its own implementation is not certified); defense-in-depth
  for a leaked local disk or file, documented as such.

## Anti-goals

- The key in code, in a committed file, in a log line, in a rendered dbt `profiles.yml` or `target/` artifact,
  or in a query result the app surfaces (for example `duckdb_databases().options`).
- The application reading the key from anywhere but its process environment, or knowing how the environment was
  populated.
- A private bronze row reaching the public HF bucket, a public-context read returning a private row, or a
  DuckLake-native write to an HF bronze table.
- Silently opening unencrypted, or writing a mixed (part-encrypted) lake, on a key/flag mismatch.

## Constraints

- **Key resolution seam.** One resolver (beside `storage/roots.py`) returns the key for a run. It reads one
  environment variable, **`LAKEHOUSE_ENCRYPTION_KEY`**. This name matches the scheduler secret guard
  `schedule/base.py:39` (`(^|_)(TOKEN|SECRET|PASSWORD|PASSWD|KEY)(_|$)`), so `schedule install` treats it as a
  secret and never writes it into a plist or unit file (`launchd.py:91`, `systemd.py:56-58`); a test asserts
  `check_env` rejects the name. The resolver is the single place the key enters the app; the retrieval mechanism
  is pluggable behind it later without touching callers. Added to `lakehouse.mask.SECRET_ENV_VARS` so the
  `Masker` redacts it. The owner (or platform tooling) sets the variable; the session never holds it.
- **Key format.** A fixed charset and length, validated by the resolver before the value is placed in any SQL;
  on a bad value the resolver raises its own message (never echoing the key). At least `MIN_SECRET_LENGTH` (8)
  characters. The plan pins the charset (hex or base64url) and minimum entropy. The attach SQL is never logged.
- **Every catalog-open site passes the key**, or fails closed against an encrypted catalog: `working.py`
  `attach` (:199-204), `reader.py` (:96), `restore.py` (:154, plain `duckdb.connect`), `relocate.py` (:93, plain
  `duckdb.connect`), and `dbt/profiles.yml.j2`. The migration opens the **source** with its own key. The
  enumeration is complete (both reviewers verified a grep of `ATTACH` / `duckdb.connect(` in `src/lakehouse`).
- **Scope detection.** Encrypt only when `roots.lake.local_path is not None` (a `LocalStorage` lake) AND the key
  is present. An `hf://` lake never takes the app-level options.
- import-linter contracts, pyright strict, ruff, 120-col all stay green.

## The design

### Local modes
The A′ model — native bronze writes, the `_visibility` column, raw-in-catalog, and a **single** root — is the
**encrypted-local** mode: a local lake with the key present. A local lake with **no key** keeps today's model
unchanged: `ducklake_add_data_files` registration, `place_root`, **two** roots (lake + restricted), the file-move
visibility change, and no `_visibility` column. HF is always today's model. So there are three modes —
encrypted-local (A′), unencrypted-local (today), and HF (today) — and the writer, the `_visibility` column, the
one-root layout, raw-in-catalog, and the integrity guard all key off **encrypted-local**, not "local". The
migration (below) converts an unencrypted-local lake to the encrypted-local model. Every "local" in the design
sections below means the encrypted-local mode unless it says otherwise.

### One encrypted catalog, one local root
An encrypted-local lake is one DuckLake catalog whose single `DATA_PATH` is the lake root. Creation and every
open consult the key resolver; when the lake is local and the key is present, the attach adds `ENCRYPTED`,
`DATA_INLINING_ROW_LIMIT 0`, and `METADATA_PARAMETERS`.

### Key delivery across processes (owner decision 2)
The resolver reads the environment; every process that opens the lake reads the same environment. The CLI and the
local UI service read the key from their own environment, however it was set. Scheduled runs: the environment is
populated by the platform (the plan defines the per-platform mechanism); the app reads the environment the
launcher gives it, never a plist or unit file. The dbt subprocess (`runs/dbt.py`) receives the key through its
**environment**, and `profiles.yml.j2` references it with dbt's `env_var(...)` rather than rendering the literal;
dbt must not write the resolved attach SQL to `logs/` or `target/`. The plan confirms dbt-duckdb passes
`METADATA_PARAMETERS MAP {…}` (a dict renders without `MAP`; a spike is required). The key is never written to a
file the app produces.

### Bronze writes become DuckLake-native (local)
On a local lake the writer writes a match's rows natively (`INSERT`), so DuckLake writes and encrypts the parquet,
replacing the pyarrow write + `ducklake_add_data_files` of today (`bronze/writer.py:236`). `swap_match_files`
becomes delete-the-match's-rows then `INSERT`, in one atomic single-catalog transaction. On HF, the writer is
unchanged.

### The per-file identity model (resolves round-2 A-08 / B-15)
A DuckLake-native file is `ducklake-<uuid>.parquet`, so the `<match_key>-<content_version>-<write_id>` filename
no longer carries meaning on local. The observable-contract (Hyrum) consumers of `parse_file_name` / `match_of`
— `bronze/writer.py`, `catalog/moves.py`, `runs/match_writer.py`, `catalog/restore.py`, `swap_files`, and
`ops_match_files` (`root`/`path`) — are listed here so the plan replaces every one on the local path:

- **Change detection** (today's `content_version`): a per-match content hash kept in `ops_matches` (beside
  `parser_version`), used by the skip-if-unchanged logic instead of the filename.
- **Match → rows**: the bronze tables carry `match_key`; delete/replace is by `match_key`, not by filename.
- **`ops_match_files`** records DuckLake's file path under one root on local.

### Visibility: `ops_matches` plus a local-only row column (resolves round-3 A-09, B-11)
`ops_matches.visibility` is the per-match authoritative flag on **both** backends (`runs/history.py:94,331-337`),
unchanged. A visibility change updates it with `set_match_visibility` in one transaction with the ledger record
(below). On **HF** this is all that changes in the metadata; the data move stays the physical file move
(`runs/visibility.py:97-98`), so the **HF bronze schema is unchanged** (no row column).

On **local**, bronze and source-listing tables additionally gain an audit-class row column **`_visibility`** (not
`visibility`, which collides with the pining source column `_pining__sources.yml:20`). It is needed on local
because one table co-locates many matches' rows and (for source-level listings) per-record public/private
records that have no `match_key` to join to `ops_matches`. It is set in the **same transaction** as the write or
the flip. Invariant (local only): every local bronze/listing row's `_visibility` is non-null and, for a
match-scoped row, equals its match's `ops_matches.visibility`. `_visibility` is an audit-class column (like
`_ingested_at`), not a dbt source business-contract column; the plan states how the local dbt contracts admit it
and HF's do not (the local/HF schema difference is part of the already-accepted storage fork).

### The invariant that replaces the guard (resolves round-2 A-05 / B-10, round-3 A-10 / B-17)
`assert_bronze_registered_only` (`bronze/writer.py:315`) forbids any DuckLake-native bronze write
(backend-agnostic). The replacement is defined per backend, enforced at the shared writer-interface choke point,
and tested on both:

- **HF (non-encrypted, two-root): the native-write prohibition is retained.** The HF adapter keeps
  `assert_bronze_registered_only` (or an equal-strength check); a native bronze write on HF is still a bug.
- **Local:** (a) every local bronze/listing row has a non-null `_visibility`; (b) a match-scoped row's
  `_visibility` equals its match's `ops_matches.visibility`; (c) **no public-context read or export returns a
  private row.**

(c) is enforced **today by the route gate**, not by a row-level filter: `ANON_VIEWABLE_ROUTES` (`ui/shell.py:118`)
admits an anonymous caller to the `monitoring` page only, which carries no match identity (§7.2); every other
page is operator-only (`VIEW_PRIVATE`). So no anonymous read reaches a match row today. The low-level catalog
reader (`reader.py:51-97`) does **not** filter on visibility and never did — the physical split protected
file-level access, not query-time reads. `services/authz.py` `scoped_reads` / `ScopedReads.visible` (:97-121) is
the **reserved row-level seam** for the day an anonymous-viewable page reads match-scoped rows; it has no
production caller yet. When wired, it must filter on the **authoritative `_visibility`** audit column for local
bronze/listing rows, never on the pining source field `visibility` (`_pining__sources.yml:20`): reading the
source `visibility` would **leak** a listing row whose source says `public` while its match is private, and would
**over-restrict** a bronze data row that has no source `visibility` (dropped as `None`). So `_row_visibility`
(`authz.py:89-94`) reads `_visibility` when the row has it (local bronze/listing) and `visibility` only for rows
whose authoritative column is `visibility` (`ops_matches`, `dim_*`) — one seam, the correct field per row type.
Because `scoped_reads` is a post-fetch Python filter, (c) holds only if every public-context read routes through
it; an AST/registry gate proving that single choke point, and a check that every `ANON_VIEWABLE_ROUTES` route
reads only through the seam or carries no match identity, are impl-review gates. The local Confidential-data
guarantee therefore rests on two things: at-rest encryption of all local data, and the route gate (with
`scoped_reads` as the row-level seam when an anonymous page needs match rows). HF routing stays `place_root` at
write time plus the two-bucket split.

The §23.6 discipline is retained on local: `tables_outside_allowlist` (`runs/dbt.py:155`, allowlist
`dim_* / *_links / ops_* / lineage_*`) still forbids a dbt model materializing a native table outside the
allowlist, so dbt never materializes private row-level data into a derived table that bypasses `scoped_reads`.
Bronze is now legitimately native on local; the plan states whether bronze tables join the allowlist or the
check is made bronze-aware, and silver/gold models that read bronze carry or filter `_visibility` on local.

### Restore re-applies the latest visibility (resolves round-2 B-09)
A visibility change writes a **visibility record** to the ledger (reuse `ops_move_records`, or a sibling
`ops_visibility_changes`: `match_key`, `new_visibility`, `at`, `run_id`; no file keys on local) in the same
commit as the `ops_matches` (and local `_visibility`) update. **Restore** reads the latest visibility per match
from the ledger in the pre-restore `CURRENT`, restores the target version, then re-applies it before publishing:
on local an `UPDATE` of `ops_matches` (restore.py already does this at :269) and `_visibility`; on HF the
existing physical re-placement (`restore.py:187-200`). A restored pre-flip version therefore never republishes
an out-of-date visibility. Cleanup keeps versions as restore points as today; the re-apply makes any kept
version safe regardless of the value frozen in it.

### The is-encrypted flag and fail-closed both directions (resolves round-2 A-06 / B-05)
A plaintext boolean **`encrypted`** is added to `roots_record` (`publish.py:74`), persisted as `storage_roots` in
`CURRENT` and each manifest (`working.py:246,262`) and read by `check_roots` **before** the attach
(`working.py:139`). It is written at creation and checked on every open:

- record says encrypted, key absent or wrong → the DuckDB attach rejects it (fail-closed for free; reproduced).
- record says unencrypted, key present → the open stops before writing (no mixed lake), **except** the migration
  (below), which legitimately opens an unencrypted source with the key set and carries a stated exemption.

The flag need not be adversarially authenticated: the encrypted case is enforced by DuckDB regardless; the flag
only prevents the operator-error mixed-lake case.

### Raw deliveries are encrypted (owner decision 1; resolves round-2 B-12 / B-03)
All **local** raw is encrypted at rest with the lakehouse key. The mechanism is left to the plan with a
recommendation: store raw as **DuckLake-native rows** (a content-addressed BLOB table, so `ENCRYPTED` covers it
and no bespoke cryptography is introduced — B's spike8 confirmed a native BLOB is not cleartext on disk), rather
than a hand-rolled AEAD over the `raw/<sha256>.zst` files (the fallback the plan may choose).

> **RULING (implementation, 2026-10-30): the AEAD fallback was chosen, not BLOB-in-catalog.** `RawArchive` is
> storage-only (it takes a `Storage`, not a catalog; `runs/run.py` constructs it with no `WorkingCatalog`), so a
> BLOB table would mean threading a catalog into the raw zone and reworking content-addressing, `copy_to` and the
> moves. The AEAD (`raw/encryption.py`: AES-256-GCM, a 12-byte nonce prepended, HKDF-SHA256 from the lakehouse key
> — a vetted library, `cryptography`, the plan's sanctioned fallback) is localized to `store`/`read` and leaves
> the rest of the raw zone unchanged (it copies opaque bytes). The plan's mutant "store raw outside the catalog →
> cleartext grep → RED" does not apply; the replacement guarantee is `test_raw_encryption.py` (at-rest ciphertext,
> wrong-key failure, single-root placement, and the round-trip preserving each delivery's public/private
> classification). HKDF uses `salt=None`: acceptable because the key is a high-entropy random (>= 256 bits, the
> resolver's floor), not a low-entropy password that would need stretching.

The plan spikes
large raw objects (per-BLOB size limits). On local there is one root, so the raw move between roots
(`moves.py:10-14`, `_raw_records`) does not run; each raw delivery keeps its public/private classification for the
local read side only (owner decision 3 makes an encrypted-local → `hf://` move a non-requirement, and
**`relocate` refuses it** — see Hosted paths). On HF, raw is unchanged (bucket
files, platform-encrypted).

**Residual (accepted, S1-IMPL-15):** a raw object's file name is its plaintext `content_sha256` (`raw/<sha>.zst`),
so anyone with read access to the lake directory and a candidate delivery can confirm, by hashing, that the lake
holds that delivery — a confirmation oracle on the content, though the bytes stay encrypted. The name is the
content-addressing key used by `locate`, `copy_to` and lineage, so a keyed name (an HMAC of the hash under the
lakehouse key) would be a larger change to content addressing; it is recorded here as an accepted residual and a
future option, not changed in S1. The lake directory itself is access-controlled by the OS.

### `duckdb_databases()` leaks the key (resolves round-2 B-14)
On a keyed connection `duckdb_databases().options` holds the key in cleartext (B spike). No `src/` code calls it
today (grep). The design forbids reading `duckdb_databases` (or any catalog introspection that returns the key)
on a keyed connection, enforced by a test/AST gate; if ever needed, its result is masked before it can reach a
log or the UI.

### Temp and spill files (resolves round-2 B-08, round-3 B-16)
DuckDB 2.0.0 has `temp_file_encryption` (default `false`; `SET temp_file_encryption = true` is accepted — B
spike). Keyed connections set `temp_file_encryption = true` on the working catalog, the reader, AND the keyed dbt
profile (`profiles.yml.j2`), so spill bytes are encrypted.

> **RESIDUAL surfaced to the owner (2026-10-30, audit S1-IMPL-04).** The plan named a spike that *proves by a
> cleartext grep* that a keyed run's spill holds no marker. That proof is **infeasible**: an implementation spike
> forced an external sort to spill under a 20 MB `memory_limit`, and the **positive control failed** — even the
> UNKEYED spill files held no cleartext marker (DuckDB writes spill in a compressed/binary temp format, so a known
> string is never stored contiguously). A byte grep therefore cannot distinguish keyed from unkeyed spill, so it
> cannot prove encryption either way. The oracle that remains is: `temp_file_encryption = true` is applied on every
> keyed connection (verified via `current_setting`, `test_spill_encryption.py`), and DuckDB encrypts spill per its
> documented behavior when that setting is on. **The owner RULED on this residual (2026-10-30): accept the
> setting-applied oracle** — trust DuckDB's `temp_file_encryption` without an independent byte-proof, recorded (not
> a silent residual). No proven failure was found; the spike disproved only the grep method, not the encryption.

### Migrating an existing unencrypted local lake (resolves round-2 B-06, round-3 B-19, plan-round-2 B-13)
> **DEFERRED to a future cycle (owner, 2026-10-30) — see Non-goals.** This section is retained as the future-cycle
> specification for `encrypt_migrate` + `lakehouse encrypt`; it is NOT an S1 deliverable. The `migrating` exemption
> seam is kept dormant for it.

The migration is a **transform**, not a copy: it converts an unencrypted-local (two-root, `add_data_files`, no
`_visibility`) lake into the encrypted-local (A′) model — raw files → BLOB rows, bronze → native + `_visibility`
set from the current visibility, the per-match content hash and the visibility ledger seeded — opening the
**source** with its own key under the exemption (a plain `duckdb.connect`, as `relocate.py` uses today, fails
against an encrypted source), then repoints `CURRENT` atomically. **Pre-migration restore points are dropped**
(owner decision 4): a pre-migration kept version is an old-model catalog whose files the migration deletes, and
re-encrypting it would leave an old-model catalog the integrity guard rejects, so the migration starts the
encrypted lake fresh from the transformed `CURRENT` and versions created before the migration are not restorable
afterward. The operator keeps a copy of the old lake folder before migrating if a pre-encryption snapshot is
wanted. The old plaintext objects (catalog, bronze files, raw files, the restricted root, and the pre-migration
version/manifest objects) are removed by the guarded cleanup only after the new `CURRENT` is published; the plan
pins the order so a crash leaves a recoverable state. Re-keying an already-encrypted lake (rotation) is out of
S1 scope (deferred; see Non-goals). This is an owner-run `lakehouse` command.

### Hosted paths and the relocate refusal (resolves round-4 B-25)
No change to an `hf://` lake: it resolves no local key and takes neither `ENCRYPTED` nor `METADATA_PARAMETERS`.
`relocate` accepts `hf://` roots, but an **encrypted-local → `hf://`** relocate is **refused**: the relocated
catalog could not open on HF (no key there) and the private bronze would land in the public bucket as ciphertext
outside the bucket access split. The refusal is explicit, with a test; a supported encrypted-local → HF migration
(decrypt + re-split by `place_root`) is out of scope for S1, which the owner accepts (owner decision 3).

Relocating an encrypted-local lake to a **new local path** needs the keyed plain open of the metadata file
(`duckdb.connect` + the key + `STORAGE_VERSION 'latest'`) — the same machinery as the deferred migration — so it is
**deferred with the migration (owner, 2026-10-30)**. In S1 `relocate` refuses an encrypted-local lake for **any**
destination: `→ hf://` is the permanent refusal above, and a local destination raises a clear "S1 does not relocate
an encrypted lake" error (not a silent or partial move). When the migration cycle lands, the local relocate is
built with it.

## Behavior preservation and the local oracle (resolves round-2 A-07, round-3 B-17)

Query *rows* are unchanged on HF. On local, bronze/listing tables gain the audit column `_visibility`; readers,
dbt and the UI see the same matches. What changes on local: how rows are written (native vs `add_data_files`);
visibility is in `ops_matches` plus the local `_visibility` column, not the root; local files, the catalog, raw,
and spill are encrypted. The HF write mechanism and the HF bronze schema are unchanged.

`tests/e2e/test_no_private_rows.py` checks four things today (:51-91): (1)(2)(3) every native file (under
`data/main/`, from `ducklake_list_files`, and inlined) belongs to a `NATIVE_TABLE_ALLOWLIST` table; (4) no
native file holds a row-level column of a private match. The oracle is **forked by backend**:

- **HF:** unchanged — checks 1–4 hold; a private match's bronze files are in the restricted bucket only.
- **Local:** checks 1–3 (the §23.6 allowlist discipline) are retained, adapted so bronze tables are expected
  native on local (bronze joins the allowlist or the check is bronze-aware); they still forbid a dbt model
  materializing an unexpected native table. Check 4 **inverts** on local by design (private row-level bronze now
  lives natively, encrypted), so it is replaced by: (a) a cleartext grep of the data path, the catalog file and the
  raw store finds no private marker (the spill directory is excluded — a spill grep is infeasible even unencrypted,
  see "Temp and spill files"; spill encryption rests on the applied setting, the surfaced residual); and (b) no
  public-materialized table (a
  native/table-materialized dbt model in `NATIVE_TABLE_ALLOWLIST`, or any surface an anonymous caller can reach)
  holds **row-level** data (events, tracking frames) of a private match — a metadata row that merely lists a
  private match's existence (for example `dim_matches`, which left-joins `ops_matches`, `dim_matches.sql:15-18`)
  is allowed, exactly as today's check 4 (a row-level column on a private match's row). The anonymous route gate,
  and the reserved `scoped_reads` seam when wired, enforce the read side. `DATA_INLINING_ROW_LIMIT 0` keeps check
  3's inlined set empty for bronze.

A local lake with **no key set** stays unencrypted exactly as today, unless the `encrypted` flag says it was
created encrypted — then it fails closed.

## Test and verification plan

- Create a local lake with the key → the native parquet, the catalog, the raw store, and a keyed-run spill dir
  hold no cleartext secret. Mutants: drop `ENCRYPTED` → parquet grep red; drop the catalog key → catalog grep /
  reopen-without-key red; leave raw unencrypted → raw grep red; drop `temp_file_encryption` (if the spike proves
  it encrypts) → spill grep red.
- Reopen without the key and with a wrong key both fail closed (exit 2); reopen with the key reads.
- Open an unencrypted lake with the key set → fail closed via the `encrypted` flag; the migration path is exempt.
- A visibility change updates `ops_matches` (+ local `_visibility`) and writes the ledger record in one
  transaction; a crash leaves no partial split. Mutant: split across two transactions → crash test red.
- The invariant: (HF) a DuckLake-native bronze write is rejected → red mutant on the retained guard; (local)
  `_visibility` non-null and equals `ops_matches`.
- The read side (local): an anonymous request to a non-viewable route is gated (sign-in notice, no service read),
  and `monitoring` reads no match row; `_row_visibility` reads the authoritative `_visibility`, not the pining
  source `visibility` — mutant reading the source `visibility` → a private match's listing row with source
  `public` leaks → red; mutant that drops bronze rows lacking a source `visibility` → a public bronze read
  returns nothing → red. When `scoped_reads` is wired, a public read returns no private row.
- A **standing** test over the whole `ANON_VIEWABLE_ROUTES` set (not only `monitoring`): every anonymous-viewable
  route reads only through the seam or carries no match identity, so adding a route that exposes match data goes
  red (B-27).
- `relocate` refuses an encrypted-local → `hf://` move (keyless catalog on HF + private ciphertext in the public
  bucket); mutant that allows it → red.
- Restore a version from before a public→private flip → the match is still private; mutant: skip the re-apply →
  red.
- The §23.6 allowlist check holds on local (no dbt model materializes an unexpected native table); inlined set
  for bronze is empty (`DATA_INLINING_ROW_LIMIT 0`, set at every writing attach).
- The detach→bytes→reattach-with-key version round-trip reads back; a version at rest holds no cleartext secret.
- (DEFERRED with the migration, owner 2026-10-30 — future-cycle test, not an S1 deliverable) The migration
  transforms an existing unencrypted local lake to A′, repoints `CURRENT` atomically, removes old plaintext only
  after the new `CURRENT`, a post-migration run passes the integrity guard, and a **pre-migration version is not
  restorable** afterward (owner decision 4).
- The key resolver validates charset/length and raises its own message; no attach error, log line, rendered dbt
  profile, dbt log, `target/` artifact, or `duckdb_databases()` result exposes the key; `check_env` rejects
  `LAKEHOUSE_ENCRYPTION_KEY` as a scheduler secret. The env var is in `SECRET_ENV_VARS`; the `Masker` redacts it.
- The full default-tier suite stays green on the no-key path (unencrypted local lake, HF unchanged).
- Local gate: `uv run ruff check . && uv run ruff format --check . && uv run pyright && uv run lint-imports &&
  uv run pytest -m "not live and not hf and not bench"` (LAKEHOUSE_OS_TESTS=1) `&& uv run python
  scripts/pip_audit.py`.

## Reviewer focus

1. **Visibility model** — `ops_matches` authoritative both backends; local-only `_visibility` row column; HF
  schema and flip path unchanged; the per-row invariant is local-scoped and does not assert anything false on HF.
2. **The read side** — the route gate (`ANON_VIEWABLE_ROUTES`) is today's enforcement; `scoped_reads` is the
  reserved row-level seam that reads the authoritative `_visibility` (never the source `visibility`), with the
  single-choke-point gate an impl-review check; the §23.6 allowlist is retained on local; the guarantee rests on
  encryption + the route gate.
3. **Restore re-apply** and the **is-encrypted flag** (both directions, migration exemption).
4. **Key delivery** — one resolver, environment-only, name pinned to the scheduler secret guard, never persisted.
5. **Raw** and **spill** encryption, and the migration's re-encrypt of kept versions (plan spikes named).

## Left to the plan stage

- The env var resolver precedence, key charset/length, and whether `init` requires or warns on a local lake
  without a key; the per-platform environment-population mechanism for scheduled runs.
- The writer-interface seam and the exact `bronze/writer.py` / `listings.py` / `runs/visibility.py` /
  `runs/match_files.py` changes; the local `_visibility` schema change and its contract/parity tests; the
  `content_version`/`write_id` replacement.
- The raw mechanism (BLOB-in-catalog recommended) + its large-object spike; the `temp_file_encryption` spill
  spike; the dbt `MAP`-option spike; the re-encrypt-kept-versions feasibility spike.
- The visibility ledger table and the restore re-apply; the migration command, its exemption, crash-safety, and
  cleanup order.
- The `relocate` refusal of an encrypted-local → `hf://` move; and the read-side impl-review gates — the AST or
  registry check that every public-context read routes through `scoped_reads`, and that every
  `ANON_VIEWABLE_ROUTES` route reads only through it or carries no match identity; wiring `_row_visibility` to the
  authoritative `_visibility` per row type.
- The exact `working.py` / `reader.py` / `restore.py` / `relocate.py` / `profiles.yml.j2` key wiring, the
  `STORAGE_VERSION 'latest'` on keyed plain opens, the `DATA_INLINING_ROW_LIMIT 0` at every writing attach, and
  the exit-2 error mapping.
- Whether to unify HF on native writes (needs the owner-run `hf://` native-write spike; the session cannot hold
  `HF_TOKEN`). Default: keep the fork.
- The commit/approval gate: this rides the single pre-commit-program commit; the owner gives the final approval
  of this spec, the plan, and the implementation; no self-commit.
