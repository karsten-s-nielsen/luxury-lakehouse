# ADR-007: Single writer, immutable catalog versions and the `CURRENT` compare-and-swap

## Status

Accepted (2026-10-26). Mechanism chosen in plan review round 1. HF routing by owner decision §29.9 (spec §10.6).
The HF half is verified in Task 33 (spec §26 items 2, 3 and 15).

## Context

- A DuckDB catalog file cannot be opened by a second process while the writer has it attached. A remote reader on HF
  must never range-read an object that is being replaced (spec §10.4).
- Exactly one run may change the catalog at a time. A scheduled run, a button run and a CLI run can start together,
  and a crashed run leaves its lease behind.
- `HfApi` bucket writes have no conditional write. The S3 gateway `https://s3.hf.co` has `If-None-Match` and
  `If-Match` (spec §29.9).
- HF buckets are not versioned, so the catalog versions are the lake's only restore points (spec §10.8).

## Decision

**Lease (Task 7).** The lease is a chain of generation objects `runs/lease/<12-digit generation>.json`.

- A generation object is created only with `put_if_absent`, so exactly one taker wins each generation.
- A taker creates generation N+1 only when N is released, or when N's expiry plus `TAKEOVER_GRACE` (2 min) has passed.
  It then lists again; a newer generation means the listing was stale, so it gives up.
- The holder renews every `HEARTBEAT` (60 s) with a plain `put` of its own object; `LEASE_TTL` is 10 min.
- `assert_held()` passes only when generation N+1 does not exist, the own object still names the run, and at least
  `COMMIT_MARGIN` (60 s) is left on the holder's own clock.
- Release stops the heartbeat first, then writes `released`.
- A run that loses the race writes `runs/skipped/<run_id>.json` ("skipped: run in progress") and stops. The next holder
  folds the record into run history and only then removes it.

**Immutable versions and `CURRENT` (Task 9).**

- Each catalog commit publishes `catalog/v/<seq>.ducklake` and its manifest `catalog/v/<seq>.files.json` with
  `put_if_absent`. A published version is never overwritten.
- The first sequence of a working copy is `max(listed sequences, CURRENT.seq) + 1`, so a leftover version of a crashed
  run is skipped, never reused.
- `catalog/CURRENT` holds `{seq, version_key, size, sha256, parent_seq, lease_generation, storage_roots,
  self_checksum}`. `publish_current` is its only writer: `put_if_absent` on an empty lake, and otherwise
  `replace_if_match` with the tag that this writer last read or wrote.
- Commit order: `assert_held`, `DETACH`, create the version, create the manifest, verify the full SHA-256 of the
  uploaded version, `assert_held` again, `publish_current`, re-attach. No `CHECKPOINT` runs on the DuckLake attach
  (ADR-001).
- When the version or manifest object already exists, the run stops. A lost lease, or a `CURRENT` that changed since
  this writer read it, raises `LeaseLostError`; otherwise it raises `ConflictError("version <seq> already exists:
  another writer created it")`.
- Readers read `CURRENT`, check its self checksum (3 tries, then a loud failure), check the version size, and attach
  the version read-only with `override_data_path`. A reader keeps one attachment per version.
- `WorkingCatalog.open` and `CatalogReader` compare the resolved roots with `CURRENT.storage_roots` and raise
  `RootMismatchError` on any difference. Only `relocate` and a restore across a relocation skip this check.

**Fencing.** A stale holder that misses every lease check is still fenced three ways:

1. versions and manifests are created only if absent;
2. `CURRENT` changes only by compare-and-swap;
3. the orphan sweep deletes only objects older than its own run's `started_storage_time`, the storage time of its
   lease generation (Task 12).

**HF routing (spec §29.9).** The lease objects, `catalog/v/*` and `catalog/CURRENT` go through the S3 gateway with
the conditional headers. With gateway credentials, the Job also reads, lists, stats and deletes those keys through
the gateway, so it reads its own writes on the channel that wrote them.

## Consequences

- Readers never block the writer, and they never see a catalog that names files that do not exist yet.
- A failed compare-and-swap can leave an unreferenced version; version retention deletes it like an in-run version of
  a crashed run (Task 12).
- On a file system without hard links (exFAT, FAT32, many SMB shares), local `put_if_absent` falls back to
  `O_CREAT | O_EXCL`. A partial generation object counts as held until its `mtime` plus `LEASE_TTL` plus
  `TAKEOVER_GRACE`.
- On HF, the safety of this design depends on the gateway's conditional writes and on `s3://` and `hf://` naming the
  same object. Task 33 verifies both. If either fails, the builder stops and brings the evidence to the owner
  (Gate G-C).

## Amendment (T6, owner-approved 2026-10-31; `docs/specs/2026-10-31-v2-t6-hf-single-writer-design.md`)

**Evidence.** The T3 Task 12 HF Jobs (2026-10-31) showed that the S3 gateway honours `If-None-Match` and `If-Match`
for sequential requests only: two processes both won lease generation 1 within 11 ms; 6-way single-part creates had
up to 6 winners; a 6-way `If-Match` race on one ETag had up to 4 successes; and `If-None-Match` on
`CompleteMultipartUpload` was ignored, so a second sequential multipart create overwrote the object.

**Decision.**

- **The HF lease fence of record is a ledger:** a private HF dataset repository per lake (`<lake bucket>-control`,
  Job variable `LAKEHOUSE_CONTROL_REPO`) holding `lease.json`. A generation is won only by
  `HfApi.create_commit(..., parent_commit=<head>)`, which the Hub refuses when the head moved (`storage/ledger.py`,
  `Lease._acquire_on_ledger`). The bucket generation object stays as a second fence; the heartbeat and the release stay
  bucket writes; `assert_held` reads the ledger and fails closed. A local lake keeps the file-system lease.
- **Every HF conditional write is one part** (`put_if_absent`, `replace_if_match`), up to `SINGLE_PART_LIMIT`
  (2,048 MiB, measured); a larger body raises `StorageLimitError`. The catalog keys stay `catalog/v/<seq>.ducklake`, so
  the sequential version fence applies.
- **`CURRENT` is read back** after its compare-and-swap; a different pointer raises `LeaseLostError`. An existing
  version that neither a lost lease nor a moved `CURRENT` explains raises `IntegrityError` (no longer `ConflictError`).
- **Accepted residuals (owner):** a holder stalled for about 11 minutes between its last `assert_held` and its
  `CURRENT` write, or whose version upload overlaps a new holder's upload of the same sequence.
