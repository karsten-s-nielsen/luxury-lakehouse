# ADR-001: Storage engine — DuckLake 1.1 with our own Parquet files

## Status

Accepted (2026-10-26). Owner decision: approach 1, after a measured spike (spec §10.1, §25).

## Context

v2 is an open-source lakehouse that runs on a laptop, on a Mac Mini staging host and on Hugging Face (a Space and a
scheduled Job). It needs one storage engine for all three. The engine must:

- keep tracking data small, because one tracking match holds about 1 to 3.2 million player rows;
- let each file live in the root that its match visibility selects (the lake root or the restricted root, spec §9);
- give readers a consistent catalog while a run writes;
- run with no server process.

The storage spike of 2026-10-26 measured six engines on the same sorted, integer-centimetre SkillCorner players table
(spec §25.2). The owner later moved the coordinate unit to integer millimetres (spec §29.15, 2026-10-27); the Task 30
benchmark re-measures the bytes with the final schema:

| Engine | MB per match | 2 matches after replace and maintenance | Frame-window read |
|---|---:|---:|---:|
| pyarrow tuned Parquet, own manifest | 1.29 | 2.71 | 4.3 ms |
| DuckLake + our tuned files | 1.29 | 2.72 | 14.1 ms |
| DuckLake native writer (unpartitioned / partitioned) | 4.54 / 3.45 | 8.21 / 7.55 | 34 / 42 ms |
| Delta (delta-rs, zstd) | 4.59 | 9.97 | 17.4 ms |
| Iceberg (pyiceberg + SQLite catalog, zstd) | 4.63 | 14.66 | 18.2 ms |
| DuckDB native file | 5.51 | 16.51 | 1.4 ms |
| Lance | 8.32 | 17.98 | 13.8 ms |

DuckDB's Parquet writer picks dictionary encoding for these integers, and DuckLake exposes no
`dictionary_size_limit` option. So the DuckLake native writer cannot reach the size of our tuned files.

## Decision

- Use a **DuckLake 1.1** catalog in a single DuckDB file, on **DuckDB 2.0.x** LTS.
- v2 writes **every bronze file itself** with pyarrow, in the tuned layout of spec §10.2: one file per match per table.
  It then registers each file with `ducklake_add_data_files`. A file goes to the root that its match visibility
  selects.
- Tables that hold our own files have `auto_compact = false`, because DuckLake compaction rewrites them with the larger
  dictionary-encoded writer.
- Small tables (dimensions, links, ops tables) may use the DuckLake native writer through dbt. They always go to the
  lake root, through its `data_path`.
- Every DuckDB connection comes from `lakehouse.catalog.duck.connect_duckdb`, which applies the resource caps and the
  optional `LAKEHOUSE_DUCKDB_EXTENSION_DIR`.

## Parse-memory budget (Task 30, measured 2026-10-27; spec §10.9, §25.1, §26 item 13)

Measured on the Mac Mini staging host (M4 Pro, 24 GB, macOS 26.2, Python 3.12.16, DuckDB 2.0.0), integer-millimetre
coordinates (spec §29.15), one match per provider, peak RSS sampled in the parse child process. `tests/bench/budget.json`
holds these; `uv run pytest -m bench` fails above them. The budget is the measured peak plus 25 % headroom
(author's call).

| Provider | Peak RSS (measured → budget) | Parse time | Bronze MB/match | Rows |
| --- | --- | --- | --- | --- |
| IDSSE | 8488.9 MB → 10611.1 MB | 30.1 s → 37.6 s | 19.0 → 23.8 | 146,211 frames · 3,216,642 player rows |
| SkillCorner | 485.4 MB → 606.8 MB | 1.8 s → 2.2 s | 2.53 → 3.16 | 60,530 frames · 894,982 player rows |

One IDSSE match parse peaks near 8.5 GB of RSS. That fits the Mac Mini and an HF `cpu-upgrade` Job (32 GB), but it
leaves little room on `cpu-basic` (16 GB) and forbids parsing two IDSSE matches in one process; the HF Job flavour
and concurrency (Task 33) must respect this.

## Live end-to-end (Task 31, Mac Mini, 2026-10-27; spec §17, §G2)

`tests/live/test_e2e_public_small.py` ran the full pipeline on the real public sources, two matches per active module
(StatsBomb, Wyscout, IDSSE, SkillCorner — 8 matches, 4 providers), laptop defaults, one process, §10.9 caps. Result:
every active module wrote bronze, the dbt dimensions built, the review queue held items or links, **0 matches failed**,
and a second run added and re-parsed nothing (§G2). Wall time 394.9 s for both runs together (the first with all
downloads and the full parse, the second a no-op). The two IDSSE matches with referee tracking (J03WMX, J03WN1) now
ingest after the referee-FrameSet fix (below).

## Catalog at scale (Task 32, Mac Mini, 2026-10-28; spec §26 item 4)

A scale harness replayed each of the 253 public matches from the raw archive and wrote three re-keyed replicas per
match through the normal write and commit path, so the catalog took 1,012 bronze match writes (759 replicas plus 253
originals). Measured at that peak: 763 retained version snapshots; catalog 16 GB (snapshot size min 18.1 MB, median
22.0 MB, max 27.3 MB; total 17.0 GB); native Parquet (the DuckLake-written files) 1.3 GB; the `CURRENT` snapshot
25.7 MB. Attach plus first query stayed flat at 0.067 s, about 45 times under the §19.5 3 s budget, because a reader
attaches only `CURRENT`, never the version history. Version retention (§10.8) kept 763 of the 1,038 sequences.

Caveat on the measurement: the replicas are content-identical to their originals (only `match_key` and
`native_match_id` differ), so the resolver collapses them as duplicate matches; `dim_matches` resolves to 274 and the
gold marts dedupe. The catalog, bronze and attach storage scale is therefore measured at about 1,000 matches, but the
resolved dimension is not. The native 1.3 GB is the public match mix, not the all-IDSSE projection of §25.2
(SkillCorner about 1.9 GB, IDSSE about 9.1 GB for 1,000 matches of that provider). The staging lake was restored to
the clean 253 matches after the measurement.

## Consequences

- Bronze tracking data costs about 2.5 MB per SkillCorner match and 19.0 MB per IDSSE match in integer millimetres
  (Task 30 measurement, spec §25.1, §29.15; the earlier 1.9 / 9.1 MB figures were the integer-centimetre spike).
- The IDSSE parser ingests referee and official FrameSets (Task 31): silly-kicks drops them, so v2 reconciles only the
  match-team frames with silly-kicks and writes the referee rows from its own scan, keyed by the referee's person with
  a null team in the roster. A FrameSet of an object the metadata does not name is still refused (spec §12).
- DuckLake stores registered files by absolute path, so a moved lake root needs `lakehouse relocate` (spec §10.5.1).
  DuckLake-written files are stored relative to `data_path` and re-root through `override_data_path` (spec §10.4).
- A DuckDB catalog file allows one attacher. So the writer works on a private working copy and publishes immutable
  versions (ADR-007). dbt gets the working copy only after the run detaches it.
- `CHECKPOINT` on a DuckLake attach runs DuckLake maintenance (a probe on duckdb 2.0.0 merged 3 data files into 1).
  That maintenance could delete files outside the guarded cleanup of spec §10.3 step 7. So no v2 code runs
  `CHECKPOINT` on the `lake` attach. `DETACH` flushes the catalog file, and the commit fails loudly if a WAL file
  remains (ADR-007).
- A DuckDB or DuckLake upgrade must re-run the relocation test and the snapshot-equality test (spec §26 item 6).

## Amendment (2026-11-01, T12, owner-approved; spec §10.3 step 6, §10.2, §10.8)

`CHECKPOINT` stays forbidden on the `lake` attach, but the non-bronze DuckLake-managed tables (the ops tables and any
native dimension or link) still need their small files merged, because every insert writes one data file and v2 never
runs DuckLake's auto-compaction. So the run end runs DuckLake's **targeted** maintenance on those tables only, under
v2's own control: `ducklake_rewrite_data_files` (delete threshold 0.0) for files with delete files, then
`ducklake_merge_adjacent_files` in bounded creation-time windows. Neither call deletes a file — the replaced files
leave through the guarded cleanup of spec §10.3 step 7 once their snapshots expire — so the step stays inside the
cleanup's file-safety invariant. A fail-closed guard keeps `auto_compact = false` on every bronze table before any
merge: DuckLake's rewrite still makes those files 2.7–3.5× larger (above), and they are one file per match by design.

Also (D1/D2): the process raises its open-file soft limit at every entry point before it opens a catalog, because one
DuckLake scan of a large table opens hundreds of file descriptors (300 files → 369–478; 10,000 → 834). The catalog
commands stop with a clear error below a minimum soft limit; the scheduled-task definitions carry the raised limit
(`docs/ops/deploy.md`).
