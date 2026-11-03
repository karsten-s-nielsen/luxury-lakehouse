# ADR-006: Visibility, channel precedence, the restricted root and visibility moves

## Status

Accepted (2026-10-26). Owner decisions §29.2, §29.6, §29.7, §29.10, §29.11, §29.12 and §29.14 (spec §9).

## Context

- A match can arrive from a public direct source and from the owner's pining API. Pining entries carry their own
  `visibility` flag; some pining entries are public (for example 7 IDSSE matches).
- Private data must never be readable by the public Space, and on HF the Space's token cannot read a second bucket.
- A match's visibility can change in either direction, and every catalog version is a restore point that must keep
  working after such a change.

## Decision

- **Trust the source flag.** Each match takes its visibility from the source; the code never infers it from a token.
- **Precedence.** If the pining copy has extra fields (a field with at least one non-null value that the public copy
  lacks entirely), the pining copy is kept; otherwise the public copy is kept. The match takes the flag of the **kept**
  copy. The choice uses only the current copies, so it does not depend on the order of arrival, and it is made again
  whenever a copy changes. The decision and its reason go into `ops_lineage`.
- **Two roots.** Every install has a lake root and a restricted root. The restricted root holds every bronze file of a
  private match and every raw file of a private delivery. The lake root holds the catalog, config-side and run-side
  objects, the files of public matches and the DuckLake-written tables. Row-level data never enters a lake-root table:
  every silver and gold model over row-level bronze is a dbt view.
- **Moves.** A visibility change moves files between the roots under new file names (new write ids), with one move
  record per file (`ops_move_records`).
  - Public to private works per match: every lake-root object that the catalog registers, that a kept version names,
    that only DuckLake tracks, or that only exists under the match's file-name prefix is covered. Named objects get a
    restricted copy; the others get a record with no new key. One catalog commit registers the copies and writes the
    records; right after it, the lake-root objects are deleted. Every run starts by deleting each lake-root object that
    a public-to-private record names, and a guard stops the run if such a key is still registered in `CURRENT`.
  - The raw files of the match's pining deliveries are copied to the restricted root too; a lake-root raw object stays
    when a public-channel delivery has the same content, because it is public at its source.
  - Private to public copies the current restricted files to the lake root; the old restricted copies stay for the
    normal cleanup.
- **Restores.** A restore gives every match its latest visibility and keeps the restored version's content: moved
  lake-root keys resolve to their restricted copies, and a match that is public now gets lake-root copies.
- **Every held match is checked.** Every run lists each pining module that holds ingested matches, active or not, for
  visibility only.

## Consequences

- Dimension and link tables in the lake root still hold names and metadata of private matches (players, teams,
  competitions, dates), and the lake-root catalog holds each restricted file's path, size and column statistics. None
  of these is event, tracking, lineup or raw content.
- Between a move's commit and its delete, normally seconds, a lake-root copy of a now-private match still exists;
  after a crash in that gap, it lasts until the next run starts.
- A reader attached to a pre-move version gets an IO error for that match until it reads the new `CURRENT`; the
  services layer shows this as an error, never as missing data.
- Move records are pruned only after an event (no kept version names the old key and DuckLake no longer tracks it),
  never after a fixed time.
