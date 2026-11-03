# ADR-005: Raw archive

## Status

Accepted (2026-10-26). Owner decision; replaces guess 19 (spec §13).

## Context

- Sources change and vanish: Gradient Sports files on Google Drive, SkillCorner files behind an LFS quota.
- A parser bug found later must be fixable without fetching every file again.
- The channels' change signals are different kinds of value (git blob SHA-1, md5, LFS sha256, a Drive id with a day,
  pining `updated_at`). Some are not hashes at all, so they cannot name stored content.
- The same file can arrive through two channels, once inside a gzip or bz2 wrapper and once without.
- Measured cost at zstd 9 (spec §25.3): SkillCorner 5.94 MB and IDSSE 36.64 MB per match.

## Decision

- **Hash identity.** `content_sha256` is the SHA-256 of the decoded bytes, after removal of a transport wrapper that
  magic bytes identify. The raw zone, `_source_hash`, lineage comparison and no-op detection use only this hash.
  `delivery_sha256` (the bytes as delivered) and the channel's `change_signal` are stored in lineage for audit and for
  the download decision.
- **Layout.** One immutable object per content: `raw/<content_sha256>.zst`, the decoded bytes compressed with zstd
  level 9. An object is created only if absent and never overwritten. When a source changes a file, both versions
  stay. The wrapper is recorded in lineage (`ops_lineage`).
- **Visibility.** A raw file takes the flag of its own delivery: a private delivery's file lives only under `raw/` in
  the restricted root, a public delivery's file under `raw/` in the lake root. This holds even when the public copy of
  the match wins precedence (ADR-006).
- **Switch.** Each install has an on/off switch: on for the HF install and the Mac Mini staging install, off on
  laptops. With the archive off, nothing is stored and a visibility move makes no raw call.
- **Reads.** `RawArchive.locate` looks in the restricted root first, then in the lake root; the bytes are the same
  wherever the object is found. `RawArchive.read` checks the decoded bytes against their hash. Offline reparse reads
  only through `read`.

## Consequences

- One file delivered twice, with and without a wrapper, costs one object.
- A raw object can exist in both roots at once (a public and a private delivery of the same content). This is safe,
  because the public copy is public at its source.
- When a match becomes private, the raw files of its pining deliveries are copied to the restricted root; the lake-root
  object is deleted only if no public-channel delivery has the same content (ADR-006, spec §29.12).
- Disk use grows with every changed file; there is no retention for raw objects in cycle 1.
