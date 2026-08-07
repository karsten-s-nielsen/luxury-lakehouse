# ADR-072: The HF publish seam — GuardedFrame, receipts, and one door

| Field | Value |
|---|---|
| **Date** | 2026-08-06 |
| **Status** | Accepted |
| **Deciders** | Karsten Nielsen |

## Context

ADR-064 made the redistribution decision per-match (`access_tier`), and ADR-049 gave every row-level dataset a permanently-private `<repo>-restricted` companion. Both rely on each publisher *remembering* to do four things in order: `split_restricted(df, column="access_tier")`, `assert_no_private_leak(public_df, ...)`, drop `access_tier`, then upload.

That is a convention, and it was asserted like one. The invariant was checked by substring searches over publisher source in four hand-maintained lists — `test_hf_publish_parity.py`, `test_gradientsports_hf_exclusion.py`, `test_publish_shot_freeze_frames.py`, `test_publish_xg_shot_data_v3.py` — none derived from `PUBLISHER_REGISTRY`. A substring check passes on a mention in a comment, on a call against the *restricted* frame, and on a call placed *after* the upload. It cannot fail for the right reason.

Against those checks sat **15 direct upload call sites across 15 files** (12 under `scripts/`, 3 under `src/ingestion/`, covering 12 registry entries). Fifteen doors, one optional turnstile. Two of them — `publish_shots_on_target_hf` and `publish_obso_pausa_inputs_hf` — were registered `fail_closed` but called the guard **nowhere**, because the invocation assertion was parametrized over the six *split* publishers only. `publish_shots_on_target_hf` reads `fct_shots`, a cross-provider mart, and uploaded via `api.upload_file`, so an `upload_folder`-only ban would also have missed it.

The forcing function is the prospective commercial StatsBomb 360 subscription (`docs/superpowers/specs/2026-08-06-statsbomb-commercial-360-containment-design.md`). Today `statsbomb` is public-by-licence, so those two unguarded publishers are harmless. The moment StatsBomb becomes restrictable they publish paid club data to public repos with nothing in the way.

## Decision

Every public HF publish goes through one seam in `ingestion.hf_upload_seam`: `prepare_public_upload(df, publisher=...)` performs guard → split → drop and returns `GuardedFrame`s that are the **only** objects able to write a Parquet; `upload_guarded(staging_dir, frames=..., ...)` refuses to upload any file no receipt accounts for, and **derives repo privacy from the frames' tier** rather than taking a caller-supplied flag.

## Alternatives considered

| Option | Pros | Cons | Why rejected |
|---|---|---|---|
| A. Extend the existing substring assertion to all 12 registry entries | One-line change | Institutionalises a check that passes on a comment mention, a restricted-frame call, or a post-upload call | Cheapest, and enforces nothing. The next publisher reintroduces the gap. |
| B. `publish_public_frame(df, …)` — a single frame in, upload out | Smallest API | 14 of 15 call sites stage a *folder* (partitioned by `competition_id` / `data_source` / sub-table); `publish_football2vec_embeddings_hf` guards three frames under a degradation policy | A port too narrow for its callers gets bypassed, and then the ban is an obstacle to route around rather than a boundary — worse than the convention, because it *looks* enforced |
| C. Constructor sentinel on `GuardedFrame` to block forgery | Runtime-enforced, no receipt bookkeeping | Verified empirically on Python 3.10: `dataclasses.replace` re-runs `__post_init__` **and carries unreplaced fields through**, so the sentinel rides along and frame substitution still works. `__replace__` (the hook that would fix this) landed in 3.13; this repo is pinned `>=3.10,<3.11` | Closes one of the two forgery routes, not both |
| D. **Chosen** — two-call seam, frame-object authorization on the receipt, tier-derived privacy, AST ban | Closes both forgery routes at runtime; fits folder staging and multi-frame degradation; privacy cannot be forgotten because there is no parameter | Publishers carry a `GuardedFrame` through their staging code; the receipt holds strong refs to staged frames | — |

## Consequences

### Positive

- **A bypass now requires a line that is obviously wrong to a reviewer *and* fails a gate.** Three independent controls: frame authorization (runtime), the staging-dir path diff (runtime), the AST ban (lint).
- **Repo privacy cannot be forgotten.** `upload_guarded` derives `private` from `GuardedFrame.tier` and asserts the ADR-049 `-restricted` suffix in both directions. This removes a fail-open default structurally identical to the `stamp_access_tier(visibility=None)` defect the containment spec removes at R-6a.
- **The two unguarded publishers are closed.** `publish_shots_on_target_hf` now selects `dm.access_tier` (the `dim_matches` join already existed; only the column was missing) and asserts non-null loudly rather than letting a `LEFT JOIN` NULL fail-safe into silent withholding. `publish_obso_pausa_inputs_hf` joins `dim_matches` on `(provider, native_match_id)` — `bronze.idsse_events` carries the native string id, not `match_key`.
- **Six substring assertions retired** in favour of two AST gates derived from `PUBLISHER_REGISTRY`, so a new publisher is covered the day it is added.

### Negative

- **Enforcement is not perfect and cannot be in Python.** `receipt._authorize(df)` defeats the runtime check; it is banned by the AST gate and is an explicit, greppable, single-underscore line. That is a categorically different posture from typing a public constructor, not an absolute barrier.
- **`GuardedFrame` threads through publisher staging code.** `groupby` / `drop_columns` are the only two derivations provided; a publisher needing a third must add it to the seam rather than reach for pandas.
- **The receipt holds strong references** to every staged frame for the life of the publish. Deliberate: `id()` can be recycled by a later allocation and would authorize an arbitrary frame by coincidence.

### Neutral

- `hf_upload_seam` imports `split_restricted` from `hf_publish` **function-locally**, so there is no module-level cycle. Publishers import the seam directly; an `hf_publish` re-export was tried and abandoned because ruff's isort explodes the `X as X` form into one import statement per name.
- Mode (`split` / `fail_closed` / `derived`) is read from `PUBLISHER_REGISTRY` inside the seam, making it a property of the call rather than of a docstring that had already drifted from reality.

## Enforcement

| Invariant | Test |
|---|---|
| Both forgery routes refuse to write | `test_hf_upload_seam.py::test_directly_constructed_guarded_frame_refuses_to_write`, `::test_replace_substituted_frame_refuses_to_write` |
| Unaccounted staged file refuses to upload | `test_hf_upload_seam.py::test_upload_guarded_refuses_an_unrecorded_file` |
| Tier ↔ repo privacy and `-restricted` suffix, both directions | `test_hf_upload_seam.py::test_restricted_frames_*`, `::test_public_frames_refuse_a_restricted_repo_id` |
| Missing `access_tier` raises `LeakDetectedError` in **every** mode | `test_hf_upload_seam.py::test_missing_access_tier_column_raises_leak_error_in_every_mode` |
| No publisher touches the HF API or forges a frame | `test_publisher_seam_conformance.py::test_publisher_does_not_bypass_the_seam` |
| Every publisher routes through the seam | `test_publisher_seam_conformance.py::test_publisher_routes_through_the_seam` |
| Discovery cannot go vacuous | `test_publisher_seam_conformance.py::test_publisher_discovery_finds_every_file` (asserts exactly 15) |
| Staged tree, `path_in_repo`, `delete_patterns`, repo privacy per publisher shape | `test_publisher_upload_contract.py` |

**Not covered by any of the above, and not coverable without credentials:** that the SQL each publisher runs returns the assumed columns, and that HF accepts the resulting upload. No publisher has been executed end to end. In particular the `delete_patterns` correction below turns five previously-inert sweeps into genuinely destructive ones, and that has never run.

## Related

- Amends [ADR-049](ADR-049-restricted-hf-dataset-companion-repos.md) — the split is now performed inside the seam; publishers no longer import `split_restricted`.
- Amends [ADR-064](ADR-064-per-match-access-tier.md) — the guard is reached through `prepare_public_upload`, not called directly.
- [ADR-014](ADR-014-hf-card-inventory-parity.md) — `upload_hf_readme` is deliberately outside the seam. It pushes documentation, not data, and is a bare-name call the AST ban does not match.
- Spec: `docs/superpowers/specs/2026-08-06-statsbomb-commercial-360-containment-design.md` (PR-1). Plan: `docs/superpowers/plans/2026-08-06-publish-seam-pr1.md`.

## Amendment 2026-08-06 — the `["data/*"]` sweep correction

Four publishers passed `delete_patterns=["data/*"]` with `path_in_repo="data"`. Patterns are matched **relative** to `path_in_repo`, so those swept **nothing** and had silently no-opped since they were written — the same class documented in CLAUDE.md, which already mandates `["**"]`.

Corrected in `src/ingestion/publish_freeze_frame_hf.py`, `src/ingestion/publish_xg_shots_hf.py`, `scripts/publish_line_breaking_passes_hf.py`, `scripts/publish_obso_pausa_inputs_hf.py`. `scripts/publish_freeze_frame_hf.py` had no sweep at all while its twin did; both now sweep `["**"]`, since divergent behaviour between twins publishing the *same* repo makes the outcome depend on run order.

**This is the one behavioural change in an otherwise behaviour-preserving PR.** Five repos that previously deleted nothing will begin deleting stale siblings on their next run. That is the intended fix — the stale-part-file class poisoned a PSxG retrain on 2026-06-21 — but it warrants an operator dry-run on one repo before the first scheduled publish. The pre-existing coverage (`test_publisher_delete_patterns_sweep_whole_path_in_repo`) is parametrized over the six split publishers only, so four of the five changed files had zero tests; `test_publisher_upload_contract.py` was added to close that.

## Amendment 2026-08-07 — the token is derived too

Third instance of this ADR's core principle, found by running the seam for real.

`upload_guarded` took `token: str` from the caller. Three `src/ingestion/` publishers resolved it with a hand-rolled `os.environ.get("HF_TOKEN", "") or get_token()` — byte-identical copy-paste that skips `ingestion.utils.resolve_hf_token()`'s **second** source, the Databricks secret scope `hf`/`token`. On serverless there is no env var and no CLI cache, so that scope is the *only* source: all three raised `RuntimeError` before doing any work, on every job run, while `hf_sync` caught the exception, logged ERROR and reported **SUCCESS**.

That is why `xg-freeze-frame-data` accumulated 103 stale part-files nobody ever swept — the publisher that would have swept them had never once run to completion from the job.

`upload_guarded` now derives the token via `resolve_hf_token()` when none is passed, raising `TokenUnavailableError` if nothing resolves. An explicit token is still honoured for tests and callers that already hold one.

The pattern is now consistent across all three properties the seam owns:

| Property | Was | Now |
|---|---|---|
| Repo privacy | `private: bool = False` — caller-passed, fail-open | derived from `GuardedFrame.tier` |
| Sweep | `delete_patterns` optional — 4/4 callers wrong | derived from write semantics (`["**"]`) |
| HF token | `token: str` — caller-passed, 3/3 serverless publishers wrong | derived via `resolve_hf_token()` |

Each was a safety property a caller could get wrong silently, and each was got wrong. **A safety property that can be passed will eventually be passed wrong; derive it or enforce it.**

Enforced by an AST gate over `src/ingestion/` (no ad-hoc `get_token()` / `HF_TOKEN` read outside `resolve_hf_token`). `scripts/` is deliberately out of scope — PEP 723 jobs run on HF Jobs with `HF_TOKEN` injected via `--secrets`, so env-first is correct there. `CLAUDE.md`'s token rule, which stated the orchestration-script guidance unconditionally and would have sanctioned the bug if applied to a Databricks module, is scoped in the same change.
