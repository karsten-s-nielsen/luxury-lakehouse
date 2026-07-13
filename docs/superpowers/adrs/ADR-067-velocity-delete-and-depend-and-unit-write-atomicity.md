# ADR-067: Velocity delete-and-depend, loud drain failures, and all-or-nothing unit writes

| Field | Value |
|---|---|
| **Date** | 2026-07-12 |
| **Status** | Accepted |
| **Deciders** | Karsten Nielsen |

## Context

On 2026-07-11, run `85619159042760` reported **SUCCESS**. Inside it, the work unit
`skillcorner:1552423:2` wrote **0 of its 550 SPADL actions** into `fct_action_context`. Nobody knew
for a month; it surfaced sideways while investigating an unrelated silly-kicks 4.46.0 handoff.

The root cause was a **hand-maintained copy of a library function**.
`analytics/action_context/convert.py::_derive_velocities_savgol` re-implemented silly-kicks'
short-group velocity fallback but **dropped upstream's `len(x_vals) <= 1` guard** — a guard whose own
comment in `silly_kicks/tracking/preprocess/_velocity.py:69-76` names the very incident class it was
added for (*"GS WC2022 match 3851, away #10 has exactly 1 frame in period 2"*). A player track of
exactly one frame therefore reached `np.gradient`, which requires ≥ 2 points, and raised.

Everything after that behaved *as designed*, and that is the uncomfortable part:

| step | behaviour | verdict |
|---|---|---|
| UDF re-raises with the group key (`match_id=1552423, period=2, frame_batch_id=184`) | ADR-002 §5 | **correct** |
| one raising batch fails the WHOLE `applyInPandas` write → unit emits 0 rows | Spark | **correct, but a 550× amplifier** |
| `drain_worker` catches, `failed += 1`, continues (`drain.py:170-181`) | deliberate | **correct** — one bad unit must not destroy a 5.5 h drain |
| `main_drain_worker` logs the summary and **returns** | — | **the defect**: the task exits 0 |
| queue has no status column; nothing checks the mart | — | **invisible** |

A raised exception and a silently-passing invariant produced an **identical signature** in the mart.
That is why an earlier version of the design document reasoned its way to the wrong root cause: it
assumed "nothing failed" without checking the driver log, which said otherwise in one line.

Compounding it, the per-unit completeness invariant that ADR-040 added to catch exactly this class
could not have caught it either — see Decision 3.

## Decision

### 1. Delete the velocity port; depend on silly-kicks (`delete-and-depend`)

Both lakehouse copies of `_derive_velocities_savgol` are **deleted**:

- `analytics/action_context/convert.py` (AC path, via `sk_frame_adapters._finalize`)
- `ingestion/tracking_context.py` (legacy TC-1 path)

SkillCorner and Metrica now derive velocity through the silly-kicks builders' **`preprocess=` seam** —
the same seam **GS and IDSSE already used** (`pipeline.py:160,248`). The delete-and-depend was done
for frame *building* in TF-23 (ADR-034) and left half-finished at *velocity*; this closes it.
Precedent: ADR-055 (DFL parse port).

TC-1 does not go through a builder, so it calls the three passes explicitly via a new
`_apply_sk_velocities` helper.

**Two traps, both load-bearing:**

- **`PreprocessConfig.default()` — never `PreprocessConfig(derive_velocity=True)`.** `is_default()` is
  **flag-based**, set only by the `.default()` factory (`_config_dataclass.py:74,113`). A hand-built
  config is passed through `resolve_preprocess` **unpromoted** (`_resolve.py:36-39`) and silently uses
  the **universal** `sg_window_seconds=0.4` instead of SkillCorner's tuned **1.0**.
- **Public API only.** `silly_kicks.tracking.preprocess._resolve` is private and not in `__all__`; TC-1
  uses `PreprocessConfig.for_provider(provider)` instead. Depending on a private module is a Hyrum's Law
  break we would be volunteering for.

### 2. The drain must fail its task when units failed

`main_drain_worker` now calls `raise_on_failed_units(summary, run_id=...)` **after** logging the
summary. The per-unit `except Exception` in `drain.py` **stays** — it is what lets a drain survive one
bad unit and roll its slice forward. The defect was never the catch; it was exiting 0 afterwards.

**Timeouts are deliberately excluded.** They roll forward by design and are a capacity signal, not a
correctness one. Only `failed` means a unit produced no rows and never will.

### 3. Re-anchor the completeness invariant off the frame clock

`expected_actions_within_coverage` derived its expectation from the **frame timestamps** — the same
quantity the dispatch filter uses to select actions. A corrupted clock shrank `emitted` and `expected`
**together**, holding the ratio near 1.0. The guard validated its output against an expectation derived
from its own corrupted input, and was **structurally incapable of detecting the class it was written
for**.

The expectation is now the unit's **bronze SPADL action count**, which no frame clock can move. The
frame window survives only as an *excuse* for a shortfall — and that excuse is itself **bounded** by
`MISMATCH_OVERLAP_FLOOR = 0.2` (adopted from silly-kicks' constant of the same name), because
otherwise a broken window would "explain" the very shortfall it caused.

The `MIN_EXPECTED_ACTIONS_FOR_CHECK = 10` skip band is **removed**: it silently exempted any unit
clipped to fewer than 10 covered actions — a real half-match included. Fixtures are now exempted by an
**explicit `is_slice` flag**, never by size.

### 4. A work unit is written all-or-nothing

One raising batch ⇒ **zero** rows for the unit. This is now a **stated contract** with a test
(`test_unit_write_atomicity.py`), not an emergent property of `applyInPandas`. A partially-written unit
is silent corruption: downstream cannot distinguish a short half from a lost one.

### 5. Two-sided, NaN-rejecting time-base guards

`assert_{work_unit,frames}_time_base` were a **one-sided floor** (`>= 1800.0`). Two mis-based clocks
passed silently: an **over-subtracted** re-base (the documented −2700 s SkillCorner double-subtraction,
ADR-040) sits far *below* the floor, and **NaN** passes every bound (`nan >= 1800.0` is `False`). Both
are now rejected explicitly.

## Alternatives considered

- **Patch the missing `if` in both copies.** Rejected: it fixes the instance, not the class. The copies
  are "kept in sync" by a comment claiming they "match silly-kicks" — and that comment is precisely what
  made the omission invisible to review. The next drift would be equally invisible.
- **Make the drain raise immediately on a failing unit.** Rejected: it destroys the drain's ability to
  complete a 5.5-hour queue and roll a slice forward. Drain to completion, *then* fail.
- **Special-case `expected == 0` in the completeness invariant.** Rejected: it does not repair the
  self-referentiality (the guard still derives its expectation from the corrupted clock), and it leaves
  the 1–9 band wide open.
- **A mart-level dbt gate as the primary fix.** Deferred: it would be a second, inverted implementation
  of the planner's predicate, asserted against a different table on an independent cron, while the drain
  legitimately rolls slices forward — so backlog would turn it red on a *correct* state. A guard that
  cries wolf gets muted. See "Not in scope".

## Consequences

### Positive

- A silent zero-row unit is now impossible to ship as SUCCESS: the task fails loudly (Decision 2), and
  the invariant that should have caught it can now actually see it (Decision 3).
- One fewer hand-maintained copy of a library function; the drift guard becomes a delete-and-depend
  guard (`test_convert_drift.py`).
- SkillCorner and Metrica gain **interpolation**, which they never had — the same preprocessing GS and
  IDSSE already receive.

### Negative

- **`vx`/`vy`/`speed` CHANGE for SkillCorner and Metrica** (two-pass on smoothed positions vs the old
  single pass on raw). SG parameters are unchanged (SC 1.0 s / Metrica 0.4 s — identical to the port's).
- **⚠️ Interpolation moves POSITIONS, not just velocities.** `interpolate_frames` **writes back to `x`
  and `y`** (`_interpolation.py:97-98`), filling previously-NaN positions for gaps ≤ `max_gap`
  (SC 0.6 s / Metrica 0.56 s). So **every position-derived feature moves** for SC + Metrica — pitch
  control, DAS, ghost-GK, xT-GK, defensive line, team shape, nearest-defender distances — and rows that
  were previously NaN-excluded now **participate**. SkillCorner is broadcast tracking and therefore
  NaN-heavy by construction, so this is a **wide** change for SC specifically. "It's just the velocity
  change" is a **wrong** explanation of the golden diff.
- **Rows in groups shorter than the SG window do NOT move** — `_smoothing.py:30-31` passes short groups
  through un-smoothed, so `derive_velocities` runs `np.gradient` on raw positions, byte-identical to the
  old port. **Unmoved rows are expected**, not evidence of a failed migration.
- **TC-1 is live**, so `bronze.spadl_tracking_context` changes too and needs its own recompute. Its
  consumers (GK identity, IDSSE minutes) must not be left on mixed-vintage velocities.
- Both goldens must be re-baselined **deliberately**. A re-baseline must never be used to paper over an
  unexpected delta: **GS and IDSSE must not move at all.**

### Neutral

- `is_slice` is an **interim fence**. The real fix is for `extract_action_context_fixture` to slice
  fixture *actions* to the frame window, making a fixture a faithful miniature of a production unit — at
  which point the flag can go. That means regenerating committed parquet and re-baselining goldens, so it
  is deliberately out of scope here. It must not look like the permanent answer.
- Empty bronze remains **unsupported upstream** (the builders read `src["match_id"].iloc[0]` and raise
  before preprocess runs) — unchanged by this ADR, and now pinned by `test_empty_bronze_is_unsupported`.
- The completeness check is still skipped entirely when frames carry no `timestamp` column
  (`pipeline.py:588`). That is now the **only** remaining silent skip on this path.

## Not in scope (deliberately deferred)

- **Work-queue terminal state (D9) + the mart-level completeness gate (D8) — to be shipped TOGETHER in
  the next PR.** They are one unit of work, not two: the gate is only sound if it can distinguish "this
  unit is missing because it FAILED" from "this unit is missing because the drain has not reached it
  yet". The drain legitimately rolls a worker's remaining slice forward, and `dbt-live-ci` runs on its
  own daily cron — so without a run-completion signal a naive `> 0` gate against gold goes red on a
  *correct* mid-backlog state, and a guard that cries wolf gets muted within a month.

  That signal **is** the queue's terminal state, which does not exist today: `DeltaWorkQueue` exposes
  only `ensure_table` / `prune` / `enqueue` / `units_for_worker` — enqueue is the sole writer, so the
  queue records what was *planned* and never what *succeeded*. It also needs `queued`/`running` +
  `started_at`, not merely a terminal stamp: a status written only at unit end leaves an OOM'd driver's
  in-flight and not-yet-started units indistinguishable at NULL, which is precisely the question
  ("how far did the drain get?") the column exists to answer.

  A `skipped_no_frames` state is explicitly NOT part of it: frames are the driving table
  (`action_context.py:528-552`), so a no-frames period is never enumerated and there is no moment at
  which a skip could be stamped.

  Shipping D8 alone was considered and rejected. With Decision 2 live, a failing unit already fails its
  task loudly, so the gate is defence-in-depth rather than the fix — which is exactly why it is worth
  waiting to build it soundly.
- **GS `sg_window_seconds` promotion bug.** `pipeline.py:248` hand-builds its `PreprocessConfig`, so per
  Decision 1's first trap **GS is running at the universal 0.4 s instead of its tuned 0.333 s**. Real,
  pre-existing, and small — but fixing it here would move GS values and ambush the golden diff, which
  asserts GS does *not* move. Filed separately.
- **The 38-action residual** across 11 matches (see the spec's §2). Not this defect — the failed unit
  accounts for exactly 550. To be characterised once the re-anchored invariant is live.
- **GS 10510 / 10511 extra time (891 actions).** Not recoverable: the provider shipped ET *events* with
  no ET *tracking frames*, so the frame-derived planner correctly never enqueued those units.

## Related

- **Specs:** `docs/superpowers/specs/2026-07-12-action-context-coverage-completeness-design.md`
- **Plans:** `docs/superpowers/plans/2026-07-12-action-context-coverage-completeness.md`
- **ADRs:** ADR-002 §5 (hard-fail-first in UDFs — worked exactly as designed here), ADR-034 / TF-23 (the
  frame-builder half of this delete-and-depend), ADR-040 + amendment (time-base guards, the completeness
  invariant this ADR re-anchors), ADR-055 (delete-and-depend precedent)
- **Incident:** run `85619159042760`, worker 5, `ac1_drain_unit_failed unit=skillcorner:1552423:2`
