# Action-context coverage completeness — design (v3)

**Date:** 2026-07-12
**Status:** Proposed
**Supersedes:** v1 (hypothesis falsified by the drain logs) and v2 (fixed the bug, left the class in place).
§10 keeps the record of what v1 got wrong, because the failure mode is instructive.
**Origin:** Surfaced sideways from the silly-kicks 4.46.0 (PR-S113) handoff. Those NULLs are not ours.
Investigating them was.

---

## 1. Summary

**Root cause (evidenced).** `analytics/action_context/convert.py:79-88` re-implements silly-kicks'
short-group velocity fallback and **drops its `len(x_vals) <= 1` guard**. A one-frame player track therefore
reaches `np.gradient`, which requires ≥ 2 elements, and raises. Such a track exists in SkillCorner match
`1552423`, period 2, frame batch 184.

| # | step | consequence |
|---|---|---|
| 1 | 1-frame track hits `np.gradient` (`convert.py:82`) | `ValueError` |
| 2 | UDF re-raises with the group key (ADR-002 §5 — **working as designed**) | `RuntimeError: … match_id=1552423, period=2, frame_batch_id=184` |
| 3 | One raising batch fails the **whole** `applyInPandas` write | unit emits **0 rows** — all 550 actions, not just batch 184's |
| 4 | `drain_worker` catches it (`drain.py:170-181`): `failed += 1`, `continue` | drain survives (intentional — §4 D2) |
| 5 | `main_drain_worker` logs, never raises (`action_context.py:1300-1308`) | task → **SUCCESS** |
| 6 | Job **SUCCESS**; queue has no status; no mart check | **nobody ever knew** |

**The one-line defect cost 550 rows. The silence cost a month.** Steps 3–6 are the subject of this spec.

**Bounded.** Across all 8 workers of `run_id` 85619159042760: 373 units processed, **exactly 1 failed** —
`skillcorner:1552423:2`, reconciling exactly with the 550 missing rows.

**Chosen remedy (owner decision):** do not patch the copy — **delete it**. The lakehouse is a consumer, not a
source (§4 D1). This changes `vx`/`vy`/`speed` for SkillCorner and Metrica, so a full AC recompute is required —
**which subsumes the 550-row backfill.**

## 2. Evidence

Live `soccer_analytics` + Databricks job API, 2026-07-12. Run `85619159042760` (2026-07-11), wheel **0.5.77**.

**The swallowed failure** (worker 5 driver log):

```
ERROR ingestion.action_context_drain:
  ac1_drain_unit_failed run_id=85619159042760 worker_id=5 unit=skillcorner:1552423:2 err=
  RuntimeError: action_context UDF failed for match_id=1552423, period=2, frame_batch_id=184:
    analytics/action_context/pipeline.py:428          enrich_batch
    analytics/action_context/pipeline.py:192          _convert_tracking_batch
    analytics/action_context/sk_frame_adapters.py:145 convert_skillcorner_bronze_to_frames
    analytics/action_context/sk_frame_adapters.py:98  _finalize
    analytics/action_context/convert.py:82            _derive_velocities_savgol
    numpy/lib/function_base.py:1222                   gradient
  ValueError: Shape of array too small to calculate a numerical gradient,
             at least (edge_order + 1) elements are required.

INFO  ac1_drain_end run_id=… worker_id=5 processed=46 failed=1 timed_out=0 rows=29166
```

Job result: **SUCCESS**. Task `compute_action_context`: **SUCCESS**.

**The guard we dropped** — silly-kicks `tracking/preprocess/_velocity.py:69-76`, which even names the incident
it was fixed for:

```python
if len(x_vals) < window_frames:
    # Single-frame groups have no meaningful velocity -- np.gradient
    # requires at least 2 points. Real-world example: GS WC2022
    # match 3851, away #10 has exactly 1 frame in period 2.
    if len(x_vals) <= 1:          # <-- ABSENT from both lakehouse copies
        vx[idx_arr] = np.nan
        vy[idx_arr] = np.nan
        continue
```

**The 1,479-action gap** (bronze SPADL rows with no `fct_action_context` row):

| population | actions | cause |
|---|---|---|
| SkillCorner `1552423` p2 | **550** | This defect. Recovered by the §5 recompute. |
| GS `10510` + `10511`, extra time | **891** | Provider gap — **no ET tracking frames exist**, while ET *events* were shipped. The frame-derived planner correctly never enqueued them. **Not recoverable.** |
| scattered, 1–11 across 11 matches | **38** | Unexplained. See §8. |

**The mart column is clean.** `fct_action_context.is_gk_distribution` has **zero** NULLs, every provider. The
NULLs silly-kicks' loader sees are LEFT-JOIN misses, masked by its `fillna(False)`.

> **899-vs-922 delta.** silly-kicks reported 899 GS NULLs; this probe measures 922 (SkillCorner matches
> exactly: 557 = 557). Unexplained — most plausibly a different bronze snapshot. §7 pins the snapshot so the
> denominator stops moving.

## 3. Goals / non-goals

**Goals** — delete the duplicated velocity derivation (close the *class*, not the instance); make a swallowed
unit failure impossible to ship as SUCCESS; fix the completeness invariant's self-referentiality; recover the
550 rows; record the 891 that cannot be.

**Non-goals** — xT-GK v2, the silly-kicks pin, the ρ model. Gradient Sports' ET tracking gap (a provider
conversation).

## 4. Defects

### D1 — The bug lives in a hand-maintained copy of a library function. Delete it. *(review N1)*

**Correcting the review's premise:** the lakehouse **has** adopted `silly_kicks.tracking.skillcorner.convert_to_frames`
(`sk_frame_adapters.py:139-142`, TF-23/ADR-034). The duplication is **not** the converter — it is only the
velocity step, applied afterwards in `_finalize`.

And our port is **not a stale copy**; it is a *documented deliberate divergence* (`convert.py:29-33`):
silly-kicks runs a **two-pass** pipeline (`smooth_frames` → `derive_velocities` on smoothed positions); ours
applies a **single SG derivative pass on raw positions** — *"numerically slightly noisier but practically
equivalent… Acceptable for v1; align with two-pass if velocity quality proves insufficient."*

That has two consequences the review missed:

- A parity test *"asserting the port reproduces `derive_velocities`"* is **impossible as specified** — the
  algorithms differ by construction; such a test would assert something false.
- Deleting the port **changes numbers**. It is a migration, not a patch.

**There are TWO copies, and the drift test welds them together.** The identical unguarded `np.gradient` also
sits at `ingestion/tracking_context.py:897-901`, and `test_convert_drift.py:46` asserts **AST-level equality**
between the two `_derive_velocities_savgol` bodies. Fixing one alone breaks that test. The legacy path carries
the same latent crash today.

**Call surface (mapped, and narrower than feared):**

| path | providers |
|---|---|
| AC — `sk_frame_adapters._finalize` (`convert.py`) | SkillCorner, Metrica |
| legacy — `tracking_context.py:1050,1169` | Metrica, SkillCorner |

Gradient Sports and IDSSE route through silly-kicks' native adapters (ADR-035) and are **unaffected**.

**Fix.** Delete `_derive_velocities_savgol` from **both** copies and route both paths through silly-kicks
(`tracking.preprocess.smooth_frames` → `derive_velocities`, or the builder's native `PreprocessConfig`
`derive_velocity=True`, which `skillcorner.py:118,282-291` already supports but leaves off by default).
`test_convert_drift.py` already carries a "must not re-grow" symbol list (`:35-42`) — add
`_derive_velocities_savgol` to it, and delete the now-meaningless AST-equality test.

**Integration detail that will bite:** `derive_velocities` **raises** unless `smooth_frames` ran first
(it requires `_preprocessed_with`), and `smooth_frames` adds `x_smoothed`/`y_smoothed` columns —
which `_finalize`'s exact-schema check (`sk_frame_adapters.py:100-102`, symmetric diff) will reject.
The extra columns must be dropped before the schema assert.

**Blast radius, stated plainly:** `vx`/`vy`/`speed` change for SkillCorner and Metrica → pitch control, DAS,
ghost-GK, xT-GK all move for those providers → **full AC recompute + golden re-baseline (mini AND full) +
benchmark re-check**. The legacy `tracking_context` consumers (GK identity, IDSSE minutes) must be checked for
velocity dependence before its copy is switched.

### D2 — The drain reports SUCCESS with failed units

`drain.py:170-181` swallows; `action_context.py:1300-1308` never raises on `failed > 0`.

**Chesterton's Fence: the swallow is correct and stays.** It exists so one bad unit cannot destroy a 5.5-hour
drain and so a worker's slice rolls forward. The defect is that the *task* then exits 0.

**Fix:** drain to completion, **then fail the task if `failed > 0`**, surfacing `summary.failed_units`. Work is
still not lost; the failure is loud. Timeouts (`summary.timed_out`) are deliberately **excluded** — they roll
forward by design and are a capacity signal, not a correctness one.

### D3 — The completeness invariant is self-referential *(review B4)* — and so is its excuse *(review S1)*

`expected_actions_within_coverage` (`completeness.py:33-58`) derives its expectation from the **frame
timestamps** — the same quantity the dispatch filter uses to select actions. A corrupted clock shrinks
`emitted` and `expected` **together**; the ratio stays ≈ 1.0. The guard validates its output against an
expectation derived from its corrupted input.

**Fix (two levels, and the second is the one that closes the class):**

1. **Anchor `expected` on the bronze SPADL action count** for the `(provider, match, period)` unit — an input
   independent of the frame clock.
2. **Bound the excuse.** A shortfall may be excused only when the missing actions lie outside frame coverage —
   but "outside frame coverage" is *computed from the same suspect window*. A broken clock would produce a
   window that "explains" its own shortfall, and the invariant goes quiet again. So before any window-based
   excuse is accepted, require the frame window to **overlap the action time span by a floor**. Adopt
   silly-kicks' idiom directly: `validate_time_base` / `MISMATCH_OVERLAP_FLOOR = 0.2`
   (`silly_kicks/tracking/utils.py:28,597`). Below the floor, the shortfall is **unexplained → raise**.

**Constraint that must not be broken.** The window-relative design exists to keep slice fixtures and genuine
partial broadcast coverage valid. The partial-coverage regression test is **mandatory**.

### D4 — The skip band is `expected < 10`, not `expected == 0` *(review B5)*

`MIN_EXPECTED_ACTIONS_FOR_CHECK: int = 10` (`completeness.py:30`) with an unconditional early return below it.
A broken window clipping a unit to 9 covered actions and emitting 0 passes silently **today**.

The docstring is **stale** — *"`0` skips the check — nothing to lose"* (`completeness.py:76-77`) — while the
`Raises` section documents the real `< 10`. That stale prose is exactly what misled v1 of this spec into
proposing an `expected == 0` special-case. Fix it in the same change.

**Re-derived band (stated, not deferred):** with a frame-clock-independent expectation (D3), the M13 boundary
ambiguity that justified the threshold applies to **slice fixtures**, not to production halves. So the
production band becomes **0** — every unit is checked — and fixtures are exempted **by size** (an explicit
`is_slice` / small-fixture flag), not by a magic threshold that also silently exempts real half-matches.

### D5 — `assert_frames_time_base` has no lower bound and no NaN check

`time_base_guard.py:43,94` is a one-sided floor (`>= 1800.0`). The documented −2700 s double-subtraction
(ADR-040) passes trivially. **`NaN >= 1800.0` is `False`, so NaN also passes silently today** — a lower bound
alone does not catch it; explicit `math.isnan` rejection is required. Latent, not implicated here, but the same
guard family and the same blind spot. D3's overlap floor makes this partially redundant, which is a good sign.

### D6 — One raising batch zeroes the whole unit: a 550× amplifier, neither pinned nor decided *(review S2)*

Step 3 of §1 is asserted, not tested. It is what turned a one-track defect into a half-match loss.

- **Pin it:** a test that a raising batch yields **zero** written rows for the unit, so a future change to
  write atomicity cannot silently alter the blast radius.
- **Decide it:** all-or-nothing per unit **is** defensible — a partially-written unit is the silent-corruption
  case ADR-040 exists to prevent, and is arguably worse than none. **This spec adopts it as a stated decision**
  rather than leaving it an emergent property of `applyInPandas`.

### D7 — Existing e2e assertions are vacuous or unreachable

- `test_gs_e2e_convert_and_enrich_does_not_crash` asserts only `set(result.columns) == expected` — but
  `_empty_result()` (`pipeline.py:119-123`) returns a **zero-row frame carrying the full schema**, so it passes
  on an empty emit: precisely the failure we are trying to catch. Add `assert len(result) > 0`.
- The SkillCorner/Metrica `len(result) > 0` assertions (`test_e2e.py:98,116`) — including the one whose message
  is literally *"SC P2 slice resolved zero actions"* — sit behind `AC1_E2E=1` and **never run in CI**. Promote
  them, or add mini-slice fixtures mirroring `J03WMXmini_p1`.

A test that cannot fail on the bug it names is not a test.

### D8 — Mart-level gate (defence in depth; permitted to be dropped)

With D2 shipped, a failing unit fails its task loudly, so the gate is **not load-bearing** — it is a backstop
for a unit failing in some *new* way.

**Review B6 is right that a naive gate is a trap:** it would be a second, inverted implementation of the
planner's predicate, against a different table (`gold.fct_action_context`, incremental, INNER JOIN
`dim_matches`), on an independent daily cron, while the drain legitimately rolls slices forward. Backlog, a
rolled-over slice, or a newly-ingested match would each turn it red on a **correct** state — and a guard that
cries wolf gets muted.

So the gate must be **run-completion-aware** (assert only over units a *completed* run enqueued, that have
frames), plus a parity test asserting the Python planner predicate and the dbt SQL agree on the current corpus.
**If that parity cannot be made to hold cleanly, drop the gate.** D1 + D2 + D3 already close this incident, and
a false-firing guard is worse than none.

### D9 — Queue has no terminal state (deferred)

`DeltaWorkQueue` exposes `ensure_table` / `prune` / `enqueue` / `units_for_worker` — **no update or merge
method**. Enqueue is the only writer.

Two things v1 got wrong: `skipped_no_frames` **has no decision point** (frames are the driving table,
`action_context.py:528-552` — a no-frames period is never enumerated, so there is no moment to stamp a skip;
**dropped**), and a status stamped only at unit *end* cannot answer "how far did the drain get" — an OOM'd
driver leaves in-flight and not-yet-started units indistinguishable at NULL. That needs
`queued`/`running`/`succeeded`/`failed` + `started_at`.

With D2 shipped this is **operability, not detection**. Given the writer does not exist, and the schema-parity
test guards only the CREATE migration and not ALTERs (`test_work_queue_schema_parity.py:13-15` hard-codes the
2026-06-02 file; `kde_backend` column ordering already differs between fresh-catalog and live-prod, unguarded),
this is **deferred to its own change**. Not needed to close this incident; carries real migration risk.

## 5. Sequencing

1. **D2** — fail the task on `failed > 0`. **First**, so the large recompute in step 4 cannot fail silently.
2. **D1** — delete both velocity copies; route through silly-kicks; drop the AST-equality test and add the
   symbol to the no-re-grow list; handle the `smooth_frames` schema-drift detail.
3. **D3 + D4 + D5 + D6 + D7** — invariant re-anchor + overlap-floor excuse; skip band → 0 with fixtures exempt
   by size; NaN/lower-bound rejection; pin the all-or-nothing decision; make the e2e assertions real.
4. **Full AC recompute** (SkillCorner + Metrica values change) → **subsumes the 550-row backfill**; re-baseline
   mini AND full goldens; re-run benchmarks.
5. **D8** gate, only if planner parity holds. **D9** deferred.

D2 before everything is not optional: recomputing before the task fails loudly would risk the recompute failing
the same silent way.

## 6. Testing

| defect | test |
|---|---|
| D1 | 1-frame player track → NaN velocity, no raise (the guard behaviour, **not** numeric parity — the algorithms differ by construction) |
| D1 | `test_convert_drift` asserts `_derive_velocities_savgol` does not re-grow in either module |
| D1 (e2e) | reproduce the real `skillcorner:1552423:2` batch-184 slice locally — pure pandas, no Spark, via `run_work_unit` |
| D2 | drain with one failing unit → task exits non-zero, failed labels surfaced; timeouts still roll forward |
| D3 | corrupted frame clock, intact bronze counts → **raises** (passes today) |
| D3 | window overlapping the action span below `MISMATCH_OVERLAP_FLOOR` → excuse **rejected**, raises |
| D3 (regression) | genuine partial broadcast coverage → does **not** raise |
| D4 | 9 covered actions, 0 emitted → raises |
| D5 | frame min of −2700 s **and** NaN → raises |
| D6 | a raising batch ⇒ zero rows written for the unit (pins the amplification) |
| D7 | GS e2e asserts `len(result) > 0`; SC/Metrica row assertions run in the default suite |
| recompute | `skillcorner:1552423:2` has 550 AC rows; 0 NULL |

**Snapshot pinning.** Record bronze `_ingested_at` bounds for every §2 figure, so the denominator is fixed and
the 899/922 delta cannot silently re-open.

## 7. Risks

- **D1 changes values.** SkillCorner + Metrica `vx`/`vy`/`speed` move → every downstream tracking feature moves
  for those providers. Goldens must be re-baselined *deliberately*, and a re-baseline must never be used to
  paper over an unexpected delta. Diff the recompute against the current mart and **explain** the deltas before
  accepting them.
- **Legacy `tracking_context` consumers.** GK identity and IDSSE minutes read `bronze.spadl_tracking_context`.
  Check their velocity dependence before switching that copy.
- **D2 turns latent failures loud.** The first post-D2 drain may fail where it previously "succeeded". Expected
  post-D1 state is 0 failures; if a drain still fails, that is new information — investigate, do not suppress.
- **D3's excuse floor** could false-fire on genuinely sparse tracking. The partial-coverage regression test is
  the control.
- **D8 can cry wolf.** Explicitly permitted to be dropped rather than shipped unsound.

## 8. Open

- **The 38-action residual.** Not this defect (the failed unit accounts for exactly 550). Likely M13
  slice-boundary ownership or actions outside frame coverage. Characterise **after D3**, whose
  frame-clock-independent expectation is the instrument that would explain it. Recorded, not fixed.
- **GS 10510 / 10511 extra time (891 actions).** Not recoverable — no ET frames exist. Record as a known
  provider coverage gap; raise with Gradient Sports, who ship ET events without ET tracking.
- **No in-process duplicate-`action_id` guard in production.** The M13 `RuntimeError` exists only in the local
  hexagon (`pipeline.py:572-579`); `ingestion/action_context.py` has none (ADR-040:52-54). A second asymmetry
  between the two drivers that the lockstep sentinels do not cover. Out of scope; recorded.

## 9. ADR

D1 (delete-and-depend on a library function, changing production values) and D6 (all-or-nothing unit write as a
*stated* contract) both meet the ADR bar in `CLAUDE.md` — a cross-cutting dependency change and a structural
pipeline trade-off. One ADR covering both, referencing ADR-040 and ADR-055's delete-and-depend precedent.

## 10. What v1 of this spec got wrong (kept deliberately)

v1 asserted: *"The guard was live, the unit still emitted zero rows, and nothing failed."* The last clause was
**never checked**. The drain's swallow means a raised exception and a vacuously-passing invariant produce an
**identical signature** from the mart — so "nothing failed" was unknowable from the evidence v1 had, and it was
false. From that premise v1 built a vacuity hypothesis and proposed tightening the invariant — a fix that would
not have touched this bug, because **the invariant never ran**: the write raised before it. v1's §3 was
additionally self-refuting: the absolute-clock cause it named would have been caught by `assert_frames_time_base`,
contradicting its own "nothing failed".

Two process lessons — **to be promoted into `docs/engineering/` so they outlive this spec**:

1. **Read the logs before theorising.** The driver log was one query away and settled in minutes what the spec
   had reasoned about at length.
2. **A stale docstring propagated straight into a design.** `completeness.py:76-77` said "`0` skips the check";
   the code says `< 10`. v1 trusted the prose over the code. Prose is not a contract — and neither is a comment
   claiming a copy "matches silly-kicks" (D1).

What v1 got right, and this version keeps: gate on diagnosis before fixing; demand that any fix explain *why the
existing guard passed*; separate the gap by cause rather than symptom; record the 899/922 delta rather than
smoothing it; state blast radius up front.
