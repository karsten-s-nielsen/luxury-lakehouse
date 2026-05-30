# ADR-030: Gradient Sports bronze writer dedup is the data-quality boundary

| Field | Value |
|---|---|
| **Date** | 2026-05-30 |
| **Status** | Accepted |
| **Deciders** | Karsten Nielsen |

## Context

silly-kicks PR-S72 heads-up (2026-05-30) reported that the Gradient Sports provider ships content-divergent duplicate `(period, frameNum)` records — up to **16 copies of one frame** observed in match 10502 (175,969 unique frame keys vs 176,818 raw records; 59 duplicated frame keys; max 16 copies at frame `(1, 7315)`; 19,527 duplicate `(period, frame, player/ball)` rows after flattening). The copies differ in player positions/visibility, so they are NOT byte-identical (i.e., not a `.drop_duplicates()`-trivially-safe case for any downstream consumer).

Each duplicated frame fans out at `_flatten_frame` to N × 23 narrow-format rows (22 players + 1 ball per copy). Downstream consequences for any silly-kicks consumer:

1. **`_pressure_bekkers`** crashes on a 3-D `ball_pos` numpy broadcast error (silly-kicks 4.0.1 ships defense-in-depth ball-row dedup that catches this).
2. **~15 other tracking-features** (`pitch_control_at_action`, `add_das`, `add_team_shape`, `add_gk_influence`, `add_off_ball_context`, `add_line_break`, `add_obso`, `add_pausa`, `add_space_creation`, `add_cover_shadows`, `add_shape_graph`, `add_action_context`, `add_actor_pre_window`, `add_pressure_on_actor` with non-bekkers methods, `add_defensive_line`) **silently inflate**: their inputs become N × players and N × ball rows for the affected frame, producing wrong values without any error. The lakehouse AC-1 GS production path is affected on every match whose bronze contains duplicates.

silly-kicks 4.0.1's `_pressure_bekkers` defense-in-depth fixes the crash but does NOT fix the silent-inflation in the other 15 features — bekkers is the ONE feature where the input shape mismatch triggers a NumPy broadcast error; the others process row-by-row and silently produce wrong aggregates. silly-kicks correctly declines to take per-feature defense-in-depth across all 15 — that's caller-side data-quality responsibility.

## Decision

Add a keep-first dedup pass on `(period, frameNum)` at the **GS bronze writer** (`src/ingestion/gradientsports_tracking.py:_iter_unique_frames`), wrapping the frame iterator in both `stream_tracking_to_parquet` (production path) and `parse_tracking` (in-memory test path). The wrapper logs the dropped-duplicate count (production WARNING level) so the bug rate remains observable per-match. DO NOT add adapter-level dedup in `pipeline.py` / `convert.py`; DO NOT add per-feature dedup in lakehouse code; let silly-kicks own its defense-in-depth scope. Bronze is the lakehouse's data-quality boundary — duplicates never make it past it.

## Alternatives considered

| Option | Pros | Cons | Why rejected |
|---|---|---|---|
| A. Adapter-level dedup in `_bronze_gradientsports_to_converter_input` | One change point; no bronze re-ingestion needed | Bronze still has duplicates; every future hexagon/consumer must add its own adapter dedup; halfway-house creates maintenance asymmetry | Rejected — moves the data-quality boundary from "earliest" to "per-consumer" |
| B. Per-feature defense-in-depth (mirror silly-kicks 4.0.1 for the other 15 features) | Pure code change; no bronze touch | N copies of the dedup logic across the silly-kicks codebase that we don't own; the user-maintained silly-kicks would carry permanent maintenance burden for one provider's quirk | Rejected — over-couples silly-kicks to a provider-specific bronze data shape; silly-kicks correctly declined this scope |
| C. Skip dedup; trust the operator to re-ingest manually after each GS data refresh | Zero code change | Silent silent-inflation in 15 features the next time anyone forgets to dedup; violates the no-silent-degradation rule | Rejected — silent-bad-data is the hard-rule violation we're trying to prevent |
| D. Wait for GS provider to fix their feed | Provider-side fix; no lakehouse code touch | Provider is external; no commitment; the bug has shipped for some unknown time already and there's no signal it will be addressed | Rejected — no actionable timeline; defensive ingestion is the lakehouse's responsibility regardless |
| E. Bronze writer dedup at the source (chosen) | Earliest point; clean data forever; every downstream consumer (current AC-1, future SPADL/VAEP hexagon, ad-hoc queries) benefits without per-consumer code; "fail at source" pattern consistent with other lakehouse ingestion quirks (IDSSE DFL XML quirks live in `idsse.py`) | Requires a one-time re-ingestion of existing GS bronze to clean already-stored duplicates (~hours of compute, op step) | — |

## Consequences

### Positive

- Bronze is the single data-quality boundary: every downstream consumer (AC-1 today; future SPADL/VAEP hexagon per task #51; any future ad-hoc query) sees clean GS frames without per-consumer code.
- Pairs cleanly with silly-kicks 4.0.1's bekkers defense-in-depth: silly-kicks protects against the catastrophic crash class; the lakehouse fixes the upstream data shape. Both layers serve different purposes; neither duplicates the other's work.
- Dedup observability: the `_iter_unique_frames` helper logs `dropped N duplicate (period, frameNum) record(s) out of M unique frame(s) kept` at WARNING level per-ingestion. If the dup rate spikes or drops to zero (provider-side fix), it's visible in CI/prod logs.
- Synthetic 16-copy regression test (`tests/test_gradientsports_ingestion.py::TestFrameDedup`) keeps the dedup behavior locked: future refactors that drop the dedup wrapper fail loudly at PR time.

### Negative

- One-time operator burden: existing GS bronze (5 matches per the §8 audit: 10502, 10506, 10508, 10510, 10511, 10517) needs re-ingestion to clean already-stored duplicates. AC-1 GS prod runs against the un-deduped bronze would still silently inflate values; the PR description documents the post-merge sequence (re-ingest before triggering `compute_action_context` for GS). No automatic remediation script (deletion + re-write is per-match-id manual work; size is small enough not to warrant a remediation pipeline).
- The dedup is keep-first, content-blind. The N content-divergent copies may carry different player positions/visibility flags; we silently pick the first. If the "best" copy is always the second (e.g., higher-confidence smoothed positions), this is a silent quality loss. Provider docs don't specify which copy is canonical; keep-first matches the silly-kicks PR-S72 loader-side choice for consistency. Future audit could compare keep-first vs keep-best-confidence if the difference matters.
- The helper is provider-specific (lives in `gradientsports_tracking.py`, not a shared `dedup.py`). If another provider exhibits the same bug class, this code is the template but not directly reusable. Acceptable: until we have a second case, abstraction would be speculative.

### Neutral

- The lakehouse's "fail at source" pattern is consistent with how other provider quirks are handled today: IDSSE DFL XML parsing quirks live in `src/ingestion/idsse.py` at ingest time, not at every downstream consumer. The GS dedup follows the same template — provider-specific bronze writer is the right home.
- silly-kicks `_pressure_bekkers` defense-in-depth (4.0.1) remains useful even after this dedup ships: it protects future silly-kicks consumers who DON'T have lakehouse-style bronze dedup. The two fixes are complementary, not duplicate.

## Related

- **Commits:** TBD (single commit per branch, this PR)
- **Specs:** silly-kicks PR-S72 heads-up communication (2026-05-30); no separate lakehouse spec — small-enough surgical change documented in this ADR
- **Issues / PRs:** silly-kicks PR-S72 (4.0.1 ship), lakehouse PR (this ADR)
- **ADRs:** complements `ADR-029-silly-kicks-4-et-direction-adoption` (same PR cycle); independent decision
- **External references:** silly-kicks PR-S72 evidence (match 10502 dup-frame counts)

## Notes

Operational sequence post-merge (operator step, not automated):

1. Merge this PR.
2. **Do NOT trigger `compute_action_context` for GS** until step 3 completes — existing GS bronze still has duplicates and would silently inflate values.
3. Re-ingest GS bronze (`ingest_gradientsports` task for the 5 affected matches: 10502, 10506, 10508, 10510, 10511, 10517 per the §8 audit inventory). The dedup wrapper applies on re-ingestion and the WARNING log records the dropped-duplicate count per-match.
4. Run `compute_action_context` GS — clean bronze + silly-kicks 4.0.0 produces correct AC-1 values.

(silly-kicks 4.0.1 patch bump in a later wheel cycle adds the bekkers defense-in-depth as belt-and-suspenders, but is not blocking for this sequence — by step 4 the bronze is already clean, so the defense-in-depth never has anything to catch.)
