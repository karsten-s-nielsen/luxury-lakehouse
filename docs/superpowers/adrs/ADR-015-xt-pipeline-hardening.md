# ADR-015: XTGrid Typed Wrapper and Differential Validation for the xT Pipeline

| Field | Value |
|---|---|
| **Date** | 2026-04-26 |
| **Status** | Accepted |
| **Deciders** | Karsten S. Nielsen (human), Claude Opus 4.7 (AI) |

## Context

The D66 ExT v2 reproduction design spike (2026-04-25) produced a side-effect investigation that surfaced three latent bugs in the existing Singh-2018 xT pipeline (`src/analytics/expected_threat.py`, `src/analytics/off_ball_xt.py`, and their ingestion peers). Each bug looks small in isolation but they share a single architectural shape:

1. **Workflow card prose drift** — `workflow-cards/wf-xt-grids.yaml` Algorithm section claimed "16x12 grid" while `ExpectedThreatParams` defaults `n_zones_x=12, n_zones_y=8`. Pure documentation drift, but it shows the same primitive-obsession pattern: a code-derived constant duplicated into free-form prose with no enforcement that they stay in sync.
2. **Hardcoded grid resolution in the off-ball lookup path** — `src/analytics/off_ball_xt._lookup_xt` and `src/ingestion/off_ball_xt._load_xt_grid_from_spark` both hardcode `12` and `8` as grid dimensions instead of deriving from `xt_grid.shape`. Production runs correctly today because both producer (`expected_threat.py`) and consumer use the same `(12, 8)` defaults — a happy coincidence, not an enforced invariant. ExT v2's planned 24×16 conditional grid would silently miscalibrate every off-ball xT call until all three hardcoded sites were updated together.
3. **Magic-number value-range cap** — `validate_xt_grid` rejects grids with `max > 0.50`. Reasonable for unconditional 12×8 xT (peaks ~0.20–0.35 in production), structurally wrong for ExT v2's per-source-cell conditional formulation at 24×16 where near-box cells can approach 1.0 mathematically.

The xT pipeline today has exactly one downstream consumer (off-ball xT). The ROADMAP "ExT-style Conditional xT (xT v2 Candidate)" section and the TODO/On-Deck list (D60 EPV, D61 DOS, D62 pass-failure classification, D63 decay-weighted pass network, U6 three-axis VAEP) commit to four-to-six new consumers landing in the next two cycles. Without architectural intervention, each new consumer would either reimplement xT lookup with its own hardcoded constants — multiplying the bug-2 surface — or carry the same primitive-obsession patterns into the v2 build.

The user explicitly chose an architectural fix (this ADR's decision) over the spike's original "tactical now, architectural with Phase 0" recommendation after weighing trade-offs. See `feat/xt-pipeline-hardening` branch and the conversation captured in `memory/project_ext_v2_reproduction_posture.md`.

## Decision

Introduce `XTGrid` as the canonical typed wrapper for xT lookup grids, owning grid values, metadata (pitch dimensions, coordinate system, source competition), lookup behavior (with cross-coordinate-system conversion), structural validation (with optional opt-in upper bound), and differential validation against the previous run's grid. Eliminate the standalone `grid_to_dataframe`, `validate_xt_grid`, and `_lookup_xt` functions in favor of methods on the wrapper. Enforce workflow-card prose ↔ code-default parity for grid resolution claims via a new SSOT test.

### 1. XTGrid typed wrapper as canonical xT lookup primitive

`analytics.expected_threat.XTGrid` is a frozen dataclass at the boundary between xT producer and consumer. Producer (`compute_expected_threat_grid`) returns `XTGrid`; loader (`_load_xt_grid_from_spark`) returns `XTGrid`; consumers (`compute_off_ball_xt_frame`, future D60–D63 consumers, U6 framing) accept `XTGrid` only. Raw `np.ndarray` does not flow through the xT pipeline.

The wrapper owns:

- **Metadata**: `values: np.ndarray`, `pitch_length: float`, `pitch_width: float`, `coord_system: Literal["spadl", "statsbomb"]`, `competition_id: str | None`. Frozen dataclass status enables safe Spark `applyInPandas` closure capture per project conventions.
- **`lookup(x, y, input_coord_system)`**: looks up xT for a physical position. Handles cross-coordinate conversion when the input is in a different coordinate system than the grid (production case: tracking input in StatsBomb 120×80 → SPADL grid). Cell binning derives from `values.shape`, eliminating Bug 2's hardcoded resolution.
- **`validate_structural(max_value=None)`**: replaces standalone `validate_xt_grid`. Default `None` disables the upper-bound check, making the validator v2-friendly. Pass `max_value=0.50` explicitly to preserve v1 behavior (the global-grid validator in `src/ingestion/expected_threat.py` does this, locking in v1 semantics).
- **`validate_differential(previous, max_relative_change=0.30)`**: see §2.
- **`to_dataframe()`**: replaces standalone `grid_to_dataframe`. Embeds `competition_id` automatically when set on the wrapper.

The wrapper supports arbitrary resolutions (12×8 today, 24×16 for ExT v2, anything else) by deriving shape from `values` rather than hardcoding. ExT v2 Phase 0 (per `project_ext_v2_reproduction_posture.md`) inherits this clean abstraction — no XTGrid retrofit needed during the v2 build.

### 2. Differential validation against historical baseline

`XTGrid.validate_differential(previous, max_relative_change=0.30)` replaces the Bug 3 magic-number cap with a comparison against the previous run's grid. Logic:

- `previous=None` (first run for this `competition_id`) → skip check.
- `previous.values.max() <= 0.0` (degenerate baseline) → skip check (relative change undefined).
- Otherwise: reject if `|new.values.max() - previous.values.max()| / previous.values.max() > max_relative_change`.

The default `0.30` (30%) is calibrated for the workflow's daily cadence — in normal operation a daily incremental SPADL update changes the global grid's peak by far less. A 30% jump in a single run is the actual failure mode worth catching: training-data corruption, convergence failure, coordinate flip, or upstream schema drift that broke action classification.

The validator catches both increases AND decreases (the cap-only `[0.001, 0.50]` check missed the "values collapsed to 0.0001" failure mode). It adapts as the data evolves — ExT v2's higher-resolution grids with naturally higher peak values are accepted as long as each run is incremental.

The previous-run grid is loaded via a new `_load_previous_grid` helper in `src/ingestion/expected_threat.py` that uses `tolerate_missing_table` (per ADR-002 §3) to handle the bootstrap case — first-run on a fresh catalog returns `None` cleanly without a silent swallow.

### 3. Workflow card SSOT for grid resolution claims

`workflow-cards/wf-xt-grids.yaml` Algorithm section no longer cites a specific grid resolution. The prose now reads "configurable grid (default 12x8 cells — see `ExpectedThreatParams.n_zones_x` / `n_zones_y` in `src/analytics/expected_threat.py`)" with an explicit pointer to the code SSOT and the parity test.

A new `src/tests/test_workflow_card_xt_grid_ssot.py` parity test asserts that any `NxM grid` or `NxM cells` phrasing in the workflow card matches `ExpectedThreatParams()` defaults. The regex matches only ASCII `x` — to opt out of the parity check for a specific phrase (e.g., a historical reference to Singh's original 12x8 seed when the current default has moved on), substitute the U+00D7 multiplication-sign character for ASCII `x`.

This same SSOT pattern is intentionally not generalized to other workflow cards in this ADR — the rule earns its keep when a second card surfaces the same drift, not preemptively.

## Alternatives considered

| Option | Pros | Cons | Why rejected |
|---|---|---|---|
| A. Tactical fixes only (1-line YAML + 10-LOC shape-derivation + 5-LOC param) | Smallest diff surface, lowest review burden | Doesn't solve primitive obsession; v2 work re-faces the same constants; spike doc would ship as a deferred ADR awkward to standalone-commit per `feedback_no_doc_only_commits.md` | User explicitly chose the architectural fix in conversation 2026-04-25 after weighing trade-offs |
| B. Three separate ADRs (one per bug) | Maximum traceability per defect | Fragmentation; the three bugs are symptoms of one architectural shape; ADR-002 sets multi-section precedent for related controls | Cohesion preferred; matches ADR-002 pattern |
| C. XTGrid wrapper + differential validator + YAML SSOT — single ADR with three sections | Cleanest narrative; one cohesive ship; pre-positions ExT v2 | Larger diff surface than Option A | **Chosen** |
| D. Differential validator as a reusable module (xG calibration, VAEP drift, etc.) | Reusability across workflows | Premature generalization — only xT needs it today; YAGNI | Defer reusability until a second consumer materializes |
| E. Embed coord_system + grid metadata in the bronze table itself (Delta table properties or sidecar JSON) | Self-describing serialization; no out-of-band convention required | Schema change with downstream impact (off-ball xT loader, future D60+ readers); adds complexity to first-run bootstrap | Defer until a second producer (e.g., ExT v2 conditional grids) creates a real ambiguity. Today the convention "grids in bronze.expected_threat_grids are SPADL 105×68" is enforced at the loader, sufficient for the single-producer case |

## Consequences

### Positive

- ExT v2 Phase 0 (deferred per `project_ext_v2_reproduction_posture.md`, gated on Kimball PR 7) inherits a clean abstraction — no XTGrid wrapper retrofit needed during the v2 build.
- All three latent bugs eliminated in one cycle, removing the silent-miscalibration risk that would have surfaced during ExT v2 rollout.
- Differential validator catches regression patterns the magic-number cap couldn't: drift in either direction, gradual vs sudden change, training-data corruption that doesn't blow the absolute upper.
- New domain primitive establishes a contract for the four-to-six future xT consumers committed in TODO/ROADMAP (D60 EPV, D61 DOS, D62 pass-failure classification, D63 decay-weighted pass network, U6 three-axis VAEP, neural xT successor).
- SSOT discipline propagates to the workflow card layer with an opt-out mechanism (U+00D7 substitution) for legitimate historical references.

### Negative

- Wrapper overhead per `lookup()` call (microsecond-scale Python method dispatch). Off-ball xT's per-frame loop already does N lookups per frame; the marginal cost is negligible against the pitch-control compute that dominates the per-frame budget.
- Differential validator requires a Spark read of the previous bronze grid before writing the new one (one extra `SELECT zone_x, zone_y, xt_value FROM ... WHERE competition_id = X` per workflow run, sub-second per grid given the 96-row payload).
- Wheel bump to 0.3.15 required before this can ship (HF Jobs script `scripts/compute_xt_grid_hf.py` is wheel-pinned). Bumped + propagated to 21 consumer files in the same commit.
- The currently-stashed design doc `docs/superpowers/specs/2026-04-25-ext-v2-reproduction-design.md` (D66 spike output) cannot ship in this commit per `feedback_no_doc_only_commits.md` — it will land with Phase 0 of the ExT v2 reproduction build.

### Neutral

- `bronze.expected_threat_grids` schema unchanged. XTGrid metadata is process-side only and not persisted to Delta. Future v2 work may extend the schema (Option E in Alternatives) when a second producer creates an ambiguity worth resolving.
- `OffBallXtParams` drops `pitch_length` and `pitch_width` fields. Pitch dimensions now live on the `XTGrid` instance passed in, not duplicated on the consumer's params. Tests updated in the same cycle.

## Related

- **Branch:** `feat/xt-pipeline-hardening`
- **Specs:** `docs/superpowers/specs/2026-04-25-ext-v2-reproduction-design.md` (D66 spike output; currently stashed, ships with Phase 0 of ExT v2 build)
- **Memories:** `project_ext_v2_reproduction_posture.md`, `project_ext_v2_session_handoff_2026_04_25.md`, `feedback_no_doc_only_commits.md`, `feedback_no_value_leaking_env_check.md`
- **TODO/ROADMAP:** D66 (TODO/On-Deck), "ExT-style Conditional xT (xT v2 Candidate)" (ROADMAP)
- **ADRs:** complementary to ADR-002 (silent exception swallow elimination); both are data-integrity defense-in-depth controls in the same family. The differential validator's pattern (compare-against-previous to catch regression in either direction) could be reused if a second workflow surfaces the same shape; not generalized in this ADR per Alternatives Option D.
- **External references:** Singh, Karun (2018). "Introducing Expected Threat (xT)." [karun.in/blog/expected-threat.html](https://karun.in/blog/expected-threat.html). Salimi & Salmankhah (2026). "ExT: Improving the Computational Efficiency and Spatial Granularity of the Expected Threat Model." LISS Football Analytics Symposium 2026-04-23 (poster, paper pre-publication; tracked at T1 in `docs/research/external-research-tracking.md`).

## Notes

The conversation that produced this decision walked through three sequencing options ("tactical now / architectural with Phase 0 / standalone v1 hygiene cycle") and the user picked the third — fix all three bugs properly on a dedicated `feat/xt-pipeline-hardening` branch independent of any v2 timing. Reasoning captured in `project_ext_v2_reproduction_posture.md`: the v1 architectural work has standalone value (good engineering even if v2 never happens), and coupling it to v2 timing creates an artificial dependency.

The `_load_previous_grid` helper uses `tolerate_missing_table` (ADR-002 §3) to handle the first-run case — confirming that ADR-002's helper continues to be the right pattern for "table may not exist on first run" bootstrap reads, not a silent `except Exception:` swallow.

Test coverage added in this cycle: `TestXTGrid` (4 tests), `TestXTGridLookup` (8 tests), `TestXTGridToDataFrame` (4 tests), `TestXTGridStructuralValidation` (7 tests), `TestXTGridDifferentialValidation` (5 tests), plus `TestWorkflowCardXTGridSSOT` (2 tests) and parameterized arbitrary-resolution loader test. Total xT test surface: 74 tests passing pre-commit.
