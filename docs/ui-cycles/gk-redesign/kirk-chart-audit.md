# Chart Choice Audit — GK Redesign Mockups

| Field | Value |
|---|---|
| Page file | `docs/ui-cycles/gk-redesign/generate_mockups.py` (spec mockup generator, pre-implementation) |
| State module | n/a (mockups; the eventual page is `hf_taipy_app/src/state/goalkeeper.py`) |
| Audited | 2026-06-10 |
| Reference version | mad-skills 2.3.0 |
| Charts analysed | 8 |
| Findings | High: 1 (fixed in-cycle), Medium: 1 (mitigated), Low: 1 (accepted with rationale) |

Findings were applied to the generator BEFORE first render — this report records the decisions so
the eventual page implementation inherits them.

## Findings

### Finding #1 — Pressure split: grouped bar buries the gap story  (Severity: High — FIXED)

**Chart:** `pressure_split()` — was `go.Bar` × 2 series, `barmode="group"`
**Current choice (original):** Clustered bar chart
**Inferred data shape:** Ordinal, few (3 pressure terciles) × 2 series (GK vs league)
**Inferred question intent:** Deviation — "how far above the league is this GK under pressure?" The
reader's question is the GAP, not the absolute magnitudes.
**Kirk rationale:** The perceptual advantage of baseline-aligned length is diluted when the viewer
must mentally subtract bar heights across clusters; when the analytical question shifts from
absolute sizes to the difference between two measurements, the connected dot plot is Kirk's
nominated upgrade.
**Resolution:** Rebuilt as a **connected dot plot** (dumbbell per tercile, league→GK connector,
delta annotated on the connector). The widening gap across terciles — the Eyestone "same pass,
different value" story — is now the primary visual signal.
**Reference:** `references/04-alternatives-catalog.md#clustered-bar-chart`

### Finding #2 — xT-GK components: floating baselines in the stacked bar  (Severity: Medium — MITIGATED)

**Chart:** `components()` — `go.Bar` × 5, `barmode="relative"`, horizontal
**Current choice:** Stacked (diverging) bar chart
**Inferred data shape:** Categorical, few (8 GKs) × 5 signed parts summing to the composite
**Inferred question intent:** Part-to-whole (what KIND of value) + secondary cross-GK comparison
per component
**Kirk rationale:** Only the bottom segment and the total share a fixed baseline; intermediate
segments float, making per-component comparison across GKs imprecise. For nominal parts Kirk
recommends small multiples of plain bars.
**Resolution (mitigation, not replacement):** The stacked-diverging form is KEPT for the composite
gestalt (negative RAV/pressure reading left of zero is itself the insight), and the page spec gains
a **per-component drill-down interaction**: selecting a component re-renders as a single sorted
bar chart for that component (Kirk's small-multiple equivalent, served interactively). Recorded in
the mockup subtitle so the spec carries the affordance.
**Alternatives considered:** 1. **Small multiples of plain bars** — precise per-component
comparison (becomes the drill-down). 2. **Diverging bar chart** — already in effect via
`barmode="relative"` for the bipolar gain/loss split.
**Reference:** `references/04-alternatives-catalog.md#stacked-bar-chart`

### Finding #3 — Overview radar: Kirk's documented ambivalence  (Severity: Low — ACCEPTED)

**Chart:** `radar()` — `go.Scatterpolar` × 4 traces
**Current choice:** Radar chart with percentile reference rings
**Inferred data shape:** Quantitative, 3+ vars (6 percentile axes), single entity vs league refs
**Inferred question intent:** Multi-dimensional profiling (comparison vs reference)
**Kirk rationale:** Radar polygon shape depends on the arbitrary ordering of axes; overlaying
multiple entities becomes unreadable; for arbitrary variable sets a bar or dot plot communicates
individual values more accurately.
**Why accepted:** (a) the radar IS the domain-standard GK-profile idiom (StatsBomb GK radars; the
app already ships a Player Radar page — cross-page consistency); (b) all axes share one scale
(league percentiles) which removes the mixed-scale distortion; (c) only ONE entity is profiled —
league median and top/bottom-5% rings are reference lines, not competing polygons. **Constraint
carried into the spec:** multi-GK comparison must use small multiples (one radar per GK), never
overlaid polygons; axis ordering is fixed page-wide so polygon shapes stay comparable across GKs.
**Alternatives:** 1. **Bar chart** (percentile bars) — more accurate individual-value reading;
available as the drill-down list view. 2. **Dot plot** — percentile strip, considered for the
rankings table's inline cells.
**Reference:** `references/04-alternatives-catalog.md#radar-chart`

## Charts not flagged

- `philosophy_bump()` — Bump chart (rank across 6 philosophy presets). Matches selection matrix
  for rank-across-conditions × Ranking; Kirk: bump answers "who overtook whom?" — exactly the
  preset-switch question.
- `ghost_tether()` — Spatial pitch scene (contour + markers + tether). Spatially constrained
  encoding; Kirk's chart-selection framework intentionally defers (skill's image-only carve-out).
- `preshot_cone()` — Spatial pitch scene (cone polygon + vectors). Same carve-out.
- `distribution_map()` — Connection map on the pitch (origin→destination, value-colored). Matches
  Spatial × Flow; width + color redundantly encode value.
- `risk_reward()` — Scatter plot, Quantitative 2 vars × Relationship, 8 labeled points with
  median quadrant lines. Matches matrix; quadrant labels supply the annotated regions Kirk asks
  for on sparse scatters.

## Related skills

For chart integrity (zero-baseline, bubble sizing, pie sector integrity), colour (consistency,
palette, diverging scales), accessibility (red-green, redundant encoding), and annotation
sufficiency — these checks are **not** covered by this skill. Run
`mad-scientist-skills:cognitive-interface-audit` for the complementary integrity lens. (Planned
for the implemented page per the repo's CHI-audit cadence; mockups already follow the app's
dark-theme palette and every title is a Kirk "question title" with a scale-and-direction subtitle.)
