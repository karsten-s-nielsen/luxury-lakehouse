# Chart Choice Audit — Goalkeeper Tracking page (implementation re-run)

| Field | Value |
|---|---|
| Page file | `hf_taipy_app/src/pages/gk_tracking.py` |
| State module | `hf_taipy_app/src/state/gk_tracking.py` |
| Audited | 2026-06-11 (plan Task 9.1 — re-run over the mockup-stage baseline `kirk-chart-audit.md`) |
| Reference version | mad-skills 2.3.0 |
| Charts analysed | 7 |
| Findings | High: 0, Medium: 0, Low: 1 (+1 optimization fix applied during audit) |

The implemented builders are 1:1 ports of the v3 prototype charts whose types were already
audit-fixed at mockup stage (grouped-bar→connected-dot upgrade, radar dropped, bump retained).
This re-run verifies the implementation didn't regress those decisions.

## Charts not flagged

- `_build_bump_figure` — Bump chart; rank-across-presets × Ranking ("who overtakes whom under a
  game-model switch"). Matches matrix; selected GK distinguished by color AND line weight
  (redundant encoding).
- `_build_dist_map` ×2 — Connection map on the pitch (Spatial × Flow), value carried by color +
  width (redundant); 5th–95th pct scale stated in the on-chart caption.
- `_build_scene_figure` — Spatial pitch scene (contour + markers + tether); chart-selection
  framework carve-out. Grid provenance (`stored`/`model`/`stored-fallback`) rendered in the
  TITLE per the no-silent-substitution rule.
- `_build_context_figure` — Dot plot across ordinal line-height terciles (Ordinal × Deviation),
  n= carried in category labels; game-state series appears only when the data supports it,
  with an on-chart caption when hidden.
- `_build_closing_figure` — Connected dot plot (GK vs sample) — the baseline F1 Kirk upgrade,
  preserved in implementation.
- `_build_cone_figure` — Spatial scene; carve-out.
- `_build_shotmap_figure` — Scatter (Quantitative 2 vars × Relationship), goals encoded by
  shape (star) + color (red) — redundant encoding; n= in legend labels.

## Findings

### Finding #1 — PEV display rule is latently relevant (Severity: Low)
**Context:** No xT-GK component chart ships in v1, so the spec's "PEV ≈ 0 by construction —
must caption" display rule has no current target. If a component-decomposition chart is added
later (it exists in the v1 mockup set), the caption rule in the mart contract column comment
(`xt_gk_pev`) and spec §4.1 applies. No action now; recorded so it isn't lost.

### Applied during audit — arrow-count bound (optimization, not chart-choice)
`_build_dist_map` emitted one Plotly trace per pass with no bound; at full-corpus volumes that
degrades browser render time. Bounded at `_MAX_MAP_ARROWS = 500` with the truncation stated in
the chart TITLE (no silent caps).

## Related skills

Chart integrity / colour / accessibility / annotation checks belong to
`mad-scientist-skills:cognitive-interface-audit` — static pass done this cycle (scale+direction
in every metric help_text; no raw IDs — dim-joined display names only; provenance captions;
em-dash empty metrics; graceful warning paths verified live in the boot smoke). Full audit-mode
re-run against the data-populated page remains a Task 11 follow-item.
