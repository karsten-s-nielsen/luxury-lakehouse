# ADR-051: GK tracking page — side-by-side mart family, staging-gated UI, pure-port ghost service

| Field | Value |
|---|---|
| **Date** | 2026-06-11 |
| **Status** | Accepted |
| **Deciders** | Karsten Skyt Nielsen, Claude |

## Context

The Goalkeeper Analytics page is being completely redone as a three-tab, tracking-provider-only
page built on the new `fct_action_context` GK column families (xT-GK with six precomputed
philosophy presets, Ghost-GK positioning, GK influence zones, pre-shot geometry). The existing GK
page and its marts (`fct_goalkeeper_stats`, `fct_gk_actions_detail`) are StatsBomb/Wyscout-only
and serve production today. The redesign must deploy without any production-visible change until
final sign-off. Spec: `docs/superpowers/specs/2026-06-11-gk-analytics-redesign-design.md`
(planning-mode architecture/security/observability audits + two cross-session review rounds
recorded in `2026-06-11-gk-analytics-redesign-audit-findings.md`).

## Decision

1. **Side-by-side mart family.** Two NEW marts — `fct_gk_tracking_actions` (action grain,
   tracking providers, GK-family projection of `stg_action_context__values` + Kimball keys +
   computed orientation columns) and `fct_gk_tracking_stats` (GK × match grain, dual-role
   aggregates) — instead of extending the legacy GK marts. Nothing legacy is modified; old-page
   cutover is a separate, later, explicitly-approved PR.

2. **Env-flag staging gating.** The new page registers in the Taipy app only when
   `LL_GK_TRACKING_PAGE=1`. The flag is set on the STAGING Space only (which must be
   private/org-visibility while the flag is on — it exposes an unreviewed page and GradientSports
   per-player WC2022 metrics). Production deploys are safe at any time: flag absent → page absent;
   marts are additive and unconsumed elsewhere.

3. **Ghost-grid service as a pure hexagonal port.** `GhostGridProvider.grid(...)` takes
   `frame_players` as DATA — the state layer performs all I/O; adapters are pure and DB-free
   testable. v1 ships `StoredSpreadProvider` only (Gaussian blob from the stored optimum +
   spread). `ModelGridProvider` (true silly-kicks density grid) is a fast-follow gated on a
   PUBLIC silly-kicks loader entrypoint (`_ghost_gk_model_cached` is private; shipping against it
   bakes a break into the next upstream bump). In model mode, failures degrade LOUDLY to a
   `stored-fallback` grid (ERROR log + on-chart `source` caption — never silent substitution).
   The model artifact, when adopted, pins its HF Hub revision (commit hash, not `main`).

4. **Orientation reconciliation, single macro home.** `ghost_gk_*` is canonical (defended goal at
   x≈0) while `pre_shot_gk_*`/`defensive_line_x` are frame-oriented. The mirror flag is anchored
   on the stored `pre_shot_gk_distance_to_goal`: the defended goal is whichever end's distance
   residual matches the stored value — exact for every GK position including sweeping keepers
   (a naive `|Δx| > 52.5` rule mis-mirrors a sweeping GK by ~15 m). The positional rule survives
   ONLY as the residual-tie tiebreak. All of it lives in `dbt_project/macros/gk_tracking_geometry.sql`,
   and the canonical actual position + mirror flag are STORED on the mart (`gk_actual_x/y`,
   `gk_frame_mirrored`) so no consumer ever re-derives orientation. REVISIT when the upstream AC
   coordinate convention is unified (relayed to the AC session 2026-06-11).

   **4b. Orphan-row policy (merge never deletes).** Both marts write via incremental MERGE (a
   `table` rebuild of a TRIGGERED synced mart strands its synced table per ADR-043 amendment 2).
   Consequence: rows whose source disappears (AC wipe + selective recompute) would linger.
   Resolution: the stats mart self-heals via an orphan-sweep `post_hook` (anti-join against the
   actions mart — exact and cheap at its grain); the ACTIONS mart's orphans follow the existing
   AC-family operator practice — `DELETE FROM fct_gk_tracking_actions` (scoped or full) before
   re-derive, alongside the ADR-043 tooling (`scripts/rederive_synced_marts.py`).

5. **Provider gating in the UI, provider-agnostic marts.** The marts carry all four tracking
   providers; the UI gates on `GK_TRACKING_PROVIDERS = ('gradientsports', 'idsse', 'skillcorner')`
   in `hf_taipy_app/src/queries/gk_tracking.py`. Metrica is excluded (anonymized players violate
   "raw IDs never reach the user"). StatsBomb 360 is excluded from v1: it cannot support the
   flagship tab (xT-GK requires tracking), its closing-time/pre-shot values use a different
   estimator (`pitch_control_method='voronoi'`, not poolable with `spearman`), and its coverage
   is sparse and non-random. Adding either later is a one-constant change plus per-chart
   estimator segmentation.

## Alternatives considered

| Option | Pros | Cons | Why rejected |
|---|---|---|---|
| Extend `fct_goalkeeper_stats` with tracking columns | one GK mart | mutates a production mart mid-flight; provider semantics diverge (PSxG vs geometry); cutover entangled with build | side-by-side is strictly safer and the old mart retires wholesale later |
| Branch-divergent staging deploy (page only on a branch) | no flag code | staging drifts from main; deploys stop being reproducible from one codebase (twelve-factor I) | env flag keeps one codebase, many deploys |
| Adapter fetches its own frame (service→queries I/O) | fewer call-site args | impure adapter, untestable without DB, lazy-import seam | architecture-audit A2; state owns I/O |
| `table` materialization for the stats mart | simplest dbt | strands the TRIGGERED synced table every rebuild (ADR-043 am. 2) → ADR-041 heal re-snapshot downtime | review H1; merge with full-recompute body is equally simple |
| Naive `\|Δx\|>52.5` orientation mirror | no extra column needed | provably wrong for sweeping keepers — the exact rows Tab 3 highlights | review H3; distance-residual anchor is exact |

## Consequences

### Positive
- Production is bit-identical until sign-off; the whole feature is dark-launched.
- The read-side contract reconciliation test (`test_gk_tracking_read_contract.py`) makes a mart
  column rename fail CI on the consumer side — a pattern worth adopting repo-wide.
- All app logic is testable without a database (pure builders, pure helpers, pure adapters).

### Negative
- Two registries must stay in sync for TRIGGERED synced marts (`refresh_synced_tables.py` +
  `dbt_project.yml: triggered_synced_marts`) — enforced by `test_strand_safe_rederive.py`.
- The five preset composites and the orientation heuristic freeze upstream-provisional behavior
  into the mart; upstream re-tuning or convention unification requires re-materialization
  (single-macro change site).
- The stats mart full-recompute body re-aggregates everything each run — acceptable at this
  grain (~thousands of rows at full corpus), revisit only if the grain ever changes.

### Operations note
- Synced tables: create via `scripts/create_synced_table.py`, then
  `uv run --extra sdk python scripts/maintain_synced_tables.py --skip-refresh` (ADR-005).
  Indexes: actions `(defending_gk_player_key, match_key)`, `(player_key, match_key)`,
  `(match_key, action_id)`; plus verify `fct_tracking_frames_synced (match_key, period, frame)`
  for the scene query. Verify with `scripts/create_indexes.py --verify`.
- `--full-refresh` selecting these models is forbidden by the ADR-043 on-run-start tripwire.
- AC wipe/re-derive runbook: operator-DELETE the actions mart rows for affected matches (or all)
  BEFORE the next build; the stats mart then self-heals via its post_hook sweep.

## Related
- **ADRs:** ADR-043 (strand-safe re-derive; dual registry + tripwire), ADR-041 (synced heal),
  ADR-005 (Lakebase grants/maintenance), ADR-002 (telemetry exception rules), ADR-013
  (mart consumption pattern), ADR-048 (xT-GK columns this page consumes)
- **Spec/plan/audits:** `docs/superpowers/specs/2026-06-11-gk-analytics-redesign-design.md`,
  `docs/superpowers/plans/2026-06-11-gk-analytics-redesign.md`,
  `docs/superpowers/specs/2026-06-11-gk-analytics-redesign-audit-findings.md`
- **Prototypes (normative layout):** `docs/ui-cycles/gk-redesign/mockups/v3_tab{1,2,3}_*.png`
